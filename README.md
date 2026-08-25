# bq_sync_kit

A cross-project JSONL → BigQuery file sync tool.

Every job follows the same steps:

0. Optionally run a **producer** script that generates the JSONL in the first place — for example, exporting rows out of MySQL. Any executable works, so a new data source needs a new script, not a change to this repo;
1. Scan the configured directories for JSONL files whose **data date is earlier than the current date** (today's in-progress segments are left alone), or take the file list straight from the producer's manifest;
2. Upload them to the corresponding BigQuery warehouse using each project's own configuration (`WRITE_APPEND`, equivalent to `bq load --noreplace --source_format=NEWLINE_DELIMITED_JSON`);
3. Optionally run a **cleanup** script once a file has been accepted, to drop the exported rows from the source;
4. Optionally, `mv` successfully synced files into an archive directory.

All projects share a single MySQL state database, which records which files have already been synced and provides cross-host mutual exclusion via MySQL's `GET_LOCK`.

## Installation

```bash
git clone git@github.com:CNYoki/bq-sync-kit.git
cd bq-sync-kit
uv venv --python 3.11 .venv && uv pip install -e ".[dev]"
```

## Quick start

```bash
cp config.example.yaml config.yaml   # edit as needed
cp .env.example .env && chmod 600 .env   # secrets referenced as ${VAR} from the config
bq-sync-kit --config config.yaml validate        # validate the config
bq-sync-kit --config config.yaml list            # show the expanded projects and jobs
bq-sync-kit --config config.yaml run --dry-run   # list files only; no MySQL / BigQuery connection
bq-sync-kit --config config.yaml run             # run the real sync
```

Without `--config`, the following locations are checked in order: `$BQ_SYNC_KIT_CONFIG`,
`./config.yaml`, `~/.config/bq_sync_kit/config.yaml`, `/etc/bq_sync_kit/config.yaml`.

## Project layout

```
bq_sync_kit/            the package itself
├── cli.py              argument parsing and subcommands
├── config.py           YAML loading, include expansion, ${VAR} expansion, inheritance
├── discovery.py        file scanning and data-date extraction
├── sync.py             per-file orchestration: lock → produce → upload → clean up → archive
├── hooks.py            producer / cleanup script execution and manifest parsing
├── bigquery_loader.py  chunked upload and load-job polling
├── repository.py       state-table reads and writes
├── db.py               engine and named locks
├── models.py           SQLAlchemy models
└── notifier.py         failure notifications
scripts/
├── bq_sync.sh          cron wrapper: flock, .env loading, logging, log rotation
├── crontab.example     sample schedule
├── user_live_visits_drain.py           producer + cleanup for a drained MySQL table
└── douyin_creator_checkpoint_export.py producer for an export-only MySQL table
tests/                  pytest suite
config.example.yaml     annotated reference config
.env.example            secrets template
```

`config.yaml`, `.env`, `conf.d/` and `logs/` hold local deployment state and are gitignored —
keep credentials out of the repo and reference them through environment variables.

## Commands

| Command | Purpose |
| --- | --- |
| `run` | Run the sync. Supports `--project`, `--job`, `--dry-run`, `--boundary-date`, `--limit` |
| `list` | Print the job configuration after inheritance is expanded — useful for confirming paths and target tables |
| `status` | Inspect the sync records in MySQL; supports filters such as `--status failed` |
| `init-db` | Create only the state database and state table |
| `validate` | Validate the configuration only |

Omitting the subcommand is equivalent to `run`, which makes it easy to drop straight into crontab:

```bash
30 1 * * * /srv/bq_sync_kit/.venv/bin/bq-sync-kit --config /etc/bq_sync_kit/config.yaml run >> /var/log/bq_sync_kit.log 2>&1
```

For a real deployment prefer [`scripts/bq_sync.sh`](scripts/bq_sync.sh), which resolves its own
paths, sources `.env`, guards against overlapping runs with `flock`, writes a dated log file and
prunes logs older than 30 days. See [`scripts/crontab.example`](scripts/crontab.example) for a
sample schedule.

## Configuration

The config is YAML, with the hierarchy `defaults` → `project` → `job`; keys defined at a lower
level override those above. All strings support `${VAR}` and `${VAR:-default}` expansion.
See [`config.example.yaml`](config.example.yaml) for a complete example.

```yaml
mysql:                     # state database shared by all projects
  host: 127.0.0.1
  database: bq_sync

defaults:
  timezone: Asia/Singapore  # timezone used to decide what counts as "before the current date"
  location: US

projects:
  - name: mediacrawler
    root: /home/yoki/work/    # base directory for relative paths
    project_id: my-gcp-project
    credentials_path: /etc/gcp/credentials.json
    jobs:
      - name: creator_contents
        source_glob: data/jsonl/creator_contents_*.jsonl
        target_table: table.list
        archive_dir: /data1/    # leave empty to keep files in place
```

Once you have a lot of projects, you can split them into one file per project and pull them
together with `include` (included files have lower precedence than the main file):

```yaml
include:
  - conf.d/*.yaml
```

### Inheritable keys

`project_id`, `credentials_path`, `location`, `timezone`, `root`, `archive_dir`,
`archive_layout`, `date_source`, `date_pattern`, `date_format`, `recursive`,
`skip_empty_files`, `autodetect_schema`, `ignore_unknown_values`,
`max_bad_records`, `write_disposition`, `upload_timeout_seconds`,
`job_timeout_seconds`, `notification`, `producer`, `cleanup`, `require_past_date`.

`name`, `source_glob(s)` and `target_table` can only be set on a job.

### Where the data date comes from

File naming conventions differ from project to project, so `date_source` selects where the date is read from:

| `date_source` | Description | Example |
| --- | --- | --- |
| `filename` (default) | Match the regex against the file name | `creator_contents_2026-08-19.jsonl` |
| `path` | Match the regex against the full path — useful when the date is in a directory name | `out-20260708/out_00000.jsonl` |
| `mtime` | Use the file's modification time | The file name contains no date at all |

`date_pattern` must contain exactly one capture group, and `date_format` is the `strptime`
format for that group:

```yaml
date_source: path
date_pattern: 'out-(\d{8})/'
date_format: "%Y%m%d"
```

### Producer and cleanup hooks

A job does not have to start from files that already exist. `producer` names an arbitrary
executable that generates them, which is how a MySQL table gets into BigQuery: the script exports
rows to JSONL, and an optional `cleanup` script deletes those rows once the file has been accepted.
Adding another data source means writing another script — no change to this package.

```yaml
jobs:
  - name: orders_drain
    target_table: warehouse.orders
    producer:
      command: ["python3", "scripts/mysql_drain.py", "--table", "orders"]
      env:
        MYSQL_DSN: ${ORDERS_MYSQL_DSN:-}
      timeout_seconds: 1800
      manifest: true          # take the file list from the manifest
      on_error: fail          # fail | skip
      run_on_dry_run: false   # --dry-run does not touch the source by default
    cleanup:
      command: ["python3", "scripts/mysql_drain.py", "--delete"]
    archive_dir: /data1/archive/
```

`command` accepts a list of arguments or a single string (split with `shlex`); set `shell: true`
to hand the string to `/bin/sh` instead. `cwd` defaults to the job's `root`. A non-zero exit code
or a timeout fails the hook — `on_error: skip` downgrades a producer failure to a skipped job.
An empty `cleanup.command` means nothing is deleted.

#### What the producer receives

| Variable | Meaning |
| --- | --- |
| `BQ_SYNC_MANIFEST` | Path the script should write its manifest to |
| `BQ_SYNC_PROJECT` / `BQ_SYNC_JOB` | Project and job name |
| `BQ_SYNC_TARGET_TABLE` | The job's configured target table |
| `BQ_SYNC_ROOT` | The job's `root`; relative manifest paths resolve against it |
| `BQ_SYNC_TIMEZONE` | The job's timezone |
| `BQ_SYNC_BOUNDARY_DATE` | The date being treated as "today" |
| `BQ_SYNC_DRY_RUN` | `1` when the run is a dry run |
| `BQ_SYNC_STATE_DSN` | DSN of the kit's own state database |
| `BQ_SYNC_STATE_TABLE` | Name of the state table inside it |

The last two matter to export-only producers, which have to read the state table
themselves — see below. Do not assume the state database is the same one the
export reads from; it usually is not.

#### The manifest

```json
{"files": [
  {"path": "export/orders_2026-08-23.jsonl",
   "segment_date": "2026-08-23",
   "rows": 12043,
   "cleanup_token": "id:1..90210"}
]}
```

Only `path` is required, and a bare array of paths is accepted too. `segment_date` falls back to
the usual `date_source` extraction when omitted. `target_table` overrides the job's target for that
one file. `cleanup_token` is an opaque string handed back to the cleanup script — typically the id
range or watermark the export covered. A missing or empty manifest means "nothing to do this
round", which is not an error.

`rows` is worth declaring: the row count is verified against the file before anything else happens,
and a mismatch fails the file **without running cleanup**. A truncated export is the one failure
that would silently lose data, and this is what catches it. The count comes from the same pass that
computes the sha256, so it costs no extra I/O.

Hooks run in their own process group and the whole group is killed on timeout, so
a shell hook cannot leave children behind still writing to the source. A hook that
cannot be started at all — missing file, not executable — is reported the same way
as one that exits non-zero, and so honours `on_error`.

Set `manifest: false` if the script would rather just drop files where `source_glob` can find them.
In that mode the normal scan applies, including the "data date earlier than today" filter — set
`require_past_date: false` when the export covers today's rows.

#### What the cleanup receives

`BQ_SYNC_CLEANUP_TOKEN`, `BQ_SYNC_FILE`, `BQ_SYNC_SEGMENT_DATE`, `BQ_SYNC_ROWS`,
`BQ_SYNC_SHA256`, `BQ_SYNC_TARGET_TABLE`, plus `BQ_SYNC_PROJECT` / `BQ_SYNC_JOB` / `BQ_SYNC_ROOT`.

Cleanup runs after the file has passed its checks and MySQL holds a durable "this file must be
loaded" record — not after the BigQuery load. **Cleanup scripts must be safe to run twice**: if the
process dies mid-way, the next run retries with the same token. `DELETE ... WHERE id BETWEEN a AND b`
is naturally idempotent; `DELETE ... LIMIT n` is not. A cleanup that exits 0
without deleting anything is recorded as done, so validate your own arguments —
a batch size of zero turns the whole contract into a silent no-op.

Because cleanup runs before the load, a job that configures it may not also relax
the load: `max_bad_records > 0` and `ignore_unknown_values: true` are rejected at
config time. Both let BigQuery report success while dropping data that no longer
exists at the source.

#### Export-only sources

A source that must keep its rows — a crawler's checkpoint table, say — gets a producer and no
cleanup. Nothing on the source side then records what has already been exported, so re-scanning the
same window every round would append the same rows again. Let the state table answer instead: connect
with `BQ_SYNC_STATE_DSN` / `BQ_SYNC_STATE_TABLE` and query for the `segment_date`
values already registered under this `project_name` / `job_name`, then skip those
days. Treat a state table that is not there as a fatal error rather than an empty
skip set: the kit creates it before any producer runs, so its absence means the
script is looking at the wrong database, and carrying on would re-export every
historical day. Include every status, not just `success` —
`uploading` and `failed` files are still on disk and are retried from the state table by the kit
itself, so exporting them a second time only creates duplicates.

Such a job must still stop at the day boundary (`updated_at < today`). Exporting a day that is
still being written registers it in the state table, and every row that lands in that day afterwards
is then skipped forever.

`scripts/douyin_creator_checkpoint_export.py` implements this pattern;
`scripts/user_live_visits_drain.py` is the drain counterpart, where deletion is what bounds the
next round.

### Archiving

An empty `archive_dir` means source files are kept after a successful upload. `archive_layout: date`
creates subdirectories under the archive directory by data date. If the archive directory falls
within the scan scope it is excluded automatically, and files with the same name will not overwrite
one another (a content-digest prefix is prepended). A failed archive affects only that one file —
its status remains `success`, and the move is retried on the next run.

### Failure notifications

`notification` supports three parameter formats — `json`, `form` and `query` — and the
`{title}` and `{content}` placeholders can be used inside `params` and `headers`:

```yaml
notification:
  enabled: true
  url: https://example.com/push
  parameter_format: json
  params:
    title: "{title}"
    content: "{content}"
```

## Consistency design

- **State table**: `(project_name, job_name, file_key)` is unique, and `file_key` is the sha256 of
  the file's absolute path, so identically named jobs in different projects never interfere.
- **No duplicate uploads**: files whose status is `success` are skipped outright.
- **Crash recovery**: each attempt derives a deterministic BigQuery job ID from
  `(project, job, target_table, file sha256, attempt)`. If the process dies inside the window where
  BigQuery has accepted the load but MySQL has not yet been updated, the next run reuses the same
  job ID and BigQuery rejects the duplicate, so no data is written twice.
- **Files still being written**: size and mtime are compared before and after computing the sha256;
  if the file is still being written, it is skipped and counted as a failure.
- **Mutual exclusion**: a MySQL named lock is acquired per project name, so only one sync process
  may run for a given project at a time, while different projects can run in parallel.
- **Failure isolation**: an error on one file does not affect other files in the same job, nor any
  other project.
- **Producers run under the lock**: the producer executes inside the per-project named lock, so two
  hosts can never export the same rows twice.
- **Cleanup blocks the next export**: if a cleanup fails, its rows are still in the source *and*
  already on their way to BigQuery. The next run retries the cleanup first and refuses to run the
  producer until it succeeds, which is what keeps duplicates out of BigQuery. The pending token
  lives in the state table, so this survives a crash.
- **Nothing is dropped on the floor**: a manifest only describes the current round, so files from
  earlier rounds that have not reached BigQuery yet are pulled back out of the state table and
  retried alongside it. A file whose cleanup already succeeded is never cleaned twice.

Exit codes: `0` when everything succeeds, `1` when any file fails to sync, archive or clean up,
`2` on a configuration error.

## License

[MIT](LICENSE) © CNYoki
