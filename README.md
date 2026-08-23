# bq_sync_kit

A cross-project JSONL → BigQuery file sync tool.

Every job follows the same three steps:

1. Scan the configured directories for JSONL files whose **data date is earlier than the current date** (today's in-progress segments are left alone);
2. Upload them to the corresponding BigQuery warehouse using each project's own configuration (`WRITE_APPEND`, equivalent to `bq load --noreplace --source_format=NEWLINE_DELIMITED_JSON`);
3. Optionally, `mv` successfully synced files into an archive directory.

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
├── sync.py             per-file orchestration: lock → upload → record → archive
├── bigquery_loader.py  chunked upload and load-job polling
├── repository.py       state-table reads and writes
├── db.py               engine and named locks
├── models.py           SQLAlchemy models
└── notifier.py         failure notifications
scripts/
├── bq_sync.sh          cron wrapper: flock, .env loading, logging, log rotation
└── crontab.example     sample schedule
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
`job_timeout_seconds`, `notification`.

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

Exit codes: `0` when everything succeeds, `1` when any file fails to sync or archive, `2` on a
configuration error.

## License

[MIT](LICENSE) © CNYoki
