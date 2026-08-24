# -*- coding: utf-8 -*-
"""producer / cleanup 外挂脚本的端到端行为。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
import stat

import pytest

from bq_sync_kit.bigquery_loader import LoadResult
from bq_sync_kit.config import ConfigError, build_settings
from bq_sync_kit.hooks import HookError, parse_manifest
from bq_sync_kit.notifier import FailureNotifier
from bq_sync_kit.repository import SyncRepository
from bq_sync_kit.sync import SyncRunner

BOUNDARY = date(2026, 8, 24)


class FakeLoader:
    def __init__(self, fail_for: set[str] | None = None):
        self.calls: list[dict] = []
        self.fail_for = fail_for or set()

    def load(self, *, job_id, path: Path, target_table, settings) -> LoadResult:
        self.calls.append({"path": path, "target_table": target_table})
        if path.name in self.fail_for:
            raise RuntimeError("boom")
        return LoadResult(job_id=job_id, output_rows=2)

    def close(self) -> None:
        pass


class RecordingNotifier(FailureNotifier):
    def __init__(self):
        self.messages: list[tuple[str, str]] = []

    async def send(self, *, title: str, content: str) -> bool:
        self.messages.append((title, content))
        return True


def write_script(path: Path, body: str) -> Path:
    path.write_text("#!/bin/sh\n" + body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def make_runner(document, loader, notifier):
    settings = build_settings(document)
    return SyncRunner(
        settings,
        repository=SyncRepository(settings.mysql),
        loader=loader,
        notifier_factory=lambda job: notifier,
    )


@pytest.fixture
def producer_document(tmp_path: Path, base_document: dict) -> dict:
    """一个用 shell 脚本充当 MySQL 导出的 job。"""
    write_script(
        tmp_path / "producer.sh",
        f'''set -e
mkdir -p "$BQ_SYNC_ROOT/data"
printf '{{"id":1}}\\n{{"id":2}}\\n' > "$BQ_SYNC_ROOT/data/orders_2026-08-23.jsonl"
cat > "$BQ_SYNC_MANIFEST" <<'MANIFEST'
{{"files": [{{"path": "data/orders_2026-08-23.jsonl",
             "segment_date": "2026-08-23", "rows": 2,
             "cleanup_token": "id:1..2"}}]}}
MANIFEST
echo run >> "{tmp_path / 'producer.log'}"
''',
    )
    write_script(
        tmp_path / "cleanup.sh",
        f'echo "$BQ_SYNC_CLEANUP_TOKEN|$BQ_SYNC_ROWS|$BQ_SYNC_SEGMENT_DATE"'
        f' >> "{tmp_path / "cleanup.log"}"\n',
    )
    job = base_document["projects"][0]["jobs"][0]
    job["producer"] = {"command": ["sh", str(tmp_path / "producer.sh")]}
    job["cleanup"] = {"command": ["sh", str(tmp_path / "cleanup.sh")]}
    return base_document


async def test_producer_output_is_loaded_and_source_cleaned(
    tmp_path, producer_document
):
    loader = FakeLoader()
    notifier = RecordingNotifier()
    runner = make_runner(producer_document, loader, notifier)

    summary = await runner.run(boundary_date=BOUNDARY)

    assert (summary.discovered, summary.succeeded, summary.cleaned) == (1, 1, 1)
    assert summary.cleanup_failed == 0 and summary.ok
    assert [call["path"].name for call in loader.calls] == [
        "orders_2026-08-23.jsonl"
    ]
    # cleanup 拿到了 manifest 里的 token、行数和数据日期。
    assert (tmp_path / "cleanup.log").read_text().strip() == "id:1..2|2|2026-08-23"
    assert not notifier.messages

    async with SyncRepository(build_settings(producer_document).mysql) as repo:
        record = (await repo.recent_records(limit=1))[0]
    assert record["status"] == "success"
    assert record["cleanup_status"] == "done"
    assert record["cleanup_token"] == "id:1..2"
    assert record["expected_rows"] == 2


async def test_producer_runs_even_when_data_date_is_today(
    tmp_path, producer_document
):
    """manifest 声明了 segment_date，就不受“早于今天”的过滤影响。"""
    loader = FakeLoader()
    runner = make_runner(producer_document, loader, RecordingNotifier())

    summary = await runner.run(boundary_date=date(2026, 8, 23))

    assert summary.succeeded == 1


async def test_row_count_mismatch_fails_and_skips_cleanup(
    tmp_path, producer_document
):
    """导出被截断时必须拦住，绝不能让源端把行删掉。"""
    write_script(
        tmp_path / "producer.sh",
        f'''set -e
mkdir -p "$BQ_SYNC_ROOT/data"
printf '{{"id":1}}\\n' > "$BQ_SYNC_ROOT/data/orders_2026-08-23.jsonl"
cat > "$BQ_SYNC_MANIFEST" <<'MANIFEST'
{{"files": [{{"path": "data/orders_2026-08-23.jsonl",
             "segment_date": "2026-08-23", "rows": 9,
             "cleanup_token": "id:1..9"}}]}}
MANIFEST
''',
    )
    loader = FakeLoader()
    notifier = RecordingNotifier()
    runner = make_runner(producer_document, loader, notifier)

    summary = await runner.run(boundary_date=BOUNDARY)

    assert (summary.failed, summary.succeeded, summary.cleaned) == (1, 0, 0)
    assert not loader.calls
    assert not (tmp_path / "cleanup.log").exists()
    assert "声明 9 行，实际 1 行" in notifier.messages[0][1]


async def test_cleanup_failure_blocks_the_next_producer_run(
    tmp_path, producer_document
):
    """清理没成功就再导一遍，等于在 BigQuery 里造重复行——必须拦住。"""
    flag = tmp_path / "cleanup_ok"
    write_script(
        tmp_path / "cleanup.sh",
        f'''if [ -f "{flag}" ]; then
  echo "$BQ_SYNC_CLEANUP_TOKEN" >> "{tmp_path / 'cleanup.log'}"
  exit 0
fi
echo "源端还没准备好" >&2
exit 7
''',
    )
    document = producer_document
    notifier = RecordingNotifier()

    summary = await make_runner(document, FakeLoader(), notifier).run(
        boundary_date=BOUNDARY
    )
    # 数据照样进仓，但清理失败被记下来了。
    assert (summary.succeeded, summary.cleanup_failed) == (1, 1)
    assert not summary.ok
    assert any("清理失败" in title for title, _ in notifier.messages)

    producer_runs = (tmp_path / "producer.log").read_text().count("run")
    assert producer_runs == 1

    # 第二轮：清理仍然失败，producer 不许再跑。
    summary = await make_runner(document, FakeLoader(), notifier).run(
        boundary_date=BOUNDARY
    )
    assert summary.failed == 1
    assert (tmp_path / "producer.log").read_text().count("run") == 1
    assert any("待清理数据未处理完" in title for title, _ in notifier.messages)

    # 第三轮：清理成功了，producer 恢复运行。
    flag.write_text("ok", encoding="utf-8")
    summary = await make_runner(document, FakeLoader(), notifier).run(
        boundary_date=BOUNDARY
    )
    assert summary.cleaned >= 1
    assert (tmp_path / "producer.log").read_text().count("run") == 2
    assert (tmp_path / "cleanup.log").read_text().strip() == "id:1..2"


async def test_producer_failure_is_reported(tmp_path, producer_document):
    write_script(tmp_path / "producer.sh", 'echo "连不上库" >&2\nexit 3\n')
    notifier = RecordingNotifier()
    loader = FakeLoader()

    summary = await make_runner(producer_document, loader, notifier).run(
        boundary_date=BOUNDARY
    )

    assert summary.failed == 1 and not loader.calls
    title, content = notifier.messages[0]
    assert "producer 执行失败" in title
    assert "退出码 3" in content and "连不上库" in content


async def test_producer_failure_can_be_downgraded_to_skip(
    tmp_path, producer_document
):
    write_script(tmp_path / "producer.sh", "exit 3\n")
    producer_document["projects"][0]["jobs"][0]["producer"]["on_error"] = "skip"
    notifier = RecordingNotifier()

    summary = await make_runner(producer_document, FakeLoader(), notifier).run(
        boundary_date=BOUNDARY
    )

    assert summary.failed == 0 and summary.ok
    assert not notifier.messages


async def test_producer_without_manifest_falls_back_to_glob(
    tmp_path, producer_document
):
    write_script(
        tmp_path / "producer.sh",
        '''set -e
mkdir -p "$BQ_SYNC_ROOT/data"
printf '{"id":1}\\n' > "$BQ_SYNC_ROOT/data/events_2026-08-23.jsonl"
''',
    )
    job = producer_document["projects"][0]["jobs"][0]
    job["producer"]["manifest"] = False
    loader = FakeLoader()

    summary = await make_runner(producer_document, loader, RecordingNotifier()).run(
        boundary_date=BOUNDARY
    )

    assert summary.succeeded == 1
    assert loader.calls[0]["path"].name == "events_2026-08-23.jsonl"


async def test_glob_mode_can_opt_out_of_the_past_date_filter(
    tmp_path, producer_document
):
    job = producer_document["projects"][0]["jobs"][0]
    job["producer"]["manifest"] = False
    job["require_past_date"] = False
    write_script(
        tmp_path / "producer.sh",
        '''set -e
mkdir -p "$BQ_SYNC_ROOT/data"
printf '{"id":1}\\n' > "$BQ_SYNC_ROOT/data/events_2026-08-24.jsonl"
''',
    )
    loader = FakeLoader()

    summary = await make_runner(producer_document, loader, RecordingNotifier()).run(
        boundary_date=BOUNDARY
    )

    assert summary.succeeded == 1


async def test_dry_run_does_not_execute_the_producer(
    tmp_path, producer_document
):
    loader = FakeLoader()
    summary = await make_runner(producer_document, loader, RecordingNotifier()).run(
        boundary_date=BOUNDARY, dry_run=True
    )

    assert summary.discovered == 0 and not loader.calls
    assert not (tmp_path / "producer.log").exists()


async def test_manifest_can_override_the_target_table(
    tmp_path, producer_document
):
    write_script(
        tmp_path / "producer.sh",
        '''set -e
mkdir -p "$BQ_SYNC_ROOT/data"
printf '{"id":1}\\n' > "$BQ_SYNC_ROOT/data/orders_2026-08-23.jsonl"
cat > "$BQ_SYNC_MANIFEST" <<'MANIFEST'
[{"path": "data/orders_2026-08-23.jsonl", "target_table": "other.orders"}]
MANIFEST
''',
    )
    loader = FakeLoader()

    await make_runner(producer_document, loader, RecordingNotifier()).run(
        boundary_date=BOUNDARY
    )

    assert loader.calls[0]["target_table"] == "other.orders"


def test_manifest_rejects_a_missing_file(tmp_path, base_document):
    job = build_settings(base_document).projects[0].jobs[0]
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"files": ["data/nope.jsonl"]}', encoding="utf-8")

    with pytest.raises(HookError, match="文件不存在"):
        parse_manifest(manifest, job=job)


def test_manifest_absence_means_no_output(tmp_path, base_document):
    job = build_settings(base_document).projects[0].jobs[0]
    assert parse_manifest(tmp_path / "missing.json", job=job) == []


def test_hook_command_rejects_a_bad_type(base_document):
    base_document["projects"][0]["jobs"][0]["cleanup"] = {"command": 42}
    with pytest.raises(ConfigError, match="需要字符串或字符串数组"):
        build_settings(base_document)


async def test_failed_file_is_retried_without_cleaning_twice(
    tmp_path, producer_document
):
    """manifest 只描述本轮产出，上一轮失败的文件必须从状态表里捞回来重试。"""
    once = tmp_path / "produced_once"
    write_script(
        tmp_path / "producer.sh",
        f'''set -e
if [ -f "{once}" ]; then
  echo '{{"files": []}}' > "$BQ_SYNC_MANIFEST"
  exit 0
fi
mkdir -p "$BQ_SYNC_ROOT/data"
printf '{{"id":1}}\\n{{"id":2}}\\n' > "$BQ_SYNC_ROOT/data/orders_2026-08-23.jsonl"
cat > "$BQ_SYNC_MANIFEST" <<'MANIFEST'
{{"files": [{{"path": "data/orders_2026-08-23.jsonl",
             "segment_date": "2026-08-23", "rows": 2,
             "cleanup_token": "id:1..2"}}]}}
MANIFEST
touch "{once}"
''',
    )
    notifier = RecordingNotifier()

    # 第一轮：源端已清理，但 BigQuery 没收下。
    failing = FakeLoader(fail_for={"orders_2026-08-23.jsonl"})
    summary = await make_runner(producer_document, failing, notifier).run(
        boundary_date=BOUNDARY
    )
    assert (summary.failed, summary.cleaned) == (1, 1)
    assert (tmp_path / "cleanup.log").read_text().count("id:1..2") == 1

    # 第二轮：producer 没有新产出，失败的文件仍然要重传，且不能再删一次源端。
    loader = FakeLoader()
    summary = await make_runner(producer_document, loader, notifier).run(
        boundary_date=BOUNDARY
    )
    assert summary.succeeded == 1
    assert loader.calls[0]["path"].name == "orders_2026-08-23.jsonl"
    assert (tmp_path / "cleanup.log").read_text().count("id:1..2") == 1
