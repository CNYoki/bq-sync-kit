# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from bq_sync_kit.bigquery_loader import LoadResult
from bq_sync_kit.config import build_settings
from bq_sync_kit.notifier import FailureNotifier
from bq_sync_kit.repository import SyncRepository
from bq_sync_kit.sync import SyncRunner


class FakeLoader:
    """记录调用并可按文件名注入失败的 BigQueryJsonlLoader 替身。"""

    def __init__(self, fail_for: set[str] | None = None):
        self.calls: list[dict] = []
        self.fail_for = fail_for or set()
        self.closed = False

    def load(self, *, job_id, path: Path, target_table, settings) -> LoadResult:
        self.calls.append(
            {
                "job_id": job_id,
                "path": path,
                "target_table": target_table,
                "location": settings.location,
            }
        )
        if path.name in self.fail_for:
            raise RuntimeError("boom")
        return LoadResult(job_id=job_id, output_rows=3)

    def close(self) -> None:
        self.closed = True


class RecordingNotifier(FailureNotifier):
    def __init__(self):
        self.messages: list[tuple[str, str]] = []

    async def send(self, *, title: str, content: str) -> bool:
        self.messages.append((title, content))
        return True


def write(path: Path, content: str = '{"a":1}\n') -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def make_runner(document, loader, notifier=None):
    settings = build_settings(document)
    return (
        SyncRunner(
            settings,
            repository=SyncRepository(settings.mysql),
            loader=loader,
            notifier_factory=lambda job: notifier or RecordingNotifier(),
        ),
        settings,
    )


async def test_successful_run_uploads_archives_and_records(
    tmp_path, base_document
):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")
    write(tmp_path / "data" / "events_2026-08-19.jsonl")
    base_document["projects"][0]["jobs"][0]["archive_dir"] = "archive"
    loader = FakeLoader()
    runner, settings = make_runner(base_document, loader)

    summary = await runner.run(boundary_date=date(2026, 8, 20))

    assert (summary.discovered, summary.succeeded, summary.archived) == (2, 2, 2)
    assert summary.failed == 0 and summary.ok
    assert len(loader.calls) == 2
    assert loader.closed
    # 源文件已搬走，归档目录里各留一份。
    assert not (tmp_path / "data" / "events_2026-08-18.jsonl").exists()
    assert sorted(p.name for p in (tmp_path / "archive").iterdir()) == [
        "events_2026-08-18.jsonl",
        "events_2026-08-19.jsonl",
    ]

    repository = SyncRepository(settings.mysql)
    async with repository:
        records = await repository.recent_records()
    assert {record["status"] for record in records} == {"success"}
    assert {record["loaded_rows"] for record in records} == {3}
    assert all(record["archived_path"] for record in records)


async def test_already_synced_file_is_skipped_on_second_run(
    tmp_path, base_document
):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")
    loader = FakeLoader()
    runner, settings = make_runner(base_document, loader)

    first = await runner.run(boundary_date=date(2026, 8, 20))
    assert first.succeeded == 1

    runner_again, _ = make_runner(base_document, loader)
    second = await runner_again.run(boundary_date=date(2026, 8, 20))

    assert second.skipped == 1
    assert second.succeeded == 0
    assert len(loader.calls) == 1  # 没有重复上传


async def test_failed_upload_is_recorded_and_notified(tmp_path, base_document):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")
    write(tmp_path / "data" / "events_2026-08-19.jsonl")
    base_document["projects"][0]["jobs"][0]["archive_dir"] = "archive"
    notifier = RecordingNotifier()
    loader = FakeLoader(fail_for={"events_2026-08-18.jsonl"})
    runner, settings = make_runner(base_document, loader, notifier)

    summary = await runner.run(boundary_date=date(2026, 8, 20))

    assert (summary.succeeded, summary.failed, summary.archived) == (1, 1, 1)
    assert not summary.ok
    assert len(notifier.messages) == 1
    assert "events_2026-08-18.jsonl" in notifier.messages[0][1]
    # 失败的文件留在原地等下次重试。
    assert (tmp_path / "data" / "events_2026-08-18.jsonl").exists()

    repository = SyncRepository(settings.mysql)
    async with repository:
        failed = await repository.recent_records(statuses=["failed"])
    assert len(failed) == 1
    assert "boom" in failed[0]["error_message"]
    assert failed[0]["attempt_count"] == 1


async def test_retry_after_failure_increments_attempt_and_new_job_id(
    tmp_path, base_document
):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")
    failing = FakeLoader(fail_for={"events_2026-08-18.jsonl"})
    runner, settings = make_runner(base_document, failing)
    await runner.run(boundary_date=date(2026, 8, 20))

    healthy = FakeLoader()
    runner_again, _ = make_runner(base_document, healthy)
    summary = await runner_again.run(boundary_date=date(2026, 8, 20))

    assert summary.succeeded == 1
    assert healthy.calls[0]["job_id"] != failing.calls[0]["job_id"]
    repository = SyncRepository(settings.mysql)
    async with repository:
        records = await repository.recent_records()
    assert records[0]["attempt_count"] == 2
    assert records[0]["status"] == "success"


async def test_dry_run_touches_neither_mysql_nor_bigquery(
    tmp_path, base_document
):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")
    base_document["mysql"]["dsn"] = "mysql+asyncmy://invalid:1/none"
    loader = FakeLoader()
    runner, _ = make_runner(base_document, loader)

    summary = await runner.run(boundary_date=date(2026, 8, 20), dry_run=True)

    assert summary.discovered == 1
    assert summary.succeeded == 0
    assert loader.calls == []
    assert (tmp_path / "data" / "events_2026-08-18.jsonl").exists()


async def test_jobs_across_projects_use_their_own_bigquery_settings(
    tmp_path, base_document
):
    write(tmp_path / "alpha" / "events_2026-08-18.jsonl")
    write(tmp_path / "beta" / "events_2026-08-18.jsonl")
    base_document["projects"] = [
        {
            "name": "alpha",
            "root": str(tmp_path / "alpha"),
            "_base_dir": str(tmp_path),
            "location": "US",
            "jobs": [
                {
                    "name": "events",
                    "source_glob": "*.jsonl",
                    "target_table": "alpha.raw_events",
                }
            ],
        },
        {
            "name": "beta",
            "root": str(tmp_path / "beta"),
            "_base_dir": str(tmp_path),
            "location": "asia-east2",
            "jobs": [
                {
                    "name": "events",
                    "source_glob": "*.jsonl",
                    "target_table": "beta.raw_events",
                }
            ],
        },
    ]
    loader = FakeLoader()
    runner, settings = make_runner(base_document, loader)

    summary = await runner.run(boundary_date=date(2026, 8, 20))

    assert summary.succeeded == 2
    assert {(call["target_table"], call["location"]) for call in loader.calls} == {
        ("alpha.raw_events", "US"),
        ("beta.raw_events", "asia-east2"),
    }
    assert [job.qualified_name for job in summary.jobs] == [
        "alpha/events",
        "beta/events",
    ]

    repository = SyncRepository(settings.mysql)
    async with repository:
        records = await repository.recent_records()
    # 同名 job 分属不同项目，状态互不覆盖。
    assert {record["project_name"] for record in records} == {"alpha", "beta"}


async def test_only_selected_project_runs(tmp_path, base_document):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")
    loader = FakeLoader()
    runner, _ = make_runner(base_document, loader)

    summary = await runner.run(
        projects=["demo"], jobs=["demo/events"], boundary_date=date(2026, 8, 20)
    )
    assert summary.succeeded == 1

    with pytest.raises(Exception, match="未知项目"):
        runner_two, _ = make_runner(base_document, FakeLoader())
        await runner_two.run(projects=["ghost"])
