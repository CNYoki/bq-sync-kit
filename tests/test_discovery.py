# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date
import os
import time

import pytest

from bq_sync_kit.config import build_settings
from bq_sync_kit.discovery import discover_files, move_to_archive


def build_job(base_document, **job_overrides):
    base_document["projects"][0]["jobs"][0].update(job_overrides)
    return build_settings(base_document).projects[0].jobs[0]


def write(path, content="{}\n"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_only_files_before_today_are_picked(tmp_path, base_document):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")
    write(tmp_path / "data" / "events_2026-08-19.jsonl")
    write(tmp_path / "data" / "events_2026-08-20.jsonl")

    job = build_job(base_document, source_glob="data/events_*.jsonl")
    found = discover_files(job, today=date(2026, 8, 20))

    assert [item.path.name for item in found] == [
        "events_2026-08-18.jsonl",
        "events_2026-08-19.jsonl",
    ]
    assert found[0].segment_date == date(2026, 8, 18)


def test_undated_and_empty_files_are_skipped(tmp_path, base_document):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")
    write(tmp_path / "data" / "events_no_date.jsonl")
    write(tmp_path / "data" / "events_2026-08-17.jsonl", content="")

    job = build_job(base_document, source_glob="data/events_*.jsonl")
    found = discover_files(job, today=date(2026, 8, 20))

    assert [item.path.name for item in found] == ["events_2026-08-18.jsonl"]


def test_date_from_directory_name(tmp_path, base_document):
    write(tmp_path / "out-20260708" / "out_00000.jsonl")
    write(tmp_path / "out-20260721" / "out_00000.jsonl")

    job = build_job(
        base_document,
        source_glob="out-*/out_*.jsonl",
        date_source="path",
        date_pattern=r"out-(\d{8})/",
        date_format="%Y%m%d",
    )
    found = discover_files(job, today=date(2026, 8, 20))

    assert [item.segment_date for item in found] == [
        date(2026, 7, 8),
        date(2026, 7, 21),
    ]


def test_date_from_mtime(tmp_path, base_document):
    path = write(tmp_path / "data" / "anything.jsonl")
    old = time.time() - 3 * 86400
    os.utime(path, (old, old))

    job = build_job(
        base_document, source_glob="data/*.jsonl", date_source="mtime"
    )
    found = discover_files(job, today=date.today())

    assert len(found) == 1
    assert found[0].segment_date < date.today()


def test_archived_files_are_not_rediscovered(tmp_path, base_document):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")
    write(tmp_path / "data" / "archive" / "events_2026-08-01.jsonl")

    job = build_job(
        base_document,
        source_globs=["data/**/*.jsonl"],
        recursive=True,
        archive_dir="data/archive",
    )
    found = discover_files(job, today=date(2026, 8, 20))

    assert [item.path.name for item in found] == ["events_2026-08-18.jsonl"]


def test_overlapping_globs_are_deduplicated(tmp_path, base_document):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")

    job = build_job(
        base_document,
        source_globs=["data/*.jsonl", "data/events_*.jsonl"],
    )
    assert len(discover_files(job, today=date(2026, 8, 20))) == 1


def test_archive_dir_equal_to_source_dir_is_rejected(tmp_path, base_document):
    write(tmp_path / "data" / "events_2026-08-18.jsonl")
    job = build_job(base_document, source_glob="data/*.jsonl", archive_dir="data")

    with pytest.raises(ValueError, match="archive_dir"):
        discover_files(job, today=date(2026, 8, 20))


def test_move_to_archive_keeps_both_files_on_name_clash(tmp_path):
    source = write(tmp_path / "data" / "events_2026-08-18.jsonl", "a\n")
    archive = tmp_path / "archive"
    archive.mkdir()
    (archive / "events_2026-08-18.jsonl").write_text("b\n", encoding="utf-8")

    moved = move_to_archive(source, archive_dir=str(archive), root=tmp_path)

    assert moved.parent == archive.resolve()
    assert moved.name != "events_2026-08-18.jsonl"
    assert moved.read_text(encoding="utf-8") == "a\n"
    assert (archive / "events_2026-08-18.jsonl").read_text(
        encoding="utf-8"
    ) == "b\n"
    assert not source.exists()


def test_move_to_archive_with_date_layout(tmp_path):
    source = write(tmp_path / "data" / "events_2026-08-18.jsonl")

    moved = move_to_archive(
        source,
        archive_dir="archive",
        root=tmp_path,
        keep_date_subdir=True,
        segment_date=date(2026, 8, 18),
    )

    assert moved.parent == (tmp_path / "archive" / "2026-08-18").resolve()
