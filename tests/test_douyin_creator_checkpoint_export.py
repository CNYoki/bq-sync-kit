# -*- coding: utf-8 -*-
"""douyin_creator_checkpoint 导出脚本的测试。

和 user_live_visits 那份一样用假连接顶替 MySQL。这个 job 不删源端数据，所以重点从
"DELETE 的作用范围"换成了"哪些天会被导出"——状态表挡不住已导过的日期，就会在
BigQuery 里堆出重复行。另外盯住时区偏移：DATETIME 不带时区，漏补偏移量整批数据会
平移 8 小时。
"""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
from datetime import date, datetime
import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "douyin_creator_checkpoint_export.py"
)
_spec = importlib.util.spec_from_file_location(
    "douyin_creator_checkpoint_export", _SCRIPT
)
assert _spec and _spec.loader
export = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export)


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self._connection = connection
        self._rows: list[tuple] = []

    async def execute(self, sql: str, args: tuple | None = None) -> int:
        self._connection.queries.append((" ".join(sql.split()), args))
        response = self._connection.responses.popleft()
        if isinstance(response, int):
            return response
        self._rows = list(response)
        return len(self._rows)

    async def fetchall(self) -> list[tuple]:
        return self._rows

    async def __aenter__(self) -> "FakeCursor":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        return None


class FakeConnection:
    def __init__(self, responses: list) -> None:
        self.responses = deque(responses)
        self.queries: list[tuple[str, tuple | None]] = []
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    async def ensure_closed(self) -> None:
        self.closed = True


CONVERTERS = export.build_converters("Asia/Shanghai")


def make_row(row_id: int, biz_key: str, updated: datetime) -> tuple:
    """一行 SELECT 结果：id 在最前，后面按 COLUMNS 的顺序。"""
    return (
        row_id,
        biz_key,
        3,
        42,
        '["主播用户-20260804"]',
        datetime(2026, 8, 25, 16, 8, 54),
        updated,
    )


def decode(row: tuple) -> dict:
    return json.loads(export.encode_row(row[1:], CONVERTERS))


def test_biz_key_is_renamed_to_sec_uid():
    record = decode(make_row(1, "MS4wLjABAAAA", datetime(2026, 8, 25, 16, 49, 42)))
    assert record["sec_uid"] == "MS4wLjABAAAA"
    assert "biz_key" not in record
    assert set(record) == {
        "sec_uid",
        "page_count",
        "aweme_count",
        "user_tags",
        "created_at",
        "updated_at",
    }


def test_datetimes_carry_the_source_offset():
    """列里是北京墙上时间，不补 +08:00 会被 BigQuery 当成 UTC。"""
    record = decode(make_row(1, "k", datetime(2026, 8, 25, 16, 49, 42)))
    assert record["updated_at"] == "2026-08-25 16:49:42+08:00"
    assert record["created_at"] == "2026-08-25 16:08:54+08:00"


def test_utc_source_timezone_gives_a_zero_offset():
    convert = export.make_timestamp_converter("UTC")
    assert convert(datetime(2026, 8, 25, 16, 49, 42)) == "2026-08-25 16:49:42+00:00"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, []),
        ("", []),
        ('["a", "b"]', ["a", "b"]),
        (b'["\xe4\xb8\xad"]', ["中"]),
        (["a"], ["a"]),
        ("[]", []),
    ],
)
def test_user_tags_become_a_json_array(raw, expected):
    assert export._as_tags(raw) == expected


@pytest.mark.parametrize("raw", ["{", '{"a": 1}', '"a"', "3"])
def test_malformed_user_tags_are_rejected(raw):
    # REPEATED 字段吃不下这些形状；悄悄写成 [] 会把标签整列吞掉。
    with pytest.raises(ValueError):
        export._as_tags(raw)


def test_null_counters_stay_null():
    row = (1, "k", None, None, None, None, None)
    record = decode(row)
    assert record["page_count"] is None
    assert record["aweme_count"] is None
    assert record["user_tags"] == []
    assert record["updated_at"] is None


def test_candidate_dates_are_filtered_by_name_done_and_boundary():
    connection = FakeConnection([[(date(2026, 8, 23),), (date(2026, 8, 24),)]])
    dates = asyncio.run(export.fetch_dates(connection, boundary=date(2026, 8, 25)))

    assert dates == [date(2026, 8, 23), date(2026, 8, 24)]
    sql, args = connection.queries[0]
    assert "`checkpoint_name` = %s" in sql
    assert "`done` = 1" in sql
    # 严格小于边界日期：当天还在写的那一天不碰。
    assert "`updated_at` < %s" in sql
    assert args == ("douyin_creator", date(2026, 8, 25))


def test_export_pages_with_an_id_keyset_and_renames_atomically(tmp_path: Path):
    first = [make_row(index, f"k{index}", datetime(2026, 8, 24, 10, 0)) for index in (1, 2)]
    second = [make_row(3, "k3", datetime(2026, 8, 24, 11, 0))]
    connection = FakeConnection([first, second])

    path, rows = asyncio.run(
        export.export_one_date(
            connection,
            day=date(2026, 8, 24),
            batch_size=2,
            out_dir=tmp_path,
            converters=CONVERTERS,
        )
    )

    assert rows == 3
    assert path.name == "douyin_creator_checkpoint_2026-08-24.jsonl"
    assert not path.with_name(path.name + ".tmp").exists()
    written = [json.loads(line) for line in path.read_text("utf-8").splitlines()]
    assert [item["sec_uid"] for item in written] == ["k1", "k2", "k3"]

    # 第一页不带游标，第二页从上一页最后一个 id 往后接。
    first_sql, first_args = connection.queries[0]
    second_sql, second_args = connection.queries[1]
    assert "`id` > %s" not in first_sql
    assert "`id` > %s" in second_sql
    assert second_args[3] == 2
    # 半开区间切天，边界那一秒不会同时落进两天。
    assert first_args[1:3] == (
        datetime(2026, 8, 24, 0, 0),
        datetime(2026, 8, 25, 0, 0),
    )


def test_export_stops_on_a_short_page(tmp_path: Path):
    connection = FakeConnection([[make_row(1, "k1", datetime(2026, 8, 24, 10, 0))]])

    _, rows = asyncio.run(
        export.export_one_date(
            connection,
            day=date(2026, 8, 24),
            batch_size=5,
            out_dir=tmp_path,
            converters=CONVERTERS,
        )
    )

    assert rows == 1
    assert len(connection.queries) == 1


def test_exported_dates_come_from_the_state_table():
    connection = FakeConnection([[(1,)], [(date(2026, 8, 23),), (datetime(2026, 8, 24, 0, 0),)]])

    dates = asyncio.run(
        export.fetch_exported_dates(
            connection,
            state_table="bq_file_sync_record",
            project_name="p",
            job_name="j",
        )
    )

    assert dates == {date(2026, 8, 23), date(2026, 8, 24)}
    sql, args = connection.queries[1]
    assert "project_name = %s" in sql and "job_name = %s" in sql
    assert args == ("p", "j")


def test_a_missing_state_table_means_nothing_was_exported():
    connection = FakeConnection([[(0,)]])

    dates = asyncio.run(
        export.fetch_exported_dates(
            connection,
            state_table="bq_file_sync_record",
            project_name="p",
            job_name="j",
        )
    )

    assert dates == set()
    assert len(connection.queries) == 1


@pytest.mark.parametrize("name", ["", "bq record", "a;DROP", "`x`"])
def test_bad_state_table_names_are_rejected(name):
    with pytest.raises(SystemExit):
        export._validate_identifier(name, what="--state-table")


def _export_args(tmp_path: Path, **overrides: Any) -> argparse.Namespace:
    values: dict[str, Any] = {
        "dsn": "mysql://u:p@h:3306/db",
        "state_dsn": "",
        "state_table": "bq_file_sync_record",
        "project": "p",
        "job": "j",
        "record_check": True,
        "boundary_date": date(2026, 8, 25),
        "source_timezone": "Asia/Shanghai",
        "out_dir": str(tmp_path / "out"),
        "manifest": str(tmp_path / "manifest.json"),
        "max_dates": 0,
        "batch_size": 100,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _patch_connections(monkeypatch, connections: list[FakeConnection]) -> None:
    queue = deque(connections)

    async def fake_connect(dsn: str):
        return queue.popleft()

    monkeypatch.setattr(export, "connect", fake_connect)


def test_dates_already_in_the_record_are_not_exported_again(
    tmp_path: Path, monkeypatch
):
    """这个 job 不删源端，重复导出只能靠状态表挡。"""
    state = FakeConnection([[(1,)], [(date(2026, 8, 23),)]])
    source = FakeConnection(
        [
            [(date(2026, 8, 23),), (date(2026, 8, 24),)],
            [make_row(7, "k7", datetime(2026, 8, 24, 9, 0))],
        ]
    )
    _patch_connections(monkeypatch, [state, source])

    assert asyncio.run(export.command_export(_export_args(tmp_path))) == 0

    manifest = json.loads((tmp_path / "manifest.json").read_text("utf-8"))
    assert [entry["segment_date"] for entry in manifest["files"]] == ["2026-08-24"]
    assert manifest["files"][0]["rows"] == 1
    # 只对 08-24 发过取数请求，08-23 连查都没查。
    assert len(source.queries) == 2
    assert source.queries[1][1][1] == datetime(2026, 8, 24, 0, 0)
    assert state.closed and source.closed


def test_no_record_check_exports_every_candidate_date(tmp_path: Path, monkeypatch):
    source = FakeConnection(
        [
            [(date(2026, 8, 23),), (date(2026, 8, 24),)],
            [make_row(1, "k1", datetime(2026, 8, 23, 9, 0))],
            [make_row(2, "k2", datetime(2026, 8, 24, 9, 0))],
        ]
    )
    _patch_connections(monkeypatch, [source])

    args = _export_args(tmp_path, record_check=False)
    assert asyncio.run(export.command_export(args)) == 0

    manifest = json.loads((tmp_path / "manifest.json").read_text("utf-8"))
    assert [entry["segment_date"] for entry in manifest["files"]] == [
        "2026-08-23",
        "2026-08-24",
    ]


def test_max_dates_limits_one_round(tmp_path: Path, monkeypatch):
    state = FakeConnection([[(1,)], []])
    source = FakeConnection(
        [
            [(date(2026, 8, 23),), (date(2026, 8, 24),)],
            [make_row(1, "k1", datetime(2026, 8, 23, 9, 0))],
        ]
    )
    _patch_connections(monkeypatch, [state, source])

    args = _export_args(tmp_path, max_dates=1)
    assert asyncio.run(export.command_export(args)) == 0

    manifest = json.loads((tmp_path / "manifest.json").read_text("utf-8"))
    assert [entry["segment_date"] for entry in manifest["files"]] == ["2026-08-23"]


def test_an_empty_day_is_left_out_of_the_manifest(tmp_path: Path, monkeypatch):
    """空文件登记进 manifest，这一天就会被状态表永久挡住。"""
    state = FakeConnection([[(1,)], []])
    source = FakeConnection([[(date(2026, 8, 24),)], []])
    _patch_connections(monkeypatch, [state, source])

    assert asyncio.run(export.command_export(_export_args(tmp_path))) == 0

    manifest = json.loads((tmp_path / "manifest.json").read_text("utf-8"))
    assert manifest["files"] == []
    assert not (
        tmp_path / "out" / "douyin_creator_checkpoint_2026-08-24.jsonl"
    ).exists()


def test_record_check_without_project_and_job_is_refused(monkeypatch):
    # 静默跳过检查会让同一天每轮重导一遍。
    monkeypatch.setenv("CREATOR_CHECKPOINT_MYSQL_DSN", "mysql://u:p@h:3306/db")
    monkeypatch.delenv("BQ_SYNC_PROJECT", raising=False)
    monkeypatch.delenv("BQ_SYNC_JOB", raising=False)
    with pytest.raises(SystemExit):
        export.main(["export"])


def test_out_dir_is_resolved_against_the_kit_root(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("BQ_SYNC_ROOT", str(tmp_path))
    assert export._resolve_out_dir("data/jsonl") == tmp_path / "data" / "jsonl"
    assert export._resolve_out_dir("/abs/dir") == Path("/abs/dir")
