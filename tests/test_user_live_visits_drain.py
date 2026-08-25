# -*- coding: utf-8 -*-
"""user_live_visits drain 脚本的测试。

脚本要连 MySQL，这里用一个假连接顶替：它按预先排好的队列回应 execute，
并把收到的 SQL 和参数记下来供断言。重点盯两件事——导出的分页边界，
以及 DELETE 的作用范围（这条错了会丢真实数据）。
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

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "user_live_visits_drain.py"
_spec = importlib.util.spec_from_file_location("user_live_visits_drain", _SCRIPT)
assert _spec and _spec.loader
drain = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(drain)


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
        self.commits = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    async def commit(self) -> None:
        self.commits += 1

    async def ensure_closed(self) -> None:
        self.closed = True


def make_row(uid: int, web_rid: str, day: date) -> tuple:
    return (
        uid,
        web_rid,
        day,
        "sec_uid_x",
        "昵称",
        12,
        34,
        datetime(2026, 8, 23, 10, 0, 0),
        datetime(2026, 8, 23, 22, 30, 15),
        datetime(2026, 8, 23, 22, 30, 15),
    )


# ---- 行编码 ----


def test_date_day_is_encoded_as_plain_date():
    record = json.loads(drain.encode_row(make_row(1, "rid", date(2026, 8, 24))))
    assert record["date_day"] == "2026-08-24"
    assert record["uid"] == 1
    assert record["first_seen_at"] == "2026-08-23 10:00:00"


def test_null_columns_stay_null():
    row = list(make_row(1, "rid", date(2026, 8, 24)))
    row[3] = None  # sec_uid
    row[6] = None  # follower_count
    record = json.loads(drain.encode_row(tuple(row)))
    assert record["sec_uid"] is None
    assert record["follower_count"] is None


# ---- cleanup token ----


def test_token_round_trips():
    assert drain.parse_token(drain.build_token(date(2026, 8, 24))) == date(
        2026, 8, 24
    )


@pytest.mark.parametrize(
    "token",
    [
        "",
        "   ",
        "not json",
        json.dumps({"date_day": "2026-08-24"}),  # 缺 table
        json.dumps({"table": "other_table", "date_day": "2026-08-24"}),
        json.dumps({"table": "user_live_visits"}),  # 缺日期
        json.dumps({"table": "user_live_visits", "date_day": "24/08/2026"}),
    ],
)
def test_bad_tokens_are_rejected(token):
    """token 可疑就必须拒绝——放过去等于对整张表执行 DELETE。"""
    with pytest.raises(SystemExit):
        drain.parse_token(token)


# ---- 导出 ----


def test_export_pages_with_a_keyset_and_renames_atomically(tmp_path: Path):
    day = date(2026, 8, 23)
    first = [make_row(1, "a", day), make_row(2, "b", day)]
    second = [make_row(3, "c", day)]
    connection = FakeConnection([first, second])

    path, rows = asyncio.run(
        drain.export_one_date(
            connection, day=day, batch_size=2, out_dir=tmp_path
        )
    )

    assert rows == 3
    assert path == tmp_path / "user_live_visits_2026-08-23.jsonl"
    assert [json.loads(line)["uid"] for line in path.read_text().splitlines()] == [
        1,
        2,
        3,
    ]
    # 不留临时文件，说明 rename 走完了。
    assert list(tmp_path.iterdir()) == [path]

    # 第一页不带游标，第二页从上一页最后一行的 (uid, web_rid) 接着取。
    assert connection.queries[0][1] == (day, 2)
    assert "(`uid`, `web_rid`) > (%s, %s)" in connection.queries[1][0]
    assert connection.queries[1][1] == (day, 2, "b", 2)


def test_export_stops_on_a_short_page(tmp_path: Path):
    day = date(2026, 8, 23)
    connection = FakeConnection([[make_row(1, "a", day)]])

    _, rows = asyncio.run(
        drain.export_one_date(
            connection, day=day, batch_size=5, out_dir=tmp_path
        )
    )

    assert rows == 1
    assert len(connection.queries) == 1  # 不多问一次空页


def test_export_writes_a_manifest_entry(tmp_path: Path, monkeypatch):
    day = date(2026, 8, 23)
    connection = FakeConnection([[(day,)], [make_row(1, "a", day)]])
    monkeypatch.setattr(drain, "connect", _fake_connect(connection))
    manifest = tmp_path / "manifest.json"

    args = argparse.Namespace(
        dsn="mysql://u:p@127.0.0.1:3306/db",
        boundary_date=date(2026, 8, 24),
        out_dir=str(tmp_path / "out"),
        manifest=str(manifest),
        max_dates=0,
        batch_size=1000,
    )
    assert asyncio.run(drain.command_export(args)) == 0

    entries = json.loads(manifest.read_text())["files"]
    assert len(entries) == 1
    assert entries[0]["segment_date"] == "2026-08-23"
    assert entries[0]["rows"] == 1
    assert drain.parse_token(entries[0]["cleanup_token"]) == day
    assert connection.closed


def test_export_skips_an_empty_day(tmp_path: Path, monkeypatch):
    day = date(2026, 8, 23)
    connection = FakeConnection([[(day,)], []])
    monkeypatch.setattr(drain, "connect", _fake_connect(connection))
    manifest = tmp_path / "manifest.json"

    args = argparse.Namespace(
        dsn="mysql://u:p@127.0.0.1:3306/db",
        boundary_date=date(2026, 8, 24),
        out_dir=str(tmp_path / "out"),
        manifest=str(manifest),
        max_dates=0,
        batch_size=1000,
    )
    asyncio.run(drain.command_export(args))

    # 没有数据就不该出现在 manifest 里，也不该留下一个空文件。
    assert json.loads(manifest.read_text())["files"] == []
    assert not (tmp_path / "out" / "user_live_visits_2026-08-23.jsonl").exists()


def _fake_connect(connection: FakeConnection):
    async def _connect(dsn: str, *, autocommit: bool):
        return connection

    return _connect


# ---- 删除 ----


def test_cleanup_deletes_only_the_token_date(monkeypatch):
    connection = FakeConnection([2, 2, 1])
    monkeypatch.setattr(drain, "connect", _fake_connect(connection))

    args = argparse.Namespace(
        dsn="mysql://u:p@127.0.0.1:3306/db",
        token=drain.build_token(date(2026, 8, 23)),
        batch_size=2,
    )
    assert asyncio.run(drain.command_cleanup(args)) == 0

    assert len(connection.queries) == 3  # 满页继续，短页收手
    for sql, params in connection.queries:
        assert sql == (
            "DELETE FROM `user_live_visits` WHERE date_day = %s LIMIT %s"
        )
        assert params == (date(2026, 8, 23), 2)
    assert connection.commits == 3
    assert connection.closed


def test_cleanup_on_an_already_empty_day_is_a_no_op(monkeypatch):
    """崩溃后拿同一个 token 重跑：删到 0 行，不报错。"""
    connection = FakeConnection([0])
    monkeypatch.setattr(drain, "connect", _fake_connect(connection))

    args = argparse.Namespace(
        dsn="mysql://u:p@127.0.0.1:3306/db",
        token=drain.build_token(date(2026, 8, 23)),
        batch_size=500,
    )
    assert asyncio.run(drain.command_cleanup(args)) == 0
    assert len(connection.queries) == 1


@pytest.mark.parametrize("command", ["export", "cleanup"])
@pytest.mark.parametrize("size", ["0", "-1"])
def test_a_non_positive_batch_size_is_refused(command, size, monkeypatch):
    """cleanup 里 LIMIT 0 会删到 0 行然后正常退出，kit 于是把 cleanup 记成 done。"""
    monkeypatch.setenv("LIVE_VISITS_MYSQL_DSN", "mysql://u:p@h:3306/db")
    monkeypatch.setenv("BQ_SYNC_CLEANUP_TOKEN", drain.build_token(date(2026, 8, 24)))
    with pytest.raises(SystemExit):
        drain.main([command, "--batch-size", size])
