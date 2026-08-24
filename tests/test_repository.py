# -*- coding: utf-8 -*-
"""状态层：自动补列，以及建库时的连接参数。"""

from __future__ import annotations

from typing import Any

from sqlalchemy import inspect, text

import pytest

from bq_sync_kit import db
from bq_sync_kit.config import MySQLSettings
from bq_sync_kit.db import create_engine
from bq_sync_kit.repository import SyncRepository

# 本次新增之前的建表语句，用来模拟已经在跑的旧部署。
_LEGACY_TABLE = """
CREATE TABLE bq_file_sync_record (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    project_name VARCHAR(128) NOT NULL,
    job_name VARCHAR(128) NOT NULL,
    file_key VARCHAR(64) NOT NULL,
    file_path TEXT NOT NULL,
    segment_date DATE NOT NULL,
    file_size BIGINT NOT NULL,
    file_sha256 VARCHAR(64) NOT NULL,
    target_table VARCHAR(512) NOT NULL,
    status VARCHAR(20) NOT NULL,
    attempt_count INTEGER NOT NULL,
    bigquery_job_id VARCHAR(128),
    loaded_rows BIGINT,
    archived_path TEXT,
    error_message TEXT,
    started_at DATETIME,
    synced_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_NEW_COLUMNS = {
    "expected_rows",
    "cleanup_token",
    "cleanup_status",
    "cleanup_error",
}


async def test_initialize_adds_columns_to_a_legacy_table(
    sqlite_mysql_settings: dict[str, Any]
):
    settings = MySQLSettings.from_mapping(sqlite_mysql_settings)
    engine = create_engine(settings)
    async with engine.begin() as connection:
        await connection.execute(text(_LEGACY_TABLE))
        await connection.execute(
            text(
                "INSERT INTO bq_file_sync_record (project_name, job_name, "
                "file_key, file_path, segment_date, file_size, file_sha256, "
                "target_table, status, attempt_count) VALUES "
                "('demo', 'events', 'k', '/tmp/a.jsonl', '2026-08-01', 1, "
                "'sha', 'demo.raw', 'success', 1)"
            )
        )
    await engine.dispose()

    async with SyncRepository(settings) as repository:
        async with repository.engine.connect() as connection:
            columns = {
                column["name"]
                for column in await connection.run_sync(
                    lambda sync_connection: inspect(sync_connection).get_columns(
                        "bq_file_sync_record"
                    )
                )
            }
        # 旧数据还在，新列补上了。
        records = await repository.recent_records(limit=10)

    assert _NEW_COLUMNS <= columns
    assert len(records) == 1
    assert records[0]["cleanup_status"] is None


@pytest.mark.asyncio
async def test_create_database_connects_without_the_target_database(monkeypatch):
    """建库时必须先断开库名，否则连的就是那个还不存在的库。

    URL.set() 会忽略值为 None 的参数，用它清 database 是无效的——这条曾经让
    create_database 在全新的状态库上必然失败（MySQL 1049）。
    """
    recorded: list[Any] = []

    class FakeConnection:
        async def scalar(self, *args: Any, **kwargs: Any) -> int:
            return 1  # 库已存在，走不到 CREATE DATABASE

        async def __aenter__(self) -> "FakeConnection":
            return self

        async def __aexit__(self, *exc_info: Any) -> None:
            return None

    class FakeEngine:
        def connect(self) -> FakeConnection:
            return FakeConnection()

        async def dispose(self) -> None:
            return None

    def fake_create_async_engine(url: Any, **kwargs: Any) -> FakeEngine:
        recorded.append(url)
        return FakeEngine()

    monkeypatch.setattr(db, "create_async_engine", fake_create_async_engine)
    await db.create_database_if_not_exists(
        MySQLSettings(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="pw",
            database="brand_new_db",
        )
    )

    assert len(recorded) == 1
    assert recorded[0].database is None
    assert recorded[0].host == "127.0.0.1"
