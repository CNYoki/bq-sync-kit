# -*- coding: utf-8 -*-
"""状态表的自动补列。"""

from __future__ import annotations

from typing import Any

from sqlalchemy import inspect, text

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
