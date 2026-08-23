# -*- coding: utf-8 -*-
"""共享 MySQL 中的文件同步状态表。

表名可配置，因此使用 SQLAlchemy Core 动态构建 Table 而不是声明式 ORM。
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)


STATUS_UPLOADING = "uploading"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"


def build_state_table(table_name: str, metadata: MetaData | None = None) -> Table:
    metadata = metadata if metadata is not None else MetaData()
    return Table(
        table_name,
        metadata,
        # sqlite 只对 INTEGER 主键自增，加 variant 方便本地/测试用 sqlite。
        Column(
            "id",
            BigInteger().with_variant(Integer, "sqlite"),
            primary_key=True,
            autoincrement=True,
        ),
        # project_name 让多个项目共用一张表而不会互相覆盖状态。
        Column("project_name", String(128), nullable=False),
        Column("job_name", String(128), nullable=False),
        Column("file_key", String(64), nullable=False),
        Column("file_path", Text, nullable=False),
        Column("segment_date", Date, nullable=False),
        Column("file_size", BigInteger, nullable=False),
        Column("file_sha256", String(64), nullable=False),
        Column("target_table", String(512), nullable=False),
        Column("status", String(20), nullable=False),
        Column("attempt_count", Integer, nullable=False, default=0),
        Column("bigquery_job_id", String(128)),
        Column("loaded_rows", BigInteger),
        Column("archived_path", Text),
        Column("error_message", Text),
        Column("started_at", DateTime),
        Column("synced_at", DateTime),
        Column(
            "created_at",
            DateTime,
            nullable=False,
            server_default=func.current_timestamp(),
        ),
        Column(
            "updated_at",
            DateTime,
            nullable=False,
            server_default=func.current_timestamp(),
            onupdate=func.current_timestamp(),
        ),
        UniqueConstraint(
            "project_name",
            "job_name",
            "file_key",
            name=f"uq_{table_name}_project_job_file",
        ),
        Index(f"ix_{table_name}_status", "status"),
        Index(f"ix_{table_name}_segment_date", "segment_date"),
        Index(f"ix_{table_name}_project_job", "project_name", "job_name"),
    )
