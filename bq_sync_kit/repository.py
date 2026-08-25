# -*- coding: utf-8 -*-
"""共享 MySQL 中的同步状态读写。"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import logging
from pathlib import Path
from typing import Any, AsyncIterator, Sequence

from sqlalchemy import MetaData, Table, inspect, or_, select, text
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.schema import CreateColumn

from bq_sync_kit.config import MySQLSettings
from bq_sync_kit.models import (
    CLEANUP_DONE,
    CLEANUP_FAILED,
    CLEANUP_PENDING,
    STATUS_FAILED,
    STATUS_SUCCESS,
    STATUS_UPLOADING,
    build_state_table,
)
from bq_sync_kit.db import create_database_if_not_exists, create_engine

logger = logging.getLogger(__name__)

_MYSQL_LOCK_NAME_LIMIT = 64


def compute_file_key(path: Path) -> str:
    return hashlib.sha256(str(path.resolve()).encode("utf-8")).hexdigest()


def build_lock_name(scope: str) -> str:
    name = f"bq_sync_kit:{scope}"
    if len(name) <= _MYSQL_LOCK_NAME_LIMIT:
        return name
    digest = hashlib.sha256(scope.encode("utf-8")).hexdigest()[:32]
    return f"bq_sync_kit:{digest}"


def build_job_id(
    project_name: str,
    job_name: str,
    target_table: str,
    file_sha256: str,
    attempt_count: int,
) -> str:
    """同一次 attempt 生成稳定的 job ID，便于崩溃后复用 BigQuery job。"""
    identity = "\0".join(
        (project_name, job_name, target_table, file_sha256, str(attempt_count))
    ).encode("utf-8")
    return f"bq_sync_kit_{hashlib.sha256(identity).hexdigest()}"


@dataclass(frozen=True)
class FileSyncMetadata:
    project_name: str
    job_name: str
    path: Path
    segment_date: date
    size: int
    sha256: str
    target_table: str
    # 只有 producer 的 manifest 会填这两项。
    expected_rows: int | None = None
    cleanup_token: str = ""

    @property
    def file_key(self) -> str:
        return compute_file_key(self.path)


@dataclass(frozen=True)
class SyncAttempt:
    job_id: str
    attempt_count: int
    resumed: bool
    # 同一份文件之前已经清理过源端，就不要再执行一遍删除脚本。
    cleanup_done: bool = False


class SyncRepository:
    def __init__(
        self,
        settings: MySQLSettings,
        *,
        engine: AsyncEngine | None = None,
    ):
        self.settings = settings
        self._engine = engine
        self._owns_engine = engine is None
        self._table: Table | None = None
        self._metadata = MetaData()

    @property
    def table(self) -> Table:
        if self._table is None:
            raise RuntimeError("SyncRepository.initialize() 尚未执行")
        return self._table

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError("SyncRepository.initialize() 尚未执行")
        return self._engine

    async def initialize(self) -> None:
        if self._engine is None:
            if self.settings.create_database:
                await create_database_if_not_exists(self.settings)
            self._engine = create_engine(self.settings)
        self._table = build_state_table(self.settings.state_table, self._metadata)
        # 建表和补列都要在锁内做：两台机器首次升级时同时启动的话，会各自看到同一
        # 张缺列的老表并发出同样的 ADD COLUMN，后一个直接撞重复列错误、整个 run
        # 崩在这里。这个锁和 run() 里按项目加的那个是两回事，只管 schema。
        async with self.run_lock(f"schema:{self.settings.state_table}"):
            async with self._engine.begin() as connection:
                await connection.run_sync(
                    self._metadata.create_all, checkfirst=True
                )
                await self._add_missing_columns(connection)

    async def _add_missing_columns(self, connection: Any) -> None:
        """给早于本版本创建的状态表补上新增的可空列。

        create_all 只会建表，不会改表；升级后直接跑会因为缺列报错，所以这里做一次
        最小的自动迁移。只补可空列，不动既有列。
        """
        table = self.table
        existing = {
            column["name"]
            for column in await connection.run_sync(
                lambda sync_connection: inspect(sync_connection).get_columns(
                    table.name
                )
            )
        }
        dialect = connection.engine.dialect
        quoted_table = dialect.identifier_preparer.format_table(table)
        for column in table.columns:
            if column.name in existing or not column.nullable:
                continue
            definition = CreateColumn(column).compile(dialect=dialect)
            await connection.execute(
                text(f"ALTER TABLE {quoted_table} ADD COLUMN {definition}")
            )
            logger.info("状态表补列: %s.%s", table.name, column.name)

    async def close(self) -> None:
        if self._engine is not None and self._owns_engine:
            await self._engine.dispose()
            self._engine = None

    async def __aenter__(self) -> "SyncRepository":
        await self.initialize()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    @asynccontextmanager
    async def run_lock(self, scope: str) -> AsyncIterator[None]:
        """跨主机互斥：同一 scope 同时只允许一个同步进程。"""
        if not self.engine.dialect.name.startswith("mysql"):
            logger.warning(
                "当前数据库 %s 不支持 GET_LOCK，跳过跨进程互斥",
                self.engine.dialect.name,
            )
            yield
            return

        lock_name = build_lock_name(scope)
        async with self.engine.connect() as connection:
            acquired = await connection.scalar(
                text("SELECT GET_LOCK(:lock_name, 0)"), {"lock_name": lock_name}
            )
            if acquired != 1:
                raise RuntimeError(f"另一个同步进程正在运行: {scope}")
            try:
                yield
            finally:
                await connection.execute(
                    text("SELECT RELEASE_LOCK(:lock_name)"),
                    {"lock_name": lock_name},
                )

    async def is_successful(
        self, project_name: str, job_name: str, path: Path
    ) -> bool:
        table = self.table
        async with self.engine.connect() as connection:
            status = await connection.scalar(
                select(table.c.status).where(
                    table.c.project_name == project_name,
                    table.c.job_name == job_name,
                    table.c.file_key == compute_file_key(path),
                )
            )
        return status == STATUS_SUCCESS

    async def reserve_attempt(
        self, metadata: FileSyncMetadata, *, cleanup_required: bool = False
    ) -> SyncAttempt:
        """登记一次上传尝试，返回本次使用的 BigQuery job ID。"""
        table = self.table
        async with self.engine.begin() as connection:
            record = (
                await connection.execute(
                    select(table).where(
                        table.c.project_name == metadata.project_name,
                        table.c.job_name == metadata.job_name,
                        table.c.file_key == metadata.file_key,
                    )
                )
            ).mappings().first()

            if (
                record is not None
                and record["status"] == STATUS_UPLOADING
                and record["file_sha256"] == metadata.sha256
                and record["target_table"] == metadata.target_table
                and record["bigquery_job_id"]
            ):
                # 上一次进程在“BQ 已提交、状态未落库”的窗口里崩溃，复用同一
                # job ID 让 BigQuery 侧去重，避免重复写入。
                return SyncAttempt(
                    job_id=record["bigquery_job_id"],
                    attempt_count=int(record["attempt_count"] or 0),
                    resumed=True,
                    cleanup_done=record["cleanup_status"] == CLEANUP_DONE,
                )

            # 文件内容没变说明源端那批行已经删过了，保留 done 状态。
            cleanup_done = (
                record is not None
                and record["cleanup_status"] == CLEANUP_DONE
                and record["file_sha256"] == metadata.sha256
            )
            now = datetime.now()
            attempt_count = (
                1 if record is None else int(record["attempt_count"] or 0) + 1
            )
            job_id = build_job_id(
                metadata.project_name,
                metadata.job_name,
                metadata.target_table,
                metadata.sha256,
                attempt_count,
            )
            values = {
                "file_path": str(metadata.path),
                "segment_date": metadata.segment_date,
                "file_size": metadata.size,
                "file_sha256": metadata.sha256,
                "target_table": metadata.target_table,
                "status": STATUS_UPLOADING,
                "attempt_count": attempt_count,
                "bigquery_job_id": job_id,
                "loaded_rows": None,
                "error_message": None,
                "started_at": now,
                "synced_at": None,
                "expected_rows": metadata.expected_rows,
                "cleanup_token": metadata.cleanup_token or None,
                "cleanup_status": (
                    CLEANUP_DONE
                    if cleanup_done
                    else (CLEANUP_PENDING if cleanup_required else None)
                ),
                "cleanup_error": None,
            }
            if record is None:
                await connection.execute(
                    table.insert().values(
                        project_name=metadata.project_name,
                        job_name=metadata.job_name,
                        file_key=metadata.file_key,
                        **values,
                    )
                )
            else:
                await connection.execute(
                    table.update()
                    .where(table.c.id == record["id"])
                    .values(**values)
                )
        return SyncAttempt(
            job_id=job_id,
            attempt_count=attempt_count,
            resumed=False,
            cleanup_done=cleanup_done,
        )

    async def mark_success(
        self,
        metadata: FileSyncMetadata,
        bigquery_job_id: str,
        loaded_rows: int | None,
    ) -> None:
        await self._finish(
            metadata,
            status=STATUS_SUCCESS,
            bigquery_job_id=bigquery_job_id,
            loaded_rows=loaded_rows,
            error_message=None,
        )

    async def mark_failed(
        self,
        metadata: FileSyncMetadata,
        bigquery_job_id: str,
        error_message: str,
    ) -> None:
        await self._finish(
            metadata,
            status=STATUS_FAILED,
            bigquery_job_id=bigquery_job_id,
            loaded_rows=None,
            error_message=error_message[:4000],
        )

    async def mark_archived(
        self, metadata: FileSyncMetadata, archived_path: Path
    ) -> None:
        table = self.table
        async with self.engine.begin() as connection:
            await connection.execute(
                table.update()
                .where(
                    table.c.project_name == metadata.project_name,
                    table.c.job_name == metadata.job_name,
                    table.c.file_key == metadata.file_key,
                )
                .values(archived_path=str(archived_path))
            )

    async def _finish(
        self,
        metadata: FileSyncMetadata,
        *,
        status: str,
        bigquery_job_id: str,
        loaded_rows: int | None,
        error_message: str | None,
    ) -> None:
        table = self.table
        values: dict[str, Any] = {
            "status": status,
            "bigquery_job_id": bigquery_job_id or None,
            "loaded_rows": loaded_rows,
            "error_message": error_message,
        }
        if status == STATUS_SUCCESS:
            values["synced_at"] = datetime.now()
        async with self.engine.begin() as connection:
            result = await connection.execute(
                table.update()
                .where(
                    table.c.project_name == metadata.project_name,
                    table.c.job_name == metadata.job_name,
                    table.c.file_key == metadata.file_key,
                )
                .values(**values)
            )
            if result.rowcount == 0:
                raise RuntimeError(f"同步记录不存在: {metadata.path}")

    async def set_cleanup_status(
        self,
        project_name: str,
        job_name: str,
        file_key: str,
        *,
        status: str,
        error_message: str | None = None,
    ) -> None:
        table = self.table
        async with self.engine.begin() as connection:
            await connection.execute(
                table.update()
                .where(
                    table.c.project_name == project_name,
                    table.c.job_name == job_name,
                    table.c.file_key == file_key,
                )
                .values(
                    cleanup_status=status,
                    cleanup_error=(
                        error_message[:4000] if error_message else None
                    ),
                )
            )

    async def unfinished_cleanups(
        self, project_name: str, job_name: str
    ) -> list[dict[str, Any]]:
        """还没清理干净的记录。

        只要存在这样的记录，就说明源端还留着已经导出过的数据，此时绝不能再跑
        producer——否则同一批数据会被导出第二遍，在 BigQuery 里变成重复行。
        """
        table = self.table
        query = (
            select(table)
            .where(
                table.c.project_name == project_name,
                table.c.job_name == job_name,
                or_(
                    table.c.cleanup_status == CLEANUP_PENDING,
                    table.c.cleanup_status == CLEANUP_FAILED,
                ),
            )
            .order_by(table.c.id)
        )
        async with self.engine.connect() as connection:
            rows = (await connection.execute(query)).mappings().all()
        return [dict(row) for row in rows]

    async def retryable_records(
        self, project_name: str, job_name: str
    ) -> list[dict[str, Any]]:
        """还没成功进仓的记录。

        producer 的 manifest 只描述本轮产出，上一轮失败的文件不会再出现在里面；
        源端的行却可能已经删了，所以必须从状态表里把它们捞回来重试。
        """
        table = self.table
        query = (
            select(table)
            .where(
                table.c.project_name == project_name,
                table.c.job_name == job_name,
                table.c.status.in_([STATUS_UPLOADING, STATUS_FAILED]),
            )
            .order_by(table.c.segment_date, table.c.id)
        )
        async with self.engine.connect() as connection:
            rows = (await connection.execute(query)).mappings().all()
        return [dict(row) for row in rows]

    async def unarchived_records(
        self, project_name: str, job_name: str
    ) -> list[dict[str, Any]]:
        """已经成功进仓、却还没归档成功的记录。

        普通 glob 发现能靠再次扫到文件来补做归档；manifest 模式下 producer 只描述
        本轮产出，上一轮归档失败的文件不会再出现在里面，不从状态表捞回来就会永远
        留在源目录里。
        """
        table = self.table
        query = (
            select(table)
            .where(
                table.c.project_name == project_name,
                table.c.job_name == job_name,
                table.c.status == STATUS_SUCCESS,
                or_(
                    table.c.archived_path.is_(None),
                    table.c.archived_path == "",
                ),
            )
            .order_by(table.c.segment_date, table.c.id)
        )
        async with self.engine.connect() as connection:
            rows = (await connection.execute(query)).mappings().all()
        return [dict(row) for row in rows]

    async def recent_records(
        self,
        *,
        project_names: Sequence[str] | None = None,
        job_names: Sequence[str] | None = None,
        statuses: Sequence[str] | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        table = self.table
        query = select(table).order_by(table.c.updated_at.desc()).limit(limit)
        if project_names:
            query = query.where(table.c.project_name.in_(list(project_names)))
        if job_names:
            query = query.where(table.c.job_name.in_(list(job_names)))
        if statuses:
            query = query.where(table.c.status.in_(list(statuses)))
        async with self.engine.connect() as connection:
            rows = (await connection.execute(query)).mappings().all()
        return [dict(row) for row in rows]
