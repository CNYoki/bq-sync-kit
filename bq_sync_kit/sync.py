# -*- coding: utf-8 -*-
"""跨项目扫描 JSONL 并同步到各自的 BigQuery 数据仓库。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime
import logging
from pathlib import Path
from typing import Callable, Sequence
from zoneinfo import ZoneInfo

from bq_sync_kit.bigquery_loader import (
    BigQueryJsonlLoader,
    ClientRegistry,
    LoadResult,
)
from bq_sync_kit.config import KitSettings, SyncJob
from bq_sync_kit.discovery import (
    DiscoveredFile,
    discover_files,
    move_to_archive,
    sha256_file,
)
from bq_sync_kit.notifier import FailureNotifier
from bq_sync_kit.repository import FileSyncMetadata, SyncRepository

logger = logging.getLogger(__name__)


@dataclass
class JobSummary:
    project_name: str
    job_name: str
    discovered: int = 0
    skipped: int = 0
    succeeded: int = 0
    failed: int = 0
    archived: int = 0
    archive_failed: int = 0

    @property
    def qualified_name(self) -> str:
        return f"{self.project_name}/{self.job_name}"


@dataclass
class SyncSummary:
    jobs: list[JobSummary] = field(default_factory=list)

    def _total(self, attribute: str) -> int:
        return sum(getattr(job, attribute) for job in self.jobs)

    @property
    def discovered(self) -> int:
        return self._total("discovered")

    @property
    def skipped(self) -> int:
        return self._total("skipped")

    @property
    def succeeded(self) -> int:
        return self._total("succeeded")

    @property
    def failed(self) -> int:
        return self._total("failed")

    @property
    def archived(self) -> int:
        return self._total("archived")

    @property
    def archive_failed(self) -> int:
        return self._total("archive_failed")

    @property
    def ok(self) -> bool:
        return self.failed == 0 and self.archive_failed == 0

    def format_line(self) -> str:
        return (
            f"discovered={self.discovered} skipped={self.skipped} "
            f"succeeded={self.succeeded} failed={self.failed} "
            f"archived={self.archived} archive_failed={self.archive_failed}"
        )


class SyncRunner:
    def __init__(
        self,
        settings: KitSettings,
        *,
        repository: SyncRepository | None = None,
        loader: BigQueryJsonlLoader | None = None,
        notifier_factory: Callable[[SyncJob], FailureNotifier] | None = None,
        hash_function: Callable[[Path], str] = sha256_file,
    ):
        self.settings = settings
        self.repository = repository or SyncRepository(settings.mysql)
        self.loader = loader or BigQueryJsonlLoader(ClientRegistry())
        self._notifier_factory = notifier_factory or (
            lambda job: FailureNotifier(job.notification)
        )
        self._notifiers: dict[str, FailureNotifier] = {}
        self.hash_function = hash_function

    def _notifier(self, job: SyncJob) -> FailureNotifier:
        notifier = self._notifiers.get(job.qualified_name)
        if notifier is None:
            notifier = self._notifier_factory(job)
            self._notifiers[job.qualified_name] = notifier
        return notifier

    @staticmethod
    def today_for(job: SyncJob) -> date:
        return datetime.now(ZoneInfo(job.timezone)).date()

    async def run(
        self,
        *,
        projects: Sequence[str] | None = None,
        jobs: Sequence[str] | None = None,
        boundary_date: date | None = None,
        dry_run: bool = False,
        limit_per_job: int | None = None,
    ) -> SyncSummary:
        selected = self.settings.select_jobs(projects=projects, jobs=jobs)
        if not selected:
            logger.warning("没有匹配到任何任务")
            return SyncSummary()

        plan: list[tuple[SyncJob, list[DiscoveredFile]]] = []
        for job in selected:
            today = boundary_date or self.today_for(job)
            try:
                files = discover_files(job, today=today)
            except Exception as exc:
                logger.error("%s 扫描文件失败: %s", job.qualified_name, exc)
                raise
            if limit_per_job is not None:
                files = files[:limit_per_job]
            plan.append((job, files))

        if dry_run:
            return self._report_dry_run(plan)

        summary = SyncSummary()
        await self.repository.initialize()
        try:
            # 按项目加锁：不同项目可以并行跑，同一项目只允许一个进程。
            for project_name in dict.fromkeys(job.project_name for job, _ in plan):
                project_plan = [
                    item for item in plan if item[0].project_name == project_name
                ]
                async with self.repository.run_lock(project_name):
                    for job, files in project_plan:
                        summary.jobs.append(await self._run_job(job, files))
        finally:
            await self.repository.close()
            self.loader.close()
        return summary

    def _report_dry_run(
        self, plan: Sequence[tuple[SyncJob, list[DiscoveredFile]]]
    ) -> SyncSummary:
        summary = SyncSummary()
        for job, files in plan:
            job_summary = JobSummary(
                project_name=job.project_name,
                job_name=job.job_name,
                discovered=len(files),
            )
            for file in files:
                logger.info(
                    "[dry-run] %s: %s (%s) -> %s, archive_dir=%s",
                    job.qualified_name,
                    file.path,
                    file.segment_date.isoformat(),
                    job.target_table,
                    job.archive_dir or "(不归档)",
                )
            summary.jobs.append(job_summary)
        return summary

    async def _run_job(
        self, job: SyncJob, files: Sequence[DiscoveredFile]
    ) -> JobSummary:
        summary = JobSummary(
            project_name=job.project_name,
            job_name=job.job_name,
            discovered=len(files),
        )
        logger.info(
            "%s: 待处理文件 %d 个 -> %s",
            job.qualified_name,
            len(files),
            job.target_table,
        )
        for file in files:
            try:
                if await self.repository.is_successful(
                    job.project_name, job.job_name, file.path
                ):
                    summary.skipped += 1
                    logger.info("已同步，跳过: %s", file.path)
                    # 上一轮归档失败的文件在这里补做。
                    await self._archive_file(job, file, summary)
                    continue
                await self._sync_one(job, file, summary)
            except Exception as exc:
                # 单个文件出错不影响同一 job 内其他文件和其他项目。
                summary.failed += 1
                logger.exception("处理文件异常: %s", file.path)
                await self._notify(
                    job,
                    title=f"[bq_sync_kit] 同步异常: {job.qualified_name}",
                    content=(
                        f"文件: {file.path}\n"
                        f"目标表: {job.target_table}\n"
                        f"错误: {type(exc).__name__}: {exc}"
                    ),
                )
        return summary

    async def _sync_one(
        self, job: SyncJob, file: DiscoveredFile, summary: JobSummary
    ) -> None:
        stat_before = file.path.stat()
        file_sha256 = await asyncio.to_thread(self.hash_function, file.path)
        stat_after = file.path.stat()
        if (
            stat_before.st_size != stat_after.st_size
            or stat_before.st_mtime_ns != stat_after.st_mtime_ns
        ):
            raise RuntimeError(f"计算摘要期间文件仍在写入: {file.path}")

        metadata = FileSyncMetadata(
            project_name=job.project_name,
            job_name=job.job_name,
            path=file.path,
            segment_date=file.segment_date,
            size=stat_after.st_size,
            sha256=file_sha256,
            target_table=job.target_table,
        )
        attempt = await self.repository.reserve_attempt(metadata)
        logger.info(
            "开始上传: %s -> %s, attempt=%s, resumed=%s, job_id=%s",
            file.path,
            job.target_table,
            attempt.attempt_count,
            attempt.resumed,
            attempt.job_id,
        )

        try:
            result: LoadResult = await asyncio.to_thread(
                self.loader.load,
                job_id=attempt.job_id,
                path=file.path,
                target_table=job.target_table,
                settings=job.bigquery,
            )
        except Exception as exc:
            summary.failed += 1
            error_message = f"{type(exc).__name__}: {exc}"
            await self.repository.mark_failed(
                metadata, attempt.job_id, error_message
            )
            logger.exception("BigQuery 上传失败: %s", file.path)
            await self._notify(
                job,
                title=f"[bq_sync_kit] BigQuery 同步失败: {job.qualified_name}",
                content=(
                    f"文件: {metadata.path}\n"
                    f"目标表: {metadata.target_table}\n"
                    f"数据日期: {metadata.segment_date.isoformat()}\n"
                    f"文件大小: {metadata.size}\n"
                    f"BigQuery job ID: {attempt.job_id}\n"
                    f"错误: {error_message}"
                ),
            )
            return

        await self.repository.mark_success(
            metadata, result.job_id, result.output_rows
        )
        summary.succeeded += 1
        logger.info(
            "上传成功: %s, rows=%s, job_id=%s",
            file.path,
            result.output_rows,
            result.job_id,
        )
        await self._archive_file(job, file, summary, metadata=metadata)

    async def _archive_file(
        self,
        job: SyncJob,
        file: DiscoveredFile,
        summary: JobSummary,
        *,
        metadata: FileSyncMetadata | None = None,
    ) -> None:
        if not job.archive_dir:
            return
        try:
            destination = await asyncio.to_thread(
                move_to_archive,
                file.path,
                archive_dir=job.archive_dir,
                root=job.root,
                file_sha256=metadata.sha256 if metadata else "",
                keep_date_subdir=job.archive_layout == "date",
                segment_date=file.segment_date,
            )
        except Exception as exc:
            summary.archive_failed += 1
            logger.exception("归档失败: %s", file.path)
            await self._notify(
                job,
                title=f"[bq_sync_kit] 归档失败: {job.qualified_name}",
                content=(
                    f"源文件: {file.path}\n"
                    f"归档目录: {job.archive_dir}\n"
                    f"目标表: {job.target_table}\n"
                    f"错误: {type(exc).__name__}: {exc}"
                ),
            )
            return

        summary.archived += 1
        logger.info("已归档: %s -> %s", file.path, destination)
        record = metadata or FileSyncMetadata(
            project_name=job.project_name,
            job_name=job.job_name,
            path=file.path,
            segment_date=file.segment_date,
            size=file.size,
            sha256="",
            target_table=job.target_table,
        )
        try:
            await self.repository.mark_archived(record, destination)
        except Exception:
            logger.warning("回写归档路径失败: %s", file.path, exc_info=True)

    async def _notify(self, job: SyncJob, *, title: str, content: str) -> None:
        try:
            await self._notifier(job).send(title=title, content=content)
        except Exception:
            logger.exception("通知发送失败: %s", job.qualified_name)
