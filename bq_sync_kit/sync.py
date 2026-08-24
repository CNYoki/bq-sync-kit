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
    FileDigest,
    digest_file,
    discover_files,
    move_to_archive,
)
from bq_sync_kit.hooks import HookError, cleanup_env, run_hook, run_producer
from bq_sync_kit.models import CLEANUP_DONE, CLEANUP_FAILED
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
    cleaned: int = 0
    cleanup_failed: int = 0

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
    def cleaned(self) -> int:
        return self._total("cleaned")

    @property
    def cleanup_failed(self) -> int:
        return self._total("cleanup_failed")

    @property
    def ok(self) -> bool:
        return (
            self.failed == 0
            and self.archive_failed == 0
            and self.cleanup_failed == 0
        )

    def format_line(self) -> str:
        return (
            f"discovered={self.discovered} skipped={self.skipped} "
            f"succeeded={self.succeeded} failed={self.failed} "
            f"archived={self.archived} archive_failed={self.archive_failed} "
            f"cleaned={self.cleaned} cleanup_failed={self.cleanup_failed}"
        )


class SyncRunner:
    def __init__(
        self,
        settings: KitSettings,
        *,
        repository: SyncRepository | None = None,
        loader: BigQueryJsonlLoader | None = None,
        notifier_factory: Callable[[SyncJob], FailureNotifier] | None = None,
        digest_function: Callable[[Path], FileDigest] = digest_file,
    ):
        self.settings = settings
        self.repository = repository or SyncRepository(settings.mysql)
        self.loader = loader or BigQueryJsonlLoader(ClientRegistry())
        self._notifier_factory = notifier_factory or (
            lambda job: FailureNotifier(job.notification)
        )
        self._notifiers: dict[str, FailureNotifier] = {}
        self.digest_function = digest_function

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

        if dry_run:
            return await self._report_dry_run(
                selected, boundary_date=boundary_date, limit_per_job=limit_per_job
            )

        summary = SyncSummary()
        await self.repository.initialize()
        try:
            # 按项目加锁：不同项目可以并行跑，同一项目只允许一个进程。
            # producer 会改动源端数据，必须在锁内执行，否则两台机器会各导一份。
            for project_name in dict.fromkeys(
                job.project_name for job in selected
            ):
                project_jobs = [
                    job for job in selected if job.project_name == project_name
                ]
                async with self.repository.run_lock(project_name):
                    for job in project_jobs:
                        job_summary = JobSummary(
                            project_name=job.project_name,
                            job_name=job.job_name,
                        )
                        summary.jobs.append(job_summary)
                        today = boundary_date or self.today_for(job)
                        files = await self._prepare_files(
                            job,
                            today=today,
                            limit_per_job=limit_per_job,
                            summary=job_summary,
                        )
                        if files is None:
                            continue
                        job_summary.discovered = len(files)
                        await self._run_job(job, files, job_summary)
        finally:
            await self.repository.close()
            self.loader.close()
        return summary

    async def _prepare_files(
        self,
        job: SyncJob,
        *,
        today: date,
        limit_per_job: int | None,
        summary: JobSummary,
    ) -> list[DiscoveredFile] | None:
        """准备本轮要处理的文件；返回 None 表示这个 job 本轮不再往下走。"""
        if job.cleanup.enabled:
            try:
                await self._retry_unfinished_cleanups(job, summary)
            except HookError as exc:
                if not job.producer.enabled:
                    # 没有 producer 就不存在重复导出的风险，记一笔继续往下走。
                    summary.cleanup_failed += 1
                    logger.error("%s: %s", job.qualified_name, exc)
                    return await self._discover(job, today, limit_per_job)
                # 源端还留着已导出的数据，再跑一次 producer 就会导出第二遍。
                summary.failed += 1
                logger.error("%s: %s", job.qualified_name, exc)
                await self._notify(
                    job,
                    title=f"[bq_sync_kit] 待清理数据未处理完: {job.qualified_name}",
                    content=(
                        f"上一轮的 cleanup 没有成功，本轮已跳过 producer 以免重复导出。\n"
                        f"错误: {exc}"
                    ),
                )
                return None

        if job.producer.enabled:
            try:
                produced = await run_producer(job, today=today)
            except HookError as exc:
                if job.producer.on_error == "skip":
                    logger.warning("%s: producer 失败，已跳过: %s",
                                   job.qualified_name, exc)
                    return None
                summary.failed += 1
                logger.error("%s: %s", job.qualified_name, exc)
                await self._notify(
                    job,
                    title=f"[bq_sync_kit] producer 执行失败: {job.qualified_name}",
                    content=str(exc),
                )
                return None
            if produced is not None:
                files = await self._with_retryable(job, produced)
                if limit_per_job is not None:
                    files = files[:limit_per_job]
                return files

        return await self._discover(job, today, limit_per_job)

    async def _discover(
        self, job: SyncJob, today: date, limit_per_job: int | None
    ) -> list[DiscoveredFile]:
        try:
            files = discover_files(job, today=today)
        except Exception as exc:
            logger.error("%s 扫描文件失败: %s", job.qualified_name, exc)
            raise
        if limit_per_job is not None:
            files = files[:limit_per_job]
        return files

    async def _with_retryable(
        self, job: SyncJob, produced: Sequence[DiscoveredFile]
    ) -> list[DiscoveredFile]:
        """把状态表里还没成功进仓的文件并进本轮产物一起重试。"""
        files = {file.path: file for file in produced}
        for record in await self.repository.retryable_records(
            job.project_name, job.job_name
        ):
            path = Path(record["file_path"])
            if path in files:
                continue
            if not path.is_file():
                logger.error(
                    "%s: 待重试的文件已不存在，需要人工确认: %s (状态 %s)",
                    job.qualified_name,
                    path,
                    record["status"],
                )
                continue
            logger.info("%s: 重试上一轮未完成的文件: %s", job.qualified_name, path)
            files[path] = DiscoveredFile(
                path=path,
                segment_date=record["segment_date"],
                size=record["file_size"],
                expected_rows=record["expected_rows"],
                target_table=record["target_table"],
                cleanup_token=record["cleanup_token"] or "",
            )
        return sorted(
            files.values(), key=lambda item: (item.segment_date, str(item.path))
        )

    async def _retry_unfinished_cleanups(
        self, job: SyncJob, summary: JobSummary
    ) -> None:
        """把上一轮没做完的 cleanup 补上，补不上就抛错拦住 producer。"""
        if not job.cleanup.enabled:
            return
        pending = await self.repository.unfinished_cleanups(
            job.project_name, job.job_name
        )
        if not pending:
            return
        logger.info(
            "%s: 有 %d 条记录的 cleanup 未完成，先补做",
            job.qualified_name,
            len(pending),
        )
        stuck: list[str] = []
        for record in pending:
            path = Path(record["file_path"])
            try:
                await self._run_cleanup(
                    job,
                    file_key=record["file_key"],
                    path=path,
                    segment_date=record["segment_date"],
                    target_table=record["target_table"],
                    cleanup_token=record["cleanup_token"] or "",
                    rows=record["expected_rows"],
                    sha256=record["file_sha256"] or "",
                )
            except HookError as exc:
                stuck.append(f"{path}: {exc}")
                continue
            summary.cleaned += 1
        if stuck:
            raise HookError("; ".join(stuck))

    async def _run_cleanup(
        self,
        job: SyncJob,
        *,
        file_key: str,
        path: Path,
        segment_date: date,
        target_table: str,
        cleanup_token: str,
        rows: int | None,
        sha256: str,
    ) -> None:
        """执行 cleanup 钩子并把结果落库；失败时抛 HookError。"""
        try:
            await run_hook(
                job.cleanup,
                root=job.root,
                label=f"{job.qualified_name} cleanup",
                env=cleanup_env(
                    job,
                    path=path,
                    segment_date=segment_date,
                    target_table=target_table,
                    cleanup_token=cleanup_token,
                    rows=rows,
                    sha256=sha256,
                ),
            )
        except HookError as exc:
            await self.repository.set_cleanup_status(
                job.project_name,
                job.job_name,
                file_key,
                status=CLEANUP_FAILED,
                error_message=str(exc),
            )
            raise
        await self.repository.set_cleanup_status(
            job.project_name, job.job_name, file_key, status=CLEANUP_DONE
        )
        logger.info("已清理源端数据: %s", path)

    async def _report_dry_run(
        self,
        selected: Sequence[SyncJob],
        *,
        boundary_date: date | None,
        limit_per_job: int | None,
    ) -> SyncSummary:
        summary = SyncSummary()
        for job in selected:
            today = boundary_date or self.today_for(job)
            job_summary = JobSummary(
                project_name=job.project_name, job_name=job.job_name
            )
            summary.jobs.append(job_summary)

            files: list[DiscoveredFile] | None = None
            if job.producer.enabled:
                if job.producer.run_on_dry_run:
                    files = await run_producer(job, today=today, dry_run=True)
                else:
                    # producer 会改动源端数据，dry-run 默认不碰它。
                    logger.info(
                        "[dry-run] %s: 跳过 producer (%s)，产物未知",
                        job.qualified_name,
                        job.producer.display,
                    )
                    if job.producer.manifest:
                        continue
            if files is None:
                files = discover_files(job, today=today)
            if limit_per_job is not None:
                files = files[:limit_per_job]

            job_summary.discovered = len(files)
            for file in files:
                logger.info(
                    "[dry-run] %s: %s (%s) -> %s, archive_dir=%s, cleanup=%s",
                    job.qualified_name,
                    file.path,
                    file.segment_date.isoformat(),
                    file.target_table or job.target_table,
                    job.archive_dir or "(不归档)",
                    job.cleanup.display or "(不清理)",
                )
        return summary

    async def _run_job(
        self,
        job: SyncJob,
        files: Sequence[DiscoveredFile],
        summary: JobSummary,
    ) -> JobSummary:
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
        digest = await asyncio.to_thread(self.digest_function, file.path)
        stat_after = file.path.stat()
        if (
            stat_before.st_size != stat_after.st_size
            or stat_before.st_mtime_ns != stat_after.st_mtime_ns
        ):
            raise RuntimeError(f"计算摘要期间文件仍在写入: {file.path}")

        if (
            file.expected_rows is not None
            and file.expected_rows != digest.line_count
        ):
            # 导出被截断是唯一会真丢数据的场景：这里拦住，cleanup 就不会执行。
            raise RuntimeError(
                f"行数与 producer 声明的不一致: {file.path} "
                f"声明 {file.expected_rows} 行，实际 {digest.line_count} 行"
            )

        metadata = FileSyncMetadata(
            project_name=job.project_name,
            job_name=job.job_name,
            path=file.path,
            segment_date=file.segment_date,
            size=stat_after.st_size,
            sha256=digest.sha256,
            target_table=file.target_table or job.target_table,
            expected_rows=file.expected_rows,
            cleanup_token=file.cleanup_token,
        )
        attempt = await self.repository.reserve_attempt(
            metadata, cleanup_required=job.cleanup.enabled
        )
        # 文件已经通过校验并落库（有 sha256、有“必须 load 这个文件”的记录），
        # 到这里就算本工具正确收下了数据，可以让源端把已导出的行删掉。
        if job.cleanup.enabled and not attempt.cleanup_done:
            try:
                await self._run_cleanup(
                    job,
                    file_key=metadata.file_key,
                    path=file.path,
                    segment_date=file.segment_date,
                    target_table=metadata.target_table,
                    cleanup_token=file.cleanup_token,
                    rows=file.expected_rows,
                    sha256=digest.sha256,
                )
            except HookError as exc:
                # 清理失败不该拦住数据进仓；但源端还留着这批行，下一轮会先补做
                # cleanup，补不上就不会再跑 producer。
                summary.cleanup_failed += 1
                logger.error("清理源端数据失败: %s: %s", file.path, exc)
                await self._notify(
                    job,
                    title=f"[bq_sync_kit] 清理失败: {job.qualified_name}",
                    content=(
                        f"文件: {file.path}\n"
                        f"cleanup_token: {file.cleanup_token or '(空)'}\n"
                        f"错误: {exc}"
                    ),
                )
            else:
                summary.cleaned += 1

        logger.info(
            "开始上传: %s -> %s, attempt=%s, resumed=%s, job_id=%s",
            file.path,
            metadata.target_table,
            attempt.attempt_count,
            attempt.resumed,
            attempt.job_id,
        )

        try:
            result: LoadResult = await asyncio.to_thread(
                self.loader.load,
                job_id=attempt.job_id,
                path=file.path,
                target_table=metadata.target_table,
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
