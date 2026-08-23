# -*- coding: utf-8 -*-
"""BigQuery NEWLINE_DELIMITED_JSON load job 封装。"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Any, BinaryIO

from bq_sync_kit.config import BigQuerySettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadResult:
    job_id: str
    output_rows: int | None


def format_size(size: float) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TiB"


class ClientRegistry:
    """按 (project_id, credentials_path, location) 复用 BigQuery client。"""

    def __init__(self) -> None:
        self._clients: dict[tuple[str, str, str], Any] = {}

    def get(self, settings: BigQuerySettings) -> Any:
        key = settings.client_key
        client = self._clients.get(key)
        if client is not None:
            return client

        from google.cloud import bigquery

        credentials = None
        if settings.credentials_path:
            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_file(
                settings.credentials_path
            )

        kwargs: dict[str, Any] = {}
        if settings.project_id:
            kwargs["project"] = settings.project_id
        if credentials is not None:
            kwargs["credentials"] = credentials
        if settings.location:
            kwargs["location"] = settings.location
        client = bigquery.Client(**kwargs)
        self._clients[key] = client
        return client

    def close(self) -> None:
        for client in self._clients.values():
            close = getattr(client, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:  # pragma: no cover - 清理失败不影响结果
                    logger.debug("关闭 BigQuery client 失败", exc_info=True)
        self._clients.clear()


class UploadProgressReader:
    """在 resumable upload 的分片之间打印进度的文件代理。"""

    def __init__(
        self,
        source: BinaryIO,
        *,
        total_size: int,
        path: Path,
        report_bytes: int = 100 * 1024 * 1024,
    ):
        self._source = source
        self._total_size = total_size
        self._path = path
        self._report_bytes = max(1, report_bytes)
        self._next_report = self._report_bytes

    def __getattr__(self, name: str) -> Any:
        return getattr(self._source, name)

    def read(self, size: int = -1) -> bytes:
        # 只有上一个 HTTP 分片被接受后才会发起新的 read。
        confirmed = self._source.tell()
        if confirmed >= self._next_report:
            percent = (
                confirmed * 100 / self._total_size if self._total_size else 100.0
            )
            logger.info(
                "上传进度: %s %.1f%% (%s/%s)",
                self._path,
                min(percent, 100.0),
                format_size(confirmed),
                format_size(self._total_size),
            )
            self._next_report = confirmed + self._report_bytes
        return self._source.read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._source.seek(offset, whence)

    def tell(self) -> int:
        return self._source.tell()


class BigQueryJsonlLoader:
    def __init__(self, registry: ClientRegistry | None = None):
        self._registry = registry or ClientRegistry()
        self._validated_targets: set[tuple[str, str]] = set()

    def close(self) -> None:
        self._registry.close()

    def _validate_target_location(
        self, client: Any, target_table: str, settings: BigQuerySettings
    ) -> None:
        """区域不一致时提前给出可操作的错误，而不是等 BigQuery 报 404。"""
        key = (settings.location, target_table)
        if key in self._validated_targets:
            return

        table = client.get_table(target_table)
        configured = settings.location.strip()
        actual = str(getattr(table, "location", "") or "").strip()
        if configured and actual and configured.casefold() != actual.casefold():
            raise ValueError(
                "BigQuery 区域不一致: "
                f"目标表 {target_table} 位于 {actual}，配置的 location 是 "
                f"{configured}；请把 location 改为 {actual}"
            )
        self._validated_targets.add(key)

    def load(
        self,
        *,
        job_id: str,
        path: Path,
        target_table: str,
        settings: BigQuerySettings,
    ) -> LoadResult:
        from google.api_core.exceptions import Conflict
        from google.cloud import bigquery

        client = self._registry.get(settings)
        self._validate_target_location(client, target_table, settings)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=settings.write_disposition,
            autodetect=settings.autodetect_schema,
            ignore_unknown_values=settings.ignore_unknown_values,
            max_bad_records=settings.max_bad_records,
        )

        try:
            file_size = path.stat().st_size
            logger.info(
                "准备传输文件: %s, size=%s, request_timeout=%ss",
                path,
                format_size(file_size),
                settings.upload_timeout_seconds,
            )
            with path.open("rb") as source:
                progress_source = UploadProgressReader(
                    source, total_size=file_size, path=path
                )
                load_job = client.load_table_from_file(
                    progress_source,
                    target_table,
                    size=file_size,
                    job_id=job_id,
                    job_config=job_config,
                    location=settings.location or None,
                    rewind=True,
                    timeout=settings.upload_timeout_seconds,
                )
            logger.info("文件传输完成: %s (100%%)，等待 BigQuery 写入目标表", path)
        except Conflict:
            # 同一次 attempt 复用 job ID，用来恢复“BQ 已成功、MySQL 状态未落库”
            # 的崩溃窗口。
            logger.info("复用已存在的 BigQuery job: %s", job_id)
            load_job = client.get_job(job_id, location=settings.location or None)

        load_job.result(timeout=settings.job_timeout_seconds)
        return LoadResult(
            job_id=load_job.job_id,
            output_rows=getattr(load_job, "output_rows", None),
        )
