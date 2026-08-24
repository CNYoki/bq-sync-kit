# -*- coding: utf-8 -*-
"""外挂脚本钩子。

producer 负责产出 JSONL（例如从 MySQL 导出），cleanup 负责在 JSONL 被本工具
正确收下之后清理源端数据（例如 DELETE 掉已导出的行）。两者都是任意可执行程序，
通过环境变量接收上下文，通过 manifest 文件回报产物，因此新增数据源不需要改
本仓库的代码。
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
import tempfile
from typing import Any, Iterator, Mapping

from bq_sync_kit.config import HookSettings, SyncJob
from bq_sync_kit.discovery import DiscoveredFile, extract_segment_date, resolve_path

logger = logging.getLogger(__name__)

_OUTPUT_TAIL = 2000


@contextmanager
def _manifest_workspace() -> Iterator[Path]:
    """给 producer 准备一个一次性的 manifest 落点。"""
    with tempfile.TemporaryDirectory(prefix="bq_sync_kit_") as directory:
        yield Path(directory) / "manifest.json"


class HookError(RuntimeError):
    """外挂脚本执行失败，或它的产物不合法。"""


@dataclass(frozen=True)
class HookResult:
    returncode: int
    stdout: str
    stderr: str


def _tail(value: str) -> str:
    value = value.strip()
    if len(value) <= _OUTPUT_TAIL:
        return value
    return "..." + value[-_OUTPUT_TAIL:]


def _decode(raw: bytes | None) -> str:
    if not raw:
        return ""
    return raw.decode("utf-8", errors="replace")


async def run_hook(
    hook: HookSettings,
    *,
    root: Path,
    label: str,
    env: Mapping[str, str] | None = None,
) -> HookResult:
    """执行一个外挂脚本；非零退出码或超时都抛 HookError。"""
    if not hook.enabled:
        raise HookError(f"{label}: 未配置 command")

    cwd = resolve_path(hook.cwd, root=root) if hook.cwd else root
    if not cwd.is_dir():
        raise HookError(f"{label}: 工作目录不存在: {cwd}")

    environ = dict(os.environ)
    environ.update(hook.env)
    environ.update(env or {})

    logger.info("%s: 执行 %s (cwd=%s)", label, hook.display, cwd)
    if hook.shell:
        process = await asyncio.create_subprocess_shell(
            hook.command_line,
            cwd=str(cwd),
            env=environ,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    else:
        process = await asyncio.create_subprocess_exec(
            *hook.command,
            cwd=str(cwd),
            env=environ,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    try:
        raw_stdout, raw_stderr = await asyncio.wait_for(
            process.communicate(), timeout=hook.timeout_seconds
        )
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise HookError(
            f"{label}: 执行超时 ({hook.timeout_seconds}s): {hook.display}"
        ) from None

    stdout = _decode(raw_stdout)
    stderr = _decode(raw_stderr)
    if stdout.strip():
        logger.info("%s stdout:\n%s", label, _tail(stdout))
    if stderr.strip():
        logger.info("%s stderr:\n%s", label, _tail(stderr))

    if process.returncode != 0:
        raise HookError(
            f"{label}: 退出码 {process.returncode}: {hook.display}\n"
            f"stderr: {_tail(stderr) or '(空)'}"
        )
    return HookResult(
        returncode=process.returncode or 0, stdout=stdout, stderr=stderr
    )


def _entry_error(where: str, index: int, message: str) -> HookError:
    return HookError(f"{where}: manifest 第 {index + 1} 项{message}")


def _parse_entry(
    raw: Any, *, index: int, job: SyncJob, where: str
) -> DiscoveredFile:
    if isinstance(raw, str):
        raw = {"path": raw}
    if not isinstance(raw, Mapping):
        raise _entry_error(where, index, "必须是字符串或对象")

    raw_path = str(raw.get("path") or "").strip()
    if not raw_path:
        raise _entry_error(where, index, "缺少 path")
    path = resolve_path(raw_path, root=job.root)
    if not path.is_file():
        raise _entry_error(where, index, f"文件不存在: {path}")
    path = path.resolve()

    raw_date = raw.get("segment_date")
    if raw_date in (None, ""):
        # 没声明就退回按 date_source 从文件名 / 路径 / mtime 推断。
        segment_date = extract_segment_date(path, job)
        if segment_date is None:
            raise _entry_error(
                where,
                index,
                f"未声明 segment_date，且无法从 {path.name} 推断出日期",
            )
    elif isinstance(raw_date, date):
        segment_date = raw_date
    else:
        try:
            segment_date = datetime.strptime(
                str(raw_date).strip(), "%Y-%m-%d"
            ).date()
        except ValueError as exc:
            raise _entry_error(
                where, index, f"segment_date 需要 YYYY-MM-DD: {raw_date!r}"
            ) from exc

    raw_rows = raw.get("rows")
    if raw_rows in (None, ""):
        expected_rows = None
    else:
        try:
            expected_rows = int(raw_rows)
        except (TypeError, ValueError) as exc:
            raise _entry_error(
                where, index, f"rows 需要整数: {raw_rows!r}"
            ) from exc
        if expected_rows < 0:
            raise _entry_error(where, index, "rows 不能小于 0")

    target_table = str(raw.get("target_table") or "").strip()
    if target_table and target_table.count(".") not in (1, 2):
        raise _entry_error(
            where,
            index,
            "target_table 需要写成 dataset.table 或 project.dataset.table",
        )

    return DiscoveredFile(
        path=path,
        segment_date=segment_date,
        size=path.stat().st_size,
        expected_rows=expected_rows,
        target_table=target_table,
        cleanup_token=str(raw.get("cleanup_token") or ""),
    )


def parse_manifest(path: Path, *, job: SyncJob) -> list[DiscoveredFile]:
    """解析 producer 回写的 manifest。

    接受 ``{"files": [...]}`` 或直接一个数组；数组元素可以是对象，也可以是只写
    路径的字符串。manifest 不存在或没有条目都表示“这一轮没有产出”，不算失败。
    """
    where = f"{job.qualified_name}.producer"
    if not path.exists():
        logger.info("%s: 未写出 manifest，本轮无产出", where)
        return []

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        logger.info("%s: manifest 为空，本轮无产出", where)
        return []

    try:
        document = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HookError(f"{where}: manifest 不是合法 JSON: {exc}") from exc

    if isinstance(document, Mapping):
        entries = document.get("files")
        if entries is None:
            raise HookError(f"{where}: manifest 对象里缺少 files")
    else:
        entries = document
    if not isinstance(entries, list):
        raise HookError(f"{where}: manifest 的 files 必须是数组")

    files: dict[Path, DiscoveredFile] = {}
    for index, raw in enumerate(entries):
        entry = _parse_entry(raw, index=index, job=job, where=where)
        if entry.path in files:
            raise HookError(f"{where}: manifest 中出现重复文件: {entry.path}")
        files[entry.path] = entry
    return sorted(
        files.values(), key=lambda item: (item.segment_date, str(item.path))
    )


def producer_env(
    job: SyncJob, *, manifest_path: Path, today: date, dry_run: bool
) -> dict[str, str]:
    return {
        "BQ_SYNC_MANIFEST": str(manifest_path),
        "BQ_SYNC_PROJECT": job.project_name,
        "BQ_SYNC_JOB": job.job_name,
        "BQ_SYNC_TARGET_TABLE": job.target_table,
        "BQ_SYNC_ROOT": str(job.root),
        "BQ_SYNC_TIMEZONE": job.timezone,
        "BQ_SYNC_BOUNDARY_DATE": today.isoformat(),
        "BQ_SYNC_DRY_RUN": "1" if dry_run else "0",
    }


def cleanup_env(
    job: SyncJob,
    *,
    path: Path,
    segment_date: date,
    target_table: str,
    cleanup_token: str,
    rows: int | None,
    sha256: str,
) -> dict[str, str]:
    return {
        "BQ_SYNC_PROJECT": job.project_name,
        "BQ_SYNC_JOB": job.job_name,
        "BQ_SYNC_ROOT": str(job.root),
        "BQ_SYNC_FILE": str(path),
        "BQ_SYNC_SEGMENT_DATE": segment_date.isoformat(),
        "BQ_SYNC_TARGET_TABLE": target_table,
        "BQ_SYNC_CLEANUP_TOKEN": cleanup_token,
        "BQ_SYNC_ROWS": "" if rows is None else str(rows),
        "BQ_SYNC_SHA256": sha256,
    }


async def run_producer(
    job: SyncJob, *, today: date, dry_run: bool = False
) -> list[DiscoveredFile] | None:
    """跑 producer 脚本。

    返回 manifest 里声明的文件；``manifest: false`` 时返回 None，表示交给常规的
    glob 发现去捞产物。
    """
    label = f"{job.qualified_name} producer"
    with _manifest_workspace() as manifest_path:
        await run_hook(
            job.producer,
            root=job.root,
            label=label,
            env=producer_env(
                job, manifest_path=manifest_path, today=today, dry_run=dry_run
            ),
        )
        if not job.producer.manifest:
            return None
        return parse_manifest(manifest_path, job=job)
