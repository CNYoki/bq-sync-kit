# -*- coding: utf-8 -*-
"""发现待同步文件，以及同步完成后的归档搬移。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import glob as globlib
import hashlib
import logging
from pathlib import Path
import re
import shutil

from bq_sync_kit.config import SyncJob

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscoveredFile:
    path: Path
    segment_date: date
    size: int
    # 以下三项只有 producer 的 manifest 会填，普通 glob 发现留空。
    expected_rows: int | None = None
    target_table: str = ""
    cleanup_token: str = ""


@dataclass(frozen=True)
class FileDigest:
    sha256: str
    line_count: int


def digest_file(path: Path) -> FileDigest:
    """一遍读完，同时得到 sha256 和 JSONL 行数。

    行数用来和 producer 声明的 rows 对账；因为反正要为 sha256 读一次整个文件，
    顺手数换行不额外增加 IO。
    """
    digest = hashlib.sha256()
    newlines = 0
    last_byte = b""
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
            newlines += chunk.count(b"\n")
            last_byte = chunk[-1:]
    # 最后一行没有换行符时也算一行。
    if last_byte and last_byte != b"\n":
        newlines += 1
    return FileDigest(sha256=digest.hexdigest(), line_count=newlines)


def sha256_file(path: Path) -> str:
    return digest_file(path).sha256


def resolve_path(value: str, *, root: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = root / path
    return path


def archive_dir_for(job: SyncJob) -> Path | None:
    if not job.archive_dir:
        return None
    return resolve_path(job.archive_dir, root=job.root).resolve()


def extract_segment_date(path: Path, job: SyncJob) -> date | None:
    """按 date_source 从文件名 / 路径 / mtime 推断数据日期。"""
    if job.date_source == "mtime":
        return datetime.fromtimestamp(path.stat().st_mtime).date()

    haystack = path.name if job.date_source == "filename" else str(path)
    matched = re.search(job.date_pattern, haystack)
    if matched is None:
        return None
    raw = matched.group(1)
    try:
        return datetime.strptime(raw, job.date_format).date()
    except ValueError:
        logger.warning(
            "无法按 %s 解析日期 %r: %s", job.date_format, raw, path
        )
        return None


def discover_files(job: SyncJob, *, today: date) -> list[DiscoveredFile]:
    """返回数据日期早于 today 的文件（即“当前日期之前”的完整分段）。"""
    archive_path = archive_dir_for(job)
    files: dict[Path, DiscoveredFile] = {}

    for pattern in job.source_globs:
        resolved_pattern = resolve_path(pattern, root=job.root)
        if archive_path is not None:
            parent = resolved_pattern.parent
            if not globlib.has_magic(str(parent)):
                try:
                    same_dir = parent.resolve() == archive_path
                except OSError:
                    same_dir = False
                if same_dir:
                    raise ValueError(
                        f"{job.qualified_name}: archive_dir 不能与源文件目录相同"
                    )

        for raw_path in globlib.glob(
            str(resolved_pattern), recursive=job.recursive
        ):
            path = Path(raw_path).resolve()
            if path in files:
                continue
            if not path.is_file():
                continue
            if archive_path is not None and path.is_relative_to(archive_path):
                continue

            segment_date = extract_segment_date(path, job)
            if segment_date is None:
                logger.warning("跳过无法识别数据日期的文件: %s", path)
                continue
            if job.require_past_date and segment_date >= today:
                continue

            size = path.stat().st_size
            if size == 0 and job.skip_empty_files:
                logger.info("跳过空文件: %s", path)
                continue
            files[path] = DiscoveredFile(
                path=path, segment_date=segment_date, size=size
            )

    return sorted(
        files.values(), key=lambda item: (item.segment_date, str(item.path))
    )


def move_to_archive(
    path: Path,
    *,
    archive_dir: str,
    root: Path,
    file_sha256: str = "",
    keep_date_subdir: bool = False,
    segment_date: date | None = None,
) -> Path:
    """把已同步的文件搬到归档目录，且不覆盖同名文件。"""
    source = path.resolve()
    destination_dir = resolve_path(archive_dir, root=root).resolve()
    if destination_dir == source.parent:
        raise ValueError("archive_dir 不能与源文件目录相同")
    if keep_date_subdir and segment_date is not None:
        destination_dir = destination_dir / segment_date.isoformat()

    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / source.name
    if destination.exists():
        digest = (file_sha256 or sha256_file(source))[:12]
        destination = destination_dir / f"{source.stem}_{digest}{source.suffix}"
        sequence = 2
        while destination.exists():
            destination = (
                destination_dir
                / f"{source.stem}_{digest}_{sequence}{source.suffix}"
            )
            sequence += 1

    moved = shutil.move(str(source), str(destination))
    return Path(moved).resolve()
