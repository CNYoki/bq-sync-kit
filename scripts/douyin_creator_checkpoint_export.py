#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""douyin_creator_checkpoint 的导出脚本，作为 bq_sync_kit 的 producer 钩子。

    export   按 updated_at 的日期把 done=1 的断点导成 JSONL，并回写 $BQ_SYNC_MANIFEST

**只导出，不删除**：源端这张表是爬虫的断点续爬进度，还要继续用，所以这个 job 不配
cleanup 钩子。代价是"哪些天已经导过"没法靠"源端还剩什么"来判断——每轮都重扫一遍全表
的话，同一天会被反复导出。所以导出范围改成由状态表（bq_file_sync_record）决定：
该 job 名下已经有记录的 segment_date 一律跳过，不管那条记录是 success 还是待重试的
uploading / failed（待重试的文件由 kit 自己从状态表捞回来，不该由这里再导一份）。

日期边界与 user_live_visits 一致，只导**前一日及以前**：`updated_at < 边界日期`
（边界默认取今天）。当天还在被爬虫写的那一天不碰，否则当天余下变成 done=1 的行会永远
漏掉——它们的 updated_at 仍落在已经导过、因而会被状态表挡住的那一天里。

时区：这张表的 created_at / updated_at 是 **DATETIME**（不是 TIMESTAMP），MySQL 服务端
`time_zone = +08:00`，`now()` 写进去的是北京墙上时间。DATETIME 不带时区，读出来是什么
就是什么，所以这里显式按 --source-timezone（默认 Asia/Shanghai）补上偏移量再写进
JSONL，BigQuery 那边才会得到正确的 TIMESTAMP。同理，切天用的也是北京日期，所以 kit 里
这个 job 的 timezone 必须跟 --source-timezone 一致（默认 Asia/Shanghai 即可），不一致
会在日志里告警。

单独调试（--no-record-check 表示不查状态表，导出全部符合条件的历史日期）：

    export CREATOR_CHECKPOINT_MYSQL_DSN='mysql://user:pass@127.0.0.1:3306/douyin_crawl_hub'
    python3 scripts/douyin_creator_checkpoint_export.py export \
        --out-dir /tmp/out --no-record-check
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timedelta
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import asyncmy
from sqlalchemy.engine import make_url

logger = logging.getLogger("douyin_creator_checkpoint_export")

TABLE = "douyin_creator_checkpoint"
CHECKPOINT_NAME = "douyin_creator"
DEFAULT_STATE_TABLE = "bq_file_sync_record"
DEFAULT_SOURCE_TIMEZONE = "Asia/Shanghai"

# 显式列出列名，不用 SELECT *：将来加列时 JSONL 的结构不会悄悄跟着变。
# id 只用于 keyset 分页，不输出。
COLUMNS: tuple[str, ...] = (
    "biz_key",
    "page_count",
    "aweme_count",
    "user_tags",
    "created_at",
    "updated_at",
)

# 输出到 BigQuery 时的列名：biz_key 存的就是 sec_user_id，改叫 sec_uid 和仓里其他
# 抖音表对齐。
OUTPUT_NAMES = {"biz_key": "sec_uid"}


def _as_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _as_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _as_tags(value: Any) -> list[str]:
    """JSON 数组列 -> BigQuery 的 STRING REPEATED。

    REPEATED 字段不接受 null，NULL 一律输出成空数组。形状不对（不是数组）宁可当场
    报错，也不要悄悄写成空数组把数据吃掉。
    """
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        parsed: Any = list(value)
    else:
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        text = str(value).strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"user_tags 不是合法 JSON: {text!r}") from exc
    if not isinstance(parsed, list):
        raise ValueError(f"user_tags 需要是 JSON 数组，实际是 {type(parsed).__name__}")
    return [str(item) for item in parsed if item is not None]


def _offset_suffix(moment: datetime, zone: ZoneInfo) -> str:
    offset = moment.replace(tzinfo=zone).utcoffset() or timedelta(0)
    total_minutes = int(offset.total_seconds()) // 60
    sign = "+" if total_minutes >= 0 else "-"
    hours, minutes = divmod(abs(total_minutes), 60)
    return f"{sign}{hours:02d}:{minutes:02d}"


def make_timestamp_converter(timezone_name: str):
    """DATETIME -> 带时区偏移的字符串，例如 2026-08-25 16:49:42+08:00。

    列里存的是墙上时间，不带时区；不补偏移量直接给 BigQuery，会被当成 UTC，整批数据
    平移 8 小时。
    """
    zone = ZoneInfo(timezone_name)

    def convert(value: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, datetime):
            return str(value)
        if value.microsecond:
            base = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            base = value.strftime("%Y-%m-%d %H:%M:%S")
        return base + _offset_suffix(value, zone)

    return convert


def build_converters(timezone_name: str) -> dict[str, Any]:
    to_timestamp = make_timestamp_converter(timezone_name)
    return {
        "biz_key": _as_str,
        "page_count": _as_int,
        "aweme_count": _as_int,
        "user_tags": _as_tags,
        "created_at": to_timestamp,
        "updated_at": to_timestamp,
    }


def encode_row(row: Sequence[Any], converters: dict[str, Any]) -> str:
    record = {
        OUTPUT_NAMES.get(name, name): converters[name](value)
        for name, value in zip(COLUMNS, row)
    }
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


def build_connect_kwargs(dsn: str) -> dict[str, Any]:
    url = make_url(dsn)
    if not url.database:
        raise SystemExit("DSN 里必须带库名")
    return {
        "host": url.host or "127.0.0.1",
        "port": url.port or 3306,
        "user": url.username or "root",
        "password": url.password or "",
        "database": url.database,
        "charset": url.query.get("charset", "utf8mb4"),
    }


async def connect(dsn: str):
    # 这里不动 session time_zone：要读的两列都是 DATETIME，不随会话时区变换，
    # 改了反而会让人以为读出来的是 UTC。
    return await asyncmy.connect(**build_connect_kwargs(dsn), autocommit=True)


async def _fetch(connection, sql: str, args: tuple) -> list[tuple]:
    async with connection.cursor() as cursor:
        await cursor.execute(sql, args)
        return list(await cursor.fetchall())


def _fsync_directory(path: Path) -> None:
    handle = os.open(path, os.O_RDONLY)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


def _validate_identifier(name: str, *, what: str) -> str:
    if not name or not all(char.isalnum() or char == "_" for char in name):
        raise SystemExit(f"{what} 只允许字母、数字和下划线: {name!r}")
    return name


async def fetch_exported_dates(
    connection, *, state_table: str, project_name: str, job_name: str
) -> set[date]:
    """状态表里这个 job 已经登记过的数据日期。

    包含 success 和还没成功的 uploading / failed：后者的文件已经在磁盘上，kit 会自己
    从状态表捞回来重试，这里再导一份只会变成重复数据。
    """
    exists = await _fetch(
        connection,
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (state_table,),
    )
    if not exists or not exists[0][0]:
        # kit 会在跑 producer 之前建好状态表，所以查不到只可能是连错了库——
        # 多半是状态库和源库不在一起而 --state-dsn 没配。这时候返回空集合等于
        # 放开全部历史日期重导一遍，宁可停下来。
        raise SystemExit(
            f"状态库 {state_table} 里找不到状态表：这个 job 靠它判断哪些日期已经"
            "导过，查不到就会重复导出。请用 --state-dsn 指向 kit 的状态库"
            "（或设 $BQ_SYNC_STATE_DSN）"
        )

    rows = await _fetch(
        connection,
        f"SELECT DISTINCT `segment_date` FROM `{state_table}` "
        "WHERE project_name = %s AND job_name = %s",
        (project_name, job_name),
    )
    dates: set[date] = set()
    for (value,) in rows:
        if isinstance(value, datetime):
            dates.add(value.date())
        elif isinstance(value, date):
            dates.add(value)
        elif value:
            dates.add(datetime.strptime(str(value)[:10], "%Y-%m-%d").date())
    return dates


async def fetch_dates(connection, *, boundary: date) -> list[date]:
    """待选的数据日期：done=1 且 updated_at 早于边界日期的那些天。"""
    sql = (
        f"SELECT DISTINCT DATE(`updated_at`) FROM `{TABLE}` "
        "WHERE `checkpoint_name` = %s AND `done` = 1 AND `updated_at` < %s "
        "ORDER BY 1"
    )
    rows = await _fetch(connection, sql, (CHECKPOINT_NAME, boundary))
    result: list[date] = []
    for (value,) in rows:
        if isinstance(value, datetime):
            result.append(value.date())
        elif isinstance(value, date):
            result.append(value)
        else:
            result.append(datetime.strptime(str(value)[:10], "%Y-%m-%d").date())
    return result


async def export_one_date(
    connection,
    *,
    day: date,
    batch_size: int,
    out_dir: Path,
    converters: dict[str, Any],
) -> tuple[Path, int]:
    """把一天导出成一个 JSONL：按 id keyset 分页读、边读边写、最后原子改名。

    只有 rename 成功，这份数据才算完整落到磁盘上。
    """
    columns = ", ".join(f"`{name}`" for name in COLUMNS)
    base = (
        f"SELECT `id`, {columns} FROM `{TABLE}` "
        "WHERE `checkpoint_name` = %s AND `done` = 1 "
        "AND `updated_at` >= %s AND `updated_at` < %s"
    )
    order = " ORDER BY `id` LIMIT %s"
    day_start = datetime.combine(day, datetime.min.time())
    day_end = day_start + timedelta(days=1)

    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{TABLE}_{day.isoformat()}.jsonl"
    temporary = path.with_name(path.name + ".tmp")

    written = 0
    last_id: int | None = None
    with temporary.open("w", encoding="utf-8") as handle:
        while True:
            if last_id is None:
                sql = base + order
                args = (CHECKPOINT_NAME, day_start, day_end, batch_size)
            else:
                sql = base + " AND `id` > %s" + order
                args = (
                    CHECKPOINT_NAME,
                    day_start,
                    day_end,
                    last_id,
                    batch_size,
                )
            rows = await _fetch(connection, sql, args)
            if not rows:
                break
            for row in rows:
                handle.write(encode_row(row[1:], converters))
                handle.write("\n")
            written += len(rows)
            if len(rows) < batch_size:
                break
            last_id = rows[-1][0]
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, path)
    _fsync_directory(path.parent)
    return path, written


async def command_export(args: argparse.Namespace) -> int:
    boundary = args.boundary_date or _boundary_from_env()
    _warn_on_timezone_mismatch(args.source_timezone)
    converters = build_converters(args.source_timezone)
    out_dir = _resolve_out_dir(args.out_dir)

    skip: set[date] = set()
    if args.record_check:
        state_dsn = args.state_dsn or args.dsn
        state_table = _validate_identifier(args.state_table, what="--state-table")
        state_connection = await connect(state_dsn)
        try:
            skip = await fetch_exported_dates(
                state_connection,
                state_table=state_table,
                project_name=args.project,
                job_name=args.job,
            )
        finally:
            await state_connection.ensure_closed()
        logger.info("状态表里已登记 %d 个数据日期", len(skip))
    else:
        logger.warning("已关闭状态表检查，符合条件的日期会全部重新导出")

    connection = await connect(args.dsn)
    entries: list[dict[str, Any]] = []
    try:
        candidates = await fetch_dates(connection, boundary=boundary)
        dates = [day for day in candidates if day not in skip]
        logger.info(
            "边界 %s，候选日期 %d 个，已导过 %d 个，本轮待导: %s",
            boundary.isoformat(),
            len(candidates),
            len(candidates) - len(dates),
            ", ".join(day.isoformat() for day in dates) or "(无)",
        )
        if args.max_dates > 0 and len(dates) > args.max_dates:
            logger.info(
                "本轮只处理最早的 %d 天，其余留给下一轮", args.max_dates
            )
            dates = dates[: args.max_dates]

        for day in dates:
            path, rows = await export_one_date(
                connection,
                day=day,
                batch_size=args.batch_size,
                out_dir=out_dir,
                converters=converters,
            )
            if rows == 0:
                # 上面按天分组数出来的日期不该是空的；真为空就别留个空文件下去，
                # 也别登记进 manifest（登记了会让这一天从此被状态表挡住）。
                path.unlink(missing_ok=True)
                logger.warning("%s 没有数据，跳过", day.isoformat())
                continue
            logger.info("已导出 %s: %d 行 -> %s", day.isoformat(), rows, path)
            entries.append(
                {
                    "path": str(path),
                    "segment_date": day.isoformat(),
                    "rows": rows,
                }
            )
    finally:
        await connection.ensure_closed()

    _write_manifest(args.manifest, entries)
    logger.info("本轮导出 %d 个文件", len(entries))
    return 0


def _boundary_from_env() -> date:
    raw = os.environ.get("BQ_SYNC_BOUNDARY_DATE", "").strip()
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    timezone_name = (
        os.environ.get("BQ_SYNC_TIMEZONE", "").strip() or DEFAULT_SOURCE_TIMEZONE
    )
    return datetime.now(ZoneInfo(timezone_name)).date()


def _warn_on_timezone_mismatch(source_timezone: str) -> None:
    """kit 的 job 时区决定边界日期，源端列的时区决定行落在哪一天，两者必须一致。"""
    job_timezone = os.environ.get("BQ_SYNC_TIMEZONE", "").strip()
    if job_timezone and job_timezone != source_timezone:
        logger.warning(
            "job 时区 (%s) 与源端列时区 (%s) 不一致，切天的边界会偏移，"
            "请把 config 里这个 job 的 timezone 改成 %s",
            job_timezone,
            source_timezone,
            source_timezone,
        )


def _resolve_out_dir(value: str) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    root = os.environ.get("BQ_SYNC_ROOT", "").strip()
    return (Path(root) if root else Path.cwd()) / path


def _write_manifest(manifest: str, entries: list[dict[str, Any]]) -> None:
    target = manifest or os.environ.get("BQ_SYNC_MANIFEST", "").strip()
    document = json.dumps({"files": entries}, ensure_ascii=False)
    if not target:
        logger.info("没有 manifest 落点，直接打印:\n%s", document)
        return
    Path(target).write_text(document + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="douyin_creator_checkpoint_export",
        description=(
            f"把 {TABLE} 里 checkpoint_name={CHECKPOINT_NAME} 且 done=1 的断点"
            "按 updated_at 的日期导出成 JSONL（只导出，不删除）"
        ),
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("CREATOR_CHECKPOINT_MYSQL_DSN")
        or os.environ.get("MYSQL_DSN", ""),
        help="mysql://user:pass@host:port/db，默认取 "
        "$CREATOR_CHECKPOINT_MYSQL_DSN",
    )
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="导出 JSONL")
    export_parser.add_argument(
        "--out-dir",
        default="data/jsonl",
        help="输出目录，相对路径按 $BQ_SYNC_ROOT 解析",
    )
    export_parser.add_argument(
        "--manifest", default="", help="manifest 落点，默认取 $BQ_SYNC_MANIFEST"
    )
    export_parser.add_argument(
        "--boundary-date",
        type=lambda value: datetime.strptime(value, "%Y-%m-%d").date(),
        help="只导 updated_at 早于这一天的数据（即前一日及以前），"
        "默认取 $BQ_SYNC_BOUNDARY_DATE",
    )
    export_parser.add_argument(
        "--source-timezone",
        default=os.environ.get("CREATOR_CHECKPOINT_TIMEZONE")
        or DEFAULT_SOURCE_TIMEZONE,
        help="源端 DATETIME 列存的是哪个时区的墙上时间"
        f"（默认 {DEFAULT_SOURCE_TIMEZONE}）",
    )
    export_parser.add_argument(
        "--max-dates",
        type=int,
        default=0,
        help="每轮最多导几天，0 表示不限（默认 0）",
    )
    export_parser.add_argument("--batch-size", type=int, default=5000)

    state = export_parser.add_argument_group("状态表（决定哪些日期已经导过）")
    state.add_argument(
        "--state-dsn",
        default=os.environ.get("BQ_SYNC_STATE_DSN", ""),
        help="状态库 DSN，默认取 $BQ_SYNC_STATE_DSN（由 kit 注入），"
        "都没有时退回 --dsn",
    )
    state.add_argument(
        "--state-table",
        default=os.environ.get("BQ_SYNC_STATE_TABLE")
        or DEFAULT_STATE_TABLE,  # kit 会注入 $BQ_SYNC_STATE_TABLE
        help=f"状态表名（默认 {DEFAULT_STATE_TABLE}）",
    )
    state.add_argument(
        "--project",
        default=os.environ.get("BQ_SYNC_PROJECT", ""),
        help="状态表里的 project_name，默认取 $BQ_SYNC_PROJECT",
    )
    state.add_argument(
        "--job",
        default=os.environ.get("BQ_SYNC_JOB", ""),
        help="状态表里的 job_name，默认取 $BQ_SYNC_JOB",
    )
    state.add_argument(
        "--no-record-check",
        dest="record_check",
        action="store_false",
        help="不查状态表，把符合条件的日期全部重导（手动补数时才用）",
    )
    export_parser.set_defaults(record_check=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    if not args.dsn:
        raise SystemExit(
            "缺少 DSN：用 --dsn 或设置 $CREATOR_CHECKPOINT_MYSQL_DSN"
        )
    if getattr(args, "batch_size", 1) < 1:
        # LIMIT 0 会让每一天都读成空的，脚本删掉空文件后正常退出——一次静默空转。
        raise SystemExit("--batch-size 必须大于 0")
    if args.record_check and not (args.project and args.job):
        # 缺了这两个就没法定位状态表里的记录，静默跳过检查会导致每轮重复导出。
        raise SystemExit(
            "查状态表需要 --project / --job（或 $BQ_SYNC_PROJECT / $BQ_SYNC_JOB）；"
            "手动补数请显式加 --no-record-check"
        )
    return asyncio.run(command_export(args))


if __name__ == "__main__":
    sys.exit(main())
