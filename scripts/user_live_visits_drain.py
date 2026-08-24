#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""user_live_visits 的 drain 导出脚本，配合 bq_sync_kit 的 producer / cleanup 钩子。

    export   按 date_day 把数据导成 JSONL，并回写 $BQ_SYNC_MANIFEST
    cleanup  按 $BQ_SYNC_CLEANUP_TOKEN 删掉已经导出的那一批

这张表没有自增主键（uid × web_rid × date_day 是复合主键），所以删除不能按 id 区间
切，改成按 date_day 整天切：一天一个文件，一天一个 cleanup token，导完整天删整天。

只处理 date_day 早于边界日期（默认今天）的数据，当天还在写的那一天不碰。一天一旦
被导出，cleanup 就把 `date_day = D` 的行全删掉，不看 updated_at。代价是导出到删除
之间那几秒里如果有人补写 date_day = D 的行，会被一起删掉——写入方只写当天数据时
这个窗口不存在；如果确实存在补写历史日期的情况，把 --boundary-date 往前挪一天，
给迟到的写入留出时间。

单独调试：

    export LIVE_VISITS_MYSQL_DSN='mysql://user:pass@127.0.0.1:3306/mydb'
    python3 scripts/user_live_visits_drain.py export --out-dir /tmp/out
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Sequence

import asyncmy
from sqlalchemy.engine import make_url

logger = logging.getLogger("user_live_visits_drain")

TABLE = "user_live_visits"

# 显式列出列名，不用 SELECT *：将来加列时 JSONL 的结构不会悄悄跟着变。
COLUMNS: tuple[str, ...] = (
    "uid",
    "web_rid",
    "date_day",
    "sec_uid",
    "nick_name",
    "following_count",
    "follower_count",
    "first_seen_at",
    "last_seen_at",
    "updated_at",
)

# 复合主键里除 date_day 之外的两段，用来做 keyset 分页。
KEY_COLUMNS = ("uid", "web_rid")


def _format_datetime(value: datetime) -> str:
    if value.microsecond:
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _as_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _as_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _as_date(value: Any) -> str | None:
    """date_day 输出成 2026-08-24 这种形式。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _as_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _format_datetime(value)
    return str(value)


# 每列怎么转成 JSON。uid 按数字输出，对应 BigQuery 的 INT64；如果仓里那一列建成了
# STRING，把这里的 _as_int 换成 _as_str 就行。
CONVERTERS = {
    "uid": _as_int,
    "web_rid": _as_str,
    "date_day": _as_date,
    "sec_uid": _as_str,
    "nick_name": _as_str,
    "following_count": _as_int,
    "follower_count": _as_int,
    "first_seen_at": _as_datetime,
    "last_seen_at": _as_datetime,
    "updated_at": _as_datetime,
}


def encode_row(row: Sequence[Any]) -> str:
    record = {
        name: CONVERTERS[name](value) for name, value in zip(COLUMNS, row)
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


async def connect(dsn: str, *, autocommit: bool):
    connection = await asyncmy.connect(
        **build_connect_kwargs(dsn), autocommit=autocommit
    )
    async with connection.cursor() as cursor:
        # TIMESTAMP 列读出来是什么值取决于会话时区，钉成 UTC 才有确定结果。
        await cursor.execute("SET SESSION time_zone = '+00:00'")
    return connection


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


async def fetch_dates(
    connection, *, boundary: date, max_dates: int
) -> list[date]:
    sql = (
        f"SELECT DISTINCT date_day FROM `{TABLE}` "
        "WHERE date_day < %s ORDER BY date_day"
    )
    rows = await _fetch(connection, sql, (boundary,))
    dates = [row[0] for row in rows]
    if max_dates > 0 and len(dates) > max_dates:
        logger.info("待导出 %d 天，本轮只处理最早的 %d 天", len(dates), max_dates)
        dates = dates[:max_dates]
    return dates


async def export_one_date(
    connection, *, day: date, batch_size: int, out_dir: Path
) -> tuple[Path, int]:
    """把一天导出成一个 JSONL：keyset 分页读、边读边写、最后原子改名。

    只有 rename 成功，这份数据才算落到磁盘上——那是允许删源端数据的前提。
    """
    columns = ", ".join(f"`{name}`" for name in COLUMNS)
    keys = ", ".join(f"`{name}`" for name in KEY_COLUMNS)
    base = f"SELECT {columns} FROM `{TABLE}` WHERE date_day = %s"
    keyset = f" AND ({keys}) > (%s, %s)"
    order = f" ORDER BY {keys} LIMIT %s"

    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{TABLE}_{day.isoformat()}.jsonl"
    temporary = path.with_name(path.name + ".tmp")

    written = 0
    cursor_key: tuple[Any, Any] | None = None
    with temporary.open("w", encoding="utf-8") as handle:
        while True:
            if cursor_key is None:
                sql, args = base + order, (day, batch_size)
            else:
                sql = base + keyset + order
                args = (day, *cursor_key, batch_size)
            rows = await _fetch(connection, sql, args)
            if not rows:
                break
            for row in rows:
                handle.write(encode_row(row))
                handle.write("\n")
            written += len(rows)
            if len(rows) < batch_size:
                break
            cursor_key = (rows[-1][0], rows[-1][1])
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, path)
    _fsync_directory(path.parent)
    return path, written


def build_token(day: date) -> str:
    return json.dumps(
        {"table": TABLE, "date_day": day.isoformat()}, separators=(",", ":")
    )


def parse_token(raw: str) -> date:
    """解析并校验 cleanup token；可疑输入一律拒绝。"""
    raw = (raw or "").strip()
    if not raw:
        # 空 token 会让下面的 DELETE 退化成清空整张表。
        raise SystemExit("cleanup token 为空，拒绝执行删除")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"cleanup token 不是合法 JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("cleanup token 必须是 JSON 对象")
    if payload.get("table") != TABLE:
        raise SystemExit(
            f"cleanup token 指向的表不是 {TABLE}: {payload.get('table')!r}"
        )
    try:
        return datetime.strptime(str(payload["date_day"]), "%Y-%m-%d").date()
    except (KeyError, ValueError) as exc:
        raise SystemExit(f"cleanup token 里的日期不合法: {payload!r}") from exc


async def command_export(args: argparse.Namespace) -> int:
    boundary = args.boundary_date or _boundary_from_env()
    out_dir = _resolve_out_dir(args.out_dir)
    connection = await connect(args.dsn, autocommit=True)
    entries: list[dict[str, Any]] = []
    try:
        dates = await fetch_dates(
            connection, boundary=boundary, max_dates=args.max_dates
        )
        logger.info(
            "边界 %s，待导出日期: %s",
            boundary.isoformat(),
            ", ".join(day.isoformat() for day in dates) or "(无)",
        )

        for day in dates:
            path, rows = await export_one_date(
                connection,
                day=day,
                batch_size=args.batch_size,
                out_dir=out_dir,
            )
            if rows == 0:
                # 这一天一行都没有，既没什么可导，也没什么可删。
                path.unlink(missing_ok=True)
                logger.info("%s 没有数据，跳过", day.isoformat())
                continue
            logger.info("已导出 %s: %d 行 -> %s", day.isoformat(), rows, path)
            entries.append(
                {
                    "path": str(path),
                    "segment_date": day.isoformat(),
                    "rows": rows,
                    "cleanup_token": build_token(day),
                }
            )
    finally:
        await connection.ensure_closed()

    _write_manifest(args.manifest, entries)
    logger.info("本轮导出 %d 个文件", len(entries))
    return 0


async def command_cleanup(args: argparse.Namespace) -> int:
    day = parse_token(args.token)
    connection = await connect(args.dsn, autocommit=False)
    sql = f"DELETE FROM `{TABLE}` WHERE date_day = %s LIMIT %s"
    deleted = 0
    try:
        # 分批删，避免一次锁住太多行。
        while True:
            async with connection.cursor() as cursor:
                affected = await cursor.execute(sql, (day, args.batch_size))
            await connection.commit()
            deleted += affected or 0
            if not affected or affected < args.batch_size:
                break
    finally:
        await connection.ensure_closed()

    # 用同一个 token 再跑一次只会删到 0 行，这正是崩溃后补做时期望的行为。
    logger.info("已删除 date_day=%s 的数据: %d 行", day.isoformat(), deleted)
    return 0


def _boundary_from_env() -> date:
    raw = os.environ.get("BQ_SYNC_BOUNDARY_DATE", "").strip()
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    timezone_name = os.environ.get("BQ_SYNC_TIMEZONE", "").strip()
    if timezone_name:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo(timezone_name)).date()
    return date.today()


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
        prog="user_live_visits_drain",
        description=f"把 {TABLE} 按 date_day 导出成 JSONL，并在确认收下后删除",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("LIVE_VISITS_MYSQL_DSN")
        or os.environ.get("MYSQL_DSN", ""),
        help="mysql://user:pass@host:port/db，默认取 $LIVE_VISITS_MYSQL_DSN",
    )
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="导出 JSONL")
    export_parser.add_argument(
        "--out-dir",
        default=f"export/{TABLE}",
        help="输出目录，相对路径按 $BQ_SYNC_ROOT 解析",
    )
    export_parser.add_argument(
        "--manifest", default="", help="manifest 落点，默认取 $BQ_SYNC_MANIFEST"
    )
    export_parser.add_argument(
        "--boundary-date",
        type=lambda value: datetime.strptime(value, "%Y-%m-%d").date(),
        help="只导早于这一天的数据（也是删除的安全边界），"
        "默认取 $BQ_SYNC_BOUNDARY_DATE",
    )
    export_parser.add_argument(
        "--max-dates",
        type=int,
        default=0,
        help="每轮最多导几天，0 表示不限（默认 0）",
    )
    export_parser.add_argument("--batch-size", type=int, default=5000)

    cleanup_parser = subparsers.add_parser("cleanup", help="删除已导出的数据")
    cleanup_parser.add_argument(
        "--token",
        default=os.environ.get("BQ_SYNC_CLEANUP_TOKEN", ""),
        help="导出时生成的 token，默认取 $BQ_SYNC_CLEANUP_TOKEN",
    )
    cleanup_parser.add_argument("--batch-size", type=int, default=2000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    if not args.dsn:
        raise SystemExit("缺少 DSN：用 --dsn 或设置 $LIVE_VISITS_MYSQL_DSN")
    if args.command == "export":
        return asyncio.run(command_export(args))
    return asyncio.run(command_cleanup(args))


if __name__ == "__main__":
    sys.exit(main())
