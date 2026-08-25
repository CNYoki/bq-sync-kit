# -*- coding: utf-8 -*-
"""bq_sync_kit 命令行入口。"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date
import logging
import sys

from bq_sync_kit.config import ConfigError, KitSettings, load_settings
from bq_sync_kit.db import build_url
from bq_sync_kit.repository import SyncRepository
from bq_sync_kit.sync import SyncRunner

logger = logging.getLogger("bq_sync_kit")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bq-sync-kit",
        description="把各项目当前日期之前的 JSONL 同步到对应的 BigQuery 数据仓库",
    )
    parser.add_argument(
        "--config",
        action="append",
        dest="configs",
        metavar="PATH",
        help="配置文件路径，可重复传入（后面的覆盖前面的）",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    subparsers = parser.add_subparsers(dest="command")

    def add_selectors(target: argparse.ArgumentParser) -> None:
        target.add_argument(
            "--project",
            action="append",
            dest="projects",
            metavar="NAME",
            help="只处理指定项目，可重复传入",
        )
        target.add_argument(
            "--job",
            action="append",
            dest="jobs",
            metavar="NAME",
            help="只处理指定任务（job 或 project/job），可重复传入",
        )

    run_parser = subparsers.add_parser("run", help="执行同步")
    add_selectors(run_parser)
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只列出将要同步的文件，不连接 MySQL 与 BigQuery",
    )
    run_parser.add_argument(
        "--boundary-date",
        type=date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="覆盖“今天”，只同步早于该日期的文件（用于回放）",
    )
    run_parser.add_argument(
        "--limit",
        type=int,
        dest="limit_per_job",
        metavar="N",
        help="每个任务最多处理 N 个文件",
    )

    list_parser = subparsers.add_parser("list", help="列出配置中的项目与任务")
    add_selectors(list_parser)

    status_parser = subparsers.add_parser("status", help="查看 MySQL 中的同步记录")
    add_selectors(status_parser)
    status_parser.add_argument(
        "--status",
        action="append",
        dest="statuses",
        choices=("uploading", "success", "failed"),
        help="按状态过滤，可重复传入",
    )
    status_parser.add_argument("--limit", type=int, default=30)

    subparsers.add_parser("init-db", help="创建状态库与状态表后退出")
    subparsers.add_parser("validate", help="校验配置文件后退出")
    return parser


def _load(args: argparse.Namespace) -> KitSettings:
    settings = load_settings(args.configs)
    logger.debug(
        "已加载配置: %s", ", ".join(str(path) for path in settings.sources)
    )
    return settings


def command_list(settings: KitSettings, args: argparse.Namespace) -> int:
    selected = settings.select_jobs(
        projects=getattr(args, "projects", None),
        jobs=getattr(args, "jobs", None),
    )
    if not selected:
        print("没有匹配到任何任务")
        return 1
    current_project = None
    for job in selected:
        if job.project_name != current_project:
            current_project = job.project_name
            print(f"[{current_project}]")
        print(f"  {job.job_name}")
        print(
            f"    source_globs : "
            f"{', '.join(job.source_globs) or '(由 producer manifest 声明)'}"
        )
        print(f"    root         : {job.root}")
        print(f"    target_table : {job.target_table}")
        print(
            f"    bigquery     : project={job.bigquery.project_id or '(ADC)'} "
            f"location={job.bigquery.location} "
            f"write={job.bigquery.write_disposition}"
        )
        print(
            f"    date         : source={job.date_source} "
            f"pattern={job.date_pattern} format={job.date_format} "
            f"tz={job.timezone}"
        )
        print(
            f"    archive      : {job.archive_dir or '(不归档)'}"
            + (f" layout={job.archive_layout}" if job.archive_dir else "")
        )
        if job.producer.enabled:
            print(
                f"    producer     : {job.producer.display} "
                f"(manifest={'on' if job.producer.manifest else 'off'})"
            )
        if job.cleanup.enabled:
            print(f"    cleanup      : {job.cleanup.display}")
    return 0


async def command_run(settings: KitSettings, args: argparse.Namespace) -> int:
    runner = SyncRunner(settings)
    summary = await runner.run(
        projects=args.projects,
        jobs=args.jobs,
        boundary_date=args.boundary_date,
        dry_run=args.dry_run,
        limit_per_job=args.limit_per_job,
    )
    for job_summary in summary.jobs:
        logger.info(
            "%s: discovered=%d skipped=%d succeeded=%d failed=%d "
            "archived=%d archive_failed=%d cleaned=%d cleanup_failed=%d",
            job_summary.qualified_name,
            job_summary.discovered,
            job_summary.skipped,
            job_summary.succeeded,
            job_summary.failed,
            job_summary.archived,
            job_summary.archive_failed,
            job_summary.cleaned,
            job_summary.cleanup_failed,
        )
    logger.info("同步结束: %s", summary.format_line())
    return 0 if summary.ok else 1


async def command_status(settings: KitSettings, args: argparse.Namespace) -> int:
    selected = settings.select_jobs(projects=args.projects, jobs=args.jobs)
    repository = SyncRepository(settings.mysql)
    async with repository:
        records = await repository.recent_records(
            project_names=sorted({job.project_name for job in selected}),
            job_names=sorted({job.job_name for job in selected}),
            statuses=args.statuses,
            limit=args.limit,
        )
    if not records:
        print("没有匹配的同步记录")
        return 0
    header = f"{'状态':<10} {'项目/任务':<40} {'日期':<12} {'行数':>10}  文件"
    print(header)
    print("-" * len(header))
    for record in records:
        print(
            f"{record['status']:<10} "
            f"{record['project_name'] + '/' + record['job_name']:<40} "
            f"{record['segment_date']!s:<12} "
            f"{record['loaded_rows'] if record['loaded_rows'] is not None else '-':>10}  "
            f"{record['archived_path'] or record['file_path']}"
        )
        if record["status"] == "failed" and record["error_message"]:
            print(f"           错误: {record['error_message']}")
        if record.get("cleanup_status") in ("pending", "failed"):
            print(
                f"           清理未完成({record['cleanup_status']}): "
                f"{record.get('cleanup_error') or record.get('cleanup_token') or ''}"
            )
    return 0


async def command_init_db(settings: KitSettings) -> int:
    url = build_url(settings.mysql)
    repository = SyncRepository(settings.mysql)
    async with repository:
        logger.info(
            "状态表就绪: %s.%s", url.database, settings.mysql.state_table
        )
    return 0


def command_validate(settings: KitSettings) -> int:
    job_count = sum(1 for _ in settings.iter_jobs())
    print(
        f"配置校验通过: {len(settings.projects)} 个项目, {job_count} 个任务\n"
        f"配置文件: {', '.join(str(path) for path in settings.sources)}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    raw_args = list(argv) if argv is not None else sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(raw_args)
    if args.command is None:
        # 不带子命令时按 run 处理，方便直接写进 crontab。
        args = parser.parse_args(raw_args + ["run"])
    command = args.command
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        settings = _load(args)
        if command == "list":
            return command_list(settings, args)
        if command == "validate":
            return command_validate(settings)
        if command == "init-db":
            return asyncio.run(command_init_db(settings))
        if command == "status":
            return asyncio.run(command_status(settings, args))
        return asyncio.run(command_run(settings, args))
    except ConfigError as exc:
        logger.error("配置错误: %s", exc)
        return 2
    except KeyboardInterrupt:
        logger.warning("已被用户中断")
        return 130
    except Exception:
        logger.exception("执行失败")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
