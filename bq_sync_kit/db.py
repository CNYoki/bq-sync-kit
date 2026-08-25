# -*- coding: utf-8 -*-
"""共享 MySQL 的连接管理。"""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from bq_sync_kit.config import MySQLSettings

logger = logging.getLogger(__name__)


def build_url(settings: MySQLSettings) -> URL:
    if settings.dsn:
        return make_url(settings.dsn)
    return URL.create(
        "mysql+asyncmy",
        username=settings.user,
        password=settings.password or None,
        host=settings.host,
        port=settings.port,
        database=settings.database,
        query={"charset": settings.charset} if settings.charset else {},
    )


def _engine_kwargs(settings: MySQLSettings) -> dict:
    kwargs = {"echo": settings.echo, "pool_pre_ping": True}
    if not build_url(settings).get_backend_name().startswith("sqlite"):
        kwargs["pool_recycle"] = settings.pool_recycle
    return kwargs


async def create_database_if_not_exists(settings: MySQLSettings) -> None:
    """状态库不存在时自动创建（仅 MySQL）。"""
    url = build_url(settings)
    database = url.database
    if not database or not url.get_backend_name().startswith("mysql"):
        return

    # URL.set() 会忽略值为 None 的参数，拿它清库名是无效的——必须用 _replace，
    # 否则这里会连上那个还不存在的库，直接 1049。
    server_engine = create_async_engine(
        url._replace(database=None), echo=settings.echo
    )
    try:
        async with server_engine.connect() as connection:
            exists = await connection.scalar(
                text(
                    "SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME = :name LIMIT 1"
                ),
                {"name": database},
            )
            if exists is None:
                # 库名不能作为绑定参数，只能转义后拼接。
                quoted = database.replace("`", "``")
                await connection.execute(
                    text(
                        f"CREATE DATABASE `{quoted}` "
                        "DEFAULT CHARACTER SET utf8mb4"
                    )
                )
                await connection.commit()
                logger.info("已创建状态库: %s", database)
    finally:
        await server_engine.dispose()


def create_engine(settings: MySQLSettings) -> AsyncEngine:
    return create_async_engine(build_url(settings), **_engine_kwargs(settings))
