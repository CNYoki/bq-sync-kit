# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bq_sync_kit.config import KitSettings, build_settings


def make_settings(document: dict[str, Any]) -> KitSettings:
    return build_settings(document)


@pytest.fixture
def sqlite_mysql_settings(tmp_path: Path) -> dict[str, Any]:
    """用 sqlite 顶替共享 MySQL，让状态层可以在测试里跑。"""
    return {
        "dsn": f"sqlite+aiosqlite:///{tmp_path / 'state.db'}",
        "create_database": False,
        "state_table": "bq_file_sync_record",
    }


@pytest.fixture
def base_document(tmp_path: Path, sqlite_mysql_settings: dict[str, Any]) -> dict:
    return {
        "mysql": sqlite_mysql_settings,
        "defaults": {"timezone": "UTC", "location": "US"},
        "projects": [
            {
                "name": "demo",
                "root": str(tmp_path),
                "_base_dir": str(tmp_path),
                "jobs": [
                    {
                        "name": "events",
                        "source_glob": "data/*.jsonl",
                        "target_table": "demo.raw_events",
                    }
                ],
            }
        ],
    }
