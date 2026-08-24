# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from bq_sync_kit.config import ConfigError, build_settings, load_settings


def test_defaults_flow_down_and_job_overrides(base_document):
    base_document["defaults"]["max_bad_records"] = 5
    base_document["projects"][0]["location"] = "asia-east2"
    base_document["projects"][0]["jobs"][0]["max_bad_records"] = 0

    settings = build_settings(base_document)
    job = settings.projects[0].jobs[0]

    assert job.bigquery.location == "asia-east2"
    assert job.bigquery.max_bad_records == 0
    assert job.timezone == "UTC"
    assert job.qualified_name == "demo/events"


def test_env_expansion(monkeypatch, base_document):
    monkeypatch.setenv("DEMO_TABLE", "prod.events")
    base_document["projects"][0]["jobs"][0]["target_table"] = "${DEMO_TABLE}"
    base_document["projects"][0]["project_id"] = "${MISSING_ID:-fallback}"

    settings = build_settings(base_document)
    job = settings.projects[0].jobs[0]

    assert job.target_table == "prod.events"
    assert job.bigquery.project_id == "fallback"


def test_relative_paths_resolve_against_root(tmp_path, base_document):
    base_document["projects"][0]["root"] = "sub"
    settings = build_settings(base_document)
    assert settings.projects[0].jobs[0].root == (tmp_path / "sub").resolve()


def test_missing_target_table_is_rejected(base_document):
    del base_document["projects"][0]["jobs"][0]["target_table"]
    with pytest.raises(ConfigError, match="target_table"):
        build_settings(base_document)


def test_bad_date_pattern_is_rejected(base_document):
    base_document["projects"][0]["jobs"][0]["date_pattern"] = r"\d{4}-\d{2}-\d{2}"
    with pytest.raises(ConfigError, match="分组"):
        build_settings(base_document)


def test_duplicate_project_names_are_rejected(base_document):
    base_document["projects"].append(dict(base_document["projects"][0]))
    with pytest.raises(ConfigError, match="项目名重复"):
        build_settings(base_document)


def test_select_jobs_by_project_and_job(base_document):
    base_document["projects"][0]["jobs"].append(
        {
            "name": "profiles",
            "source_glob": "data/profiles_*.jsonl",
            "target_table": "demo.raw_profiles",
        }
    )
    settings = build_settings(base_document)

    assert len(settings.select_jobs()) == 2
    assert len(settings.select_jobs(projects=["demo"])) == 2
    assert [job.job_name for job in settings.select_jobs(jobs=["profiles"])] == [
        "profiles"
    ]
    assert [
        job.job_name for job in settings.select_jobs(jobs=["demo/events"])
    ] == ["events"]
    with pytest.raises(ConfigError, match="未知任务"):
        settings.select_jobs(jobs=["nope"])


def test_include_merges_project_files(tmp_path: Path):
    (tmp_path / "conf.d").mkdir()
    (tmp_path / "conf.d" / "a.yaml").write_text(
        yaml.safe_dump(
            {
                "projects": [
                    {
                        "name": "alpha",
                        "root": "/srv/alpha",
                        "jobs": [
                            {
                                "name": "one",
                                "source_glob": "*.jsonl",
                                "target_table": "alpha.raw",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    root_config = tmp_path / "config.yaml"
    root_config.write_text(
        yaml.safe_dump(
            {
                "include": ["conf.d/*.yaml"],
                "mysql": {"database": "bq_sync"},
                "defaults": {"location": "asia-east2"},
                "projects": [
                    {
                        "name": "beta",
                        "root": "/srv/beta",
                        "jobs": [
                            {
                                "name": "two",
                                "source_glob": "*.jsonl",
                                "target_table": "beta.raw",
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    settings = load_settings([root_config])

    assert [project.name for project in settings.projects] == ["alpha", "beta"]
    # include 进来的项目同样继承根文件的 defaults。
    assert all(
        job.bigquery.location == "asia-east2" for job in settings.iter_jobs()
    )
    assert settings.projects[0].jobs[0].root == Path("/srv/alpha")


def test_manifest_producer_does_not_need_a_source_glob(base_document):
    job = base_document["projects"][0]["jobs"][0]
    del job["source_glob"]
    job["producer"] = {"command": "sh export.sh"}

    settings = build_settings(base_document)

    assert settings.projects[0].jobs[0].source_globs == ()


def test_glob_producer_still_needs_a_source_glob(base_document):
    job = base_document["projects"][0]["jobs"][0]
    del job["source_glob"]
    job["producer"] = {"command": "sh export.sh", "manifest": False}

    with pytest.raises(ConfigError, match="必须配置 source_glob"):
        build_settings(base_document)


def test_hooks_are_inheritable_from_defaults(base_document):
    base_document["defaults"]["cleanup"] = {"command": "sh clean.sh"}
    base_document["projects"][0]["producer"] = {"command": "sh export.sh"}

    job = build_settings(base_document).projects[0].jobs[0]

    assert job.cleanup.command == ("sh", "clean.sh")
    assert job.producer.command == ("sh", "export.sh")
