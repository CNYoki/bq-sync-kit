# -*- coding: utf-8 -*-
"""跨项目 BigQuery 同步的类型化配置。

配置来源是 YAML 文件，层级为 defaults -> project -> job，
下层同名键覆盖上层；字符串支持 ${VAR} / ${VAR:-default} 环境变量展开。
"""

from __future__ import annotations

from dataclasses import dataclass, field
import glob as globlib
import logging
import os
from pathlib import Path
import re
import shlex
from typing import Any, Iterator, Mapping, MutableMapping, Sequence

import yaml


logger = logging.getLogger(__name__)


DEFAULT_DATE_PATTERN = r"(\d{4}-\d{2}-\d{2})"
DEFAULT_DATE_FORMAT = "%Y-%m-%d"
DEFAULT_STATE_TABLE = "bq_file_sync_record"

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

# 可以在 defaults / project / job 三层任意一层出现并被下层覆盖的键。
_INHERITABLE_KEYS = frozenset(
    {
        "project_id",
        "credentials_path",
        "location",
        "timezone",
        "root",
        "archive_dir",
        "archive_layout",
        "date_source",
        "date_pattern",
        "date_format",
        "recursive",
        "skip_empty_files",
        "autodetect_schema",
        "ignore_unknown_values",
        "max_bad_records",
        "write_disposition",
        "upload_timeout_seconds",
        "job_timeout_seconds",
        "notification",
        "producer",
        "cleanup",
        "require_past_date",
    }
)

_BUILTIN_DEFAULTS: dict[str, Any] = {
    "project_id": "",
    "credentials_path": "",
    "location": "US",
    "timezone": "Asia/Shanghai",
    "root": "",
    "archive_dir": "",
    "archive_layout": "flat",
    "date_source": "filename",
    "date_pattern": DEFAULT_DATE_PATTERN,
    "date_format": DEFAULT_DATE_FORMAT,
    "recursive": False,
    "skip_empty_files": True,
    "autodetect_schema": False,
    "ignore_unknown_values": False,
    "max_bad_records": 0,
    "write_disposition": "WRITE_APPEND",
    "upload_timeout_seconds": 300.0,
    "job_timeout_seconds": 3600.0,
    "notification": {},
    "producer": {},
    "cleanup": {},
    "require_past_date": True,
}

_VALID_DATE_SOURCES = frozenset({"filename", "path", "mtime"})
_VALID_ARCHIVE_LAYOUTS = frozenset({"flat", "date"})
_VALID_WRITE_DISPOSITIONS = frozenset(
    {"WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"}
)
_VALID_HOOK_ON_ERROR = frozenset({"fail", "skip"})


class ConfigError(ValueError):
    """配置文件不合法。"""


def expand_env(value: Any) -> Any:
    """递归展开字符串中的 ${VAR} 与 ${VAR:-default}。"""
    if isinstance(value, str):

        def _replace(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            resolved = os.environ.get(name)
            if resolved is None or resolved == "":
                resolved = default if default is not None else ""
            return resolved

        return _ENV_RE.sub(_replace, value)
    if isinstance(value, Mapping):
        return {key: expand_env(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [expand_env(item) for item in value]
    return value


def _merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        current = merged.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            merged[key] = _merge(current, value)
        else:
            merged[key] = value
    return merged


def _inheritable(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: item for key, item in value.items() if key in _INHERITABLE_KEYS
    }


def _as_bool(value: Any, key: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off", ""}:
            return False
    raise ConfigError(f"{key} 需要布尔值，收到: {value!r}")


def _as_float(value: Any, key: str, *, minimum: float | None = None) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{key} 需要数字，收到: {value!r}") from exc
    if minimum is not None and number <= minimum:
        raise ConfigError(f"{key} 必须大于 {minimum}")
    return number


@dataclass(frozen=True)
class MySQLSettings:
    """所有项目共用的状态库连接。"""

    dsn: str = ""
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "bq_sync"
    charset: str = "utf8mb4"
    state_table: str = DEFAULT_STATE_TABLE
    create_database: bool = True
    echo: bool = False
    pool_recycle: int = 1800

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "MySQLSettings":
        value = value or {}
        settings = cls(
            dsn=str(value.get("dsn", "")).strip(),
            host=str(value.get("host", "127.0.0.1")).strip(),
            port=int(value.get("port", 3306)),
            user=str(value.get("user", "root")).strip(),
            password=str(value.get("password", "")),
            database=str(value.get("database", "bq_sync")).strip(),
            charset=str(value.get("charset", "utf8mb4")).strip(),
            state_table=str(
                value.get("state_table", DEFAULT_STATE_TABLE)
            ).strip()
            or DEFAULT_STATE_TABLE,
            create_database=_as_bool(
                value.get("create_database", True), "mysql.create_database"
            ),
            echo=_as_bool(value.get("echo", False), "mysql.echo"),
            pool_recycle=int(value.get("pool_recycle", 1800)),
        )
        if not settings.dsn and not settings.database:
            raise ConfigError("mysql.database 不能为空")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", settings.state_table):
            raise ConfigError(
                f"mysql.state_table 只允许字母、数字和下划线: {settings.state_table}"
            )
        return settings


@dataclass(frozen=True)
class NotificationSettings:
    enabled: bool = False
    url: str = ""
    method: str = "POST"
    parameter_format: str = "json"
    params: Mapping[str, Any] = field(default_factory=dict)
    headers: Mapping[str, Any] = field(default_factory=dict)
    timeout_seconds: float = 10.0

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any] | None
    ) -> "NotificationSettings":
        value = value or {}
        settings = cls(
            enabled=_as_bool(
                value.get("enabled", False), "notification.enabled"
            ),
            url=str(value.get("url", "")).strip(),
            method=str(value.get("method", "POST")).strip().upper(),
            parameter_format=str(value.get("parameter_format", "json"))
            .strip()
            .lower(),
            params=value.get("params")
            or {"title": "{title}", "content": "{content}"},
            headers=value.get("headers") or {},
            timeout_seconds=_as_float(
                value.get("timeout_seconds", 10),
                "notification.timeout_seconds",
                minimum=0,
            ),
        )
        if settings.enabled and not settings.url:
            raise ConfigError("启用 notification 后必须配置 url")
        if settings.method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            raise ConfigError(f"不支持的通知请求方式: {settings.method}")
        if settings.parameter_format not in {"json", "form", "query"}:
            raise ConfigError(
                "notification.parameter_format 仅支持 json、form、query"
            )
        return settings


@dataclass(frozen=True)
class HookSettings:
    """一个外挂脚本的执行参数。command 为空表示该钩子未启用。"""

    command: tuple[str, ...] = ()
    command_line: str = ""
    shell: bool = False
    cwd: str = ""
    env: Mapping[str, str] = field(default_factory=dict)
    timeout_seconds: float = 1800.0
    on_error: str = "fail"

    @property
    def enabled(self) -> bool:
        return bool(self.command_line if self.shell else self.command)

    @property
    def display(self) -> str:
        return self.command_line if self.shell else " ".join(self.command)

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any] | None, *, where: str, **extra: Any
    ) -> "HookSettings":
        value = value or {}
        shell = _as_bool(value.get("shell", False), f"{where}.shell")
        raw_command = value.get("command", "")
        command: tuple[str, ...] = ()
        command_line = ""
        if isinstance(raw_command, str):
            command_line = raw_command.strip()
            if command_line and not shell:
                try:
                    command = tuple(shlex.split(command_line))
                except ValueError as exc:
                    raise ConfigError(
                        f"{where}.command 无法解析为参数列表: {exc}"
                    ) from exc
        elif isinstance(raw_command, Sequence):
            command = tuple(
                str(item) for item in raw_command if str(item).strip()
            )
            command_line = shlex.join(command) if command else ""
            if shell and not command_line:
                command_line = ""
        else:
            raise ConfigError(f"{where}.command 需要字符串或字符串数组")

        raw_env = value.get("env") or {}
        if not isinstance(raw_env, Mapping):
            raise ConfigError(f"{where}.env 需要是映射")

        on_error = str(value.get("on_error", "fail")).strip().lower()
        if on_error not in _VALID_HOOK_ON_ERROR:
            raise ConfigError(
                f"{where}.on_error 仅支持 "
                f"{', '.join(sorted(_VALID_HOOK_ON_ERROR))}"
            )

        return cls(
            command=command,
            command_line=command_line,
            shell=shell,
            cwd=str(value.get("cwd", "")).strip(),
            env={str(key): str(item) for key, item in raw_env.items()},
            timeout_seconds=_as_float(
                value.get("timeout_seconds", 1800),
                f"{where}.timeout_seconds",
                minimum=0,
            ),
            on_error=on_error,
            **extra,
        )


@dataclass(frozen=True)
class ProducerSettings(HookSettings):
    """产出 JSONL 的外挂脚本。"""

    manifest: bool = True
    run_on_dry_run: bool = False

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any] | None, *, where: str, **extra: Any
    ) -> "ProducerSettings":
        value = value or {}
        return super().from_mapping(  # type: ignore[return-value]
            value,
            where=where,
            manifest=_as_bool(
                value.get("manifest", True), f"{where}.manifest"
            ),
            run_on_dry_run=_as_bool(
                value.get("run_on_dry_run", False), f"{where}.run_on_dry_run"
            ),
            **extra,
        )


@dataclass(frozen=True)
class BigQuerySettings:
    """一个 load job 需要的全部 BigQuery 参数。"""

    project_id: str
    credentials_path: str
    location: str
    autodetect_schema: bool
    ignore_unknown_values: bool
    max_bad_records: int
    write_disposition: str
    upload_timeout_seconds: float
    job_timeout_seconds: float

    @property
    def client_key(self) -> tuple[str, str, str]:
        """凭据相同的 job 可以共用一个 BigQuery client。"""
        return (self.project_id, self.credentials_path, self.location)


@dataclass(frozen=True)
class SyncJob:
    """展开继承之后的单个同步任务。"""

    project_name: str
    job_name: str
    source_globs: tuple[str, ...]
    target_table: str
    archive_dir: str
    archive_layout: str
    root: Path
    date_source: str
    date_pattern: str
    date_format: str
    recursive: bool
    skip_empty_files: bool
    timezone: str
    bigquery: BigQuerySettings
    notification: NotificationSettings
    producer: ProducerSettings
    cleanup: HookSettings
    require_past_date: bool

    @property
    def qualified_name(self) -> str:
        return f"{self.project_name}/{self.job_name}"


@dataclass(frozen=True)
class ProjectSettings:
    name: str
    jobs: tuple[SyncJob, ...]


@dataclass(frozen=True)
class KitSettings:
    mysql: MySQLSettings
    projects: tuple[ProjectSettings, ...]
    sources: tuple[Path, ...] = ()

    def iter_jobs(self) -> Iterator[SyncJob]:
        for project in self.projects:
            yield from project.jobs

    def select_jobs(
        self,
        *,
        projects: Sequence[str] | None = None,
        jobs: Sequence[str] | None = None,
    ) -> tuple[SyncJob, ...]:
        """按项目名 / 任务名筛选；任务名支持 job 或 project/job 两种写法。"""
        wanted_projects = {name.strip() for name in projects or () if name.strip()}
        wanted_jobs = {name.strip() for name in jobs or () if name.strip()}

        known_projects = {project.name for project in self.projects}
        unknown_projects = wanted_projects - known_projects
        if unknown_projects:
            raise ConfigError(
                f"未知项目: {', '.join(sorted(unknown_projects))}"
            )

        known_jobs = {job.job_name for job in self.iter_jobs()} | {
            job.qualified_name for job in self.iter_jobs()
        }
        unknown_jobs = wanted_jobs - known_jobs
        if unknown_jobs:
            raise ConfigError(f"未知任务: {', '.join(sorted(unknown_jobs))}")

        selected = []
        for job in self.iter_jobs():
            if wanted_projects and job.project_name not in wanted_projects:
                continue
            if wanted_jobs and not (
                job.job_name in wanted_jobs
                or job.qualified_name in wanted_jobs
            ):
                continue
            selected.append(job)
        return tuple(selected)


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"解析 YAML 失败: {path}: {exc}") from exc
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ConfigError(f"配置文件顶层必须是映射: {path}")
    return dict(raw)


def _resolve_includes(
    document: Mapping[str, Any], *, base_dir: Path, seen: set[Path]
) -> dict[str, Any]:
    """把 include 指向的文件合并进当前文档，被包含文件的优先级更低。"""
    includes = document.get("include") or ()
    if isinstance(includes, str):
        includes = [includes]

    merged: dict[str, Any] = {"defaults": {}, "projects": []}
    for pattern in includes:
        pattern_path = Path(str(expand_env(pattern)))
        if not pattern_path.is_absolute():
            pattern_path = base_dir / pattern_path
        matches = sorted(globlib.glob(str(pattern_path)))
        if not matches:
            raise ConfigError(f"include 没有匹配到任何文件: {pattern_path}")
        for match in matches:
            child_path = Path(match).resolve()
            if child_path in seen:
                raise ConfigError(f"include 存在循环引用: {child_path}")
            seen.add(child_path)
            child = _resolve_includes(
                _load_yaml(child_path),
                base_dir=child_path.parent,
                seen=seen,
            )
            child.setdefault("_base_dir", str(child_path.parent))
            merged = _merge_documents(merged, child)

    current = dict(document)
    current.pop("include", None)
    current.setdefault("_base_dir", str(base_dir))
    return _merge_documents(merged, current)


def _merge_documents(
    base: Mapping[str, Any], override: Mapping[str, Any]
) -> dict[str, Any]:
    """合并两份配置文档：projects 追加，其余键覆盖。"""
    merged = _merge(
        {key: item for key, item in base.items() if key != "projects"},
        {key: item for key, item in override.items() if key != "projects"},
    )
    base_projects = list(base.get("projects") or ())
    override_projects = list(override.get("projects") or ())
    # 每个 project 记住自己所在文件的目录，用于解析相对路径。
    for entry in override_projects:
        if isinstance(entry, MutableMapping):
            entry.setdefault("_base_dir", override.get("_base_dir", ""))
    for entry in base_projects:
        if isinstance(entry, MutableMapping):
            entry.setdefault("_base_dir", base.get("_base_dir", ""))
    merged["projects"] = base_projects + override_projects
    return merged


def _build_bigquery(resolved: Mapping[str, Any], where: str) -> BigQuerySettings:
    write_disposition = (
        str(resolved["write_disposition"]).strip().upper() or "WRITE_APPEND"
    )
    if write_disposition not in _VALID_WRITE_DISPOSITIONS:
        raise ConfigError(
            f"{where}.write_disposition 仅支持 "
            f"{', '.join(sorted(_VALID_WRITE_DISPOSITIONS))}"
        )
    max_bad_records = int(resolved["max_bad_records"])
    if max_bad_records < 0:
        raise ConfigError(f"{where}.max_bad_records 不能小于 0")
    return BigQuerySettings(
        project_id=str(resolved["project_id"]).strip(),
        credentials_path=str(resolved["credentials_path"]).strip(),
        location=str(resolved["location"]).strip(),
        autodetect_schema=_as_bool(
            resolved["autodetect_schema"], f"{where}.autodetect_schema"
        ),
        ignore_unknown_values=_as_bool(
            resolved["ignore_unknown_values"], f"{where}.ignore_unknown_values"
        ),
        max_bad_records=max_bad_records,
        write_disposition=write_disposition,
        upload_timeout_seconds=_as_float(
            resolved["upload_timeout_seconds"],
            f"{where}.upload_timeout_seconds",
            minimum=0,
        ),
        job_timeout_seconds=_as_float(
            resolved["job_timeout_seconds"],
            f"{where}.job_timeout_seconds",
            minimum=0,
        ),
    )


def _build_job(
    raw_job: Mapping[str, Any],
    *,
    project_name: str,
    inherited: Mapping[str, Any],
    base_dir: Path,
) -> SyncJob:
    job_name = str(raw_job.get("name") or raw_job.get("job_name") or "").strip()
    if not job_name:
        raise ConfigError(f"项目 {project_name} 存在缺少 name 的任务")
    where = f"{project_name}/{job_name}"

    resolved = _merge(inherited, _inheritable(raw_job))

    producer = ProducerSettings.from_mapping(
        resolved.get("producer"), where=f"{where}.producer"
    )
    cleanup = HookSettings.from_mapping(
        resolved.get("cleanup"), where=f"{where}.cleanup"
    )
    require_past_date = _as_bool(
        resolved["require_past_date"], f"{where}.require_past_date"
    )

    raw_globs = raw_job.get("source_globs") or raw_job.get("source_glob") or ()
    if isinstance(raw_globs, str):
        raw_globs = [raw_globs]
    source_globs = tuple(
        str(item).strip() for item in raw_globs if str(item).strip()
    )
    if not source_globs and not (producer.enabled and producer.manifest):
        # 走 manifest 的 job 由 producer 声明产出，不需要再配 glob。
        raise ConfigError(f"{where} 必须配置 source_glob 或 source_globs")

    target_table = str(raw_job.get("target_table", "")).strip()
    if not target_table:
        raise ConfigError(f"{where} 必须配置 target_table")
    if target_table.count(".") not in (1, 2):
        raise ConfigError(
            f"{where}.target_table 需要写成 dataset.table 或 project.dataset.table"
        )

    date_source = str(resolved["date_source"]).strip().lower()
    if date_source not in _VALID_DATE_SOURCES:
        raise ConfigError(
            f"{where}.date_source 仅支持 "
            f"{', '.join(sorted(_VALID_DATE_SOURCES))}"
        )
    date_pattern = str(resolved["date_pattern"])
    if date_source in {"filename", "path"}:
        try:
            compiled = re.compile(date_pattern)
        except re.error as exc:
            raise ConfigError(f"{where}.date_pattern 不是合法正则: {exc}") from exc
        if compiled.groups < 1:
            raise ConfigError(
                f"{where}.date_pattern 必须包含一个捕获日期的分组"
            )

    archive_layout = str(resolved["archive_layout"]).strip().lower()
    if archive_layout not in _VALID_ARCHIVE_LAYOUTS:
        raise ConfigError(
            f"{where}.archive_layout 仅支持 "
            f"{', '.join(sorted(_VALID_ARCHIVE_LAYOUTS))}"
        )

    root_value = str(resolved["root"]).strip()
    root = Path(root_value) if root_value else base_dir
    if not root.is_absolute():
        root = (base_dir / root).resolve()

    bigquery = _build_bigquery(resolved, where)
    if cleanup.enabled:
        # cleanup 在 load 之前执行，靠的是“文件已完整收下”这个前提。放宽 load
        # 的容错就把这个前提拆了：BigQuery 会丢掉坏行仍然报成功，而源端那批行
        # 已经删了，丢掉的部分再也找不回来。
        if bigquery.max_bad_records > 0:
            raise ConfigError(
                f"{where}: 配了 cleanup 就不能同时设 max_bad_records > 0——"
                "源端数据在 load 之前就删了，BigQuery 丢掉的坏行无法补回"
            )
        if bigquery.ignore_unknown_values:
            raise ConfigError(
                f"{where}: 配了 cleanup 就不能同时设 ignore_unknown_values——"
                "源端数据在 load 之前就删了，被忽略的字段无法补回"
            )

    return SyncJob(
        project_name=project_name,
        job_name=job_name,
        source_globs=source_globs,
        target_table=target_table,
        archive_dir=str(resolved["archive_dir"]).strip(),
        archive_layout=archive_layout,
        root=root,
        date_source=date_source,
        date_pattern=date_pattern,
        date_format=str(resolved["date_format"]),
        recursive=_as_bool(resolved["recursive"], f"{where}.recursive"),
        skip_empty_files=_as_bool(
            resolved["skip_empty_files"], f"{where}.skip_empty_files"
        ),
        timezone=str(resolved["timezone"]).strip() or "UTC",
        bigquery=bigquery,
        notification=NotificationSettings.from_mapping(
            resolved.get("notification")
        ),
        producer=producer,
        cleanup=cleanup,
        require_past_date=require_past_date,
    )


def build_settings(
    document: Mapping[str, Any], *, sources: Sequence[Path] = ()
) -> KitSettings:
    """把（已 include 展开的）配置文档转成类型化设置。"""
    document = expand_env(document)
    defaults = _merge(_BUILTIN_DEFAULTS, _inheritable(document.get("defaults") or {}))

    raw_projects = document.get("projects") or ()
    if not raw_projects:
        raise ConfigError("配置中至少需要一个 project")

    projects: list[ProjectSettings] = []
    seen_names: set[str] = set()
    for raw_project in raw_projects:
        if not isinstance(raw_project, Mapping):
            raise ConfigError("projects 的每一项都必须是映射")
        name = str(raw_project.get("name", "")).strip()
        if not name:
            raise ConfigError("project.name 不能为空")
        if name in seen_names:
            raise ConfigError(f"项目名重复: {name}")
        seen_names.add(name)

        base_dir_value = str(raw_project.get("_base_dir") or "").strip()
        base_dir = Path(base_dir_value) if base_dir_value else Path.cwd()
        inherited = _merge(defaults, _inheritable(raw_project))

        raw_jobs = raw_project.get("jobs") or ()
        if not raw_jobs:
            raise ConfigError(f"项目 {name} 至少需要一个 job")
        jobs: list[SyncJob] = []
        seen_jobs: set[str] = set()
        for raw_job in raw_jobs:
            if not isinstance(raw_job, Mapping):
                raise ConfigError(f"项目 {name} 的 jobs 每一项都必须是映射")
            job = _build_job(
                raw_job,
                project_name=name,
                inherited=inherited,
                base_dir=base_dir,
            )
            if job.job_name in seen_jobs:
                raise ConfigError(f"项目 {name} 中任务名重复: {job.job_name}")
            seen_jobs.add(job.job_name)
            jobs.append(job)
        projects.append(ProjectSettings(name=name, jobs=tuple(jobs)))

    return KitSettings(
        mysql=MySQLSettings.from_mapping(document.get("mysql")),
        projects=tuple(projects),
        sources=tuple(sources),
    )


def default_config_paths() -> tuple[Path, ...]:
    candidates = []
    env_path = os.environ.get("BQ_SYNC_KIT_CONFIG", "").strip()
    if env_path:
        candidates.extend(Path(item) for item in env_path.split(os.pathsep) if item)
    candidates.extend(
        [
            Path.cwd() / "config.yaml",
            Path.cwd() / "config.yml",
            Path.home() / ".config" / "bq_sync_kit" / "config.yaml",
            Path("/etc/bq_sync_kit/config.yaml"),
        ]
    )
    return tuple(candidates)


def load_settings(paths: Sequence[str | Path] | None = None) -> KitSettings:
    """从一个或多个 YAML 文件加载配置；后面的文件覆盖前面的。"""
    resolved_paths: list[Path] = []
    if paths:
        for item in paths:
            path = Path(item).expanduser()
            if not path.exists():
                raise ConfigError(f"配置文件不存在: {path}")
            resolved_paths.append(path.resolve())
    else:
        for candidate in default_config_paths():
            candidate = candidate.expanduser()
            if candidate.exists():
                resolved_paths.append(candidate.resolve())
                break
        if not resolved_paths:
            raise ConfigError(
                "未找到配置文件，请用 --config 指定，或设置 BQ_SYNC_KIT_CONFIG"
            )

    document: dict[str, Any] = {"defaults": {}, "projects": []}
    for path in resolved_paths:
        seen = {path}
        current = _resolve_includes(
            _load_yaml(path), base_dir=path.parent, seen=seen
        )
        document = _merge_documents(document, current)
    return build_settings(document, sources=tuple(resolved_paths))
