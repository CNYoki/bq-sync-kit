# -*- coding: utf-8 -*-
"""可配置的 HTTP 失败消息推送。"""

from __future__ import annotations

import logging
from typing import Any

from bq_sync_kit.config import NotificationSettings

logger = logging.getLogger(__name__)


def render_template(value: Any, *, title: str, content: str) -> Any:
    replacements = {
        "{title}": title,
        "{tittle}": title,  # 兼容常见拼写错误
        "{content}": content,
    }
    if isinstance(value, str):
        for placeholder, replacement in replacements.items():
            value = value.replace(placeholder, replacement)
        return value
    if isinstance(value, dict):
        return {
            render_template(key, title=title, content=content): render_template(
                item, title=title, content=content
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        rendered = [
            render_template(item, title=title, content=content) for item in value
        ]
        return tuple(rendered) if isinstance(value, tuple) else rendered
    return value


class FailureNotifier:
    def __init__(
        self,
        settings: NotificationSettings,
        client: Any | None = None,
    ):
        self.settings = settings
        self._client = client

    async def send(self, *, title: str, content: str) -> bool:
        if not self.settings.enabled:
            return False

        import httpx

        params = render_template(
            dict(self.settings.params or {}), title=title, content=content
        )
        headers = render_template(
            dict(self.settings.headers or {}), title=title, content=content
        )
        request_kwargs: dict[str, Any] = {"headers": headers}
        if self.settings.parameter_format == "json":
            request_kwargs["json"] = params
        elif self.settings.parameter_format == "form":
            request_kwargs["data"] = params
        else:
            request_kwargs["params"] = params

        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(
            timeout=self.settings.timeout_seconds
        )
        try:
            response = await client.request(
                self.settings.method, self.settings.url, **request_kwargs
            )
            response.raise_for_status()
            return True
        finally:
            if owns_client:
                await client.aclose()
