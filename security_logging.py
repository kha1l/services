from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any


SERVICE_NAME = "servises"
_STRUCTURED_LOG_FIELDS = frozenset({
    "timestamp",
    "stream",
    "level",
    "message",
    "service",
    "version",
    "component",
    "request_id",
    "trace_id",
    "error_type",
})
_REQUEST_ID: ContextVar[str | None] = ContextVar("request_id", default=None)
_TRACE_ID: ContextVar[str | None] = ContextVar("trace_id", default=None)
_COMPONENT: ContextVar[str | None] = ContextVar("component", default=None)
_EMAIL = re.compile(
    r"(?i)[a-z0-9.!#$%&'*+/=?^_\x60{|}~-]+@"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+"
)
_PHONE = re.compile(
    r"(?:\+7|8)[ \t()\-]*[0-9]{3}[ \t()\-]*"
    r"[0-9]{3}[ \t\-]*[0-9]{2}[ \t\-]*[0-9]{2}"
)
_LONG_IDENTIFIER = re.compile(r"(?<![A-Za-z0-9])-?\d{6,}(?![A-Za-z0-9])")
_AUTHORIZATION = re.compile(
    r"(?i)\b(authorization|proxy-authorization)\b\s*[:=]\s*"
    r"(?:bearer\s+)?[^\s,;]+"
)
_QUOTED_CREDENTIAL = re.compile(
    r"""(?ix)
    (?P<prefix>
        ["']?
        (?:authorization|proxy-authorization|cookie|set-cookie|
           access_token|refresh_token|id_token|api_key|token|secret|password)
        ["']?
        \s*[:=]\s*
    )
    (?P<value>
        "(?:\\.|[^"\\])*"
        |
        '(?:\\.|[^'\\])*'
        |
        (?:bearer\s+)?[^\s,;}]+
    )
    """
)
_CREDENTIAL = re.compile(
    r"(?i)\b(authorization|proxy-authorization|cookie|set-cookie|"
    r"access_token|refresh_token|id_token|api_key|token|secret|password)"
    r"\b\s*[:=]\s*[^\s,;]+"
)
_ERROR_TYPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]{0,127}$")


def _redact_quoted_credential(match: re.Match[str]) -> str:
    raw_value = match.group("value")
    quote = raw_value[0] if raw_value.startswith(("'", '"')) else ""
    return f"{match.group('prefix')}{quote}[REDACTED]{quote}"


def pseudonymize(value: Any) -> str | None:
    """Возвращает versioned HMAC или None, если ключ псевдонимизации не задан."""

    normalized = str(value or "").strip()
    key = os.getenv("SECURITY_PSEUDONYM_KEY", "").strip()
    if not normalized or not key:
        return None
    version = os.getenv("SECURITY_PSEUDONYM_KEY_VERSION", "v1").strip() or "v1"
    digest = hmac.new(
        key.encode("utf-8"),
        normalized.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{version}:{digest}"


def safe_identifier(value: Any) -> str:
    """Не позволяет вернуть исходный идентификатор даже без HMAC-ключа."""

    return pseudonymize(value) or "[REDACTED_ID]"


def safe_error_type(value: Any) -> str:
    """Принимает только имя типа ошибки, но не произвольный текст ответа."""

    if isinstance(value, BaseException):
        return type(value).__name__
    normalized = str(value or "").strip()
    if _ERROR_TYPE.fullmatch(normalized):
        return normalized
    return "RedactedError"


def redact_text(value: Any) -> str:
    """Удаляет распространённые секреты и ПДн из диагностического текста."""

    text = str(value or "").replace("\r", " ").replace("\n", " ")
    text = _QUOTED_CREDENTIAL.sub(_redact_quoted_credential, text)
    text = _AUTHORIZATION.sub(
        lambda match: f"{match.group(1)}=[REDACTED]",
        text,
    )
    text = _CREDENTIAL.sub(lambda match: f"{match.group(1)}=[REDACTED]", text)
    text = _EMAIL.sub("[REDACTED_EMAIL]", text)
    text = _PHONE.sub("[REDACTED_PHONE]", text)
    return _LONG_IDENTIFIER.sub(
        lambda match: pseudonymize(match.group(0)) or "[REDACTED_ID]",
        text,
    )


def _correlation_id(kind: str, value: Any) -> str:
    seed = str(value or uuid.uuid4().hex)
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"{SERVICE_NAME}:{kind}:{seed}",
    ).hex


def _render_message(message: Any, args: tuple[Any, ...]) -> str:
    if not args:
        return redact_text(message)
    try:
        rendered = str(message) % args
    except (TypeError, ValueError):
        rendered = " ".join((str(message), *(str(arg) for arg in args)))
    return redact_text(rendered)


def build_structured_log(
    *,
    level: str,
    message: Any,
    args: tuple[Any, ...] = (),
    component: str | None = None,
    request_id: str | None = None,
    trace_id: str | None = None,
    error_type: Any = None,
) -> dict[str, Any]:
    """Build the fixed application-log schema used by Dagster and file logs."""

    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stream": "application",
        "level": str(level).lower(),
        "message": _render_message(message, args),
        "service": SERVICE_NAME,
        "version": os.getenv("APP_VERSION", "development"),
        "component": redact_text(component or _COMPONENT.get() or "unknown_component"),
        "request_id": request_id or _REQUEST_ID.get() or _correlation_id("request", None),
        "trace_id": trace_id or _TRACE_ID.get() or _correlation_id("trace", None),
        "error_type": safe_error_type(error_type) if error_type else None,
    }
    return {field: event[field] for field in _STRUCTURED_LOG_FIELDS}


class SafeStructuredLogger:
    """Redact and structure messages before the target logger emits them."""

    def __init__(
        self,
        delegate: Any,
        *,
        component: str,
        request_id: str,
        trace_id: str,
    ) -> None:
        self._delegate = delegate
        self._component = component
        self._request_id = request_id
        self._trace_id = trace_id

    def _emit(self, level: str, message: Any, *args: Any, **kwargs: Any) -> None:
        event = build_structured_log(
            level=level,
            message=message,
            args=args,
            component=self._component,
            request_id=self._request_id,
            trace_id=self._trace_id,
            error_type=kwargs.get("error_type"),
        )
        serialized = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        method_name = "warning" if level == "warn" else level
        method = getattr(self._delegate, method_name, None)
        if method is None:
            method = getattr(self._delegate, "info")
        method(serialized)

    def debug(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._emit("debug", message, *args, **kwargs)

    def info(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._emit("info", message, *args, **kwargs)

    def warning(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._emit("warning", message, *args, **kwargs)

    def warn(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._emit("warn", message, *args, **kwargs)

    def error(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._emit("error", message, *args, **kwargs)

    def critical(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._emit("critical", message, *args, **kwargs)

    def exception(self, message: Any, *args: Any, **kwargs: Any) -> None:
        self._emit("error", message, *args, **kwargs)


def safe_context_logger(context: Any) -> SafeStructuredLogger:
    """Bind one correlation context for a Dagster op and its child loggers."""

    run_id = getattr(context, "run_id", None)
    op = getattr(context, "op", None)
    component = getattr(op, "name", None) or "unknown_op"
    trace_id = _correlation_id("run", run_id)
    request_id = _correlation_id("op", f"{run_id}:{component}")
    _REQUEST_ID.set(request_id)
    _TRACE_ID.set(trace_id)
    _COMPONENT.set(component)
    return SafeStructuredLogger(
        context.log,
        component=component,
        request_id=request_id,
        trace_id=trace_id,
    )
