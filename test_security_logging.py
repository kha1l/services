from __future__ import annotations

import json
import logging
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from logger import _SensitiveDataFilter, _StructuredJsonFormatter
from security_logging import (
    SafeStructuredLogger,
    build_structured_log,
    pseudonymize,
    redact_text,
    safe_context_logger,
    safe_error_type,
    safe_identifier,
)

try:
    from dagster import job, op
except ModuleNotFoundError:
    job = None
    op = None


class SecurityLoggingTests(unittest.TestCase):
    def test_structured_log_uses_allowlist_and_correlation_fields(self) -> None:
        event = build_structured_log(
            level="WARNING",
            message="unit=%s Authorization: Bearer %s",
            args=("987654321", "synthetic-secret"),
            component="synthetic_op",
            request_id="request-1",
            trace_id="trace-1",
        )

        self.assertEqual(
            set(event),
            {
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
            },
        )
        self.assertNotIn("987654321", event["message"])
        self.assertNotIn("synthetic-secret", event["message"])

    def test_safe_context_emits_redacted_json_and_binds_child_loggers(self) -> None:
        messages: list[str] = []
        delegate = SimpleNamespace(
            info=messages.append,
            warning=messages.append,
            error=messages.append,
        )
        context = SimpleNamespace(
            run_id="synthetic-run",
            op=SimpleNamespace(name="synthetic_op"),
            log=delegate,
        )
        safe_context_logger(context).warning(
            "email=%s phone=%s",
            "synthetic.employee@example.test",
            "+7 (999) 123-45-67",
        )

        event = json.loads(messages[0])
        self.assertEqual(event["component"], "synthetic_op")
        self.assertRegex(event["request_id"], r"^[0-9a-f]{32}$")
        self.assertRegex(event["trace_id"], r"^[0-9a-f]{32}$")
        self.assertNotIn("synthetic.employee@example.test", messages[0])
        self.assertNotIn("+7 (999) 123-45-67", messages[0])

        record = logging.LogRecord(
            name="synthetic.child",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="child message",
            args=(),
            exc_info=None,
        )
        child_event = json.loads(_StructuredJsonFormatter().format(record))
        self.assertEqual(child_event["request_id"], event["request_id"])
        self.assertEqual(child_event["trace_id"], event["trace_id"])

    def test_structured_logger_drops_unapproved_keyword_fields(self) -> None:
        messages: list[str] = []
        logger = SafeStructuredLogger(
            SimpleNamespace(info=messages.append),
            component="synthetic_op",
            request_id="request-1",
            trace_id="trace-1",
        )
        logger.info(
            "completed",
            raw_payload="must-not-survive",
            access_token="synthetic-secret",
        )

        self.assertNotIn("raw_payload", messages[0])
        self.assertNotIn("must-not-survive", messages[0])
        self.assertNotIn("access_token", messages[0])
        self.assertNotIn("synthetic-secret", messages[0])

    @unittest.skipIf(op is None, "dagster is not installed")
    def test_safe_logger_works_with_real_dagster_op(self) -> None:
        @op
        def synthetic_security_logging_op(context) -> None:
            log = safe_context_logger(context)
            self.assertEqual(log._component, "synthetic_security_logging_op")
            log.info("synthetic dagster message")

        @job
        def synthetic_security_logging_job() -> None:
            synthetic_security_logging_op()

        result = synthetic_security_logging_job.execute_in_process()
        self.assertTrue(result.success)

    def test_pseudonym_is_hmac_and_versioned(self) -> None:
        raw_identifier = "synthetic-credential-42"
        with patch.dict(
            os.environ,
            {
                "SECURITY_PSEUDONYM_KEY": "synthetic-test-key",
                "SECURITY_PSEUDONYM_KEY_VERSION": "test-v1",
            },
            clear=False,
        ):
            pseudonym = pseudonymize(raw_identifier)

        self.assertRegex(pseudonym or "", r"^test-v1:[0-9a-f]{64}$")
        self.assertNotIn(raw_identifier, pseudonym or "")

    def test_identifier_is_omitted_without_key(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(pseudonymize("synthetic-credential-42"))
            self.assertEqual(safe_identifier("synthetic-credential-42"), "[REDACTED_ID]")

    def test_last_error_contract_accepts_only_error_type(self) -> None:
        self.assertEqual(safe_error_type("TimeoutError"), "TimeoutError")
        self.assertEqual(safe_error_type(ConnectionError("synthetic body")), "ConnectionError")
        self.assertEqual(
            safe_error_type("502 Synthetic Employee synthetic.employee@example.test"),
            "RedactedError",
        )

    def test_redaction_removes_sensitive_synthetic_values(self) -> None:
        source = json.dumps(
            {
                "email": "synthetic.employee@example.test",
                "phone": "+7 (999) 123-45-67",
                "chat_id": 987654321,
                "Authorization": "Bearer synthetic-secret",
            }
        )
        with patch.dict(os.environ, {}, clear=True):
            redacted = redact_text(source)

        self.assertNotIn("synthetic.employee@example.test", redacted)
        self.assertNotIn("+7 (999) 123-45-67", redacted)
        self.assertNotIn("987654321", redacted)
        self.assertNotIn("synthetic-secret", redacted)

    def test_file_logger_filter_redacts_rendered_message(self) -> None:
        record = logging.LogRecord(
            name="synthetic",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="email=%s chat_id=%s Authorization: Bearer synthetic-secret",
            args=("synthetic.employee@example.test", 987654321),
            exc_info=None,
        )
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(_SensitiveDataFilter().filter(record))
            rendered = record.getMessage()

        self.assertNotIn("synthetic.employee@example.test", rendered)
        self.assertNotIn("987654321", rendered)
        self.assertNotIn("synthetic-secret", rendered)


if __name__ == "__main__":
    unittest.main()
