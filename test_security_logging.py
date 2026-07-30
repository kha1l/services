from __future__ import annotations

import json
import logging
import os
import unittest
from unittest.mock import patch

from logger import _SensitiveDataFilter
from security_logging import (
    pseudonymize,
    redact_text,
    safe_error_type,
    safe_identifier,
)


class SecurityLoggingTests(unittest.TestCase):
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
