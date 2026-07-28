from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory


sys.path.insert(0, str(Path(__file__).parent))
import codex_auth_state as auth_state  # noqa: E402


AUTH_1 = "oss://doris-community-ci/codex/auth.json.1"
AUTH_2 = "oss://doris-community-ci/codex/auth.json.2"
AUTH_3 = "oss://doris-community-ci/codex/auth.json.3"
AUTH_OBJECTS = [AUTH_1, AUTH_2, AUTH_3]
NOW = datetime(2026, 7, 27, 10, 0, tzinfo=timezone.utc)


class CodexAuthStateTest(unittest.TestCase):
    def test_selection_rotates_accounts(self) -> None:
        state = auth_state.default_state()

        first = auth_state.select_auth_object(state, AUTH_OBJECTS, NOW)
        second = auth_state.select_auth_object(state, AUTH_OBJECTS, NOW)
        third = auth_state.select_auth_object(state, AUTH_OBJECTS, NOW)

        self.assertEqual(AUTH_1, first.auth_object)
        self.assertEqual(AUTH_2, second.auth_object)
        self.assertEqual(AUTH_3, third.auth_object)

    def test_selection_keeps_cursor_when_the_input_order_changes(self) -> None:
        state = auth_state.default_state()

        first = auth_state.select_auth_object(state, AUTH_OBJECTS, NOW)
        second = auth_state.select_auth_object(state, [AUTH_3, AUTH_1, AUTH_2], NOW)

        self.assertEqual(AUTH_1, first.auth_object)
        self.assertEqual(AUTH_2, second.auth_object)

    def test_rate_limit_skips_account_for_one_hour_then_recovers(self) -> None:
        state = auth_state.default_state()
        auth_state.record_result(state, AUTH_1, auth_state.RATE_LIMITED, NOW)

        during_cooldown = auth_state.select_auth_object(state, AUTH_OBJECTS, NOW + timedelta(minutes=30))
        after_cooldown = auth_state.select_auth_object(state, AUTH_OBJECTS, NOW + timedelta(hours=1))

        self.assertEqual(AUTH_2, during_cooldown.auth_object)
        self.assertEqual(AUTH_3, after_cooldown.auth_object)
        self.assertEqual(auth_state.AVAILABLE, auth_state.account_state(state, AUTH_1)["status"])

    def test_quota_exhaustion_retries_after_one_day_and_alerts_for_all_accounts(self) -> None:
        state = auth_state.default_state()
        for auth_object in AUTH_OBJECTS:
            auth_state.record_result(state, auth_object, auth_state.QUOTA_EXHAUSTED, NOW)

        unavailable = auth_state.select_auth_object(state, AUTH_OBJECTS, NOW + timedelta(hours=23))
        available_again = auth_state.select_auth_object(state, AUTH_OBJECTS, NOW + timedelta(days=1))

        self.assertIsNone(unavailable.auth_object)
        self.assertTrue(unavailable.all_quota_exhausted)
        self.assertEqual(AUTH_1, available_again.auth_object)

    def test_success_clears_the_retry_marker(self) -> None:
        state = auth_state.default_state()
        auth_state.record_result(state, AUTH_1, auth_state.QUOTA_EXHAUSTED, NOW)
        auth_state.record_result(state, AUTH_1, auth_state.AVAILABLE, NOW + timedelta(minutes=1))

        entry = auth_state.account_state(state, AUTH_1)
        self.assertEqual(auth_state.AVAILABLE, entry["status"])
        self.assertIsNone(entry["retry_after"])
        self.assertEqual("2026-07-27T10:01:00Z", entry["last_success_at"])

    def test_authentication_failure_switches_accounts_for_one_day(self) -> None:
        state = auth_state.default_state()
        auth_state.record_result(state, AUTH_1, auth_state.AUTHENTICATION_FAILED, NOW, 401)

        during_cooldown = auth_state.select_auth_object(state, AUTH_OBJECTS, NOW + timedelta(hours=23))
        after_cooldown = auth_state.select_auth_object(state, AUTH_OBJECTS, NOW + timedelta(days=1))

        self.assertEqual(AUTH_2, during_cooldown.auth_object)
        self.assertEqual(AUTH_3, after_cooldown.auth_object)

    def test_initialize_creates_state_file_for_all_accounts(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            state_file = Path(temporary_directory) / "auth-status.json"
            state = auth_state.load_state(state_file)
            for auth_object in AUTH_OBJECTS:
                auth_state.account_state(state, auth_object)
            auth_state.save_state(state_file, state)

            created = auth_state.load_state(state_file)
            self.assertEqual(set(AUTH_OBJECTS), set(created["accounts"]))

    def test_classify_failure_uses_status_and_network_rules(self) -> None:
        self.assertEqual(auth_state.RATE_LIMITED, auth_state.classify_failure("request failed: HTTP 429"))
        self.assertEqual(auth_state.QUOTA_EXHAUSTED, auth_state.classify_failure("request failed: HTTP 403"))
        self.assertEqual(
            auth_state.QUOTA_EXHAUSTED,
            auth_state.classify_failure("HTTP status 403 for request 123"),
        )
        self.assertEqual(auth_state.AUTHENTICATION_FAILED, auth_state.classify_failure("request failed: HTTP 401"))
        self.assertEqual(auth_state.TRANSIENT_FAILURE, auth_state.classify_failure("request failed: HTTP 503"))
        self.assertEqual(auth_state.TRANSIENT_FAILURE, auth_state.classify_failure("connection reset by peer"))
        self.assertEqual("fatal", auth_state.classify_failure("request failed: HTTP 422"))
        self.assertEqual("fatal", auth_state.classify_failure("reviewed output mentioned 403"))

    def test_classification_uses_only_the_terminal_event(self) -> None:
        events = "\n".join(
            (
                '{"type":"item.completed","item":{"aggregated_output":"HTTP 403"}}',
                '{"type":"turn.failed","error":{"message":"request failed: HTTP 422"}}',
            )
        )

        classification = auth_state.classify_terminal_failure(events, "")

        self.assertEqual("fatal", classification.kind)
        self.assertEqual(422, classification.http_status)

    def test_classification_uses_the_last_stderr_line_only_as_a_fallback(self) -> None:
        events = '{"type":"item.completed","item":{"aggregated_output":"HTTP 403"}}'
        stderr = "HTTP 403 in an earlier diagnostic\nrequest failed: HTTP 503"

        classification = auth_state.classify_terminal_failure(events, stderr)

        self.assertEqual(auth_state.TRANSIENT_FAILURE, classification.kind)
        self.assertEqual(503, classification.http_status)

    def test_usage_limit_message_preserves_its_reset_time(self) -> None:
        message = "You've hit your usage limit for this period; try again at Aug 2nd, 2026 1:27 AM."
        classification = auth_state.classify_terminal_failure(
            '{"type":"turn.failed","error":{"message":"' + message + '"}}', ""
        )
        state = auth_state.default_state()

        auth_state.record_result(
            state,
            AUTH_1,
            classification.kind,
            NOW,
            classification.http_status,
            auth_state.parse_timestamp(classification.retry_after),
        )

        self.assertEqual(auth_state.QUOTA_EXHAUSTED, classification.kind)
        self.assertEqual("2026-08-02T01:27:00Z", classification.retry_after)
        self.assertEqual("2026-08-02T01:27:00Z", auth_state.account_state(state, AUTH_1)["retry_after"])


if __name__ == "__main__":
    unittest.main()
