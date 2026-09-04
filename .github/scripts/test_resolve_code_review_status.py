#!/usr/bin/env python3

import unittest

from resolve_code_review_status import ResolutionError, resolve_status


HEAD_SHA = "a" * 40
OTHER_HEAD_SHA = "d" * 40
BASE_SHA = "b" * 40
OTHER_BASE_SHA = "c" * 40


def pull(number: int, *, head: str = HEAD_SHA, base: str = BASE_SHA, state: str = "open") -> dict:
    return {
        "number": number,
        "state": state,
        "head": {"sha": head},
        "base": {"sha": base},
    }


def status(
    status_id: int,
    *,
    source: str = "local",
    pr_number: int = 123,
    base: str = BASE_SHA,
    state: str = "success",
) -> dict:
    return {
        "id": status_id,
        "state": state,
        "context": f"code-review/source/{source}/pr-{pr_number}/base-{base}",
    }


class ResolveCodeReviewStatusTest(unittest.TestCase):
    def test_accepts_one_matching_source(self) -> None:
        result = resolve_status([pull(123)], [status(1)], head_sha=HEAD_SHA)
        self.assertEqual("success", result.state)

    def test_requires_every_open_pr_with_the_same_head(self) -> None:
        pulls = [pull(123), pull(124, base=OTHER_BASE_SHA)]
        statuses = [status(1, pr_number=123)]
        result = resolve_status(pulls, statuses, head_sha=HEAD_SHA)
        self.assertEqual("pending", result.state)
        self.assertIn("PR #124", result.description)

    def test_accepts_different_sources_for_shared_head(self) -> None:
        pulls = [pull(123), pull(124, base=OTHER_BASE_SHA)]
        statuses = [
            status(1, pr_number=123),
            status(2, source="automated", pr_number=124, base=OTHER_BASE_SHA),
        ]
        result = resolve_status(pulls, statuses, head_sha=HEAD_SHA)
        self.assertEqual("success", result.state)
        self.assertIn("2 open PR contexts", result.description)

    def test_does_not_reuse_a_source_after_base_changes(self) -> None:
        result = resolve_status(
            [pull(123, base=OTHER_BASE_SHA)], [status(1)], head_sha=HEAD_SHA
        )
        self.assertEqual("pending", result.state)

    def test_ignores_closed_prs_and_prs_for_other_heads(self) -> None:
        pulls = [
            pull(123),
            pull(124, state="closed"),
            pull(125, head=OTHER_HEAD_SHA),
        ]
        result = resolve_status(pulls, [status(1)], head_sha=HEAD_SHA)
        self.assertEqual("success", result.state)

    def test_latest_state_wins_within_one_source_context(self) -> None:
        statuses = [status(1), status(2, state="pending")]
        result = resolve_status([pull(123)], statuses, head_sha=HEAD_SHA)
        self.assertEqual("pending", result.state)

    def test_another_success_source_can_satisfy_the_context(self) -> None:
        statuses = [
            status(1),
            status(2, state="pending"),
            status(3, source="skip"),
        ]
        result = resolve_status([pull(123)], statuses, head_sha=HEAD_SHA)
        self.assertEqual("success", result.state)

    def test_ignores_unscoped_legacy_code_review_success(self) -> None:
        statuses = [{"id": 1, "state": "success", "context": "code-review"}]
        result = resolve_status([pull(123)], statuses, head_sha=HEAD_SHA)
        self.assertEqual("pending", result.state)

    def test_returns_pending_without_an_open_pr(self) -> None:
        result = resolve_status([], [], head_sha=HEAD_SHA)
        self.assertEqual("pending", result.state)

    def test_rejects_malformed_api_data(self) -> None:
        with self.assertRaisesRegex(ResolutionError, "base SHA"):
            resolve_status([pull(123, base="short")], [], head_sha=HEAD_SHA)


if __name__ == "__main__":
    unittest.main()
