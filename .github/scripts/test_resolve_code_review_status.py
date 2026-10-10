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

    def test_shared_head_recovers_after_another_pr_moves(self) -> None:
        statuses = [status(1, pr_number=123)]
        before = resolve_status(
            [pull(123), pull(124, base=OTHER_BASE_SHA)],
            statuses,
            head_sha=HEAD_SHA,
        )
        after = resolve_status(
            [pull(123), pull(124, head=OTHER_HEAD_SHA, base=OTHER_BASE_SHA)],
            statuses,
            head_sha=HEAD_SHA,
        )
        self.assertEqual("pending", before.state)
        self.assertEqual("success", after.state)

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

    def test_completed_blocking_review_is_failure(self) -> None:
        result = resolve_status([pull(123)], [status(1, source="automated", state="failure")], head_sha=HEAD_SHA)
        self.assertEqual("failure", result.state)
        self.assertIn("P0/P1", result.description)

    def test_local_or_skip_success_can_override_blocking_review(self) -> None:
        for source in ("local", "skip"):
            with self.subTest(source=source):
                result = resolve_status([pull(123)], [status(1, source="automated", state="failure"),
                    status(2, source=source)], head_sha=HEAD_SHA)
                self.assertEqual("success", result.state)

    def test_stale_blocking_result_is_not_reused(self) -> None:
        for pulls in ([pull(123, base=OTHER_BASE_SHA)], [pull(124)], [pull(123, head=OTHER_HEAD_SHA)]):
            with self.subTest(pulls=pulls):
                result = resolve_status(pulls, [status(1, source="automated", state="failure")], head_sha=HEAD_SHA)
                self.assertEqual("pending", result.state)

    def test_latest_automated_result_replaces_blocker(self) -> None:
        result = resolve_status([pull(123)], [status(1, source="automated", state="failure"),
            status(2, source="automated")], head_sha=HEAD_SHA)
        self.assertEqual("success", result.state)

    def test_shared_head_blocker_takes_precedence_over_pending(self) -> None:
        result = resolve_status([pull(123), pull(124)], [status(1, source="automated", state="failure")], head_sha=HEAD_SHA)
        self.assertEqual("failure", result.state)

    def test_manual_pass_survives_reruns_and_later_aggregate_writes(self) -> None:
        manual = {"id": 2, "state": "success",
                  "context": f"code-review/source/manual/pr-123/head-{HEAD_SHA}"}
        statuses = [status(1, source="automated", state="failure"), manual]
        for state in ("pending", "failure", "success"):
            statuses.extend([
                status(len(statuses) + 1, source="automated", state=state),
                {"id": len(statuses) + 2, "state": state, "context": "code-review"},
            ])
            result = resolve_status([pull(123)], statuses, head_sha=HEAD_SHA)
            self.assertEqual("success", result.state)

    def test_manual_pass_survives_base_movement(self) -> None:
        manual = {"id": 1, "state": "success",
                  "context": f"code-review/source/manual/pr-123/head-{HEAD_SHA}"}
        result = resolve_status([pull(123, base=OTHER_BASE_SHA)], [manual], head_sha=HEAD_SHA)
        self.assertEqual("success", result.state)

    def test_manual_pass_never_follows_a_new_head(self) -> None:
        manual = {"id": 1, "state": "success",
                  "context": f"code-review/source/manual/pr-123/head-{HEAD_SHA}"}
        # Even if old-head evidence is accidentally supplied, its explicit head
        # must not authorize the new commit.
        result = resolve_status([pull(123, head=OTHER_HEAD_SHA)], [manual], head_sha=OTHER_HEAD_SHA)
        self.assertEqual("pending", result.state)

    def test_manual_pass_does_not_authorize_another_pr_on_same_head(self) -> None:
        manual = {"id": 1, "state": "success",
                  "context": f"code-review/source/manual/pr-123/head-{HEAD_SHA}"}
        for other_state, expected in (("pending", "pending"), ("failure", "failure"),
                                      ("success", "success")):
            with self.subTest(other_state=other_state):
                statuses = [manual, status(2, source="automated", pr_number=124, state=other_state)]
                result = resolve_status([pull(123), pull(124)], statuses, head_sha=HEAD_SHA)
                self.assertEqual(expected, result.state)
                if expected != "success":
                    self.assertIn("PR #124", result.description)
        result = resolve_status([pull(124)], [manual], head_sha=HEAD_SHA)
        self.assertEqual("pending", result.state)

    def test_latest_manual_source_state_wins(self) -> None:
        context = f"code-review/source/manual/pr-123/head-{HEAD_SHA}"
        statuses = [{"id": 2, "state": "failure", "context": context},
                    {"id": 1, "state": "success", "context": context}]
        result = resolve_status([pull(123)], statuses, head_sha=HEAD_SHA)
        self.assertEqual("pending", result.state)

    def test_legacy_force_pass_cannot_authorize_a_pr(self) -> None:
        legacy = {"id": 1, "state": "success", "context": "code-review",
                  "description": "force pass", "creator": {"type": "User", "login": "maintainer"}}
        result = resolve_status([pull(123)], [legacy], head_sha=HEAD_SHA)
        self.assertEqual("pending", result.state)

    def test_no_open_pr_does_not_overwrite_final_status(self) -> None:
        result = resolve_status([pull(123, state="closed")], [], head_sha=HEAD_SHA)
        self.assertFalse(result.publish)
        result = resolve_status([pull(123)], [], head_sha=HEAD_SHA)
        self.assertTrue(result.publish)

    def test_rejects_malformed_api_data(self) -> None:
        with self.assertRaisesRegex(ResolutionError, "base SHA"):
            resolve_status([pull(123, base="short")], [], head_sha=HEAD_SHA)


if __name__ == "__main__":
    unittest.main()
