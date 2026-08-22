#!/usr/bin/env python3

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from validate_review_pass_comment import ValidationError, validate_comment


HEAD_SHA = "a" * 40
BASE_SHA = "b" * 40
LIVE_BASE_SHA = "c" * 40
VALIDATOR = Path(__file__).with_name("validate_review_pass_comment.py")


def make_comment(**overrides: str) -> str:
    fields = {
        "schema": "doris-repo-review/v1",
        "status": "PASS",
        "pr": "apache/doris#123",
        "commit": HEAD_SHA,
        "base": BASE_SHA,
        "reviewed_at": "2026-08-18T12:01+00:00",
        "reviewer": "doris-committer",
        "model": "gpt-5.6-sol",
        "effort": "xhigh",
        "findings": "{blocker: 0, major: 0, minor: 1, nit: 2}",
        "rounds": "2",
        "converged": "true",
    }
    fields.update(overrides)
    yaml_body = "\n".join(f"{key}: {value}" for key, value in fields.items())
    return f"""<!-- doris-repo-review:v1:begin -->
### Local pipeline review — ✅ PASS

```yaml
{yaml_body}
```

**Notes for maintainers**

_None._
<!-- doris-repo-review:v1:end -->
"""


def validate(comment: str, **overrides: object) -> dict[str, object]:
    arguments = {
        "repository": "apache/doris",
        "pr_number": 123,
        "head_sha": HEAD_SHA,
        "live_base_sha": LIVE_BASE_SHA,
        "base_compare_status": "ahead",
        "reviewed_base_committed_at": "2026-08-17T12:00:00Z",
        "live_base_committed_at": "2026-08-19T11:59:00Z",
        "comment_author": "doris-committer",
        "comment_author_permission": "write",
    }
    arguments.update(overrides)
    return validate_comment(
        comment,
        **arguments,
    )


class ValidateReviewPassCommentTest(unittest.TestCase):
    def test_accepts_allowed_models_at_xhigh_or_higher(self) -> None:
        combinations = (
            ("claude-opus-5", "xhigh"),
            ("claude-opus-5[1m]", "max"),
            ("claude-fable-5", "ultra"),
            ("claude-fable-5[1m]", "xhigh"),
            ("gpt-5.6-sol", "xhigh"),
        )
        for model, effort in combinations:
            with self.subTest(model=model, effort=effort):
                fields = validate(make_comment(model=model, effort=effort))
                self.assertEqual(model, fields["model"])

    def test_rejects_high_effort(self) -> None:
        with self.assertRaisesRegex(ValidationError, "xhigh or higher"):
            validate(make_comment(effort="high"))

    def test_rejects_unlisted_model(self) -> None:
        with self.assertRaisesRegex(ValidationError, "model is not allowed"):
            validate(make_comment(model="gpt-5.6"))

    def test_rejects_a_different_head(self) -> None:
        with self.assertRaisesRegex(ValidationError, "current PR head"):
            validate(make_comment(commit="c" * 40))

    def test_reviewed_at_does_not_expire_the_comment(self) -> None:
        fields = validate(make_comment(reviewed_at="2020-01-01T00:00+00:00"))
        self.assertEqual("2020-01-01T00:00+00:00", fields["reviewed_at"])

    def test_accepts_the_current_base_regardless_of_commit_age(self) -> None:
        fields = validate(
            make_comment(base=LIVE_BASE_SHA),
            base_compare_status="identical",
            reviewed_base_committed_at=None,
            live_base_committed_at=None,
        )
        self.assertEqual(LIVE_BASE_SHA, fields["base"])

    def test_accepts_a_base_exactly_48_hours_behind(self) -> None:
        fields = validate(
            make_comment(),
            reviewed_base_committed_at="2026-08-17T12:00:00Z",
            live_base_committed_at="2026-08-19T12:00:00Z",
        )
        self.assertEqual(BASE_SHA, fields["base"])

    def test_rejects_a_base_more_than_48_hours_behind(self) -> None:
        with self.assertRaisesRegex(ValidationError, "more than 48 hours behind"):
            validate(
                make_comment(),
                reviewed_base_committed_at="2026-08-17T11:59:59Z",
                live_base_committed_at="2026-08-19T12:00:00Z",
            )

    def test_rejects_a_diverged_base(self) -> None:
        with self.assertRaisesRegex(ValidationError, "not an ancestor"):
            validate(make_comment(), base_compare_status="diverged")

    def test_rejects_inconsistent_equal_base_comparison(self) -> None:
        with self.assertRaisesRegex(ValidationError, "compare status identical"):
            validate(make_comment(base=LIVE_BASE_SHA), base_compare_status="ahead")

    def test_rejects_base_commit_times_in_reverse_order(self) -> None:
        with self.assertRaisesRegex(ValidationError, "after the current base"):
            validate(
                make_comment(),
                reviewed_base_committed_at="2026-08-19T12:01:00Z",
                live_base_committed_at="2026-08-19T12:00:00Z",
            )

    def test_rejects_missing_base_commit_times(self) -> None:
        with self.assertRaisesRegex(ValidationError, "commit time is required"):
            validate(make_comment(), reviewed_base_committed_at=None)

    def test_rejects_a_different_reviewer(self) -> None:
        with self.assertRaisesRegex(ValidationError, "comment author"):
            validate(make_comment(reviewer="someone-else"))

    def test_accepts_admin_permission(self) -> None:
        fields = validate(make_comment(), comment_author_permission="admin")
        self.assertEqual("doris-committer", fields["reviewer"])

    def test_rejects_non_write_permissions(self) -> None:
        for permission in ("read", "triage", "none", ""):
            with self.subTest(permission=permission):
                with self.assertRaisesRegex(ValidationError, "write permission"):
                    validate(make_comment(), comment_author_permission=permission)

    def test_rejects_a_different_pr(self) -> None:
        with self.assertRaisesRegex(ValidationError, "different pull request"):
            validate(make_comment(pr="apache/doris#124"))

    def test_rejects_blocker_or_major_findings(self) -> None:
        for findings in (
            "{blocker: 1, major: 0, minor: 0, nit: 0}",
            "{blocker: 0, major: 1, minor: 0, nit: 0}",
        ):
            with self.subTest(findings=findings):
                with self.assertRaisesRegex(ValidationError, "must both be zero"):
                    validate(make_comment(findings=findings))

    def test_rejects_a_non_converged_review(self) -> None:
        with self.assertRaisesRegex(ValidationError, "did not converge"):
            validate(make_comment(converged="false"))

    def test_rejects_rounds_outside_the_pipeline_limit(self) -> None:
        for rounds in ("0", "4"):
            with self.subTest(rounds=rounds):
                with self.assertRaisesRegex(ValidationError, "between 1 and 3"):
                    validate(make_comment(rounds=rounds))

    def test_rejects_duplicate_markers(self) -> None:
        comment = make_comment() + make_comment()
        with self.assertRaisesRegex(ValidationError, "exactly one v1 marker pair"):
            validate(comment)

    def test_rejects_reversed_markers(self) -> None:
        comment = make_comment()
        marked_body = comment.split("<!-- doris-repo-review:v1:begin -->", 1)[1].split(
            "<!-- doris-repo-review:v1:end -->", 1
        )[0]
        reversed_comment = (
            "<!-- doris-repo-review:v1:end -->"
            + marked_body
            + "<!-- doris-repo-review:v1:begin -->"
        )
        with self.assertRaisesRegex(ValidationError, "out of order"):
            validate(reversed_comment)

    def test_extract_base_cli(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as comment_file:
            comment_file.write(make_comment())
            comment_file.flush()
            result = subprocess.run(
                [
                    sys.executable,
                    str(VALIDATOR),
                    "extract-base",
                    "--comment-file",
                    comment_file.name,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        self.assertEqual(BASE_SHA, result.stdout.strip())

    def test_extract_base_cli_rejects_a_non_sha(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as comment_file:
            comment_file.write(make_comment(base="not-a-sha"))
            comment_file.flush()
            result = subprocess.run(
                [
                    sys.executable,
                    str(VALIDATOR),
                    "extract-base",
                    "--comment-file",
                    comment_file.name,
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(1, result.returncode)
        self.assertIn("base is not a full SHA", result.stderr)

    def test_validate_cli_accepts_write_permission(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as comment_file:
            comment_file.write(make_comment())
            comment_file.flush()
            result = subprocess.run(
                [
                    sys.executable,
                    str(VALIDATOR),
                    "validate",
                    "--comment-file",
                    comment_file.name,
                    "--repository",
                    "apache/doris",
                    "--pr-number",
                    "123",
                    "--head-sha",
                    HEAD_SHA,
                    "--live-base-sha",
                    LIVE_BASE_SHA,
                    "--base-compare-status",
                    "ahead",
                    "--reviewed-base-committed-at",
                    "2026-08-17T12:00:00Z",
                    "--live-base-committed-at",
                    "2026-08-19T11:59:00Z",
                    "--comment-author",
                    "doris-committer",
                    "--comment-author-permission",
                    "write",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        self.assertIn("VALID: local pipeline review passed", result.stdout)


if __name__ == "__main__":
    unittest.main()
