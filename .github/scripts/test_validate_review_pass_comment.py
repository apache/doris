#!/usr/bin/env python3

import datetime as dt
import unittest

from validate_review_pass_comment import ValidationError, validate_comment


HEAD_SHA = "a" * 40
BASE_SHA = "b" * 40
NOW = dt.datetime(2026, 8, 19, 12, 0, tzinfo=dt.timezone.utc)


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


def validate(comment: str) -> dict[str, object]:
    return validate_comment(
        comment,
        repository="apache/doris",
        pr_number=123,
        head_sha=HEAD_SHA,
        comment_author="doris-committer",
        now=NOW,
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

    def test_rejects_an_expired_review(self) -> None:
        with self.assertRaisesRegex(ValidationError, "older than 48 hours"):
            validate(make_comment(reviewed_at="2026-08-17T11:59+00:00"))

    def test_rejects_a_future_review(self) -> None:
        with self.assertRaisesRegex(ValidationError, "in the future"):
            validate(make_comment(reviewed_at="2026-08-19T12:06+00:00"))

    def test_rejects_a_different_reviewer(self) -> None:
        with self.assertRaisesRegex(ValidationError, "comment author"):
            validate(make_comment(reviewer="someone-else"))

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


if __name__ == "__main__":
    unittest.main()
