#!/usr/bin/env python3
"""Validate a doris-repo-review PASS comment for the current PR head."""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path


BEGIN_MARKER = "<!-- doris-repo-review:v1:begin -->"
END_MARKER = "<!-- doris-repo-review:v1:end -->"
SCHEMA = "doris-repo-review/v1"
MAX_AGE = dt.timedelta(hours=48)
MAX_CLOCK_SKEW = dt.timedelta(minutes=5)

ALLOWED_MODELS = frozenset(
    {
        "claude-opus-5",
        "claude-opus-5[1m]",
        "claude-fable-5",
        "claude-fable-5[1m]",
        "gpt-5.6-sol",
    }
)
ALLOWED_EFFORTS = frozenset({"xhigh", "max", "ultra"})
EXPECTED_FIELDS = (
    "schema",
    "status",
    "pr",
    "commit",
    "base",
    "reviewed_at",
    "reviewer",
    "model",
    "effort",
    "findings",
    "rounds",
    "converged",
)
FINDINGS_RE = re.compile(
    r"\{blocker: (\d+), major: (\d+), minor: (\d+), nit: (\d+)\}"
)


class ValidationError(ValueError):
    """The comment is not eligible to satisfy the code-review check."""


def parse_comment(comment: str) -> dict[str, object]:
    if comment.count(BEGIN_MARKER) != 1 or comment.count(END_MARKER) != 1:
        raise ValidationError("expected exactly one v1 marker pair")
    if comment.index(BEGIN_MARKER) > comment.index(END_MARKER):
        raise ValidationError("v1 markers are out of order")

    marked_body = comment.split(BEGIN_MARKER, 1)[1].split(END_MARKER, 1)[0]
    yaml_blocks = re.findall(r"```yaml\n(.*?)\n```", marked_body, flags=re.DOTALL)
    if len(yaml_blocks) != 1:
        raise ValidationError("expected exactly one fenced yaml block")

    fields: dict[str, object] = {}
    field_order: list[str] = []
    for line in yaml_blocks[0].splitlines():
        if ":" not in line:
            raise ValidationError(f"malformed yaml field: {line!r}")
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key in fields:
            raise ValidationError(f"duplicate field: {key}")
        field_order.append(key)
        fields[key] = value

    if tuple(field_order) != EXPECTED_FIELDS:
        raise ValidationError("comment fields do not match the v1 schema")

    findings_match = FINDINGS_RE.fullmatch(str(fields["findings"]))
    if findings_match is None:
        raise ValidationError("findings must use the v1 inline-map format")
    fields["findings"] = tuple(int(value) for value in findings_match.groups())

    try:
        fields["rounds"] = int(str(fields["rounds"]))
    except ValueError as exc:
        raise ValidationError("rounds must be an integer") from exc

    return fields


def validate_comment(
    comment: str,
    *,
    repository: str,
    pr_number: int,
    head_sha: str,
    comment_author: str,
    now: dt.datetime | None = None,
) -> dict[str, object]:
    fields = parse_comment(comment)

    if fields["schema"] != SCHEMA:
        raise ValidationError(f"unsupported schema: {fields['schema']}")
    if fields["status"] != "PASS":
        raise ValidationError("status is not PASS")
    if str(fields["pr"]).casefold() != f"{repository}#{pr_number}".casefold():
        raise ValidationError("comment targets a different pull request")

    reviewed_commit = str(fields["commit"])
    if re.fullmatch(r"[0-9a-fA-F]{40}", reviewed_commit) is None:
        raise ValidationError("commit is not a full SHA")
    if reviewed_commit.casefold() != head_sha.casefold():
        raise ValidationError("reviewed commit does not match the current PR head")
    if re.fullmatch(r"[0-9a-fA-F]{40}", str(fields["base"])) is None:
        raise ValidationError("base is not a full SHA")

    if str(fields["reviewer"]).casefold() != comment_author.casefold():
        raise ValidationError("reviewer does not match the GitHub comment author")
    if fields["model"] not in ALLOWED_MODELS:
        raise ValidationError(f"model is not allowed: {fields['model']}")
    if fields["effort"] not in ALLOWED_EFFORTS:
        raise ValidationError(f"effort must be xhigh or higher: {fields['effort']}")

    blocker, major, _minor, _nit = fields["findings"]
    if blocker != 0 or major != 0:
        raise ValidationError("Blocker and Major findings must both be zero")
    if fields["converged"] != "true":
        raise ValidationError("review did not converge")
    if not 1 <= int(fields["rounds"]) <= 3:
        raise ValidationError("rounds must be between 1 and 3")

    try:
        reviewed_at = dt.datetime.fromisoformat(str(fields["reviewed_at"]))
    except ValueError as exc:
        raise ValidationError("reviewed_at is not a valid ISO-8601 timestamp") from exc
    if reviewed_at.tzinfo is None or reviewed_at.utcoffset() is None:
        raise ValidationError("reviewed_at must include a timezone offset")

    current_time = now or dt.datetime.now(dt.timezone.utc)
    if current_time.tzinfo is None or current_time.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    age = current_time.astimezone(dt.timezone.utc) - reviewed_at.astimezone(dt.timezone.utc)
    if age < -MAX_CLOCK_SKEW:
        raise ValidationError("reviewed_at is too far in the future")
    if age > MAX_AGE:
        raise ValidationError("review is older than 48 hours")

    return fields


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--comment-file", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--pr-number", type=int, required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--comment-author", required=True)
    parser.add_argument("--now", help="ISO-8601 time override for deterministic checks")
    args = parser.parse_args()

    try:
        now = dt.datetime.fromisoformat(args.now) if args.now else None
        fields = validate_comment(
            args.comment_file.read_text(encoding="utf-8"),
            repository=args.repository,
            pr_number=args.pr_number,
            head_sha=args.head_sha,
            comment_author=args.comment_author,
            now=now,
        )
    except (OSError, ValidationError, ValueError) as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1

    print(
        "VALID: local pipeline review passed "
        f"with {fields['model']} at effort {fields['effort']} for {fields['commit']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
