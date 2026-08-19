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
MAX_BASE_LAG = dt.timedelta(hours=48)

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
ALLOWED_AUTHOR_PERMISSIONS = frozenset({"write", "admin"})
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
SHA_RE = re.compile(r"[0-9a-fA-F]{40}")


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
    live_base_sha: str,
    base_compare_status: str,
    reviewed_base_committed_at: str | None,
    live_base_committed_at: str | None,
    comment_author: str,
    comment_author_permission: str,
) -> dict[str, object]:
    fields = parse_comment(comment)

    if fields["schema"] != SCHEMA:
        raise ValidationError(f"unsupported schema: {fields['schema']}")
    if fields["status"] != "PASS":
        raise ValidationError("status is not PASS")
    if str(fields["pr"]).casefold() != f"{repository}#{pr_number}".casefold():
        raise ValidationError("comment targets a different pull request")

    reviewed_commit = str(fields["commit"])
    require_full_sha("commit", reviewed_commit)
    if reviewed_commit.casefold() != head_sha.casefold():
        raise ValidationError("reviewed commit does not match the current PR head")
    reviewed_base_sha = str(fields["base"])
    require_full_sha("base", reviewed_base_sha)
    require_full_sha("live base", live_base_sha)

    same_base = reviewed_base_sha.casefold() == live_base_sha.casefold()
    if same_base:
        if base_compare_status != "identical":
            raise ValidationError("equal base SHAs must have compare status identical")
    else:
        if base_compare_status != "ahead":
            raise ValidationError("reviewed base is not an ancestor of the current PR base")
        reviewed_base_time = parse_timestamp(
            "reviewed base commit time", reviewed_base_committed_at
        )
        live_base_time = parse_timestamp("current base commit time", live_base_committed_at)
        base_lag = live_base_time.astimezone(dt.timezone.utc) - reviewed_base_time.astimezone(
            dt.timezone.utc
        )
        if base_lag < dt.timedelta(0):
            raise ValidationError("reviewed base commit time is after the current base commit time")
        if base_lag > MAX_BASE_LAG:
            raise ValidationError("reviewed base is more than 48 hours behind the current PR base")

    if str(fields["reviewer"]).casefold() != comment_author.casefold():
        raise ValidationError("reviewer does not match the GitHub comment author")
    if comment_author_permission not in ALLOWED_AUTHOR_PERMISSIONS:
        raise ValidationError("comment author does not have write permission")
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

    parse_timestamp("reviewed_at", str(fields["reviewed_at"]))

    return fields


def parse_timestamp(name: str, value: str | None) -> dt.datetime:
    if value is None:
        raise ValidationError(f"{name} is required")
    try:
        timestamp = dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(f"{name} is not a valid ISO-8601 timestamp") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValidationError(f"{name} must include a timezone offset")
    return timestamp


def require_full_sha(name: str, value: str) -> None:
    if SHA_RE.fullmatch(value) is None:
        raise ValidationError(f"{name} is not a full SHA")


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser("extract-base")
    extract_parser.add_argument("--comment-file", type=Path, required=True)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--comment-file", type=Path, required=True)
    validate_parser.add_argument("--repository", required=True)
    validate_parser.add_argument("--pr-number", type=int, required=True)
    validate_parser.add_argument("--head-sha", required=True)
    validate_parser.add_argument("--live-base-sha", required=True)
    validate_parser.add_argument("--base-compare-status", required=True)
    validate_parser.add_argument("--reviewed-base-committed-at")
    validate_parser.add_argument("--live-base-committed-at")
    validate_parser.add_argument("--comment-author", required=True)
    validate_parser.add_argument("--comment-author-permission", required=True)
    args = parser.parse_args()

    try:
        comment = args.comment_file.read_text(encoding="utf-8")
        if args.command == "extract-base":
            fields = parse_comment(comment)
            if fields["schema"] != SCHEMA:
                raise ValidationError(f"unsupported schema: {fields['schema']}")
            reviewed_base_sha = str(fields["base"])
            require_full_sha("base", reviewed_base_sha)
            print(reviewed_base_sha)
            return 0
        fields = validate_comment(
            comment,
            repository=args.repository,
            pr_number=args.pr_number,
            head_sha=args.head_sha,
            live_base_sha=args.live_base_sha,
            base_compare_status=args.base_compare_status,
            reviewed_base_committed_at=args.reviewed_base_committed_at,
            live_base_committed_at=args.live_base_committed_at,
            comment_author=args.comment_author,
            comment_author_permission=args.comment_author_permission,
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
