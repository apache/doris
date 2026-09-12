#!/usr/bin/env python3
"""Resolve the SHA-wide code-review status from PR/base-specific sources."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path


SOURCE_CONTEXT_RE = re.compile(
    r"code-review/source/(?:automated|local|skip)/pr-(\d+)/base-([0-9a-fA-F]{40})"
)
SHA_RE = re.compile(r"[0-9a-fA-F]{40}")


class ResolutionError(ValueError):
    """The supplied GitHub API data cannot be resolved safely."""


@dataclass(frozen=True)
class Resolution:
    state: str
    description: str


def resolve_status(
    pulls: list[object], statuses: list[object], *, head_sha: str
) -> Resolution:
    if SHA_RE.fullmatch(head_sha) is None:
        raise ResolutionError("head SHA is not a full SHA")

    open_contexts: set[tuple[int, str]] = set()
    for item in pulls:
        if not isinstance(item, dict) or item.get("state") != "open":
            continue
        head = item.get("head")
        base = item.get("base")
        if not isinstance(head, dict) or not isinstance(base, dict):
            raise ResolutionError("pull request is missing head or base information")
        pull_head = head.get("sha")
        base_sha = base.get("sha")
        number = item.get("number")
        if not isinstance(pull_head, str) or not isinstance(base_sha, str):
            raise ResolutionError("pull request head or base SHA is invalid")
        if pull_head.casefold() != head_sha.casefold():
            continue
        if not isinstance(number, int) or SHA_RE.fullmatch(base_sha) is None:
            raise ResolutionError("pull request number or base SHA is invalid")
        open_contexts.add((number, base_sha.casefold()))

    if not open_contexts:
        return Resolution(
            "pending", f"No open pull request currently uses {head_sha[:12]}."
        )

    latest_by_context: dict[str, dict[str, object]] = {}
    for item in statuses:
        if not isinstance(item, dict):
            raise ResolutionError("commit status entry is invalid")
        context = item.get("context")
        status_id = item.get("id")
        state = item.get("state")
        if not isinstance(context, str) or not isinstance(status_id, int):
            raise ResolutionError("commit status context or id is invalid")
        if not isinstance(state, str):
            raise ResolutionError("commit status state is invalid")
        previous = latest_by_context.get(context)
        if previous is None or status_id > int(previous["id"]):
            latest_by_context[context] = item

    approved_contexts: set[tuple[int, str]] = set()
    for context, item in latest_by_context.items():
        match = SOURCE_CONTEXT_RE.fullmatch(context)
        if match is not None and item["state"] == "success":
            approved_contexts.add((int(match.group(1)), match.group(2).casefold()))

    missing = sorted(open_contexts - approved_contexts)
    if missing:
        number, base_sha = missing[0]
        remaining = len(missing) - 1
        suffix = f" (+{remaining} more)" if remaining else ""
        return Resolution(
            "pending",
            f"Awaiting code review for PR #{number} at {base_sha[:12]}{suffix}.",
        )

    count = len(open_contexts)
    noun = "context" if count == 1 else "contexts"
    return Resolution(
        "success",
        f"Code review passed for {count} open PR {noun} on {head_sha[:12]}.",
    )


def load_list(path: Path, name: str) -> list[object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list):
        raise ResolutionError(f"{name} JSON must be an array")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pulls-file", type=Path, required=True)
    parser.add_argument("--statuses-file", type=Path, required=True)
    parser.add_argument("--head-sha", required=True)
    args = parser.parse_args()

    resolution = resolve_status(
        load_list(args.pulls_file, "pulls"),
        load_list(args.statuses_file, "statuses"),
        head_sha=args.head_sha,
    )
    print(f"state={resolution.state}")
    print(f"description={resolution.description}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
