#!/usr/bin/env python3
"""Resolve the SHA-wide code-review status from PR/base-specific sources."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
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
    target_url: str = ""


def list_reviews(helper, repository, number):
    command = [sys.executable, helper, "api", "--paginate", "--slurp",
               f"repos/{repository}/pulls/{number}/reviews?per_page=100"]
    pages = json.loads(subprocess.check_output(command, text=True))
    return [review for page in pages for review in page]


def resolve_status(
    pulls: list[object], statuses: list[object], *, head_sha: str, repo_url: str = "", reviews_for_pr=None
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

    # Join two independent records. A dismissal alone can never satisfy a PR.
    dismissed = set()
    for context, item in latest_by_context.items():
        match = re.fullmatch(r"code-review/dismissal/pr-([1-9][0-9]*)/review-([1-9][0-9]*)", context)
        if match and item["state"] == "success" and repo_url:
            number, review_id = match.groups()
            url = f"{repo_url}/pull/{number}#pullrequestreview-{review_id}"
            if item.get("target_url") == url and (item.get("creator") or {}).get("login") == "github-actions[bot]":
                dismissed.add((int(number), url))

    accepted_dismissals = {}
    approved_contexts: set[tuple[int, str]] = set()
    blocked_contexts: set[tuple[int, str]] = set()
    incomplete_contexts: set[tuple[int, str]] = set()
    for context, item in latest_by_context.items():
        match = SOURCE_CONTEXT_RE.fullmatch(context)
        if match is not None and item["state"] == "success":
            approved_contexts.add((int(match.group(1)), match.group(2).casefold()))
        elif (match is not None and item["state"] == "failure"
              and context.startswith("code-review/source/automated/")):
            key = (int(match.group(1)), match.group(2).casefold())
            url = item.get("target_url")
            description = item.get("description") or ""
            if (key in open_contexts and (key[0], url) in dismissed
                    and re.fullmatch(rf"Review completed for PR #{key[0]}: P0=0, P1=[1-9][0-9]*\.", description)
                    and (item.get("creator") or {}).get("login") == "github-actions[bot]"):
                accepted_dismissals[key] = url
                continue
            blocked_contexts.add(key)
            if isinstance(description, str) and description.startswith("Review did not converge after "):
                incomplete_contexts.add(key)

    # Preserve the authorized local PASS / skip override. Otherwise a completed
    # automated review with P0/P1 or incomplete coverage blocks instead of pending.
    accepted_dismissals = {key: url for key, url in accepted_dismissals.items() if key not in approved_contexts}
    approved_contexts.update(accepted_dismissals)
    blocked = sorted((open_contexts - approved_contexts) & blocked_contexts)
    if blocked:
        number, base_sha = blocked[0]
        if (number, base_sha) in incomplete_contexts:
            return Resolution(
                "failure", f"Review did not converge for PR #{number} at {base_sha[:12]}.",
            )
        return Resolution(
            "failure", f"Review completed with P0/P1 for PR #{number} at {base_sha[:12]}.",
        )

    # Another submitted AI review of this same head may still request changes.
    for key, url in sorted(accepted_dismissals.items()):
        number = key[0]
        if reviews_for_pr is None:
            raise ResolutionError("dismissal requires current GitHub review states")
        matching_dismissal = False
        for review in reviews_for_pr(number):
            if (review["commit_id"] != head_sha or review["user"]["login"] != "github-actions[bot]"
                    or "<!-- code-review-completion:" not in (review.get("body") or "")):
                continue
            review_url = f"{repo_url}/pull/{number}#pullrequestreview-{int(review['id'])}"
            if review["state"] == "CHANGES_REQUESTED":
                return Resolution("failure", f"An active AI review still requests changes for PR #{number}.", review_url)
            if review_url == url and review["state"] == "DISMISSED":
                matching_dismissal = True
        if not matching_dismissal:
            return Resolution("failure", f"Could not verify the dismissed AI review for PR #{number}.", url)

    missing = sorted(open_contexts - approved_contexts)
    if missing:
        number, base_sha = missing[0]
        remaining = len(missing) - 1
        suffix = f" (+{remaining} more)" if remaining else ""
        return Resolution(
            "pending",
            f"Awaiting code review for PR #{number} at {base_sha[:12]}{suffix}.",
        )

    if accepted_dismissals:
        key = min(accepted_dismissals)
        number, url = key[0], accepted_dismissals[key]
        return Resolution("success", f"Code review passed after manual P1 dismissal for PR #{number}.", url)

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
    parser.add_argument("--repo-url", default="")
    parser.add_argument("--repository")
    parser.add_argument("--github-api-helper")
    args = parser.parse_args()

    resolution = resolve_status(
        load_list(args.pulls_file, "pulls"),
        load_list(args.statuses_file, "statuses"),
        head_sha=args.head_sha, repo_url=args.repo_url,
        reviews_for_pr=lambda number: list_reviews(args.github_api_helper, args.repository, number),
    )
    print(f"state={resolution.state}")
    print(f"description={resolution.description}")
    print(f"target_url={resolution.target_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
