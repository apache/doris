#!/usr/bin/env python3
"""Validate an explicit, authorized manual code-review acceptance."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


COMMAND_RE = re.compile(r"/force-pass code-review ([0-9a-fA-F]{40})")


def validate(event: dict, pull: dict, permission: dict) -> str:
    """Return the accepted head, or reject without publishing any status."""
    if event.get("action") != "created":
        raise ValueError("only newly created comments can authorize a manual pass")
    comment = event["comment"]
    author = comment["user"]
    if (author["type"] != "User"
            or author["login"] != event["sender"]["login"]):
        raise ValueError("manual pass must be created by its human author")
    if permission.get("permission") not in {"write", "admin"}:
        raise ValueError("manual pass requires repository write permission")
    issue = event["issue"]
    if not issue.get("pull_request") or issue["number"] != pull["number"]:
        raise ValueError("comment and pull request do not match")
    if pull["state"] != "open":
        raise ValueError("manual pass requires an open pull request")
    match = COMMAND_RE.fullmatch(comment["body"].strip())
    if match is None:
        raise ValueError("expected /force-pass code-review <full 40-character head SHA>")
    head = match.group(1).lower()
    if head != pull["head"]["sha"].lower():
        raise ValueError("manual pass commit is no longer the current PR head")
    return head


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-file", type=Path, required=True)
    parser.add_argument("--pull-file", type=Path, required=True)
    parser.add_argument("--permission-file", type=Path, required=True)
    args = parser.parse_args()
    print(validate(*(json.loads(path.read_text(encoding="utf-8")) for path in (
        args.event_file, args.pull_file, args.permission_file,
    ))))


if __name__ == "__main__":
    main()
