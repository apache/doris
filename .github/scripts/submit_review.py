#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Submit and verify a final review bound to this runner invocation."""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from review_github_api import decode_json, redact, run_gh


SUBMISSION_FILE = "review-submission.json"
REJECTION_FILE = "review-submission-rejected.json"
API_ERRORS_FILE = "review-api-errors.jsonl"
RUN_FILE = "review-run.json"
RESULT_FILE = "review-result.json"
PRIORITY = re.compile(r"^\[P([0-3])\]\s+\S")
# Advisory signals for accidentally capturing the remainder of the review ledger.
# Legitimate comments may be long or quote these markers, so neither is a ban.
LONG_COMMENT_CHARS = 4000
PREVIEW_CHARS = 240
LEDGER_MARKER = re.compile(
    r"(?m)^(?:- ID:|## (?:Main Merged Findings|Dismissed Or Duplicate Points|"
    r"Proposed Final Comment Set|Convergence Rounds))(?:[ \t]|$)"
)


def write_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def priority(body):
    match = PRIORITY.match(body)
    if match is None:
        raise ValueError("Every finding must start with [P0], [P1], [P2] or [P3]")
    return int(match.group(1))


def validate_submission(value):
    if not isinstance(value, dict) or set(value) != {
        "body", "comments", "existing_blocking_comment_ids",
        "converged", "rounds", "incomplete_reason"
    }:
        raise ValueError(
            "Final review requires body, comments, existing_blocking_comment_ids, "
            "converged, rounds and incomplete_reason"
        )
    if type(value["converged"]) is not bool:
        raise ValueError("converged must be a boolean")
    if type(value["rounds"]) is not int or not 1 <= value["rounds"] <= 3:
        raise ValueError("rounds must be an integer from 1 to 3")
    reason = value["incomplete_reason"]
    if not isinstance(reason, str):
        raise ValueError("incomplete_reason must be text")
    if value["converged"]:
        if reason.strip():
            raise ValueError("A converged review cannot have an incomplete_reason")
    elif value["rounds"] != 3 or not reason.strip():
        raise ValueError("A non-converged review requires three rounds and an incomplete_reason")
    if not isinstance(value["body"], str) or not value["body"].strip():
        raise ValueError("Final review summary is empty")
    if not isinstance(value["comments"], list):
        raise ValueError("comments must be an array")
    for comment in value["comments"]:
        if not isinstance(comment, dict) or set(comment) != {"path", "position", "body"}:
            raise ValueError("Each finding requires path, position and body")
        if not isinstance(comment["path"], str) or not comment["path"]:
            raise ValueError("Finding path is empty")
        if type(comment["position"]) is not int or comment["position"] <= 0:
            raise ValueError("Finding position must be a positive diff position")
        if not isinstance(comment["body"], str):
            raise ValueError("Finding body must be text")
        priority(comment["body"])
    ids = value["existing_blocking_comment_ids"]
    if not isinstance(ids, list) or any(type(i) is not int or i <= 0 for i in ids):
        raise ValueError("Existing blocking comment IDs must be positive integers")
    if len(set(ids)) != len(ids):
        raise ValueError("Existing blocking comment IDs must be unique")
    return value


def check_submission(submission):
    """Inspect the actual payload locally without freezing or publishing it."""
    validate_submission(submission)
    comments = []
    for index, comment in enumerate(submission["comments"], start=1):
        body = comment["body"]
        warnings = []
        if len(body) > LONG_COMMENT_CHARS:
            warnings.append("Long comment: inspect the full body for unintended extracted text.")
        if LEDGER_MARKER.search(body):
            warnings.append("Review ledger marker: check for other findings or internal sections.")
        comments.append({
            "index": index, "path": comment["path"], "position": comment["position"],
            "characters": len(body), "lines": len(body.splitlines()),
            "preview_start": body[:PREVIEW_CHARS],
            "preview_end": body[-PREVIEW_CHARS:] if len(body) > PREVIEW_CHARS else "",
            "preview_truncated": len(body) > 2 * PREVIEW_CHARS,
            "warnings": warnings,
        })
    return {
        "check_only": True, "schema_valid": True,
        "note": "Local diagnostics only; no GitHub checks or submission performed. "
                "Warnings are advisory. Inspect the complete input before submitting.",
        "comments": comments,
    }


def diagnostic_text(value):
    value = redact(value)
    return value[:65536] + ("\n[truncated after 65536 characters]" if len(value) > 65536 else "")


class GitHubAPIError(ValueError):
    def __init__(self, path, method, error, stdout=None, stderr=None):
        self.details = {
            "method": method, "path": path, "error_type": type(error).__name__,
            "returncode": getattr(error, "returncode", None),
            "timeout_seconds": getattr(error, "timeout", None),
            "stdout": diagnostic_text(stdout), "stderr": diagnostic_text(stderr),
        }
        message = (
            f"GitHub API {method} {path} failed ({type(error).__name__}, "
            f"exit={self.details['returncode']}, timeout={self.details['timeout_seconds']}); "
            f"stderr={json.dumps(self.details['stderr'][:1000])}; "
            f"stdout={json.dumps(self.details['stdout'][:1000])}"
        )
        self.details["message"] = message
        super().__init__(message)


def api(path, remaining, *, payload=None, paginated=False):
    command = ["gh", "api", path]
    if paginated:
        command += ["--paginate", "--slurp"]
    if payload is not None:
        command += ["--method", "POST", "--input", "-"]
    method = "POST" if payload is not None else "GET"
    try:
        result = run_gh(
            command, input=json.dumps(payload) if payload is not None else None,
            capture_output=True, text=True, check=True, timeout=min(30, remaining()),
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise GitHubAPIError(path, method, error, error.stdout, error.stderr) from None
    try:
        value = decode_json(result, command)
    except json.JSONDecodeError as error:
        raise GitHubAPIError(path, method, error, result.stdout, result.stderr) from None
    return [item for page in value for item in page] if paginated else value


def with_api_failure(context, reason):
    diagnostics = context / API_ERRORS_FILE
    if diagnostics.exists():
        with diagnostics.open() as handle:
            first = json.loads(next(handle))
        reason += f"; first recorded API failure: {first['message']} (see {API_ERRORS_FILE})"
    return reason


def check_target(run, remaining):
    root = f"repos/{run['repository']}/pulls/{run['pr_number']}"
    pr = api(root, remaining)
    if (pr["state"] != "open" or pr["head"]["sha"] != run["head_sha"]
            or pr["base"]["sha"] != run["base_sha"]):
        raise ValueError("PR base/head or open state changed; final review is stale")
    return root


def review_payload(submission, run, root, remaining):
    counts = [0, 0, 0, 0]
    for comment in submission["comments"]:
        counts[priority(comment["body"])] += 1
    # Previously reported findings are not posted again. The reviewer must
    # explicitly reconfirm which existing P0/P1 findings still apply to this head.
    ids = submission["existing_blocking_comment_ids"]
    if ids:
        comments = {c["id"]: c for c in api(f"{root}/comments", remaining, paginated=True)}
        for comment_id in ids:
            comment = comments.get(comment_id)
            if comment is None or comment.get("in_reply_to_id") is not None:
                raise ValueError(f"Existing blocking finding {comment_id} is missing or a reply")
            # Older reviews may bold the whole heading or only the priority.
            # Normalize existing comments only; new findings keep their strict format.
            body = re.sub(r"^\*\*(\[P[0-3]\])(?:\*\*)?", r"\1", comment["body"], count=1)
            level = priority(body)
            if level > 1:
                raise ValueError(f"Existing finding {comment_id} is not P0/P1")
            counts[level] += 1
    # The marker binds the entire declared final submission to this invocation,
    # including the deduplicated findings. A parallel run cannot satisfy it.
    digest = hashlib.sha256(json.dumps(submission, sort_keys=True).encode()).hexdigest()
    marker = f"<!-- code-review-completion:{run['token']}:{digest} -->"
    body = submission["body"]
    # Render the declared convergence state ourselves and bind it to the same
    # digest/readback as the findings. A delivered review can still be incomplete.
    if submission["converged"]:
        body += f"\n\nReview converged after {submission['rounds']} round(s)."
    else:
        body += (
            "\n\n**Review did not converge after 3 rounds. This automated review cannot pass the review check.**"
            f"\n\n{submission['incomplete_reason']}"
        )
    if ids:
        body += "\n\nExisting P0/P1 findings confirmed for this head: " + ", ".join(
            f"https://github.com/{run['repository']}/pull/{run['pr_number']}#discussion_r{i}"
            for i in ids
        )
    body += f"\n\n{marker}"
    return {
        "commit_id": run["head_sha"],
        "event": "REQUEST_CHANGES" if counts[0] + counts[1] else "COMMENT",
        "body": body,
        "comments": submission["comments"],
    }, counts, marker


def verify_completion(context, run, remaining, *, submit=False, submission=None):
    try:
        return _verify_completion(context, run, remaining, submit=submit, submission=submission)
    except GitHubAPIError as error:
        diagnostic = dict(error.details, recorded_at=datetime.now(timezone.utc).isoformat(),
                          repository=run["repository"], pr_number=run["pr_number"],
                          head_sha=run["head_sha"], base_sha=run["base_sha"])
        # Append rather than replace: later readback failures must not erase the
        # original POST response. Neither this log nor the intent proves delivery.
        with (context / API_ERRORS_FILE).open("a") as handle:
            handle.write(json.dumps(diagnostic) + "\n")
        print(f"Review API diagnostic: {json.dumps(diagnostic)}", file=sys.stderr, flush=True)
        raise


def _verify_completion(context, run, remaining, *, submit=False, submission=None):
    """Never infer completion from an unowned review or a local result file."""
    path = context / SUBMISSION_FILE
    if submission is None:
        if not path.exists():
            rejection = context / REJECTION_FILE
            if rejection.exists():
                raise ValueError(json.loads(rejection.read_text())["reason"])
            raise ValueError("No final review submission was declared")
        submission = json.loads(path.read_text())
    submission = validate_submission(submission)
    if path.exists() and json.loads(path.read_text()) != submission:
        raise ValueError("Final submission already declared; refusing a different review")
    root = check_target(run, remaining)
    payload, counts, marker = review_payload(submission, run, root, remaining)
    reviews = api(f"{root}/reviews", remaining, paginated=True)
    matching = [r for r in reviews if marker in (r.get("body") or "")]
    if not matching and submit:
        if any(r.get("user", {}).get("login") == "github-actions[bot]"
               and r.get("commit_id") == run["head_sha"]
               and (r.get("submitted_at") or "") >= run["started_at"] for r in reviews):
            reason = "Another bot review was submitted during this run; refusing duplicate submission"
            # This is a rejected candidate, not a POST intent or proof of delivery.
            # Keep it separate so completion still requires exact GitHub readback.
            write_json(context / REJECTION_FILE, {"reason": reason, "submission": submission})
            raise ValueError(reason)
        if path.exists():
            raise ValueError(with_api_failure(
                context, "Final submission was attempted but cannot be verified; refusing to repost"
            ))
        # Persist the complete intent before POST: if the response is lost, the
        # runner can read back this exact review without repeating the write.
        # Exclusive creation also prevents two accidental simultaneous helper
        # invocations from both posting after their initial read found no review.
        with path.open("x") as handle:
            json.dump(submission, handle)
        api(f"{root}/reviews", remaining, payload=payload)
        matching = [r for r in api(f"{root}/reviews", remaining, paginated=True)
                    if marker in (r.get("body") or "")]
    if len(matching) != 1:
        raise ValueError(with_api_failure(
            context, "Could not verify exactly one final review from this invocation"
        ))
    review = matching[0]
    expected_state = "CHANGES_REQUESTED" if payload["event"] == "REQUEST_CHANGES" else "COMMENTED"
    if (review.get("user", {}).get("login") != "github-actions[bot]"
            or review.get("commit_id") != run["head_sha"]
            or (review.get("submitted_at") or "") < run["started_at"]
            or review.get("state") != expected_state
            or review.get("body") != payload["body"]):
        raise ValueError("Final review author, head, timestamp, state or summary did not match")
    # Replies may arrive while the model wraps up. They are not part of the
    # declared inline set and must not invalidate an otherwise complete review.
    comments = [c for c in api(f"{root}/reviews/{review['id']}/comments", remaining, paginated=True)
                if c.get("in_reply_to_id") is None]
    expected = Counter((c["path"], c["position"], c["body"]) for c in payload["comments"])
    actual = Counter((c["path"], c.get("position"), c["body"]) for c in comments)
    if actual != expected or any(
        c.get("user", {}).get("login") != "github-actions[bot]"
        or c.get("commit_id") != run["head_sha"] for c in comments
    ):
        raise ValueError("Final review inline comments were not completely verified")
    # Recheck after the API reads so a moving PR cannot pass an obsolete review.
    check_target(run, remaining)
    return {
        "state": "failure" if counts[0] + counts[1] or not submission["converged"] else "success",
        "converged": submission["converged"], "rounds": submission["rounds"],
        "incomplete_reason": submission["incomplete_reason"],
        "p0": counts[0], "p1": counts[1], "review_id": review["id"],
        "review_url": review["html_url"],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context-dir", type=Path, required=True)
    parser.add_argument("--input-file", type=Path, required=True)
    parser.add_argument("--check-only", action="store_true",
                        help="Validate and preview the input locally without submitting or writing state")
    args = parser.parse_args(argv)
    submission = json.loads(args.input_file.read_text())
    if args.check_only:
        print(json.dumps(check_submission(submission), indent=2))
        return
    run = json.loads((args.context_dir / RUN_FILE).read_text())

    def remaining():
        seconds = run["deadline"] - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("Review's shared time budget was exhausted")
        return seconds

    result = verify_completion(
        args.context_dir, run, remaining, submit=True,
        submission=submission,
    )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
