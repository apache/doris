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
import codecs
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

from review_github_api import decode_json, record_failure, redact, run_gh


SUBMISSION_FILE = "review-submission.json"
REJECTION_FILE = "review-submission-rejected.json"
API_ERRORS_FILE = "review-api-errors.jsonl"
DRAFT_FILE = "review-draft.json"
RUN_FILE = "review-run.json"
RESULT_FILE = "review-result.json"
PRIORITY = re.compile(r"^\[P([0-3])\]\s+\S")


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


def api(path, remaining, *, payload=None, paginated=False, method=None):
    command = ["gh", "api", path]
    if paginated:
        command += ["--paginate", "--slurp"]
    method = method or ("POST" if payload is not None else "GET")
    if method != "GET":
        command += ["--method", method]
    if payload is not None:
        command += ["--input", "-"]
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
    # GraphQL can return HTTP 200 with an errors array. Never count that as a
    # successful mutation, even if gh changes its exit-code handling.
    if path == "graphql" and value.get("errors"):
        error = ValueError("GraphQL errors")
        record_failure(command, error, result.stdout, result.stderr, diagnostic=result.github_response)
        raise GitHubAPIError(path, method, error, result.stdout, result.stderr)
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


THREAD_MUTATION = """mutation($input: AddPullRequestReviewThreadInput!) {
  addPullRequestReviewThread(input: $input) {
    thread { id path line diffSide }
  }
}"""


def diff_path(header):
    value = header[4:]
    if value.startswith('"'):
        # Git quotes UTF-8 bytes with C escapes (including octal), not JSON.
        value = codecs.escape_decode(value[1:-1].encode())[0].decode()
    else:
        value = value.split("\t", 1)[0]
    return None if value == "/dev/null" else value[2:]


def inline_locations(context, comments):
    """Map the existing position schema using the authoritative local PR diff."""
    wanted = {(c["path"], c["position"]) for c in comments}
    locations = {}
    old_path = new_path = position = None
    old_line = new_line = 0
    with (context / "pr.diff").open() as handle:
        for raw in handle:
            line = raw.rstrip("\n")
            if line.startswith("diff --git "):
                old_path = new_path = position = None
            elif position is None and line.startswith("--- "):
                old_path = diff_path(line)
            elif position is None and line.startswith("+++ "):
                new_path = diff_path(line)
            elif line.startswith("@@ "):
                match = re.match(r"@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
                if match is None:
                    raise ValueError("Invalid hunk in authoritative PR diff")
                old_line, new_line = map(int, match.groups())
                # The first hunk header is position zero; subsequent headers
                # consume a position, as do no-newline markers (not anchors).
                position = 0 if position is None else position + 1
            elif position is not None:
                position += 1
                key = (new_path or old_path, position)
                if line.startswith((" ", "+", "-")):
                    if key in wanted:
                        locations[key] = {"line": old_line if line[0] == "-" else new_line,
                                          "side": "LEFT" if line[0] == "-" else "RIGHT"}
                    old_line += line[0] != "+"
                    new_line += line[0] != "-"
    if set(locations) != wanted:
        missing = sorted(wanted - set(locations))
        raise ValueError(f"Finding positions are not commentable PR diff lines: {missing}")
    return [locations[(c["path"], c["position"])] for c in comments]


def another_bot_review(reviews, run):
    return any(r.get("user", {}).get("login") == "github-actions[bot]"
               and r.get("commit_id") == run["head_sha"]
               and (r.get("submitted_at") or "") >= run["started_at"] for r in reviews)


def owned_draft(review, payload):
    return (review.get("state") == "PENDING"
            and review.get("user", {}).get("login") == "github-actions[bot]"
            and review.get("commit_id") == payload["commit_id"]
            and review.get("body") == payload["body"])


def verify_inline_comments(root, review, payload, run, remaining, *, pending=False):
    # Replies may arrive during wrap-up. They are not declared findings.
    comments = [c for c in api(f"{root}/reviews/{review['id']}/comments", remaining, paginated=True)
                if c.get("in_reply_to_id") is None]
    expected = Counter((c["path"], c["position"], c["body"]) for c in payload["comments"])
    actual = Counter((c["path"], c.get("original_position") if pending else c.get("position"), c["body"])
                     for c in comments)
    if actual != expected or any(
        c.get("user", {}).get("login") != "github-actions[bot]"
        or c.get("commit_id") != run["head_sha"] for c in comments
    ):
        raise ValueError("Final review inline comments were not completely verified")


def submit_draft(context, run, root, payload, locations, remaining):
    """Create one draft, add one thread per request, verify, then publish once.

    The exclusive SUBMISSION_FILE remains the no-repost guard. A partial draft
    is not completion and is never resumed by another helper invocation.
    """
    progress = {"stage": "creating", "review_id": None, "thread_ids": []}

    def record(stage, **fields):
        progress.update(stage=stage, **fields)
        write_json(context / DRAFT_FILE, progress)

    record("creating")
    publish_attempted = False
    try:
        draft = api(f"{root}/reviews", remaining, payload={
            "commit_id": payload["commit_id"], "body": payload["body"], "comments": [],
        })
        if not owned_draft(draft, payload):
            raise ValueError("Created review is not this invocation's pending draft")
        record("created", review_id=draft["id"])
        for index, (comment, location) in enumerate(zip(payload["comments"], locations)):
            check_target(run, remaining)
            record("adding", comment_index=index, path=comment["path"], **location)
            result = api("graphql", remaining, payload={
                "query": THREAD_MUTATION, "variables": {"input": {
                    "pullRequestReviewId": draft["node_id"], "path": comment["path"],
                    "body": comment["body"], **location,
                }},
            })
            thread = result["data"]["addPullRequestReviewThread"]["thread"]
            if (thread["path"] != comment["path"] or thread["line"] != location["line"]
                    or thread["diffSide"] != location["side"]):
                raise ValueError("Added review thread location did not match")
            progress["thread_ids"].append(thread["id"])
            record("added")
        draft = api(f"{root}/reviews/{draft['id']}", remaining)
        if not owned_draft(draft, payload):
            raise ValueError("Pending review author, head, state or summary did not match")
        verify_inline_comments(root, draft, payload, run, remaining, pending=True)
        if another_bot_review(api(f"{root}/reviews", remaining, paginated=True), run):
            raise ValueError("Another bot review was submitted during this run; refusing duplicate submission")
        check_target(run, remaining)
        record("submitting")
        publish_attempted = True
        api(f"{root}/reviews/{draft['id']}/events", remaining,
            payload={"event": payload["event"], "body": payload["body"]})
        record("submitted")
    except Exception:
        # Before publishing, deleting only our exactly identified pending draft
        # prevents an add failure from leaving a draft that blocks future runs.
        # After a publish attempt, its outcome can be ambiguous: leave it alone
        # and let the existing exact final-review readback recover delivery.
        if not publish_attempted:
            try:
                drafts = [r for r in api(f"{root}/reviews", remaining, paginated=True)
                          if owned_draft(r, payload)]
                if len(drafts) > 1:
                    raise ValueError("Cannot uniquely identify this invocation's pending draft")
                if drafts:
                    current = api(f"{root}/reviews/{drafts[0]['id']}", remaining)
                    if not owned_draft(current, payload):
                        raise ValueError("Pending draft changed before cleanup")
                    api(f"{root}/reviews/{current['id']}", remaining, method="DELETE")
                    record("deleted", review_id=current["id"])
            except Exception as cleanup_error:
                # Keep the original create/add failure as the primary error.
                progress.update(stage="cleanup_failed", cleanup_error=diagnostic_text(str(cleanup_error)))
                try:
                    write_json(context / DRAFT_FILE, progress)
                except OSError:
                    pass  # Report below; do not replace the original API failure.
                print(f"Pending review cleanup needs follow-up: {progress}", file=sys.stderr)
        raise


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
        if another_bot_review(reviews, run):
            reason = "Another bot review was submitted during this run; refusing duplicate submission"
            # This is a rejected candidate, not a POST intent or proof of delivery.
            # Keep it separate so completion still requires exact GitHub readback.
            write_json(context / REJECTION_FILE, {"reason": reason, "submission": submission})
            raise ValueError(reason)
        if path.exists():
            raise ValueError(with_api_failure(
                context, "Final submission was attempted but cannot be verified; refusing to repost"
            ))
        # Resolve every anchor before creating any GitHub state. The existing
        # submission schema stays compatible with review agents and old runs.
        locations = inline_locations(context, payload["comments"]) if payload["comments"] else []
        # Persist the complete intent before POST: if the response is lost, the
        # runner can read back this exact review without repeating the write.
        # Exclusive creation also prevents two accidental simultaneous helper
        # invocations from both posting after their initial read found no review.
        with path.open("x") as handle:
            json.dump(submission, handle)
        if payload["comments"]:
            submit_draft(context, run, root, payload, locations, remaining)
        else:
            # A summary-only review does not ask GitHub to parse any diff.
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
    verify_inline_comments(root, review, payload, run, remaining)
    # Recheck after the API reads so a moving PR cannot pass an obsolete review.
    check_target(run, remaining)
    return {
        "state": "failure" if counts[0] + counts[1] or not submission["converged"] else "success",
        "converged": submission["converged"], "rounds": submission["rounds"],
        "incomplete_reason": submission["incomplete_reason"],
        "p0": counts[0], "p1": counts[1], "review_id": review["id"],
        "review_url": review["html_url"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context-dir", type=Path, required=True)
    parser.add_argument("--input-file", type=Path, required=True)
    args = parser.parse_args()
    run = json.loads((args.context_dir / RUN_FILE).read_text())

    def remaining():
        seconds = run["deadline"] - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("Review's shared time budget was exhausted")
        return seconds

    result = verify_completion(
        args.context_dir, run, remaining, submit=True,
        submission=json.loads(args.input_file.read_text()),
    )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
