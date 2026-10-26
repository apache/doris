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

import copy
import io
import itertools
import json
import re
import subprocess
import tempfile
import threading
import unittest
from contextlib import redirect_stderr
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import run_review_with_resume as runner
import submit_review as submitter
from resolve_code_review_status import resolve_status


class FinalReviewTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.context = Path(temp.name)
        self.run = {
            "repository": "apache/doris", "pr_number": "123", "head_sha": "a" * 40,
            "base_sha": "b" * 40, "token": "unique-run", "started_at": "2026-09-29T00:00:00Z",
        }
        self.root = "repos/apache/doris/pulls/123"
        self.pr = {"state": "open", "number": 123,
                   "head": {"sha": self.run["head_sha"]}, "base": {"sha": self.run["base_sha"]}}
        self.reviews = []
        self.comments = []
        self.existing = []
        self.posts = []
        self.thread_inputs = []
        self.final_events = []
        self.deletes = []
        self.diff = ("diff --git a/file.py b/file.py\nnew file mode 100644\n"
                     "--- /dev/null\n+++ b/file.py\n@@ -0,0 +1,20 @@\n" + "+line\n" * 20)
        (self.context / "pr.diff").write_text(self.diff)
        self.real_api = submitter.api
        self.api = self.enterContext(mock.patch.object(submitter, "api", side_effect=self.fake_api))
        self.remaining = lambda: 60

    def submission(self, priority=1, *, converged=True, rounds=1):
        return {"body": "All checkpoints reviewed.", "comments": [] if priority is None else [
            {"path": "file.py", "position": 12, "body": f"[P{priority}] Fix the verified issue"}
        ], "existing_blocking_comment_ids": [], "converged": converged, "rounds": rounds,
            "incomplete_reason": "" if converged else "Round 3 found another candidate; coverage is not settled."}

    def fake_api(self, path, remaining, *, payload=None, paginated=False, method=None):
        if path == self.root:
            return copy.deepcopy(self.pr)
        if path == self.root + "/reviews":
            if payload is None:
                return copy.deepcopy(self.reviews)
            self.posts.append(copy.deepcopy(payload))
            event = payload.get("event")
            self.reviews.append({
                "id": 99, "node_id": "review-node-99", "user": {"login": "github-actions[bot]"},
                "commit_id": payload["commit_id"], "submitted_at": "9999-01-01T00:00:00Z" if event else None,
                "body": payload["body"], "html_url": "https://github.com/apache/doris/pull/123#pullrequestreview-99",
                "state": ("CHANGES_REQUESTED" if event == "REQUEST_CHANGES" else "COMMENTED") if event else "PENDING",
            })
            self.comments = [dict(c, user={"login": "github-actions[bot]"},
                                  commit_id=payload["commit_id"]) for c in payload["comments"]]
            if event:
                self.final_events.append(event)
            return copy.deepcopy(self.reviews[-1])
        if path == "graphql":
            self.assertEqual(submitter.THREAD_MUTATION, payload["query"])
            inp = copy.deepcopy(payload["variables"]["input"])
            self.assertEqual("review-node-99", inp["pullRequestReviewId"])
            self.thread_inputs.append(inp)
            self.comments.append({"path": inp["path"], "body": inp["body"],
                                  "position": inp["line"], "original_position": inp["line"],
                                  "user": {"login": "github-actions[bot]"}, "commit_id": self.run["head_sha"]})
            return {"data": {"addPullRequestReviewThread": {"thread": {
                "id": f"thread-{len(self.thread_inputs)}", "path": inp["path"],
                "line": inp["line"], "diffSide": inp["side"],
            }}}}
        if path == self.root + "/reviews/99":
            if method == "DELETE":
                self.deletes.append(99)
                self.reviews = [r for r in self.reviews if r["id"] != 99]
                self.comments = []
                return {}
            return copy.deepcopy(next(r for r in self.reviews if r["id"] == 99))
        if path == self.root + "/reviews/99/events":
            self.final_events.append(payload["event"])
            self.reviews[-1].update(state="CHANGES_REQUESTED" if payload["event"] == "REQUEST_CHANGES" else "COMMENTED",
                                    submitted_at="9999-01-01T00:00:00Z", body=payload["body"])
            return copy.deepcopy(self.reviews[-1])
        if path == self.root + "/reviews/99/comments":
            return copy.deepcopy(self.comments)
        if path == self.root + "/comments":
            return copy.deepcopy(self.existing)
        self.fail(f"Unexpected API path {path}")

    def submit(self, submission=None):
        return submitter.verify_completion(self.context, self.run, self.remaining, submit=True,
                                            submission=submission or self.submission())

    def verify(self):
        return submitter.verify_completion(self.context, self.run, self.remaining)

    def gh_response(self, command, **kwargs):
        payload = json.loads(kwargs["input"]) if kwargs.get("input") is not None else None
        paginated = "--paginate" in command
        method = command[command.index("--method") + 1] if "--method" in command else None
        value = self.fake_api(command[2], self.remaining, payload=payload, paginated=paginated, method=method)
        return SimpleNamespace(stdout=json.dumps([value] if paginated else value), stderr="")

    def test_failed_post_retains_response_candidate_and_no_repost_guard(self):
        self.api.side_effect = self.real_api
        rejection = '{"message":"Validation Failed","errors":[{"field":"position"}]}'
        post_attempts = []

        def reject_post(command, **kwargs):
            if kwargs.get("input") is not None:
                post_attempts.append(json.loads(kwargs["input"]))
                raise subprocess.CalledProcessError(1, command, output=rejection,
                                                    stderr="gh: Validation Failed (HTTP 422)")
            return self.gh_response(command, **kwargs)

        with mock.patch.object(submitter.subprocess, "run", side_effect=reject_post), \
             redirect_stderr(io.StringIO()) as logs:
            with self.assertRaisesRegex(ValueError, "HTTP 422"):
                self.submit()
            for call, reason in ((self.submit, "refusing to repost"), (self.verify, "exactly one")):
                with self.assertRaisesRegex(ValueError, reason) as failure:
                    call()
                self.assertIn("HTTP 422", str(failure.exception))
                self.assertIn("Validation Failed", str(failure.exception))
        self.assertEqual(1, len(post_attempts))
        self.assertEqual(self.submission(), json.loads((self.context / submitter.SUBMISSION_FILE).read_text()))
        diagnostic = json.loads((self.context / submitter.API_ERRORS_FILE).read_text())
        self.assertEqual(("POST", self.root + "/reviews", 1),
                         (diagnostic["method"], diagnostic["path"], diagnostic["returncode"]))
        self.assertEqual(rejection, diagnostic["stdout"])
        self.assertEqual(self.run["head_sha"], diagnostic["head_sha"])
        self.assertIn("HTTP 422", logs.getvalue())
        self.assertFalse((self.context / submitter.RESULT_FILE).exists())

    def test_later_api_failure_does_not_overwrite_original_post_diagnostic(self):
        self.api.side_effect = self.real_api

        def reject_post(command, **kwargs):
            if kwargs.get("input") is not None:
                raise subprocess.CalledProcessError(1, command, stderr="HTTP 422: position invalid")
            return self.gh_response(command, **kwargs)

        with mock.patch.object(submitter.subprocess, "run", side_effect=reject_post) as gh, \
             redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "HTTP 422"):
                self.submit()
            gh.side_effect = subprocess.CalledProcessError(1, "gh", stderr="HTTP 502: read unavailable")
            with self.assertRaisesRegex(ValueError, "HTTP 502"):
                self.verify()
            gh.side_effect = self.gh_response
            with self.assertRaisesRegex(ValueError, "first recorded API failure.*HTTP 422"):
                self.verify()
        records = [json.loads(line) for line in (self.context / submitter.API_ERRORS_FILE).read_text().splitlines()]
        self.assertEqual(["POST", "GET"], [record["method"] for record in records])

    def test_timed_out_post_still_recovers_by_exact_readback(self):
        self.api.side_effect = self.real_api

        def lose_post_response(command, **kwargs):
            result = self.gh_response(command, **kwargs)
            if command[2].endswith("/events"):
                raise subprocess.TimeoutExpired(command, 30, output=b'partial response', stderr=b'timed out')
            return result

        with mock.patch.object(submitter.subprocess, "run", side_effect=lose_post_response), \
             redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "TimeoutExpired"):
                self.submit()
            self.assertEqual("failure", self.submit()["state"])
            self.assertEqual("failure", self.verify()["state"])
        self.assertEqual(1, len(self.posts))
        diagnostic = json.loads((self.context / submitter.API_ERRORS_FILE).read_text())
        self.assertEqual((30, "partial response"),
                         (diagnostic["timeout_seconds"], diagnostic["stdout"]))
        self.assertTrue(diagnostic["stderr"].startswith("timed out"))
        self.assertIn("timeout=30", diagnostic["stderr"])

    def test_runner_failure_keeps_original_post_error(self):
        (self.context / "codex_goal_prompt.txt").write_text("Review the PR")
        args = SimpleNamespace(context_dir=self.context, cwd=self.context, **{
            k: self.run[k] for k in ("repository", "pr_number", "head_sha", "base_sha")
        }, model="gpt-6-sol", effort="xhigh", budget_seconds=60)
        self.api.side_effect = self.real_api

        def reject_post(command, **kwargs):
            if kwargs.get("input") is not None:
                raise subprocess.CalledProcessError(1, command, stderr="gh: Validation Failed (HTTP 422)")
            return self.gh_response(command, **kwargs)

        def attempt(command, events, stderr, timeout, reaper=None, watchdog=None):
            run = json.loads((self.context / submitter.RUN_FILE).read_text())
            with self.assertRaisesRegex(ValueError, "HTTP 422"):
                submitter.verify_completion(self.context, run, self.remaining, submit=True,
                                            submission=self.submission())
            events.write_text(json.dumps({"type": "thread.started", "thread_id":
                "0199a213-81c0-7800-8aa1-bbab2a035a53"}) + "\n" +
                json.dumps({"type": "turn.completed"}) + "\n")
            stderr.write_text("")
            return 0

        with mock.patch.object(runner, "run_attempt", side_effect=attempt), \
             mock.patch.object(runner.HeadWatchdog, "check"), \
             mock.patch.object(submitter.subprocess, "run", side_effect=reject_post), \
             redirect_stderr(io.StringIO()):
            self.assertEqual(1, runner.run_review(args))
        error = runner.read_events(self.context / "codex-events.jsonl")[-1]["error"]["message"]
        self.assertIn("HTTP 422", error)
        self.assertIn(submitter.API_ERRORS_FILE, error)
        self.assertFalse((self.context / submitter.RESULT_FILE).exists())

    def test_submits_once_and_verifies_same_complete_review(self):
        result = self.submit()
        self.assertEqual("failure", result["state"])
        self.assertEqual(1, result["p1"])
        self.assertEqual(result, self.submit())
        self.assertEqual(result, self.verify())
        self.assertEqual(1, len(self.posts))
        self.assertEqual(self.run["head_sha"], self.posts[0]["commit_id"])

    def test_minor_or_empty_review_passes_without_requesting_changes(self):
        for level in (None, 2, 3):
            with self.subTest(priority=level):
                self.reviews.clear()
                (self.context / submitter.SUBMISSION_FILE).unlink(missing_ok=True)
                result = self.submit(self.submission(level))
                self.assertEqual("success", result["state"])
                self.assertEqual("COMMENT", self.final_events[-1])

    def test_round_limit_delivers_verified_findings_without_passing(self):
        submission = self.submission(2, converged=False, rounds=3)
        result = self.submit(submission)
        self.assertEqual(("failure", 0, 0), (result["state"], result["p0"], result["p1"]))
        self.assertFalse(result["converged"])
        self.assertEqual(3, result["rounds"])
        self.assertEqual(submission["incomplete_reason"], result["incomplete_reason"])
        self.assertEqual(submission["comments"], [{k: c[k] for k in ("path", "position", "body")}
                                                  for c in self.comments])
        self.assertEqual("COMMENT", self.final_events[0])
        self.assertIn("Review did not converge after 3 rounds", self.posts[0]["body"])
        self.assertIn(submission["incomplete_reason"], self.posts[0]["body"])
        self.assertEqual(result, self.verify())
        self.assertEqual(result, self.submit(submission))
        self.assertEqual(1, len(self.posts))

    def test_convergence_metadata_is_bound_to_frozen_submission(self):
        original = self.submission(None, converged=False, rounds=3)
        self.submit(original)
        changes = (
            {"converged": True, "incomplete_reason": ""},
            {"incomplete_reason": "A different gap"},
        )
        for changeset in changes:
            with self.subTest(changeset=changeset), self.assertRaisesRegex(ValueError, "already declared"):
                self.submit(dict(original, **changeset))
        self.assertEqual(1, len(self.posts))

    def test_convergence_statement_is_verified_by_api_readback(self):
        self.submit(self.submission(None, converged=False, rounds=3))
        self.reviews[0]["body"] = self.reviews[0]["body"].replace("did not converge", "converged")
        with self.assertRaisesRegex(ValueError, "summary did not match"):
            self.verify()

    def test_nonconverged_submission_still_checks_live_head(self):
        self.pr["head"]["sha"] = "c" * 40
        with self.assertRaisesRegex(ValueError, "stale"):
            self.submit(self.submission(None, converged=False, rounds=3))
        self.assertEqual([], self.posts)

    def test_nonconverged_post_with_lost_response_is_not_duplicated(self):
        submission = self.submission(None, converged=False, rounds=3)
        def lost_response(path, *args, **kwargs):
            result = self.fake_api(path, *args, **kwargs)
            if kwargs.get("payload") is not None:
                raise subprocess.TimeoutExpired("gh", 30)
            return result
        self.api.side_effect = lost_response
        with self.assertRaises(subprocess.TimeoutExpired):
            self.submit(submission)
        self.api.side_effect = self.fake_api
        self.assertEqual("failure", self.submit(submission)["state"])
        self.assertEqual(1, len(self.posts))

    def test_convergence_schema_rejects_ambiguous_or_premature_results(self):
        invalid = [
            {"converged": "false"}, {"converged": 1}, {"converged": None},
            {"rounds": True}, {"rounds": 0}, {"rounds": 4}, {"rounds": "3"}, {"rounds": 3.0},
            {"incomplete_reason": None}, {"incomplete_reason": "still unresolved"},
            {"converged": False, "rounds": 3, "incomplete_reason": " "},
            {"converged": False, "rounds": 2, "incomplete_reason": "not settled"},
        ]
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.submit(dict(self.submission(None), **changes))
        for field in ("converged", "rounds", "incomplete_reason"):
            value = self.submission(None)
            del value[field]
            with self.subTest(missing=field), self.assertRaises(ValueError):
                self.submit(value)
        self.assertEqual([], self.posts)

    def test_existing_confirmed_blocker_is_counted_without_duplicate_inline(self):
        self.existing = [{"id": 101, "body": "[P0] Previously reported blocker"}]
        submission = self.submission(None)
        submission["existing_blocking_comment_ids"] = [101]
        result = self.submit(submission)
        self.assertEqual((1, 0, "failure"), (result["p0"], result["p1"], result["state"]))
        self.assertEqual([], self.posts[0]["comments"])
        self.assertIn("#discussion_r101", self.posts[0]["body"])

    def test_existing_bold_blockers_are_counted_without_duplicate_inline(self):
        formats = ("**[P{level}] Previously reported blocker**",
                   "**[P{level}]** Previously reported blocker")
        for level, template in itertools.product((0, 1), formats):
            body = template.format(level=level) + "\n\nThe issue still applies."
            with self.subTest(body=body), tempfile.TemporaryDirectory() as tmp, \
                 mock.patch.object(self, "context", Path(tmp)):
                self.reviews.clear()
                self.posts.clear()
                self.existing = [{"id": 101, "body": body}]
                submission = self.submission(None)
                submission["existing_blocking_comment_ids"] = [101]
                result = self.submit(submission)
                self.assertEqual((int(level == 0), int(level == 1), "failure"),
                                 (result["p0"], result["p1"], result["state"]))
                self.assertEqual(result, self.submit(submission))
                self.assertEqual(result, self.verify())
                self.assertEqual(1, len(self.posts))
                self.assertEqual("REQUEST_CHANGES", self.posts[0]["event"])
                self.assertEqual([], self.posts[0]["comments"])
                self.assertIn("#discussion_r101", self.posts[0]["body"])

    def test_bold_non_blocking_comments_cannot_be_declared_as_blockers(self):
        submission = self.submission(None)
        submission["existing_blocking_comment_ids"] = [101]
        for level, template in itertools.product(
                (2, 3), ("**[P{level}] Minor**", "**[P{level}]** Minor")):
            body = template.format(level=level)
            with self.subTest(body=body), self.assertRaisesRegex(ValueError, "not P0/P1"):
                self.existing = [{"id": 101, "body": body}]
                self.submit(submission)
        self.assertEqual([], self.posts)
        self.assertFalse((self.context / submitter.SUBMISSION_FILE).exists())

    def test_new_findings_still_require_plain_priority(self):
        for body in ("**[P1] New finding**", "**[P1]** New finding"):
            submission = self.submission()
            submission["comments"][0]["body"] = body
            with self.subTest(body=body), self.assertRaisesRegex(ValueError, "must start with"):
                self.submit(submission)
        self.api.assert_not_called()

    def test_invalid_existing_blocker_cannot_silently_pass(self):
        submission = self.submission(None)
        submission["existing_blocking_comment_ids"] = [101]
        for comments in ([], [{"id": 101, "body": "[P2] Minor"}],
                         [{"id": 101, "body": "**[P4] Unknown priority**"}],
                         [{"id": 101, "body": "**[P1]**"}],
                         [{"id": 101, "body": "Text mentioning **[P1]** later"}],
                         [{"id": 101, "body": "[P1] Reply", "in_reply_to_id": 1}],
                         [{"id": 101, "body": "**[P1] Reply**", "in_reply_to_id": 1}]):
            with self.subTest(comments=comments), self.assertRaises(ValueError):
                self.existing = comments
                self.submit(submission)
        self.assertEqual([], self.posts)

    def test_missing_intent_cannot_use_other_reviews_or_result_file(self):
        (self.context / submitter.RESULT_FILE).write_text('{"state":"success"}')
        with self.assertRaisesRegex(ValueError, "No final review submission"):
            self.verify()
        self.api.assert_not_called()

    def test_parallel_run_cannot_satisfy_completion_or_cause_duplicate_post(self):
        self.submit()
        self.run["token"] = "another-run"
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.verify()
        with self.assertRaisesRegex(ValueError, "Another bot review"):
            self.submit()
        self.assertEqual(1, len(self.posts))

    def test_rejected_submission_preserves_distinct_findings_and_reason(self):
        self.reviews = [{"id": 98, "user": {"login": "github-actions[bot]"},
                         "commit_id": self.run["head_sha"],
                         "submitted_at": self.run["started_at"], "body": "A different finding"}]
        for level in (1, 2):
            with self.subTest(priority=level):
                submission = self.submission(level)
                with self.assertRaisesRegex(ValueError, "Another bot review") as error:
                    self.submit(submission)
                rejected = json.loads((self.context / submitter.REJECTION_FILE).read_text())
                self.assertEqual(submission, rejected["submission"])
                self.assertEqual(str(error.exception), rejected["reason"])
                with self.assertRaisesRegex(ValueError, "Another bot review"):
                    self.verify()
        self.assertEqual([], self.posts)
        self.assertFalse((self.context / submitter.SUBMISSION_FILE).exists())

    def test_review_before_start_does_not_block_next_run(self):
        self.reviews = [{"id": 98, "user": {"login": "github-actions[bot]"},
                         "commit_id": self.run["head_sha"],
                         "submitted_at": "2026-09-28T23:59:59Z", "body": "Earlier review"}]
        self.assertEqual("failure", self.submit()["state"])
        self.assertEqual(1, len(self.posts))
        self.assertFalse((self.context / submitter.REJECTION_FILE).exists())

    def test_verified_submission_takes_precedence_over_rejection(self):
        submitter.write_json(self.context / submitter.REJECTION_FILE, {
            "reason": "Earlier rejected candidate", "submission": self.submission(2),
        })
        result = self.submit()
        self.assertEqual(result, self.verify())
        self.assertEqual(1, len(self.posts))

    def test_runner_reports_rejection_instead_of_missing_submission(self):
        (self.context / "codex_goal_prompt.txt").write_text("Review the PR")
        args = SimpleNamespace(context_dir=self.context, cwd=self.context, **{
            k: self.run[k] for k in ("repository", "pr_number", "head_sha", "base_sha")
        }, model="gpt-6-sol", effort="xhigh", budget_seconds=60)

        def attempt(command, events, stderr, timeout, reaper=None, watchdog=None):
            run = json.loads((self.context / submitter.RUN_FILE).read_text())
            self.reviews = [{"user": {"login": "github-actions[bot]"},
                             "commit_id": run["head_sha"], "submitted_at": run["started_at"]}]
            with self.assertRaisesRegex(ValueError, "Another bot review"):
                submitter.verify_completion(self.context, run, self.remaining, submit=True,
                                            submission=self.submission())
            # The model can finish normally after the helper rejects its candidate.
            events.write_text(json.dumps({"type": "thread.started", "thread_id":
                "0199a213-81c0-7800-8aa1-bbab2a035a53"}) + "\n" +
                json.dumps({"type": "turn.completed"}) + "\n")
            stderr.write_text("")
            return 0

        with mock.patch.object(runner, "run_attempt", side_effect=attempt), \
             mock.patch.object(runner.HeadWatchdog, "check"), \
             mock.patch.object(runner, "check_resume_target") as guard, \
             redirect_stderr(io.StringIO()):
            self.assertEqual(1, runner.run_review(args))
        guard.assert_not_called()
        error = runner.read_events(self.context / "codex-events.jsonl")[-1]["error"]["message"]
        self.assertIn("Another bot review", error)
        self.assertNotIn("No final review submission", error)
        self.assertEqual([], self.posts)
        self.assertFalse((self.context / submitter.RESULT_FILE).exists())

    def test_simultaneous_helper_invocations_post_only_once(self):
        barrier = threading.Barrier(2, timeout=5)
        original_open = Path.open

        def concurrent_open(path, mode="r", *args, **kwargs):
            if path.name == submitter.SUBMISSION_FILE and mode == "x":
                barrier.wait()
            return original_open(path, mode, *args, **kwargs)

        with mock.patch.object(Path, "open", concurrent_open), ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(self.submit) for _ in range(2)]
            outcomes = []
            for future in futures:
                try:
                    outcomes.append(future.result())
                except FileExistsError:
                    outcomes.append(None)
        self.assertEqual(1, sum(outcome is not None for outcome in outcomes))
        self.assertEqual(1, len(self.posts))
        self.assertEqual("failure", self.verify()["state"])

    def test_reply_during_wrap_up_does_not_invalidate_final_inline_set(self):
        self.submit()
        self.comments.append({"in_reply_to_id": 1, "body": "Thanks", "path": "file.py",
                              "user": {"login": "human"}})
        self.assertEqual("failure", self.verify()["state"])

    def test_frozen_submission_cannot_change_on_retry(self):
        self.submit()
        with self.assertRaisesRegex(ValueError, "different review"):
            self.submit(self.submission(None))
        self.assertEqual(1, len(self.posts))

    def test_partial_or_modified_inline_delivery_fails(self):
        self.submit()
        original = copy.deepcopy(self.comments)
        for comments in ([], original + original,
                         [dict(original[0], position=13)],
                         [dict(original[0], body="[P1] Different")],
                         [dict(original[0], user={"login": "other"})],
                         [dict(original[0], commit_id="c" * 40)]):
            with self.subTest(comments=comments), self.assertRaisesRegex(ValueError, "inline"):
                self.comments = comments
                self.verify()

    def test_changed_review_identity_or_summary_cannot_pass(self):
        self.submit()
        original = copy.deepcopy(self.reviews[0])
        for changes in ({"commit_id": "c" * 40}, {"user": {"login": "human"}},
                        {"submitted_at": "2020-01-01T00:00:00Z"}, {"state": "DISMISSED"},
                        {"body": original["body"] + " changed"}):
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "did not match"):
                self.reviews[0] = dict(original, **changes)
                self.verify()

    def test_pr_move_or_close_rejects_before_submission(self):
        for change in ({"state": "closed"}, {"head": {"sha": "c" * 40}},
                       {"base": {"sha": "c" * 40}}):
            original = copy.deepcopy(self.pr)
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "stale"):
                self.pr.update(change)
                self.submit()
            self.pr = original
        self.assertEqual([], self.posts)

    def test_pr_move_during_readback_does_not_pass(self):
        self.submit()
        original_api = self.fake_api

        def move_after_comments(path, *args, **kwargs):
            result = original_api(path, *args, **kwargs)
            if path.endswith("/reviews/99/comments"):
                self.pr["head"]["sha"] = "c" * 40
            return result

        self.api.side_effect = move_after_comments
        with self.assertRaisesRegex(ValueError, "stale"):
            self.verify()

    def test_lost_post_response_recovers_by_readback_without_duplicate(self):
        original_api = self.fake_api

        def lost_response(path, *args, **kwargs):
            result = original_api(path, *args, **kwargs)
            if path.endswith("/events"):
                raise subprocess.TimeoutExpired("gh", 30)
            return result

        self.api.side_effect = lost_response
        with self.assertRaises(subprocess.TimeoutExpired):
            self.submit()
        self.api.side_effect = original_api
        self.assertEqual("failure", self.verify()["state"])
        self.submit()
        self.assertEqual(1, len(self.posts))

    def test_delivery_completes_execution_and_gates_findings_and_convergence(self):
        # Reproduce #66227: final write/readback, then capacity (also on the last
        # allowed attempt). Run the real verifier and SHA-wide status resolver.
        for level, converged, capacity in itertools.product((None, 0, 1, 2), (True, False), (True, False)):
            with self.subTest(priority=level, converged=converged, capacity=capacity), tempfile.TemporaryDirectory() as tmp:
                context = Path(tmp)
                (context / "codex_goal_prompt.txt").write_text("Review the PR")
                (context / "pr.diff").write_text(self.diff)
                args = SimpleNamespace(context_dir=context, cwd=context, **{
                    k: self.run[k] for k in ("repository", "pr_number", "head_sha", "base_sha")
                }, model="gpt-6-sol", effort="xhigh", budget_seconds=60)
                self.reviews.clear()

                def attempt(command, events, stderr, timeout, reaper=None, watchdog=None):
                    run = json.loads((context / submitter.RUN_FILE).read_text())
                    submitter.verify_completion(context, run, self.remaining, submit=True,
                                                submission=self.submission(level, converged=converged, rounds=3))
                    events.write_text(json.dumps({"type": "thread.started", "thread_id":
                        "0199a213-81c0-7800-8aa1-bbab2a035a53"}) + "\n" + json.dumps({
                        "type": "turn.failed" if capacity else "turn.completed",
                        "error": {"message": runner.CAPACITY_MESSAGE} if capacity else {}}) + "\n")
                    stderr.write_text("")
                    return 1 if capacity else 0

                with mock.patch.object(runner, "run_attempt", side_effect=attempt), \
                     mock.patch.object(runner, "RETRY_DELAYS", ()), \
                     mock.patch.object(runner, "HeadWatchdog"), \
                     mock.patch.object(runner, "check_resume_target") as guard, \
                     mock.patch.object(runner.time, "sleep") as sleep, redirect_stderr(io.StringIO()):
                    self.assertEqual(0, runner.run_review(args))
                guard.assert_not_called()
                sleep.assert_not_called()
                result = json.loads((context / submitter.RESULT_FILE).read_text())
                self.assertEqual(capacity, result["recovered_after_capacity"])
                self.assertEqual(converged, result["converged"])
                self.assertEqual(3, result["rounds"])
                completion = runner.read_events(context / "codex-events.jsonl")[-1]
                self.assertEqual("review.completed", completion["type"])
                self.assertEqual(converged, completion["converged"])
                state = "failure" if level in (0, 1) or not converged else "success"
                self.assertEqual(state, result["state"])
                resolution = resolve_status([self.pr], [{"id": 1, "state": result["state"],
                    "context": f"code-review/source/automated/pr-123/base-{self.run['base_sha']}"}],
                    head_sha=self.run["head_sha"])
                self.assertEqual(state, resolution.state)

    def test_unconfirmed_post_is_never_repeated(self):
        submission = self.submission()
        submitter.write_json(self.context / submitter.SUBMISSION_FILE, submission)
        with self.assertRaisesRegex(ValueError, "refusing to repost"):
            self.submit(submission)
        self.assertEqual([], self.posts)

    def test_api_unavailable_is_not_completion(self):
        self.submit()
        self.api.side_effect = subprocess.CalledProcessError(1, "gh")
        with self.assertRaises(subprocess.CalledProcessError):
            self.verify()

    def test_five_comments_use_one_draft_and_separate_mutations(self):
        submission = self.submission()
        submission["comments"] = [dict(submission["comments"][0], position=i,
                                       body=f"[P1] Finding {i}") for i in range(1, 6)]
        self.submit(submission)
        self.assertEqual(1, len(self.posts))
        self.assertNotIn("event", self.posts[0])
        self.assertEqual([], self.posts[0]["comments"])
        self.assertEqual(5, len(self.thread_inputs))
        self.assertEqual({"review-node-99"}, {c["pullRequestReviewId"] for c in self.thread_inputs})
        self.assertEqual([1, 2, 3, 4, 5], [c["line"] for c in self.thread_inputs])
        self.assertEqual(["REQUEST_CHANGES"], self.final_events)
        self.assertEqual([], self.deletes)
        self.assertEqual("submitted", json.loads((self.context / submitter.DRAFT_FILE).read_text())["stage"])

    def test_invalid_anchor_is_rejected_before_intent_or_write(self):
        submission = self.submission()
        submission["comments"][0]["position"] = 999
        with self.assertRaisesRegex(ValueError, "not commentable"):
            self.submit(submission)
        self.assertEqual([], self.posts)
        self.assertFalse((self.context / submitter.SUBMISSION_FILE).exists())

    def test_add_failure_deletes_only_owned_draft_and_never_reposts(self):
        self.reviews = [{"id": 88, "state": "PENDING", "body": "Another draft", "commit_id": self.run["head_sha"],
                         "user": {"login": "human"}}]
        original = self.fake_api

        def fail_add(path, *args, **kwargs):
            if path == "graphql":
                raise submitter.GitHubAPIError(path, "POST", ValueError("rejected"), stderr="Line could not be resolved")
            return original(path, *args, **kwargs)

        self.api.side_effect = fail_add
        with self.assertRaisesRegex(ValueError, "Line could not be resolved"), redirect_stderr(io.StringIO()):
            self.submit()
        self.assertEqual([99], self.deletes)
        self.assertEqual([88], [r["id"] for r in self.reviews])
        self.assertEqual([], self.final_events)
        with self.assertRaisesRegex(ValueError, "refusing to repost"):
            self.submit()
        self.assertEqual(1, len(self.posts))

    def test_lost_create_response_cleans_up_without_recreating(self):
        original = self.fake_api

        def lose_create(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if path == self.root + "/reviews" and kwargs.get("payload") is not None:
                raise subprocess.TimeoutExpired("gh", 30)
            return result

        self.api.side_effect = lose_create
        with self.assertRaises(subprocess.TimeoutExpired):
            self.submit()
        self.assertEqual([99], self.deletes)
        self.assertEqual([], self.final_events)
        with self.assertRaisesRegex(ValueError, "refusing to repost"):
            self.submit()
        self.assertEqual(1, len(self.posts))

    def test_wrong_thread_location_is_not_published(self):
        original = self.fake_api

        def wrong_line(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if path == "graphql":
                result["data"]["addPullRequestReviewThread"]["thread"]["line"] += 1
            return result

        self.api.side_effect = wrong_line
        with self.assertRaisesRegex(ValueError, "location did not match"):
            self.submit()
        self.assertEqual([99], self.deletes)
        self.assertEqual([], self.final_events)

    def test_incomplete_draft_is_not_published(self):
        original = self.fake_api

        def missing_comment(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            return [] if path.endswith("/comments") else result

        self.api.side_effect = missing_comment
        with self.assertRaisesRegex(ValueError, "inline comments"):
            self.submit()
        self.assertEqual([99], self.deletes)
        self.assertEqual([], self.final_events)

    def test_head_moves_between_adds_deletes_draft_without_publishing(self):
        original = self.fake_api

        def move_head(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if path == "graphql":
                self.pr["head"]["sha"] = "c" * 40
            return result

        submission = self.submission()
        submission["comments"].append(dict(submission["comments"][0], position=13))
        self.api.side_effect = move_head
        with self.assertRaisesRegex(ValueError, "stale"):
            self.submit(submission)
        self.assertEqual(1, len(self.thread_inputs))
        self.assertEqual([99], self.deletes)
        self.assertEqual([], self.final_events)

    def test_ambiguous_publish_is_never_retried_or_deleted(self):
        original = self.fake_api

        def fail_publish(path, *args, **kwargs):
            if path.endswith("/events"):
                raise subprocess.TimeoutExpired("gh", 30)
            return original(path, *args, **kwargs)

        self.api.side_effect = fail_publish
        with self.assertRaises(subprocess.TimeoutExpired):
            self.submit()
        self.assertEqual([], self.deletes)
        self.assertEqual("PENDING", self.reviews[0]["state"])
        with self.assertRaisesRegex(ValueError, "did not match"):
            self.verify()
        with self.assertRaisesRegex(ValueError, "did not match"):
            self.submit()
        self.assertEqual(1, len(self.posts))
        self.assertEqual(1, len(self.thread_inputs))
        self.assertEqual("submitting", json.loads((self.context / submitter.DRAFT_FILE).read_text())["stage"])

    def test_cleanup_failure_preserves_original_error_and_draft_id(self):
        original = self.fake_api

        def fail_add_and_cleanup(path, *args, **kwargs):
            if path == "graphql":
                raise ValueError("original add failure")
            if kwargs.get("method") == "DELETE":
                raise ValueError("cleanup unavailable")
            return original(path, *args, **kwargs)

        self.api.side_effect = fail_add_and_cleanup
        with self.assertRaisesRegex(ValueError, "original add failure"), redirect_stderr(io.StringIO()):
            self.submit()
        progress = json.loads((self.context / submitter.DRAFT_FILE).read_text())
        self.assertEqual("cleanup_failed", progress["stage"])
        self.assertEqual(99, progress["review_id"])
        self.assertIn("cleanup unavailable", progress["cleanup_error"])
        self.assertEqual([], self.final_events)

    def test_parallel_bot_review_during_adds_prevents_publication(self):
        original = self.fake_api

        def other_review(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if path == "graphql":
                self.reviews.append({"id": 100, "state": "COMMENTED", "body": "Other review",
                                     "user": {"login": "github-actions[bot]"},
                                     "commit_id": self.run["head_sha"], "submitted_at": "9999-01-01T00:00:00Z"})
            return result

        self.api.side_effect = other_review
        with self.assertRaisesRegex(ValueError, "Another bot review"):
            self.submit()
        self.assertEqual([], self.final_events)
        self.assertEqual([99], self.deletes)
        self.assertEqual([100], [r["id"] for r in self.reviews])

    def test_changed_draft_is_neither_published_nor_deleted(self):
        original = self.fake_api

        def change_draft(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if path == "graphql":
                self.reviews[0]["body"] = "Edited outside this invocation"
            return result

        self.api.side_effect = change_draft
        with self.assertRaisesRegex(ValueError, "Pending review author"):
            self.submit()
        self.assertEqual([], self.final_events)
        self.assertEqual([], self.deletes)

    def test_schema_requires_explicit_severity_and_all_fields(self):
        for value in ({"body": "summary"}, dict(self.submission(), comments="no"),
                      dict(self.submission(), existing_blocking_comment_ids=[1, 1]),
                      dict(self.submission(), comments=[{"path": "f", "position": 1, "body": "Bug"}]),
                      dict(self.submission(), comments=[{"path": "f", "position": 0, "body": "[P1] Bug"}])):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.submit(value)
        self.assertEqual([], self.posts)


class DiffLocationsTest(unittest.TestCase):
    def locations(self, diff, pairs):
        with tempfile.TemporaryDirectory() as tmp:
            context = Path(tmp)
            (context / "pr.diff").write_text(diff)
            return submitter.inline_locations(context, [{"path": p, "position": n} for p, n in pairs])

    def test_context_deletion_addition_and_later_hunk(self):
        diff = ("diff --git a/a.py b/a.py\n--- a/a.py\n+++ b/a.py\n"
                "@@ -10,3 +10,3 @@ function\n context\n-old\n+new\n last\n"
                "@@ -30 +30 @@\n-before\n+after\n")
        self.assertEqual([{"line": 10, "side": "RIGHT"}, {"line": 11, "side": "LEFT"},
                          {"line": 11, "side": "RIGHT"}, {"line": 30, "side": "LEFT"},
                          {"line": 30, "side": "RIGHT"}],
                         self.locations(diff, [("a.py", i) for i in (1, 2, 3, 6, 7)]))
        with self.assertRaisesRegex(ValueError, "not commentable"):
            self.locations(diff, [("a.py", 5)])

    def test_deleted_file_and_renamed_file_use_correct_paths(self):
        diff = ("diff --git a/old.py b/old.py\n--- a/old.py\n+++ /dev/null\n@@ -1 +0,0 @@\n-gone\n"
                "diff --git a/before.py b/after.py\n--- a/before.py\n+++ b/after.py\n"
                "@@ -1 +1 @@\n-old\n+new\n")
        self.assertEqual([{"line": 1, "side": "LEFT"}, {"line": 1, "side": "RIGHT"}],
                         self.locations(diff, [("old.py", 1), ("after.py", 2)]))

    def test_git_quoted_utf8_and_space_paths(self):
        diff = ('diff --git "a/\\344\\270\\255.py" "b/\\344\\270\\255.py"\n'
                '--- /dev/null\n+++ "b/\\344\\270\\255.py"\n@@ -0,0 +1 @@\n+text\n'
                'diff --git a/has space.py b/has space.py\n--- /dev/null\n+++ b/has space.py\t\n'
                '@@ -0,0 +1 @@\n+text\n')
        self.assertEqual([{"line": 1, "side": "RIGHT"}] * 2,
                         self.locations(diff, [("中.py", 1), ("has space.py", 1)]))

    def test_no_newline_markers_are_not_anchors(self):
        diff = ("diff --git a/a.py b/a.py\n--- a/a.py\n+++ b/a.py\n@@ -1 +1 @@\n"
                "-old\n\\ No newline at end of file\n+new\n\\ No newline at end of file\n")
        self.assertEqual([{"line": 1, "side": "RIGHT"}], self.locations(diff, [("a.py", 3)]))
        with self.assertRaisesRegex(ValueError, "not commentable"):
            self.locations(diff, [("a.py", 2)])


class ReviewWorkflowTest(unittest.TestCase):
    def test_queue_is_per_pr_and_failure_artifact_is_explicit(self):
        workflow = (Path(__file__).resolve().parents[1] / "workflows/code-review-runner.yml").read_text()
        review_job = workflow.split("  code-review:\n", 1)[1].split("  aggregate-status:", 1)[0]
        concurrency = re.search(r"^    concurrency:\n((?:      .*\n)+)", review_job, re.MULTILINE)
        self.assertIsNotNone(concurrency)
        self.assertIn("github.event.issue.number || inputs.pr_number || github.run_id", concurrency[1])
        self.assertIn("cancel-in-progress: false", concurrency[1])
        self.assertIn("queue: max", concurrency[1])
        self.assertNotIn("sha", concurrency[1])
        self.assertNotRegex(workflow, r"(?m)^concurrency:")
        artifact = review_job.split("      - name: Preserve failed final review diagnostics\n", 1)[1]
        artifact = artifact.split("      - name:", 1)[0]
        self.assertIn("always()", artifact)
        self.assertIn("steps.review.outcome == 'failure'", artifact)
        paths = artifact.split("          path: |\n", 1)[1].split("          #", 1)[0]
        self.assertEqual({"${{ env.REVIEW_CONTEXT_DIR }}/" + name for name in (
            submitter.SUBMISSION_FILE, submitter.REJECTION_FILE, submitter.API_ERRORS_FILE, submitter.DRAFT_FILE
        )}, {line.strip() for line in paths.splitlines()})
        self.assertIn("include-hidden-files: true", artifact)
        self.assertIn("if-no-files-found: ignore", artifact)


class GitHubAPITest(unittest.TestCase):
    def test_graphql_errors_in_http_success_are_recorded_as_failure(self):
        response = {"data": {"addPullRequestReviewThread": None},
                    "errors": [{"message": "Line could not be resolved"}]}
        debug = ("> POST /graphql HTTP/2.0\n\n< HTTP/2.0 200 OK\n"
                 "< X-Github-Request-Id: GRAPHQL-REQUEST\n\n" + json.dumps(response))
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.dict(submitter.os.environ, {"REVIEW_GITHUB_DIAGNOSTICS_DIR": tmp}), \
             mock.patch.object(submitter.subprocess, "run", return_value=SimpleNamespace(
                 stdout=json.dumps(response), stderr=debug
             )), redirect_stderr(io.StringIO()):
            with self.assertRaises(submitter.GitHubAPIError) as failure:
                submitter.api("graphql", lambda: 60, payload={"query": "mutation"})
            self.assertIn("Line could not be resolved", failure.exception.details["stdout"])
            records = [json.loads(p.read_text()) for p in Path(tmp).glob("*.json")]
            self.assertEqual(1, len(records))
            self.assertEqual(200, records[0]["http"]["status"])
            self.assertEqual("GRAPHQL-REQUEST", records[0]["http"]["headers"]["x-github-request-id"])

    def test_read_all_pages_and_bound_timeout(self):
        with mock.patch.object(submitter.subprocess, "run", return_value=SimpleNamespace(
            stderr="", stdout='[[{"id":1}], [{"id":2}]]'
        )) as command:
            self.assertEqual([{"id": 1}, {"id": 2}], submitter.api("reviews", lambda: 7, paginated=True))
        self.assertEqual(7, command.call_args.kwargs["timeout"])
        self.assertIn("--paginate", command.call_args.args[0])
        self.assertIn("--slurp", command.call_args.args[0])

    def test_api_error_redacts_credentials_before_logging_or_archiving(self):
        secret = "private-token-from-environment"
        error = subprocess.CalledProcessError(1, "gh", output='{"message":"invalid ghp_exampleToken"}',
                                            stderr=f"HTTP 403: token={secret}")
        with mock.patch.dict(submitter.os.environ, {"GH_TOKEN": secret}), \
             mock.patch.object(submitter.subprocess, "run", side_effect=error), \
             self.assertRaises(submitter.GitHubAPIError) as failure:
            submitter.api("reviews", lambda: 60, payload={"body": "not a credential"})
        text = json.dumps(failure.exception.details) + str(failure.exception)
        self.assertNotIn(secret, text)
        self.assertNotIn("ghp_exampleToken", text)
        self.assertIn("[REDACTED]", text)
        self.assertIn("HTTP 403", text)
        self.assertNotIn("not a credential", text)

    def test_invalid_json_retains_successful_command_output(self):
        with mock.patch.object(submitter.subprocess, "run", return_value=SimpleNamespace(
            stdout="incomplete response", stderr="diagnostic"
        )), self.assertRaises(submitter.GitHubAPIError) as failure:
            submitter.api("reviews", lambda: 7)
        self.assertEqual("JSONDecodeError", failure.exception.details["error_type"])
        self.assertEqual("incomplete response", failure.exception.details["stdout"])

    def test_diagnostics_bound_large_output_after_redaction(self):
        output = "ghp_secret " + "x" * 70000
        result = submitter.diagnostic_text(output.encode())
        self.assertNotIn("ghp_secret", result)
        self.assertLess(len(result), 65600)
        self.assertTrue(result.endswith("[truncated after 65536 characters]"))


if __name__ == "__main__":
    unittest.main()
