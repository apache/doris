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
import json
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
        self.api = self.enterContext(mock.patch.object(submitter, "api", side_effect=self.fake_api))
        self.remaining = lambda: 60

    def submission(self, priority=1):
        return {"body": "All checkpoints reviewed.", "comments": [] if priority is None else [
            {"path": "file.py", "position": 12, "body": f"[P{priority}] Fix the verified issue"}
        ], "existing_blocking_comment_ids": []}

    def fake_api(self, path, remaining, *, payload=None, paginated=False):
        if path == self.root:
            return copy.deepcopy(self.pr)
        if path == self.root + "/reviews":
            if payload is None:
                return copy.deepcopy(self.reviews)
            self.posts.append(copy.deepcopy(payload))
            self.reviews.append({
                "id": 99, "user": {"login": "github-actions[bot]"},
                "commit_id": payload["commit_id"], "submitted_at": "9999-01-01T00:00:00Z",
                "body": payload["body"], "html_url": "https://github.com/apache/doris/pull/123#pullrequestreview-99",
                "state": "CHANGES_REQUESTED" if payload["event"] == "REQUEST_CHANGES" else "COMMENTED",
            })
            self.comments = [dict(c, user={"login": "github-actions[bot]"},
                                  commit_id=payload["commit_id"]) for c in payload["comments"]]
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
                self.assertEqual("COMMENT", self.posts[-1]["event"])

    def test_existing_confirmed_blocker_is_counted_without_duplicate_inline(self):
        self.existing = [{"id": 101, "body": "[P0] Previously reported blocker"}]
        submission = self.submission(None)
        submission["existing_blocking_comment_ids"] = [101]
        result = self.submit(submission)
        self.assertEqual((1, 0, "failure"), (result["p0"], result["p1"], result["state"]))
        self.assertEqual([], self.posts[0]["comments"])
        self.assertIn("#discussion_r101", self.posts[0]["body"])

    def test_invalid_existing_blocker_cannot_silently_pass(self):
        submission = self.submission(None)
        submission["existing_blocking_comment_ids"] = [101]
        for comments in ([], [{"id": 101, "body": "[P2] Minor"}],
                         [{"id": 101, "body": "[P1] Reply", "in_reply_to_id": 1}]):
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
            if kwargs.get("payload") is not None:
                raise subprocess.TimeoutExpired("gh", 30)
            return result

        self.api.side_effect = lost_response
        with self.assertRaises(subprocess.TimeoutExpired):
            self.submit()
        self.api.side_effect = original_api
        self.assertEqual("failure", self.verify()["state"])
        self.submit()
        self.assertEqual(1, len(self.posts))

    def test_capacity_after_submission_completes_execution_and_gates_findings(self):
        # Reproduce #66227: final write/readback, then capacity (also on the last
        # allowed attempt). Run the real verifier and SHA-wide status resolver.
        for level in (None, 0, 1, 2):
            with self.subTest(priority=level), tempfile.TemporaryDirectory() as tmp:
                context = Path(tmp)
                (context / "codex_goal_prompt.txt").write_text("Review the PR")
                args = SimpleNamespace(context_dir=context, cwd=context, **{
                    k: self.run[k] for k in ("repository", "pr_number", "head_sha", "base_sha")
                }, model="gpt-6-sol", effort="xhigh", budget_seconds=60)
                self.reviews.clear()

                def attempt(command, events, stderr, timeout, reaper=None):
                    run = json.loads((context / submitter.RUN_FILE).read_text())
                    submitter.verify_completion(context, run, self.remaining, submit=True,
                                                submission=self.submission(level))
                    events.write_text(json.dumps({"type": "thread.started", "thread_id":
                        "0199a213-81c0-7800-8aa1-bbab2a035a53"}) + "\n" + json.dumps({
                        "type": "turn.failed", "error": {"message": runner.CAPACITY_MESSAGE}}) + "\n")
                    stderr.write_text("")
                    return 1

                with mock.patch.object(runner, "run_attempt", side_effect=attempt), \
                     mock.patch.object(runner, "RETRY_DELAYS", ()), \
                     mock.patch.object(runner, "check_resume_target") as guard, \
                     mock.patch.object(runner.time, "sleep") as sleep, redirect_stderr(io.StringIO()):
                    self.assertEqual(0, runner.run_review(args))
                guard.assert_not_called()
                sleep.assert_not_called()
                result = json.loads((context / submitter.RESULT_FILE).read_text())
                self.assertTrue(result["recovered_after_capacity"])
                state = "failure" if level in (0, 1) else "success"
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

    def test_schema_requires_explicit_severity_and_all_fields(self):
        for value in ({"body": "summary"}, dict(self.submission(), comments="no"),
                      dict(self.submission(), existing_blocking_comment_ids=[1, 1]),
                      dict(self.submission(), comments=[{"path": "f", "position": 1, "body": "Bug"}]),
                      dict(self.submission(), comments=[{"path": "f", "position": 0, "body": "[P1] Bug"}])):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.submit(value)
        self.assertEqual([], self.posts)


class GitHubAPITest(unittest.TestCase):
    def test_read_all_pages_and_bound_timeout(self):
        with mock.patch.object(submitter.subprocess, "run", return_value=SimpleNamespace(
            stdout='[[{"id":1}], [{"id":2}]]'
        )) as command:
            self.assertEqual([{"id": 1}, {"id": 2}], submitter.api("reviews", lambda: 7, paginated=True))
        self.assertEqual(7, command.call_args.kwargs["timeout"])
        self.assertIn("--paginate", command.call_args.args[0])
        self.assertIn("--slurp", command.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
