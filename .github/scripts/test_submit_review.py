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
import re
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import run_review_with_resume as runner
import submit_review as submitter
from resolve_code_review_status import resolve_status


class SubmissionPreflightTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)
        self.context = self.directory / "context"
        self.input = self.directory / "final.json"
        self.value = {
            "body": "All checkpoints reviewed.",
            "comments": [{"path": "file.py", "position": 12, "body": "[P1] First finding"}],
            "existing_blocking_comment_ids": [],
        }

    def check(self):
        self.input.write_text(json.dumps(self.value))
        output = io.StringIO()
        original_open = Path.open

        def read_input_only(path, mode="r", *args, **kwargs):
            self.assertEqual(self.input, path)
            self.assertEqual("r", mode)
            return original_open(path, mode, *args, **kwargs)

        with mock.patch.object(submitter, "api") as api, \
             mock.patch.object(Path, "open", read_input_only), redirect_stdout(output):
            submitter.main(["--context-dir", str(self.context),
                            "--input-file", str(self.input), "--check-only"])
        api.assert_not_called()
        return json.loads(output.getvalue())

    def test_check_needs_no_context_and_reads_only_input(self):
        result = self.check()
        self.assertTrue(result["check_only"])
        self.assertTrue(result["schema_valid"])
        self.assertNotIn("state", result)
        self.assertNotIn("review_id", result)
        self.assertFalse(self.context.exists())
        comment = result["comments"][0]
        self.assertEqual(("file.py", 12), (comment["path"], comment["position"]))
        self.assertEqual([], comment["warnings"])
        self.assertFalse(comment["preview_truncated"])

    def test_existing_run_and_submission_are_not_read_or_changed(self):
        self.context.mkdir()
        for name in (submitter.RUN_FILE, submitter.SUBMISSION_FILE, submitter.RESULT_FILE):
            (self.context / name).write_text("existing state must not be read or overwritten")
        before = {p.name: p.read_bytes() for p in self.context.iterdir()}
        self.check()
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.context.iterdir()})

    def test_greedy_ledger_capture_warns_and_bounds_only_the_preview(self):
        ledger = (
            "- ID: F1\n  Proposed inline body: [P1] First finding\n"
            "- ID: F2\n  Evidence: " + "unrelated evidence " * 300 + "\n"
            "  Proposed inline body: [P2] Second finding\n"
            "## Convergence Rounds\nInternal discussion at the end."
        )
        # Reproduce the temporary extraction pattern from the #67395 review.
        body = re.search(
            r"^- ID: F1\n(?:(?!^- ID: ).)*?^  Proposed inline body: (.+)$",
            ledger, re.M | re.S,
        ).group(1)
        self.value["comments"][0]["body"] = body
        original = copy.deepcopy(self.value)
        comment = self.check()["comments"][0]
        self.assertEqual(2, len(comment["warnings"]))
        self.assertEqual(len(body), comment["characters"])
        self.assertEqual(len(body.splitlines()), comment["lines"])
        self.assertLessEqual(len(comment["preview_start"]) + len(comment["preview_end"]), 480)
        self.assertTrue(comment["preview_truncated"])
        self.assertTrue(comment["preview_start"].startswith("[P1] First finding"))
        self.assertTrue(comment["preview_end"].endswith("Internal discussion at the end."))
        self.assertEqual(original, self.value)
        self.assertEqual(original, json.loads(self.input.read_text()))

    def test_empty_review_and_existing_findings_are_checked_without_github(self):
        self.value["comments"] = []
        self.value["existing_blocking_comment_ids"] = [101]
        self.assertEqual([], self.check()["comments"])

    def test_invalid_schema_still_fails(self):
        for change in ({"comments": "not an array"},
                       {"comments": [{"path": "f", "position": 0, "body": "[P1] Bug"}]},
                       {"comments": [{"path": "f", "position": 1, "body": "No priority"}]},
                       {"existing_blocking_comment_ids": [1, 1]},
                       {"unexpected": True}):
            with self.subTest(change=change):
                original = copy.deepcopy(self.value)
                self.value.update(change)
                with self.assertRaises(ValueError):
                    self.check()
                self.value = original

    def test_cli_exit_codes_for_valid_and_malformed_input(self):
        command = [sys.executable, "-B", str(Path(submitter.__file__).resolve()),
                   "--context-dir", str(self.context), "--input-file", str(self.input),
                   "--check-only"]
        for content, valid in ((json.dumps(self.value), True),
                               ('{"body": "missing required fields"}', False), ("{", False)):
            with self.subTest(content=content):
                self.input.write_text(content)
                result = subprocess.run(command, capture_output=True, text=True, timeout=10)
                self.assertEqual(valid, result.returncode == 0, result.stderr)
                if valid:
                    self.assertTrue(json.loads(result.stdout)["check_only"])
                else:
                    self.assertEqual("", result.stdout)
                self.assertFalse(self.context.exists())


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

    def test_preflight_can_be_corrected_then_submitted_with_the_existing_cli(self):
        submission = self.submission()
        submission["comments"][0]["body"] += "\n- ID: unrelated-finding"
        input_file = self.context / "final.json"
        input_file.write_text(json.dumps(submission))
        arguments = ["--context-dir", str(self.context), "--input-file", str(input_file)]
        with redirect_stdout(io.StringIO()) as output:
            submitter.main(arguments + ["--check-only"])
        self.assertTrue(json.loads(output.getvalue())["comments"][0]["warnings"])
        self.api.assert_not_called()
        self.assertFalse((self.context / submitter.SUBMISSION_FILE).exists())

        submission["comments"][0]["body"] = "[P1] Corrected finding\nOnly the intended evidence."
        input_file.write_text(json.dumps(submission))
        with redirect_stdout(io.StringIO()) as output:
            submitter.main(arguments + ["--check-only"])
        self.assertEqual([], json.loads(output.getvalue())["comments"][0]["warnings"])
        self.api.assert_not_called()
        self.assertFalse((self.context / submitter.SUBMISSION_FILE).exists())

        (self.context / submitter.RUN_FILE).write_text(json.dumps(dict(self.run, deadline=70)))
        with mock.patch.object(submitter.time, "monotonic", return_value=10), \
             redirect_stdout(io.StringIO()) as output:
            submitter.main(arguments)
        self.assertEqual(99, json.loads(output.getvalue())["review_id"])
        self.assertEqual(1, len(self.posts))
        self.assertEqual(submission["comments"], self.posts[0]["comments"])
        self.assertEqual(submission, json.loads((self.context / submitter.SUBMISSION_FILE).read_text()))
        self.assertEqual("failure", self.verify()["state"])

    def test_long_multiline_evidence_and_quoted_markers_are_advisory(self):
        submission = self.submission(2)
        fence = "```"
        body = (
            "[P2] Explain the plan with a complete SQL reproduction\n"
            + fence + "sql\n" + "SELECT key, SUM(value) FROM example GROUP BY key;\n" * 100
            + fence + "\nThe following skill section is quoted intentionally:\n"
            + fence + "text\n## Convergence Rounds\n- ID: example\n" + fence
        )
        submission["comments"][0]["body"] = body
        original = copy.deepcopy(submission)
        self.assertTrue(submitter.check_submission(submission)["comments"][0]["warnings"])
        self.api.assert_not_called()
        self.assertEqual("success", self.submit(submission)["state"])
        self.assertEqual(original, submission)
        self.assertEqual(body, self.posts[0]["comments"][0]["body"])
        self.assertEqual("success", self.verify()["state"])

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
