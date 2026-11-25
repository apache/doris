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

"""Exercise the FC receipt-to-status path without GitHub or model credentials."""
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import debug_review as debug


class DebugReviewTest(unittest.TestCase):
    def setUp(self):
        tmp = self.enterContext(tempfile.TemporaryDirectory())
        self.enterContext(patch.dict(os.environ, GITHUB_REPOSITORY="apache/doris", GITHUB_RUN_ID="27",
                                    GITHUB_OUTPUT=str(Path(tmp) / "outputs")))
        self.event = Path(tmp) / "event.json"
        self.enterContext(patch.dict(os.environ, GITHUB_EVENT_PATH=str(self.event)))
        self.event.write_text(json.dumps({"comment": {"body": "/review"}}))
        self.pr = dict(state="open", head={"sha": "a"*40}, base={"sha": "b"*40})
        self.snapshot = dict(pr_number=42, head_sha="a"*40, base_sha="b"*40)
        self.receipt = {"request_id": "github:review:27"}
        self.row = dict(self.snapshot, **self.receipt, preview_only=False)
        self.url = "https://github.com/apache/doris/pull/42#pullrequestreview-89"
        self.result = dict(state="failure", converged=True, rounds=2, review_id=89,
                           review_url=self.url, p0=0, p1=1)
        self.api = self.enterContext(patch.object(debug, "gh", return_value=self.pr))
        self.real_fc = debug.fc
        self.fc = self.enterContext(patch.object(debug, "fc"))
        self.enterContext(patch.object(debug.time, "sleep"))

    def run_review(self, *rows):
        self.fc.side_effect = [self.receipt, *rows]
        debug.review(42, "a"*40, "b"*40)
        self.assertEqual(("", self.snapshot, (202,)), self.fc.call_args_list[0].args)

    def posts(self):
        return [call.args for call in self.api.call_args_list if "-X" in call.args]

    def test_slash_command_binds_the_same_multiline_focus_in_approval_and_submission(self):
        for command, expected in (("/review", ""), ("/review\t重点", "重点"),
                                  ("/REVIEW  检查重试\nsecond line", "检查重试\nsecond line")):
            self.event.write_text(json.dumps({"comment": {"body": command}}))
            self.assertEqual(expected, debug.requested_focus())
        self.assertEqual("", debug.focus_suffix(""))
        self.assertEqual("/" + hashlib.sha256(expected.encode()).hexdigest(), debug.focus_suffix(expected))
        def dispatch(path, payload, codes):
            if not path:
                self.assertEqual(dict(self.snapshot, review_focus=expected), payload)
                self.assertEqual(2, len(self.posts()))
                return self.receipt
            return dict(self.row, state="completed", result=dict(self.result, gate_receipt=True))
        self.fc.side_effect = dispatch
        debug.review(42, "a"*40, "b"*40)
        self.assertTrue(all("state=pending" in post for post in self.posts()))

    def test_slash_review_requires_publication_and_a_durable_verdict(self):
        self.event.write_text(json.dumps({"comment": {"body": "/review"}}))
        for row in (dict(self.row, state="queued", preview_only=True),
                    dict(self.row, state="previewed"), dict(self.row, state="completed", result=self.result)):
            with self.subTest(row=row), self.assertRaises(RuntimeError):
                self.fc.side_effect = [self.receipt, row]
                debug.review(42, "a"*40, "b"*40)
        self.event.write_text(json.dumps({"comment": {"body": "/review " + "重"*2800}}))
        with self.assertRaisesRegex(ValueError, "8192"):
            debug.review(42, "a"*40, "b"*40)
        self.assertTrue(all("state=pending" in post for post in self.posts()))

    def test_retired_command_and_other_slash_commands_do_not_dispatch(self):
        for command in ("run debugreview", "/reviewer", "example /review"):
            with self.subTest(command=command):
                self.event.write_text(json.dumps({"comment": {"body": command}}))
                with self.assertRaisesRegex(ValueError, "/review"):
                    debug.review(42, "a"*40, "b"*40)
                self.api.assert_not_called()
                self.fc.assert_not_called()

    def test_actions_only_marks_aggregate_and_source_pending(self):
        self.run_review(dict(self.receipt, state="awaiting_status"), dict(self.row, state="running"),
                        dict(self.row, state="completed", result=dict(self.result, gate_receipt=True)))
        aggregate, source = self.posts()
        self.assertIn("context=code-review", aggregate)
        self.assertIn("context=code-review/source/automated/pr-42/base-" + "b"*40, source)
        self.assertTrue(all("state=pending" in post for post in (aggregate, source)))

    def test_publisher_records_verified_result_once_after_lost_ack(self):
        run, bot = "27", "cloud[bot]"
        records = []
        def github(path, *args):
            if "/commits/" in path:
                return [[], records]
            if "/pulls/" in path:
                return self.pr
            self.assertEqual("repos/apache/doris/statuses/" + "a"*40, path)
            fields = dict(field.split("=", 1) for field in args[3::2])
            records.append(dict(fields, creator={"login": bot}))
            raise TimeoutError("response lost after write")
        self.api.side_effect = github
        def publish():
            debug.publish_receipt("apache/doris", run, 42, "a"*40, "b"*40, self.result, bot)
        with self.assertRaises(TimeoutError):
            publish()
        publish()
        self.assertEqual(1, len(records))
        self.assertEqual(("code-review/cloud/27/pr-42/base-" + "b"*40, "failure", self.url),
                         tuple(records[0][k] for k in ("context", "state", "target_url")))
        self.assertEqual(1, len(self.posts()))

    def test_publisher_rejects_stale_snapshot_and_invalid_verdict(self):
        self.api.side_effect = [[], dict(self.pr, base={"sha": "c"*40})]
        for result in (self.result, dict(self.result, state="success")):
            with self.assertRaises(ValueError):
                debug.publish_receipt("apache/doris", "27", 42, "a"*40, "b"*40, result, "cloud[bot]")
        self.assertEqual([], self.posts())

    def test_each_fc_request_uses_a_fresh_review_audience_token(self):
        values = [(200, {"value": "first"}), (202, self.receipt),
                  (200, {"value": "next"}), (200, self.row)]
        identity_url = "https://issuer.example/token?job=17"
        with patch.dict(os.environ, ACTIONS_ID_TOKEN_REQUEST_URL=identity_url,
                        ACTIONS_ID_TOKEN_REQUEST_TOKEN="runner"), patch.object(
                            debug, "json_request", side_effect=values) as request:
            self.assertEqual(self.receipt, self.real_fc("", self.snapshot, (202,)))
            self.assertEqual(self.row, self.real_fc("/status", self.receipt, (200, 202)))
        identity = (identity_url + "&audience=urn%3Aapache%3Adoris%3Aci%3Areview", "runner")
        self.assertEqual([identity, (debug.FC, "first", self.snapshot), identity,
                          (debug.FC + "/status", "next", self.receipt)],
                         [c.args for c in request.call_args_list])

    def test_converged_pass_and_incomplete_review(self):
        for change, expected in ((dict(state="success", p1=0), "success"),
                                 (dict(converged=False, rounds=3), "failure")):
            with self.subTest(change=change):
                verdict = debug.verdict(dict(self.result, **change), self.url.split("#")[0], "run", 42)
                self.assertEqual((expected, "run"), (verdict[0], verdict[2]))

    def test_superseded_and_replayed_snapshots(self):
        self.run_review(dict(self.row, state="superseded"))
        for fields in ({"head_sha": "c"*40}, {"base_sha": "c"*40}, {"request_id": "github:review:1"}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                self.run_review(dict(self.row, **fields, state="completed", result=self.result))
        self.assertTrue(all("state=pending" in post for post in self.posts()))

    def test_failure_or_pr_change_never_writes_a_verdict(self):
        with self.assertRaises(RuntimeError):
            self.run_review(dict(self.row, state="failed"))
        self.assertTrue(all("state=pending" in post for post in self.posts()))
        self.api.reset_mock()
        self.fc.reset_mock()
        self.api.return_value = dict(self.pr, base={"sha": "c"*40})
        debug.review(42, "a"*40, "b"*40)
        self.fc.assert_not_called()
        self.assertEqual([], self.posts())

    def test_malformed_success_never_passes(self):
        for fields in (dict(state="success"), dict(review_url="https://example.org/"), dict(p1=False)):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                debug.verdict(dict(self.result, **fields), self.url.split("#")[0], "run", 42)


class ReviewWorkflowTest(unittest.TestCase):
    def test_only_trusted_pr_comments_can_request_a_cloud_review(self):
        workflow = (Path(__file__).resolve().parents[1] / "workflows/code-review-runner.yml").read_text()
        triggers = workflow.split("\non:\n", 1)[1].split("\npermissions:", 1)[0].strip()
        self.assertEqual("issue_comment:\n    types: [created]", triggers)
        jobs = workflow.split("\njobs:\n", 1)[1]
        self.assertEqual(["review-inputs", "authorize-review", "cloud-review", "aggregate-review"],
                         [line.strip()[:-1] for line in jobs.splitlines()
                          if line.startswith("  ") and not line.startswith("   ")])
        self.assertIn("github.event.issue.pull_request", jobs)
        self.assertIn('["OWNER","MEMBER","COLLABORATOR"]', jobs)
        self.assertIn("github.event.comment.body == '/review'", jobs)
        self.assertNotIn("run debugreview", workflow.lower())
        self.assertNotIn("DORIS_REVIEW_BACKEND", workflow)
        self.assertIn("name: fc-review/v1/", jobs)
        self.assertIn("needs: [review-inputs, authorize-review]", jobs)
        self.assertIn("group: code-review-pr-${{ github.event.issue.number }}", jobs)
        self.assertIn("cancel-in-progress: false", jobs)
        self.assertIn("queue: max", jobs)
