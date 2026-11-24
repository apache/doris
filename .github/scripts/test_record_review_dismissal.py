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

import unittest
from unittest.mock import patch

from record_review_dismissal import record_dismissal
from resolve_code_review_status import resolve_status
from test_resolve_code_review_status import HEAD_SHA, OTHER_BASE_SHA, pull, status

SERVER = "https://github.com"
REPO = "apache/doris"
REPO_URL = f"{SERVER}/{REPO}"
TITLE = "Dismiss review 99 on PR 123"
BOT = {"login": "github-actions[bot]"}
URL = REPO_URL + "/pull/123#pullrequestreview-99"


class ReviewDismissalTest(unittest.TestCase):
    def setUp(self):
        self.pr = pull(123)
        self.review = {"state": "DISMISSED", "commit_id": HEAD_SHA, "user": BOT}
        self.event = {"event": "review_dismissed", "actor": {"login": "alice", "id": 42, "type": "User"},
                      "dismissed_review": {"review_id": 99, "state": "changes_requested"}}
        self.permission = {"user": {"id": 42}, "permission": "write"}
        self.posts = []
        self.api = self.enterContext(patch("record_review_dismissal.api", side_effect=self.fake_api))

    def fake_api(self, path, *args):
        self.assertTrue(path.startswith(f"repos/{REPO}/"))
        path = path.removeprefix(f"repos/{REPO}/")
        if path == "pulls/123":
            return self.pr
        if path == "pulls/123/reviews/99":
            return self.review
        if path == "issues/123/timeline?per_page=100":
            self.assertEqual(("--paginate", "--slurp"), args)
            return [[{"event": "reviewed"}], [self.event]]
        if path == "collaborators/alice/permission":
            return self.permission
        if path == f"statuses/{HEAD_SHA}":
            self.assertEqual(("-X", "POST"), args[:2])
            self.posts.append(dict(arg.split("=", 1) for arg in args[3::2]))
            return {}
        self.fail(path)

    def test_receipt_matches_only_latest_eligible_source_on_its_pr_and_head(self):
        self.assertEqual(HEAD_SHA, record_dismissal(REPO, SERVER, TITLE, 42))
        receipt = dict(self.posts[0], creator=BOT, id=1)
        source = dict(status(2, source="automated", state="failure"), target_url=URL, creator=BOT,
                      description="Review completed for PR #123: P0=0, P1=2.")

        reviews = [dict(self.review, id=99, body="<!-- code-review-completion:t -->")]

        def resolve(records, pulls=None):
            return resolve_status(pulls or [self.pr], records, head_sha=HEAD_SHA, repo_url=REPO_URL,
                                  reviews_for_pr=lambda number: reviews)

        for records in ([receipt, source], [source, receipt]):
            result = resolve(records)
            self.assertEqual(("success", URL), (result.state, result.target_url))
            self.assertIn("dismissal", result.description)
        for p0, p1 in ((1, 0), (0, 2), (1, 2)):
            with self.subTest(p0=p0, p1=p1):
                finding = dict(source, description=f"Review completed for PR #123: P0={p0}, P1={p1}.")
                self.assertEqual("failure", resolve([finding]).state)
                self.assertEqual("success", resolve([finding, receipt]).state)
        reviews.append(dict(self.review, id=87, state="CHANGES_REQUESTED", body="<!-- code-review-completion:t -->"))
        self.assertEqual("failure", resolve([receipt, source]).state)
        self.assertEqual("success", resolve([receipt, source, status(3, source="skip")]).state)
        reviews[1]["state"] = "DISMISSED"
        self.assertEqual("success", resolve([receipt, source]).state)
        self.assertEqual("pending", resolve([receipt]).state)
        self.assertEqual("pending", resolve([receipt, source], [pull(123, base=OTHER_BASE_SHA)]).state)
        self.assertEqual("pending", resolve([receipt, source], [pull(123), pull(124)]).state)
        self.assertEqual("failure", resolve([receipt, source, status(3, source="automated", pr_number=124,
                         state="failure")], [pull(123), pull(124)]).state)
        self.assertEqual("pending", resolve([receipt, source, dict(source, id=3, state="pending")]).state)
        for changed in (dict(source, id=3, target_url=URL + "0"),
                        dict(source, id=3, target_url=REPO_URL + "/actions/runs/42"),
                        dict(source, id=3, description="Review completed for PR #123: P0=0, P1=0."),
                        dict(source, id=3, description="Review did not converge after 3 rounds"),
                        dict(source, id=3, creator={"login": "other"})):
            self.assertEqual("failure", resolve([source, receipt, changed]).state)
        for changed in (dict(receipt, creator={"login": "other"}), dict(receipt, state="pending"),
                        dict(receipt, target_url=URL + "0")):
            self.assertEqual("failure", resolve([source, changed]).state)

    def test_ignores_unverified_and_automatic_dismissals(self):
        for data, fields in (
            (self.pr, {"state": "closed"}), (self.review, {"commit_id": "c" * 40}),
            (self.review, {"state": "CHANGES_REQUESTED"}), (self.review, {"user": {"login": "other"}}),
            (self.event["dismissed_review"], {"review_id": 100}),
            (self.event["dismissed_review"], {"state": "approved"}),
            (self.event["dismissed_review"], {"dismissal_commit_id": HEAD_SHA}),
            (self.event["actor"], {"type": "Bot"}), (self.event["actor"], {"id": 72}),
            (self.permission, {"permission": "read"}), (self.permission["user"], {"id": 1}),
        ):
            with self.subTest(fields=fields), patch.dict(data, fields):
                self.assertIsNone(record_dismissal(REPO, SERVER, TITLE, 42))
                self.assertFalse(self.posts)

    def test_untrusted_title_is_only_a_numeric_lookup_hint(self):
        self.assertIsNone(record_dismissal(REPO, SERVER, TITLE + "\nextra", 42))
        self.api.assert_not_called()
