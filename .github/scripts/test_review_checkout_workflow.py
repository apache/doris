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

import ast
import re
import unittest
from pathlib import Path

from resolve_code_review_status import resolve_status


WORKFLOWS = Path(__file__).resolve().parents[1] / "workflows"


def expression_value(expression, github):
    """Evaluate the string/boolean subset used by these checkout expressions."""
    expression = expression.strip().removeprefix("${{").removesuffix("}}").strip()
    tree = ast.parse(expression.replace("&&", " and ").replace("||", " or "), mode="eval")

    def visit(node):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name) and node.id == "github":
            return github
        if isinstance(node, ast.Attribute):
            return visit(node.value).get(node.attr, "")
        if isinstance(node, ast.BoolOp):
            for item in node.values:
                value = visit(item)
                if isinstance(node.op, ast.And) and not value:
                    return value
                if isinstance(node.op, ast.Or) and value:
                    return value
            return value
        if isinstance(node, ast.Compare) and len(node.ops) == 1:
            left, right = visit(node.left), visit(node.comparators[0])
            if isinstance(node.ops[0], ast.Eq):
                return left == right
            if isinstance(node.ops[0], ast.NotEq):
                return left != right
        raise AssertionError(f"Unsupported workflow expression: {ast.dump(node)}")

    return visit(tree.body)


class ReviewCheckoutWorkflowTest(unittest.TestCase):
    def setUp(self):
        workflow = (WORKFLOWS / "code-review-aggregate-status.yml").read_text()
        self.step = workflow.split("      - name: Checkout trusted GitHub API diagnostics\n", 1)[1]
        self.step = self.step.split("\n      - name:", 1)[0]
        self.inputs = dict(re.findall(r"^          ([\w-]+): (.*)$", self.step, re.MULTILINE))

    def ref(self, github):
        return expression_value(self.inputs["ref"], github)

    def pr_event(self, *, action="closed", merged=True, same_repo=False, sha="b" * 40):
        return {
            "event_name": "pull_request_target",
            "repository": "apache/doris",
            "ref": "refs/heads/master",
            "sha": sha,
            "workflow_sha": sha,
            "event": {
                "action": action,
                "repository": {"id": 1, "default_branch": "master"},
                "pull_request": {
                    "state": "closed" if action == "closed" else "open",
                    "merged": merged,
                    "head": {"sha": "a" * 40, "repo": {
                        "id": 1 if same_repo else 2,
                        "full_name": "apache/doris" if same_repo else "contributor/doris",
                    }},
                    "merge_commit_sha": sha if merged else "c" * 40,
                },
            },
        }

    def assert_trusted_default_checkout(self, github):
        # An empty ref on the workflow repository makes checkout use the
        # event's trusted ref/SHA. An explicit merge SHA enters its fork guard.
        self.assertEqual("", self.ref(github))
        self.assertNotIn("repository", self.inputs)
        self.assertEqual("refs/heads/master", github["ref"])
        self.assertNotEqual(github["event"]["pull_request"]["head"]["sha"], github["sha"])

    def test_merged_fork_pr_uses_default_checkout_even_when_workflow_sha_is_merge_sha(self):
        # The two production failures: PRs #69223 and #67138.
        for sha in ("707ed45ee8cfde440a1c14ea85b78bc8be9339ce",
                    "e779124b5f60533088730175957944819d22adcb"):
            with self.subTest(sha=sha):
                github = self.pr_event(sha=sha)
                self.assertEqual(github["workflow_sha"],
                                 github["event"]["pull_request"]["merge_commit_sha"])
                self.assert_trusted_default_checkout(github)

    def test_other_pr_target_events_keep_trusted_event_checkout(self):
        for action in ("opened", "synchronize", "reopened", "ready_for_review", "edited", "closed"):
            for same_repo in (False, True):
                with self.subTest(action=action, same_repo=same_repo):
                    self.assert_trusted_default_checkout(
                        self.pr_event(action=action, merged=False, same_repo=same_repo))

    def test_other_callers_keep_workflow_sha_pinning_and_sha_fallback(self):
        for event in ("issue_comment", "workflow_dispatch"):
            for workflow_sha in ("b" * 40, ""):
                with self.subTest(event=event, workflow_sha=workflow_sha):
                    self.assertEqual(workflow_sha or "c" * 40, self.ref({
                        "event_name": event, "workflow_sha": workflow_sha, "sha": "c" * 40,
                    }))

    def test_checkout_does_not_opt_out_of_fork_guard_or_persist_credentials(self):
        self.assertEqual("false", self.inputs["persist-credentials"])
        self.assertEqual("false", self.inputs.get("allow-unsafe-pr-checkout", "false"))
        self.assertEqual(".review-api-source", self.inputs["path"])
        self.assertEqual(".github/scripts/review_github_api.py", self.inputs["sparse-checkout"])

    def test_closed_pr_still_reaches_aggregate_and_releases_shared_head_context(self):
        sync = (WORKFLOWS / "code-review-sync-result.yml").read_text()
        triggers = sync.split("  pull_request_target:\n", 1)[1].split("  issue_comment:", 1)[0]
        self.assertIn("closed", triggers)
        caller = sync.split("\n  sync-status:\n", 1)[1].split("\n  sync-previous-head-status:", 1)[0]
        self.assertIn("uses: ./.github/workflows/code-review-aggregate-status.yml", caller)
        condition = re.search(r"^    if: (.*)$", caller, re.MULTILINE).group(1)
        github = self.pr_event()
        self.assertTrue(expression_value(condition, github))
        self.assert_trusted_default_checkout(github)
        head = github["event"]["pull_request"]["head"]["sha"]
        # A merged PR must no longer hold another open PR at the same head pending.
        pulls = [{"number": n, "state": state, "head": {"sha": head}, "base": {"sha": "d" * 40}}
                 for n, state in ((1, "closed"), (2, "open"))]
        sources = [{"id": 1, "state": "success",
                    "context": "code-review/source/automated/pr-2/base-" + "d" * 40}]
        self.assertEqual("pending", resolve_status(
            [dict(p, state="open") for p in pulls], sources, head_sha=head).state)
        self.assertEqual("success", resolve_status(pulls, sources, head_sha=head).state)


if __name__ == "__main__":
    unittest.main()
