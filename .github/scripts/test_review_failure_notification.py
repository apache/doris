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

"""Exercise failure reporting before checkout using the actual workflow shell."""

import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / "workflows/code-review-runner.yml"
COMMENT = "Comment PR on review failure"
FAIL = "Fail workflow if review failed"


def workflow_step(name):
    match = re.search(
        rf"^      - name: {re.escape(name)}\n(.*?)(?=^      - name:|\Z)",
        WORKFLOW.read_text(), re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"Missing workflow step: {name}")
    return match.group(1)


class ReviewFailureNotificationTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        fake_gh = self.root / "gh"
        fake_gh.write_text(f"#!{sys.executable}\n" + textwrap.dedent('''\
            import json
            import os
            import sys
            from pathlib import Path
            Path(os.environ["GH_CALL"]).write_text(json.dumps(sys.argv[1:]))
            sys.exit(int(os.environ.get("GH_EXIT", "0")))
        '''))
        fake_gh.chmod(0o700)
        self.context = {
            "github.repository": "example/repo",
            "github.server_url": "https://github.example.test",
            "github.run_id": "1234",
            "secrets.GITHUB_TOKEN": "fake-token",
            "steps.review_inputs.outputs.pr_number": "42",
            "steps.pending_status.outcome": "success",
            "steps.checkout.outcome": "success",
            "steps.review_context.outcome": "skipped",
            "steps.review.outcome": "skipped",
        }

    def run_step(self, name, gh_exit=0):
        step = workflow_step(name)
        self.assertIn("always()", step)
        env = {
            **os.environ,
            "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}",
            "GH_CALL": str(self.root / "gh-call.json"),
            "GH_EXIT": str(gh_exit),
        }
        # Render the real env bindings, so a missing/wrong outcome binding fails
        # the behavioral tests instead of being hidden by injected shell vars.
        bindings = step.split("        env:\n", 1)[1].split("        run:", 1)[0]
        for line in bindings.splitlines():
            key, value = line.strip().split(": ", 1)
            env[key] = re.sub(
                r"\$\{\{\s*(.*?)\s*\}\}",
                lambda match: self.context.get(match.group(1), ""), value,
            )
        env["REVIEW_GH_HELPER"] = str(Path(__file__).with_name("review_github_api.py"))
        env["REVIEW_GITHUB_DIAGNOSTICS_DIR"] = str(self.root / "diagnostics")
        script = textwrap.dedent(step.split("        run: |\n", 1)[1])
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
            env=env, cwd=self.root, capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, gh_exit if name == COMMENT else 1,
                         result.stdout + result.stderr)
        if name == COMMENT:
            args = json.loads((self.root / "gh-call.json").read_text())
            self.assertEqual(args[:3], ["pr", "comment", "42"])
            self.assertIn("--repo", args)
            self.assertEqual(args[args.index("--repo") + 1], "example/repo")
            body = args[args.index("--body") + 1]
            self.assertIn("https://github.example.test/example/repo/actions/runs/1234", body)
            return body
        return result.stdout

    def assert_both_report(self, expected, *, gh_exit=0):
        for name in (COMMENT, FAIL):
            with self.subTest(step=name):
                self.assertIn(expected, self.run_step(name, gh_exit=gh_exit))

    def test_pending_status_failure_before_checkout(self):
        self.context.update({
            "steps.pending_status.outcome": "failure",
            "steps.checkout.outcome": "skipped",
        })
        self.assertFalse((self.root / ".git").exists())
        self.assert_both_report("'Mark Code Review status as pending' ended with failure")

    def test_checkout_failure_with_pending_success_or_intentionally_skipped(self):
        self.context["steps.checkout.outcome"] = "failure"
        for outcome in ("success", "skipped"):
            with self.subTest(pending=outcome):
                self.context["steps.pending_status.outcome"] = outcome
                self.assert_both_report("'Checkout repository' ended with failure")

    def test_cancelled_early_steps_are_identified(self):
        for step, label in (("pending_status", "Mark Code Review status as pending"),
                            ("checkout", "Checkout repository")):
            with self.subTest(step=step):
                self.context[f"steps.{step}.outcome"] = "cancelled"
                self.assert_both_report(f"'{label}' ended with cancelled")
                self.context[f"steps.{step}.outcome"] = "success"

    def test_skipped_or_unstarted_context_does_not_claim_context_failure(self):
        for outcome in ("skipped", ""):
            with self.subTest(context=outcome):
                self.context["steps.review_context.outcome"] = outcome
                self.assert_both_report("Review setup stopped before context preparation ran")

    def test_actual_context_failure_and_cancellation(self):
        for outcome, message in (("failure", "Review context preparation failed"),
                                 ("cancelled", "Review context preparation was cancelled")):
            with self.subTest(context=outcome):
                self.context["steps.review_context.outcome"] = outcome
                self.assert_both_report(message)

    def test_auth_and_review_reasons_are_preserved(self):
        self.context["steps.auth.outputs.failure_reason"] = "No eligible Codex account"
        self.assert_both_report("No eligible Codex account")
        self.context.pop("steps.auth.outputs.failure_reason")
        self.context.update({
            "steps.review_context.outcome": "success",
            "steps.review.outcome": "failure",
            "steps.review.outputs.failure_reason": "Final review verification failed",
        })
        self.assert_both_report("Final review verification failed")

    def test_notification_failure_does_not_replace_terminal_cause(self):
        self.context.update({
            "steps.pending_status.outcome": "failure",
            "steps.checkout.outcome": "skipped",
        })
        self.assert_both_report("'Mark Code Review status as pending' ended with failure", gh_exit=7)


if __name__ == "__main__":
    unittest.main()
