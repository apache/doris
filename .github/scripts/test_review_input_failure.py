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

"""Run actual input/failure steps without a checkout or real GitHub writes."""

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


def step(name):
    return re.search(rf"^      - name: {re.escape(name)}\n(.*?)(?=^      - name:|\Z)",
                     WORKFLOW.read_text(), re.MULTILINE | re.DOTALL).group(1)


class InputFailureTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.calls = self.root / "calls.jsonl"
        self.output = self.root / "output"
        self.output.touch()
        tool = self.root / "gh"
        tool.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""\
            import json, os, sys
            from pathlib import Path
            with Path(os.environ["GH_CALLS"]).open("a") as handle:
                handle.write(json.dumps(sys.argv[1:]) + "\\n")
            if sys.argv[1] == "api":
                print("gh: HTTP 503", file=sys.stderr)
                sys.exit(1)
            sys.exit(int(os.environ.get("COMMENT_EXIT", "0")))
        """))
        tool.chmod(0o700)
        self.context = {"github.repository": "example/repo", "github.server_url": "https://github.example.test",
                        "github.run_id": "1", "github.event_name": "issue_comment",
                        "github.event.issue.number": "69322", "github.event.comment.body": "/review",
                        "steps.review_inputs.outcome": "failure", "steps.review_context.outcome": "skipped",
                        "steps.review.outcome": "skipped", "secrets.GITHUB_TOKEN": "fake-only"}

    def run_step(self, name, comment_exit=0):
        block = step(name)
        env = dict(os.environ, PATH=f"{self.root}:{os.environ['PATH']}",
                   GH_CALLS=str(self.calls), COMMENT_EXIT=str(comment_exit),
                   GITHUB_OUTPUT=str(self.output),
                   REVIEW_GH_HELPER=str(Path(__file__).with_name("review_github_api.py")),
                   REVIEW_GITHUB_DIAGNOSTICS_DIR=str(self.root / "diagnostics"))
        bindings = block.split("        env:\n", 1)[1].split("        run:", 1)[0]
        for line in bindings.splitlines():
            key, value = line.strip().split(": ", 1)
            env[key] = re.sub(r"\$\{\{\s*(.*?)\s*\}\}", lambda m: self.context.get(m[1], ""), value)
        script = textwrap.dedent(block.split("        run: |\n", 1)[1])
        return subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
                              env=env, cwd=self.root, text=True, capture_output=True, timeout=10)

    def last_comment(self):
        args = json.loads(self.calls.read_text().splitlines()[-1])
        self.assertEqual(args[:2], ["pr", "comment"])
        self.assertEqual(args[args.index("--repo") + 1], "example/repo")
        return args[2], args[args.index("--body") + 1]

    def test_actual_failed_input_step_still_reports_event_pr(self):
        result = self.run_step("Resolve review inputs")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.output.read_text(), "")
        self.assertFalse((self.root / ".git").exists())
        result = self.run_step(COMMENT)
        self.assertEqual(result.returncode, 0, result.stderr)
        number, body = self.last_comment()
        self.assertEqual(number, "69322")
        self.assertIn("'Resolve review inputs' ended with failure", body)
        result = self.run_step(FAIL)
        self.assertEqual(result.returncode, 1)
        self.assertIn("'Resolve review inputs' ended with failure", result.stdout)
        sync = step("Sync Code Review check for current head")
        self.assertIn("steps.review_inputs.outcome == 'success'", sync)
        self.assertIn("needs.code-review.outputs.head_sha != ''", WORKFLOW.read_text())

    def test_dispatch_uses_input_pr_without_issue_event(self):
        self.context.update({"inputs.pr_number": "42", "github.event.issue.number": ""})
        self.assertEqual(self.run_step(COMMENT).returncode, 0)
        self.assertEqual(self.last_comment()[0], "42")

    def test_resolved_pr_takes_precedence_and_existing_failure_reason_survives(self):
        self.context.update({"steps.review_inputs.outputs.pr_number": "42",
                             "inputs.pr_number": "43", "steps.review_inputs.outcome": "success",
                             "steps.auth.outputs.failure_reason": "No eligible credential"})
        self.assertEqual(self.run_step(COMMENT).returncode, 0)
        number, body = self.last_comment()
        self.assertEqual(number, "42")
        self.assertIn("No eligible credential", body)

    def test_cancelled_inputs_are_identified(self):
        self.context["steps.review_inputs.outcome"] = "cancelled"
        self.assertEqual(self.run_step(COMMENT).returncode, 0)
        self.assertIn("'Resolve review inputs' ended with cancelled", self.last_comment()[1])
        result = self.run_step(FAIL)
        self.assertEqual(result.returncode, 1)
        self.assertIn("'Resolve review inputs' ended with cancelled", result.stdout)

    def test_missing_or_invalid_pr_never_posts(self):
        for value in ("", "0", "-1", "branch-name", "42\n43"):
            with self.subTest(value=value):
                self.context["github.event.issue.number"] = value
                result = self.run_step(COMMENT)
                self.assertEqual(result.returncode, 1)
                self.assertFalse(self.calls.exists())
                self.assertIn("No valid PR number", result.stdout)

    def test_failed_notification_is_not_retried_and_terminal_cause_is_preserved(self):
        result = self.run_step(COMMENT, comment_exit=1)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(self.calls.read_text().splitlines()), 1)
        result = self.run_step(FAIL)
        self.assertEqual(result.returncode, 1)
        self.assertIn("'Resolve review inputs' ended with failure", result.stdout)


if __name__ == "__main__":
    unittest.main()
