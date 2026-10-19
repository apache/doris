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

import json
import os
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

from resolve_code_review_status import resolve_status
from submit_review import validate_submission


WORKFLOW = Path(__file__).resolve().parents[1] / "workflows/code-review-runner.yml"


def step_script(name):
    step = WORKFLOW.read_text().split(f"      - name: {name}\n", 1)[1].split("\n      - name:", 1)[0]
    script = textwrap.dedent(step.split("        run: |\n", 1)[1])
    return re.sub(r"\$\{\{.*?\}\}", "test", script)


class ReviewPromptContractTest(unittest.TestCase):
    def test_prompt_schema_supports_both_delivery_outcomes(self):
        prompt = WORKFLOW.read_text().split("<<'PROMPT'\n", 1)[1].split("          PROMPT\n", 1)[0]
        example = next(line.strip() for line in prompt.splitlines() if line.strip().startswith('{"body":'))
        submission = json.loads(example.replace("<diff_position>", "1"))
        validate_submission(submission)
        validate_submission(dict(submission, converged=False, rounds=3,
                                 incomplete_reason="Round 3 still found candidates."))
        # Regression guard for the clauses that blocked #67549 and #68499.
        for obsolete in (
            "can only be submitted after all subagents return",
            "MUST remain incomplete until every suspicious point",
            "Only finish when there are no unresolved suspicious points",
            "declare the COMPLETE final review",
            "Submit only after all review completion criteria are satisfied",
        ):
            self.assertNotIn(obsolete, prompt)
        self.assertIn("do not start a fourth round", prompt)
        self.assertIn("MUST NOT prevent delivery of verified findings", prompt)


@unittest.skipUnless(shutil.which("jq"), "workflow shell test requires jq")
class ReviewConvergenceWorkflowTest(unittest.TestCase):
    def test_verified_result_flows_through_source_and_aggregate_status(self):
        output_script = step_script("Run automated code review").split("status=$?\n", 1)[1]
        source_script = step_script("Sync Code Review check for current head")
        # Replace the only external mutation with an argument capture. Run the
        # actual Bash/jq blocks, then feed their source into the real resolver.
        capture = """gh() {
  python3 -c 'import json, sys; print(json.dumps(sys.argv[1:]))' "$@"
}
"""
        for converged, p0, p1, expected in (
            (True, 0, 0, "success"),
            (True, 1, 0, "failure"),
            (True, 0, 1, "failure"),
            (False, 0, 0, "failure"),
            (False, 0, 1, "failure"),
        ):
            with self.subTest(converged=converged, p0=p0, p1=p1), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / "review-result.json").write_text(json.dumps({
                    "state": expected, "p0": p0, "p1": p1, "converged": converged,
                    "rounds": 3, "incomplete_reason": "" if converged else "Coverage incomplete",
                }))
                env = {**os.environ, "status": "0", "REVIEW_CONTEXT_DIR": tmp,
                       "GITHUB_OUTPUT": str(root / "output"), "GITHUB_STEP_SUMMARY": str(root / "summary")}
                subprocess.run(["bash", "-e", "-c", output_script], env=env,
                               capture_output=True, text=True, check=True)
                outputs = dict(line.split("=", 1) for line in (root / "output").read_text().splitlines())
                self.assertEqual(str(converged).lower(), outputs["converged"])
                result = subprocess.run(["bash", "-e", "-c", capture + source_script], env={
                    **env, "BASE_SHA": "b" * 40, "HEAD_SHA": "a" * 40,
                    "PR_NUMBER": "123", "REPO": "apache/doris", "JOB_STATUS": "success",
                    "REVIEW_CONTEXT_OUTCOME": "success", "REVIEW_OUTCOME": "success",
                    "REVIEW_STATE": outputs["review_state"], "REVIEW_P0": outputs["p0"],
                    "REVIEW_P1": outputs["p1"], "REVIEW_CONVERGED": outputs["converged"],
                    "REVIEW_ROUNDS": outputs["rounds"],
                }, capture_output=True, text=True, check=True)
                args = json.loads(result.stdout)
                posted = dict(args[i + 1].split("=", 1) for i in range(len(args)) if args[i] == "-f")
                self.assertEqual(expected, posted["state"])
                self.assertIn("repos/apache/doris/statuses/" + "a" * 40, args)
                resolution = resolve_status([{
                    "state": "open", "number": 123, "head": {"sha": "a" * 40}, "base": {"sha": "b" * 40},
                }], [{"id": 1, **posted}], head_sha="a" * 40)
                self.assertEqual(expected, resolution.state)
                if not converged:
                    self.assertIn("did not converge", posted["description"])
                    self.assertIn("did not converge", resolution.description)
                else:
                    self.assertNotIn("did not converge", resolution.description)


if __name__ == "__main__":
    unittest.main()
