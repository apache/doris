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

"""Run manual acceptance and aggregation shell steps with a local fake gh."""

import copy
import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SYNC = ROOT / ".github/workflows/code-review-sync-result.yml"
AGGREGATE = ROOT / ".github/workflows/code-review-aggregate-status.yml"
HEAD = "a" * 40
BASE = "b" * 40


def step(path, name):
    return path.read_text().split(f"      - name: {name}\n", 1)[1].split("\n      - name:", 1)[0]


class ManualReviewWorkflowTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.temp = Path(temporary.name)
        fake = self.temp / "gh"
        fake.write_text(f"#!{sys.executable}\n" + textwrap.dedent('''\
            import json
            import os
            import sys
            from pathlib import Path
            root = Path(os.environ["RUNNER_TEMP"])
            args = sys.argv[1:]
            endpoint = next(arg for arg in args if arg.startswith("repos/"))
            fixture = json.loads((root / "fixture.json").read_text())
            if fixture.get("fail_path") == endpoint or ("POST" in args and fixture.get("post_fail")):
                print("Simulated GitHub API failure", file=sys.stderr)
                sys.exit(7)
            if "POST" in args:
                with (root / "posts.jsonl").open("a") as output:
                    output.write(json.dumps(args) + "\\n")
                print("{}")
            elif "/permission" in endpoint:
                print(json.dumps(fixture["permission"]))
            elif "pulls?state=open" in endpoint:
                print(json.dumps([fixture["pulls"]]))
            elif "/commits/" in endpoint:
                print(json.dumps([fixture["statuses"]]))
            elif "/pulls/" in endpoint:
                print(json.dumps(fixture["pull"]))
            else:
                raise AssertionError(args)
        '''))
        fake.chmod(0o700)
        self.pull = {"number": 123, "state": "open", "head": {"sha": HEAD}, "base": {"sha": BASE}}
        self.event = {
            "action": "created",
            "comment": {"body": f"/force-pass code-review {HEAD}",
                        "user": {"login": "maintainer", "type": "User"}},
            "sender": {"login": "maintainer"},
            "issue": {"number": 123, "pull_request": {"url": "test"}},
        }
        self.manual = {"id": 2, "state": "success",
                       "context": f"code-review/source/manual/pr-123/head-{HEAD}"}
        self.failed = {"id": 1, "state": "failure",
                       "context": f"code-review/source/automated/pr-123/base-{BASE}"}
        self.env = {
            **os.environ, "PATH": f"{self.temp}{os.pathsep}{os.environ['PATH']}",
            "RUNNER_TEMP": str(self.temp), "GH_TOKEN": "fake-token",
            "GITHUB_EVENT_PATH": str(self.temp / "event.json"),
            "GITHUB_OUTPUT": str(self.temp / "output"),
            "REPO": "apache/doris", "PR_NUMBER": "123", "HEAD_SHA": HEAD,
            "COMMENT_AUTHOR": "maintainer",
            "COMMENT_URL": "https://github.com/apache/doris/pull/123#issuecomment-1",
            "REVIEW_GH_HELPER": str(ROOT / ".github/scripts/review_github_api.py"),
            "REVIEW_GITHUB_DIAGNOSTICS_DIR": str(self.temp / "diagnostics"),
            "PYTHONDONTWRITEBYTECODE": "1",
        }

    def run_step(self, path, name, fixture, **env):
        (self.temp / "fixture.json").write_text(json.dumps(fixture))
        (self.temp / "event.json").write_text(json.dumps(self.event))
        (self.temp / "output").write_text("")
        (self.temp / "posts.jsonl").write_text("")
        script = textwrap.dedent(step(path, name).split("        run: |\n", 1)[1])
        script = re.sub(r"\$\{\{.*?\}\}", "test", script)
        result = subprocess.run(
            ["bash", "-eo", "pipefail", "-c", script], cwd=ROOT,
            env={**self.env, **env}, text=True, capture_output=True, timeout=10,
        )
        outputs = dict(line.split("=", 1) for line in (self.temp / "output").read_text().splitlines())
        posts = [json.loads(line) for line in (self.temp / "posts.jsonl").read_text().splitlines()]
        return result, outputs, posts

    def test_authorization_and_failed_write_never_publish_acceptance(self):
        for case in ("valid", "stale", "read-only", "write-failed"):
            with self.subTest(case=case):
                fixture = {"permission": {"permission": "write"}, "pull": copy.deepcopy(self.pull)}
                if case == "stale":
                    fixture["pull"]["head"]["sha"] = "c" * 40
                elif case == "read-only":
                    fixture["permission"]["permission"] = "read"
                elif case == "write-failed":
                    fixture["post_fail"] = True
                result, outputs, posts = self.run_step(SYNC, "Validate and record manual pass", fixture)
                if case == "valid":
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(outputs, {"accepted": "true", "head_sha": HEAD})
                    self.assertEqual(len(posts), 1)
                    self.assertIn(f"context={self.manual['context']}", posts[0])
                    self.assertIn(f"target_url={self.env['COMMENT_URL']}", posts[0])
                else:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertNotIn("accepted", outputs)
                    self.assertEqual(posts, [])

    def test_api_read_failures_leave_diagnostics_without_acceptance(self):
        for target in ("repos/apache/doris/collaborators/maintainer/permission",
                       "repos/apache/doris/pulls/123"):
            with self.subTest(target=target):
                before = set((self.temp / "diagnostics").glob("*.json"))
                fixture = {"permission": {"permission": "write"}, "pull": self.pull, "fail_path": target}
                result, outputs, posts = self.run_step(SYNC, "Validate and record manual pass", fixture)
                self.assertEqual(result.returncode, 7, result.stderr)
                self.assertNotIn("accepted", outputs)
                self.assertEqual(posts, [])
                records = [json.loads(p.read_text()) for p in
                           set((self.temp / "diagnostics").glob("*.json")) - before]
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0]["returncode"], 7)
                self.assertIn("Simulated GitHub API failure", records[0]["stderr"])

    def test_aggregate_preserves_pr_and_head_scope(self):
        self.assertIn("if: steps.resolution.outputs.publish == 'true'",
                      step(AGGREGATE, "Publish aggregate code-review status"))
        for case, pulls, statuses, state, publish in (
            ("manual-over-failure", [self.pull], [self.failed, self.manual], "success", "true"),
            ("new-head", [{**self.pull, "head": {"sha": "c" * 40}}], [self.manual], "pending", "true"),
            ("shared-head", [self.pull, {**self.pull, "number": 124}],
             [self.manual, {**self.failed, "context": self.failed["context"].replace("pr-123", "pr-124")}],
             "failure", "true"),
            ("closed-head", [], [self.manual], "pending", "false"),
        ):
            with self.subTest(case=case):
                fixture = {"pulls": pulls, "statuses": statuses}
                head = "c" * 40 if case == "new-head" else HEAD
                result, outputs, posts = self.run_step(
                    AGGREGATE, "Resolve review status across open PR contexts", fixture, HEAD_SHA=head)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual((outputs["state"], outputs["publish"]), (state, publish))
                self.assertEqual(posts, [])
                if publish == "true":
                    result, _, posts = self.run_step(
                        AGGREGATE, "Publish aggregate code-review status", fixture,
                        HEAD_SHA=head, STATE=state, DESCRIPTION=outputs["description"])
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(len(posts), 1)
                    self.assertIn("context=code-review", posts[0])
                    self.assertIn(f"state={state}", posts[0])

    def test_only_serialized_aggregate_writes_the_final_context(self):
        for path, expected in ((SYNC, 0), (ROOT / ".github/workflows/code-review-runner.yml", 0),
                               (AGGREGATE, 1)):
            with self.subTest(workflow=path.name):
                final_writes = re.findall(r"-f context=['\"]code-review['\"](?:\s|$)", path.read_text())
                self.assertEqual(len(final_writes), expected)
        self.assertIn("group: code-review-status-${{ inputs.head_sha }}", AGGREGATE.read_text())


if __name__ == "__main__":
    unittest.main()
