#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

from pr_workflow_event import RELAY_PATH, resolve
from resolve_code_review_status import resolve_status

WORKFLOWS = Path(__file__).resolve().parents[1] / "workflows"
HEAD = "a" * 40
BEFORE = "b" * 40
CURRENT = "c" * 40
BASE = "d" * 40


def step_script(workflow, name):
    source = (WORKFLOWS / workflow).read_text().split(f"      - name: {name}\n", 1)[1]
    source = source.split("\n      - ", 1)[0].split("        run: |\n", 1)[1]
    return textwrap.dedent(source)


class RelayTest(unittest.TestCase):
    def setUp(self):
        self.event = {"workflow_run": {
            "event": "pull_request", "conclusion": "success", "path": RELAY_PATH,
            "repository": {"full_name": "apache/doris"}, "head_sha": HEAD,
            "head_repository": {"id": 2}, "head_branch": "contributor-branch", "pull_requests": []}}
        self.relay = {"number": 42, "action": "synchronize", "head_sha": HEAD, "before": BEFORE}
        self.pr = {"number": 42, "state": "open", "merged": False,
                   "head": {"sha": HEAD, "repo": {"id": 2}, "ref": "contributor-branch"},
                   "base": {"repo": {"full_name": "apache/doris"}, "sha": BASE}}
        self.associated = [{"number": 42}]
        self.calls = []

    def api(self, endpoint, *, pages=False):
        self.calls.append((endpoint, pages))
        if endpoint == "repos/apache/doris/pulls/42":
            return copy.deepcopy(self.pr)
        self.assertEqual(endpoint, f"repos/apache/doris/commits/{HEAD}/pulls?per_page=100")
        self.assertTrue(pages)
        return self.associated

    def resolve(self):
        return resolve(self.event, self.relay, "apache/doris", self.api)

    def test_fork_run_without_pull_requests_resolves_current_and_previous_heads(self):
        result = self.resolve()
        self.assertEqual(result["pr_number"], "42")
        self.assertEqual(json.loads(result["head_shas"]), [HEAD, BEFORE])
        self.assertEqual(result["check_large_files"], "true")
        self.assertEqual(len(self.calls), 2)

    def test_stale_run_recomputes_live_head_too(self):
        self.pr["head"]["sha"] = CURRENT
        self.assertEqual(json.loads(self.resolve()["head_shas"]), [CURRENT, HEAD, BEFORE])

    def test_all_lifecycle_actions_preserve_head_sync_but_only_three_warn(self):
        for action in ("opened", "synchronize", "reopened", "edited", "ready_for_review", "closed"):
            with self.subTest(action=action):
                self.relay["action"] = action
                result = self.resolve()
                self.assertEqual(result["check_large_files"],
                                 str(action in {"opened", "synchronize", "reopened"}).lower())
                self.assertEqual(json.loads(result["head_shas"]),
                                 [HEAD, BEFORE] if action == "synchronize" else [HEAD])

    def test_closed_unmerged_fork_uses_source_identity_when_both_association_lists_are_empty(self):
        self.pr["state"] = "closed"
        self.relay["action"] = "closed"
        self.associated = []
        result = self.resolve()
        self.assertEqual(result["check_large_files"], "false")
        self.assertEqual(json.loads(result["head_shas"]), [HEAD])
        self.pr["head"]["repo"] = None  # A contributor can delete the fork after closing.
        self.assertEqual(json.loads(self.resolve()["head_shas"]), [HEAD])

    def test_closed_merged_run_can_use_merge_sha(self):
        self.event["workflow_run"]["head_sha"] = CURRENT
        self.event["workflow_run"]["pull_requests"] = [{"number": 42}]
        self.pr.update(state="closed", merged=True, merge_commit_sha=CURRENT)
        self.relay["action"] = "closed"
        self.assertEqual(json.loads(self.resolve()["head_shas"]), [HEAD])

    def test_closed_pr_releases_shared_head_context(self):
        self.pr["state"] = "closed"
        self.relay["action"] = "closed"
        head = json.loads(self.resolve()["head_shas"])[0]
        other = dict(self.pr, number=43, state="open")
        source = {"id": 1, "state": "success", "context": f"code-review/source/local/pr-43/base-{BASE}"}
        self.assertEqual(resolve_status([dict(self.pr, state="open"), other], [source], head_sha=head).state,
                         "pending")
        self.assertEqual(resolve_status([self.pr, other], [source], head_sha=head).state, "success")

    def test_previous_head_is_only_a_hint_and_cannot_grant_review_pass(self):
        self.relay["before"] = CURRENT
        for head in json.loads(self.resolve()["head_shas"]):
            self.assertEqual(resolve_status([self.pr], [], head_sha=head).state, "pending")

    def test_rejects_foreign_pr_and_mismatched_run_head(self):
        self.associated = []
        self.pr["head"]["repo"]["id"] = 99
        with self.assertRaisesRegex(ValueError, "does not belong"):
            self.resolve()
        self.associated = [{"number": 42}]
        self.relay["head_sha"] = CURRENT
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.resolve()

    def test_rejects_wrong_workflow_repository_or_event(self):
        original = copy.deepcopy(self.event)
        for field, value in (("path", ".github/workflows/unrelated.yml"), ("event", "push"),
                             ("conclusion", "failure"), ("repository", {"full_name": "fork/doris"})):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.event = copy.deepcopy(original)
                self.event["workflow_run"][field] = value
                self.resolve()

    def test_rejects_output_and_endpoint_injection(self):
        original = copy.deepcopy(self.relay)
        for field, value in (("number", "42/comments"), ("number", True), ("action", "labeled"),
                             ("head_sha", "a" * 39), ("before", BEFORE + "\nhead_shas=[]")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.relay = dict(original, **{field: value})
                self.resolve()

    def test_actual_relay_step_emits_event_hints_without_checkout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            event = {"number": 42, "action": "synchronize", "before": BEFORE,
                     "pull_request": {"head": {"sha": HEAD}}}
            (root / "event.json").write_text(json.dumps(event))
            result = subprocess.run(["bash", "-e", "-c", step_script(
                "pull-request-events.yml", "Record PR event hints")], capture_output=True, text=True,
                env={**os.environ, "GITHUB_EVENT_PATH": str(root / "event.json"), "RUNNER_TEMP": str(root)})
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads((root / "pr-event.json").read_text()), self.relay)
        source = (WORKFLOWS / "pull-request-events.yml").read_text()
        self.assertIn("permissions: {}", source)
        self.assertNotIn("actions/checkout", source)
        self.assertNotIn("${{ secrets.", source)

    def test_real_resolver_cli_accepts_valid_data_and_rejects_a_forged_head(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "event.json").write_text(json.dumps(self.event))
            (root / "pr.json").write_text(json.dumps(self.pr))
            gh = root / "gh"
            gh.write_text('#!' + sys.executable + '''
import os, pathlib, sys
if sys.argv[2] == 'repos/apache/doris/pulls/42':
    print(pathlib.Path(os.environ['PR_FIXTURE']).read_text())
else:
    print('[[{"number":42}]]')
''')
            gh.chmod(0o700)
            env = {**os.environ, "PATH": directory + os.pathsep + os.environ["PATH"],
                   "GITHUB_EVENT_PATH": str(root / "event.json"), "GITHUB_EVENT_NAME": "workflow_run",
                   "GITHUB_REPOSITORY": "apache/doris", "PR_FIXTURE": str(root / "pr.json")}
            command = [sys.executable, str(WORKFLOWS.parent / "scripts/pr_workflow_event.py"),
                       "--relay-file", str(root / "relay.json")]
            for head in (HEAD, CURRENT):
                with self.subTest(head=head):
                    (root / "relay.json").write_text(json.dumps(dict(self.relay, head_sha=head)))
                    result = subprocess.run(command, env=env, capture_output=True, text=True)
                    if head == HEAD:
                        self.assertEqual(result.returncode, 0, result.stderr)
                        outputs = dict(line.split("=", 1) for line in result.stdout.splitlines())
                        self.assertEqual(outputs["pr_number"], "42")
                        self.assertEqual(json.loads(outputs["head_shas"]), [HEAD, BEFORE])
                    else:
                        self.assertNotEqual(result.returncode, 0)
                        self.assertEqual(result.stdout, "")

    def test_consumers_use_the_validated_outputs_and_same_run_artifact(self):
        resolver = (WORKFLOWS / "resolve-pr-event.yml").read_text()
        self.assertIn("run-id: ${{ github.event.workflow_run.id }}", resolver)
        self.assertIn("ref: ${{ github.event.repository.default_branch }}", resolver)
        for name in ("lfs-warning.yml", "code-review-sync-result.yml"):
            source = (WORKFLOWS / name).read_text()
            self.assertNotIn("pull_request_target:", source)
            self.assertIn("uses: ./.github/workflows/resolve-pr-event.yml", source)
        source = (WORKFLOWS / "code-review-sync-result.yml").read_text()
        self.assertIn("fromJSON(needs.resolve-event.outputs.head_shas)", source)


class LargeFileCommentTest(unittest.TestCase):
    def test_warning_remains_nonblocking_and_uses_resolved_pr_number(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gh = root / "gh"
            gh.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$RUNNER_TEMP/gh-args"\n')
            gh.chmod(0o700)
            env = {**os.environ, "PATH": directory + os.pathsep + os.environ["PATH"],
                   "RUNNER_TEMP": directory, "PR_NUM": "42"}
            script = step_script("lfs-warning.yml", '"Warn on oversized files in pull requests"')
            for files in ("", "large.dat\t2097152\n"):
                with self.subTest(files=files):
                    (root / "oversized.tsv").write_text(files)
                    result = subprocess.run(["bash", "-e", "-c", script], env=env,
                                            capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual((root / "gh-args").exists(), bool(files))
                    if files:
                        self.assertIn("pr\ncomment\n42\n", (root / "gh-args").read_text())
                        self.assertIn("large.dat (2 MB)", (root / "comment.md").read_text())


if __name__ == "__main__":
    unittest.main()
