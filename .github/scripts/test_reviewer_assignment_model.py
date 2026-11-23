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

"""Exercise complete inputs, isolated model execution, local replay and read-only dispatch."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import reviewer_assignment as assignment
import reviewer_assignment_model as model
import replay_reviewer_assignment as replay
from reviewer_assignment_diagnostics import diagnostics_report
from test_reviewer_assignment import FakeGitHub, fixture_config


class ModelReplayTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.tool = self.root / "tool"
        shutil.copytree(assignment.TOOL, self.tool)
        config = fixture_config()
        config["modules"]["search"]["reviewers"] = ["Author", "Alice", "Bob"]
        (self.tool / "reviewer-routing.json").write_text(json.dumps(config))
        self.workspace = self.root / "input"
        self.api = FakeGitHub()
        event = {"action": "labeled", "label": {"name": "need-reviewers"}, "pull_request": {"number": 42}}
        assignment.prepare(self.api, event, "apache/doris", "100", self.workspace, self.tool)
        self.binary = self.root / "codex"
        self.binary.write_text('#!' + sys.executable + r"""
import json, os, sys, time
from pathlib import Path
if sys.argv[1:] == ['--version']:
    print('codex-cli 0.166.0')
    sys.exit(0)
capture = Path(os.environ['FAKE_CAPTURE'])
capture.mkdir(exist_ok=True)
text = sys.stdin.read()
(capture / 'stdin').write_text(text)
(capture / 'argv.json').write_text(json.dumps(sys.argv[1:]))
(capture / 'credentials.json').write_text(json.dumps({k: os.environ.get(k) for k in ['GH_TOKEN','OSS_AK','OSS_SK']}))
print(json.dumps({'type':'turn.started'}), flush=True)
time.sleep(float(os.environ.get('FAKE_SLEEP', '0')))
code = int(os.environ.get('FAKE_CODEX_EXIT', '0'))
if code == 0:
    payload = json.loads(text.split('ROUTING_INPUT_JSON:\n', 1)[1])
    assert payload['complete_diff'] and payload['pr']['files'] and payload['module_catalog']
    result = {'modules':json.loads(os.environ.get('FAKE_MODULES','["search"]')),
              'reason':'The changed query belongs to the selected module.'}
    Path(sys.argv[sys.argv.index('--output-last-message')+1]).write_text(json.dumps(result))
    print(json.dumps({'type':'item.completed','item':{'id':'final','type':'agent_message','text':json.dumps(result)}}))
    print(json.dumps({'type':'turn.completed','usage':{'input_tokens':1000,'cached_input_tokens':800,'output_tokens':120}}))
sys.exit(code)
""")
        self.binary.chmod(0o700)
        self.capture = self.root / "capture"
        self.enterContext(patch.dict(os.environ, {"FAKE_CAPTURE": str(self.capture), "FAKE_CODEX_EXIT": "0",
                                                 "FAKE_SLEEP": "0", "FAKE_MODULES": '["search"]'}))

    def test_inline_input_is_complete_repeatable_and_enumerates_semantic_alternatives(self):
        prompt = (self.workspace / "prompt.txt").read_text()
        payload = json.loads(prompt.split("ROUTING_INPUT_JSON:\n", 1)[1])
        self.assertEqual(payload["complete_diff"], (self.workspace / "pr.diff").read_text())
        self.assertEqual(payload["pr"]["files"][0]["path"], self.api.files[0]["filename"])
        self.assertEqual(payload["pr_details"]["body"], self.api.pr["body"])
        self.assertIn("connector.trino", [m["id"] for m in payload["module_catalog"]])
        self.assertNotIn("connector.trino", payload["candidate_summary"]["path_routing_modules"])
        manifest = (self.workspace / "input-manifest.json").read_text()
        assignment.model_input(self.workspace, self.tool)
        self.assertEqual((self.workspace / "prompt.txt").read_text(), prompt)
        self.assertEqual((self.workspace / "input-manifest.json").read_text(), manifest)
        schema = json.loads((self.workspace / "schema.json").read_text())
        self.assertEqual(schema["properties"]["modules"]["minItems"], 1)
        self.assertIn("reason", schema["required"])

    def test_shared_runner_receives_inline_input_and_cannot_inherit_github_credentials(self):
        with patch.dict(os.environ, {"GH_TOKEN": "private", "OSS_AK": "private", "OSS_SK": "private"}):
            self.assertEqual(model.run_model(self.workspace, self.binary, self.root / "runner"), 0)
        self.assertEqual((self.capture / "stdin").read_text(), (self.workspace / "prompt.txt").read_text())
        args = json.loads((self.capture / "argv.json").read_text())
        self.assertIn("features.shell_tool=false", args)
        self.assertIn("features.apps=false", args)
        self.assertIn("features.plugins=false", args)
        self.assertEqual(json.loads((self.capture / "credentials.json").read_text()),
                         {"GH_TOKEN": None, "OSS_AK": None, "OSS_SK": None})
        self.assertEqual(set(assignment.finalize_selection(self.workspace, self.tool)), {"Alice", "Bob"})

    def test_changed_input_is_rejected_before_model_execution(self):
        (self.workspace / "pr.diff").write_text("changed after hashing")
        with self.assertRaisesRegex(ValueError, "changed after preparation: pr.diff"):
            model.run_model(self.workspace, self.binary, self.root / "runner")
        self.assertFalse(self.capture.exists())

    def test_model_failure_and_real_timeout_retain_exit_metadata(self):
        for label, env, timeout in [("error", {"FAKE_CODEX_EXIT": "17"}, 10),
                                    ("timeout", {"FAKE_SLEEP": "5"}, 0.2)]:
            with self.subTest(label=label), patch.dict(os.environ, env):
                code = model.run_model(self.workspace, self.binary, self.root / label, timeout)
                self.assertEqual(code, 17 if label == "error" else 124)
                run = json.loads((self.workspace / "model-run.json").read_text())
                self.assertEqual(run["exit_code"], code)
                self.assertFalse((self.workspace / "module-selection.json").exists())

    def test_local_snapshot_replay_runs_production_selection_without_github_calls(self):
        target = self.root / "replay"
        with patch.object(assignment, "TOOL", self.tool), \
                patch.object(replay.ReadOnlyGitHub, "request", side_effect=AssertionError("Unexpected GitHub call")):
            self.assertEqual(replay.replay(target, self.binary, snapshot=self.workspace), 0)
        self.assertEqual(set(json.loads((target / "selection.json").read_text())["reviewers"]), {"Alice", "Bob"})
        report = json.loads((target / "diagnostics.json").read_text())
        self.assertFalse(report["github_mutations"])
        self.assertEqual(report["files"]["validation.json"]["value"]["status"], "accepted")

    def test_local_replay_rejects_empty_model_results_and_preserves_diagnostics(self):
        target = self.root / "empty-replay"
        with patch.object(assignment, "TOOL", self.tool), patch.dict(os.environ, {"FAKE_MODULES": "[]"}):
            with self.assertRaisesRegex(ValueError, "despite eligible path modules"):
                replay.replay(target, self.binary, snapshot=self.workspace)
        self.assertFalse((target / "selection.json").exists())
        report = json.loads((target / "diagnostics.json").read_text())
        self.assertEqual(report["files"]["validation.json"]["value"]["status"], "rejected")

    def test_manual_dispatch_is_validated_and_cannot_apply_an_assignment(self):
        event = {"inputs": {"pr_number": "70665"}}
        normalized = assignment.request_event(event, "workflow_dispatch")
        self.assertEqual(normalized["pull_request"]["number"], 70665)
        for number in ["-1", "abc", "1; echo injected", 1]:
            with self.subTest(number=number), self.assertRaisesRegex(ValueError, "positive integer"):
                assignment.request_event({"inputs": {"pr_number": number}}, "workflow_dispatch")
        event_path = self.root / "event.json"
        event_path.write_text(json.dumps(event))
        result = subprocess.run([sys.executable, assignment.__file__, "apply", "--workspace", str(self.workspace)],
                                env={**os.environ, "GITHUB_EVENT_PATH": str(event_path),
                                     "GITHUB_EVENT_NAME": "workflow_dispatch"}, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Manual runs cannot mutate a PR", result.stderr)

    def test_nonfatal_and_mcp_errors_are_retained_without_tool_arguments(self):
        events = [
            {"type":"item.completed","item":{"id":"mcp","type":"mcp_tool_call","server":"local",
             "tool":"read_file","status":"failed","arguments":{"private":"secret-argument"},
             "error":{"message":"access_token=ghp_secret"}}},
            {"type":"item.completed","item":{"id":"error","type":"error","message":"Code mode execution failed"}},
        ]
        report = diagnostics_report(self.workspace, "\n".join(json.dumps(e) for e in events), "")
        self.assertEqual(report["event_types"]["item.completed"], 2)
        self.assertEqual(report["item_types"]["mcp_tool_call"], 1)
        self.assertEqual(report["tool_calls"][0]["status"], "failed")
        self.assertIn("Code mode execution failed", report["model_errors"])
        self.assertNotIn("secret-argument", json.dumps(report))
        self.assertNotIn("ghp_secret", json.dumps(report))

    def test_classifier_reason_is_required_and_bounded(self):
        for reason in [None, "", "   ", "x" * 1025]:
            with self.subTest(reason=reason), self.assertRaises(ValueError):
                model.response_modules({"modules": ["search"], "reason": reason})


if __name__ == "__main__":
    unittest.main()
