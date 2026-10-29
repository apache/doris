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

"""Run the actual workflow shell steps against fake OSS, Codex and GitHub.

Requires bash, jq and GNU coreutils (gdate is accepted on macOS).
Tests invoking the review helper require Linux process supervision.
Run: python3 -m unittest discover -s .github/scripts -p test_review_auth_quarantine.py
No credentials or network access are used.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / "workflows/code-review-runner.yml"
PREFIX = "oss://doris-community-ci/codex/"
INVALIDATED_MESSAGE = "Your session has ended. Please log in again."
INVALIDATED_STDERR = (
    '2026-10-28T06:26:31.373554Z ERROR codex_login::auth::manager: '
    'Failed to refresh token status=401 Unauthorized detail=TokenErrorDetail { '
    'error_code: Some("refresh_token_invalidated"), '
    'error_message: Some("Your session has ended. Please log in again."), .. }'
)
REUSED_MESSAGE = (
    "Your access token could not be refreshed because your refresh token was already used. "
    "Please log out and sign in again."
)


def workflow_step(name):
    match = re.search(
        rf"^      - name: {re.escape(name)}\n(.*?)(?=^      - name:|\Z)",
        WORKFLOW.read_text(), re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"Missing workflow step: {name}")
    return match.group(1)


def step_script(name):
    match = re.search(r"^        run: \|\n((?:          .*\n|\n)+)",
                      workflow_step(name), re.MULTILINE)
    if match is None:
        raise AssertionError(f"Missing shell script: {name}")
    return textwrap.dedent(match.group(1))


FAKE_OSS = r'''
import json
import os
from pathlib import Path
import shutil
import sys

root = Path(os.environ["FAKE_OSS_ROOT"])
args = sys.argv[1:]
op = next(arg for arg in args if arg in ("ls", "cp"))
args = [arg for arg in args[args.index(op) + 1:] if not arg.startswith("-")]
with (root / "calls.jsonl").open("a") as log:
    log.write(json.dumps([op, *args]) + "\n")
prefix = "oss://doris-community-ci/codex/"
if os.environ.get("FAKE_OSS_FAIL") == f"{op}:{args[-1] if op == 'cp' else args[0]}":
    sys.exit(1)
if op == "ls":
    for path in sorted((root / "objects").iterdir()):
        name = prefix + path.name
        if name.startswith(args[0]):
            print(name)
    sys.exit(0)
source, destination = args
if source.startswith(prefix):
    source = root / "objects" / source.removeprefix(prefix)
if destination.startswith(prefix):
    destination = root / "objects" / destination.removeprefix(prefix)
if not Path(source).exists():
    sys.exit(1)
shutil.copyfile(source, destination)
# Simulate a concurrent replacement immediately after a candidate is downloaded.
if os.environ.get("FAKE_OSS_REPLACE_AFTER_DOWNLOAD") == Path(source).name:
    source.write_text(os.environ["FAKE_OSS_REPLACEMENT"])
'''

FAKE_CODEX = r'''
import os
from pathlib import Path
import sys

if os.environ.get("FAKE_FINAL_REVIEW") == "1":
    import json, subprocess
    request = Path(os.environ["REVIEW_CONTEXT_DIR"], "final-input.json")
    request.write_text(json.dumps({"body": "Complete review", "comments": [],
                                   "existing_blocking_comment_ids": [], "converged": True,
                                   "rounds": 1, "incomplete_reason": ""}))
    subprocess.run([sys.executable, str(Path(os.environ["RUNNER_TEMP"], "submit_review.py")),
                    "--context-dir", os.environ["REVIEW_CONTEXT_DIR"], "--input-file", str(request)],
                   check=True, stdout=subprocess.DEVNULL)
print('{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}')
print(os.environ.get("FAKE_CODEX_EVENTS", ""))
print(os.environ.get("FAKE_CODEX_STDERR", ""), file=sys.stderr, flush=True)
# Rotation during a run must not change the identity used for the failure marker.
if "FAKE_CODEX_ROTATED_AUTH" in os.environ:
    Path(os.environ["CODEX_HOME"], "auth.json").write_text(os.environ["FAKE_CODEX_ROTATED_AUTH"])
sys.exit(int(os.environ.get("FAKE_CODEX_STATUS", "1")))
'''

FAKE_GH = r'''
import json
import os
from pathlib import Path
import sys

helper = next((arg.split("/contents/.github/scripts/", 1)[1].split("?ref=", 1)[0]
               for arg in sys.argv if "/contents/.github/scripts/" in arg), None)
if sys.argv[1] == "api" and helper:
    sys.stdout.write(Path(os.environ["FAKE_REVIEW_HELPER"]).with_name(helper).read_text())
elif sys.argv[1] == "api":
    root = f"repos/{os.environ['REPO']}/pulls/{os.environ['PR_NUMBER']}"
    path = sys.argv[2]
    saved = Path(os.environ["REVIEW_CONTEXT_DIR"], "fake-review.json")
    if path == root:
        print(json.dumps({"state": "open", "head": {"sha": os.environ["HEAD_SHA"]},
                          "base": {"sha": os.environ["BASE_SHA"]}}))
    elif path == root + "/reviews":
        if "POST" in sys.argv:
            payload = json.load(sys.stdin)
            review = {"id": 99, "body": payload["body"], "commit_id": payload["commit_id"],
                      "user": {"login": "github-actions[bot]"}, "state": "COMMENTED",
                      "submitted_at": "2099-01-01T00:00:00Z", "html_url": "https://example.test/review/99"}
            saved.write_text(json.dumps(review))
            print(json.dumps(review))
        else:
            print(json.dumps([[json.loads(saved.read_text())] if saved.exists() else []]))
    elif path.endswith("/reviews/99/comments"):
        print("[[]]")
    else:
        raise AssertionError(path)
else:
    Path(os.environ["FAKE_COMMENT_FILE"]).write_text(sys.argv[-1])
'''


class ReviewAuthQuarantineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.objects = self.root / "objects"
        self.objects.mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name, source in (("ossutil", FAKE_OSS), ("codex", FAKE_CODEX), ("gh", FAKE_GH)):
            path = self.bin / name
            path.write_text(f"#!{sys.executable}\n" + source)
            path.chmod(0o700)
        # Deterministic candidate order, so the tests must encounter invalid .2 first.
        shuf = self.bin / "shuf"
        shuf.write_text("#!/bin/sh\nexec cat \"$@\"\n")
        shuf.chmod(0o700)
        if shutil.which("gdate"):
            (self.bin / "date").symlink_to(shutil.which("gdate"))
        self.env = dict(os.environ)
        self.env.update({
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_OSS_ROOT": str(self.root),
            "FAKE_COMMENT_FILE": str(self.root / "comment.txt"),
            "OSS_AK": "fake", "OSS_SK": "fake", "OSS_ENDPOINT": "unused",
            "REPO": "example/repo", "PR_NUMBER": "1", "HEAD_SHA": "a" * 40,
            "GITHUB_WORKSPACE": str(self.root),
            "BASE_SHA": "b" * 40, "HELPER_REF": "test-pin", "REVIEW_TIMEOUT_MINUTES": "120",
            "REVIEW_GH_HELPER": str(Path(__file__).resolve().with_name("review_github_api.py")),
            "FAKE_REVIEW_HELPER": str(Path(__file__).resolve().with_name("run_review_with_resume.py")),
        })
        self.new_runner()

    def new_runner(self):
        runner = Path(tempfile.mkdtemp(dir=self.root))
        shutil.copyfile(self.env["REVIEW_GH_HELPER"], runner / "review_github_api.py")
        context = runner / "review-context"
        context.mkdir()
        (context / "codex_goal_prompt.txt").write_text("Fake review")
        self.env.update({
            "RUNNER_TEMP": str(runner), "CODEX_HOME": str(runner / "codex-home"),
            "GITHUB_ENV": str(runner / "env"), "GITHUB_OUTPUT": str(runner / "output"),
            "REVIEW_CONTEXT_DIR": str(context),
        })
        self.env.pop("CODEX_AUTH_OSS_OBJECT", None)

    def run_step(self, name, expected=0, **env):
        if name == "Run automated code review":
            if sys.platform != "linux":
                self.skipTest("review helper process supervision requires Linux")
            # Each invocation models a separate review step with its own attempt files.
            context = Path(tempfile.mkdtemp(dir=self.env["RUNNER_TEMP"]))
            (context / "codex_goal_prompt.txt").write_text("Fake review")
            self.env["REVIEW_CONTEXT_DIR"] = str(context)
        return self.run_script(step_script(name), expected=expected, **env)

    def run_script(self, script, expected=0, **env):
        output = Path(self.env["GITHUB_OUTPUT"])
        output.write_text("")
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
            env={**self.env, **env}, cwd=self.root, capture_output=True, text=True, timeout=15, check=False,
        )
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        env_file = Path(self.env["GITHUB_ENV"])
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                key, value = line.split("=", 1)
                self.env[key] = value
        return result.stdout + result.stderr, output.read_text()

    def put_auth(self, slot=2, refresh="refresh-old", **fields):
        auth = {"auth_mode": "chatgpt", "tokens": {"access_token": "access-fake", "refresh_token": refresh}}
        auth.update(fields)
        self.object(f"auth.json.{slot}").write_text(json.dumps(auth))
        return auth

    def object(self, name):
        return self.objects / name

    def marker(self, slot=2, refresh="refresh-old"):
        digest = hashlib.sha256(refresh.encode()).hexdigest()
        return self.object(f"auth.json.{slot}.invalid.{digest}")

    def configure(self, expected=0, **env):
        return self.run_step("Configure Codex auth", expected=expected, **env)

    def fail_review(self, events=None, stderr="", expected_invalid=True,
                    expected_reason="refresh_token_reused", **env):
        if events is None:
            events = [{"type": "turn.failed", "error": {"message": REUSED_MESSAGE}}]
        logs, outputs = self.run_step(
            "Run automated code review", expected=1,
            FAKE_CODEX_EVENTS="\n".join(json.dumps(event) for event in events),
            FAKE_CODEX_STDERR=stderr, **env,
        )
        self.assertEqual(f"auth_invalid_reason={expected_reason}\n" in outputs, expected_invalid, outputs)
        return logs, outputs

    def record(self, expected=0, reason="refresh_token_reused", **env):
        return self.run_step("Record invalid Codex auth", expected=expected,
                             AUTH_INVALID_REASON=reason, **env)

    def classify_failure(self, events=(), stderr="", status=1):
        # Execute the real workflow's post-run shell independently of Linux
        # process supervision, so classification and OSS tests also run on macOS.
        context = Path(self.env["REVIEW_CONTEXT_DIR"])
        (context / "codex-events.jsonl").write_text(
            "\n".join(json.dumps(event) for event in events))
        (context / "codex-stderr.log").write_text(stderr)
        (context / "review-result.json").write_text(json.dumps({
            "state": "success", "p0": 0, "p1": 0, "converged": True, "rounds": 1,
        }))
        script = step_script("Run automated code review")
        script = script[script.index('failure_reason=""'):]
        _, outputs = self.run_script(script, expected=int(status != 0), status=str(status))
        return next((line.split("=", 1)[1] for line in outputs.splitlines()
                     if line.startswith("auth_invalid_reason=")), "")

    def test_invalidated_stderr_is_quarantined_and_replacement_is_eligible(self):
        self.put_auth()
        self.put_auth(3, "refresh-other")
        self.configure()
        # The terminal message hides the permanent refresh failure on stderr.
        reason = self.classify_failure(events=[{
            "type": "turn.failed", "error": {"message": "workspace routing discovery unauthorized (401)"},
        }], stderr=INVALIDATED_STDERR)
        self.assertEqual(reason, "refresh_token_invalidated")
        self.record(reason=reason)
        marker = json.loads(self.marker().read_text())
        self.assertEqual(marker["reason"], reason)
        self.assertNotIn("refresh-old", self.marker().read_text())
        self.new_runner()
        logs, _ = self.configure()
        self.assertIn("Skipping Codex auth auth.json.2", logs)
        self.assertEqual(self.env["CODEX_AUTH_OSS_OBJECT"], PREFIX + "auth.json.3")
        self.put_auth(refresh="refresh-new")
        self.new_runner()
        self.configure()
        self.assertEqual(self.env["CODEX_AUTH_OSS_OBJECT"], PREFIX + "auth.json.2")
        self.assertFalse(self.marker(refresh="refresh-new").exists())

    def test_permanent_auth_codes_in_terminal_events_and_stderr(self):
        for code, message in (("refresh_token_reused", REUSED_MESSAGE),
                              ("refresh_token_invalidated", INVALIDATED_MESSAGE)):
            samples = [
                ([{"type": "turn.failed", "error": {"code": code, "message": "Refresh failed"}}], ""),
                ([{"type": "error", "code": code, "message": "Refresh failed"}], ""),
                ([{"type": "error", "message": code}], ""),
                ([{"type": "error", "message": message}], ""),
                ([], json.dumps({"error": {"code": code}})),
                ([], INVALIDATED_STDERR.replace("refresh_token_invalidated", code)),
            ]
            for events, stderr in samples:
                with self.subTest(code=code, events=events, stderr=stderr):
                    self.assertEqual(self.classify_failure(events, stderr), code)

    def test_auth_like_tool_output_and_generic_401_do_not_quarantine(self):
        for message in ("401 Unauthorized", "workspace routing discovery unauthorized (401)",
                        "token_revoked", "Request timed out", "You've hit your usage limit"):
            with self.subTest(message=message):
                self.assertEqual(self.classify_failure(events=[
                    {"type": "item.completed", "item": {"type": "command_execution",
                     "aggregated_output": INVALIDATED_STDERR + INVALIDATED_MESSAGE}},
                    {"type": "turn.failed", "error": {"message": message}},
                ], stderr="ERROR codex_models_manager::manager: auth error code: token_revoked"), "")

    def test_success_does_not_quarantine_prior_invalidated_stderr(self):
        self.assertEqual(self.classify_failure(
            events=[{"type": "error", "code": "refresh_token_invalidated"},
                    {"type": "turn.completed"}], stderr=INVALIDATED_STDERR, status=0), "")

    def test_invalidated_marker_write_failure_and_notification(self):
        self.put_auth()
        self.configure()
        self.record(expected=1, reason="refresh_token_invalidated",
                    FAKE_OSS_FAIL="cp:" + PREFIX + self.marker().name)
        self.assertFalse(self.marker().exists())
        for outcome in ("success", "failure"):
            with self.subTest(outcome=outcome):
                self.run_step(
                    "Comment PR on review failure", AUTH_FAILURE_REASON="",
                    AUTH_INVALID_REASON="refresh_token_invalidated", AUTH_INVALID_RECORD_OUTCOME=outcome,
                    REVIEW_FAILURE_REASON=INVALIDATED_MESSAGE, REVIEW_CONTEXT_OUTCOME="success",
                    REVIEW_OUTCOME="failure", USAGE_LIMIT_RETRY_AFTER_EPOCH="",
                    RUN_URL="https://example.test/run",
                )
                body = Path(self.env["FAKE_COMMENT_FILE"]).read_text()
                self.assertEqual("excluded from future reviews" in body, outcome == "success")
                self.assertEqual("may still be selected" in body, outcome == "failure")

    def test_unknown_auth_reason_cannot_write_marker(self):
        self.put_auth()
        self.configure()
        self.record(expected=1, reason="401 Unauthorized")
        self.assertFalse(self.marker().exists())

    def test_reused_token_is_persisted_and_skipped_on_next_run(self):
        self.put_auth()
        self.put_auth(3, "refresh-other")
        self.configure()
        self.fail_review()
        self.record()
        marker = json.loads(self.marker().read_text())
        self.assertEqual(marker["reason"], "refresh_token_reused")
        self.assertNotIn("refresh-old", self.marker().read_text())
        self.new_runner()
        logs, _ = self.configure()
        self.assertIn("Skipping Codex auth auth.json.2", logs)
        self.assertEqual(self.env["CODEX_AUTH_OSS_OBJECT"], PREFIX + "auth.json.3")
        local_auth = Path(self.env["CODEX_HOME"], "auth.json")
        self.assertEqual(local_auth.read_bytes(), self.object("auth.json.3").read_bytes())
        original = Path(self.env["RUNNER_TEMP"], "codex-auth-original.sha256").read_text().strip()
        self.assertEqual(original, hashlib.sha256(local_auth.read_bytes()).hexdigest())
        self.assertEqual(local_auth.stat().st_mode & 0o777, 0o600)
        logs, _ = self.run_step("Sync refreshed Codex auth back to OSS")
        self.assertIn("not refreshed; skipping", logs)

    def test_replacement_refresh_token_restores_eligibility(self):
        self.put_auth()
        self.configure()
        self.record()
        self.put_auth(refresh="refresh-new")
        self.new_runner()
        self.configure()
        self.assertEqual(self.env["CODEX_AUTH_OSS_OBJECT"], PREFIX + "auth.json.2")
        self.assertTrue(self.marker().exists())
        self.assertFalse(self.marker(refresh="refresh-new").exists())

    def test_metadata_and_formatting_do_not_restore_reused_token(self):
        self.put_auth()
        self.configure()
        self.record()
        auth = self.put_auth(last_refresh="later")
        auth["tokens"]["access_token"] = "access-new"
        self.object("auth.json.2").write_text(json.dumps(auth, indent=4))
        self.new_runner()
        logs, outputs = self.configure(expected=1)
        self.assertIn("No eligible", outputs)
        self.assertNotIn("invalid date", logs)
        self.assertNotIn("Earliest usage-limit retry", outputs)

    def test_late_failure_does_not_quarantine_replacement(self):
        self.put_auth()
        self.configure()
        replacement = self.put_auth(refresh="refresh-new")
        self.fail_review(FAKE_CODEX_ROTATED_AUTH=json.dumps(replacement))
        self.record()
        self.assertTrue(self.marker().exists())
        self.assertFalse(self.marker(refresh="refresh-new").exists())
        self.new_runner()
        self.configure()

    def test_delayed_old_marker_does_not_overwrite_new_token_marker(self):
        self.put_auth()
        self.configure()
        old_env = dict(self.env)
        self.put_auth(refresh="refresh-new")
        self.new_runner()
        self.configure()
        self.record()
        self.env = old_env
        self.record()
        self.record()  # The marker write is idempotent.
        self.assertTrue(self.marker().exists())
        self.assertTrue(self.marker(refresh="refresh-new").exists())
        self.new_runner()
        self.configure(expected=1)

    def test_usage_context_cannot_overwrite_quarantine(self):
        self.put_auth()
        self.configure()
        self.record()
        self.run_step("Record Codex usage limit", RETRY_AFTER_EPOCH="1")
        self.new_runner()
        logs, _ = self.configure(expected=1)
        self.assertIn("Skipping Codex auth auth.json.2", logs)

    def test_mixed_cooldown_and_invalid_pool_reports_both(self):
        self.put_auth()
        self.marker().write_text("{}")
        self.put_auth(3, "refresh-other")
        self.object("auth.json.3.context").write_text(json.dumps({
            "version": 1, "state": "usage_limited", "retry_after_epoch": int(time.time()) + 3600,
        }))
        _, outputs = self.configure(expected=1)
        self.assertIn("Earliest usage-limit retry", outputs)
        self.assertIn("invalid refresh tokens must be replaced", outputs)

    def test_marker_lookup_failure_does_not_select_token(self):
        self.put_auth()
        _, outputs = self.configure(expected=1, FAKE_OSS_FAIL="ls:" + PREFIX + self.marker().name)
        self.assertIn("Failed to check invalid-token marker", outputs)
        self.assertNotIn("CODEX_AUTH_OSS_OBJECT", self.env)

    def test_failed_marker_upload_is_visible(self):
        self.put_auth()
        self.configure()
        logs, _ = self.record(expected=1, FAKE_OSS_FAIL="cp:" + PREFIX + self.marker().name)
        self.assertIn("may still be selected", logs)
        self.assertFalse(self.marker().exists())

    def test_exact_marker_lookup_does_not_confuse_prefix_matches(self):
        self.put_auth()
        self.object(self.marker().name + ".unrelated").write_text("{}")
        self.configure()

    def test_selected_snapshot_is_not_downloaded_again(self):
        original = self.put_auth()
        replacement = json.loads(json.dumps(original))
        replacement["tokens"]["refresh_token"] = "refresh-new"
        self.configure(FAKE_OSS_REPLACE_AFTER_DOWNLOAD="auth.json.2",
                       FAKE_OSS_REPLACEMENT=json.dumps(replacement))
        local = json.loads(Path(self.env["CODEX_HOME"], "auth.json").read_text())
        self.assertEqual(local, original)
        self.record()
        self.new_runner()
        self.configure()

    def test_structured_reused_codes_and_terminal_message(self):
        for event in (
            {"type": "turn.failed", "error": {"code": "refresh_token_reused", "message": "Refresh failed"}},
            {"type": "error", "code": "refresh_token_reused", "message": "Refresh failed"},
            {"type": "error", "message": "refresh_token_reused"},
            {"type": "error", "message": REUSED_MESSAGE},
        ):
            with self.subTest(event=event):
                self.fail_review(events=[event])

    def test_codex_stderr_reused_code(self):
        self.fail_review(events=[], stderr='Token refresh failed: 401 Unauthorized\n{\n  "error": {\n    "code": "refresh_token_reused"\n  }\n}')

    def test_codex_stderr_invalidated_code_survives_runner_failure_summary(self):
        self.fail_review(events=[{
            "type": "error", "message": "workspace routing discovery unauthorized (401)",
        }], stderr=INVALIDATED_STDERR, expected_reason="refresh_token_invalidated")

    def test_unrelated_failures_are_not_quarantined(self):
        for message in ("401 Unauthorized", "We're experiencing high demand", "Request timed out", "You've hit your usage limit"):
            with self.subTest(message=message):
                _, outputs = self.fail_review(
                    events=[{"type": "turn.failed", "error": {"message": message}}], expected_invalid=False,
                )
                if "usage limit" in message:
                    self.assertIn("usage_limit_retry_after_epoch=", outputs)

    def test_tool_output_quoting_reused_error_is_not_quarantined(self):
        self.fail_review(events=[
            {"type": "item.completed", "item": {"type": "command_execution", "aggregated_output": REUSED_MESSAGE}},
            {"type": "turn.failed", "error": {"message": "Request timed out"}},
        ], expected_invalid=False)

    def test_zero_exit_auth_failure_still_reaches_quarantine(self):
        _, outputs = self.fail_review(FAKE_CODEX_STATUS="0")
        self.assertIn(REUSED_MESSAGE, outputs)
        self.assertNotIn("no new pull request review", outputs)

    def test_zero_exit_without_terminal_event_does_not_pass_with_a_review(self):
        _, outputs = self.fail_review(
            events=[], expected_invalid=False, FAKE_CODEX_STATUS="0"
        )
        # The fake GitHub API reports a review, but an incomplete attempt must
        # still fail rather than borrowing that review as proof of completion.
        self.assertIn("without a terminal turn event", outputs)

    def test_success_is_not_quarantined_even_with_earlier_stderr_error(self):
        _, outputs = self.run_step(
            "Run automated code review", FAKE_CODEX_STATUS="0", FAKE_FINAL_REVIEW="1",
            FAKE_CODEX_EVENTS=json.dumps({"type": "turn.completed", "usage": {}}),
            FAKE_CODEX_STDERR='{"code":"refresh_token_reused"}',
        )
        self.assertNotIn("auth_invalid_reason", outputs)

    def test_capacity_after_submission_is_success_without_auth_quarantine(self):
        _, outputs = self.run_step(
            "Run automated code review", FAKE_FINAL_REVIEW="1", FAKE_CODEX_STATUS="1",
            FAKE_CODEX_EVENTS=json.dumps({"type": "turn.failed", "error": {
                "message": "Selected model is at capacity. Please try a different model."
            }}),
        )
        self.assertIn("review_state=success", outputs)
        self.assertNotIn("auth_invalid_reason", outputs)
        self.assertNotIn("failure_reason", outputs)

    def test_sync_separates_execution_failure_from_blocking_findings(self):
        # Exercise the real workflow shell; record POST args instead of writing a status.
        for execution, verdict, expected in (("success", "success", "success"),
                ("success", "failure", "failure"), ("failure", "success", "pending")):
            with self.subTest(execution=execution, verdict=verdict):
                result = subprocess.run(["bash", "-eo", "pipefail", "-c",
                    'python3() { shift; printf "%s\\n" "$@"; };\n' + re.sub(r"\$\{\{.*?\}\}", "test", step_script("Sync Code Review check for current head"))],
                    env={**self.env, "JOB_STATUS": execution, "REVIEW_CONTEXT_OUTCOME": "success",
                         "REVIEW_OUTCOME": execution, "REVIEW_STATE": verdict, "REVIEW_P0": "0", "REVIEW_P1": "1"},
                    capture_output=True, text=True, check=True)
                self.assertIn(f"state={expected}", result.stdout)

    def test_failure_comment_reports_marker_write_outcome(self):
        for outcome in ("success", "failure"):
            with self.subTest(outcome=outcome):
                self.run_step(
                    "Comment PR on review failure", AUTH_FAILURE_REASON="",
                    AUTH_INVALID_REASON="refresh_token_reused", AUTH_INVALID_RECORD_OUTCOME=outcome,
                    REVIEW_FAILURE_REASON=REUSED_MESSAGE, RUN_URL="https://example.test/run",
                )
                body = Path(self.env["FAKE_COMMENT_FILE"]).read_text()
                if outcome == "success":
                    self.assertIn("excluded from future reviews", body)
                else:
                    self.assertIn("may still be selected", body)
                    self.assertNotIn("excluded from future reviews", body)

    def test_marker_step_runs_after_failed_review_with_explicit_detection(self):
        step = workflow_step("Record invalid Codex auth")
        self.assertIn("always() && steps.auth.outcome == 'success'", step)
        self.assertIn("contains(fromJSON(", step)
        self.assertIn('"refresh_token_reused","refresh_token_invalidated"', step)
        self.assertIn("steps.review.outputs.auth_invalid_reason)", step)
        self.assertNotIn("continue-on-error: true", step)


if __name__ == "__main__":
    unittest.main()
