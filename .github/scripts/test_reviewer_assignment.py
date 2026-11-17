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

"""Exercise label-event routing and recoverable GitHub mutations without a network."""

import copy
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import reviewer_assignment as assignment
import select_reviewers as selector
from reviewer_candidates import eligible_candidates

NOW = dt.datetime(2026, 11, 17, 12, tzinfo=dt.timezone.utc)
PREFIX = "repos/apache/doris"
ISSUE = PREFIX + "/issues/42"
PULL = PREFIX + "/pulls/42"


def user(login):
    return {"login": login, "type": "User"}


class FakeGitHub:
    def __init__(self):
        self.pr = {"number": 42, "state": "open", "draft": False, "title": "Fix an index query",
                   "body": "PR text is data", "user": user("Author"), "changed_files": 1,
                   "head": {"sha": "a" * 40},
                   "base": {"sha": "b" * 40, "repo": {"full_name": "apache/doris"}},
                   "requested_reviewers": [], "requested_teams": [], "assignees": [user("Existing")]}
        self.files = [{"filename": "be/src/storage/index/inverted/query/query.cpp",
                       "additions": 4, "deletions": 2}]
        self.labels = {"bug", "need-reviewers"}
        self.repo_labels = {"bug", "need-reviewers"}
        self.comments = []
        self.reviews = []
        self.calls = []
        self.fail_once = {}
        self.silently_ignore = set()
        self.unassignable = set()
        self.bot_accounts = set()
        self.flip_head_on_pull_read = None
        self.pull_reads = 0

    def request(self, method, endpoint, payload=None, *, pages=False, raw=False):
        self.calls.append((method, endpoint, copy.deepcopy(payload), pages, raw))
        status = self.fail_once.pop((method, endpoint), None)
        if status:
            raise assignment.GitHubError(status, "Injected API failure")
        if method == "GET" and endpoint == PULL:
            if raw:
                return "diff --git a/query.cpp b/query.cpp\n+fixed_query();\n"
            self.pull_reads += 1
            if self.pull_reads == self.flip_head_on_pull_read:
                self.pr["head"]["sha"] = "c" * 40
            return copy.deepcopy(self.pr)
        if method == "GET" and endpoint == PULL + "/files?per_page=100":
            assert pages
            return copy.deepcopy(self.files)
        if method == "GET" and endpoint == PULL + "/reviews?per_page=100":
            assert pages
            return copy.deepcopy(self.reviews)
        if method == "GET" and endpoint == ISSUE + "/comments?per_page=100":
            assert pages
            return copy.deepcopy(self.comments)
        if method == "GET" and endpoint.startswith("users/"):
            person = endpoint.split("/")[-1]
            return {"login": person, "type": "Bot" if person in self.bot_accounts else "User"}
        if method == "GET" and endpoint.startswith(PREFIX + "/assignees/"):
            if endpoint.split("/")[-1] in self.unassignable:
                raise assignment.GitHubError(404, "This user cannot be assigned")
            return ""
        if method == "GET" and endpoint.startswith(PREFIX + "/labels/"):
            name = endpoint.split("/")[-1]
            if name not in self.repo_labels:
                raise assignment.GitHubError(404, "Missing label")
            return {"name": name}
        if method == "POST" and endpoint == PREFIX + "/labels":
            self.repo_labels.add(payload["name"])
            return payload
        if method == "POST" and endpoint == ISSUE + "/comments":
            result = {"id": len(self.comments) + 1, "body": payload["body"],
                      "user": {"login": assignment.BOT, "type": "Bot"},
                      "html_url": "https://github.com/apache/doris/pull/42#issuecomment-1"}
            self.comments.append(copy.deepcopy(result))
            return result
        if method == "POST" and endpoint == ISSUE + "/assignees":
            existing = {person["login"] for person in self.pr["assignees"]}
            self.pr["assignees"] += [user(p) for p in payload["assignees"]
                                     if p not in existing and p not in self.silently_ignore]
            return copy.deepcopy(self.pr)
        if method == "GET" and endpoint == ISSUE:
            return {"assignees": copy.deepcopy(self.pr["assignees"]),
                    "labels": [{"name": name} for name in sorted(self.labels)]}
        if method == "PATCH" and endpoint.startswith(PREFIX + "/issues/comments/"):
            index = int(endpoint.split("/")[-1]) - 1
            self.comments[index]["body"] = payload["body"]
            return copy.deepcopy(self.comments[index])
        if method == "POST" and endpoint == ISSUE + "/labels":
            self.labels.update(payload["labels"])
            return [{"name": name} for name in self.labels]
        if method == "DELETE" and endpoint == ISSUE + "/labels/need-reviewers":
            self.labels.discard("need-reviewers")
            return []
        raise AssertionError((method, endpoint, payload, pages, raw))


class AssignmentTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.tool = self.root / "tool"
        self.tool.mkdir()
        self.workspace = self.root / "workspace"
        self.api = FakeGitHub()
        self.mapping = {"as_of": "2026-11-17T09:26:38Z", "modules": [{"id": "search", "candidates": [
            {"login": person, "auto_eligible": True, "last_review": "2026-11-10T00:00:00Z"}
            for person in ("Author", "Alice", "Bob")]}]}
        self.save_mapping()
        (self.tool / "README.md").write_text("Trusted guide")
        self.event = {"action": "labeled", "label": {"name": "need-reviewers"},
                      "pull_request": {"number": 42}}

    def save_mapping(self):
        (self.tool / "module-reviewers.json").write_text(json.dumps(self.mapping))

    def prepare(self, run="100"):
        return assignment.prepare(self.api, self.event, "apache/doris", run,
                                  self.workspace, self.tool, NOW)

    def context(self):
        return json.loads((self.workspace / "context.json").read_text())

    def apply(self, people=("Alice", "Bob")):
        return assignment.apply(self.api, self.context(), {"reviewers": list(people)}, self.tool, NOW)

    def mutations(self):
        return [call for call in self.api.calls if call[0] != "GET"]

    def test_prepare_reads_guides_data_and_all_pages_without_mutations(self):
        self.api.pr["draft"] = True  # The explicit label also routes draft PRs.
        self.api.files *= 1
        self.api.reviews = [{"user": user("Bob"), "state": "COMMENTED"}]
        self.assertFalse(self.prepare())
        self.assertEqual(self.mutations(), [])
        self.assertTrue((self.workspace / "tools/reviewer-routing/README.md").exists())
        prompt = (self.workspace / "prompt.txt").read_text()
        self.assertIn("ADD ASSIGNEES", prompt)
        self.assertIn("REVIEWER_RULES.md", prompt)
        self.assertEqual(json.loads((self.workspace / "schema.json").read_text())[
            "properties"]["reviewers"]["maxItems"], 2)
        self.assertEqual(json.loads((self.workspace / "pr.json").read_text())["existing_reviewers"], ["Bob"])

    def test_relabeling_creates_a_new_assignment_despite_done_label(self):
        self.prepare()
        self.apply()
        self.assertEqual(self.api.labels, {"bug", "reviewers-assigned"})
        self.api.labels.add("need-reviewers")
        self.assertFalse(self.prepare(run="101"))
        self.apply(people=("Bob",))
        self.assertEqual(len(self.api.comments), 2)
        self.assertEqual(self.api.labels, {"bug", "reviewers-assigned"})
        self.assertIn("@Bob, please review this PR.", self.api.comments[1]["body"])

    def test_same_run_retry_reuses_pair_and_does_not_reping(self):
        self.prepare()
        result = self.apply()
        calls_before = len(self.mutations())
        self.assertTrue(self.prepare())
        self.apply()
        self.assertEqual(len(self.api.comments), 1)
        self.assertEqual(len(self.mutations()), calls_before + 1)  # Idempotent label addition only.
        self.assertTrue(result["assigned"])
        self.assertEqual({p["login"] for p in self.api.pr["assignees"]}, {"Existing", "Alice", "Bob"})

    def test_invalid_choices_do_not_mutate_github(self):
        self.prepare()
        for selected in ([], ["Alice", "Bob", "Author"], ["Author"], ["Mallory"], ["Alice", "ALICE"], [1]):
            with self.subTest(selected=selected), self.assertRaises(ValueError):
                self.apply(selected)
        self.assertEqual(self.mutations(), [])

    def test_invalid_output_schema_is_rejected(self):
        allowed = {"alice": "Alice"}
        for value in ([], {"reviewers": "Alice"}, {"reviewers": ["Alice"], "comment": "injected"}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                assignment.validate_selection(value, "Author", allowed)

    def test_case_normalization(self):
        self.prepare()
        self.assertEqual(self.apply(("aLiCe",))["reviewers"], ["Alice"])

    def test_bot_or_unassignable_user_is_rejected_before_writes(self):
        self.prepare()
        self.api.bot_accounts.add("Alice")
        with self.assertRaisesRegex(ValueError, "human"):
            self.apply()
        self.api.bot_accounts.clear()
        self.api.unassignable.add("Bob")
        with self.assertRaises(assignment.GitHubError):
            self.apply()
        self.assertEqual(self.mutations(), [])

    def test_silent_assignee_failure_does_not_complete_comment_or_labels(self):
        self.prepare()
        self.api.silently_ignore.add("Bob")
        with self.assertRaisesRegex(ValueError, "did not assign"):
            self.apply()
        self.assertEqual(self.api.labels, {"bug", "need-reviewers"})
        self.assertNotIn("please review", self.api.comments[0]["body"])
        self.api.silently_ignore.clear()
        self.assertTrue(self.prepare())
        self.apply()
        self.assertEqual(len(self.api.comments), 1)
        self.assertIn("@Alice, @Bob, please review this PR.", self.api.comments[0]["body"])

    def test_label_failure_retries_without_another_mention(self):
        self.prepare()
        self.api.fail_once[("POST", ISSUE + "/labels")] = 403
        with self.assertRaises(assignment.GitHubError):
            self.apply()
        self.assertIn("need-reviewers", self.api.labels)
        self.assertTrue(self.prepare())
        self.apply()
        self.assertEqual(len(self.api.comments), 1)
        patches = [c for c in self.api.calls if c[0] == "PATCH"]
        self.assertEqual(len(patches), 1)

    def test_comment_failure_keeps_request_label_and_recovers_checkpoint(self):
        self.prepare()
        self.api.fail_once[("PATCH", PREFIX + "/issues/comments/1")] = 500
        with self.assertRaises(assignment.GitHubError):
            self.apply()
        self.assertEqual(self.api.labels, {"bug", "need-reviewers"})
        self.assertTrue(self.prepare())
        self.apply()
        self.assertEqual(len(self.api.comments), 1)

    def test_rerun_cannot_change_checkpoint_choices(self):
        self.prepare()
        self.apply(("Alice",))
        with self.assertRaisesRegex(ValueError, "cannot change"):
            self.apply(("Bob",))

    def test_regular_user_cannot_forge_a_checkpoint(self):
        self.prepare()
        self.api.comments.append({"id": 99, "user": user("Mallory"),
                                  "body": assignment.marker(self.context(), ["Bob"])})
        self.assertFalse(self.prepare())

    def test_changed_or_closed_pr_is_not_mutated(self):
        self.prepare()
        self.api.pr["head"]["sha"] = "d" * 40
        with self.assertRaisesRegex(ValueError, "head changed"):
            self.apply()
        self.api.pr["head"]["sha"] = "a" * 40
        self.api.pr["state"] = "closed"
        with self.assertRaisesRegex(ValueError, "no longer open"):
            self.apply()
        self.assertEqual(self.mutations(), [])

    def test_head_change_during_preparation_is_rejected(self):
        self.api.flip_head_on_pull_read = 2
        with self.assertRaisesRegex(ValueError, "head changed"):
            self.prepare()

    def test_truncated_files_fail_before_codex(self):
        self.api.pr["changed_files"] = 3228
        with self.assertRaisesRegex(ValueError, "Incomplete PR file"):
            self.prepare()
        self.assertFalse((self.workspace / "prompt.txt").exists())

    def test_stale_or_unqualified_candidates_are_rejected(self):
        self.mapping["modules"][0]["candidates"][1]["auto_eligible"] = False
        self.mapping["modules"][0]["candidates"][2]["last_review"] = "2025-01-01T00:00:00Z"
        self.save_mapping()
        with self.assertRaisesRegex(ValueError, "non-author"):
            self.prepare()
        self.mapping["as_of"] = "2026-01-01T00:00:00Z"
        self.save_mapping()
        with self.assertRaisesRegex(ValueError, "expired"):
            self.prepare()

    def test_other_labels_are_not_routed(self):
        self.event["label"]["name"] = "bug"
        with self.assertRaisesRegex(ValueError, "labeled events"):
            self.prepare()
        self.assertEqual(self.api.calls, [])

    def test_workflow_honors_override_despite_old_unqualified_history(self):
        module = self.mapping["modules"][0]
        module["reviewer_override"] = ["Alice"]
        module["candidates"][1].update(auto_eligible=False, last_review="2025-01-01T00:00:00Z")
        self.save_mapping()
        self.prepare()
        allowed = assignment.candidates(self.tool, NOW)
        self.assertEqual(allowed, {"alice": "Alice"})
        with self.assertRaisesRegex(ValueError, "maintainer override"):
            assignment.validate_selection({"reviewers": ["Bob"]}, "Author", allowed)
        self.assertTrue(self.apply(("Alice",))["assigned"])

    def test_queued_labeled_event_runs_even_if_prior_run_removed_the_label(self):
        self.api.labels = {"bug", "reviewers-assigned"}
        self.assertFalse(self.prepare(run="102"))
        self.apply(("Alice",))
        self.assertEqual(len(self.api.comments), 1)
        self.assertEqual(self.api.labels, {"bug", "reviewers-assigned"})

    def test_concurrent_request_label_removal_is_already_complete(self):
        self.prepare()
        original = self.api.request
        def request(method, endpoint, payload=None, **kwargs):
            if method == "DELETE":
                self.api.labels.remove("need-reviewers")
                raise assignment.GitHubError(404, "An operator already removed the label")
            return original(method, endpoint, payload, **kwargs)
        self.api.request = request
        self.assertTrue(self.apply()["assigned"])
        self.assertEqual(self.api.labels, {"bug", "reviewers-assigned"})


class TransportTest(unittest.TestCase):
    def result(self, stdout):
        result = subprocess.CompletedProcess([], 0, stdout=stdout, stderr="")
        result.github_response = {}
        return result

    def test_pagination_is_requested_and_flattened(self):
        with patch.object(assignment, "run_gh", return_value=self.result('[[{"id":1}],[{"id":2}]]')) as run:
            self.assertEqual(assignment.GitHub().request("GET", "repos/apache/doris/issues/42/comments", pages=True),
                             [{"id": 1}, {"id": 2}])
            self.assertIn("--paginate", run.call_args.args[0])
            self.assertIn("--slurp", run.call_args.args[0])
            self.assertEqual(run.call_args.kwargs["timeout"], 90)

    def test_write_payload_is_json_stdin(self):
        with patch.object(assignment, "run_gh", return_value=self.result('{}')) as run:
            assignment.GitHub().request("POST", "repos/apache/doris/issues/42/assignees", {"assignees": ["Alice"]})
            self.assertEqual(run.call_args.args[0][-2:], ["--input", "-"])
            self.assertEqual(json.loads(run.call_args.kwargs["input"]), {"assignees": ["Alice"]})

    def test_known_http_failure_is_exposed_without_swallowing(self):
        failure = subprocess.CalledProcessError(1, ["gh"], stderr="GitHub GET failed; HTTP=404; request_id=example")
        with patch.object(assignment, "run_gh", side_effect=failure), self.assertRaises(assignment.GitHubError) as error:
            assignment.GitHub().request("GET", "repos/apache/doris/labels/missing")
        self.assertEqual(error.exception.status, 404)


class ReviewerOverrideTest(unittest.TestCase):
    EXPECTED = {
        "connector.trino": ["morningman", "Gabriel39", "924060929"],
        "connector.fluss": ["morningman", "Gabriel39"],
        "connector.mongodb": ["Gabriel39"],
        "runtime.memory": ["yiguolei", "mrhhsg"],
    }
    PATHS = {
        "connector.trino": "fe/fe-connector/fe-connector-trino/src/main/java/TrinoConnector.java",
        "connector.fluss": "fe/fe-connector/fe-connector-fluss/src/main/java/FlussCatalog.java",
        "connector.mongodb": "fe/fe-connector/fe-connector-mongodb/src/main/java/MongoDBCatalog.java",
        "runtime.memory": "be/src/runtime/memory/mem_tracker_limiter.cpp",
    }

    def setUp(self):
        self.mapping = json.loads((assignment.TOOL / "module-reviewers.json").read_text())
        self.modules = {module["id"]: module for module in self.mapping["modules"]}

    def pr(self, module, author="Contributor", unavailable=()):
        return {"number": 42, "author": author, "files": [self.PATHS[module]],
                "unavailable_reviewers": list(unavailable)}

    def test_exact_maintainer_lists_and_order_are_preserved(self):
        for module, expected in self.EXPECTED.items():
            with self.subTest(module=module):
                data = self.modules[module]
                self.assertEqual(data["reviewer_override"], expected)
                self.assertEqual(data["recommended"], expected)
                self.assertEqual(data["confidence"], "maintainer")
                self.assertEqual([p["login"] for p in eligible_candidates(data, NOW)], expected)

    def test_each_module_selects_its_first_configured_reviewer(self):
        for module, expected in self.EXPECTED.items():
            with self.subTest(module=module):
                result = selector.plan(self.pr(module), self.mapping, NOW, limit=2)
                self.assertEqual([p["login"] for p in result["reviewers"]], expected[:1])
                self.assertFalse(result["uncovered_modules"])
                evidence = result["reviewers"][0]["evidence"][module]
                self.assertEqual(evidence["selection_source"], "maintainer_override")
                self.assertEqual(evidence["confidence"], "maintainer")

    def test_author_exclusion_uses_the_next_configured_person(self):
        for module, expected in self.EXPECTED.items():
            with self.subTest(module=module):
                result = selector.plan(self.pr(module, author=expected[0].upper()), self.mapping, NOW)
                self.assertEqual([p["login"] for p in result["reviewers"]], expected[1:2])
                self.assertEqual(result["uncovered_modules"], [module] if len(expected) == 1 else [])

    def test_exhausted_overrides_do_not_fall_back_to_historical_names(self):
        for module, expected in self.EXPECTED.items():
            with self.subTest(module=module):
                result = selector.plan(self.pr(module, unavailable=expected), self.mapping, NOW)
                self.assertEqual(result["reviewers"], [])
                self.assertEqual(result["uncovered_modules"], [module])

    def test_multi_module_coverage_can_reuse_a_configured_reviewer(self):
        pr = self.pr("connector.trino")
        pr["files"].append(self.PATHS["connector.mongodb"])
        result = selector.plan(pr, self.mapping, NOW, limit=2)
        self.assertEqual([p["login"] for p in result["reviewers"]], ["Gabriel39"])
        self.assertEqual(set(result["reviewers"][0]["covers"]), {"connector.trino", "connector.mongodb"})

    def test_overrides_do_not_rewrite_historical_evidence(self):
        original = copy.deepcopy(self.mapping)
        trino = self.modules["connector.trino"]
        morningman = next(p for p in trino["candidates"] if p["login"] == "morningman")
        self.assertFalse(morningman["auto_eligible"])
        self.assertGreater((NOW - assignment.timestamp(morningman["last_review"])).days, 180)
        result = selector.plan(self.pr("connector.trino"), self.mapping, NOW)
        evidence = result["reviewers"][0]["evidence"]["connector.trino"]
        self.assertEqual(evidence["review_prs"], morningman["review_prs"])
        self.assertEqual(evidence["last_review"], morningman["last_review"])
        self.assertEqual(self.mapping, original)

    def test_maintainer_without_historical_records_can_be_selected(self):
        mapping = {"as_of": self.mapping["as_of"], "repository": "apache/doris", "modules": [
            {"id": "runtime.memory", "candidates": [], "reviewer_override": ["NewMaintainer"]}]}
        result = selector.plan(self.pr("runtime.memory"), mapping, NOW)
        self.assertEqual([p["login"] for p in result["reviewers"]], ["NewMaintainer"])

    def test_history_routing_and_global_expiry_remain_in_effect(self):
        pr = {"number": 42, "author": "Contributor", "files": ["be/src/storage/index/inverted/query/query.cpp"]}
        result = selector.plan(pr, self.mapping, NOW)
        self.assertEqual(result["reviewers"][0]["login"], "eldenmoon")
        self.assertEqual(result["reviewers"][0]["evidence"]["search"]["selection_source"], "history")
        with self.assertRaisesRegex(AssertionError, "older than 90"):
            selector.plan(self.pr("connector.trino"), self.mapping, NOW + dt.timedelta(days=91))


class AuthPoolShellTest(unittest.TestCase):
    PREFIX = "oss://doris-community-ci/codex/"

    def setUp(self):
        # Reuse the reference workflow's fake OSS protocol and snapshot-race fixture.
        from test_review_auth_quarantine import FAKE_OSS
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.objects = self.root / "objects"
        self.objects.mkdir()
        self.runner = self.root / "runner"
        self.runner.mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        binary = self.bin / "ossutil"
        binary.write_text("#!" + sys.executable + "\n" + FAKE_OSS)
        binary.chmod(0o700)
        shuffle = self.bin / "shuf"
        shuffle.write_text('#!/bin/sh\nexec cat "$@"\n')
        shuffle.chmod(0o700)
        if shutil.which("gdate"):
            (self.bin / "date").symlink_to(shutil.which("gdate"))
        self.env = {**os.environ, "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
                    "RUNNER_TEMP": str(self.runner), "FAKE_OSS_ROOT": str(self.root),
                    "GITHUB_ENV": str(self.runner / "env"), "GITHUB_OUTPUT": str(self.runner / "output"),
                    "OSS_AK": "test-ak", "OSS_SK": "test-sk", "OSS_ENDPOINT": "example"}
        self.env.pop("CODEX_AUTH_OSS_OBJECT", None)
        Path(self.env["GITHUB_ENV"]).touch()
        Path(self.env["GITHUB_OUTPUT"]).touch()
        self.log = self.root / "calls.jsonl"
        self.original = self.put_auth()

    def put_auth(self, slot=2, refresh="test-refresh"):
        value = {"auth_mode": "chatgpt", "tokens": {"access_token": "test-access", "refresh_token": refresh}}
        self.object(f"auth.json.{slot}").write_text(json.dumps(value))
        return value

    def object(self, name):
        return self.objects / name

    def invalid_marker(self, refresh="test-refresh", slot=2):
        digest = hashlib.sha256(refresh.encode()).hexdigest()
        return self.object(f"auth.json.{slot}.invalid.{digest}")

    def step_script(self, name):
        workflow = (assignment.ROOT / ".github/workflows/reviewer-assignment.yml").read_text()
        step = workflow.split("      - name: " + name + "\n", 1)[1].split("\n      - name:", 1)[0]
        body = step.split("        run: |\n", 1)[1]
        return "\n".join(line[10:] if line.startswith("          ") else line for line in body.splitlines())

    def run_step(self, name):
        result = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", self.step_script(name)],
                                env=self.env, capture_output=True, text=True, timeout=15)
        for line in Path(self.env["GITHUB_ENV"]).read_text().splitlines():
            key, value = line.split("=", 1)
            self.env[key] = value
        return result

    def configure(self):
        result = self.run_step("Configure Codex auth")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.local = Path(self.env["CODEX_HOME"]) / "auth.json"
        self.log.write_text("")
        return result

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def test_reuses_existing_numbered_auth_without_additional_configuration(self):
        self.configure()
        self.assertEqual(self.env["CODEX_AUTH_OSS_OBJECT"], self.PREFIX + "auth.json.2")
        self.assertEqual(json.loads(self.local.read_text()), self.original)
        self.assertIn('inherit = "none"', self.local.with_name("config.toml").read_text())
        self.assertEqual(self.local.stat().st_mode & 0o777, 0o600)

    def test_shared_invalid_token_marker_skips_that_token(self):
        self.invalid_marker().write_text("{}")
        self.put_auth(slot=3, refresh="other-refresh")
        self.configure()
        self.assertEqual(self.env["CODEX_AUTH_OSS_OBJECT"], self.PREFIX + "auth.json.3")

    def test_replacement_refresh_token_restores_same_slot(self):
        self.invalid_marker().write_text("{}")
        replacement = self.put_auth(refresh="replacement-refresh")
        self.configure()
        self.assertEqual(json.loads(self.local.read_text()), replacement)
        self.assertEqual(self.env["CODEX_AUTH_OSS_OBJECT"], self.PREFIX + "auth.json.2")

    def test_shared_future_cooldown_skips_usage_limited_account(self):
        self.object("auth.json.2.context").write_text(json.dumps({
            "version": 1, "state": "usage_limited",
            "retry_after_epoch": int(dt.datetime.now(dt.timezone.utc).timestamp()) + 3600}))
        self.put_auth(slot=3, refresh="other-refresh")
        self.configure()
        self.assertEqual(self.env["CODEX_AUTH_OSS_OBJECT"], self.PREFIX + "auth.json.3")

    def test_expired_cooldown_does_not_block_account(self):
        self.object("auth.json.2.context").write_text(json.dumps({
            "version": 1, "state": "usage_limited",
            "retry_after_epoch": int(dt.datetime.now(dt.timezone.utc).timestamp()) - 60}))
        self.configure()
        self.assertEqual(self.env["CODEX_AUTH_OSS_OBJECT"], self.PREFIX + "auth.json.2")

    def test_invalid_context_does_not_disable_account(self):
        self.object("auth.json.2.context").write_text('{"version":2,"state":"unknown"}')
        result = self.configure()
        self.assertIn("Ignoring unusable Codex auth context", result.stdout)
        self.assertEqual(self.env["CODEX_AUTH_OSS_OBJECT"], self.PREFIX + "auth.json.2")

    def test_metadata_and_nonnumeric_objects_are_not_auth_candidates(self):
        self.object("auth.json.2").unlink()
        self.object("auth.json.backup").write_text(json.dumps(self.original))
        self.object("auth.json.3.context").write_text(json.dumps(self.original))
        result = self.run_step("Configure Codex auth")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("No numbered Codex review auth objects", result.stdout)
        self.assertNotIn("CODEX_AUTH_OSS_OBJECT", self.env)

    def test_all_invalid_accounts_fail_without_selecting_a_token(self):
        self.invalid_marker().write_text("{}")
        result = self.run_step("Configure Codex auth")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("No eligible Codex review credentials", result.stdout)
        self.assertNotIn("CODEX_AUTH_OSS_OBJECT", self.env)

    def test_marker_lookup_failure_does_not_select_unchecked_token(self):
        self.env["FAKE_OSS_FAIL"] = "ls:" + self.PREFIX + self.invalid_marker().name
        result = self.run_step("Configure Codex auth")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Failed to check invalid-token marker", result.stdout)
        self.assertNotIn("CODEX_AUTH_OSS_OBJECT", self.env)

    def test_selected_checked_snapshot_is_not_downloaded_again(self):
        replacement = copy.deepcopy(self.original)
        replacement["tokens"]["refresh_token"] = "operator-refresh"
        self.env["FAKE_OSS_REPLACE_AFTER_DOWNLOAD"] = "auth.json.2"
        self.env["FAKE_OSS_REPLACEMENT"] = json.dumps(replacement)
        self.configure()
        self.assertEqual(json.loads(self.local.read_text()), self.original)
        self.assertEqual(json.loads(self.object("auth.json.2").read_text()), replacement)

    def test_unchanged_auth_does_not_access_oss(self):
        self.configure()
        self.assertEqual(self.run_step("Sync refreshed Codex auth").returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_refreshed_auth_is_saved_to_the_same_selected_object(self):
        self.configure()
        refreshed = copy.deepcopy(self.original)
        refreshed["tokens"]["refresh_token"] = "replacement-refresh"
        self.local.write_text(json.dumps(refreshed))
        result = self.run_step("Sync refreshed Codex auth")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(self.object("auth.json.2").read_text()), refreshed)
        self.assertEqual(len(self.calls()), 2)
        self.assertEqual(self.calls()[-1][-1], self.env["CODEX_AUTH_OSS_OBJECT"])

    def test_concurrent_remote_change_is_preserved_like_reference_runner(self):
        self.configure()
        local = copy.deepcopy(self.original)
        local["tokens"]["refresh_token"] = "local-refresh"
        self.local.write_text(json.dumps(local))
        remote = copy.deepcopy(self.original)
        remote["tokens"]["refresh_token"] = "operator-refresh"
        self.object("auth.json.2").write_text(json.dumps(remote))
        result = self.run_step("Sync refreshed Codex auth")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("skipping stale auth sync", result.stdout)
        self.assertEqual(json.loads(self.object("auth.json.2").read_text()), remote)
        self.assertEqual(len(self.calls()), 1)

    def test_invalid_local_auth_is_not_uploaded(self):
        self.configure()
        self.local.write_text('{"auth_mode":"chatgpt","tokens":{}}')
        self.assertNotEqual(self.run_step("Sync refreshed Codex auth").returncode, 0)
        self.assertEqual(self.calls(), [])
        self.assertEqual(json.loads(self.object("auth.json.2").read_text()), self.original)


class WorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = (assignment.ROOT / ".github/workflows/reviewer-assignment.yml").read_text()
        cls.runner = (assignment.ROOT / ".github/workflows/code-review-runner.yml").read_text()

    def test_reapplication_is_an_event_not_a_one_time_flag(self):
        self.assertIn("pull_request_target:\n    types: [labeled]", self.workflow)
        self.assertIn("github.event.label.name == 'need-reviewers'", self.workflow)
        self.assertNotIn("contains(github.event.pull_request.labels", self.workflow)
        self.assertIn("queue: max", self.workflow)
        self.assertIn("cancel-in-progress: false", self.workflow)

    def test_only_trusted_code_and_a_read_only_model_run(self):
        self.assertNotRegex(self.workflow, r"(?m)^\s+ref:")
        self.assertEqual(self.workflow.count("persist-credentials: false"), 2)
        self.assertIn("--sandbox read-only", self.workflow)
        self.assertIn("env -u GH_TOKEN -u GITHUB_TOKEN -u OSS_AK -u OSS_SK", self.workflow)
        self.assertNotIn("dangerously-bypass", self.workflow)

    def test_write_permissions_are_limited_to_the_final_job(self):
        select, assign = self.workflow.split("\n  assign:\n", 1)
        self.assertNotIn("issues: write", select)
        self.assertNotIn("pull-requests: write", self.workflow)
        self.assertIn("issues: write", assign)
        self.assertNotIn("secrets.OSS_", assign)
        self.assertIn("needs: select", assign)

    def test_auth_reuses_reference_pool_and_optimistic_write_guard(self):
        self.assertIn("auth_prefix='oss://doris-community-ci/codex/auth.json.'", self.workflow)
        self.assertIn('"${candidate}.context"', self.workflow)
        self.assertIn('"${candidate}.invalid.${refresh_token_hash}"', self.workflow)
        self.assertNotIn("REVIEWER_ASSIGNMENT_CODEX_AUTH_OBJECT", self.workflow)
        self.assertNotIn("codex/reviewer-assignment/auth.json", self.workflow)
        self.assertIn('cp -f "$CODEX_HOME/auth.json" "$CODEX_AUTH_OSS_OBJECT"', self.workflow)
        self.assertIn("always() && steps.auth.outcome == 'success'", self.workflow)
        self.assertIn('if [ "$remote_hash" != "$original_hash" ]', self.workflow)
        self.assertIn('if [ "$local_hash" = "$original_hash" ]', self.workflow)
        self.assertIn('inherit = "none"', self.workflow)

    def test_ci_checkout_includes_workflow_sources_used_by_self_tests(self):
        checkout = self.workflow.split("      - name: Checkout trusted routing tools\n", 1)[1]
        checkout = checkout.split("\n      - name:", 1)[0]
        self.assertIn("            .github/scripts", checkout)
        self.assertIn("            .github/workflows", checkout)
        self.assertIn("            tools/reviewer-routing", checkout)

    def test_github_token_and_write_permissions_are_a_subset_of_reference(self):
        self.assertIn("GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}", self.workflow)
        for permission in ("contents: read", "pull-requests: read", "issues: write"):
            self.assertIn(permission, self.workflow)
        for permission in ("contents: read", "pull-requests: write", "issues: write"):
            self.assertIn(permission, self.runner)
        self.assertNotIn("secrets.REVIEWER_ASSIGNMENT", self.workflow)

    def test_runtime_and_checksums_match_the_existing_runner(self):
        archive = re.search(r"oss://[^\s]+codex-exec-goal-[^\s]+\.tar\.gz", self.workflow)[0]
        self.assertIn(archive, self.runner)
        hashes = re.findall(r'echo "([0-9a-f]{64}) ', self.workflow)
        self.assertEqual(len(hashes), 2)
        for checksum in hashes:
            self.assertIn(checksum, self.runner)

    def test_no_auth_prompt_or_session_artifacts_are_uploaded(self):
        upload = self.workflow.split("      - name: Publish the validated assignment plan\n")[1]
        upload = upload.split("\n  assign:", 1)[0]
        self.assertIn("/context.json", upload)
        self.assertIn("/selection.json", upload)
        self.assertNotIn("auth.json", upload)
        self.assertNotIn("codex.log", upload)
        self.assertNotIn("prompt.txt", upload)


if __name__ == "__main__":
    unittest.main()
