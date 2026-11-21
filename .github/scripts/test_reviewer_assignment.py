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
import reviewer_assignment_model as model
import reviewer_assignment_diagnostics as diagnostics
import reviewer_assignment_usage as usage
from reviewer_assignment_diff import git_changes, parse_numstat
import ci_context as selector
from ci_context import candidate_summary
from routing_config import load_config, load_routing
from classify_paths import classify

PREFIX = "repos/apache/doris"
ISSUE = PREFIX + "/issues/42"
PULL = PREFIX + "/pulls/42"


def user(login):
    return {"login": login, "type": "User"}


def fixture_config():
    """Test policy is independent of the production reviewer lists."""
    config = {"schema_version": 1, "fallback_reviewer": "morningman", "disabled_reviewers": [],
              "test_routes": [], "modules": {}}
    rules = [
        ("other", ".", [], 0),
        ("test.other", "^regression-test/", [], 5),
        ("search", "^be/src/storage/index/inverted/",
         ["SearchFirst", "SearchSecond", "SearchThird", "SearchFourth", "SearchFifth", "SearchSixth"], 400),
        ("connector.trino", "^fe/fe-connector/fe-connector-trino/", ["morningman", "Gabriel39", "924060929"], 350),
        ("connector.fluss", "^fe/fe-connector/fe-connector-fluss/", ["morningman", "Gabriel39"], 350),
        ("connector.mongodb", "^fe/fe-connector/fe-connector-mongodb/", ["Gabriel39"], 350),
        ("runtime.memory", "^be/src/runtime/memory/", ["yiguolei", "mrhhsg"], 300),
    ]
    for mid, regex, reviewers, priority in rules:
        config["modules"][mid] = {"label": mid, "priority": priority, "patterns": [regex], "reviewers": reviewers}
    return config


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
        self.config = fixture_config()
        for module in self.config["modules"].values():
            module["reviewers"] = []
        self.config["modules"]["search"]["reviewers"] = ["Author", "Alice", "Bob"]
        self.save_config()
        (self.tool / "README.md").write_text("Trusted guide")
        (self.tool / "CI_GUIDE.md").write_text((assignment.TOOL / "CI_GUIDE.md").read_text())
        self.event = {"action": "labeled", "label": {"name": "need-reviewers"},
                      "pull_request": {"number": 42}}

    def save_config(self):
        (self.tool / "reviewer-routing.json").write_text(json.dumps(self.config))

    def prepare(self, run="100"):
        return assignment.prepare(self.api, self.event, "apache/doris", run,
                                  self.workspace, self.tool)

    def context(self):
        return json.loads((self.workspace / "context.json").read_text())

    def apply(self, people=("Alice", "Bob")):
        return assignment.apply(self.api, self.context(), {"reviewers": list(people)}, self.tool)

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
        self.assertIn("complete_diff", prompt)
        self.assertIn(self.api.pr["body"], prompt)
        self.assertIn("+fixed_query();", prompt)
        self.assertEqual(json.loads(prompt.split("ROUTING_INPUT_JSON:\n", 1)[1])["complete_diff"],
                         (self.workspace / "pr.diff").read_text())
        summary = json.loads((self.workspace / "candidate-summary.json").read_text())
        self.assertEqual(summary["path_routing_modules"], ["search"])
        self.assertEqual([p["login"] for p in summary["modules"][0]["candidates"]], ["Alice", "Bob"])
        self.assertTrue(summary["modules"][0]["candidates"][1]["existing_reviewer"])
        self.assertEqual((self.workspace / "pr.diff").read_text(),
                         "diff --git a/query.cpp b/query.cpp\n+fixed_query();\n")
        self.assertEqual(json.loads((self.workspace / "schema.json").read_text())[
            "properties"]["modules"]["maxItems"], 1)
        self.assertEqual(json.loads((self.workspace / "schema.json").read_text())[
            "properties"]["modules"]["items"]["enum"], ["search"])
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
        shutil.rmtree(self.workspace)  # A rerun starts on a fresh runner.
        self.assertTrue(self.prepare())
        self.assertFalse((self.workspace / "candidate-summary.json").exists())
        self.apply()
        self.assertEqual(len(self.api.comments), 1)
        self.assertEqual(len(self.mutations()), calls_before + 1)  # Idempotent label addition only.
        self.assertTrue(result["assigned"])
        self.assertEqual({p["login"] for p in self.api.pr["assignees"]}, {"Existing", "Alice", "Bob"})

    def test_module_output_is_sampled_saved_and_applied_without_github_selection_writes(self):
        self.prepare()
        assignment.write_json(self.workspace / "module-selection.json", {"modules": ["search"], "reason": "Index query changes"})
        selected = assignment.finalize_selection(self.workspace, self.tool)
        self.assertEqual(set(selected), {"Alice", "Bob"})
        self.assertEqual(self.mutations(), [])
        self.assertEqual(json.loads((self.workspace / "selection.json").read_text()), {"reviewers": selected})
        pools = json.loads((self.workspace / "selected-candidates.json").read_text())["modules"]
        self.assertEqual(pools[0]["id"], "search")
        self.assertEqual([p["login"] for p in pools[0]["candidates"]], ["Alice", "Bob"])
        result = self.apply(selected)
        self.assertEqual(result["reviewers"], selected)
        self.assertIn(", ".join("@" + p for p in selected) + ", please review this PR.", self.api.comments[0]["body"])

    def test_finalizing_or_recovering_a_saved_pair_never_draws_again(self):
        self.prepare()
        assignment.write_json(self.workspace / "module-selection.json", {"modules": ["search"], "reason": "Index query changes"})
        chosen = assignment.finalize_selection(self.workspace, self.tool)
        with patch.object(assignment, "sample_reviewers", side_effect=AssertionError("Unexpected second draw")):
            self.assertEqual(assignment.finalize_selection(self.workspace, self.tool), chosen)
            self.apply(chosen)
            shutil.rmtree(self.workspace)
            self.assertTrue(self.prepare())
            self.assertEqual(assignment.finalize_selection(self.workspace, self.tool), chosen)

    def test_new_label_event_discards_local_selection_and_permits_a_fresh_draw(self):
        self.prepare()
        assignment.write_json(self.workspace / "module-selection.json", {"modules": ["search"], "reason": "Index query changes"})
        assignment.finalize_selection(self.workspace, self.tool)
        self.apply()
        self.assertFalse(self.prepare(run="101"))
        self.assertFalse((self.workspace / "selection.json").exists())
        self.assertFalse((self.workspace / "module-selection.json").exists())
        self.assertFalse((self.workspace / "selected-candidates.json").exists())
        assignment.write_json(self.workspace / "module-selection.json", {"modules": ["search"], "reason": "Index query changes"})
        with patch.object(selector.random, "SystemRandom") as source:
            source.return_value.choice.side_effect = lambda pairs: pairs[0]
            self.assertEqual(set(assignment.finalize_selection(self.workspace, self.tool)), {"Alice", "Bob"})
            source.return_value.choice.assert_called_once()

    def test_invalid_module_output_is_not_saved_or_applied(self):
        self.prepare()
        for value in ({"modules": ["missing"]}, {"reviewers": ["Alice"]}, {"modules": ["search", "search"]}):
            with self.subTest(value=value):
                assignment.write_json(self.workspace / "module-selection.json", value)
                with self.assertRaises(ValueError):
                    assignment.finalize_selection(self.workspace, self.tool)
                self.assertFalse((self.workspace / "selection.json").exists())
        self.assertEqual(self.mutations(), [])

    def test_empty_module_selection_is_rejected_when_path_candidates_exist(self):
        self.prepare()
        assignment.write_json(self.workspace / "module-selection.json", {"modules": [], "reason": "No relevant module"})
        with self.assertRaisesRegex(ValueError, "despite eligible path modules: search"):
            assignment.finalize_selection(self.workspace, self.tool)
        self.assertFalse((self.workspace / "selection.json").exists())
        self.assertEqual(self.mutations(), [])
        report = diagnostics.diagnostics_report(self.workspace, "", "")
        self.assertEqual(report["files"]["validation.json"]["value"]["status"], "rejected")

    def test_diagnostics_distinguish_an_empty_selected_pool_from_no_selected_modules(self):
        self.config["modules"]["search"]["reviewers"] = []
        self.save_config()
        self.prepare()
        assignment.write_json(self.workspace / "module-selection.json", {"modules": ["search"], "reason": "Index query changes"})
        with self.assertRaisesRegex(ValueError, "without eligible candidates"):
            assignment.finalize_selection(self.workspace, self.tool)
        report = diagnostics.diagnostics_report(self.workspace, "", "")
        self.assertEqual(report["files"]["module-selection.json"]["value"], {"modules": ["search"], "reason": "Index query changes"})
        pools = report["files"]["selected-candidates.json"]["value"]["modules"]
        self.assertEqual(pools[0]["id"], "search")
        self.assertEqual(pools[0]["candidates"], [])

    def test_real_validation_cli_turns_model_modules_into_the_assignment_artifact(self):
        self.prepare()
        repository = self.root / "checkout"
        scripts = repository / ".github/scripts"
        scripts.mkdir(parents=True)
        for name in ("reviewer_assignment.py", "reviewer_assignment_diff.py", "reviewer_assignment_model.py", "review_github_api.py"):
            shutil.copy2(Path(assignment.__file__).parent / name, scripts / name)
        shutil.copytree(assignment.TOOL, repository / "tools/reviewer-routing")
        (repository / "tools/reviewer-routing/reviewer-routing.json").write_text(json.dumps(self.config))
        assignment.write_json(self.workspace / "module-selection.json", {"modules": ["search"], "reason": "Index query changes"})
        event = self.root / "event.json"
        assignment.write_json(event, self.event)
        env = {**os.environ, "GITHUB_EVENT_PATH": str(event), "GITHUB_REPOSITORY": "apache/doris",
               "GITHUB_RUN_ID": "100", "GITHUB_EVENT_NAME": "pull_request_target", "PYTHONDONTWRITEBYTECODE": "1"}
        command = [sys.executable, str(scripts / "reviewer_assignment.py"), "validate", "--workspace", str(self.workspace)]
        result = subprocess.run(command, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        selected = json.loads(result.stdout)["reviewers"]
        self.assertEqual(set(selected), {"Alice", "Bob"})
        self.assertEqual(json.loads((self.workspace / "selection.json").read_text()), {"reviewers": selected})
        self.assertTrue(self.apply(selected)["assigned"])

    def test_invalid_choices_do_not_mutate_github(self):
        self.prepare()
        for selected in (["Alice", "Bob", "Author"], ["Author"], ["Mallory"], ["Alice", "ALICE"], [1]):
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

    def test_truncated_files_require_verified_git_reconstruction(self):
        self.api.pr["changed_files"] = 3228
        with patch.object(assignment, "git_changes", side_effect=ValueError("Reconstructed PR file count mismatch")) as git:
            with self.assertRaisesRegex(ValueError, "file count mismatch"):
                self.prepare()
        git.assert_called_once()
        self.assertFalse((self.workspace / "prompt.txt").exists())

    def test_removed_candidates_are_rejected_without_substitution(self):
        self.config["modules"]["search"]["reviewers"] = ["Author"]
        self.save_config()
        self.prepare()
        schema = json.loads((self.workspace / "schema.json").read_text())
        self.assertEqual(schema["properties"]["modules"]["maxItems"], 0)
        assignment.write_json(self.workspace / "module-selection.json", {"modules": [], "reason": "No eligible candidate"})
        self.assertEqual(assignment.finalize_selection(self.workspace, self.tool), [])
        with self.assertRaisesRegex(ValueError, "active"):
            self.apply(("Alice",))
        self.assertTrue(self.apply(())["fallback"])

    def test_other_labels_are_not_routed(self):
        self.event["label"]["name"] = "bug"
        with self.assertRaisesRegex(ValueError, "labeled events"):
            self.prepare()
        self.assertEqual(self.api.calls, [])

    def test_workflow_reads_the_single_config_without_other_data_files(self):
        self.config["modules"]["search"]["reviewers"] = ["Alice"]
        self.save_config()
        self.prepare()
        allowed = assignment.candidates(self.tool)
        self.assertEqual(allowed, {"alice": "Alice"})
        with self.assertRaisesRegex(ValueError, "active"):
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

    def test_diff_406_uses_git_reconstruction(self):
        request = self.api.request
        def limited(method, endpoint, payload=None, **kwargs):
            if kwargs.get("raw"):
                raise assignment.GitHubError(406, "Diff exceeded 20000 lines")
            return request(method, endpoint, payload, **kwargs)
        self.api.request = limited
        complete = "diff --git a/complete.cpp b/complete.cpp\n+complete_change();\n"
        with patch.object(assignment, "git_changes", create=True,
                          return_value=(self.api.files, complete, "e" * 40)) as git:
            self.prepare()
        git.assert_called_once()
        self.assertEqual((self.workspace / "pr.diff").read_text(), complete)
        self.assertEqual(self.context()["diff_source"], "git")
        self.assertEqual(self.context()["merge_base_sha"], "e" * 40)
        self.assertEqual(self.mutations(), [])

    def test_api_file_cap_uses_reconstructed_complete_file_list(self):
        self.api.pr["changed_files"] = 2
        files = self.api.files + [{"filename": "be/src/runtime/memory/mem_tracker.cpp", "additions": 2, "deletions": 0}]
        self.config["modules"]["runtime.memory"]["reviewers"] = ["Alice"]
        self.save_config()
        with patch.object(assignment, "git_changes", return_value=(files, "complete diff", "e" * 40)):
            self.prepare()
        pr = json.loads((self.workspace / "pr.json").read_text())
        self.assertEqual(pr["changed_files"], 2)
        self.assertEqual({f["path"] for f in pr["files"]}, {f["filename"] for f in files})
        self.assertEqual(self.context()["diff_source"], "git")

    def test_diff_permission_error_is_not_treated_as_a_size_limit(self):
        request = self.api.request
        def forbidden(method, endpoint, payload=None, **kwargs):
            if kwargs.get("raw"):
                raise assignment.GitHubError(403, "Forbidden")
            return request(method, endpoint, payload, **kwargs)
        self.api.request = forbidden
        with patch.object(assignment, "git_changes") as git:
            with self.assertRaises(assignment.GitHubError):
                self.prepare()
        git.assert_not_called()
        self.assertEqual(self.mutations(), [])

    def test_empty_selection_assigns_fallback_and_explains_triage(self):
        self.prepare()
        result = self.apply(())
        self.assertEqual(result["reviewers"], ["morningman"])
        self.assertTrue(result["fallback"])
        self.assertEqual({p["login"] for p in self.api.pr["assignees"]}, {"Existing", "morningman"})
        self.assertEqual(self.api.labels, {"bug", "reviewers-assigned"})
        self.assertIn("No suitable reviewer was found. @morningman, please help assign reviewers for this PR.",
                      self.api.comments[0]["body"])
        self.assertEqual(assignment.checkpoint(self.api, self.context())[1]["reviewers"], [])

    def test_fallback_retry_on_fresh_runner_preserves_empty_selection_and_comment(self):
        self.prepare()
        self.api.fail_once[("POST", ISSUE + "/labels")] = 403
        with self.assertRaises(assignment.GitHubError):
            self.apply(())
        shutil.rmtree(self.workspace)
        self.assertTrue(self.prepare())
        self.assertEqual(json.loads((self.workspace / "selection.json").read_text()), {"reviewers": []})
        self.apply(())
        self.assertEqual(len(self.api.comments), 1)
        self.assertEqual(len([c for c in self.api.calls if c[0] == "PATCH"]), 1)
        with self.assertRaisesRegex(ValueError, "cannot change"):
            self.apply(("Alice",))

    def test_fallback_can_assign_the_author_for_manual_triage(self):
        self.api.pr["user"] = user("morningman")
        self.prepare()
        self.assertTrue(self.apply(())["fallback"])
        self.assertIn("help assign reviewers", self.api.comments[0]["body"])

    def test_empty_configured_pools_can_still_request_fallback(self):
        self.config["modules"]["search"]["reviewers"] = []
        self.save_config()
        self.prepare()
        self.assertEqual(assignment.candidates(self.tool), {})
        self.assertTrue(self.apply(())["fallback"])

    def test_qualified_morningman_is_a_normal_review_request(self):
        self.config["modules"]["search"]["reviewers"] = ["morningman"]
        self.save_config()
        self.prepare()
        self.assertFalse(self.apply(("morningman",))["fallback"])
        self.assertIn("@morningman, please review this PR.", self.api.comments[0]["body"])
        self.assertNotIn("No suitable reviewer", self.api.comments[0]["body"])

    def test_unassignable_fallback_does_not_complete_the_request(self):
        self.prepare()
        self.api.unassignable.add("morningman")
        with self.assertRaises(assignment.GitHubError):
            self.apply(())
        self.assertEqual(self.mutations(), [])
        self.assertIn("need-reviewers", self.api.labels)

    def test_retry_cannot_switch_the_fallback_login(self):
        self.prepare()
        self.apply(())
        self.config["fallback_reviewer"] = "Alice"
        self.save_config()
        with self.assertRaisesRegex(ValueError, "fallback reviewer changed"):
            self.prepare()

    def test_changed_fallback_configuration_controls_assignment_and_message(self):
        self.config["fallback_reviewer"] = "NewTriage"
        self.save_config()
        self.prepare()
        result = self.apply(())
        self.assertEqual(result["reviewers"], ["NewTriage"])
        self.assertIn("@NewTriage, please help assign reviewers", self.api.comments[0]["body"])

    def test_disabled_account_is_excluded_from_validation_and_summary(self):
        self.config["disabled_reviewers"] = ["aLiCe"]
        self.save_config()
        self.prepare()
        summary = json.loads((self.workspace / "candidate-summary.json").read_text())
        self.assertEqual([p["login"] for p in summary["modules"][0]["candidates"]], ["Bob"])
        with self.assertRaisesRegex(ValueError, "active"):
            self.apply(("Alice",))
        self.assertTrue(self.apply(("Bob",))["assigned"])

    def test_silent_fallback_assignment_failure_recovers_without_a_second_comment(self):
        self.prepare()
        self.api.silently_ignore.add("morningman")
        with self.assertRaisesRegex(ValueError, "did not assign"):
            self.apply(())
        self.assertIn("need-reviewers", self.api.labels)
        self.assertNotIn("No suitable reviewer", self.api.comments[0]["body"])
        self.api.silently_ignore.clear()
        self.assertTrue(self.prepare())
        self.apply(())
        self.assertEqual(len(self.api.comments), 1)
        self.assertIn("No suitable reviewer", self.api.comments[0]["body"])


class GitChangesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "source"
        self.repo.mkdir()
        self.git("init", "--quiet")
        self.git("config", "user.email", "routing-test@example.org")
        self.git("config", "user.name", "Routing Test")
        (self.repo / "renamed-old.txt").write_text("unchanged\n" * 20)
        (self.repo / "removed.txt").write_text("removed\n")
        (self.repo / "binary.bin").write_bytes(b"\0before\0")
        (self.repo / "tab\tpath.txt").write_text("old\n")
        self.merge_base = self.commit("Common ancestor")
        self.git("mv", "renamed-old.txt", "renamed-new.txt")
        (self.repo / "removed.txt").unlink()
        (self.repo / "new.txt").write_text("first\nsecond\n")
        (self.repo / "binary.bin").write_bytes(b"\0after\0")
        (self.repo / "tab\tpath.txt").write_text("new\n")
        head = self.commit("PR changes")
        self.git("checkout", "--quiet", "--detach", self.merge_base)
        (self.repo / "target-only.txt").write_text("Must not appear in the PR diff\n")
        base = self.commit("Target branch advances")
        self.pr = {"head": {"sha": head}, "base": {"sha": base},
                   "changed_files": 5, "additions": 3, "deletions": 2}
        self.files = [
            {"filename": "binary.bin", "additions": 0, "deletions": 0},
            {"filename": "new.txt", "additions": 2, "deletions": 0},
            {"filename": "removed.txt", "additions": 0, "deletions": 1},
            {"filename": "renamed-new.txt", "previous_filename": "renamed-old.txt", "additions": 0, "deletions": 0},
            {"filename": "tab\tpath.txt", "additions": 1, "deletions": 1},
        ]
        self.api = self
        self.compare_calls = []

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.repo), *args], check=True,
                              capture_output=True, text=True).stdout.strip()

    def commit(self, message):
        self.git("add", "--all")
        self.git("commit", "--quiet", "-m", message)
        return self.git("rev-parse", "HEAD")

    def request(self, method, endpoint):
        self.assertEqual(method, "GET")
        self.assertEqual(endpoint, f"{PREFIX}/compare/{self.pr['base']['sha']}...{self.pr['head']['sha']}?per_page=1")
        self.compare_calls.append(endpoint)
        return {"merge_base_commit": {"sha": self.merge_base}}

    def reconstruct(self, api_files=None):
        return git_changes(self.api, "apache/doris", self.pr,
                           self.files if api_files is None else api_files, self.root, self.repo.as_uri())

    def test_actual_git_diff_uses_merge_base_and_handles_renames_binary_and_tabs(self):
        files, diff, merge_base = self.reconstruct()
        self.assertEqual(files, self.files)
        self.assertEqual(merge_base, self.merge_base)
        self.assertIn("+first\n+second", diff)
        self.assertIn("rename from renamed-old.txt", diff)
        self.assertNotIn("target-only.txt", diff)
        self.assertEqual(len(self.compare_calls), 1)
        self.assertEqual(list(self.root.glob("reviewer-assignment-git-*")), [])

    def test_reconstructs_more_than_the_3000_file_api_cap(self):
        self.git("checkout", "--quiet", "--detach", self.merge_base)
        directory = self.repo / "many"
        directory.mkdir()
        files = []
        for number in range(3001):
            filename = f"many/f{number:04}.txt"
            (self.repo / filename).write_text("added\n")
            files.append({"filename": filename, "additions": 1, "deletions": 0})
        self.pr.update(changed_files=3001, additions=3001, deletions=0)
        self.pr["head"]["sha"] = self.commit("Many files")
        result, diff, _ = self.reconstruct(files[:3000])
        self.assertEqual(result, files)
        self.assertIn("many/f3000.txt", diff)

    def test_mismatched_api_counts_or_records_stop_reconstruction(self):
        for key in ("changed_files", "additions", "deletions"):
            with self.subTest(key=key):
                self.pr[key] += 1
                with self.assertRaisesRegex(ValueError, "count.*mismatch"):
                    self.reconstruct()
                self.pr[key] -= 1
        incorrect = copy.deepcopy(self.files)
        incorrect[1]["filename"] = "not-in-this-pr.txt"
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.reconstruct(incorrect)
        self.assertEqual(list(self.root.glob("reviewer-assignment-git-*")), [])

    def test_unfetchable_snapshot_fails_and_cleans_temporary_git_objects(self):
        self.pr["head"]["sha"] = "f" * 40
        with self.assertRaises(subprocess.CalledProcessError):
            self.reconstruct()
        self.assertEqual(list(self.root.glob("reviewer-assignment-git-*")), [])

    def test_invalid_sha_is_rejected_before_fetch(self):
        self.pr["head"]["sha"] = "--upload-pack=untrusted"
        with self.assertRaisesRegex(ValueError, "Invalid PR commit"):
            self.reconstruct()
        self.assertEqual(self.compare_calls, [])

    def test_numstat_preserves_newlines_and_tabs_in_paths(self):
        result = parse_numstat("2\t1\tline\nwith\ttab.txt\0" + "0\t0\t\0old\tname\0new\nname\0")
        self.assertEqual(result, [
            {"filename": "line\nwith\ttab.txt", "additions": 2, "deletions": 1},
            {"filename": "new\nname", "previous_filename": "old\tname", "additions": 0, "deletions": 0}])


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


class ConfiguredReviewersTest(unittest.TestCase):
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
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tool = Path(self.temp.name)
        self.config = fixture_config()
        self.save_config()

    def save_config(self):
        (self.tool / "reviewer-routing.json").write_text(json.dumps(self.config))
        self.mapping = load_routing(self.tool)
        self.modules = {module["id"]: module for module in self.mapping["modules"]}

    def pr(self, module, author="Contributor", unavailable=()):
        return {"number": 42, "author": author, "files": [self.PATHS[module]],
                "unavailable_reviewers": list(unavailable)}

    def test_requested_maintainer_lists_and_order_are_preserved(self):
        for module, expected in self.EXPECTED.items():
            with self.subTest(module=module):
                self.assertEqual(self.modules[module]["reviewers"], expected)
                self.assertEqual([p["login"] for p in self.modules[module]["candidates"]], expected)

    def test_each_module_draws_two_distinct_configured_reviewers_when_available(self):
        for module, expected in self.EXPECTED.items():
            with self.subTest(module=module):
                result = selector.sample_reviewers({"modules": [module]}, self.pr(module), self.mapping)
                self.assertEqual(len(result), min(2, len(expected)))
                self.assertEqual(len(set(result)), len(result))
                self.assertLessEqual(set(result), set(expected))

    def test_author_exclusion_is_case_insensitive(self):
        for module, expected in self.EXPECTED.items():
            with self.subTest(module=module):
                result = selector.sample_reviewers({"modules": [module]},
                                                  self.pr(module, author=expected[0].upper()), self.mapping)
                self.assertEqual(set(result), set(expected[1:]))

    def test_exhausted_pools_do_not_substitute_unlisted_people(self):
        for module, expected in self.EXPECTED.items():
            with self.subTest(module=module):
                result = selector.sample_reviewers({"modules": [module]},
                                                  self.pr(module, unavailable=expected), self.mapping)
                self.assertEqual(result, [])

    def test_multi_module_coverage_can_reuse_a_configured_reviewer(self):
        pr = self.pr("connector.trino")
        pr["files"].append(self.PATHS["connector.mongodb"])
        result = selector.sample_reviewers({"modules": ["connector.trino", "connector.mongodb"]}, pr, self.mapping)
        self.assertEqual(len(result), 2)
        self.assertIn("Gabriel39", result)
        self.assertLessEqual(set(result), set(self.EXPECTED["connector.trino"]))

    def test_five_person_pool_has_exactly_ten_equally_selectable_pairs(self):
        expected = {("SearchFirst", "SearchSecond"), ("SearchFirst", "SearchThird"),
                    ("SearchFirst", "SearchFourth"), ("SearchFirst", "SearchFifth"),
                    ("SearchSecond", "SearchThird"), ("SearchSecond", "SearchFourth"),
                    ("SearchSecond", "SearchFifth"), ("SearchThird", "SearchFourth"),
                    ("SearchThird", "SearchFifth"), ("SearchFourth", "SearchFifth")}
        pr = {"number": 42, "author": "Contributor", "files": ["be/src/storage/index/inverted/query.cpp"]}
        actual = set()
        for index in range(10):
            with patch.object(selector.random, "SystemRandom") as random_source:
                choose = random_source.return_value.choice
                choose.side_effect = lambda pairs: pairs[index]
                actual.add(tuple(selector.sample_reviewers({"modules": ["search"]}, pr, self.mapping)))
                choose.assert_called_once()
                self.assertEqual(len(choose.call_args.args[0]), 10)
        self.assertEqual(actual, expected)

    def test_filtering_precedes_the_five_person_window(self):
        self.config["modules"]["search"]["reviewers"] = ["Disabled", "Author", "Unavailable", "A", "B", "C", "D", "E"]
        self.config["disabled_reviewers"] = ["DISABLED"]
        self.save_config()
        pr = {"number": 42, "author": "AUTHOR", "files": ["be/src/storage/index/inverted/query.cpp"],
              "unavailable_reviewers": ["UNAVAILABLE"]}
        self.assertEqual(selector.eligible_pools(pr, self.mapping, ["search"])["search"], ["A", "B", "C", "D", "E"])
        for index in range(10):
            with patch.object(selector.random, "SystemRandom") as source:
                source.return_value.choice.side_effect = lambda pairs: pairs[index]
                selected = selector.sample_reviewers({"modules": ["search"]}, pr, self.mapping)
                self.assertEqual(len(selected), 2)
                self.assertLessEqual(set(selected), {"A", "B", "C", "D", "E"})

    def test_disjoint_modules_get_one_person_each(self):
        pr = self.pr("connector.trino")
        for index in range(6):
            with patch.object(selector.random, "SystemRandom") as source:
                source.return_value.choice.side_effect = lambda pairs: pairs[index]
                result = selector.sample_reviewers({"modules": ["connector.trino", "runtime.memory"]}, pr, self.mapping)
                self.assertEqual(len(set(result) & set(self.EXPECTED["connector.trino"])), 1)
                self.assertEqual(len(set(result) & set(self.EXPECTED["runtime.memory"])), 1)

    def test_shared_account_is_deduplicated_without_case_sensitivity(self):
        self.config["modules"]["connector.trino"]["reviewers"] = ["GABRIEL39"]
        self.save_config()
        result = selector.sample_reviewers({"modules": ["connector.trino", "connector.mongodb"]},
                                           self.pr("connector.trino"), self.mapping)
        self.assertEqual([p.lower() for p in result], ["gabriel39"])

    def test_empty_module_does_not_discard_the_other_modules_pool(self):
        self.config["modules"]["connector.mongodb"]["reviewers"] = []
        self.save_config()
        result = selector.sample_reviewers({"modules": ["runtime.memory", "connector.mongodb"]},
                                           self.pr("runtime.memory"), self.mapping)
        self.assertEqual(set(result), {"yiguolei", "mrhhsg"})

    def test_invalid_module_selections_fail_before_sampling(self):
        invalid = (None, [], {"reviewers": []}, {"modules": "search"}, {"modules": [1]},
                   {"modules": ["search", "search"]}, {"modules": ["missing"]},
                   {"modules": ["search", "connector.trino", "runtime.memory"]},
                   {"modules": ["search"], "reviewers": ["SearchFirst"]})
        for value in invalid:
            with self.subTest(value=value), patch.object(selector.random, "SystemRandom") as source:
                with self.assertRaises(ValueError):
                    selector.sample_reviewers(value, self.pr("connector.trino"), self.mapping)
                source.assert_not_called()
        self.assertEqual(selector.sample_reviewers({"modules": []}, self.pr("connector.trino"), self.mapping), [])


class CandidateSummaryTest(unittest.TestCase):
    EXPECTED = ConfiguredReviewersTest.EXPECTED
    PATHS = ConfiguredReviewersTest.PATHS
    setUp = ConfiguredReviewersTest.setUp
    save_config = ConfiguredReviewersTest.save_config
    pr = ConfiguredReviewersTest.pr

    def test_summary_preserves_config_order_and_author_exclusion(self):
        result = candidate_summary(self.pr("connector.trino", author="MORNINGMAN"), self.mapping)
        module = result["modules"][0]
        self.assertEqual([p["login"] for p in module["candidates"]], ["Gabriel39", "924060929"])
        self.assertTrue(all("preference_weight" not in p for p in module["candidates"]))

    def test_existing_reviewers_do_not_consume_ci_budget(self):
        pr = self.pr("connector.trino")
        pr["files"].append(self.PATHS["runtime.memory"])
        pr["existing_reviewers"] = ["morningman", "yiguolei", "SomeoneElse"]
        result = candidate_summary(pr, self.mapping)
        self.assertEqual(result["path_module_weights"], {"connector.trino": 1, "runtime.memory": 1})
        self.assertEqual(set(selector.sample_reviewers({"modules": ["runtime.memory"]}, pr, self.mapping)),
                         {"yiguolei", "mrhhsg"})
        self.assertTrue(result["modules"][0]["candidates"][0]["existing_reviewer"])

    def test_summary_contains_only_active_configured_candidates(self):
        pr = {"number": 42, "author": "Contributor", "files": [
            "be/src/storage/index/inverted/query/query.cpp"]}
        result = candidate_summary(pr, self.mapping)
        people = result["modules"][0]["candidates"]
        self.assertEqual([p["login"] for p in people], self.modules["search"]["reviewers"][:5])
        self.assertTrue(all(set(p) == {"login", "existing_reviewer"} for p in people))
        self.assertNotIn("patterns", result["modules"][0])

    def test_queries_can_reclassify_but_keep_the_five_person_window(self):
        result = candidate_summary(self.pr("connector.trino"), self.mapping, ["search"])
        self.assertEqual([m["id"] for m in result["modules"]], ["search"])
        self.assertEqual(len(result["modules"][0]["candidates"]), 5)
        self.assertNotIn("path_module_weights", result)
        with self.assertRaises(KeyError):
            candidate_summary(self.pr("connector.trino"), self.mapping, ["not-a-module"])

    def test_unclassified_paths_and_exhausted_pools_do_not_invent_candidates(self):
        pr = {"number": 42, "author": "Contributor", "files": ["unknown/path.txt"]}
        result = candidate_summary(pr, self.mapping)
        self.assertEqual(result["path_routing_modules"], ["other"])
        self.assertEqual(result["modules"][0]["candidates"], [])
        result = candidate_summary(self.pr("connector.mongodb", author="Gabriel39"), self.mapping)
        self.assertEqual(result["modules"][0]["candidates"], [])

    def test_summary_does_not_modify_the_loaded_policy(self):
        original = copy.deepcopy(self.mapping)
        candidate_summary(self.pr("connector.trino"), self.mapping)
        self.assertEqual(self.mapping, original)

    def test_module_lookup_cli_works_outside_the_repository(self):
        with tempfile.TemporaryDirectory() as temporary:
            pr = Path(temporary) / "pr.json"
            pr.write_text(json.dumps(self.pr("connector.trino")))
            tool = Path(temporary) / "tool"
            shutil.copytree(assignment.TOOL, tool)
            (tool / "reviewer-routing.json").write_text(json.dumps(self.config))
            command = [sys.executable, str(tool / "ci_context.py")]
            listing = subprocess.run(command + ["--list-modules"], cwd=temporary,
                                     capture_output=True, text=True, check=True)
            self.assertEqual(json.loads(listing.stdout)["runtime.memory"], self.modules["runtime.memory"]["label"])
            query = subprocess.run(command + [str(pr), "--modules", "runtime.memory"], cwd=temporary,
                                   capture_output=True, text=True, check=True)
            self.assertEqual([p["login"] for p in json.loads(query.stdout)["modules"][0]["candidates"]],
                             ["yiguolei", "mrhhsg"])


class ConfigurationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tool = Path(self.temp.name)
        self.config = fixture_config()
        self.path = self.tool / "reviewer-routing.json"

    def routing(self):
        self.path.write_text(json.dumps(self.config))
        return load_routing(self.tool)

    def test_new_module_and_new_reviewers_need_only_one_configuration_edit(self):
        self.config["modules"]["connector.new"] = {
            "label": "New connector", "priority": 500,
            "patterns": ["^fe/fe-connector/fe-connector-new/"], "reviewers": ["NewMaintainer", "Backup"]}
        routing = self.routing()
        pr = {"number": 42, "author": "Contributor", "files": ["fe/fe-connector/fe-connector-new/New.java"]}
        result = selector.path_context(pr, routing)
        self.assertEqual(result["routing_modules"], ["connector.new"])
        self.assertEqual(set(selector.sample_reviewers({"modules": ["connector.new"]}, pr, routing)),
                         {"NewMaintainer", "Backup"})
        self.assertEqual({p.name for p in self.tool.iterdir()}, {"reviewer-routing.json"})

    def test_reordering_removing_and_pausing_reviewers_change_the_sampling_pool(self):
        pr = {"number": 42, "author": "Contributor", "files": ["be/src/runtime/memory/mem_tracker.cpp"]}
        self.config["modules"]["runtime.memory"]["reviewers"] = ["Backup", "Primary"]
        self.assertEqual(selector.eligible_pools(pr, self.routing(), ["runtime.memory"])["runtime.memory"], ["Backup", "Primary"])
        self.config["modules"]["runtime.memory"]["reviewers"] = ["Primary", "Backup"]
        self.assertEqual(selector.eligible_pools(pr, self.routing(), ["runtime.memory"])["runtime.memory"], ["Primary", "Backup"])
        self.config["disabled_reviewers"] = ["PRIMARY"]
        active = self.routing()
        self.assertEqual(selector.sample_reviewers({"modules": ["runtime.memory"]}, pr, active), ["Backup"])
        module = next(m for m in active["modules"] if m["id"] == "runtime.memory")
        self.assertEqual(module["candidates"], [{"login": "Backup"}])
        self.config["modules"]["runtime.memory"]["reviewers"] = []
        self.assertEqual(selector.sample_reviewers({"modules": ["runtime.memory"]}, pr, self.routing()), [])

    def test_disabled_people_do_not_consume_the_five_person_window(self):
        self.config["disabled_reviewers"] = ["SearchFirst", "SearchSecond", "SearchThird"]
        pr = {"number": 42, "author": "Contributor", "files": ["be/src/storage/index/inverted/query.cpp"],
              "existing_reviewers": ["SearchFirst", "SearchSecond", "SearchThird"]}
        result = selector.sample_reviewers({"modules": ["search"]}, pr, self.routing())
        self.assertEqual(len(result), 2)
        self.assertLessEqual(set(result), {"SearchFourth", "SearchFifth", "SearchSixth"})

    def test_test_routes_and_path_priorities_are_read_from_the_same_file(self):
        self.config["modules"]["new"] = {"label": "New", "priority": 500,
            "patterns": ["^be/src/runtime/"], "reviewers": ["NewMaintainer"]}
        self.config["test_routes"] = [{"pattern": "^regression-test/suites/new_", "module": "new"}]
        routing = self.routing()
        self.assertEqual(classify("be/src/runtime/memory/mem_tracker.cpp", routing["config"])["module"], "new")
        self.assertEqual(classify("regression-test/suites/new_p0/test.groovy", routing["config"])["module"], "new")

    def test_case_insensitive_duplicate_logins_are_rejected(self):
        for field in ("disabled_reviewers", "reviewers"):
            with self.subTest(field=field):
                config = self.config if field == "disabled_reviewers" else self.config["modules"]["search"]
                original = config[field]
                config[field] = ["Alice", "ALICE"]
                with self.assertRaisesRegex(ValueError, "duplicate logins"):
                    self.routing()
                config[field] = original

    def test_duplicate_json_keys_cannot_silently_override_a_module(self):
        self.path.write_text('{"schema_version":1,"schema_version":1}')
        with self.assertRaisesRegex(ValueError, "Duplicate configuration key"):
            load_config(self.path)

    def test_typos_regex_errors_and_unknown_test_modules_are_actionable(self):
        changes = [
            (lambda c: c.update(disabled_reviewer=[]), "Unknown top-level"),
            (lambda c: c["modules"]["search"].update(reveiwers=[]), "requires exactly"),
            (lambda c: c["modules"]["search"].update(patterns=["["]), "Invalid regex"),
            (lambda c: c.update(test_routes=[{"pattern": "test", "module": "missing"}]), "Unknown test_routes module"),
            (lambda c: c["modules"]["search"].update(reviewers=["@Alice"]), "invalid GitHub login"),
        ]
        for change, expected in changes:
            with self.subTest(expected=expected):
                self.config = fixture_config()
                change(self.config)
                with self.assertRaisesRegex(ValueError, expected):
                    self.routing()

    def test_fallback_cannot_be_disabled_and_generic_fallbacks_are_required(self):
        self.config["disabled_reviewers"] = ["MORNINGMAN"]
        with self.assertRaisesRegex(ValueError, "fallback_reviewer is disabled"):
            self.routing()
        self.config = fixture_config()
        del self.config["modules"]["test.other"]
        with self.assertRaisesRegex(ValueError, "fallback categories"):
            self.routing()

    def test_configuration_check_cli_reports_real_failure(self):
        self.config["modules"]["search"]["reviewers"] = ["@Invalid"]
        self.path.write_text(json.dumps(self.config))
        result = subprocess.run([sys.executable, str(assignment.TOOL / "routing_config.py"),
                                 "--config", str(self.path), "--check"], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid GitHub login", result.stderr)


class DiagnosticsTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.workspace = Path(temp.name)

    def test_filtered_trace_keeps_failed_and_unfinished_commands_without_raw_session_content(self):
        events = [
            {"type": "item.started", "item": {"id": "ok", "type": "command_execution",
             "command": "cat pr.diff", "status": "in_progress"}},
            {"type": "item.completed", "item": {"id": "ok", "type": "command_execution",
             "command": "cat pr.diff", "status": "completed", "exit_code": 0,
             "aggregated_output": "successful-private-output"}},
            {"type": "item.completed", "item": {"id": "fail", "type": "command_execution",
             "command": "python3 ci_context.py --token=ghp_commandsecret", "status": "failed", "exit_code": 127,
             "aggregated_output": "x" * 5000 + "\npython3: command not found; ghp_outputsecret"}},
            {"type": "item.started", "item": {"id": "pending", "type": "command_execution",
             "command": "cat candidate-summary.json", "status": "in_progress"}},
            {"type": "item.completed", "item": {"type": "reasoning", "text": "private-reasoning"}},
            {"type": "item.completed", "item": {"type": "agent_message", "text": "private-message"}},
            {"type": "turn.failed", "error": {"message": "request failed: ghp_errorsecret"}},
        ]
        with patch.dict(os.environ, {"TEST_TOKEN": "environment-secret"}):
            report = diagnostics.diagnostics_report(
                self.workspace, "\n".join(json.dumps(e) for e in events),
                "startup failed: environment-secret " + "x" * 4100 + " ghp_stderrsecret")
        self.assertEqual(len(report["commands"]), 3)
        self.assertEqual(report["commands"][0]["exit_code"], 0)
        self.assertNotIn("output_tail", report["commands"][0])
        self.assertEqual(report["commands"][1]["exit_code"], 127)
        self.assertIn("python3: command not found", report["commands"][1]["output_tail"])
        self.assertLessEqual(len(report["commands"][1]["output_tail"]), diagnostics.LIMIT)
        self.assertEqual(report["commands"][2]["status"], "in_progress")
        self.assertIsNone(report["commands"][2]["exit_code"])
        self.assertIn("request failed", report["model_errors"][0])
        saved = json.dumps(report)
        for secret in ("successful-private-output", "private-reasoning", "private-message",
                       "environment-secret", "commandsecret", "outputsecret", "errorsecret", "stderrsecret"):
            self.assertNotIn(secret, saved)

    def test_missing_malformed_and_redacted_documents_are_reported_without_auth_or_prompts(self):
        (self.workspace / "module-selection.json").write_text('{"modules":[')
        (self.workspace / "selection.json").write_text('{"token":"ghp_documentsecret"}')
        (self.workspace / "auth.json").write_text("private-auth")
        (self.workspace / "prompt.txt").write_text("private-prompt")
        report = diagnostics.diagnostics_report(self.workspace, '{"type":', "")
        self.assertEqual(report["files"]["context.json"]["status"], "missing")
        self.assertEqual(report["files"]["module-selection.json"]["status"], "unparsed")
        self.assertEqual(report["files"]["selection.json"]["value"]["token"], "[REDACTED]")
        self.assertEqual(report["malformed_event_lines"], 1)
        for secret in ("private-auth", "private-prompt", "documentsecret"):
            self.assertNotIn(secret, json.dumps(report))

    def test_real_diagnostics_cli_records_checkpoint_reuse_without_model_events(self):
        assignment.write_json(self.workspace / "selection.json", {"reviewers": ["Alice", "Bob"]})
        output = self.workspace / "diagnostics/result.json"
        command = [sys.executable, str(Path(diagnostics.__file__)), "--workspace", str(self.workspace),
                   "--events", str(self.workspace / "missing-events"), "--stderr", str(self.workspace / "missing-stderr"),
                   "--version", str(self.workspace / "missing-version"), "--output", str(output)]
        result = subprocess.run(command, env={**os.environ, "REUSE_SELECTION": "true"},
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(output.read_text())
        self.assertEqual(json.loads(result.stdout), report)
        self.assertTrue(report["reused_selection"])
        self.assertEqual(report["files"]["selection.json"]["value"]["reviewers"], ["Alice", "Bob"])
        self.assertEqual(report["files"]["module-selection.json"]["status"], "missing")


class UsageTest(unittest.TestCase):
    def event(self, **counts):
        return json.dumps({"type": "turn.completed", "usage": {
            "input_tokens": 1000, "cached_input_tokens": 800, "output_tokens": 120, **counts}})

    def test_sums_turns_without_double_counting_cached_or_reasoning_tokens(self):
        events = "\n".join([self.event(reasoning_output_tokens=50),
                            self.event(input_tokens=2000, cached_input_tokens=1700, reasoning_output_tokens=20)])
        result = usage.usage_report(events, "success", 12, 0)
        self.assertEqual(result["input_tokens"], 3000)
        self.assertEqual(result["cached_input_tokens"], 2500)
        self.assertEqual(result["uncached_input_tokens"], 500)
        self.assertEqual(result["output_tokens"], 240)
        self.assertEqual(result["reasoning_output_tokens"], 70)
        self.assertEqual(result["completed_turns"], 2)
        self.assertTrue(result["usage_complete"])

    def test_absent_reasoning_field_does_not_invent_zero(self):
        result = usage.usage_report(self.event(), "success", 12, 0)
        self.assertTrue(result["usage_complete"])
        self.assertIsNone(result["reasoning_output_tokens"])

    def test_failure_with_no_completion_does_not_claim_zero(self):
        result = usage.usage_report('{"type":"turn.failed","error":{"message":"secret"}}', "failure", 9, 1)
        self.assertIsNone(result["input_tokens"])
        self.assertFalse(result["usage_complete"])
        self.assertNotIn("secret", json.dumps(result) + usage.markdown(result))
        self.assertIn("not zero", usage.markdown(result))

    def test_timeout_retains_partial_counts_and_marks_them_incomplete(self):
        result = usage.usage_report(self.event() + '\n{"type":', "failure", 480, 124)
        self.assertEqual(result["status"], "timed_out")
        self.assertEqual(result["input_tokens"], 1000)
        self.assertEqual(result["malformed_event_lines"], 1)
        self.assertFalse(result["usage_complete"])

    def test_checkpoint_reuse_and_skipped_model_are_known_zero_usage(self):
        for kwargs, expected in (({"reused": True}, "reused"), ({}, "not_run")):
            with self.subTest(expected=expected):
                result = usage.usage_report("", "skipped", **kwargs)
                self.assertEqual(result["status"], expected)
                self.assertEqual(result["input_tokens"], 0)
                self.assertEqual(result["elapsed_seconds"], 0)
                self.assertTrue(result["usage_complete"])

    def test_success_without_usage_is_unavailable_and_invalid_fields_are_not_summed(self):
        result = usage.usage_report("", "success", 1, 0)
        self.assertFalse(result["usage_complete"])
        self.assertIsNone(result["input_tokens"])
        for invalid in (True, -1, "1000", None):
            with self.subTest(invalid=invalid):
                result = usage.usage_report(self.event(input_tokens=invalid), "success", 1, 0)
                self.assertFalse(result["usage_complete"])
                self.assertIsNone(result["input_tokens"])

    def test_inconsistent_cached_counts_are_reported_as_an_error(self):
        with self.assertRaisesRegex(ValueError, "cannot exceed"):
            usage.usage_report(self.event(cached_input_tokens=1001), "success", 1, 0)


class ModelUsageShellTest(unittest.TestCase):
    def test_actual_model_and_report_steps_record_success_and_failure_without_raw_events(self):
        for exit_code in (0, 124):
            with self.subTest(exit_code=exit_code), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                binary = root / "codex"
                binary.write_text('#!' + sys.executable + '''
import json, os, sys
if sys.argv[1:] == ['--version']:
    print('codex-cli test')
    sys.exit(0)
print(json.dumps({"type":"item.completed","item":{"text":"secret-context"}}))
code = int(os.environ['FAKE_CODEX_EXIT'])
if code == 0:
    print(json.dumps({"type":"turn.completed","usage":{
        "input_tokens":1000,"cached_input_tokens":800,"output_tokens":120}}))
sys.exit(code)
''')
                binary.chmod(0o700)
                assignment.prepare(FakeGitHub(), {"action": "labeled", "label": {"name": "need-reviewers"},
                                   "pull_request": {"number": 42}}, "apache/doris", "100", root)
                output = root / "outputs"
                env = {**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                       "RUNNER_TEMP": str(root), "ASSIGNMENT_WORKSPACE": str(root),
                       "CODEX_ASSIGNMENT_BIN": str(binary), "GITHUB_OUTPUT": str(output),
                       "FAKE_CODEX_EXIT": str(exit_code), "GITHUB_REPOSITORY": "apache/doris",
                       "GITHUB_RUN_ID": "100", "GITHUB_RUN_ATTEMPT": "1",
                       "GITHUB_STEP_SUMMARY": str(root / "summary")}
                script = AuthPoolShellTest.step_script(self, "Ask Codex to identify at most two modules")
                result = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, exit_code, result.stderr)
                values = dict(line.split("=", 1) for line in output.read_text().splitlines())
                self.assertEqual(int(values["exit_code"]), exit_code)
                self.assertGreaterEqual(int(values["elapsed_seconds"]), 0)
                env.update(CODEX_STEP_OUTCOME="success" if exit_code == 0 else "failure",
                           CODEX_EXIT_CODE=values["exit_code"],
                           CODEX_ELAPSED_SECONDS=values["elapsed_seconds"])
                script = AuthPoolShellTest.step_script(self, "Summarize Codex usage")
                result = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                saved = (root / "reviewer-assignment-metrics/usage.json").read_text()
                report = json.loads(saved)
                self.assertEqual(report["status"], "success" if exit_code == 0 else "timed_out")
                self.assertEqual(report["input_tokens"], 1000 if exit_code == 0 else None)
                summary = (root / "summary").read_text()
                self.assertNotIn("secret-context", saved + summary)
                self.assertIn("Cached input", summary)
                script = AuthPoolShellTest.step_script(self, "Collect reviewer selection diagnostics")
                result = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                saved = (root / "reviewer-assignment-diagnostics/selection.json").read_text()
                self.assertEqual(json.loads(saved)["cli_version"], "codex-cli test")
                self.assertNotIn("secret-context", saved + result.stdout)


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
        self.assertIn("reviewer_assignment_model.py", self.workflow)
        source = Path(model.__file__).read_text()
        self.assertIn('"--sandbox", "read-only"', source)
        self.assertIn('"GH_TOKEN", "GITHUB_TOKEN", "OSS_AK", "OSS_SK"', source)
        self.assertIn('"features.shell_tool=false"', source)
        self.assertNotIn("dangerously-bypass", self.workflow)

    def test_write_permissions_are_limited_to_the_final_job(self):
        select, assign = self.workflow.split("\n  assign:\n", 1)
        self.assertNotIn("issues: write", select)
        self.assertNotIn("pull-requests: write", select)
        self.assertIn("pull-requests: write", assign)
        self.assertIn("issues: write", assign)
        self.assertNotIn("secrets.OSS_", assign)
        self.assertIn("needs: select", assign)
        self.assertIn("if: github.event_name == 'pull_request_target'", assign)

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

    def test_usage_reporting_survives_model_failure_and_uploads_only_numeric_metadata(self):
        report = self.workflow.split("      - name: Summarize Codex usage\n", 1)[1]
        report = report.split("      - name: Sync refreshed Codex auth\n", 1)[0]
        self.assertEqual(report.count("if: always()"), 2)
        self.assertEqual(report.count("continue-on-error: true"), 2)
        upload = report.split("      - name: Publish Codex usage metrics\n", 1)[1]
        self.assertIn("/reviewer-assignment-metrics/usage.json", upload)
        self.assertNotIn("codex.jsonl", upload)
        self.assertNotIn("auth.json", upload)
        self.assertIn("retention-days: 7", upload)

    def test_debug_artifacts_survive_failures_without_uploading_raw_events(self):
        select, assign = self.workflow.split("\n  assign:\n", 1)
        collect = select.split("      - name: Collect reviewer selection diagnostics\n", 1)[1]
        collect = collect.split("      - name: Publish the validated assignment plan\n", 1)[0]
        self.assertEqual(collect.count("if: always()"), 2)
        self.assertEqual(collect.count("continue-on-error: true"), 2)
        upload = collect.split("      - name: Publish reviewer selection diagnostics\n", 1)[1]
        self.assertIn("path: ${{ runner.temp }}/reviewer-assignment-diagnostics/", upload)
        self.assertNotIn("codex.jsonl", upload)
        self.assertNotIn("auth.json", upload)
        self.assertIn("retention-days: 7", upload)
        self.assertIn("REVIEW_GITHUB_DIAGNOSTICS_DIR:", select)
        self.assertIn("REVIEW_GITHUB_DIAGNOSTICS_DIR:", assign)
        upload = assign.split("      - name: Publish assignment API diagnostics\n", 1)[1]
        self.assertIn("if: always()", upload)
        self.assertIn("continue-on-error: true", upload)
        self.assertIn("/reviewer-assignment-diagnostics/github/*.json", upload)


if __name__ == "__main__":
    unittest.main()
