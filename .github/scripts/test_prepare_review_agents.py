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

"""Test release review context preparation without credentials or model calls."""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from prepare_review_agents import SKILL_PATH, prepare_review_skill

SCRIPTS = Path(__file__).resolve().parent
WORKFLOW = SCRIPTS.parent / "workflows/code-review-runner.yml"
EXISTING_GUIDE = "be/src/runtime/AGENTS.md"
MISSING_GUIDE = "fe/fe-core/AGENTS.md"
BRANCH_ONLY_GUIDE = "be/src/format_v2/AGENTS.md"


def step_script(name):
    step = re.search(
        rf"^      - name: {re.escape(name)}\n(.*?)(?=^      - name:|\Z)",
        WORKFLOW.read_text(), re.MULTILINE | re.DOTALL,
    ).group(1)
    return textwrap.dedent(re.search(
        r"^        run: \|\n((?:          .*\n|\n)+)", step, re.MULTILINE,
    ).group(1))


class ReviewGuidanceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.git("init", "-q")
        self.git("config", "user.name", "Review Test")
        self.git("config", "user.email", "review-test@example.invalid")
        self.write(SKILL_PATH, f"Read `{EXISTING_GUIDE}` and `{MISSING_GUIDE}`.\n"
                   f"Read `{MISSING_GUIDE}` again. Shorthand: `fe/.../persist/AGENTS.md`.\n")
        self.write(EXISTING_GUIDE, "Trusted runtime guide\n")
        self.write(MISSING_GUIDE, "Trusted FE guide\n")
        self.trusted = self.commit("Trusted workflow instructions")
        self.git("rm", "-q", SKILL_PATH, MISSING_GUIDE)
        self.write(EXISTING_GUIDE, "Release-specific runtime guide\n")
        self.write(BRANCH_ONLY_GUIDE, "Release-specific format guide\n")
        self.base = self.commit("Old release without skill")
        self.write("be/src/format_v2/reader.cpp", "PR change\n")
        self.head = self.commit("PR change")
        self.context = self.repo / ".code-review.test"
        self.context.mkdir()

    def git(self, *args, cwd=None):
        return subprocess.run(["git", *args], cwd=cwd or self.repo, check=True,
                              capture_output=True, text=True).stdout.strip()

    def write(self, path, content):
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def commit(self, message):
        self.git("add", ".")
        self.git("commit", "-qm", message)
        return self.git("rev-parse", "HEAD")

    def prepare(self, branch="branch-4.1", trusted=None):
        return prepare_review_skill(self.repo, branch, trusted or self.trusted, self.context)

    def check_bundle(self):
        manifest = json.loads((self.context / "review_guidance_sources.json").read_text())
        self.assertEqual(manifest["trusted_ref"], self.trusted)
        self.assertEqual(manifest["guides"][EXISTING_GUIDE], EXISTING_GUIDE)
        skill = (self.repo / manifest["skill"]).read_text()
        for guide in manifest["guides"].values():
            self.assertTrue((self.repo / guide).is_file())
            self.assertIn(f"`{guide}`", skill)
        fallback = self.repo / manifest["guides"][MISSING_GUIDE]
        self.assertEqual(fallback.read_text(), "Trusted FE guide\n")
        self.assertEqual((self.repo / EXISTING_GUIDE).read_text(), "Release-specific runtime guide\n")
        self.assertFalse((self.repo / MISSING_GUIDE).exists())
        self.assertFalse((self.repo / SKILL_PATH).exists())
        self.assertEqual(self.git("diff", "HEAD"), "")
        return manifest

    def test_all_three_release_branches_get_resolvable_guides(self):
        for branch in ("branch-4.2", "branch-4.1", "branch-4.0"):
            with self.subTest(branch=branch):
                self.context = self.repo / f".code-review.{branch}"
                self.context.mkdir()
                prompt = self.prepare(branch)
                manifest = self.check_bundle()
                self.assertIn(manifest["skill"], prompt)
                self.assertIn("take precedence", prompt)
                self.assertEqual(manifest["base_ref"], branch)

    def test_other_branches_do_not_load_fallback(self):
        for branch in ("master", "branch-4.1.4", "branch-3.1", "branch-3.0"):
            with self.subTest(branch=branch):
                prompt = self.prepare(branch, trusted="not-a-ref")
                self.assertIn("code review skill in this repository", prompt)
                self.assertNotIn(SKILL_PATH, prompt)
                self.assertEqual(list(self.context.iterdir()), [])

    def test_checkout_skill_is_preserved(self):
        self.write(SKILL_PATH, "Release-specific skill\n")
        prompt = self.prepare(trusted="not-a-ref")
        self.assertIn(f"`{SKILL_PATH}`", prompt)
        self.assertEqual((self.repo / SKILL_PATH).read_text(), "Release-specific skill\n")
        self.assertEqual(list(self.context.iterdir()), [])

    def test_missing_trusted_guide_fails_before_model_start(self):
        self.write(SKILL_PATH, "Read `missing/AGENTS.md`\n")
        bad_ref = self.commit("Incomplete trusted instructions")
        (self.repo / SKILL_PATH).unlink()
        with self.assertRaises(subprocess.CalledProcessError):
            self.prepare(trusted=bad_ref)
        self.assertFalse((self.context / "review_guidance_sources.json").exists())

    def test_mutable_ref_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "immutable workflow commit SHA"):
            self.prepare(trusted="master")

    def test_context_outside_checkout_is_rejected(self):
        self.context = self.root
        with self.assertRaises(ValueError):
            self.prepare()

    def test_missing_workflow_commit_is_fetched_from_origin(self):
        checkout = self.root / "checkout"
        checkout.mkdir()
        self.git("init", "-q", cwd=checkout)
        self.git("remote", "add", "origin", str(self.repo), cwd=checkout)
        context = checkout / ".code-review.test"
        context.mkdir()
        prepare_review_skill(checkout, "branch-4.0", self.trusted, context)
        self.assertEqual(self.git("rev-parse", f"{self.trusted}^{{commit}}", cwd=checkout), self.trusted)
        manifest = json.loads((context / "review_guidance_sources.json").read_text())
        self.assertTrue((checkout / manifest["skill"]).is_file())

    @unittest.skipUnless(shutil.which("jq"), "workflow shell requires jq")
    def test_actual_workflow_prepares_context_and_final_prompt(self):
        # Execute the real shell blocks. Only GitHub HTTP is replaced by a fixture.
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        gh = bin_dir / "gh"
        gh.write_text(f"#!{sys.executable}\n" + textwrap.dedent('''
            import json
            import os
            import sys
            from pathlib import Path
            endpoint = sys.argv[-1]
            if endpoint == "repos/apache/doris/pulls/66227":
                print(json.dumps({"head": {"sha": os.environ["HEAD_SHA"]},
                                  "base": {"sha": os.environ["BASE_SHA"], "ref": "branch-4.1"}}))
            else:
                assert endpoint == ("repos/apache/doris/contents/.github/scripts/"
                                    "prepare_review_agents.py?ref=" + os.environ["HELPER_REF"])
                print(Path(os.environ["TEST_HELPER"]).read_text())
        '''))
        gh.chmod(0o755)
        if sys.platform == "darwin":
            # GNU sed -i and BSD sed -i have different argv conventions.
            sed = bin_dir / "sed"
            sed.write_text('#!/bin/bash\nif [ "$1" = -i ]; then shift; exec /usr/bin/sed -i "" "$@"; fi\nexec /usr/bin/sed "$@"\n')
            sed.chmod(0o755)
        env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
               "REPO": "apache/doris", "PR_NUMBER": "66227", "HEAD_SHA": self.head,
               "BASE_SHA": self.base, "HELPER_REF": self.trusted,
               "REVIEW_CONTEXT_DIR": str(self.context), "REVIEW_CONTEXT_REL": self.context.name,
               "RUNNER_TEMP": str(self.root), "TEST_HELPER": str(SCRIPTS / "prepare_review_agents.py")}
        for step in ("Prepare authoritative PR context and required AGENTS guides", "Prepare review prompt"):
            result = subprocess.run(["bash", "-e", "-c", step_script(step)],
                                    cwd=self.repo, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        manifest = self.check_bundle()
        prompt = (self.context / "review_prompt.txt").read_text()
        self.assertIn(manifest["skill"], prompt)
        self.assertIn(self.trusted, prompt)
        self.assertIn(BRANCH_ONLY_GUIDE, prompt)
        self.assertNotIn("PLACEHOLDER_", prompt)
        self.assertEqual((self.context / "required_agents.txt").read_text(), BRANCH_ONLY_GUIDE + "\n")
        self.assertEqual((self.context / "pr_changed_files.txt").read_text(), "be/src/format_v2/reader.cpp\n")
        self.assertIn("+PR change", (self.context / "pr.diff").read_text())
        self.assertIn(f"{self.context.name}/review_prompt.txt",
                      (self.context / "codex_goal_prompt.txt").read_text())


if __name__ == "__main__":
    unittest.main()
