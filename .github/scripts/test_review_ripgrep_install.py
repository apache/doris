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

"""Exercise the workflow installer with real checksum, tar and install commands.

The download is local and the release digest is replaced with the fixture digest.
No credentials or network access are used. Requires bash and GNU sha256sum.
"""

import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / "workflows/code-review-runner.yml"


class ReviewRipgrepInstallTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="review ripgrep ")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bin = self.root / "fake-bin"
        self.bin.mkdir()
        self.scratch = self.root / "scratch"
        self.scratch.mkdir()
        self.archive = self.root / "release.tar.gz"
        self.github_path = self.root / "github-path"
        self.github_path.touch()
        self.env = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}",
                        RUNNER_TEMP=str(self.root / "runner"), TMPDIR=str(self.scratch),
                        GITHUB_PATH=str(self.github_path), FIXTURE_ARCHIVE=str(self.archive),
                        CURL_LOG=str(self.root / "curl.json"), CURL_EXIT="0")
        self.write_tool("curl", f"#!{sys.executable}\n" + textwrap.dedent("""\
            import json, os, shutil, sys
            from pathlib import Path
            args = sys.argv[1:]
            Path(os.environ["CURL_LOG"]).write_text(json.dumps(args))
            if os.environ["CURL_EXIT"] != "0":
                sys.exit(int(os.environ["CURL_EXIT"]))
            shutil.copyfile(os.environ["FIXTURE_ARCHIVE"], args[args.index("--output") + 1])
            """))
        # A system rg must not hide a broken newly installed binary.
        self.write_tool("rg", "#!/bin/sh\necho 'old system ripgrep'\n")
        self.write_tool("sudo", "#!/bin/sh\necho 'Unexpected sudo/apt invocation' >&2\nexit 99\n")
        step = re.search(r"^      - name: Install ripgrep\n(.*?)(?=^      - name:)",
                         WORKFLOW.read_text(), re.MULTILINE | re.DOTALL)
        self.assertIsNotNone(step)
        self.assertIn("timeout-minutes: 5", step.group(1))
        run = re.search(r"^        run: \|\n((?:          .*\n|\n)+)",
                        step.group(1), re.MULTILINE)
        self.assertIsNotNone(run)
        self.script = textwrap.dedent(run.group(1))
        pin = re.search(r"\b[a-f0-9]{64}\b", self.script)
        self.assertIsNotNone(pin, "The release archive must have a pinned SHA-256")
        self.pin = pin.group()
        self.make_archive()

    def write_tool(self, name, content):
        tool = self.bin / name
        tool.write_text(content)
        tool.chmod(0o755)

    def make_archive(self, binary=b"#!/bin/sh\necho 'ripgrep 14.1.0'\n", member="rg"):
        with tarfile.open(self.archive, "w:gz") as archive:
            info = tarfile.TarInfo(f"ripgrep-14.1.0-x86_64-unknown-linux-musl/{member}")
            info.size = len(binary)
            info.mode = 0o755
            archive.addfile(info, io.BytesIO(binary))
        self.digest = hashlib.sha256(self.archive.read_bytes()).hexdigest()

    def run_install(self):
        result = subprocess.run(["bash", "-e", "-c", self.script.replace(self.pin, self.digest)],
                                env=self.env, text=True, capture_output=True, timeout=10)
        self.assertEqual(list(self.scratch.iterdir()), [], "Download scratch directory leaked")
        return result

    def assert_failed(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.github_path.read_text(), "", "Failed install was added to PATH")

    def test_installs_verified_binary_for_subsequent_steps(self):
        result = self.run_install()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("ripgrep 14.1.0", result.stdout)
        installed_bin = self.github_path.read_text().strip()
        self.assertTrue(Path(installed_bin).is_relative_to(Path(self.env["RUNNER_TEMP"])))
        env = dict(self.env, PATH=f"{installed_bin}:{self.env['PATH']}")
        self.assertEqual(Path(shutil.which("rg", path=env["PATH"])), Path(installed_bin) / "rg")
        version = subprocess.run(["rg", "--version"], env=env, text=True,
                                 capture_output=True, check=True)
        self.assertEqual(version.stdout, "ripgrep 14.1.0\n")
        args = json.loads(Path(self.env["CURL_LOG"]).read_text())
        for option in ("--fail", "--location", "--show-error"):
            self.assertIn(option, args)

        def value(option):
            return int(args[args.index(option) + 1])

        attempts = value("--retry") + 1
        self.assertGreater(attempts, 1)
        self.assertGreater(value("--connect-timeout"), 0)
        self.assertGreater(value("--max-time"), 0)
        self.assertLessEqual(value("--connect-timeout"), value("--max-time"))
        self.assertGreater(value("--retry-delay"), 0)
        self.assertGreater(value("--retry-max-time"), 0)
        self.assertLess(value("--retry-max-time") + value("--max-time"), 240)
        self.assertLess(attempts * value("--max-time")
                        + (attempts - 1) * value("--retry-delay"), 240)

    def test_download_failure_stops_installation(self):
        self.env["CURL_EXIT"] = "22"
        result = self.run_install()
        self.assert_failed(result)
        self.assertEqual(result.returncode, 22)
        self.assertFalse(Path(self.env["RUNNER_TEMP"]).exists())

    def test_checksum_failure_stops_before_extraction(self):
        self.archive.write_bytes(b"corrupt or substituted release")
        result = self.run_install()
        self.assert_failed(result)
        self.assertIn("FAILED", result.stdout)
        self.assertFalse(Path(self.env["RUNNER_TEMP"]).exists())

    def test_invalid_archive_stops_installation(self):
        self.archive.write_bytes(b"not a tar archive")
        self.digest = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        self.assert_failed(self.run_install())

    def test_missing_binary_stops_installation(self):
        self.make_archive(member="README.md")
        self.assert_failed(self.run_install())

    def test_broken_binary_cannot_be_masked_by_system_rg(self):
        self.make_archive(binary=b"#!/bin/sh\nexit 17\n")
        result = self.run_install()
        self.assert_failed(result)
        self.assertEqual(result.returncode, 17)


if __name__ == "__main__":
    unittest.main()
