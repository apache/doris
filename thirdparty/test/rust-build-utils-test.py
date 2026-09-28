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

"""Exercise cache recovery and interrupted Rust archive installation without builds."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

HELPERS = Path(__file__).resolve().parents[1] / "rust-build-utils.sh"


class RustBuildTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.env = dict(os.environ, TP_INSTALL_DIR=str(self.root / "prefix"),
                        CARGO_HOME=str(self.root / "cargo-home"), KERNEL="Linux",
                        STRIP_TP_LIB="OFF", RUST_TOOLCHAIN_IDENTITY="rustc test compiler")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env["PATH"] = str(self.bin) + os.pathsep + os.environ["PATH"]
        self.executable("rustc", "printf 'rustc test compiler\\n'\n")

    def executable(self, name, body):
        path = self.bin / name
        path.write_text("#!/usr/bin/env bash\nset -e\n" + body)
        path.chmod(0o755)

    def shell(self, command):
        return subprocess.run(["bash", "-c", 'set -eo pipefail; source "$1"; ' + command,
                               "test", str(HELPERS)], cwd=self.root, env=self.env,
                              universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def prepare_archive(self, name, contents="new"):
        archive = self.root / name
        archive.write_text(contents)
        return archive

    def install(self, name):
        return self.shell('install_rust_archive "' + name + '"')

    def check(self):
        return self.shell('check_rust_toolchain_identity test "' + str(self.bin / "cargo") + '"')

    def test_both_archives_have_independent_verified_identities(self):
        for name in ("liblance_c.a", "libpaimon_c.a"):
            self.prepare_archive(name)
            self.assertEqual(self.install(name).returncode, 0)
        self.assertEqual(self.check().returncode, 0)
        self.executable("rustc", "echo different-compiler\n")
        self.assertNotEqual(self.check().returncode, 0)

    def test_strip_failure_preserves_previous_install(self):
        for name in ("liblance_c.a", "libpaimon_c.a"):
            with self.subTest(name=name):
                self.env["STRIP_TP_LIB"] = "OFF"
                self.prepare_archive(name, "old")
                self.assertEqual(self.install(name).returncode, 0)
                self.prepare_archive(name)
                self.env["STRIP_TP_LIB"] = "ON"
                self.executable("strip", "exit 1\n")
                self.assertNotEqual(self.install(name).returncode, 0)
                self.assertEqual((self.root / "prefix/lib64" / name).read_text(), "old")
                self.assertEqual(self.check().returncode, 0)

    def test_strip_failure_on_first_install_publishes_nothing(self):
        self.env["STRIP_TP_LIB"] = "ON"
        self.executable("strip", "exit 1\n")
        for name in ("liblance_c.a", "libpaimon_c.a"):
            self.prepare_archive(name)
            self.assertNotEqual(self.install(name).returncode, 0)
            self.assertFalse((self.root / "prefix/lib64" / name).exists())
        self.assertEqual(self.check().returncode, 0)

    def test_interruption_after_archive_rename_is_detected(self):
        # Both a first install and a replacement must fail closed if the second
        # rename fails: the next build may not mix an unidentified Rust stdlib.
        for name in ("liblance_c.a", "libpaimon_c.a"):
            for replacement in (False, True):
                with self.subTest(name=name, replacement=replacement):
                    mv = self.bin / "mv"
                    if mv.exists():
                        mv.unlink()
                    destination = self.root / "prefix/lib64" / name
                    if destination.exists():
                        destination.unlink()
                    if Path(str(destination) + ".rust-id").exists():
                        Path(str(destination) + ".rust-id").unlink()
                    if replacement:
                        self.prepare_archive(name, "old")
                        self.assertEqual(self.install(name).returncode, 0)
                    self.prepare_archive(name)
                    self.executable("mv", 'case "$3" in *.rust-id) exit 1;; esac\nexec /bin/mv "$@"\n')
                    self.assertNotEqual(self.install(name).returncode, 0)
                    self.assertNotEqual(self.check().returncode, 0)
                    mv.unlink()
                    self.assertEqual(self.install(name).returncode, 0)
                    self.assertEqual(self.check().returncode, 0)


if __name__ == "__main__":
    unittest.main()
