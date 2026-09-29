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

"""Run with python3 thirdparty/test/paimon-build-test.py; no Rust downloads required."""

import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class PaimonBuildTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.env = dict(os.environ)
        for key in ("CARGO", "CARGO_NET_OFFLINE", "RUSTUP_TOOLCHAIN"):
            self.env.pop(key, None)

    def write(self, name, text=""):
        path = self.work / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def executable(self, name, text):
        path = self.write(name, "#!/bin/bash\nset -e\n" + text)
        path.chmod(0o755)
        return path

    def run_bash(self, script):
        return subprocess.run(["bash", "-ec", script], env=self.env,
                              cwd=self.work, text=True, capture_output=True)

    def check_install(self, system, missing, clean=0, empty=None, expect_rebuild=False):
        script = (ROOT / "build.sh").read_text()
        gate = script[script.index("# build thirdparty libraries if necessary."):
                      script.index("update_submodule() {")]
        sentinel = re.search(r"else\n    LAST_THIRDPARTY_LIB='([^']+)'", gate)[1]
        if system == "Darwin":
            sentinel = "libbrotlienc.a"
        files = ["lib/" + sentinel, "lib64/liblance_c.a", "lib64/libpaimon_c.a",
                 "include/paimon_rust/paimon.h"]
        for name in files:
            if name != missing:
                self.write("installed/" + name, "" if name == empty else "complete")
        # Execute the real installation gate, replacing only the expensive build.
        self.executable("build-thirdparty.sh", 'printf "%s\\n" "$*" > "$BUILD_LOG"\n')
        self.env.update(DORIS_THIRDPARTY=str(self.work), TARGET_SYSTEM=system,
                        CLEAN=str(clean), PARALLEL="2", BUILD_LOG=str(self.work / "build.log"))
        result = self.run_bash(gate)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.work / "build.log").exists(), missing is not None or expect_rebuild)
        if missing is not None or expect_rebuild:
            args = (self.work / "build.log").read_text().strip()
            self.assertEqual(args, "-j 2" + (" --clean" if clean else ""))
            # A full rebuild also replaces Lance built with the previous Rust version.
            self.assertFalse((self.work / "installed/lib64/liblance_c.a").exists())

    def test_old_linux_install_rebuilds(self):
        self.check_install("Linux", "lib64/libpaimon_c.a")

    def test_old_macos_install_rebuilds(self):
        self.check_install("Darwin", "lib64/libpaimon_c.a")

    def test_missing_header_rebuilds(self):
        self.check_install("Linux", "include/paimon_rust/paimon.h")

    def test_missing_lance_rebuilds(self):
        self.check_install("Linux", "lib64/liblance_c.a")

    def test_clean_option_is_preserved(self):
        self.check_install("Linux", "lib64/libpaimon_c.a", clean=1)

    def test_complete_install_is_reused(self):
        self.check_install("Linux", None)

    def test_incomplete_pair_rebuilds(self):
        self.write("installed/lib64/.paimon-installing")
        self.check_install("Linux", None, expect_rebuild=True)

    def test_empty_header_rebuilds(self):
        self.check_install("Linux", None, empty="include/paimon_rust/paimon.h", expect_rebuild=True)

    def test_empty_archive_rebuilds(self):
        self.check_install("Linux", None, empty="lib64/libpaimon_c.a", expect_rebuild=True)

    def archive_installer(self):
        script = (ROOT / "thirdparty/build-thirdparty.sh").read_text()
        return "\n".join(re.search(r"^" + name + r"\(\) \{\n.*?^\}", script, re.M | re.S)[0]
                         for name in ("install_rust_archive", "install_paimon_rust"))

    def test_header_archive_pair_publication(self):
        archive = self.write("build/libpaimon_c.a", "new archive")
        header = self.write("build/paimon.h", "new header")
        target_archive = self.work / "installed/lib64/libpaimon_c.a"
        target_header = self.work / "installed/include/paimon_rust/paimon.h"
        marker = target_archive.parent / ".paimon-installing"
        self.env.update(TP_INSTALL_DIR=str(self.work / "installed"), STRIP_TP_LIB="OFF",
                        KERNEL="Linux", ARCHIVE=str(archive), HEADER=str(header))
        failures = {
            "header-copy": 'cp() { if [[ "$1" = -p && "$2" = "$HEADER" ]]; then '
                           'printf partial > "${@: -1}"; return 1; fi; command cp "$@"; }',
            "archive-publish": 'mv() { if [[ "${@: -1}" = */libpaimon_c.a ]]; then return 1; fi; '
                               'command mv "$@"; }',
            "header-publish": 'mv() { if [[ "${@: -1}" = */paimon.h ]]; then return 1; fi; '
                              'command mv "$@"; }',
            "success": "",
        }
        for replacement in (False, True):
            for stage, injection in failures.items():
                with self.subTest(replacement=replacement, stage=stage):
                    for path in (target_archive, target_header, marker):
                        if path.exists():
                            path.unlink()
                        path.parent.mkdir(parents=True, exist_ok=True)
                    if replacement:
                        target_archive.write_text("old archive")
                        target_header.write_text("old header")
                    result = self.run_bash(self.archive_installer() + "\n" + injection
                                           + '\ninstall_paimon_rust "$ARCHIVE" "$HEADER"')
                    if stage == "success":
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertEqual(target_header.read_text(), "new header")
                        self.assertEqual(target_archive.read_text(), "new archive")
                        self.assertFalse(marker.exists())
                    else:
                        self.assertNotEqual(result.returncode, 0)
                        if replacement:
                            self.assertEqual(target_header.read_text(), "old header")
                        else:
                            self.assertFalse(target_header.exists())
                        self.assertEqual(marker.exists(), stage != "header-copy")
                        # A retry must repair the pair and clear its incomplete marker.
                        retry = self.run_bash(self.archive_installer()
                                              + '\ninstall_paimon_rust "$ARCHIVE" "$HEADER"')
                        self.assertEqual(retry.returncode, 0, retry.stderr)
                        self.assertEqual(target_header.read_text(), "new header")
                        self.assertEqual(target_archive.read_text(), "new archive")
                        self.assertFalse(marker.exists())
                    self.assertEqual(list(target_header.parent.glob("*.tmp.*")), [])

    def check_archive_publication(self, name):
        source = self.write("build/" + name, "new archive")
        source.chmod(0o644)
        destination = self.work / "installed/lib64" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.env.update(TP_INSTALL_DIR=str(self.work / "installed"),
                        STRIP_TP_LIB="ON", KERNEL="Linux", ARCHIVE=str(source))
        failures = {
            "copy": 'cp() { printf partial > "${@: -1}"; return 1; }',
            "strip": 'strip() { printf damaged > "${@: -1}"; return 1; }',
            "publish": 'mv() { return 1; }',
            "success": '',
        }
        for replacement in (False, True):
            for stage, injection in failures.items():
                with self.subTest(archive=name, replacement=replacement, stage=stage):
                    if destination.exists():
                        destination.unlink()
                    if replacement:
                        destination.write_text("old archive")
                    # Execute the production publisher with failures after partial copy/strip;
                    # neither a first install nor a replacement may expose those bytes.
                    result = self.run_bash(self.archive_installer() +
                                           '\nstrip() { :; }\n' + injection +
                                           '\ninstall_rust_archive "$ARCHIVE"')
                    if stage == "success":
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertEqual(destination.read_text(), "new archive")
                        self.assertEqual(destination.stat().st_mode & 0o777, 0o644)
                    else:
                        self.assertNotEqual(result.returncode, 0)
                        if replacement:
                            self.assertEqual(destination.read_text(), "old archive")
                        else:
                            self.assertFalse(destination.exists())
                    self.assertEqual(list(destination.parent.glob(name + ".tmp.*")), [])

    def test_atomic_lance_archive_publication(self):
        self.check_archive_publication("liblance_c.a")

    def test_atomic_paimon_archive_publication(self):
        self.check_archive_publication("libpaimon_c.a")

    def check_header(self, offline, inherited_offline="", cargo_override=True):
        script = (ROOT / "thirdparty/build-thirdparty.sh").read_text()
        function = re.search(r"^build_paimon_rust\(\) \{\n.*?^\}", script, re.M | re.S)[0]
        cargo = self.executable("custom/cargo", r'''case "$1" in
    --version) echo 'cargo 1.94.0' ;;
    build)
        mkdir -p "$CARGO_TARGET_DIR/release"
        printf 'archive' > "$CARGO_TARGET_DIR/release/libpaimon_c.a"
        ;;
    metadata) printf '%s' "${CARGO_NET_OFFLINE:-unset}" > "$METADATA_LOG" ;;
    *) exit 90 ;;
esac
''')
        # PATH's Cargo deliberately fails: metadata must use the selected executable.
        self.executable("bin/cargo", "exit 91\n")
        cbindgen = self.executable("bin/cbindgen", r'''"${CARGO:-cargo}" metadata
while [[ "$1" != --output ]]; do shift; done
printf '/* Generated test header */\n' > "$2"
''')
        self.write("src/paimon/Cargo.toml")
        self.env.update(PATH=str(self.work / "bin") + os.pathsep + os.environ["PATH"],
                        TP_SOURCE_DIR=str(self.work / "src"), PAIMON_RUST_SOURCE="paimon",
                        TP_INSTALL_DIR=str(self.work / "installed"), BUILD_DIR="build",
                        PARALLEL="1", KERNEL="Linux", STRIP_TP_LIB="OFF",
                        RUSTUP_TOOLCHAIN="1.94.0", PAIMON_RUST_CARGO_OFFLINE=offline,
                        PAIMON_RUST_CBINDGEN=str(cbindgen),
                        METADATA_LOG=str(self.work / "metadata.log"))
        if cargo_override:
            self.env["PAIMON_RUST_CARGO"] = str(cargo)
        else:
            self.env.pop("PAIMON_RUST_CARGO", None)
            self.env["CARGO"] = str(cargo)
        if inherited_offline:
            self.env["CARGO_NET_OFFLINE"] = inherited_offline
        result = self.run_bash("check_if_source_exist() { :; }\n" + self.archive_installer()
                               + "\n" + function + "\nbuild_paimon_rust")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        expected = "true" if offline.upper() == "ON" else inherited_offline or "unset"
        self.assertEqual((self.work / "metadata.log").read_text(), expected)
        self.assertTrue((self.work / "installed/include/paimon_rust/paimon.h").exists())
        self.assertTrue((self.work / "installed/lib64/libpaimon_c.a").exists())

    def test_header_uses_selected_cargo(self):
        self.check_header("OFF")

    def test_header_metadata_is_offline(self):
        self.check_header("on", cargo_override=False)

    def test_offline_override_wins(self):
        self.check_header("ON", inherited_offline="false", cargo_override=False)

    def test_inherited_offline_is_preserved(self):
        self.check_header("OFF", inherited_offline="true", cargo_override=False)


if __name__ == "__main__":
    unittest.main(verbosity=2)
