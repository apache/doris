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

    def check_install(self, system, missing, clean=0):
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
                self.write("installed/" + name)
        # Execute the real installation gate, replacing only the expensive build.
        self.executable("build-thirdparty.sh", 'printf "%s\\n" "$*" > "$BUILD_LOG"\n')
        self.env.update(DORIS_THIRDPARTY=str(self.work), TARGET_SYSTEM=system,
                        CLEAN=str(clean), PARALLEL="2", BUILD_LOG=str(self.work / "build.log"))
        result = self.run_bash(gate)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.work / "build.log").exists(), missing is not None)
        if missing is not None:
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

    def check_header(self, offline, inherited_offline="", cargo_override=True):
        script = (ROOT / "thirdparty/build-thirdparty.sh").read_text()
        function = re.search(r"^build_paimon_rust\(\) \{\n.*?^\}", script, re.M | re.S)[0]
        cargo = self.executable("custom/cargo", r'''case "$1" in
    --version) echo 'cargo 1.94.0' ;;
    build)
        mkdir -p "$CARGO_TARGET_DIR/release"
        touch "$CARGO_TARGET_DIR/release/libpaimon_c.a"
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
        result = self.run_bash("check_if_source_exist() { :; }\n" + function + "\nbuild_paimon_rust")
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
