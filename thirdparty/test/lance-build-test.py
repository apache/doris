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

"""Run with python3 thirdparty/test/lance-build-test.py; no Rust build or downloads."""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SYMBOL = "lance_dataset_storage_bases_json"
LOCAL_SYMBOL = "lance_local_uri_path_json"
HEADER = "#define LANCE_STORAGE_BASES_JSON_VERSION 1\n#define LANCE_LOCAL_URI_PATH_JSON_VERSION 1\n"


class LanceBuildTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.prefix = self.work / "thirdparty"
        self.env = dict(os.environ)
        self.env.pop("NM", None)
        self.env.update(DORIS_HOME=str(ROOT), DORIS_THIRDPARTY=str(self.prefix),
                        BUILD_BE="1", CLEAN="0", PARALLEL="2")
        self.write("thirdparty/installed/lib64/unrelated.a", "keep unrelated dependency")

    def write(self, name, text):
        path = self.work / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def executable(self, name, text):
        path = self.write(name, "#!/usr/bin/env bash\nset -e\n" + text)
        path.chmod(0o755)
        return path

    def run_bash(self, script):
        return subprocess.run(["bash", "-eo", "pipefail", "-c", script], env=self.env,
                              cwd=self.work, text=True, capture_output=True)

    def archive(self, name, symbol=SYMBOL, undefined=False):
        source = self.write(name + ".c", (f"extern void {symbol}(void);\n"
                                           f"void caller(void) {{ {symbol}(); }}\n") if undefined
                            else f"void {symbol}(void) {{}}\n")
        with source.open("a") as handle:
            handle.write(f"void {LOCAL_SYMBOL}(void) {{}}\n")
        obj = source.with_suffix(".o")
        target = source.with_suffix(".a")
        subprocess.run(["cc", "-c", str(source), "-o", str(obj)], check=True,
                       capture_output=True)
        subprocess.run(["ar", "rcs", str(target), str(obj)], check=True, capture_output=True)
        return target

    def installed(self, header=HEADER, symbol=SYMBOL, undefined=False):
        self.write("thirdparty/installed/include/lance/lance.h", header)
        archive = self.archive("fixtures/installed", symbol, undefined)
        shutil.copy2(archive, self.prefix / "installed/lib64/liblance_c.a")
        self.write("thirdparty/installed/include/paimon_rust/paimon.h", "/* complete old Paimon */")
        shutil.copy2(archive, self.prefix / "installed/lib64/libpaimon_c.a")

    def gate(self):
        script = (ROOT / "build.sh").read_text()
        return script[script.index("# Lance TVF storage policy needs"):
                      script.index("update_submodule() {")]

    def compatible(self):
        functions = self.gate().split('if [[ "${BUILD_BE}" -eq 1 ]]')[0]
        return self.run_bash(functions + '\nlance_c_is_compatible "$DORIS_THIRDPARTY/installed"')

    def matching_source(self, repair=True):
        self.prefix.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / "thirdparty/vars.sh", self.prefix / "vars.sh")
        self.write("thirdparty/download-thirdparty.sh", "# lance-c-storage-bases.patch\n")
        patch = self.prefix / "patches/lance-c-storage-bases.patch"
        patch.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / "thirdparty/patches/lance-c-storage-bases.patch", patch)
        new_header = self.write("fixtures/new.h", HEADER)
        new_archive = self.archive("fixtures/new")
        self.env.update(BUILD_LOG=str(self.work / "build.log"), NEW_HEADER=str(new_header),
                        NEW_ARCHIVE=str(new_archive))
        script = '''install_lance_c() { :; }
build_paimon_rust() { :; }
install_paimon_rust() { :; }
printf '%s\\n' "$*" > "$BUILD_LOG"
printf '%s|%s|%s\\n' "${LANCE_C_CARGO:-}" "${PAIMON_RUST_CARGO:-}" "${RUSTUP_TOOLCHAIN:-}" > "${BUILD_LOG}.toolchain"
'''
        if repair:
            script += '''cp "$NEW_HEADER" "$DORIS_THIRDPARTY/installed/include/lance/lance.h"
cp "$NEW_ARCHIVE" "$DORIS_THIRDPARTY/installed/lib64/liblance_c.a"
rm -f "$DORIS_THIRDPARTY/installed/lib64/.lance-installing"
if [[ "${FAIL_BEFORE_PAIMON_INSTALL:-}" == 1 ]]; then exit 23; fi
mkdir -p "$DORIS_THIRDPARTY/installed/include/paimon_rust"
printf '/* matching Paimon */' > "$DORIS_THIRDPARTY/installed/include/paimon_rust/paimon.h"
cp "$NEW_ARCHIVE" "$DORIS_THIRDPARTY/installed/lib64/libpaimon_c.a"
if [[ "${INCOMPLETE_PAIMON:-}" == 1 ]]; then : > "$DORIS_THIRDPARTY/installed/lib64/libpaimon_c.a"; fi
rm -f "$DORIS_THIRDPARTY/installed/lib64/.paimon-installing"
'''
        self.executable("thirdparty/build-thirdparty.sh", script)

    def assert_unrelated_preserved(self):
        self.assertEqual((self.prefix / "installed/lib64/unrelated.a").read_text(),
                         "keep unrelated dependency")

    def test_complete_installed_only_pair_is_reused(self):
        self.installed()
        result = self.run_bash(self.gate())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.work / "build.log").exists())
        self.assert_unrelated_preserved()

    def test_old_header_new_archive_and_old_archive_new_header_fail_closed(self):
        for header, symbol, undefined in [("/* old header */\n", SYMBOL, False),
                                          (HEADER, "old_lance_api", False),
                                          (HEADER, SYMBOL, True),
                                          (HEADER.replace("VERSION 1", "VERSION 2"), SYMBOL, False)]:
            with self.subTest(header=header, symbol=symbol, undefined=undefined):
                self.installed(header, symbol, undefined)
                archive = self.prefix / "installed/lib64/liblance_c.a"
                before = archive.read_bytes()
                result = self.run_bash(self.gate())
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Refresh the compilation image", result.stderr)
                self.assertEqual(before, archive.read_bytes())
                self.assert_unrelated_preserved()

    def test_incomplete_installed_pair_is_not_reused(self):
        for defect in ("marker", "missing_header", "empty_header", "empty_archive"):
            with self.subTest(defect=defect):
                self.installed()
                marker = self.prefix / "installed/lib64/.lance-installing"
                marker.unlink(missing_ok=True)
                header = self.prefix / "installed/include/lance/lance.h"
                archive = self.prefix / "installed/lib64/liblance_c.a"
                if defect == "marker":
                    marker.touch()
                elif defect == "missing_header":
                    header.unlink()
                elif defect == "empty_header":
                    header.write_text("")
                else:
                    archive.write_text("")
                result = self.run_bash(self.gate())
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("header/archive pair", result.stderr)
                self.assertEqual(marker.exists(), defect == "marker")
                self.assert_unrelated_preserved()

    def test_fe_only_does_not_check_the_new_lance_abi(self):
        self.installed("/* old header */\n", "old_lance_api")
        self.env.update(BUILD_BE="0", NM=str(self.executable("bin/nm", "exit 77\n")))
        result = self.run_bash(self.gate())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_unrelated_preserved()

    def test_complete_matching_source_rebuilds_paired_rust_dependencies_and_repairs_marker(self):
        for clean in ("0", "1"):
            with self.subTest(clean=clean):
                self.installed("/* old header */\n", "old_lance_api")
                self.write("thirdparty/installed/lib64/.lance-installing", "")
                self.matching_source()
                self.env["CLEAN"] = clean
                result = self.run_bash(self.gate())
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual((self.work / "build.log").read_text().strip(), "-j 2 lance_c paimon_rust")
                self.assertFalse((self.prefix / "installed/lib64/.lance-installing").exists())
                self.assertEqual(self.compatible().returncode, 0)
                self.assert_unrelated_preserved()

    def test_abi_repair_uses_one_cargo_command_for_both_dependencies(self):
        self.installed("/* old header */\n", "old_lance_api")
        self.matching_source()
        self.env.update(LANCE_C_CARGO="/selected/cargo", RUSTUP_TOOLCHAIN="1.97.1")
        result = self.run_bash(self.gate())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.work / "build.log.toolchain").read_text().strip(),
                         "/selected/cargo|/selected/cargo|1.97.1")
        self.assert_unrelated_preserved()

    def test_abi_repair_rejects_conflicting_per_library_cargo_overrides(self):
        self.installed("/* old header */\n", "old_lance_api")
        self.matching_source()
        self.env.update(LANCE_C_CARGO="/new/cargo", PAIMON_RUST_CARGO="/old/cargo")
        result = self.run_bash(self.gate())
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("same cargo command", result.stderr)
        self.assertFalse((self.work / "build.log").exists())
        self.assert_unrelated_preserved()

    def test_old_or_incomplete_source_is_not_used_for_rebuild(self):
        for defect in ("patch", "pin", "download", "installer"):
            with self.subTest(defect=defect):
                self.installed("/* old header */\n", "old_lance_api")
                self.matching_source()
                (self.work / "build.log").unlink(missing_ok=True)
                if defect == "patch":
                    self.write("thirdparty/patches/lance-c-storage-bases.patch", "old patch\n")
                elif defect == "pin":
                    with (self.prefix / "vars.sh").open("a") as handle:
                        handle.write('\nLANCE_C_SOURCE="another-source"\n')
                elif defect == "download":
                    self.write("thirdparty/download-thirdparty.sh", "# old download recipe\n")
                else:
                    self.write("thirdparty/build-thirdparty.sh", "# old installer\n")
                result = self.run_bash(self.gate())
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("matching complete third-party sources", result.stderr)
                self.assertFalse((self.work / "build.log").exists())
                self.assert_unrelated_preserved()

    def test_rebuild_failure_or_incompatible_output_does_not_continue(self):
        for mode in ("failure", "old_output"):
            with self.subTest(mode=mode):
                self.installed("/* old header */\n", "old_lance_api")
                self.matching_source(repair=False)
                if mode == "failure":
                    with (self.prefix / "build-thirdparty.sh").open("a") as handle:
                        handle.write("exit 23\n")
                result = self.run_bash(self.gate())
                self.assertNotEqual(result.returncode, 0)
                if mode == "old_output":
                    self.assertIn("Rebuilt Lance SDK header/archive pair", result.stderr)
                self.assert_unrelated_preserved()

    def test_nm_symbol_spelling_definition_and_stream_completion(self):
        self.installed()
        cases = [
            ("gnu", f"printf '00000000 T {SYMBOL}\\n'\n", True),
            ("darwin", f"printf '00000000 T _{SYMBOL}\\n'\n", True),
            ("weak", f"printf '00000000 W {SYMBOL}\\n'\n", True),
            ("undefined", f"printf '                 U {SYMBOL}\\n'\n", False),
            ("darwin-undefined", f"printf '                 U _{SYMBOL}\\n'\n", False),
            ("nm-failure", f"printf '00000000 T {SYMBOL}\\n'; exit 2\n", False),
            ("long", f"printf '\\nmember.o:\\n00000000 T {SYMBOL}\\n'\n"
                     "for ((i=0; i<20000; i++)); do printf '00000000 T other_symbol_%s\\n' \"$i\"; done\n", True),
        ]
        for name, script, expected in cases:
            script = f"printf '00000000 T {LOCAL_SYMBOL}\\n'\n" + script
            with self.subTest(name=name):
                self.env["NM"] = str(self.executable("bin/nm", script))
                result = self.compatible()
                self.assertEqual(result.returncode == 0, expected, result.stderr)

    def test_pair_retry_after_lance_publish_rebuilds_both_even_when_lance_is_compatible(self):
        self.installed("/* old header */\n", "old_lance_api")
        self.matching_source()
        self.env["FAIL_BEFORE_PAIMON_INSTALL"] = "1"
        first = self.run_bash(self.gate())
        self.assertNotEqual(first.returncode, 0)
        marker = self.prefix / "installed/lib64/.lance-paimon-repairing"
        self.assertTrue(marker.exists())
        self.assertEqual(self.compatible().returncode, 0)
        self.assertIn("old Paimon", (self.prefix / "installed/include/paimon_rust/paimon.h").read_text())
        (self.work / "build.log").unlink()
        self.env.pop("FAIL_BEFORE_PAIMON_INSTALL")
        retry = self.run_bash(self.gate())
        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assertEqual((self.work / "build.log").read_text().strip(), "-j 2 lance_c paimon_rust")
        self.assertFalse(marker.exists())
        self.assertIn("matching Paimon", (self.prefix / "installed/include/paimon_rust/paimon.h").read_text())
        self.assert_unrelated_preserved()

    def test_pair_marker_survives_incomplete_paimon_output_and_installed_only_retry(self):
        self.installed("/* old header */\n", "old_lance_api")
        self.matching_source()
        self.env["INCOMPLETE_PAIMON"] = "1"
        result = self.run_bash(self.gate())
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Paimon SDK header/archive pair is incomplete", result.stderr)
        marker = self.prefix / "installed/lib64/.lance-paimon-repairing"
        self.assertTrue(marker.exists())
        (self.prefix / "build-thirdparty.sh").unlink()
        retry = self.run_bash(self.gate())
        self.assertNotEqual(retry.returncode, 0)
        self.assertTrue(marker.exists())
        self.assert_unrelated_preserved()

    def test_pending_pair_bypasses_full_prefix_rebuild_even_if_paimon_install_was_interrupted(self):
        self.installed()
        self.write("thirdparty/installed/lib/hadoop_hdfs/native/libhdfs.a", "sentinel")
        self.write("thirdparty/installed/lib64/.lance-paimon-repairing", "")
        self.write("thirdparty/installed/lib64/.paimon-installing", "")
        (self.prefix / "installed/lib64/libpaimon_c.a").unlink()
        self.matching_source()
        script = (ROOT / "build.sh").read_text()
        full_gate = script[script.index("# build thirdparty libraries if necessary."):
                           script.index("# Lance TVF storage policy needs")]
        result = self.run_bash('TARGET_SYSTEM=Linux\n' + full_gate + self.gate())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.work / "build.log").read_text().strip(), "-j 2 lance_c paimon_rust")
        self.assertFalse((self.prefix / "installed/lib64/.lance-paimon-repairing").exists())
        self.assert_unrelated_preserved()

    def test_cmake_rejects_individual_and_paired_incomplete_markers(self):
        source = (ROOT / "be/cmake/thirdparty.cmake").read_text()
        fragment = source[source.index("# Never consume Rust SDK artifacts"):
                          source.index("add_thirdparty(lance_c")]
        script = self.write("guard.cmake", f'set(THIRDPARTY_DIR "{self.prefix / "installed"}")\n' + fragment)
        for name in (".lance-installing", ".paimon-installing", ".lance-paimon-repairing"):
            marker = self.write("thirdparty/installed/lib64/" + name, "")
            result = subprocess.run(["cmake", "-P", str(script)], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Rust SDK installation is incomplete", result.stderr)
            marker.unlink()
        result = subprocess.run(["cmake", "-P", str(script)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def installer(self):
        script = (ROOT / "thirdparty/build-thirdparty.sh").read_text()
        return "\n".join(re.search(r"^" + name + r"\(\) \{\n.*?^\}", script, re.M | re.S)[0]
                         for name in ("install_rust_archive", "install_lance_c"))

    def test_header_archive_pair_publication_and_retry(self):
        archive = self.write("build/liblance_c.a", "new archive")
        headers = self.work / "build/include/lance"
        self.write("build/include/lance/lance.h", "new C header")
        self.write("build/include/lance/lance.hpp", "new C++ header")
        self.env.update(TP_INSTALL_DIR=str(self.prefix / "installed"), STRIP_TP_LIB="ON",
                        KERNEL="Linux", ARCHIVE=str(archive), HEADERS=str(headers))
        target_archive = self.prefix / "installed/lib64/liblance_c.a"
        target_headers = self.prefix / "installed/include/lance"
        marker = self.prefix / "installed/lib64/.lance-installing"
        failures = {
            "header-copy": 'cp() { if [[ "$1" = -pR ]]; then '
                           'printf partial > "${@: -1}/lance.h"; return 1; fi; command cp "$@"; }',
            "archive-copy": 'cp() { if [[ "$1" = -p && "$2" = "$ARCHIVE" ]]; then '
                            'printf partial > "${@: -1}"; return 1; fi; command cp "$@"; }',
            "archive-strip": 'strip() { printf damaged > "${@: -1}"; return 1; }',
            "archive-publish": 'mv() { if [[ "${@: -1}" = */liblance_c.a ]]; then return 1; fi; '
                               'command mv "$@"; }',
            "header-publish": 'mv() { if [[ "${@: -1}" = */include/lance ]]; then return 1; fi; '
                              'command mv "$@"; }',
            "success": "",
        }
        for replacement in (False, True):
            for stage, injection in failures.items():
                with self.subTest(replacement=replacement, stage=stage):
                    for path in (target_archive, marker):
                        path.unlink(missing_ok=True)
                    if target_headers.exists():
                        shutil.rmtree(target_headers)
                    if replacement:
                        self.write("thirdparty/installed/include/lance/lance.h", "old C header")
                        self.write("thirdparty/installed/include/lance/lance.hpp", "old C++ header")
                        target_archive.write_text("old archive")
                    result = self.run_bash(self.installer() + '\nstrip() { :; }\n' + injection
                                           + '\ninstall_lance_c "$ARCHIVE" "$HEADERS"')
                    if stage == "success":
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertFalse(marker.exists())
                    else:
                        self.assertNotEqual(result.returncode, 0)
                        self.assertEqual(marker.exists(), stage != "header-copy")
                        if replacement and stage != "header-publish":
                            self.assertEqual((target_headers / "lance.h").read_text(), "old C header")
                        retry = self.run_bash(self.installer() + '\nstrip() { :; }\n'
                                              + 'install_lance_c "$ARCHIVE" "$HEADERS"')
                        self.assertEqual(retry.returncode, 0, retry.stderr)
                        self.assertFalse(marker.exists())
                    self.assertEqual((target_headers / "lance.h").read_text(), "new C header")
                    self.assertEqual((target_headers / "lance.hpp").read_text(), "new C++ header")
                    self.assertEqual(target_archive.read_text(), "new archive")
                    self.assertEqual(list(target_headers.parent.glob(".lance.tmp.*")), [])
                    self.assertEqual(list(target_archive.parent.glob("liblance_c.a.tmp.*")), [])
                    self.assert_unrelated_preserved()

    def test_unstaged_headers_do_not_begin_publication(self):
        archive = self.write("build/liblance_c.a", "new archive")
        headers = self.work / "build/include/lance"
        self.write("build/include/lance/lance.h", "new C header")
        self.env.update(TP_INSTALL_DIR=str(self.prefix / "installed"), STRIP_TP_LIB="OFF",
                        KERNEL="Linux", ARCHIVE=str(archive), HEADERS=str(headers))
        self.write("thirdparty/installed/include/lance/lance.h", "old header")
        self.write("thirdparty/installed/lib64/liblance_c.a", "old archive")
        result = self.run_bash(self.installer() + '\ninstall_lance_c "$ARCHIVE" "$HEADERS"')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((self.prefix / "installed/include/lance/lance.h").read_text(), "old header")
        self.assertEqual((self.prefix / "installed/lib64/liblance_c.a").read_text(), "old archive")
        self.assertFalse((self.prefix / "installed/lib64/.lance-installing").exists())
        self.assert_unrelated_preserved()


if __name__ == "__main__":
    unittest.main()
