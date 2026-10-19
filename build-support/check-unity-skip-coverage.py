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

"""Unity-skip coverage for test-included source files.

A few tests `#include` a be/src .cpp directly (to reach file-static helpers or
to instantiate a template with test types). That only links because the source
file is opted out of unity batching: if it stays in a batch, the batch object is
pulled into the doris_be_test link for a *sibling's* symbol and its copy of the
file's definitions collides with the test's inlined copy -- duplicate strong
symbols, reported at the end of the BE UT build, an hour after the mistake.
This script reports the mistake at configure time instead, with the exact skip
entry to add.

It is the mirror of the fail-loud check inside doris_skip_unity_inclusion()
(be/CMakeLists.txt): that one catches skip entries pointing at files that no
longer exist; this one catches files that need an entry and do not have one.

A source file counts as covered when its owning CMakeLists (nearest ancestor
with a CMakeLists.txt, else any CMakeLists naming it) does one of:

  literal entry     the file's path appears in a list fed to
                    doris_skip_unity_inclusion(), via set()/list(APPEND)/direct
                    arguments;
  whole-target      the skip list is initialized from the target's own source
  opt-out           list (`set(<VAR> ${SRC_FILES})`), minus any
                    `list(FILTER <VAR> EXCLUDE REGEX ...)` the file matches;
  no unity          the owning CMakeLists never sets UNITY_BUILD, so there are
                    no batches to collide with (archive link rules protect
                    plain object files).

Any other CMake idiom is unknown to this script on purpose: it fails loudly so
the idiom is either replaced with a literal entry or taught here, instead of
being silently guessed wrong.

Usage:
    build-support/check-unity-skip-coverage.py
"""

import os
import re
import sys

TEST_ROOT = "be/test"
SRC_ROOT = "be/src"

TEST_INCLUDE = re.compile(r'^\s*#\s*include\s+"([^"]+\.(?:cpp|cc))"')
SKIP_CALL = re.compile(r"doris_skip_unity_inclusion\s*\(([^)]*)\)", re.S)
SET_BLOCK = re.compile(r"\bset\s*\(\s*(\w+)([^)]*)\)", re.S)
APPEND_BLOCK = re.compile(r"\blist\s*\(\s*APPEND\s+(\w+)([^)]*)\)", re.S)
FILTER_BLOCK = re.compile(
    r'\blist\s*\(\s*FILTER\s+(\w+)\s+EXCLUDE\s+REGEX\s+"([^"]*)"\s*\)'
)
VAR_REF = re.compile(r"\$\{(\w+)\}")


def strip_comments(text):
    return "\n".join(line.split("#", 1)[0] for line in text.splitlines())


def cpp_tokens(blob):
    """Path-shaped tokens in a CMake argument blob, ${...} prefixes stripped."""
    tokens = []
    for token in blob.split():
        if not token.endswith((".cpp", ".cc")):
            continue
        tokens.append(re.sub(r"\$\{\w+\}", "", token).lstrip("/"))
    return tokens


class CMakeFile:
    def __init__(self, path):
        self.path = path
        self.directory = os.path.dirname(path)
        with open(path, encoding="utf-8", errors="ignore") as handle:
            text = strip_comments(handle.read())
        self.sets_unity = "UNITY_BUILD" in text

        sets = {}
        for name, blob in SET_BLOCK.findall(text):
            sets.setdefault(name, []).append(blob)
        for name, blob in APPEND_BLOCK.findall(text):
            sets.setdefault(name, []).append(blob)
        filters = {}
        for name, regex in FILTER_BLOCK.findall(text):
            filters.setdefault(name, []).append(regex)

        # Literal skip entries and whether the skip list is seeded from the
        # target's whole source list.
        self.entries = []
        self.whole_target = False
        self.whole_target_excludes = []
        for blob in SKIP_CALL.findall(text):
            self.entries.extend(cpp_tokens(blob))
            for var in VAR_REF.findall(blob):
                for var_blob in sets.get(var, ()):
                    self.entries.extend(cpp_tokens(var_blob))
                    if re.search(r"\$\{\w*SRC_FILES\}", var_blob):
                        self.whole_target = True
                        self.whole_target_excludes.extend(filters.get(var, ()))

    def resolved_entries(self):
        """Skip entries as repo-relative paths (handles ../ hops)."""
        for entry in self.entries:
            yield os.path.normpath(os.path.join(self.directory, entry))

    def covers(self, repo_path):
        """Does this CMakeLists skip-list `repo_path` (repo-relative .cpp)?"""
        for resolved in self.resolved_entries():
            if resolved == repo_path or resolved.endswith("/" + repo_path):
                return True
        if self.whole_target and repo_path.startswith(self.directory + "/"):
            # cmake applies FILTER regexes to absolute paths; approximate with
            # the repo-relative one, which shares every path component that the
            # tree's regexes (".*/format_v2/.*") anchor on.
            return not any(
                re.search(regex, repo_path)
                for regex in self.whole_target_excludes
            )
        return False


def owning_cmakelists(repo_path, cmake_files):
    directory = os.path.dirname(repo_path)
    while directory.startswith(SRC_ROOT):
        candidate = os.path.join(directory, "CMakeLists.txt")
        if candidate in cmake_files:
            return cmake_files[candidate]
        directory = os.path.dirname(directory)
    return None


def test_included_sources():
    """(test file, line, be/src-relative include) for every src .cpp a test
    includes."""
    found = []
    for directory, _, names in os.walk(TEST_ROOT):
        for name in names:
            if not name.endswith((".cpp", ".cc", ".h", ".hpp")):
                continue
            path = os.path.join(directory, name)
            with open(path, encoding="utf-8", errors="ignore") as handle:
                for lineno, line in enumerate(handle, 1):
                    match = TEST_INCLUDE.match(line)
                    if not match:
                        continue
                    include = match.group(1)
                    if os.path.exists(os.path.join(SRC_ROOT, include)):
                        found.append((path, lineno, include))
    return found


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    cmake_files = {}
    for directory, _, names in os.walk(SRC_ROOT):
        if "CMakeLists.txt" in names:
            path = os.path.join(directory, "CMakeLists.txt")
            cmake_files[path] = CMakeFile(path)

    failures = 0
    checked = 0
    for test_path, lineno, include in test_included_sources():
        checked += 1
        repo_path = os.path.join(SRC_ROOT, include)
        owner = owning_cmakelists(repo_path, cmake_files)
        if owner is not None and not owner.sets_unity:
            continue  # no unity batches in this target, nothing to collide with
        if owner is not None and owner.covers(repo_path):
            continue
        if any(f.covers(repo_path) for f in cmake_files.values()):
            continue  # cross-directory entry (e.g. appended into a sibling's list)
        failures += 1
        print(
            f"error: {test_path}:{lineno} includes {repo_path}, which is not "
            "opted out of unity batching",
            file=sys.stderr,
        )
        print(
            "  reason: the unity batch object containing this file gets pulled "
            "into the doris_be_test link for a sibling's symbol, and its "
            "definitions collide with the test's inlined copy -- duplicate "
            "strong symbols at the end of the BE UT build",
            file=sys.stderr,
        )
        if owner is not None:
            entry = os.path.relpath(repo_path, owner.directory)
            print(
                f"  fix:    add ${{CMAKE_CURRENT_SOURCE_DIR}}/{entry} to the "
                f"doris_skip_unity_inclusion list in {owner.path}",
                file=sys.stderr,
            )
        else:
            print(
                "  fix:    add the file to the doris_skip_unity_inclusion "
                "list of the CMakeLists whose source glob compiles it; for "
                "the cross-directory entry form, see the adbc_reader.cpp "
                "entry in be/src/format/CMakeLists.txt",
                file=sys.stderr,
            )
    if failures:
        print(f"\n{failures} unity-skip coverage violation(s)", file=sys.stderr)
        return 1
    print(f"unity-skip coverage: {checked} test-included source file(s) all covered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
