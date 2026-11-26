#!/usr/bin/env bash
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
#
# The compile-bench tools read a build directory that may or may not have been
# configured with ENABLE_UNITY_BUILD=ON, and a unity TU looks nothing like the
# per-file shape they were written against. Both behaviours below are invisible
# on a real build (one is a refusal, the other a grouping key), so they need a
# fixture to stay honest.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "${ROOT}/compile-bench" <<'PY'
import importlib.util
import os
import stat
import sys
import tempfile
import types

bench_dir = sys.argv[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(bench_dir, name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cut_impact = load("cut_impact")
report = load("report")
repo = cut_impact.REPO_ROOT
failures = []


def check(label, cond):
    print(("  ok   " if cond else "  FAIL ") + label)
    if not cond:
        failures.append(label)


def load_tus_with(deps_output):
    """Run cut_impact.load_tus against a canned `ninja -t deps` database."""
    tmp = tempfile.mkdtemp()
    fake_ninja = os.path.join(tmp, "ninja")
    with open(fake_ninja, "w") as fh:
        fh.write("#!/bin/sh\ncat <<'EOF'\n" + deps_output + "\nEOF\n")
    os.chmod(fake_ninja, os.stat(fake_ninja).st_mode | stat.S_IEXEC)
    old_path = os.environ["PATH"]
    os.environ["PATH"] = tmp + os.pathsep + old_path
    try:
        return cut_impact.load_tus(os.path.join(tmp, "build"), types.SimpleNamespace(files=set()))
    finally:
        os.environ["PATH"] = old_path


print("cut_impact.load_tus")

# GREEN: the per-file shape the analysis is written against.
standalone = (
    "src/service/CMakeFiles/Service.dir/http/action/health_action.cpp.o: "
    "#deps 2, deps mtime 1 (VALID)\n"
    "    {repo}/be/src/service/http/action/health_action.cpp\n"
    "    {repo}/be/src/service/http/action/health_action.h"
).format(repo=repo)
tus = load_tus_with(standalone)
check("keeps a standalone TU", list(tus) == [repo + "/be/src/service/http/action/health_action.cpp"])

# RED: a unity TU. Its member sources are invisible here, so the analysis must
# refuse rather than quietly run on whatever is left.
unity = (
    "src/information_schema/CMakeFiles/InformationSchema.dir/Unity/unity_0_cxx.cxx.o: "
    "#deps 2, deps mtime 1 (VALID)\n"
    "    src/information_schema/CMakeFiles/InformationSchema.dir/Unity/unity_0_cxx.cxx\n"
    "    {repo}/be/src/information_schema/schema_scanner.cpp"
).format(repo=repo)
try:
    load_tus_with(unity)
    check("refuses a unity deps database", False)
except SystemExit as exc:
    message = str(exc)
    check("refuses a unity deps database", "unity translation units" in message)
    check("names the way out", "ENABLE_UNITY_BUILD=OFF" in message)

print("report.group_keys")
unity_src = ("be/build_Release_compile_bench/src/storage/CMakeFiles/Storage.dir"
             "/Unity/unity_0_cxx.cxx")
check("unity TU groups under the module it batches",
      report.group_keys(unity_src) == ("be/src/storage", "be/src/storage"))
check("ordinary source is unchanged",
      report.group_keys("be/src/service/http/action/health_action.cpp")
      == ("be/src/service", "be/src/service/http"))
check("build-tree non-unity path is unchanged",
      report.group_keys("be/build_Release/src/agent/foo.cpp")[0] == "be")

if failures:
    sys.exit("{} check(s) failed".format(len(failures)))
print("all checks passed")
PY
