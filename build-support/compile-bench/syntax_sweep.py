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
"""Parallel -fsyntax-only sweep over BE TUs using compile_commands.json.

Validates include-structure changes (header cuts, forward-declaration swaps)
against every TU without mutating the ninja build state: each compile command
is replayed with -o/-c/-MD/-MT/-MF/-ftime-trace* stripped and -fsyntax-only
appended, so nothing is written to the build directory.

Sweep scope: all first-party TUs -- standalone be/src entries AND unity
batches (CMake unity .cxx live under the build dir and would be silently
skipped by a be/src path filter; each batch textually includes its member
.cpp files, so checking the batch checks the members). Third-party contrib
(openblas/faiss/clucene/orc) and generated sources are excluded.

--no-pch strips the PCH preamble (-include-pch / forced cmake_pch.hxx
include) so every TU is checked against its NATURAL include closure. This is
the mode that catches "TU compiles only because the PCH leaks symbols it
never includes" debt -- mandatory before slimming pch.h, and the only mode
that works while a header included by pch.h has been edited but the .pch has
not been rebuilt.

Usage:
  python3 syntax_sweep.py [--build-dir DIR] [--jobs N] [--filter SUBSTR]
                          [--no-pch] [--fail-list FILE] [--fail-log FILE]
                          [--timeout SECS]

Exit code: 0 if every TU passes, 1 otherwise.
"""

import argparse
import concurrent.futures
import json
import os
import re
import shlex
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
ANSI = re.compile(r"\x1b\[[0-9;]*m")

STRIP_WITH_ARG = ("-o", "-MT", "-MF")
STRIP_FLAGS = ("-c", "-MD", "-MMD")


def mangle(cmd, no_pch=False):
    args = shlex.split(cmd)
    out = []
    i = 0
    while i < len(args):
        a = args[i]
        if a in STRIP_WITH_ARG:
            i += 2
            continue
        if a in STRIP_FLAGS or a.startswith("-ftime-trace"):
            i += 1
            continue
        if no_pch:
            if a == "-Winvalid-pch":
                i += 1
                continue
            # -Xclang -include-pch -Xclang <file.pch>
            # -Xclang -include     -Xclang <cmake_pch.hxx>
            if (a == "-Xclang" and i + 3 < len(args)
                    and args[i + 1] in ("-include-pch", "-include")
                    and args[i + 2] == "-Xclang"
                    and ("cmake_pch" in args[i + 3]
                         or args[i + 3].endswith(".pch"))):
                i += 4
                continue
            # plain -include <cmake_pch.hxx> form
            if a == "-include" and i + 1 < len(args) and "cmake_pch" in args[i + 1]:
                i += 2
                continue
        out.append(a)
        i += 1
    out.append("-fsyntax-only")
    return out


def display_name(path, src_prefix):
    if path.startswith(src_prefix):
        return path[len(src_prefix):]
    be_prefix = os.path.dirname(os.path.dirname(src_prefix)) + os.sep
    if path.startswith(be_prefix + "test" + os.sep):
        return path[len(be_prefix):]
    if "/Unity/" in path:
        # .../src/<dir>/CMakeFiles/<tgt>.dir/Unity/unity_N_cxx.cxx
        m = re.search(r"/src/([^/]+)/CMakeFiles/[^/]+/Unity/(unity_\d+)", path)
        if m:
            return f"{m.group(1)}/[{m.group(2)}]"
    return path


def first_party(e, src_prefix, test_prefix=None):
    f = e["file"]
    if f.startswith(src_prefix):
        return True
    if test_prefix and f.startswith(test_prefix):
        return True
    # CMake unity batches for first-party targets live under the build dir
    return "/Unity/" in f and ("/src/" in f or (test_prefix and "/test/" in f))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build-dir",
                    default=os.path.join(REPO_ROOT, "be", "build_Release_compile_bench"))
    ap.add_argument("--jobs", type=int, default=max(2, (os.cpu_count() or 8) // 2))
    ap.add_argument("--filter", default="",
                    help="only sweep TUs whose path contains this substring")
    ap.add_argument("--no-pch", action="store_true",
                    help="strip PCH preamble: check each TU's natural include closure")
    ap.add_argument("--fail-list", default="",
                    help="write sorted display names of failing TUs to this file")
    ap.add_argument("--fail-log", default="",
                    help="write full stderr of every failing TU to this file")
    ap.add_argument("--timeout", type=int, default=600,
                    help="per-TU timeout in seconds (counts as failure)")
    ap.add_argument("--include-tests", action="store_true",
                    help="also sweep be/test TUs (point --build-dir at a "
                         "-DMAKE_TEST=ON tree; be/test builds without a PCH, "
                         "so the UT line always compiles natural closures)")
    args = ap.parse_args()

    src_prefix = os.path.join(REPO_ROOT, "be", "src") + os.sep
    test_prefix = (os.path.join(REPO_ROOT, "be", "test") + os.sep
                   if args.include_tests else None)
    with open(os.path.join(args.build_dir, "compile_commands.json")) as f:
        entries = [e for e in json.load(f)
                   if first_party(e, src_prefix, test_prefix)
                   and args.filter in e["file"]]
    print(f"{len(entries)} TUs to check ({args.jobs} jobs"
          f"{', natural closure / no PCH' if args.no_pch else ''})", flush=True)
    t0 = time.time()
    fails = []
    done = 0

    def run(e):
        cmd = mangle(e["command"], no_pch=args.no_pch)
        name = display_name(e["file"], src_prefix)
        try:
            p = subprocess.run(cmd, cwd=e["directory"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                               text=True, timeout=args.timeout)
            return name, p.returncode, p.stderr
        except subprocess.TimeoutExpired:
            return name, 124, f"TIMEOUT after {args.timeout}s"

    with concurrent.futures.ThreadPoolExecutor(args.jobs) as ex:
        for name, rc, err in ex.map(run, entries):
            done += 1
            if rc != 0:
                fails.append((name, err))
                print(f"FAIL {name}", flush=True)
            if done % 50 == 0:
                print(f"  ...{done}/{len(entries)} ({time.time() - t0:.0f}s, "
                      f"{len(fails)} failures)", flush=True)

    print(f"\n==== {len(fails)} failing TU(s) of {len(entries)} "
          f"in {time.time() - t0:.0f}s ====", flush=True)
    if args.fail_list:
        with open(args.fail_list, "w") as f:
            for name, _ in sorted(fails):
                f.write(name + "\n")
    if args.fail_log and fails:
        with open(args.fail_log, "w") as f:
            for name, err in fails:
                f.write(f"===== {name}\n{ANSI.sub('', err)}\n")
    for name, err in fails[:15]:
        first = [l for l in ANSI.sub("", err).splitlines() if " error: " in l][:2]
        print(name)
        for l in first:
            print("   ", l[:200])
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
