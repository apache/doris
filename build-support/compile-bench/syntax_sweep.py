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
appended, so nothing is written to the build directory. Front-end-only checks
run in roughly half the time of a real compile and catch every missing-include
or missing-declaration fallout a cut can cause.

Usage:
  python3 syntax_sweep.py [--build-dir DIR] [--jobs N] [--filter SUBSTR]
                          [--fail-log FILE]

  --filter limits the sweep to TUs whose source path contains SUBSTR
  (e.g. --filter load/memtable for a quick re-check of one subsystem).

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


def mangle(cmd):
    args = shlex.split(cmd)
    out, skip = [], False
    for a in args:
        if skip:
            skip = False
            continue
        if a in STRIP_WITH_ARG:
            skip = True
            continue
        if a in STRIP_FLAGS or a.startswith("-ftime-trace"):
            continue
        out.append(a)
    out.append("-fsyntax-only")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build-dir",
                    default=os.path.join(REPO_ROOT, "be", "build_Release_compile_bench"))
    ap.add_argument("--jobs", type=int, default=max(2, (os.cpu_count() or 8) // 2))
    ap.add_argument("--filter", default="",
                    help="only sweep TUs whose path contains this substring")
    ap.add_argument("--fail-log", default="",
                    help="write full stderr of every failing TU to this file")
    args = ap.parse_args()

    src_prefix = os.path.join(REPO_ROOT, "be", "src") + os.sep
    with open(os.path.join(args.build_dir, "compile_commands.json")) as f:
        entries = [e for e in json.load(f)
                   if e["file"].startswith(src_prefix) and args.filter in e["file"]]
    print(f"{len(entries)} TUs to check ({args.jobs} jobs)", flush=True)
    t0 = time.time()
    fails = []
    done = 0

    def run(e):
        p = subprocess.run(mangle(e["command"]), cwd=e["directory"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                           text=True)
        return e["file"], p.returncode, p.stderr

    with concurrent.futures.ThreadPoolExecutor(args.jobs) as ex:
        for src, rc, err in ex.map(run, entries):
            done += 1
            if rc != 0:
                fails.append((src, err))
                print(f"FAIL {src.replace(src_prefix, '')}", flush=True)
            if done % 100 == 0:
                print(f"  …{done}/{len(entries)} ({time.time() - t0:.0f}s, "
                      f"{len(fails)} failures)", flush=True)

    print(f"\n==== {len(fails)} failing TU(s) of {len(entries)} "
          f"in {time.time() - t0:.0f}s ====", flush=True)
    if args.fail_log and fails:
        with open(args.fail_log, "w") as f:
            for src, err in fails:
                f.write(f"===== {src}\n{ANSI.sub('', err)}\n")
    for src, err in fails[:15]:
        first = [l for l in ANSI.sub("", err).splitlines() if " error: " in l][:2]
        print(src.replace(src_prefix, ""))
        for l in first:
            print("   ", l[:200])
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
