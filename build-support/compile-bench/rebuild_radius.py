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
"""Report how many objects a header change actually rebuilds.

Reads ninja's dependency database (`ninja -t deps`) and counts, per header,
the object targets that list it -- the incremental rebuild radius. Headers
reachable from pch.h are flagged separately: a PCH dependency invalidates
every first-party object no matter how few TUs name a symbol from it, so
those are the ones worth cutting first.

Usage:
  python3 rebuild_radius.py [--build-dir DIR] [header-suffix ...]

Header arguments are matched as path suffixes ("core/field.h"); without
any, a default set of hot BE headers is reported.

Note that `touch <header> && ninja -n | wc -l` is NOT a usable substitute
here: CONFIGURE_DEPENDS globs make the dry run stop at "Re-running CMake"
before it lists a single compile edge.
"""

import argparse
import os
import subprocess
import sys

DEFAULT_HEADERS = [
    "storage/olap_common.h",
    "storage/utils.h",
    "util/uid_util.h",
    "util/json/path_in_data.h",
    "storage/index/zone_map/zone_map_index.h",
    "exprs/expr_zonemap_filter.h",
    "storage/index/inverted/inverted_index_iterator.h",
    "storage/index/inverted/inverted_index_parser.h",
    "storage/tablet/tablet_schema.h",
    "util/pretty_printer.h",
    "exprs/function/function.h",
    "core/column/column.h",
    "core/field.h",
    "core/types.h",
    "common/status.h",
    "runtime/runtime_profile.h",
]


def main():
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build-dir",
                    default=os.path.join(repo, "be", "build_Release_compile_bench"))
    ap.add_argument("headers", nargs="*", help="path suffixes to report on")
    args = ap.parse_args()
    headers = args.headers or DEFAULT_HEADERS

    counts = {h: 0 for h in headers}
    pch_deps = set()
    objects = 0

    proc = subprocess.Popen(["ninja", "-C", args.build_dir, "-t", "deps"],
                            stdout=subprocess.PIPE, text=True, errors="replace",
                            bufsize=1 << 20)
    target, target_is_pch, hits = None, False, set()

    def flush():
        nonlocal objects
        if target is None:
            return
        if target_is_pch:
            pch_deps.update(hits)
        else:
            for h in hits:
                counts[h] += 1
        if target.endswith((".o", ".obj")):
            objects += 1

    for line in proc.stdout:
        if line[:1] not in (" ", "\t", "\n"):
            flush()
            target = line.split(":")[0].strip()
            target_is_pch = "cmake_pch" in target or target.endswith(".pch")
            hits = set()
        elif line.strip():
            dep = line.strip()
            for h in headers:
                if dep.endswith(h):
                    hits.add(h)
    flush()
    proc.wait()

    print(f"{objects:,} object targets in the deps database\n")
    print(f"{'header':58s} {'dependents':>10s}   via PCH (= rebuilds everything)")
    for h in headers:
        flag = "YES" if h in pch_deps else "no"
        print(f"{h:58s} {counts[h]:>10,}   {flag}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
