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

"""Layering guard for BE headers.

A handful of headers -- runtime/exec_env.h above all -- are included, directly or
not, by most of the backend. Anything they pull in becomes a dependency of nearly
every translation unit, so one stray include can multiply rebuild cost by an order
of magnitude and does so silently: the code still compiles, only the build slows
down. This script turns that into a build error instead.

Each rule names a hub header and a directory prefix it must not reach. Keeping a
hub out of a subsystem is what lets that subsystem's headers be edited cheaply.

Usage:
    build-support/check-header-deps.py            # enforce the rules
    build-support/check-header-deps.py --report   # rank headers by rebuild cost
"""

import argparse
import collections
import os
import re
import sys

INCLUDE = re.compile(r'^\s*#\s*include\s+"([^"]+)"')
SOURCE_ROOTS = ("be/src", "be/test")
INCLUDE_ROOT = "be/src"

# (hub header, forbidden prefix, allowed exceptions, why).
# A hub must not reach the forbidden subtree through ANY chain of includes, except
# through the listed headers. An exception is only appropriate for a header that
# carries declarations or plain data types and pulls in nothing of its own -- adding
# one should be a deliberate decision, which is exactly why they are listed here
# instead of being inferred.
RULES = [
    (
        "runtime/exec_env.h",
        "storage/index/",
        {
            # A leaf statistics struct with no project includes of its own, which
            # storage/olap_common.h carries as a plain data type.
            "storage/index/inverted/inverted_index_stats.h",
        },
        "ExecEnv only ever names index types as pointers and already forward-declares "
        "them; reaching the index implementation headers from here puts the whole "
        "index writer stack (and CLucene) in front of most of the backend",
    ),
]

# Forward-declaration headers are the sanctioned way through a barrier: they carry
# declarations only, so they cost nothing to include.
FWD_SUFFIX = "_fwd.h"


def load_includes():
    """Maps each repo file to the list of project headers it includes."""
    includes = {}
    for root in SOURCE_ROOTS:
        for directory, _, names in os.walk(root):
            for name in names:
                if not name.endswith((".h", ".hpp", ".cpp", ".cc")):
                    continue
                path = os.path.join(directory, name)
                with open(path, encoding="utf-8", errors="ignore") as handle:
                    includes[path] = [
                        match.group(1)
                        for match in (INCLUDE.match(line) for line in handle)
                        if match
                    ]
    return includes


def resolve(header, includes):
    """Maps an include spelling to a repo path, or None when it is external."""
    path = os.path.join(INCLUDE_ROOT, header)
    return path if path in includes else None


def reachable(start, includes):
    """Every header reachable from `start`, with the chain that got there."""
    chains = {start: [start]}
    frontier = [start]
    while frontier:
        current = frontier.pop()
        path = resolve(current, includes)
        if path is None:
            continue
        for nxt in includes[path]:
            if nxt in chains:
                continue
            chains[nxt] = chains[current] + [nxt]
            frontier.append(nxt)
    return chains


def translation_units_affected(includes):
    """How many translation units each header can force a rebuild of."""
    users = collections.defaultdict(set)
    for path, headers in includes.items():
        for header in headers:
            users[header].add(path)
    counts = {}
    for header in users:
        seen, frontier = set(), [header]
        while frontier:
            current = frontier.pop()
            for user in users.get(current, ()):
                if user in seen:
                    continue
                seen.add(user)
                if user.startswith(INCLUDE_ROOT + "/"):
                    frontier.append(user[len(INCLUDE_ROOT) + 1:])
        counts[header] = sum(1 for f in seen if f.endswith((".cpp", ".cc")))
    return counts


def enforce(includes):
    failures = 0
    for hub, forbidden, allowed, why in RULES:
        if resolve(hub, includes) is None:
            print(f"error: rule names a missing header: {hub}", file=sys.stderr)
            failures += 1
            continue
        chains = reachable(hub, includes)
        for header, chain in sorted(chains.items()):
            if not header.startswith(forbidden) or header.endswith(FWD_SUFFIX):
                continue
            if header in allowed:
                continue
            failures += 1
            print(f"error: {hub} must not reach {forbidden}*", file=sys.stderr)
            print(f"  reason: {why}", file=sys.stderr)
            print("  chain:  " + "\n       -> ".join(chain), file=sys.stderr)
            print(
                "  fix:    forward-declare the type in the header and include the "
                "real header in the .cpp, or route it through a *_fwd.h",
                file=sys.stderr,
            )
            break
    return failures


def report(includes):
    counts = translation_units_affected(includes)
    ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:30]
    print(f"{'TUs rebuilt':>11}  header")
    for header, count in ranked:
        print(f"{count:>11}  {header}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        action="store_true",
        help="rank headers by how many translation units they force a rebuild of",
    )
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    includes = load_includes()

    if args.report:
        report(includes)
        return 0

    failures = enforce(includes)
    if failures:
        print(f"\n{failures} header layering violation(s)", file=sys.stderr)
        return 1
    print(f"header layering: {len(RULES)} rule(s) satisfied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
