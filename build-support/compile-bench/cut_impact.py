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
"""Estimate the blast radius of cutting one #include edge from a hub header.

Before removing `#include "T"` from hub header H, you want to know:
  1. which TUs currently reach T only through H (they will stop seeing T);
  2. which of those TUs (or headers they keep) actually reference symbols
     declared in T's include subtree, and therefore need a direct include
     added ("seeding") before the edge can be cut safely.

Data sources:
  * `ninja -t deps` from a completed compile-bench build directory: the real,
    per-TU flat header closure (what each TU actually included in this build
    configuration).
  * The parsed include graph of be/src + gensrc/build: edge structure, built
    by scanning `#include` directives and resolving them the way the compiler
    does (includer dir first for quoted includes, then -I roots).

Per candidate TU the script runs two BFS traversals over the parsed graph,
both restricted to the TU's real closure: one with the edge, one without.
The difference is the set of headers this TU loses. Symbols defined in lost
headers are then matched (word-level, comments stripped) against the files
the TU keeps; every hit becomes a seeding suggestion "file F must directly
include header L". The result is an estimate — conditional includes the
parser cannot see and symbol matches inside string literals can produce
noise — so spot-check a sample (e.g. -fsyntax-only) before mass edits.

Usage:
  # single edge: what happens if H stops including T
  python3 cut_impact.py edge runtime/exec_env.h \
      information_schema/schema_routine_load_job_scanner.h

  # rank every direct project include of a hub by cut impact
  python3 cut_impact.py audit runtime/exec_env.h

  # machine-readable detail / manual-verification sample
  python3 cut_impact.py edge H T --json out.json --sample 5
"""

import argparse
import collections
import json
import os
import random
import re
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

SOURCE_EXTS = (".cpp", ".cc", ".c", ".cxx")
HEADER_EXTS = (".h", ".hpp", ".hh", ".inc", ".ipp")

INCLUDE_RE = re.compile(r'^\s*#\s*include\s+(["<])([^">]+)[">]', re.M)
COMMENT_RE = re.compile(r"//[^\n]*|/\*.*?\*/", re.S)
TOKEN_RE = re.compile(r"[A-Za-z_]\w{2,}")
# Type *definitions* only (a trailing '{' is required), not forward decls.
TYPE_DEF_RE = re.compile(
    r"\b(?:class|struct|enum(?:\s+(?:class|struct))?)\s+"
    r"(?:\[\[[^\]]*\]\]\s*|[A-Z_]{3,}\s+)?"  # attributes / export macros
    r"([A-Za-z_]\w*)\s*(?:final\s*)?(?::[^;{}]*)?\{"
)
USING_RE = re.compile(r"\busing\s+([A-Za-z_]\w*)\s*=")
TYPEDEF_RE = re.compile(r"\btypedef\b[^;]*?\b([A-Za-z_]\w*)\s*;")
DEFINE_RE = re.compile(r"^\s*#\s*define\s+([A-Za-z_]\w*)", re.M)


def read_text(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return ""


class IncludeGraph:
    """Parsed include-edge structure over project files (be/src, gensrc/build)."""

    def __init__(self, roots):
        self.roots = roots  # ordered list of abs dirs, first match wins for display
        self.files = {}  # abs path -> True
        self.includes = {}  # abs path -> list of abs paths (project files only)
        self._stripped = {}  # abs path -> comment-stripped text (lazy)
        self._tokens = {}  # abs path -> set of identifier tokens (lazy)
        self._symbols = {}  # abs path -> set of defined top-level names (lazy)
        self._scan()
        self._parse_edges()

    def _scan(self):
        for root in self.roots:
            for dirpath, _dirnames, filenames in os.walk(root):
                for name in filenames:
                    if name.endswith(SOURCE_EXTS) or name.endswith(HEADER_EXTS):
                        p = sys.intern(os.path.normpath(os.path.join(dirpath, name)))
                        self.files[p] = True

    def _parse_edges(self):
        for path in self.files:
            edges = []
            for m in INCLUDE_RE.finditer(read_text(path)):
                quoted = m.group(1) == '"'
                target = self.resolve(m.group(2), os.path.dirname(path), quoted)
                if target is not None and target != path:
                    edges.append(target)
            self.includes[path] = edges

    def resolve(self, inc, includer_dir, quoted=True):
        if quoted:
            cand = sys.intern(os.path.normpath(os.path.join(includer_dir, inc)))
            if cand in self.files:
                return cand
        for root in self.roots:
            cand = sys.intern(os.path.normpath(os.path.join(root, inc)))
            if cand in self.files:
                return cand
        return None

    def display(self, path):
        for root in self.roots:
            if path.startswith(root + os.sep):
                return os.path.relpath(path, root)
        return path

    def subtree(self, start):
        """All files reachable from `start` in the unrestricted parsed graph."""
        seen = {start}
        queue = collections.deque([start])
        while queue:
            for nxt in self.includes.get(queue.popleft(), ()):
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        return seen

    def stripped_text(self, path):
        if path not in self._stripped:
            self._stripped[path] = COMMENT_RE.sub(" ", read_text(path))
        return self._stripped[path]

    def tokens(self, path):
        if path not in self._tokens:
            self._tokens[path] = set(TOKEN_RE.findall(self.stripped_text(path)))
        return self._tokens[path]

    def symbols(self, path):
        """Top-level names *defined* by this header (types, aliases, macros)."""
        if path not in self._symbols:
            text = self.stripped_text(path)
            names = set()
            for regex in (TYPE_DEF_RE, USING_RE, TYPEDEF_RE, DEFINE_RE):
                names.update(regex.findall(text))
            self._symbols[path] = {n for n in names if len(n) >= 3}
        return self._symbols[path]


def load_tus(build_dir, graph):
    """source abs path -> set of project files in its real (ninja) closure."""
    proc = subprocess.Popen(
        ["ninja", "-C", build_dir, "-t", "deps"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    tus = {}
    src_prefix = os.path.join(REPO_ROOT, "be", "src") + os.sep
    gen_prefix = os.path.join(REPO_ROOT, "gensrc", "build") + os.sep
    cur = None  # dep set of the current block's TU; None while skipping a block
    expect_source = False  # the next indented line is the block's source file
    for line in proc.stdout:
        if line.startswith("    "):
            p = line.strip()
            if not os.path.isabs(p):
                p = os.path.join(build_dir, p)
            p = os.path.normpath(p)
            if expect_source:  # first dep line = the source file itself
                expect_source = False
                if p.startswith((src_prefix, gen_prefix)):
                    key = sys.intern(p)
                    cur = tus.setdefault(key, set())
                    cur.add(key)
                else:
                    cur = None  # foreign TU (contrib etc.): skip whole block
            elif cur is not None and p in graph.files:
                cur.add(sys.intern(p))
        else:  # block header "<target>: #deps N, ... (VALID|STALE)" or noise
            expect_source = line.rstrip().endswith("(VALID)")
            cur = None
    proc.wait()
    return tus


def analyze_edge(graph, tus, edges, collect_seeds=True, freq_cap=250):
    """Simulate removing include edges [(hub, cut), ...]; return per-TU losses
    and seed advice. Symbols referenced by more than `freq_cap` files are
    treated as noise (e.g. thrift's nested `enum type`) and ignored."""
    cut_set = set(edges)
    targets = {c for _h, c in edges}
    res = {
        "edges": list(edges),
        "candidates": 0,  # TUs whose real closure contains any cut target
        "affected": {},  # source -> sorted list of lost files
        "unaffected": 0,  # still reach everything via other paths
        "parser_gap": [],  # closure has a target but parsed BFS never saw it
        "seeds": collections.defaultdict(lambda: collections.defaultdict(set)),
        "seed_tu_count": collections.Counter(),  # (file, lost) -> #TUs
        "lost_spread": collections.Counter(),  # lost file -> #TUs losing it
        "tu_needs": {},  # source -> [(seed file, lost header), ...]
    }

    def bfs(source, allowed, skip_edges):
        seen = {source}
        queue = collections.deque([source])
        while queue:
            node = queue.popleft()
            for nxt in graph.includes.get(node, ()):
                if skip_edges and (node, nxt) in cut_set:
                    continue
                if nxt in allowed and nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        return seen

    for source, closure in sorted(tus.items()):
        if not targets & closure:
            continue
        res["candidates"] += 1
        before = bfs(source, closure, skip_edges=False)
        if not targets & before:
            res["parser_gap"].append(source)
            continue
        after = bfs(source, closure, skip_edges=True)
        lost = before - after
        if not lost:
            res["unaffected"] += 1
            continue
        res["affected"][source] = sorted(lost)
        for f in lost:
            res["lost_spread"][f] += 1

    if collect_seeds and res["affected"]:
        all_lost = set()
        for lost in res["affected"].values():
            all_lost.update(lost)
        # symbol -> headers (in the lost pool) defining it
        sym_owners = collections.defaultdict(set)
        for lf in all_lost:
            for s in graph.symbols(lf):
                sym_owners[s].add(lf)
        symset = set(sym_owners)
        # drop noise: symbols that occur in more files than freq_cap
        sym_freq = collections.Counter()
        file_hits = {}
        for path in graph.files:
            hit = graph.tokens(path) & symset
            if hit:
                file_hits[path] = hit
                sym_freq.update(hit)
        noisy = {s for s, n in sym_freq.items() if n > freq_cap}
        # file -> {lost header -> matched symbols}
        refs = collections.defaultdict(lambda: collections.defaultdict(set))
        header_refs = collections.defaultdict(set)  # lost header -> {files}
        for path, hit in file_hits.items():
            own = graph.symbols(path) if path.endswith(HEADER_EXTS) else ()
            for s in hit - noisy:
                if s in own:  # file defines this name itself (e.g. its own State)
                    continue
                for owner in sym_owners[s]:
                    if path != owner:
                        refs[path][owner].add(s)
                        header_refs[owner].add(path)
        for source, lost in res["affected"].items():
            kept = tus[source] - set(lost)
            needs = []
            for lf in lost:
                for f in header_refs.get(lf, ()):
                    if f in kept:
                        res["seeds"][f][lf].update(refs[f][lf])
                        res["seed_tu_count"][(f, lf)] += 1
                        needs.append((f, lf))
            if needs:
                res["tu_needs"][source] = needs
    return res


def print_edge_report(graph, res, sample=0, top=15):
    d = graph.display
    print("== Cut impact ==")
    for hub, cut in res["edges"]:
        print(f"   {d(hub)} -/-> {d(cut)}")
    n_aff = len(res["affected"])
    print(f"TUs whose real closure contains a cut header   : {res['candidates']}")
    print(f"  affected (lose headers once the edge is cut) : {n_aff}")
    print(f"  unaffected (other include paths still exist) : {res['unaffected']}")
    if res["parser_gap"]:
        print(f"  unanalyzable (parser gap, verify manually)   : {len(res['parser_gap'])}")
        for s in res["parser_gap"][:5]:
            print(f"      {d(s)}")
    if not n_aff:
        return

    for hub, _cut in res["edges"]:
        hub_seed = res["seeds"].get(hub)
        if hub_seed:
            syms = sorted({s for ss in hub_seed.values() for s in ss})
            print(f"Hub self-check: {d(hub)} itself references: {', '.join(syms[:8])}")
            print("  -> this hub still needs (some of) the cut subtree; NOT a dead include.")
        else:
            print(f"Hub self-check: {d(hub)} references nothing from the lost subtree "
                  "-> dead include for this hub itself.")

    print(f"\nLost-header spread (top {top} by #TUs losing it):")
    for f, n in res["lost_spread"].most_common(top):
        print(f"  {n:5d} TUs lose  {d(f)}")

    if res["seeds"]:
        print("\nSeed list — add these direct includes BEFORE cutting the edge:")
        rows = []
        for f, per_lost in res["seeds"].items():
            for lf, syms in per_lost.items():
                rows.append((res["seed_tu_count"][(f, lf)], f, lf, sorted(syms)))
        rows.sort(key=lambda r: (-r[0], r[1]))
        for n_tu, f, lf, syms in rows:
            shown = ", ".join(syms[:6]) + (" …" if len(syms) > 6 else "")
            print(f"  {d(f)}\n      + #include \"{d(lf)}\"   [{n_tu} TU(s); uses: {shown}]")
        seeded_files = len(res["seeds"])
        print(f"  ({seeded_files} file(s) need seeding, {len(rows)} include line(s) total)")
    else:
        print("\nSeed list: EMPTY — no kept file references any lost symbol; "
              "cut is predicted safe without preparatory edits.")

    clean = n_aff - len(res["tu_needs"])
    print(f"\nPer-TU verdict: {clean}/{n_aff} affected TUs reference nothing they lose "
          f"(predicted to compile unchanged); {len(res['tu_needs'])} TU(s) rely on the "
          "seeded file(s) above (per-TU detail in --json).")

    if sample:
        print(f"\n-- Random sample of {min(sample, n_aff)} affected TU(s) for manual verification --")
        for source in random.sample(sorted(res["affected"]), min(sample, n_aff)):
            lost = res["affected"][source]
            print(f"  TU {d(source)}  loses {len(lost)} header(s):")
            for f in lost[:8]:
                print(f"      {d(f)}")
            if len(lost) > 8:
                print(f"      … and {len(lost) - 8} more")


def edge_json(graph, res):
    d = graph.display
    return {
        "edges": [[d(h), d(c)] for h, c in res["edges"]],
        "candidates": res["candidates"],
        "unaffected": res["unaffected"],
        "parser_gap": [d(s) for s in res["parser_gap"]],
        "affected": {d(s): [d(f) for f in lost] for s, lost in res["affected"].items()},
        "tu_needs": {d(s): [[d(f), d(lf)] for f, lf in needs]
                     for s, needs in res["tu_needs"].items()},
        "lost_spread": {d(f): n for f, n in res["lost_spread"].most_common()},
        "seeds": {
            d(f): {
                d(lf): {"symbols": sorted(syms),
                        "tu_count": res["seed_tu_count"][(f, lf)]}
                for lf, syms in per_lost.items()
            }
            for f, per_lost in res["seeds"].items()
        },
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build-dir",
                    default=os.path.join(REPO_ROOT, "be", "build_Release_compile_bench"),
                    help="ninja build dir with a completed build (default: bench dir)")
    sub = ap.add_subparsers(dest="mode", required=True)
    ap_edge = sub.add_parser("edge", help="impact of cutting one include edge")
    ap_edge.add_argument("hub", help="header that contains the include, e.g. runtime/exec_env.h")
    ap_edge.add_argument("cut", help="the included header to remove, e.g. io/cache/fs_file_cache_storage.h")
    ap_edge.add_argument("--and", dest="extra", nargs=2, action="append", default=[],
                         metavar=("HUB", "CUT"),
                         help="cut this edge too (repeatable): combined-scenario impact")
    ap_edge.add_argument("--json", help="write full per-TU detail to this file")
    ap_edge.add_argument("--sample", type=int, default=0,
                         help="print N random affected TUs for manual spot-checking")
    ap_audit = sub.add_parser("audit", help="rank all direct project includes of a hub")
    ap_audit.add_argument("hub")
    ap_why = sub.add_parser("why", help="explain how a TU reaches a header (real closure)")
    ap_why.add_argument("tu", help="TU source, e.g. runtime/exec_env.cpp")
    ap_why.add_argument("target", help="header to explain, e.g. gen_cpp/FrontendService_types.h")
    args = ap.parse_args()

    roots = [os.path.join(REPO_ROOT, "be", "src"),
             os.path.join(REPO_ROOT, "gensrc", "build")]
    extra_common = os.path.join(REPO_ROOT, "common")
    if os.path.isdir(extra_common):
        roots.append(extra_common)

    print("Parsing include graph …", file=sys.stderr)
    graph = IncludeGraph(roots)
    print(f"  {len(graph.files)} project files", file=sys.stderr)
    print("Loading real per-TU closures (ninja -t deps) …", file=sys.stderr)
    tus = load_tus(args.build_dir, graph)
    print(f"  {len(tus)} TUs with valid deps", file=sys.stderr)

    def must_resolve(name):
        p = graph.resolve(name, os.getcwd(), quoted=True)
        if p is None:
            sys.exit(f"error: cannot resolve '{name}' under {', '.join(roots)}")
        return p

    if args.mode == "edge":
        edges = []
        for h, c in [(args.hub, args.cut)] + args.extra:
            hub, cut = must_resolve(h), must_resolve(c)
            if cut not in graph.includes.get(hub, ()):
                sys.exit(f"error: {graph.display(hub)} has no direct include of "
                         f"{graph.display(cut)}")
            edges.append((hub, cut))
        res = analyze_edge(graph, tus, edges)
        print_edge_report(graph, res, sample=args.sample)
        if args.json:
            with open(args.json, "w") as f:
                json.dump(edge_json(graph, res), f, indent=1)
            print(f"\nfull detail written to {args.json}")
    elif args.mode == "why":
        tu, target = must_resolve(args.tu), must_resolve(args.target)
        closure = tus.get(tu)
        if closure is None:
            sys.exit(f"error: {graph.display(tu)} is not a compiled TU in this build")
        if target not in closure:
            print(f"{graph.display(tu)} does NOT include {graph.display(target)} in the real build.")
            return
        parent = {tu: None}
        queue = collections.deque([tu])
        while queue and target not in parent:
            node = queue.popleft()
            for nxt in graph.includes.get(node, ()):
                if nxt in closure and nxt not in parent:
                    parent[nxt] = node
                    queue.append(nxt)
        if target not in parent:
            print("in the real closure, but the parsed graph cannot trace a path "
                  "(conditional include the parser missed?)")
            return
        chain, node = [], target
        while node is not None:
            chain.append(node)
            node = parent[node]
        print("shortest include chain:")
        for i, node in enumerate(reversed(chain)):
            print(f"  {'  ' * i}{graph.display(node)}")
        parents_in_closure = sorted(
            p for p in closure
            if target in graph.includes.get(p, ()))
        print(f"\nall direct includers of {graph.display(target)} inside this TU's closure "
              f"({len(parents_in_closure)}):")
        for p in parents_in_closure:
            print(f"  {graph.display(p)}")
    else:  # audit
        hub = must_resolve(args.hub)
        targets = graph.includes.get(hub, [])
        print(f"== Audit: {graph.display(hub)} — {len(targets)} direct project include(s) ==")
        print(f"{'cut candidate':58s} {'cand':>5s} {'affect':>6s} {'seedF':>5s}  top lost header")
        for cut in targets:
            res = analyze_edge(graph, tus, [(hub, cut)])
            top = res["lost_spread"].most_common(1)
            top_s = f"{graph.display(top[0][0])}({top[0][1]})" if top else "-"
            print(f"{graph.display(cut):58s} {res['candidates']:5d} "
                  f"{len(res['affected']):6d} {len(res['seeds']):5d}  {top_s}")


if __name__ == "__main__":
    main()
