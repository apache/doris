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

"""Report generator for `build.sh --compile-bench` runs.

Generate a report for one run (done automatically at the end of a bench build,
can be re-run manually at any time):

    python3 report.py <run_dir> [--build-dir DIR] [--top N] [--build-status S]

Compare two runs (arguments are run dirs or summary.json paths):

    python3 report.py compare <old> <new>

Inputs inside <run_dir> (see bench-lib.sh):
    meta.tsv, phases.tsv, compile_log.jsonl, ninja_log.txt
Optional, from the build dir when COMPILE_BENCH_TRACE=ON was used:
    per-TU clang -ftime-trace JSON files (next to the object files)

Outputs inside <run_dir>: report.txt (human) and summary.json (machine).
"""

import argparse
import json
import os
import sys
from collections import defaultdict

TOP_DIRS = 25
TOP_HEADERS = 30
TOP_TEMPLATES = 20
TOP_TAIL = 10
WIDTH = 78


def section(title):
    text = "-- " + title + " "
    return text + "-" * max(0, WIDTH - len(text))


def fmt_dur(seconds):
    seconds = float(seconds)
    if seconds < 0:
        return "?"
    if seconds < 60:
        return "{:.1f}s".format(seconds)
    minutes = int(seconds // 60)
    if minutes < 60:
        return "{}m{:02d}s".format(minutes, int(seconds % 60))
    return "{}h{:02d}m".format(minutes // 60, minutes % 60)


def read_meta(run_dir):
    meta = {}
    path = os.path.join(run_dir, "meta.tsv")
    if os.path.isfile(path):
        with open(path) as fh:
            for line in fh:
                parts = line.rstrip("\n").split("\t", 1)
                if len(parts) == 2:
                    meta[parts[0]] = parts[1]
    return meta


def read_phases(run_dir):
    phases = []
    path = os.path.join(run_dir, "phases.tsv")
    if os.path.isfile(path):
        with open(path) as fh:
            for line in fh:
                parts = line.rstrip("\n").split("\t")
                if len(parts) != 3:
                    continue
                try:
                    start_ms, end_ms = int(parts[1]), int(parts[2])
                except ValueError:
                    continue
                phases.append(
                    {"name": parts[0], "dur_s": (end_ms - start_ms) / 1000.0}
                )
    return phases


def is_cmake_probe(record):
    for key in ("src", "out", "cwd"):
        value = record.get(key) or ""
        if "CMakeScratch" in value or "CMakeTmp" in value:
            return True
    return False


def read_compile_log(run_dir):
    records = []
    path = os.path.join(run_dir, "compile_log.jsonl")
    if os.path.isfile(path):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                if not is_cmake_probe(record):
                    records.append(record)
    return records


def read_ninja_log(run_dir, build_dir):
    """Return {output_path: (start_ms, end_ms)}, deduped keeping the last entry."""
    path = os.path.join(run_dir, "ninja_log.txt")
    if not os.path.isfile(path) and build_dir:
        path = os.path.join(build_dir, ".ninja_log")
    edges = {}
    if not os.path.isfile(path):
        return edges
    with open(path) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 4:
                continue
            try:
                start_ms, end_ms = int(parts[0]), int(parts[1])
            except ValueError:
                continue
            edges[parts[3]] = (start_ms, end_ms)
    return edges


def rel_path(path, meta):
    if not path:
        return "?"
    doris_home = meta.get("doris_home")
    if doris_home:
        home = doris_home.rstrip("/") + "/"
        if path.startswith(home):
            return path[len(home):]
    return path


def shorten_header(path, meta):
    doris_home = (meta.get("doris_home") or "").rstrip("/")
    if doris_home:
        if path.startswith(doris_home + "/thirdparty/installed/include/"):
            return "<thirdparty>/" + path[len(doris_home + "/thirdparty/installed/include/"):]
        if path.startswith(doris_home + "/"):
            return path[len(doris_home) + 1:]
    return path


def group_keys(rel_src):
    """Return (level1, level2) directory grouping keys for a doris_home-relative source."""
    rel_dir = os.path.dirname(rel_src)
    if rel_dir.startswith("be/src"):
        prefix, rest = "be/src", rel_dir[len("be/src"):].strip("/")
    else:
        prefix, rest = "", rel_dir
    parts = [p for p in rest.split("/") if p]
    level1 = "/".join([prefix] + parts[:1]) if prefix else ("/".join(parts[:1]) or ".")
    level2 = "/".join([prefix] + parts[:2]) if prefix else ("/".join(parts[:2]) or ".")
    return level1 or ".", level2 or "."


def load_time_traces(compiles, meta, build_dir):
    """Aggregate clang -ftime-trace JSONs written next to the object files.

    Durations reported by clang are microseconds. "Source" and template
    instantiation timings are inclusive of nested work, so sums across headers
    overlap; they rank hotspots, they are not additive wall time.
    """
    headers = defaultdict(lambda: [0.0, 0])       # path -> [total_s, count]
    templates = defaultdict(lambda: [0.0, 0])     # symbol -> [total_s, count]
    tu_split = {}                                 # src -> {total, frontend, backend}
    parsed = 0
    for record in compiles:
        out = record.get("out")
        if not out:
            continue
        base = os.path.join(record.get("cwd") or build_dir or "", out)
        trace_path = os.path.splitext(base)[0] + ".json"
        if not os.path.isfile(trace_path):
            continue
        try:
            with open(trace_path) as fh:
                events = json.load(fh).get("traceEvents", [])
        except (ValueError, OSError):
            continue
        parsed += 1
        maxima = defaultdict(float)
        # clang >= 20 emits "Source" as async begin/end pairs (ph "b"/"e") that
        # nest on one tid; older clang emits complete events with "dur".
        source_stacks = defaultdict(list)
        for event in events:
            dur_s = event.get("dur", 0) / 1e6
            name = event.get("name", "")
            detail = (event.get("args") or {}).get("detail", "")
            if name == "Source":
                phase = event.get("ph")
                if phase == "b":
                    source_stacks[event.get("tid")].append((detail, event.get("ts", 0)))
                    continue
                if phase == "e":
                    stack = source_stacks.get(event.get("tid"))
                    if not stack:
                        continue
                    detail, begin_ts = stack.pop()
                    dur_s = (event.get("ts", 0) - begin_ts) / 1e6
                if detail:
                    entry = headers[detail]
                    entry[0] += dur_s
                    entry[1] += 1
            elif name in ("InstantiateClass", "InstantiateFunction") and detail:
                entry = templates[detail]
                entry[0] += dur_s
                entry[1] += 1
            elif name in ("ExecuteCompiler", "Frontend", "Backend",
                          "Total Frontend", "Total Backend"):
                if dur_s > maxima[name]:
                    maxima[name] = dur_s
        tu_split[record.get("src") or out] = {
            "total_s": maxima["ExecuteCompiler"],
            "frontend_s": max(maxima["Frontend"], maxima["Total Frontend"]),
            "backend_s": max(maxima["Backend"], maxima["Total Backend"]),
        }
    return {
        "parsed": parsed,
        "headers": headers,
        "templates": templates,
        "tu_split": tu_split,
    }


def build_report(run_dir, build_dir, top_n, build_status):
    meta = read_meta(run_dir)
    if not build_dir:
        build_dir = meta.get("build_dir")
    phases = read_phases(run_dir)
    records = read_compile_log(run_dir)
    ninja_edges = read_ninja_log(run_dir, build_dir)

    compiles = [r for r in records if r.get("kind") in ("compile", "pch")]
    links = [r for r in records if r.get("kind") == "link"]
    failed = [r for r in records if r.get("rc", 0) != 0]

    lines = []
    out = lines.append
    out("=" * 78)
    out(" Doris BE compile benchmark report        run: {}".format(
        meta.get("run_id", os.path.basename(run_dir.rstrip("/")))))
    out("=" * 78)
    out(" build status : {}".format(build_status))
    for key in ("date_utc", "git_branch", "git_commit", "uname", "ncpu", "parallel",
                "build_type", "generator", "toolchain", "cxx_version", "enable_pch",
                "time_trace", "build_dir"):
        if key in meta:
            out(" {:<13}: {}".format(key, meta[key]))

    # ---- Phases -------------------------------------------------------------
    out("")
    out(section("Phases"))
    total_s = None
    for phase in phases:
        if phase["name"] == "total":
            total_s = phase["dur_s"]
    for phase in phases:
        if phase["name"] == "total":
            continue
        pct = " ({:5.1f}%)".format(100.0 * phase["dur_s"] / total_s) if total_s else ""
        out(" {:<24} {:>8}{}".format(phase["name"], fmt_dur(phase["dur_s"]), pct))
    if total_s is not None:
        out(" {:<24} {:>8}".format("total (wall)", fmt_dur(total_s)))

    build_phase_s = None
    for phase in phases:
        if phase["name"] == "build":
            build_phase_s = phase["dur_s"]

    # ---- Build summary ------------------------------------------------------
    out("")
    out(section("Build summary"))
    sum_wall = sum(r["wall_s"] for r in compiles)
    sum_cpu = sum(r["user_s"] + r["sys_s"] for r in compiles)
    out(" compile units (compile+pch)   : {}".format(len(compiles)))
    out(" sum of TU wall time           : {}".format(fmt_dur(sum_wall)))
    out(" sum of TU cpu time (user+sys) : {}".format(fmt_dur(sum_cpu)))
    if build_phase_s:
        out(" build phase wall              : {}".format(fmt_dur(build_phase_s)))
        edge_sum = sum_wall + sum(r["wall_s"] for r in links)
        out(" effective parallelism         : {:.1f}x  (sum TU+link wall / build wall)"
            .format(edge_sum / build_phase_s))
    if compiles:
        slowest = max(compiles, key=lambda r: r["wall_s"])
        out(" slowest single TU             : {}  ({})".format(
            fmt_dur(slowest["wall_s"]), rel_path(slowest.get("src"), meta)))
        hungriest = max(compiles, key=lambda r: r.get("maxrss_mb", 0))
        out(" largest TU peak rss           : {:.0f} MB  ({})".format(
            hungriest.get("maxrss_mb", 0), rel_path(hungriest.get("src"), meta)))
    for link in sorted(links, key=lambda r: r["wall_s"], reverse=True)[:5]:
        out(" link {:<24} : {}  peak rss {:.0f} MB".format(
            os.path.basename(link.get("out") or "?"),
            fmt_dur(link["wall_s"]), link.get("maxrss_mb", 0)))
    if failed:
        out(" FAILED commands               : {}".format(len(failed)))
        for record in failed[:10]:
            out("   rc={:<4} {}".format(
                record.get("rc"), rel_path(record.get("src") or record.get("out"), meta)))

    # ---- Top slow TUs -------------------------------------------------------
    out("")
    out(section("Top {} slowest translation units (wall)".format(top_n)))
    out(" {:>4}  {:>8} {:>8} {:>7} {:>9}  {}".format(
        "rank", "wall", "user", "sys", "maxrss", "file"))
    ranked = sorted(compiles, key=lambda r: r["wall_s"], reverse=True)
    for idx, record in enumerate(ranked[:top_n], 1):
        out(" {:>4}  {:>8} {:>8} {:>7} {:>7.0f}MB  {}{}".format(
            idx, fmt_dur(record["wall_s"]), fmt_dur(record["user_s"]),
            fmt_dur(record["sys_s"]), record.get("maxrss_mb", 0),
            rel_path(record.get("src"), meta),
            "  [pch]" if record.get("kind") == "pch" else ""))

    # ---- Directory rollup ---------------------------------------------------
    for level, title in ((0, "top-level directory"), (1, "second-level directory")):
        rollup = defaultdict(lambda: [0.0, 0])
        for record in compiles:
            rel = rel_path(record.get("src") or record.get("out") or "?", meta)
            key = group_keys(rel)[level]
            entry = rollup[key]
            entry[0] += record["wall_s"]
            entry[1] += 1
        out("")
        out(section("Wall time by {}".format(title)))
        out(" {:>9} {:>6} {:>8}  {}".format("wall-sum", "count", "avg", "directory"))
        ordered = sorted(rollup.items(), key=lambda kv: kv[1][0], reverse=True)
        for key, (wall, count) in ordered[:TOP_DIRS]:
            out(" {:>9} {:>6} {:>8}  {}".format(
                fmt_dur(wall), count, fmt_dur(wall / count), key))

    # ---- Ninja tail: what the build waits on at the end ---------------------
    if ninja_edges:
        out("")
        out(section("Last finishers (critical-path tail, from .ninja_log)"))
        out(" {:>10} {:>10} {:>8}  {}".format("start", "end", "dur", "output"))
        tail = sorted(ninja_edges.items(), key=lambda kv: kv[1][1], reverse=True)
        for output, (start_ms, end_ms) in tail[:TOP_TAIL]:
            out(" {:>10} {:>10} {:>8}  {}".format(
                fmt_dur(start_ms / 1000.0), fmt_dur(end_ms / 1000.0),
                fmt_dur((end_ms - start_ms) / 1000.0), output))

    # ---- Optional -ftime-trace analysis -------------------------------------
    trace = None
    if meta.get("time_trace") == "ON" and build_dir:
        trace = load_time_traces(compiles, meta, build_dir)
        out("")
        out(section("[-ftime-trace] parsed {} trace files".format(trace["parsed"])))
        if trace["parsed"]:
            out("")
            out("   Top headers by inclusive parse time (overlapping, ranks hotspots):")
            out(" {:>9} {:>7} {:>8}  {}".format("total", "count", "avg", "header"))
            for path, (total, count) in sorted(
                    trace["headers"].items(), key=lambda kv: kv[1][0],
                    reverse=True)[:TOP_HEADERS]:
                out(" {:>9} {:>7} {:>8}  {}".format(
                    fmt_dur(total), count, fmt_dur(total / count),
                    shorten_header(path, meta)))
            out("")
            out("   Top template instantiations (inclusive):")
            out(" {:>9} {:>7}  {}".format("total", "count", "symbol"))
            for symbol, (total, count) in sorted(
                    trace["templates"].items(), key=lambda kv: kv[1][0],
                    reverse=True)[:TOP_TEMPLATES]:
                out(" {:>9} {:>7}  {}".format(fmt_dur(total), count, symbol[:110]))
            out("")
            out("   Frontend (parse/instantiate) vs backend (codegen/opt) of slowest TUs:")
            out(" {:>9} {:>9} {:>9}  {}".format("total", "frontend", "backend", "file"))
            for record in ranked[:15]:
                split = trace["tu_split"].get(record.get("src") or "")
                if not split:
                    continue
                out(" {:>9} {:>9} {:>9}  {}".format(
                    fmt_dur(split["total_s"]), fmt_dur(split["frontend_s"]),
                    fmt_dur(split["backend_s"]), rel_path(record.get("src"), meta)))
        else:
            out("   (no trace files found under {} - was the build dir wiped?)"
                .format(build_dir))

    out("")
    out("=" * 78)

    summary = {
        "meta": meta,
        "build_status": build_status,
        "phases": {p["name"]: round(p["dur_s"], 1) for p in phases},
        "totals": {
            "compile_units": len(compiles),
            "sum_tu_wall_s": round(sum_wall, 1),
            "sum_tu_cpu_s": round(sum_cpu, 1),
            "build_phase_s": round(build_phase_s, 1) if build_phase_s else None,
        },
        "files": {
            rel_path(r.get("src") or r.get("out"), meta): {
                "wall_s": r["wall_s"],
                "user_s": r["user_s"],
                "maxrss_mb": r.get("maxrss_mb", 0),
                "kind": r.get("kind"),
            }
            for r in compiles + links
        },
    }
    if trace and trace["parsed"]:
        summary["headers_top"] = {
            shorten_header(path, meta): round(total, 1)
            for path, (total, _) in sorted(
                trace["headers"].items(), key=lambda kv: kv[1][0], reverse=True)[:100]
        }
        summary["templates_top"] = {
            symbol[:200]: round(total, 1)
            for symbol, (total, _) in sorted(
                trace["templates"].items(), key=lambda kv: kv[1][0], reverse=True)[:100]
        }
    return lines, summary


def cmd_report(args):
    run_dir = args.run_dir
    if not os.path.isdir(run_dir):
        print("ERROR: run dir not found: {}".format(run_dir), file=sys.stderr)
        return 1
    lines, summary = build_report(run_dir, args.build_dir, args.top, args.build_status)
    report_path = os.path.join(run_dir, "report.txt")
    with open(report_path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    with open(os.path.join(run_dir, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=1, sort_keys=True)
    print("\n".join(lines))
    print("Report written to {}".format(report_path))
    return 0


def load_summary(path):
    if os.path.isdir(path):
        path = os.path.join(path, "summary.json")
    with open(path) as fh:
        return json.load(fh)


def cmd_compare(args):
    old, new = load_summary(args.old), load_summary(args.new)
    print("=" * 78)
    print(" Compile benchmark comparison")
    print("   old: {} ({})".format(old["meta"].get("run_id"), old["meta"].get("git_commit")))
    print("   new: {} ({})".format(new["meta"].get("run_id"), new["meta"].get("git_commit")))
    print("=" * 78)

    print("")
    print(section("Phases"))
    print(" {:<24} {:>9} {:>9} {:>9}".format("phase", "old", "new", "delta"))
    for name in sorted(set(old["phases"]) | set(new["phases"])):
        old_s, new_s = old["phases"].get(name), new["phases"].get(name)
        delta = "" if old_s is None or new_s is None else "{:+.1f}s".format(new_s - old_s)
        print(" {:<24} {:>9} {:>9} {:>9}".format(
            name,
            fmt_dur(old_s) if old_s is not None else "-",
            fmt_dur(new_s) if new_s is not None else "-",
            delta))

    old_files, new_files = old.get("files", {}), new.get("files", {})
    both = set(old_files) & set(new_files)
    deltas = sorted(
        ((new_files[f]["wall_s"] - old_files[f]["wall_s"], f) for f in both),
        key=lambda pair: pair[0])
    threshold = 0.5
    improved = [d for d in deltas if d[0] < -threshold]
    regressed = [d for d in deltas if d[0] > threshold]

    print("")
    print(section("Per-file wall time changes (threshold {:.1f}s)".format(threshold)))
    print(" improved: {}   regressed: {}   only-in-old: {}   only-in-new: {}".format(
        len(improved), len(regressed),
        len(set(old_files) - both), len(set(new_files) - both)))
    for title, items in (("Top improvements", improved[:20]),
                         ("Top regressions", list(reversed(regressed[-20:])))):
        print("")
        print("   {}:".format(title))
        for delta, name in items:
            print("   {:>+8.1f}s  {:>8} -> {:<8} {}".format(
                delta, fmt_dur(old_files[name]["wall_s"]),
                fmt_dur(new_files[name]["wall_s"]), name))
    return 0


def main():
    argv = sys.argv[1:]
    if argv and argv[0] == "compare":
        parser = argparse.ArgumentParser(
            prog="report.py compare", description="compare two compile-bench runs")
        parser.add_argument("old", help="run dir or summary.json of the baseline")
        parser.add_argument("new", help="run dir or summary.json of the new run")
        return cmd_compare(parser.parse_args(argv[1:]))

    if argv and argv[0] == "report":
        argv = argv[1:]
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_dir", help="compile-bench run directory")
    parser.add_argument("--build-dir", default=None,
                        help="BE build dir (default: build_dir from meta.tsv)")
    parser.add_argument("--top", type=int, default=40,
                        help="how many slowest TUs to list (default 40)")
    parser.add_argument("--build-status", default="ok",
                        help="build outcome recorded in the report")
    return cmd_report(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
