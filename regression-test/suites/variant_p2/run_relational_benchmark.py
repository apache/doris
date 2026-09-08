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

"""Run the regression suite with unrestricted load and verified 8-core queries."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("load", "query"))
    parser.add_argument("--conf", required=True)
    parser.add_argument("--cpus", help="Eight comma-separated physical CPU IDs (query only)")
    parser.add_argument("--rows", type=int, default=44_273_863)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--keys", default="actor_login,actor_id")
    parser.add_argument("--stream-load", action="store_true",
                        help="Load public variant_p2 files over HTTP instead of authenticated S3")
    parser.add_argument("--resume-files", default="",
                        help="Comma-separated public variant_p2 files to append after an interrupted load")
    parser.add_argument("--spill", action="store_true", help="Separate forced-spill correctness/stability run")
    parser.add_argument("--output", required=True, help="New evidence directory")
    args = parser.parse_args()
    if args.phase == "query" and (args.stream_load or args.resume_files):
        parser.error("--stream-load and --resume-files apply only to the load phase")
    if args.resume_files and not args.stream_load:
        parser.error("--resume-files requires --stream-load")
    repo = Path(__file__).resolve().parents[3]
    evidence = Path(args.output).resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    processes = {}
    original = {}
    for name in ("be", "fe"):
        pid = int((repo / f"output/{name}/bin/{name}.pid").read_text())
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
        if str(repo) not in command:
            raise RuntimeError(f"Refusing to change unrelated {name} PID {pid}")
        processes[name] = pid
        original[pid] = {int(t.name): os.sched_getaffinity(int(t.name))
                         for t in Path(f"/proc/{pid}/task").iterdir()}
    cpus = None
    if args.phase == "query":
        cpus = {int(cpu) for cpu in (args.cpus or "").split(",") if cpu}
        topology = {tuple((Path(f"/sys/devices/system/cpu/cpu{cpu}/topology") / item)
                          .read_text().strip() for item in ("physical_package_id", "core_id"))
                    for cpu in cpus}
        if len(cpus) != 8 or len(topology) != 8:
            raise ValueError("Select exactly eight distinct physical cores")
        cache = (repo / "be/build_RELEASE/CMakeCache.txt").read_text()
        if "CMAKE_BUILD_TYPE:STRING=RELEASE" not in cache.upper():
            raise RuntimeError("A Release build is required for performance measurements")
        binary = repo / "output/be/lib/doris_be"
        running = Path(f"/proc/{processes['be']}/exe")
        if not os.path.samefile(binary, running):
            raise RuntimeError("Running BE differs from the worktree output binary")
        release_binary = repo / "be/build_RELEASE/src/service/doris_be"
        with release_binary.open("rb") as built, binary.open("rb") as installed:
            if hashlib.file_digest(built, "sha256").digest() != hashlib.file_digest(installed, "sha256").digest():
                raise RuntimeError("Installed BE does not match the Release build")
    elif args.cpus:
        raise ValueError("Do not bind CPUs during ingestion")
    else:
        for pid in processes.values():
            if len(os.sched_getaffinity(pid)) <= 8:
                raise RuntimeError("Load requires each FE/BE process to be unrestricted beyond eight CPUs")
    manifest = dict(vars(args), processes=processes, checkout=str(repo),
                    head=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
                    started=time.time(), original_affinity={str(p): {str(t): sorted(m) for t, m in ts.items()}
                                                           for p, ts in original.items()})
    manifest["harness_sha256"] = {
        name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
        for name in ("load.groovy", "relational_performance.groovy", Path(__file__).name)
    }
    if cpus:
        with (repo / "output/be/lib/doris_be").open("rb") as binary:
            manifest["be_sha256"] = hashlib.file_digest(binary, "sha256").hexdigest()
    (evidence / "manifest.json").write_text(json.dumps(manifest, indent=2))
    environment = dict(os.environ, VARIANT_BENCH_PHASE=args.phase,
                       VARIANT_BENCH_ROWS=str(args.rows), VARIANT_BENCH_REPEATS=str(args.repeats),
                       VARIANT_BENCH_WARMUPS=str(args.warmups),
                       VARIANT_BENCH_KEYS=args.keys,
                       VARIANT_BENCH_RESULTS=str(evidence / "samples.jsonl"),
                       VARIANT_BENCH_SPILL=str(args.spill).lower(),
                       VARIANT_P2_USE_STREAM_LOAD=str(args.stream_load).lower(),
                       VARIANT_P2_RESUME_FILES=args.resume_files,
                       VARIANT_BENCH_CPUS=args.cpus or "unrestricted")
    for key in list(environment):
        if key.lower() in ("http_proxy", "https_proxy", "all_proxy"):
            del environment[key]
    environment["NO_PROXY"] = environment["no_proxy"] = "127.0.0.1,localhost"
    child = None
    try:
        if cpus:
            # Re-enumerate until every thread has the mask. New threads inherit
            # the creator's mask; the monitor below rejects any subsequent drift.
            for pid in processes.values():
                for _ in range(10):
                    tids = [int(t.name) for t in Path(f"/proc/{pid}/task").iterdir()]
                    for tid in tids:
                        try:
                            os.sched_setaffinity(tid, cpus)
                        except ProcessLookupError:
                            pass
                    if all(os.sched_getaffinity(int(t.name)) == cpus
                           for t in Path(f"/proc/{pid}/task").iterdir()):
                        break
                else:
                    raise RuntimeError("Could not establish CPU affinity")
        suites = (["load_p2", "variant_relational_performance"]
                  if args.phase == "load" else ["variant_relational_performance"])
        with (evidence / "host-load.jsonl").open("w") as host:
            for suite in suites:
                suite_environment = environment.copy()
                if args.phase == "load" and suite == "variant_relational_performance":
                    suite_environment["VARIANT_BENCH_PHASE"] = "prepare"
                log_name = ({"load_p2": "load.log"}.get(suite, "prepare.log")
                            if args.phase == "load" else "regression.log")
                command = [str(repo / "run-regression-test.sh"), "--conf", args.conf, "--run",
                           "-d", "variant_p2", "-s", suite]
                with (evidence / log_name).open("w") as log:
                    child = subprocess.Popen(command, cwd=repo, env=suite_environment, stdout=log,
                                             stderr=subprocess.STDOUT, start_new_session=True)
                    while child.poll() is None:
                        host.write(json.dumps({"time": time.time(),
                                              "loadavg": Path("/proc/loadavg").read_text().strip()}) + "\n")
                        host.flush()
                        if cpus:
                            for pid in processes.values():
                                for thread in Path(f"/proc/{pid}/task").iterdir():
                                    try:
                                        if os.sched_getaffinity(int(thread.name)) != cpus:
                                            raise RuntimeError(f"CPU affinity drift: {thread}")
                                    except ProcessLookupError:
                                        pass
                        time.sleep(1)
                if child.returncode:
                    raise RuntimeError(f"Regression failed: inspect {evidence / log_name}")
                child = None
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            child.wait()
        if cpus:
            for pid, threads in original.items():
                for _ in range(10):
                    for thread in Path(f"/proc/{pid}/task").iterdir():
                        try:
                            os.sched_setaffinity(int(thread.name), threads.get(int(thread.name), threads[pid]))
                        except ProcessLookupError:
                            pass
                    if all(os.sched_getaffinity(int(thread.name)) ==
                           threads.get(int(thread.name), threads[pid])
                           for thread in Path(f"/proc/{pid}/task").iterdir()):
                        break
                else:
                    raise RuntimeError("Could not restore CPU affinity")
    if args.phase == "query":
        events = [json.loads(line) for line in (evidence / "samples.jsonl").read_text().splitlines()]
        if not events or events[-1]["event"] != "complete":
            raise RuntimeError("Missing completion record")
        groups = {}
        for event in events:
            if event["event"] == "sample" and event["round"] >= 0:
                groups.setdefault((event["key"], event["operation"], event["mode"]), []).append(event["ms"])
        summary = [{"key": key, "operation": operation, "mode": mode, "samples_ms": values,
                    "median_ms": statistics.median(values), "min_ms": min(values), "max_ms": max(values)}
                   for (key, operation, mode), values in groups.items()]
        (evidence / "summary.json").write_text(json.dumps(summary, indent=2))
    print(evidence)


if __name__ == "__main__":
    main()
