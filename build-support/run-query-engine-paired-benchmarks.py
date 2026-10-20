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

"""Run saved BE benchmark binaries through run-be-ut.sh, alternating each sample.

Set DORIS_BE_UT_EXECUTABLE to this script and provide QUERY_ENGINE_BENCH_BASELINE,
QUERY_ENGINE_BENCH_CANDIDATE, QUERY_ENGINE_BENCH_CPU and QUERY_ENGINE_BENCH_OUTPUT.
The runner supplies the runtime environment and forwards its gtest arguments.
"""

import csv
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import select
import statistics
import subprocess
import sys

READY = "QUERY_ENGINE_READY"
RESULT = "QUERY_ENGINE_BENCH"


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class Worker:
    def __init__(self, name, binary, cpu, output):
        self.name = name
        self.binary = binary
        self.sha256 = file_hash(binary)
        self.pending = b""
        self.peak_rss_kib = 0
        self.log = (output / f"{name}.log").open("w")
        slot = "a" if name == "baseline" else "b"
        runtime = output / slot
        (runtime / "ut_dir").mkdir(parents=True, exist_ok=True)
        read_fd, self.control_fd = os.pipe()
        env = dict(os.environ, QUERY_ENGINE_BENCH_CONTROL_FD=str(read_fd))
        arguments = [
            argument for argument in sys.argv[1:] if not argument.startswith("--gtest_output=")
        ]
        arguments.append(f"--gtest_output=xml:{output / f'{slot}.xml'}")
        affinity = os.sched_getaffinity(0)
        try:
            os.sched_setaffinity(0, {int(cpu)})
            self.process = subprocess.Popen(
                ["doris_be_test", *arguments],
                executable=str(binary),
                cwd=runtime,
                env=env,
                pass_fds=(read_fd,),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        except BaseException:
            os.close(self.control_fd)
            self.log.close()
            raise
        finally:
            os.sched_setaffinity(0, affinity)
            os.close(read_fd)

    def next_line(self):
        while b"\n" not in self.pending:
            readable, _, _ = select.select([self.process.stdout], [], [], 60)
            if not readable:
                raise TimeoutError(f"{self.name}: no benchmark output for 60 seconds")
            chunk = os.read(self.process.stdout.fileno(), 65536)
            if not chunk:
                if not self.pending:
                    return None
                self.pending += b"\n"
                break
            self.pending += chunk
        raw, self.pending = self.pending.split(b"\n", 1)
        line = raw.decode("utf-8", errors="replace")
        self.log.write(line + "\n")
        self.log.flush()
        return line

    def event(self, expected):
        while (line := self.next_line()) is not None:
            prefix = line.split(",", 1)[0]
            if prefix not in (READY, RESULT):
                continue
            if prefix != expected:
                raise RuntimeError(f"{self.name}: expected {expected}, received {line}")
            return line.split(",")[1:]
        return None

    def measure(self):
        os.write(self.control_fd, b"r")
        result = self.event(RESULT)
        if result is None or len(result) != 5:
            raise RuntimeError(f"{self.name}: missing benchmark sample")
        try:
            status = Path(f"/proc/{self.process.pid}/status").read_text()
        except FileNotFoundError:
            status = ""
        for line in status.splitlines():
            if line.startswith("VmHWM:"):
                self.peak_rss_kib = max(self.peak_rss_kib, int(line.split()[1]))
                break
        return result

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        os.close(self.control_fd)
        self.log.close()


def summarize(samples):
    baseline = [row[0] for row in samples]
    candidate = [row[1] for row in samples]
    ratios = [row[1] / row[0] for row in samples]
    # Pair AB and BA samples so execution order has equal weight.
    rounds = [
        math.sqrt(ratios[index] * ratios[index + 1]) for index in range(0, len(ratios) - 1, 2)
    ]
    return {
        "samples": len(samples),
        "baseline_median_ns": statistics.median(baseline),
        "candidate_median_ns": statistics.median(candidate),
        "paired_ratio_median": statistics.median(ratios),
        "paired_ratio_min": min(ratios),
        "paired_ratio_max": max(ratios),
        "balanced_rounds": len(rounds),
        "unpaired_samples": len(ratios) % 2,
        "balanced_ratio_median": statistics.median(rounds) if rounds else None,
    }


def run(workers, output, cpu):
    cases = {}
    pair_number = 0
    previous_label = None
    with (output / "pairs.csv").open("w", newline="") as destination:
        writer = csv.writer(destination)
        writer.writerow(
            ("case", "sample", "iterations", "baseline_ns", "candidate_ns", "checksum", "order")
        )
        while True:
            ready = [worker.event(READY) for worker in workers]
            if ready == [None, None]:
                break
            if ready[0] != ready[1] or ready[0] is None or len(ready[0]) != 2:
                raise RuntimeError(f"Benchmark cases differ: {ready}")
            label, sample = ready[0]
            if previous_label is not None and label != previous_label:
                print(previous_label, json.dumps(summarize(cases[previous_label])), flush=True)
            previous_label = label
            results = [None, None]
            order = (0, 1) if pair_number % 2 == 0 else (1, 0)
            for index in order:
                results[index] = workers[index].measure()
            left, right = results
            if left[:3] != right[:3] or left[:2] != ready[0]:
                raise RuntimeError(f"Benchmark sample shapes differ: {results}")
            if Decimal(left[4]) != Decimal(right[4]):
                raise RuntimeError(f"Benchmark checksums differ: {results}")
            iterations = int(left[2])
            elapsed = [int(left[3]), int(right[3])]
            if iterations <= 0 or min(elapsed) <= 0:
                raise RuntimeError(f"Invalid benchmark work or timing: {results}")
            cases.setdefault(label, []).append(tuple(value / iterations for value in elapsed))
            writer.writerow(
                (label, sample, iterations, *elapsed, left[4], "AB" if order[0] == 0 else "BA")
            )
            destination.flush()
            pair_number += 1
    for worker in workers:
        if worker.process.wait(timeout=10) != 0:
            raise RuntimeError(f"{worker.name} benchmark tests failed; see {worker.name}.log")
    if not cases:
        raise RuntimeError("No paired benchmark samples were produced")
    print(previous_label, json.dumps(summarize(cases[previous_label])), flush=True)
    summary = {
        "cpu": cpu,
        "pairs": pair_number,
        "artifacts": {
            worker.name: {
                "path": str(worker.binary),
                "sha256": worker.sha256,
                "process_peak_rss_kib": worker.peak_rss_kib,
            }
            for worker in workers
        },
        "cases": {label: summarize(samples) for label, samples in cases.items()},
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Paired benchmark completed: {pair_number} pairs, {len(cases)} cases", flush=True)


def main():
    output = Path(os.environ["QUERY_ENGINE_BENCH_OUTPUT"]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    cpu = os.environ["QUERY_ENGINE_BENCH_CPU"]
    workers = []
    try:
        for name in ("baseline", "candidate"):
            binary = Path(os.environ[f"QUERY_ENGINE_BENCH_{name.upper()}"]).resolve(strict=True)
            workers.append(Worker(name, binary, cpu, output))
        run(workers, output, cpu)
    finally:
        for worker in workers:
            worker.close()


if __name__ == "__main__":
    main()
