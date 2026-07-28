#!/usr/bin/env python
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

"""Run the Parquet smoke matrix and same-agent ABBA performance gate."""

from __future__ import print_function

import argparse
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile


try:
    string_types = (basestring,)
    text_type = unicode
except NameError:
    string_types = (str,)
    text_type = str


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--doris-home", required=True)
    parser.add_argument("--head-binary", required=True)
    parser.add_argument("--base-binary", required=True)
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--comparator", required=True)
    return parser.parse_args()


def load_json(path):
    with io.open(path, encoding="utf-8") as stream:
        return json.load(stream)


def write_json(path, document):
    with io.open(path, "w", encoding="utf-8") as stream:
        stream.write(text_type(json.dumps(document, indent=2, sort_keys=True)))
        stream.write(u"\n")


def find_executable(name):
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        candidate = os.path.join(directory, name)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def find_libjvm(search_root):
    for root, _, files in os.walk(search_root):
        if "libjvm.so" in files:
            return os.path.join(root, "libjvm.so")
    return None


def command_text(command):
    return " ".join(command)


def run_command(command, environment, stdout_path=None):
    print("INFO: run {0}".format(command_text(command)))
    sys.stdout.flush()
    if stdout_path is None:
        subprocess.check_call(command, env=environment)
        return
    with open(stdout_path, "w") as output_stream:
        subprocess.check_call(command, env=environment, stdout=output_stream)


def environment_value(name, default, converter):
    value = os.environ.get(name)
    return converter(value) if value not in (None, "") else converter(default)


def require_policy(policy):
    if policy.get("schema_version") != 1:
        raise ValueError("unsupported policy schema version")
    minimum_counts = policy["minimum_case_counts"]
    measurement = policy["measurement"]
    thresholds = policy["thresholds"]
    gate_cases = policy["gate_cases"]
    for group in ("ParquetDecoder", "ParquetReader"):
        if int(minimum_counts[group]) <= 0:
            raise ValueError("invalid minimum case count for {0}".format(group))
    if not isinstance(gate_cases, list) or not gate_cases:
        raise ValueError("performance gate case list is empty")
    if len(set(gate_cases)) != len(gate_cases):
        raise ValueError("performance gate case list contains duplicates")
    for case_name in gate_cases:
        if not isinstance(case_name, string_types):
            raise ValueError("performance gate case name is not a string")
    if int(measurement["repetitions"]) <= 1:
        raise ValueError("measurement repetitions must be greater than one")
    if int(measurement["max_inconclusive_retries"]) < 0:
        raise ValueError("invalid inconclusive retry count")
    if float(thresholds["regression_pct"]) <= float(thresholds["warning_pct"]):
        raise ValueError("regression threshold must exceed warning threshold")


def read_case_list(path):
    with io.open(path, encoding="utf-8") as stream:
        return [line.strip() for line in stream if line.strip()]


def validate_smoke(path, prefix, expected_count):
    payload = load_json(path)
    benchmarks = payload.get("benchmarks") if isinstance(payload, dict) else None
    if not isinstance(benchmarks, list):
        raise ValueError("benchmark JSON has no benchmark list: {0}".format(path))
    matched = [
        benchmark
        for benchmark in benchmarks
        if isinstance(benchmark, dict)
        and isinstance(benchmark.get("name"), string_types)
        and benchmark["name"].startswith(prefix)
    ]
    if len(matched) != expected_count:
        raise ValueError(
            "unexpected smoke result count for {0}: expected={1}, actual={2}".format(
                prefix, expected_count, len(matched)
            )
        )
    failed = [
        benchmark["name"]
        for benchmark in matched
        if benchmark.get("error_occurred", False) is not False
    ]
    if failed:
        raise ValueError(
            "benchmark JSON reports failures for {0}: {1}".format(
                prefix, ", ".join(failed)
            )
        )


def compare(
    python,
    comparator,
    result_dir,
    prefix,
    thresholds,
    environment,
):
    command = [
        python,
        comparator,
        "--base-a1",
        os.path.join(result_dir, prefix + "base-a1.json"),
        "--head-b1",
        os.path.join(result_dir, prefix + "head-b1.json"),
        "--head-b2",
        os.path.join(result_dir, prefix + "head-b2.json"),
        "--base-a2",
        os.path.join(result_dir, prefix + "base-a2.json"),
        "--output-json",
        os.path.join(result_dir, "comparison.json"),
        "--output-markdown",
        os.path.join(result_dir, "comparison.md"),
        "--regression-threshold-pct",
        str(thresholds["regression_pct"]),
        "--warning-threshold-pct",
        str(thresholds["warning_pct"]),
        "--confidence-margin-pct",
        str(thresholds["confidence_margin_pct"]),
        "--max-cv-pct",
        str(thresholds["max_cv_pct"]),
    ]
    print("INFO: run {0}".format(command_text(command)))
    sys.stdout.flush()
    return subprocess.call(command, env=environment)


def main():
    args = parse_args()
    policy = load_json(args.policy)
    require_policy(policy)

    for binary in (args.head_binary, args.base_binary):
        if not os.path.isfile(binary) or not os.access(binary, os.X_OK):
            raise ValueError("Parquet benchmark binary not found: {0}".format(binary))
    if not os.path.isdir(args.result_dir):
        os.makedirs(args.result_dir)

    taskset = find_executable("taskset")
    if taskset is None:
        raise ValueError("taskset is required for the performance gate")
    search_root = os.environ.get("JAVA_HOME", "/usr/lib/jvm")
    jvm_library = find_libjvm(search_root)
    if jvm_library is None:
        raise ValueError("libjvm.so not found under {0}".format(search_root))

    environment = os.environ.copy()
    jvm_directory = os.path.dirname(jvm_library)
    old_library_path = environment.get("LD_LIBRARY_PATH")
    environment["LD_LIBRARY_PATH"] = (
        jvm_directory
        if not old_library_path
        else jvm_directory + os.pathsep + old_library_path
    )
    environment["DORIS_HOME"] = args.doris_home
    fixture_root = tempfile.mkdtemp(prefix="tmp.", dir=args.result_dir)
    environment["TMPDIR"] = fixture_root

    minimum_counts = policy["minimum_case_counts"]
    measurement = policy["measurement"]
    thresholds = dict(policy["thresholds"])
    cpu = environment_value(
        "PARQUET_BENCHMARK_CPU", measurement["cpu"], int
    )
    min_time = environment_value(
        "PARQUET_BENCHMARK_MIN_TIME", measurement["min_time"], str
    )
    warmup_time = environment_value(
        "PARQUET_BENCHMARK_WARMUP_TIME", measurement["warmup_time"], str
    )
    repetitions = int(measurement["repetitions"])
    retries = int(measurement["max_inconclusive_retries"])
    thresholds["regression_pct"] = environment_value(
        "PARQUET_REGRESSION_THRESHOLD_PCT", thresholds["regression_pct"], float
    )
    thresholds["warning_pct"] = environment_value(
        "PARQUET_WARNING_THRESHOLD_PCT", thresholds["warning_pct"], float
    )
    thresholds["confidence_margin_pct"] = environment_value(
        "PARQUET_CONFIDENCE_MARGIN_PCT",
        thresholds["confidence_margin_pct"],
        float,
    )
    thresholds["max_cv_pct"] = environment_value(
        "PARQUET_MAX_CV_PCT", thresholds["max_cv_pct"], float
    )
    if cpu < 0:
        raise ValueError("benchmark CPU must be non-negative")
    subprocess.check_call([taskset, "-c", str(cpu), "true"], env=environment)

    case_list_path = os.path.join(args.result_dir, "cases.txt")
    base_case_list_path = os.path.join(args.result_dir, "base-cases.txt")
    try:
        run_command(
            [args.head_binary, "--benchmark_list_tests"],
            environment,
            case_list_path,
        )
        cases = read_case_list(case_list_path)
        counts = {
            group: sum(case.startswith(group + "/") for case in cases)
            for group in ("ParquetDecoder", "ParquetReader")
        }
        for group, minimum in minimum_counts.items():
            if counts.get(group, 0) < int(minimum):
                raise ValueError(
                    "unexpected Parquet benchmark matrix: "
                    "decoder={0}, reader={1}".format(
                        counts["ParquetDecoder"], counts["ParquetReader"]
                    )
                )

        for group in ("ParquetDecoder", "ParquetReader"):
            output_path = os.path.join(
                args.result_dir,
                "{0}-smoke.json".format(
                    "decoder" if group == "ParquetDecoder" else "reader"
                ),
            )
            run_command(
                [
                    args.head_binary,
                    "--benchmark_filter=^{0}/".format(group),
                    "--benchmark_min_time={0}".format(policy["smoke_min_time"]),
                    "--benchmark_out={0}".format(output_path),
                    "--benchmark_out_format=json",
                ],
                environment,
            )
            validate_smoke(output_path, group + "/", counts[group])
        print(
            "INFO: Parquet microbenchmark smoke passed: "
            "{0} decoder cases, {1} reader cases".format(
                counts["ParquetDecoder"], counts["ParquetReader"]
            )
        )

        run_command(
            [args.base_binary, "--benchmark_list_tests"],
            environment,
            base_case_list_path,
        )
        base_cases = set(read_case_list(base_case_list_path))
        head_cases = set(cases)
        for case_name in policy["gate_cases"]:
            if case_name not in head_cases or case_name not in base_cases:
                raise ValueError(
                    "performance gate case missing from base or PR: {0}".format(
                        case_name
                    )
                )

        gate_filter = "|".join(
            "^{0}$".format(re.escape(case_name))
            for case_name in policy["gate_cases"]
        )

        def run_phase(phase, binary):
            run_command(
                [
                    taskset,
                    "-c",
                    str(cpu),
                    binary,
                    "--benchmark_filter={0}".format(gate_filter),
                    "--benchmark_min_time={0}".format(min_time),
                    "--benchmark_min_warmup_time={0}".format(warmup_time),
                    "--benchmark_repetitions={0}".format(repetitions),
                    "--benchmark_out={0}".format(
                        os.path.join(args.result_dir, phase + ".json")
                    ),
                    "--benchmark_out_format=json",
                ],
                environment,
            )

        def run_abba(prefix):
            run_phase(prefix + "base-a1", args.base_binary)
            run_phase(prefix + "head-b1", args.head_binary)
            run_phase(prefix + "head-b2", args.head_binary)
            run_phase(prefix + "base-a2", args.base_binary)

        run_abba("")
        comparison_status = compare(
            sys.executable,
            args.comparator,
            args.result_dir,
            "",
            thresholds,
            environment,
        )
        attempt = 0
        while comparison_status == 3 and attempt < retries:
            attempt += 1
            print(
                "WARN: suspicious regression is noisy; retry the complete "
                "ABBA measurement ({0}/{1})".format(attempt, retries)
            )
            shutil.move(
                os.path.join(args.result_dir, "comparison.json"),
                os.path.join(
                    args.result_dir,
                    "comparison-attempt-{0}.json".format(attempt),
                ),
            )
            shutil.move(
                os.path.join(args.result_dir, "comparison.md"),
                os.path.join(
                    args.result_dir,
                    "comparison-attempt-{0}.md".format(attempt),
                ),
            )
            prefix = "retry-{0}-".format(attempt)
            run_abba(prefix)
            comparison_status = compare(
                sys.executable,
                args.comparator,
                args.result_dir,
                prefix,
                thresholds,
                environment,
            )

        if comparison_status == 3:
            print("ERROR: performance comparison remained inconclusive after retry")
            return 3
        if comparison_status != 0:
            return comparison_status
        print("INFO: Parquet microbenchmark performance gate passed")
        return 0
    finally:
        shutil.rmtree(fixture_root)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as error:
        print(
            "ERROR: command failed with exit code {0}: {1}".format(
                error.returncode, command_text(error.cmd)
            ),
            file=sys.stderr,
        )
        sys.exit(2)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print("ERROR: {0}".format(error), file=sys.stderr)
        sys.exit(2)
