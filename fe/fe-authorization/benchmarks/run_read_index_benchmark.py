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

"""Compare native, scope-only, SR-style calls and indexed Boolean checks serially."""

import argparse
import json
import os
from pathlib import Path
import subprocess

import run_benchmark


def dependency_manifest(part):
    path = Path(part)
    if path.is_file():
        return {"path": part, "kind": "file", "sha256": run_benchmark.digest(path)}
    classes = [{"path": str(entry.relative_to(path)), "sha256": run_benchmark.digest(entry)}
               for entry in sorted(path.rglob("*.class"))]
    return {"path": part, "kind": "directory", "classFiles": classes}


def run_case(args, destination, runtime, dependencies, users, variant):
    name = f"{users}-{variant}"
    adapter = ""
    if variant == "scope":
        adapter = run_benchmark.ADAPTER
    if variant == "indexed":
        adapter = "org.apache.doris.catalog.authorizer.ranger.doris.DorisIndexedReadBenchmarkAdapter"
    config = {
        "users": users, "tables": args.tables, "tablesPerUser": args.tables_per_user,
        "columns": 3, "warmups": args.warmups, "samples": args.samples, "seed": 83,
        "mode": "sr-columns" if variant == "sr" else "cascade",
        "requirement": "read", "refresh": True, "controllerVerdictsOnly": True, "bootstrapAdminPolicy": True,
        "adapter": adapter, "serviceDef": str(args.service_def.resolve()),
    }
    config_path = destination / f"{name}-config.json"
    result_path = destination / f"{name}-result.json"
    config_path.write_text(json.dumps(config, indent=2) + "\n")
    command = ["java", "-Xms256m", f"-Xmx{args.heap}", "-XX:+UseG1GC",
               f"-Dlog4j2.configurationFile={destination / 'runtime/log4j2.xml'}",
               f"-Xlog:gc:file={destination / (name + '-gc.log')}:time,uptime,level,tags",
               "-cp", runtime + os.pathsep + dependencies, run_benchmark.MAIN,
               str(config_path), str(result_path)]
    print(f"START users={users} variant={variant}", flush=True)
    with (destination / f"{name}.log").open("w") as log:
        completed = subprocess.run(command, cwd=run_benchmark.ROOT, stdout=log, stderr=subprocess.STDOUT)
    if completed.returncode:
        raise RuntimeError(f"{name} failed with exit {completed.returncode}; see {destination / (name + '.log')}")
    result = json.loads(result_path.read_text())
    print(f"DONE users={users} variant={variant} p50Ms={result['p50Ms']:.6f} "
          f"retainedMiB={result['retainedPolicyMiB']:.2f} nativeCalls={result['nativeEngineCalls']}", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classpath-file", type=Path, required=True)
    parser.add_argument("--service-def", type=Path, required=True)
    parser.add_argument("--baseline-runtime", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--users", type=int, nargs="+", default=[260, 500, 3000])
    parser.add_argument("--variants", nargs="+", choices=["stock", "scope", "sr", "indexed"],
                        default=["stock", "scope", "sr", "indexed"])
    parser.add_argument("--tables", type=int, default=5000)
    parser.add_argument("--tables-per-user", type=int, default=800)
    parser.add_argument("--warmups", type=int, default=20)
    parser.add_argument("--samples", type=int, default=120)
    parser.add_argument("--heap", default="12g")
    args = parser.parse_args()
    destination = args.destination.resolve()
    if destination.exists():
        parser.error("destination must be new to preserve frozen evidence")
    destination.mkdir(parents=True)
    dependencies = run_benchmark.read_classpath(args.classpath_file)
    current = run_benchmark.stage(destination, dependencies)
    classes = destination / "runtime/classes"
    baseline_jars = sorted(args.baseline_runtime.resolve().glob("*.jar"))
    if len(baseline_jars) != len(run_benchmark.MODULES):
        parser.error("baseline-runtime must contain the five frozen Doris plugin/API jars")
    baseline = os.pathsep.join(str(path) for path in [classes, *baseline_jars])
    manifest = {
        "baseline": [{"path": str(path), "sha256": run_benchmark.digest(path)} for path in baseline_jars],
        "dependencies": [dependency_manifest(part) for part in dependencies.split(os.pathsep)],
        "comparison": "controller Boolean verdicts; SR is only its SELECT-column call pattern",
    }
    (destination / "comparison-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (destination / "runtime/log4j2.xml").write_text(
        '<Configuration status="ERROR"><Appenders><Console name="console" target="SYSTEM_ERR">'
        '<PatternLayout pattern="%level %logger: %msg%n"/></Console></Appenders>'
        '<Loggers><Root level="error"><AppenderRef ref="console"/></Root></Loggers></Configuration>\n')
    comparisons = []
    for users in args.users:
        results = {}
        for variant in args.variants:
            runtime = current if variant == "indexed" else baseline
            results[variant] = run_case(args, destination, runtime, dependencies, users, variant)
            fingerprints = {entry["requestVerdictFingerprint"] for entry in results.values()}
            if len(fingerprints) != 1:
                raise RuntimeError(f"Final request verdict mismatch for users={users}")
            (destination / "comparisons.json").write_text(
                json.dumps([*comparisons, {"users": users, "results": results}], indent=2) + "\n")
        comparisons.append({"users": users, "results": results})


if __name__ == "__main__":
    main()
