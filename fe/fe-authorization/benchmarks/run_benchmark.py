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

"""Freeze packaged plugins and run stock/optimized engines in separate JVMs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
MODULES = [
    "fe/fe-extension-spi",
    "fe/fe-authorization/fe-authorization-api",
    "fe/fe-authorization/fe-authorization-spi",
    "fe/fe-authorization/fe-authorization-plugins/fe-authorization-plugin-ranger-common",
    "fe/fe-authorization/fe-authorization-plugins/fe-authorization-plugin-ranger-doris",
]
MAIN = "org.apache.doris.catalog.authorizer.ranger.doris.V2AuthorizationBenchmark"
ADAPTER = "org.apache.doris.catalog.authorizer.ranger.doris.DorisScopeBenchmarkAdapter"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_classpath(path):
    # Resolve against the invocation directory before changing subprocess cwd.
    return os.pathsep.join(str(Path(part).resolve()) for part in path.read_text().strip().split(os.pathsep))


def stage(destination, dependencies):
    runtime = destination / "runtime"
    runtime.mkdir()
    jars = []
    manifest = []
    for module in MODULES:
        target = ROOT / module / "target"
        candidates = [p for p in target.glob("*.jar") if not any(
            excluded in p.name for excluded in ["sources", "tests", "javadoc", "original-"])]
        if len(candidates) != 1:
            raise RuntimeError(f"Package {module} first; expected one jar, found {candidates}")
        original = candidates[0]
        with zipfile.ZipFile(original) as archive:
            for source in (ROOT / module / "src/main/java").rglob("*.java"):
                name = str(source.relative_to(ROOT / module / "src/main/java")).replace(".java", ".class")
                compiled = target / "classes" / name
                if source.stat().st_mtime > compiled.stat().st_mtime or archive.read(name) != compiled.read_bytes():
                    raise RuntimeError(f"Stale packaged class: {source}; package the module again")
        frozen = runtime / original.name
        shutil.copyfile(original, frozen)
        jars.append(frozen)
        manifest.append({"artifact": module, "sha256": digest(frozen)})
    sources = []
    for name in ["V2AuthorizationBenchmark.java", "DorisScopeBenchmarkAdapter.java",
                 "DorisIndexedReadBenchmarkAdapter.java"]:
        source = HERE / "src/org/apache/doris/catalog/authorizer/ranger/doris" / name
        frozen = runtime / name
        shutil.copyfile(source, frozen)
        sources.append(frozen)
        manifest.append({"source": name, "sha256": digest(source)})
    classes = runtime / "classes"
    classes.mkdir()
    classpath = os.pathsep.join(str(p) for p in jars) + os.pathsep + dependencies
    subprocess.run(["javac", "-proc:none", "-cp", classpath, "-d", str(classes),
                    *map(str, sources)], cwd=ROOT, check=True)
    (runtime / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return os.pathsep.join(str(p) for p in [classes, *jars])


def run_case(args, destination, runtime, dependencies, users, variant):
    name = f"{args.label}-{args.mode}-{users}-{variant}"
    config = {
        "users": users, "tablesPerUser": args.tables_per_user, "tables": args.tables,
        "columns": args.columns, "warmups": args.warmups, "samples": args.samples,
        "seed": args.seed, "mode": args.mode, "requirement": "read", "refresh": True,
        "adapter": "" if variant == "stock" else ADAPTER,
        "serviceDef": str(args.service_def.resolve()),
    }
    config_path = destination / f"{name}-config.json"
    result_path = destination / f"{name}-result.json"
    config_path.write_text(json.dumps(config, indent=2) + "\n")
    command = ["java", "-Xms256m", f"-Xmx{args.heap}", "-XX:+UseG1GC",
               f"-Dlog4j2.configurationFile={destination / 'runtime/log4j2.xml'}",
               f"-Xlog:gc:file={destination / (name + '-gc.log')}:time,uptime,level,tags",
               "-cp", runtime + os.pathsep + dependencies, MAIN, str(config_path), str(result_path)]
    print(f"START {name}", flush=True)
    with (destination / f"{name}.log").open("w") as log:
        subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
    result = json.loads(result_path.read_text())
    if not result.get("revocationVerified"):
        raise RuntimeError(f"Refresh/revocation not verified: {name}")
    print(f"DONE {name}: {result['retainedPolicyMiB']:.1f} MiB, P50 {result['p50Ms']:.3f} ms", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stock-classpath-file", type=Path, required=True)
    parser.add_argument("--optimized-classpath-file", type=Path)
    parser.add_argument("--service-def", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--label", default="ranger")
    parser.add_argument("--users", nargs="+", type=int, default=[260, 500, 3000])
    parser.add_argument("--tables-per-user", type=int, default=800)
    parser.add_argument("--tables", type=int, default=5000)
    parser.add_argument("--columns", type=int, default=3)
    parser.add_argument("--samples", type=int, default=40)
    parser.add_argument("--warmups", type=int, default=8)
    parser.add_argument("--seed", type=int, default=83)
    parser.add_argument("--mode", choices=["cascade", "column"], default="cascade")
    parser.add_argument("--heap", default="12g")
    args = parser.parse_args()
    if not all(c.isalnum() or c in "-_." for c in args.label):
        parser.error("label must contain only letters, digits, '.', '_' or '-'")
    destination = args.destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    stock = read_classpath(args.stock_classpath_file)
    optimized = read_classpath(args.optimized_classpath_file) if args.optimized_classpath_file else stock
    runtime = stage(destination, optimized)
    (destination / "runtime/log4j2.xml").write_text(
        '<Configuration status="ERROR"><Appenders><Console name="console" target="SYSTEM_ERR">'
        '<PatternLayout pattern="%level %logger: %msg%n"/></Console></Appenders>'
        '<Loggers><Root level="error"><AppenderRef ref="console"/></Root></Loggers></Configuration>\n')
    comparisons = []
    for users in args.users:
        before = run_case(args, destination, runtime, stock, users, "stock")
        after = run_case(args, destination, runtime, optimized, users, "optimized")
        for field in ["requests", "authorizationQuestions", "verdictAndAuditFingerprint"]:
            if before[field] != after[field]:
                raise RuntimeError(f"Workload/decision mismatch: {users}/{field}")
        comparisons.append({"users": users, "stock": before, "optimized": after,
                            "retainedMemoryReductionPercent": 100 * (1 - after["retainedPolicyMiB"]
                                                                     / before["retainedPolicyMiB"]),
                            "p50Speedup": before["p50Ms"] / after["p50Ms"]})
        (destination / "comparisons.json").write_text(json.dumps(comparisons, indent=2) + "\n")


if __name__ == "__main__":
    main()
