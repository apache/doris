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

"""Extract MaxCompute P2 seed SQL from the suites; never contact a service."""

import argparse
import hashlib
import json
import re
from pathlib import Path

import sqlparse

SOURCES = (
    "test_external_catalog_maxcompute",
    "test_max_compute_all_type",
    "test_max_compute_complex_type",
    "test_max_compute_partition_prune",
    "test_max_compute_timestamp",
    "test_max_compute_schema",
)
CROSS_TABLES = {"other_db_mc_tb", "other_db_mc_parts"}


def extract(repo):
    statements = {role: [] for role in ("base", "cross", "schema")}
    tables = {role: [] for role in statements}
    sources = {}
    for name in SOURCES:
        path = repo / "regression-test/suites/external_table_p2/maxcompute" / (name + ".groovy")
        text = path.read_text()
        blocks = [block for block in re.findall(r"/\*(.*?)\*/", text, re.S)
                  if re.search(r"\bCREATE\s+TABLE\b", block, re.I)]
        assert len(blocks) == 1, f"Expected one seed block in {path}"
        block = re.sub(r"(?m)^\s*//.*$", "", blocks[0])
        sources[str(path.relative_to(repo))] = hashlib.sha256(path.read_bytes()).hexdigest()
        default_role = "schema" if name == "test_max_compute_schema" else "base"
        for raw in sqlparse.split(block):
            sql = sqlparse.format(raw, strip_comments=True).strip()
            assert sql.endswith(";"), f"Unterminated seed statement in {path}: {sql}"
            command = sql.split()[0].upper()
            if command in {"SELECT", "SHOW"}:
                continue
            # Source comments include destructive reset commands and the original
            # globally unique project name. Select a fresh target project externally.
            if command == "DROP":
                assert re.match(r"DROP\s+TABLE\b", sql, re.I), sql
                continue
            if command == "USE" and not re.match(r"USE\s+SCHEMA\b", sql, re.I):
                continue
            assert command in {"CREATE", "INSERT", "SET", "USE", "ALTER"}, sql
            if command == "ALTER":
                assert re.fullmatch(r"ALTER\s+TABLE\s+[`\w]+\s+MERGE\s+SMALLFILES\s*;", sql, re.I), sql
            match = re.match(r"(?:(?:CREATE|ALTER)\s+TABLE|INSERT\s+INTO(?:\s+TABLE)?)\s+([`\w.]+)",
                             sql, re.I)
            role = default_role
            if match:
                table = match.group(1).replace("`", "").split(".")[-1]
                role = "cross" if table in CROSS_TABLES else default_role
                if command == "CREATE":
                    assert table not in tables[role], f"Duplicate seed table: {role}.{table}"
                    tables[role].append(table)
            else:
                assert command in {"SET", "USE"} or re.match(r"CREATE\s+SCHEMA\b", sql, re.I), sql
            statements[role].append(sql)
    assert {role: len(names) for role, names in tables.items()} == {"base": 21, "cross": 2, "schema": 5}
    return statements, tables, sources


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument("--output", type=Path, required=True, help="A new directory outside tracked source")
    args = parser.parse_args()
    statements, tables, sources = extract(args.repo.resolve())
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {"sources_sha256": sources, "projects": {}}
    for role, items in statements.items():
        sql = "-- Select the dedicated target project for role: " + role + "\n"
        sql += "-- Fresh initialization only; CREATE refuses existing tables.\n"
        sql += "SET odps.sql.type.system.odps2=true;\n\n" + "\n\n".join(items) + "\n"
        filename = role + ".sql"
        (args.output / filename).write_text(sql)
        manifest["projects"][role] = {"file": filename, "tables": tables[role],
                                      "sha256": hashlib.sha256(sql.encode()).hexdigest()}
        print(f"{role}: {len(tables[role])} tables -> {filename}")
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
