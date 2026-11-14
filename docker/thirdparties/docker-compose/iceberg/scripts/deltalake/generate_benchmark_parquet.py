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

"""Convert SF1 TPC tool output to shared Hive/Delta Parquet files (DuckDB 1.4.4)."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import tempfile

import duckdb


def convert(kind, generator, data_dir, output):
    ddl = generator / ("dss.ddl" if kind == "tpch" else "tpcds.sql")
    extension = "tbl" if kind == "tpch" else "dat"
    manifest = {}
    with tempfile.TemporaryDirectory(dir=output) as temporary:
        connection = duckdb.connect(config={"threads": "4", "memory_limit": "2GB",
                                            "temp_directory": temporary})
        try:
            tables = re.findall(r"create\s+table\s+(\w+)\s*\((.*?)\);", ddl.read_text(), re.I | re.S)
            for table, body in tables:
                table = table.lower()
                if table == "dbgen_version":
                    continue
                columns = re.findall(
                    r"(?:^|,\s*)\s*(\w+)\s+(integer|date|decimal\s*\(\s*\d+\s*,\s*\d+\s*\)"
                    r"|(?:var)?char\(\d+\))", body, re.I)
                types = {name.lower(): re.sub(r"(?:var)?char\(\d+\)", "VARCHAR", dtype, flags=re.I)
                         for name, dtype in columns}
                # TPC tool rows have a trailing separator; discard that empty column explicitly.
                types["_trailing"] = "VARCHAR"
                source = data_dir / f"{table}.{extension}"
                if kind == "tpcds":
                    # The 3.2.0 country distributions include Latin-1 names such as RÉUNION.
                    utf8 = Path(temporary) / source.name
                    with source.open(encoding="latin-1") as reader, utf8.open("w", encoding="utf-8") as writer:
                        shutil.copyfileobj(reader, writer)
                    source = utf8
                relation = connection.read_csv(str(source), columns=types, header=False,
                                               delimiter="|", quotechar="", escapechar="")
                relation = relation.project(",".join(name for name in types if name != "_trailing"))
                folder = output / f"delta_{kind}" / table / "data"
                folder.mkdir(parents=True, exist_ok=True)
                parquet = folder / "part-00000.parquet"
                relation.write_parquet(str(parquet), compression="snappy", row_group_size=122880)
                count = connection.execute("SELECT count(*) FROM read_parquet(?)", [str(parquet)]).fetchone()[0]
                digest = hashlib.sha256()
                with parquet.open("rb") as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(chunk)
                manifest[table] = {"rows": count, "bytes": parquet.stat().st_size, "sha256": digest.hexdigest()}
                print(kind, table, count, flush=True)
        finally:
            connection.close()
    assert len(manifest) == (8 if kind == "tpch" else 24), (kind, len(manifest))
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for workload in ["tpch", "tpcds"]:
        parser.add_argument(f"--{workload}-generator-dir", type=Path, required=True)
        parser.add_argument(f"--{workload}-data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("Output already contains files; reuse its fixtures for tests or choose a new output directory")
    args.output.mkdir(parents=True, exist_ok=True)
    result = {kind: convert(kind, getattr(args, f"{kind}_generator_dir"),
                            getattr(args, f"{kind}_data_dir"), args.output) for kind in ["tpch", "tpcds"]}
    (args.output / "manifest.json").write_text(json.dumps(result, indent=2) + "\n")
