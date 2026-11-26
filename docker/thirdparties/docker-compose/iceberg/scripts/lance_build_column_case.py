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

"""Build the column-case fixtures with lance_fixture_requirements.txt.

The standard MinIO mirror publishes them as warehouse/lance/column_case.lance and
warehouse/lance/column_case_conflict.lance, which test_lance_s3_tvf reads through the s3()
table function. Lance names columns case-sensitively and Doris does not: column_case.lance
has one mixed-case column, which keeps its spelling, and column_case_conflict.lance has two
columns whose names differ only in case, which Doris refuses.
"""

import argparse
from pathlib import Path

import lance
import pyarrow as pa


def expected_tables() -> dict:
    mixed_case = pa.Table.from_pylist(
        [{"row_id": 1, "UserName": "a"}, {"row_id": 2, "UserName": "b"}, {"row_id": 3, "UserName": "c"}],
        schema=pa.schema([("row_id", pa.int32()), ("UserName", pa.string())]))
    conflict = pa.Table.from_pylist(
        [{"id": 1, "ID": 2}], schema=pa.schema([("id", pa.int32()), ("ID", pa.int32())]))
    return {"column_case.lance": mixed_case, "column_case_conflict.lance": conflict}


def check_pinned_writer() -> None:
    if not __debug__:
        raise RuntimeError("Fixture verification requires assertions; do not use python -O")
    if lance.__version__ != "7.0.0":
        raise RuntimeError("Use the pinned pylance 7.0.0 fixture writer")


def check(output_dir: Path) -> None:
    check_pinned_writer()
    for name, expected in expected_tables().items():
        # Validate the persisted field names with their case, and the values.
        actual = lance.dataset(str(output_dir / name)).to_table()
        assert actual.equals(expected), f"column-case fixture differs from expected data: {output_dir / name}"
        print(f"Verified the column-case fixture at {output_dir / name}")


def build(output_dir: Path) -> None:
    check_pinned_writer()
    tables = expected_tables()
    for name in tables:
        if (output_dir / name).exists():
            raise FileExistsError(output_dir / name)
    for name, table in tables.items():
        lance.write_dataset(table, str(output_dir / name), data_storage_version="2.2")
    check(output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).parent / "preinstalled_data/lance")
    parser.add_argument("--check", action="store_true", help="Verify the committed fixtures without rebuilding")
    args = parser.parse_args()
    (check if args.check else build)(args.output_dir)
