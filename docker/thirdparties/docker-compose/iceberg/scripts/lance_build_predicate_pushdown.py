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
"""Build the Lance Directory catalog of the pushdown suites with lance_fixture_requirements.txt.

The standard MinIO mirror publishes the catalog root at warehouse/lance. This writes the
__manifest and one new table into a root that already holds the committed all_types.lance and
the committed vs_ivf_pq_f32 dataset:

  all_types                 registered at the root, at all_types.lance
  doris.vs_ivf_pq_f32       registered at its committed location; two 512-row fragments that
                            test_lance_fragment_grouping groups
  doris.predicate_pushdown  created here with the rows of the scalar pushdown suites, written in
                            two appends so that the table has two fragments
"""

import argparse
import datetime
import importlib.metadata
import io
import re
import shutil
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import lance
import lance_namespace
import pyarrow as pa
from lance_namespace import (CreateNamespaceRequest, CreateTableRequest, DescribeTableRequest,
                             ListTablesRequest, RegisterTableRequest)
from pyarrow import ipc

REQUIREMENTS_FILE = Path(__file__).parent / "lance_fixture_requirements.txt"
PINNED_WRITERS = ("pylance", "lance-namespace")
MANIFEST_DIR = "__manifest"
ALL_TYPES_DIR = "all_types.lance"
NAMESPACE = "doris"
VECTOR_TABLE = "vs_ivf_pq_f32"
VECTOR_DIR = "b3c9ce40_doris$vs_ivf_pq_f32"
TABLE = "predicate_pushdown"

SCHEMA = pa.schema([
    pa.field("row_id", pa.int64(), nullable=False),
    pa.field("bool_value", pa.bool_()),
    pa.field("int8_value", pa.int8()),
    pa.field("int16_value", pa.int16()),
    pa.field("int32_value", pa.int32()),
    pa.field("int64_value", pa.int64()),
    pa.field("float32_value", pa.float32()),
    pa.field("float64_value", pa.float64()),
    pa.field("decimal128_value", pa.decimal128(18, 2)),
    pa.field("utf8_value", pa.string()),
    pa.field("date32_value", pa.date32()),
    pa.field("array_value", pa.list_(pa.field("element", pa.string()))),
])

# Every type has NULLs, boundary values, signed values or duplicates for operator coverage.
# The first append holds row_id 1-6 and the second 7-11.
NULLS = (None,) * 11
ROWS = [
    (1,) + NULLS,
    (2, False, -128, -32768, -2147483648, -9223372036854775807, -100.5, -100.5,
     Decimal("-100.50"), "minimum", datetime.date(1969, 12, 31), ["negative"]),
    (3, False, -100, -100, -100, -100, -100.0, -100.0,
     Decimal("-100.00"), "negative", datetime.date(1970, 1, 1), ["negative"]),
    (4, False, -1, -1, -1, -1, -1.0, -1.0,
     Decimal("-1.00"), "minus-one", datetime.date(2023, 12, 31), ["negative"]),
    (5, True, 0, 0, 0, 0, 0.0, 0.0,
     Decimal("0.00"), "", datetime.date(2024, 1, 1), []),
    (6, True, 1, 1, 1, 1, 1.0, 1.0,
     Decimal("1.00"), "one", datetime.date(2024, 1, 2), ["positive"]),
    (7, True, 10, 10, 10, 10, 10.0, 10.0,
     Decimal("10.00"), "ten-a", datetime.date(2024, 1, 10), ["positive", "lance"]),
    (8, False, 10, 10, 10, 10, 10.0, 10.0,
     Decimal("10.00"), "ten-b", datetime.date(2024, 1, 10), ["duplicate"]),
    (9, True, 100, 100, 100, 100, 100.0, 100.0,
     Decimal("100.00"), "hundred", datetime.date(2024, 4, 10), ["positive"]),
    (10, True, 127, 32767, 2147483647, 9223372036854775807, 1000.5, 1000.5,
     Decimal("9999999999999999.99"), "maximum", datetime.date(2024, 12, 31), ["positive"]),
    (11,) + NULLS,
]
FIRST_APPEND_ROWS = 6


def table() -> pa.Table:
    return pa.Table.from_pylist([dict(zip(SCHEMA.names, row)) for row in ROWS], schema=SCHEMA)


def check_pinned_writer() -> None:
    # The self-check below reads the fixture back with the writer that produced it, so it
    # cannot catch a wrong writer; refuse anything but the pinned pylance and lance-namespace.
    if not __debug__:
        raise RuntimeError("Fixture verification requires assertions; do not use python -O")
    requirements = REQUIREMENTS_FILE.read_text()
    for distribution in PINNED_WRITERS:
        pinned = re.search(rf"^{re.escape(distribution)}==(\S+)$", requirements, re.MULTILINE)
        assert pinned, f"no {distribution} pin found in {REQUIREMENTS_FILE.name}"
        installed = importlib.metadata.version(distribution)
        if installed != pinned.group(1):
            raise RuntimeError(f"Use the pinned {distribution} {pinned.group(1)}, not {installed}")


def compact_manifest(root: Path) -> None:
    # Every namespace mutation leaves a manifest fragment, index delta and version behind; fold
    # them together so that the committed fixture stays small. Only the manifest is compacted.
    manifest = lance.dataset(str(root / MANIFEST_DIR))
    manifest.optimize.compact_files()
    manifest.optimize.optimize_indices(num_indices_to_merge=len(manifest.list_indices()))
    manifest.cleanup_old_versions(older_than=timedelta(0), delete_unverified=True)
    # cleanup_old_versions does not reclaim superseded index deltas.
    manifest = lance.dataset(str(root / MANIFEST_DIR))
    referenced = {index["uuid"] for index in manifest.list_indices()}
    for index_dir in (root / MANIFEST_DIR / "_indices").iterdir():
        if index_dir.name not in referenced:
            shutil.rmtree(index_dir)
    # The writer last updated the optional latest-version hint before the cleanup above; point
    # it at the version that survived, or readers that trust it open a deleted manifest.
    hint = root / MANIFEST_DIR / "_versions" / "latest_version_hint.json"
    hint.write_text(f'{{"version":{manifest.version}}}')


def build(root: Path) -> None:
    check_pinned_writer()
    if (root / MANIFEST_DIR).exists():
        raise FileExistsError(root / MANIFEST_DIR)
    for committed in (ALL_TYPES_DIR, VECTOR_DIR):
        if not (root / committed).is_dir():
            raise FileNotFoundError(root / committed)
    namespace = lance_namespace.connect("dir", {"root": str(root)})
    namespace.register_table(RegisterTableRequest(id=["all_types"], location=ALL_TYPES_DIR))
    namespace.create_namespace(CreateNamespaceRequest(id=[NAMESPACE]))
    namespace.register_table(RegisterTableRequest(id=[NAMESPACE, VECTOR_TABLE], location=VECTOR_DIR))
    rows = table()
    buffer = io.BytesIO()
    with ipc.new_stream(buffer, SCHEMA) as writer:
        writer.write_table(rows.slice(0, FIRST_APPEND_ROWS))
    response = namespace.create_table(CreateTableRequest(id=[NAMESPACE, TABLE]), buffer.getvalue())
    # The namespace hashes a random prefix into the directory name; use the location it returns.
    lance.write_dataset(rows.slice(FIRST_APPEND_ROWS), response.location, mode="append")
    compact_manifest(root)
    check(root)


def check(root: Path) -> None:
    check_pinned_writer()
    namespace = lance_namespace.connect("dir", {"root": str(root)})
    assert "all_types" in namespace.list_tables(ListTablesRequest(id=[])).tables
    assert sorted(namespace.list_tables(ListTablesRequest(id=[NAMESPACE])).tables) == sorted(
        [TABLE, VECTOR_TABLE])
    location = namespace.describe_table(DescribeTableRequest(id=[NAMESPACE, TABLE])).location
    dataset = lance.dataset(location)
    assert dataset.schema.remove_metadata() == SCHEMA, dataset.schema
    assert dataset.to_table().to_pylist() == table().to_pylist()
    assert [f.to_table(columns=["row_id"])["row_id"].to_pylist() for f in dataset.get_fragments()] == [
        list(range(1, FIRST_APPEND_ROWS + 1)), list(range(FIRST_APPEND_ROWS + 1, len(ROWS) + 1))]
    vector = namespace.describe_table(DescribeTableRequest(id=[NAMESPACE, VECTOR_TABLE])).location
    assert len(lance.dataset(vector).get_fragments()) == 2
    manifest = lance.dataset(str(root / MANIFEST_DIR))
    hint = (root / MANIFEST_DIR / "_versions" / "latest_version_hint.json").read_text()
    assert hint == f'{{"version":{manifest.version}}}', hint
    print(f"Verified {NAMESPACE}.{TABLE} at {location} and __manifest version {manifest.version}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, default=Path(__file__).parent / "preinstalled_data/lance")
    parser.add_argument("--check", action="store_true", help="Verify the committed fixture without rebuilding")
    args = parser.parse_args()
    (check if args.check else build)(args.root)
