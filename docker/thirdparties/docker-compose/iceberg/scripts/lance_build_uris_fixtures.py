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

"""Build the Lance uris-TVF regression fixtures.

Generates several Lance datasets with strictly-equal schemas but disjoint
row_id values, all derived from all_types.lance by shifting only its row_id
column. The uris TVF regression case reads them as separate locations:

  lance_1/a.lance      row_id += 100000
  lance_1/b.lance      row_id += 200000
  lance_2/a.lance      row_id += 300000

The row_id offsets MUST stay in sync with the assertions hard-coded in
test_lance_s3_tvf.groovy (1..12, 100001..100012, 200001..200012, 300001..300012).

Run it with the writer pinned in lance_fixture_requirements.txt; the main
catalog script's check_pinned_writer() enforces the pin for the whole catalog,
but this standalone script does not.

Usage:
  python3 lance_build_uris_fixtures.py
  python3 lance_build_uris_fixtures.py --check
  python3 lance_build_uris_fixtures.py /path/to/preinstalled_data
"""
import argparse
import shutil
from pathlib import Path

import lance
import pyarrow as pa

ALL_TYPES_DIR = "all_types.lance"
# (subdir, filename, row_id offset) - offsets MUST match test_lance_s3_tvf.groovy.
URIS_FIXTURE_TARGETS = (
    ("lance_1", "a.lance", 100000),
    ("lance_1", "b.lance", 200000),
    ("lance_2", "a.lance", 300000),
)
DEFAULT_ROOT = Path(__file__).resolve().parent / "preinstalled_data"


def build(output: Path) -> None:
    source = output / "lance" / ALL_TYPES_DIR
    assert source.is_dir(), f"uris fixture source missing: {source}"
    src_ds = lance.dataset(str(source))
    src_schema = src_ds.schema
    tbl = src_ds.to_table()
    # to_table() materializes lance.blob.v2 into a human-readable struct whose layout
    # differs from the extension's physical storage type, and tags it with a legacy
    # "lance-encoding:blob" marker that 7.0.0 refuses to rewrite for data version 2.2.
    # Replace the column with a placeholder built from the extension's real storage
    # type (struct<data, uri, position, size>). The blob column is never read by the
    # test, only its schema must match all_types.lance; the column then writes as a
    # fresh v2 blob and the merge's strict schema-equality check passes.
    for i, f in enumerate(src_schema):
        if isinstance(f.type, pa.ExtensionType) and f.type.extension_name == "lance.blob.v2":
            n = tbl.num_rows
            st = f.type.storage_type
            placeholders = [
                pa.array([b""] * n, type=pa.large_binary()),
                pa.array([None] * n, type=pa.string()),
                pa.array([0] * n, type=pa.uint64()),
                pa.array([0] * n, type=pa.uint64()),
            ]
            storage = pa.StructArray.from_arrays(placeholders, fields=st)
            ext_arr = pa.ExtensionArray.from_storage(f.type, storage)
            tbl = tbl.set_column(i, f, ext_arr)
            break
    rid_idx = src_schema.get_field_index("row_id")
    assert rid_idx >= 0, "all_types.lance must expose a row_id column"
    rid_field = src_schema.field(rid_idx)
    for sub, name, offset in URIS_FIXTURE_TARGETS:
        target = output / sub / name
        if target.exists():
            shutil.rmtree(target)
        # Same schema, distinct row_id range: shift only the row_id values.
        shifted = pa.compute.add(tbl.column(rid_idx), offset)
        tbl2 = tbl.set_column(rid_idx, rid_field, shifted)
        lance.write_dataset(tbl2, str(target), schema=src_schema, mode="create",
                            data_storage_version="2.2")


def check(output: Path) -> None:
    expected = lance.dataset(str(output / "lance" / ALL_TYPES_DIR)).schema
    for sub, name, offset in URIS_FIXTURE_TARGETS:
        path = output / sub / name
        assert path.is_dir(), f"uris fixture missing: {path}"
        actual = lance.dataset(str(path)).schema
        # Structural (not metadata) equality is what the uris TVF merge enforces.
        assert expected.equals(actual, check_metadata=False), (
            f"uris fixture {path} schema differs from "
            f"{output / 'lance' / ALL_TYPES_DIR}: {actual}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Lance uris-TVF fixtures.")
    parser.add_argument("output", nargs="?", default=DEFAULT_ROOT,
                        help="preinstalled_data root directory")
    parser.add_argument("--check", action="store_true",
                        help="verify the fixtures instead of building them")
    args = parser.parse_args()
    (check if args.check else build)(Path(args.output))


if __name__ == "__main__":
    main()
