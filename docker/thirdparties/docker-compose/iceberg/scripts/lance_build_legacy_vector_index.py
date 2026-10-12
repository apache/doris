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

"""Generate the Lance legacy vector index regression fixture.

legacy_vector_index.lance is a root table of the preinstalled Directory catalog next to
time_travel.lance. Its vector index was written by pylance 0.18.2, which predates index details
in the manifest, so no segment of vec_idx carries details:

    version 1  create        row_id 1..512   fragment 0
    version 2  vector index  vec_idx (IVF_PQ, l2, 2 partitions, 2 sub-vectors) over fragment 0

vec = [row_id, row_id + 1, row_id + 2, row_id + 3] as float32. Lance infers the missing details
from the index files when it loads the index, so the FE plans vec_idx like any other index and
the BE searches it through the planned segment instead of falling back to a flat search.

Build it with pylance 0.18.2, not the writer pinned in lance_fixture_requirements.txt; the
catalog build carries it over as-is. --check runs with any reader.

Usage:
  pip install pylance==0.18.2 && python3 lance_build_legacy_vector_index.py preinstalled_data/lance
  python3 lance_build_legacy_vector_index.py --check preinstalled_data/lance
"""
import argparse
import shutil
from pathlib import Path

import lance
import pyarrow as pa

LEGACY_DIR = "legacy_vector_index.lance"
WRITER_VERSION = "0.18.2"
ROWS = 512
DIMENSION = 4


def build(output: Path) -> None:
    if lance.__version__ != WRITER_VERSION:
        raise SystemExit(f"build {LEGACY_DIR} with pylance {WRITER_VERSION}, not {lance.__version__}")
    if output.exists():
        shutil.rmtree(output)
    ids = list(range(1, ROWS + 1))
    table = pa.table({
        "row_id": pa.array(ids, pa.int32()),
        "vec": pa.array([[float(i + j) for j in range(DIMENSION)] for i in ids],
                        pa.list_(pa.float32(), DIMENSION)),
    })
    uri = str(output)
    lance.write_dataset(table, uri, mode="create")
    lance.dataset(uri).create_index("vec", "IVF_PQ", name="vec_idx", metric="l2", num_partitions=2,
                                    num_sub_vectors=2, sample_rate=256)
    assert lance.dataset(uri).version == 2, lance.dataset(uri).version


def check(output: Path) -> None:
    dataset = lance.dataset(str(output))
    assert [v["version"] for v in dataset.versions()] == [1, 2], dataset.versions()
    row_ids = sorted(dataset.to_table(columns=["row_id"])["row_id"].to_pylist())
    assert row_ids == list(range(1, ROWS + 1)), "rows differ"
    indexes = dataset.list_indices()
    assert [index["name"] for index in indexes] == ["vec_idx"], indexes
    assert sorted(indexes[0]["fragment_ids"]) == [0], indexes
    manifests = sorted((output / "_versions").glob("*.manifest"))
    assert manifests, f"no manifest under {output / '_versions'}"
    for manifest in manifests:
        content = manifest.read_bytes()
        assert b"VectorIndexDetails" not in content, f"{manifest} carries index details"
        assert WRITER_VERSION.encode() in content, f"{manifest} was not written by pylance {WRITER_VERSION}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", type=Path, help="directory holding the fixture")
    parser.add_argument("--check", action="store_true", help="verify the existing fixture")
    args = parser.parse_args()
    if args.check:
        check(args.root / LEGACY_DIR)
    else:
        build(args.root / LEGACY_DIR)
        check(args.root / LEGACY_DIR)
    print(f"{LEGACY_DIR} {'checked' if args.check else 'written'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
