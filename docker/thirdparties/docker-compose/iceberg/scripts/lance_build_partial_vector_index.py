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

"""Generate the partially indexed quantized vector fixture.

partial_vector_index.lance is a root table of the preinstalled Directory catalog. A quantized
index covers its first fragment only, so a search over it plans a mixed set of splits: one per
physical index segment, plus a flat split per uncovered fragment.

    version 1  create        row_id 1..512    fragment 0
    version 2  vector index  vec_idx (IVF_PQ, l2, 2 partitions, 2 sub-vectors) over fragment 0
    version 3  append        row_id 513..640  fragment 1, covered by no index

vec = [row_id, row_id + 1, row_id + 2, row_id + 3] as float32, so the squared L2 distance
between rows r and n is exactly 4 * (n - r)^2 and every expected distance is an analytic value
rather than a recall comparison. A query a quarter step past row 512 therefore has its true
neighbours alternating between the two arms: 512, 513, 511, 514, 510, 515.

An IVF_PQ segment scores its candidates from the compressed codes, so without exact rescoring
the indexed rows report approximations while the appended fragment reports exact distances.
Measured through Doris on the committed codebook, the unrefined merge returns 512, 513, 511,
514, 515, 510 and reports row 510 at 30.2798 against its analytic 20.25, which pushes it past
the exactly scored 515: both the distance and the order are wrong. Rows 511 and 512 happen to
reconstruct exactly from their codes here, so which indexed rows the error reaches, and by how
much, is a property of the trained codebook rather than of the shape - earlier builds of these
same parameters reported 1.2808, 4.3113 and 25.25 for the three indexed rows, and 0.53, 3.53
and 20.25. What a test can pin is therefore the rescored side, which is always exactly these
analytic constants.

For the same reason the committed fixture must not be regenerated on its own. Rescoring recomputes
the distances of the candidates the index already chose and does not widen that choice, so a
retrained codebook can return a different candidate set; regenerate the suite's .out alongside it.

These parameters keep candidate selection intact while leaving that error visible. num_bits=4
over this ladder does not: it collapses the index so the true neighbours are not selected at
all, which is a recall failure rather than the scale mismatch under test.

index_file_version V3 is what the Doris BE reads through lance-c; the default would produce an
index the backend cannot open.

Usage:
  python3 lance_build_partial_vector_index.py preinstalled_data/lance
  python3 lance_build_partial_vector_index.py --check preinstalled_data/lance
"""
import argparse
import shutil
from pathlib import Path

import lance
import pyarrow as pa

PARTIAL_DIR = "partial_vector_index.lance"
INDEXED_ROWS = 512
APPENDED_ROWS = 128
DIMENSION = 4
INDEX_NAME = "vec_idx"


def rows_of(first_id: int, last_id: int) -> pa.Table:
    ids = list(range(first_id, last_id + 1))
    return pa.table({
        "row_id": pa.array(ids, pa.int32()),
        "vec": pa.array([[float(i + j) for j in range(DIMENSION)] for i in ids],
                        pa.list_(pa.float32(), DIMENSION)),
    })


def build(output: Path) -> None:
    if output.exists():
        shutil.rmtree(output)
    uri = str(output)
    lance.write_dataset(rows_of(1, INDEXED_ROWS), uri, mode="create", data_storage_version="2.2")
    lance.dataset(uri).create_index("vec", "IVF_PQ", name=INDEX_NAME, metric="l2",
                                    num_partitions=2, num_sub_vectors=2, sample_rate=256,
                                    index_file_version="V3")
    lance.write_dataset(rows_of(INDEXED_ROWS + 1, INDEXED_ROWS + APPENDED_ROWS), uri,
                        mode="append", data_storage_version="2.2")
    assert lance.dataset(uri).version == 3, lance.dataset(uri).version


def check(output: Path) -> None:
    dataset = lance.dataset(str(output))
    assert [v["version"] for v in dataset.versions()] == [1, 2, 3], dataset.versions()
    row_ids = sorted(dataset.to_table(columns=["row_id"])["row_id"].to_pylist())
    assert row_ids == list(range(1, INDEXED_ROWS + APPENDED_ROWS + 1)), "rows differ"
    assert len(dataset.get_fragments()) == 2, dataset.get_fragments()
    indexes = dataset.list_indices()
    assert [index["name"] for index in indexes] == [INDEX_NAME], indexes
    # The append must stay outside the index, which is what makes the plan mixed.
    assert sorted(indexes[0]["fragment_ids"]) == [0], indexes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", type=Path, help="directory holding the fixture")
    parser.add_argument("--check", action="store_true", help="verify the existing fixture")
    args = parser.parse_args()
    if args.check:
        check(args.root / PARTIAL_DIR)
    else:
        build(args.root / PARTIAL_DIR)
        check(args.root / PARTIAL_DIR)
    print(f"{PARTIAL_DIR} {'checked' if args.check else 'written'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
