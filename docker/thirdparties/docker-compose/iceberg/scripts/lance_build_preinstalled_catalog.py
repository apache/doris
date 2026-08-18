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

"""Rebuild the preinstalled Lance Directory (V2) catalog fixture.

This offline generator produces docker-compose/iceberg/scripts/preinstalled_data/lance,
the fixture that the iceberg docker environment copies into MinIO at s3://warehouse/lance.
It exists because lance-spark-bundle does not expose vector index creation through SQL, so
indexed vector-search fixtures cannot be built by the Spark preinstall scripts. That gap is
why the previous fixture had no vector index at all: run07_create_vector_types.sql created
doris.vector_search and deferred index creation to a companion script that never existed, so
every "indexed" query in test_lance_vector_search silently ran a flat KNN scan.

The generated catalog contains:
  - __manifest            Directory Namespace V2 manifest table (with its scalar indexes).
  - all_types.lance       The pre-existing compatibility-mode root table, re-registered as-is.
  - The `doris` namespace with one indexed vector table per ANN algorithm (hash-prefixed
    directories), listed in VECTOR_TABLES below.

Every vector table holds identical deterministic data: 1024 rows in two 512-row fragments,
16-dim Float32 `embedding` where embedding[j] = (row_id - 1) + j. For a query equal to the
vector of row r, the exact squared L2 distance of row n is 16 * (n - r)^2, so head/tail
queries have no distance ties. All values are integers below 2^11 and therefore exact in
Float32.

Environment: python3 with the exact pins in lance_fixture_requirements.txt. Index training
(IVF kmeans) is not bit-reproducible across runs, so regenerating this fixture changes the
binary output and requires regenerating the dependent regression .out files. Reproducible
properties are asserted by the self-check below instead: logical metadata, the exact L2
distance ladder the regression goldens encode, plan shape, and the partition-boundary
discriminator. Indexed-vs-flat agreement is lossy for IVF_PQ, so it is only recorded.

The self-check is the entire contract for a fixture whose bytes cannot be reproduced, so
this script refuses to run under python -O, where assert statements are stripped.

Usage:
  python3 lance_build_preinstalled_catalog.py            # rebuild in place
  python3 lance_build_preinstalled_catalog.py --check    # self-check the existing fixture
"""

import argparse
import io
import json
import shutil
import sys
import tempfile
from datetime import timedelta
from pathlib import Path

import lance
import lance_namespace
import pyarrow as pa
import pyarrow.ipc as ipc
from lance_namespace_urllib3_client.models import (
    CreateNamespaceRequest,
    CreateTableRequest,
    DescribeTableRequest,
    ListTablesRequest,
    RegisterTableRequest,
)

DIM = 16
ROWS = 1024
FRAGMENT_ROWS = 512
NUM_PARTITIONS = 4
NAMESPACE = "doris"
ALL_TYPES_DIR = "all_types.lance"
MANIFEST_DIR = "__manifest"

# 4-bit PQ keeps codebook training comfortable on 1024 rows. This only serves fixture
# stability and is not a Doris compatibility statement about PQ parameters.
PQ_BUILD_PARAMS = {"num_sub_vectors": 4, "num_bits": 4}

# One table per ANN algorithm and element type, identical data, exactly one index named
# embedding_<the table name without its vs_ prefix>. Naming is vs_<algorithm>_<element
# type>, so one table is exactly one cell of the algorithm x element type matrix and a
# missing combination is visible from the table list alone. The build loop and the
# self-check are driven entirely by these specs; follow-up work for #66495 adds the
# remaining algorithms (IVF_FLAT, IVF_SQ, IVF_HNSW_*) and element types here.
VECTOR_TABLES = {
    "vs_ivf_pq_f32": {"index_type": "IVF_PQ", "params": PQ_BUILD_PARAMS},
}

# The head query is exactly row 1's vector; the tail query is row 1024's. Only endpoint
# vectors are used so that 16 * (n - r)^2 never ties between two different rows n.
HEAD_QUERY = [float(j) for j in range(DIM)]
TAIL_QUERY = [float(ROWS - 1 + j) for j in range(DIM)]
# On this collinear data IVF kmeans yields four contiguous row ranges, and row 256 lands near
# one of the internal edges - which side, and at exactly which row, changes every time the
# index is retrained, so nothing here hardcodes it (--check prints the measured range). What
# matters is only that part of row 256's true neighbourhood falls in an adjacent partition.
# The regression suites query this row with nprobes=1 as their silent-fallback discriminator:
# a real single-partition probe must miss those neighbours (result != flat), while a silent
# flat fallback returns exactly the flat result - verified directly: on an unindexed copy of
# this data Lance ignores nprobes entirely and returns the flat rows. The self-check pins
# this property for every vector table, so regenerating the fixture with partition edges that
# no longer split row 256's neighbourhood fails here instead of in the suites. Each table
# trains its own IVF clustering, so this is checked per table.
BOUNDARY_ROW = 256
BOUNDARY_QUERY = [float(BOUNDARY_ROW - 1 + j) for j in range(DIM)]


def make_fragment_table(row_offset_start: int, row_offset_end: int) -> pa.Table:
    offsets = list(range(row_offset_start, row_offset_end))
    embedding = pa.FixedSizeListArray.from_arrays(
        pa.array(
            [float(offset + j) for offset in offsets for j in range(DIM)],
            type=pa.float32(),
        ),
        DIM,
    )
    table = pa.table(
        {
            "row_id": pa.array([offset + 1 for offset in offsets], type=pa.int64()),
            "category": pa.array(
                ["even" if offset % 2 == 0 else "odd" for offset in offsets]
            ),
            "label": pa.array([f"item-{offset + 1:04d}" for offset in offsets]),
            "embedding": embedding,
        }
    )
    # The Spark fixture this replaces declared every column NOT NULL, and its DESC golden is
    # the only place in the Lance suites that records a non-nullable Lance column mapping to
    # Doris 'No'. pyarrow defaults to nullable, so restate it to keep that coverage.
    return table.cast(
        pa.schema([pa.field(f.name, f.type, nullable=False) for f in table.schema])
    )


def index_name_of(table_name: str) -> str:
    # vs_ivf_pq_f32 -> embedding_ivf_pq_f32
    return "embedding_" + table_name.removeprefix("vs_")


def create_vector_table(namespace, table_name: str) -> str:
    first = make_fragment_table(0, FRAGMENT_ROWS)
    buffer = io.BytesIO()
    with ipc.new_stream(buffer, first.schema) as writer:
        writer.write_table(first)
    response = namespace.create_table(
        CreateTableRequest(id=[NAMESPACE, table_name]), buffer.getvalue()
    )
    # Never predict the hashed storage path; always use the location the namespace returns.
    location = response.location
    lance.write_dataset(make_fragment_table(FRAGMENT_ROWS, ROWS), location, mode="append")
    return location


def compact_manifest(root: Path) -> None:
    # Every namespace mutation above leaves a manifest fragment, index delta, and version
    # behind. Fold them together so the committed fixture stays small and reviewable. Only
    # the manifest is compacted: the vector tables must keep exactly two fragments.
    manifest = lance.dataset(str(root / MANIFEST_DIR))
    manifest.optimize.compact_files()
    manifest.optimize.optimize_indices(num_indices_to_merge=len(manifest.list_indices()))
    manifest.cleanup_old_versions(older_than=timedelta(0), delete_unverified=True)
    # cleanup_old_versions does not reclaim superseded index deltas; drop every index
    # directory the resulting manifest version no longer references.
    manifest = lance.dataset(str(root / MANIFEST_DIR))
    referenced = {index["uuid"] for index in manifest.list_indices()}
    for index_dir in (root / MANIFEST_DIR / "_indices").iterdir():
        if index_dir.name not in referenced:
            shutil.rmtree(index_dir)
    # pylance 4.0.1 does not write the optional latest-version hint. Write it to keep the
    # fixture shape identical to the previous one for every consuming reader.
    hint = root / MANIFEST_DIR / "_versions" / "latest_version_hint.json"
    hint.write_text(f'{{"version":{manifest.version}}}')


def build(root: Path, all_types_source: Path) -> None:
    shutil.copytree(all_types_source, root / ALL_TYPES_DIR)
    namespace = lance_namespace.connect("dir", {"root": str(root)})
    namespace.register_table(
        RegisterTableRequest(id=["all_types"], location=ALL_TYPES_DIR)
    )
    namespace.create_namespace(CreateNamespaceRequest(id=[NAMESPACE]))
    for table_name, spec in VECTOR_TABLES.items():
        location = create_vector_table(namespace, table_name)
        # DirectoryNamespace.create_table_index exists but raises UnsupportedOperationError,
        # so open the physical dataset at the location the namespace returned and index it
        # there. index_file_version V3 is what the Doris BE reads through lance-c; the
        # default would produce an index the backend cannot open.
        lance.dataset(location).create_index(
            "embedding",
            spec["index_type"],
            name=index_name_of(table_name),
            metric="L2",
            num_partitions=NUM_PARTITIONS,
            sample_rate=256,
            index_file_version="V3",
            **spec["params"],
        )
    compact_manifest(root)


def topk(dataset, query, k: int, use_index: bool, **nearest_kwargs):
    nearest = {"column": "embedding", "q": query, "k": k, "use_index": use_index}
    if use_index:
        nearest.setdefault("nprobes", NUM_PARTITIONS)
    nearest.update(nearest_kwargs)
    table = dataset.scanner(nearest=nearest).to_table()
    return list(zip(table["row_id"].to_pylist(), table["_distance"].to_pylist()))


def check_vector_dataset(name: str, location: str, index_type: str):
    dataset = lance.dataset(location)
    assert dataset.count_rows() == ROWS, f"{name}: expected {ROWS} rows"
    fragments = dataset.get_fragments()
    assert len(fragments) == 2, f"{name}: expected 2 fragments"
    embedding_type = dataset.schema.field("embedding").type
    assert pa.types.is_fixed_size_list(embedding_type), f"{name}: embedding type"
    assert embedding_type.list_size == DIM, f"{name}: embedding dimension"
    assert embedding_type.value_type == pa.float32(), f"{name}: embedding element type"
    for field in dataset.schema:
        assert not field.nullable, f"{name}: column {field.name} must be NOT NULL"

    # The regression goldens are hand-checkable only because embedding[j] = (row_id-1)+j,
    # which makes the exact squared L2 distance between rows r and n equal 16*(n-r)^2. Every
    # distance in the .out files and every comment in the suites encodes that ladder, so
    # assert it against a flat scan rather than trusting the row/dimension counts above: a
    # change to the data shape would otherwise leave the self-check green and surface only
    # as an opaque golden diff after a full docker regression run.
    ladder = [(row, 16.0 * step * step) for step, row in enumerate(range(1, 11))]
    assert topk(dataset, HEAD_QUERY, 10, use_index=False) == ladder, (
        f"{name}: flat top-10 from row 1 is not the 16*(n-r)^2 ladder; the fixture data "
        "shape changed and every dependent golden and comment is now stale"
    )

    indices = dataset.list_indices()
    assert len(indices) == 1, f"{name}: expected exactly one index"
    index = indices[0]
    assert index["name"] == index_name_of(name), f"{name}: index name {index['name']}"
    assert index["type"] == index_type, f"{name}: index type {index['type']}"
    indexed_fragments = set(index["fragment_ids"])
    all_fragments = {fragment.fragment_id for fragment in fragments}
    assert indexed_fragments == all_fragments, f"{name}: index does not cover all fragments"

    for query in (HEAD_QUERY, TAIL_QUERY):
        indexed_plan = dataset.scanner(
            nearest={"column": "embedding", "q": query, "k": 5, "nprobes": NUM_PARTITIONS}
        ).explain_plan(True)
        assert "ANNSubIndex" in indexed_plan, f"{name}: indexed plan lacks ANNSubIndex"
        assert "ANNIvfPartition" in indexed_plan, f"{name}: plan lacks ANNIvfPartition"
        flat_plan = dataset.scanner(
            nearest={"column": "embedding", "q": query, "k": 5, "use_index": False}
        ).explain_plan(True)
        assert "ANNSubIndex" not in flat_plan, f"{name}: flat plan uses ANN"
        assert "KNNVectorDistance" in flat_plan, f"{name}: flat plan lacks KNN node"
    return dataset


def check_lossy_results(name: str, dataset) -> None:
    # IVF_PQ stores quantized codes, so agreement with flat search is an observed property
    # of this frozen fixture and the pinned Lance version, never a guarantee. The
    # regression suite therefore queries it with refine_factor, which reranks candidates
    # with exact distances; record what that suite will observe.
    raw = topk(dataset, HEAD_QUERY, 5, use_index=True)
    flat = topk(dataset, HEAD_QUERY, 5, use_index=False)
    assert len(raw) == 5, f"{name}: indexed search must return k rows"
    raw_agreement = "matches" if raw == flat else "differs from"
    print(f"record: {name} full-probe top-5 {raw_agreement} flat search: {raw}")
    refined = topk(dataset, HEAD_QUERY, 5, use_index=True, refine_factor=10)
    refined_agreement = "matches" if refined == flat else "differs from"
    print(f"record: {name} refined top-5 {refined_agreement} flat search: {refined}")


def check_boundary_discriminator(name: str, dataset) -> None:
    # See BOUNDARY_ROW above. The lossy index uses refine_factor so the comparison against
    # flat runs on exact distances, exactly like the regression suite does.
    single_rows = topk(
        dataset, BOUNDARY_QUERY, 10, use_index=True, nprobes=1, refine_factor=10)
    flat_rows = topk(dataset, BOUNDARY_QUERY, 10, use_index=False)
    # Compare distances, not row ids: the boundary query is symmetric, so rows r-d and r+d
    # tie and either may fill the last slot. Only a missed neighbour changes the distances.
    single = [distance for _, distance in single_rows]
    flat = [distance for _, distance in flat_rows]
    assert len(single) == 10, f"{name}: boundary nprobes=1 must still return k rows"
    assert single != flat, (
        f"{name}: row {BOUNDARY_ROW} no longer discriminates nprobes=1 from flat search; "
        "the IVF partition boundaries moved. Update BOUNDARY_ROW here and the boundary "
        "queries in the regression suites together."
    )
    print(f"record: {name} boundary nprobes=1 top-10 rows: "
          f"{[row for row, _ in single_rows]}")
    # The partition edge moves on every retrain, so report where it actually landed. This is
    # the first thing to look at when a boundary golden shifts or this check starts failing.
    probed = sorted(row for row, _ in topk(
        dataset, BOUNDARY_QUERY, ROWS, use_index=True, nprobes=1, refine_factor=1))
    contiguous = probed == list(range(probed[0], probed[-1] + 1))
    print(f"record: {name} partition holding row {BOUNDARY_ROW}: rows "
          f"{probed[0]}-{probed[-1]} ({len(probed)} rows, contiguous={contiguous})")


def check_catalog(root: Path) -> None:
    namespace = lance_namespace.connect("dir", {"root": str(root)})
    tables = namespace.list_tables(ListTablesRequest(id=[NAMESPACE]))
    assert sorted(tables.tables) == sorted(VECTOR_TABLES), (
        f"unexpected {NAMESPACE} tables: {tables.tables}"
    )
    root_tables = namespace.list_tables(ListTablesRequest(id=[]))
    assert "all_types" in root_tables.tables, "all_types is not registered at the root"
    all_types = namespace.describe_table(DescribeTableRequest(id=["all_types"]))
    all_types_path = Path(all_types.location.removeprefix("file://"))
    assert all_types_path.is_dir(), f"all_types location missing: {all_types.location}"
    # all_types.lance is copied through verbatim and cannot be regenerated by this script, so
    # open it rather than only stat it: the rebuild path below deletes the previous fixture,
    # which is its only copy. 12 rows is what test_lance_catalog_all_types.out records.
    assert lance.dataset(str(all_types_path)).count_rows() == 12, (
        "all_types.lance did not survive the copy intact"
    )

    manifest = lance.dataset(str(root / MANIFEST_DIR))
    manifest_indices = {index["name"] for index in manifest.list_indices()}
    for required in ("object_id_btree", "object_type_bitmap", "base_objects_label_list"):
        assert required in manifest_indices, f"__manifest lacks index {required}"
    hint = json.loads(
        (root / MANIFEST_DIR / "_versions" / "latest_version_hint.json").read_text()
    )
    assert hint["version"] == manifest.version, (
        f"latest_version_hint {hint['version']} does not match manifest version "
        f"{manifest.version}"
    )

    for table_name, spec in VECTOR_TABLES.items():
        described = namespace.describe_table(
            DescribeTableRequest(id=[NAMESPACE, table_name])
        )
        path = Path(described.location.removeprefix("file://"))
        assert path.is_dir(), f"{table_name} location missing: {described.location}"
        dataset = check_vector_dataset(table_name, described.location, spec["index_type"])
        check_lossy_results(table_name, dataset)
        check_boundary_discriminator(table_name, dataset)
    print(f"self-check OK: {root}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "preinstalled_data" / "lance",
        help="fixture directory to rebuild (default: scripts/preinstalled_data/lance)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="only run the self-check against the existing fixture",
    )
    args = parser.parse_args()
    output: Path = args.output

    # Every verification in this script is an assert, and the self-check is the whole
    # contract for a fixture whose bytes are not reproducible. Under -O the rebuild
    # below would replace the committed fixture having verified nothing at all.
    if not __debug__:
        print("refusing to run with assertions disabled (python -O)", file=sys.stderr)
        return 1

    if args.check:
        check_catalog(output)
        return 0

    all_types_source = output / ALL_TYPES_DIR
    if not all_types_source.is_dir():
        print(f"missing all_types source: {all_types_source}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="lance_fixture_") as staging_name:
        staging = Path(staging_name) / "lance"
        staging.mkdir()
        build(staging, all_types_source)
        check_catalog(staging)
        backup = output.with_name(output.name + ".old")
        if backup.exists():
            shutil.rmtree(backup)
        output.rename(backup)
        shutil.move(str(staging), str(output))
        shutil.rmtree(backup)
    check_catalog(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
