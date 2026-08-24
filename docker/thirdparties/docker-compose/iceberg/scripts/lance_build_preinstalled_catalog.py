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
    directories), listed in VECTOR_TABLES below, plus NESTED_TABLE, a nested-field scalar
    index fixture used by the SHOW INDEX / index-inspection suites.

Every vector table holds identical deterministic data: 1024 rows in two 512-row fragments,
16-dim Float32 `embedding` where embedding[j] = (row_id - 1) + j. For a query equal to the
vector of row r, the exact squared L2 distance of row n is 16 * (n - r)^2, so head/tail
queries have no distance ties. All values are integers below 2^11 and therefore exact in
Float32.

Environment: python3 with the exact pins in lance_fixture_requirements.txt. Index training
(IVF kmeans, HNSW graph construction) is not bit-reproducible across runs, so regenerating
this fixture changes the binary output and requires regenerating the dependent regression
.out files. Reproducible properties are asserted by the self-check below instead: logical
metadata, the exact L2 distance ladder the regression goldens encode, plan shape, the
partition-boundary discriminator, and - for the graph indexes - the ef discriminator.
Indexed-vs-flat agreement is an algorithm guarantee only for IVF_FLAT; every quantizing or
graph-traversing index has its behaviour recorded rather than asserted.

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
# HNSW graph parameters, likewise chosen for a stable small fixture rather than for
# recall. 8-bit PQ under a graph index: the 4-bit codebook above is only wide enough for
# the flat IVF variant, and IVF_HNSW_PQ needs the extra precision to keep its graph
# neighbourhoods meaningful on 1024 rows.
HNSW_BUILD_PARAMS = {"max_level": 7, "m": 20, "ef_construction": 100}
HNSW_PQ_BUILD_PARAMS = {**HNSW_BUILD_PARAMS, "num_sub_vectors": 4, "num_bits": 8}
# Graph search needs an explicit candidate width, and Lance requires ef >= k when it
# reranks, so a query with refine_factor really needs ef >= top_k * refine_factor. Every
# self-check and regression query on a graph index carries this, and an ef that is too
# small is a Lance error ("ef must be greater than or equal to k"), not a silent
# degradation - the regression suite pins that error too.
HNSW_SEARCH_PARAMS = {"ef": 100}

# One table per ANN algorithm and element type, identical data, exactly one index named
# embedding_<the table name without its vs_ prefix>. Naming is vs_<algorithm>_<element
# type>, so one table is exactly one cell of the algorithm x element type matrix and a
# missing combination is visible from the table list alone. The build loop and the
# self-check are driven entirely by these specs; follow-up work for #66495 adds the
# remaining element types and distance metrics here.
#
# `exact` marks the one algorithm whose full-partition probe is guaranteed to reproduce
# the flat search: IVF_FLAT stores the original vectors, so probing every partition is an
# exhaustive scan by another name. Everything else either quantizes the vectors (PQ, SQ)
# or reaches candidates through a graph that may miss neighbours (IVF_HNSW_*), and for
# those, agreement with flat search is only ever recorded - never asserted.
# `search` carries the per-query parameters an algorithm cannot be searched without.
VECTOR_TABLES = {
    "vs_ivf_flat_f32": {"index_type": "IVF_FLAT", "params": {}, "exact": True},
    "vs_ivf_pq_f32": {"index_type": "IVF_PQ", "params": PQ_BUILD_PARAMS},
    "vs_ivf_sq_f32": {"index_type": "IVF_SQ", "params": {}},
    "vs_ivf_hnsw_flat_f32": {
        "index_type": "IVF_HNSW_FLAT",
        "params": HNSW_BUILD_PARAMS,
        "search": HNSW_SEARCH_PARAMS,
    },
    "vs_ivf_hnsw_sq_f32": {
        "index_type": "IVF_HNSW_SQ",
        "params": HNSW_BUILD_PARAMS,
        "search": HNSW_SEARCH_PARAMS,
        # The one graph index whose traversal demonstrably reacts to ef on this data; see
        # check_ef_discriminator. The regression suite pins ef against this same table.
        "ef_discriminator": True,
    },
    "vs_ivf_hnsw_pq_f32": {
        "index_type": "IVF_HNSW_PQ",
        "params": HNSW_PQ_BUILD_PARAMS,
        "search": HNSW_SEARCH_PARAMS,
    },
}

# Nested-field scalar index fixture for #66497's SHOW INDEX / lance_index_entries suites.
# The indexed child name deliberately contains a dot so the canonical field path can only
# be written with backtick quoting; the table also carries a __lance_frag_reuse system
# entry (deferred-remap compaction) so every inspection surface proves it filters reserved
# system indexes instead of bricking on them.
NESTED_TABLE = "nested_index"
NESTED_INDEX_NAME = "nested_label_btree"
NESTED_COLUMN = "attributes.`child.with.dot`"
NESTED_ROWS = 16

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
# The boundary query is symmetric - rows 256-d and 256+d are equidistant - so a top-k that
# lands mid-pair would pin an arbitrary choice of tie winner in the goldens. 9 is the last
# cut that ends on a complete pair. This is the regression contract, so the self-check below
# has to probe at exactly this k: checking 10 here while the suites query 9 would let a
# retrained index pass the generator and fail the suites.
BOUNDARY_TOP_K = 9
# Graph candidate widths for the ef discriminator: with a narrow ef the traversal has to
# settle for worse candidates than with a wide one. Same purpose as nprobes=1 for IVF - it
# proves the parameter reached the index instead of being quietly dropped. Measured on this
# fixture: a query at row 512, in the middle of the data, loses a true nearest neighbour at
# ef=5 that ef=50 finds, on every graph index that reacts to ef at all. The endpoint queries
# do not work here - row 1 sits where greedy traversal already lands on the exact answer.
EF_DISCRIMINATOR_ROW = 512
EF_QUERY = [float(EF_DISCRIMINATOR_ROW - 1 + j) for j in range(DIM)]
EF_TOP_K = 5
EF_NARROW = 5
EF_WIDE = 50


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


def make_nested_fragment_table(row_offset_start: int, row_offset_end: int) -> pa.Table:
    offsets = list(range(row_offset_start, row_offset_end))
    attributes = pa.StructArray.from_arrays(
        [
            pa.array(["even" if offset % 2 == 0 else "odd" for offset in offsets]),
            pa.array([f"item-{offset + 1:04d}" for offset in offsets]),
        ],
        fields=[
            pa.field("source", pa.string(), nullable=False),
            pa.field("child.with.dot", pa.string(), nullable=False),
        ],
    )
    table = pa.Table.from_arrays(
        [
            pa.array([offset + 1 for offset in offsets], type=pa.int64()),
            attributes,
        ],
        schema=pa.schema(
            [
                pa.field("row_id", pa.int64(), nullable=False),
                pa.field(
                    "attributes",
                    pa.struct(list(attributes.type)),
                    nullable=False,
                ),
            ]
        ),
    )
    return table


def create_nested_index_table(namespace) -> str:
    first = make_nested_fragment_table(0, NESTED_ROWS // 2)
    buffer = io.BytesIO()
    with ipc.new_stream(buffer, first.schema) as writer:
        writer.write_table(first)
    response = namespace.create_table(
        CreateTableRequest(id=[NAMESPACE, NESTED_TABLE]), buffer.getvalue()
    )
    location = response.location
    lance.write_dataset(
        make_nested_fragment_table(NESTED_ROWS // 2, NESTED_ROWS), location, mode="append"
    )
    dataset = lance.dataset(location)
    # Same physical-dataset indexing detour as the vector tables: the Directory namespace
    # does not implement create_table_index. The dotted child name only resolves with
    # backtick quoting (NESTED_COLUMN); a plain dotted path raises KeyError.
    dataset.create_scalar_index(NESTED_COLUMN, "BTREE", name=NESTED_INDEX_NAME)
    # Deferred-remap compaction merges the two fragments but leaves a reserved
    # __lance_frag_reuse system index entry behind, which is exactly what the inspection
    # surfaces must learn to skip.
    lance.dataset(location).optimize.compact_files(
        target_rows_per_fragment=NESTED_ROWS, defer_index_remap=True
    )
    return location


def compact_manifest(root: Path) -> None:
    # Every namespace mutation above leaves a manifest fragment, index delta, and version
    # behind. Fold them together so the committed fixture stays small and reviewable. Only
    # the manifest is compacted: the vector tables keep exactly two fragments, and
    # nested_index keeps its deferred-remap system entry.
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
    create_nested_index_table(namespace)
    compact_manifest(root)


def topk(dataset, query, k: int, use_index: bool, **nearest_kwargs):
    nearest = {"column": "embedding", "q": query, "k": k, "use_index": use_index}
    if use_index:
        nearest.setdefault("nprobes", NUM_PARTITIONS)
    nearest.update(nearest_kwargs)
    table = dataset.scanner(nearest=nearest).to_table()
    return list(zip(table["row_id"].to_pylist(), table["_distance"].to_pylist()))


def search_params_of(spec: dict) -> dict:
    """Per-query parameters this algorithm cannot be searched without (HNSW's ef)."""
    return dict(spec.get("search", {}))


def check_vector_dataset(name: str, location: str, index_type: str, search: dict):
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
            nearest={"column": "embedding", "q": query, "k": 5,
                     "nprobes": NUM_PARTITIONS, **search}
        ).explain_plan(True)
        assert "ANNSubIndex" in indexed_plan, f"{name}: indexed plan lacks ANNSubIndex"
        assert "ANNIvfPartition" in indexed_plan, f"{name}: plan lacks ANNIvfPartition"
        flat_plan = dataset.scanner(
            nearest={"column": "embedding", "q": query, "k": 5, "use_index": False}
        ).explain_plan(True)
        assert "ANNSubIndex" not in flat_plan, f"{name}: flat plan uses ANN"
        assert "KNNVectorDistance" in flat_plan, f"{name}: flat plan lacks KNN node"
    return dataset


def check_exact_results(name: str, dataset, search: dict) -> None:
    # IVF_FLAT keeps the original vectors, so probing every partition visits every row with
    # its true distance: equality with the flat search is the algorithm's guarantee, not a
    # property of this fixture, and it needs no refine_factor to hold. The regression suite
    # asserts the same thing against Doris, which is what turns "the query returned rows"
    # into "the query returned the right rows".
    for query, nearest_row in ((HEAD_QUERY, 1), (TAIL_QUERY, ROWS)):
        indexed = topk(dataset, query, 10, use_index=True, **search)
        flat = topk(dataset, query, 10, use_index=False)
        assert indexed == flat, (
            f"{name}: full-probe top-10 differs from flat search, which IVF_FLAT guarantees "
            f"it cannot: indexed={indexed} flat={flat}"
        )
        assert indexed[0] == (nearest_row, 0.0), (
            f"{name}: nearest row is {indexed[0]}, expected row {nearest_row} at distance 0"
        )
        distances = [distance for _, distance in indexed]
        assert distances == [16.0 * step * step for step in range(10)], (
            f"{name}: indexed distances are not the 16*(n-r)^2 ladder: {distances}"
        )
    print(f"record: {name} full-probe top-10 equals flat search at both endpoints")


def check_lossy_results(name: str, dataset, search: dict) -> None:
    # Quantizing (PQ, SQ) and graph (HNSW) indexes do not promise the flat result:
    # agreement is an observed property of this frozen fixture and the pinned Lance
    # version, never a guarantee. The regression suites therefore query them with
    # refine_factor, which reranks candidates with exact distances; record what those
    # suites will observe.
    raw = topk(dataset, HEAD_QUERY, 5, use_index=True, **search)
    flat = topk(dataset, HEAD_QUERY, 5, use_index=False)
    assert len(raw) == 5, f"{name}: indexed search must return k rows"
    raw_agreement = "matches" if raw == flat else "differs from"
    print(f"record: {name} full-probe top-5 {raw_agreement} flat search: {raw}")
    refined = topk(dataset, HEAD_QUERY, 5, use_index=True, refine_factor=10, **search)
    refined_agreement = "matches" if refined == flat else "differs from"
    print(f"record: {name} refined top-5 {refined_agreement} flat search: {refined}")


def check_boundary_discriminator(name: str, dataset, search: dict) -> None:
    # See BOUNDARY_ROW above. The lossy index uses refine_factor so the comparison against
    # flat runs on exact distances, exactly like the regression suite does.
    single_rows = topk(
        dataset, BOUNDARY_QUERY, BOUNDARY_TOP_K, use_index=True, nprobes=1,
        refine_factor=10, **search)
    flat_rows = topk(dataset, BOUNDARY_QUERY, BOUNDARY_TOP_K, use_index=False)
    # Compare distances, not row ids: the boundary query is symmetric, so rows r-d and r+d
    # tie and either may fill the last slot. Only a missed neighbour changes the distances.
    single = [distance for _, distance in single_rows]
    flat = [distance for _, distance in flat_rows]
    assert len(single) == BOUNDARY_TOP_K, (
        f"{name}: boundary nprobes=1 must still return k rows"
    )
    assert single != flat, (
        f"{name}: row {BOUNDARY_ROW} no longer discriminates nprobes=1 from flat search; "
        "the IVF partition boundaries moved. Update BOUNDARY_ROW here and the boundary "
        "queries in the regression suites together."
    )
    print(f"record: {name} boundary nprobes=1 top-{BOUNDARY_TOP_K} rows "
          f"{[row for row, _ in single_rows]} vs flat rows {[row for row, _ in flat_rows]}")
    # The partition edge moves on every retrain, so report where it actually landed. This is
    # the first thing to look at when a boundary golden shifts or this check starts failing.
    partition_search = dict(search)
    if "ef" in partition_search:
        # This diagnostic asks for every row the probed partition can return, and Lance
        # rejects a graph search whose candidate width is narrower than k.
        partition_search["ef"] = ROWS
    probed = sorted(row for row, _ in topk(
        dataset, BOUNDARY_QUERY, ROWS, use_index=True, nprobes=1, refine_factor=1,
        **partition_search))
    contiguous = probed == list(range(probed[0], probed[-1] + 1))
    print(f"record: {name} partition holding row {BOUNDARY_ROW}: rows "
          f"{probed[0]}-{probed[-1]} ({len(probed)} rows, contiguous={contiguous})")


def check_ef_discriminator(name: str, dataset, assert_it: bool) -> None:
    # The graph counterpart of the nprobes discriminator: a narrow candidate width has to
    # settle for worse neighbours than a wide one. Without it, a backend that dropped ef on
    # the floor would still return plausible rows. Both queries probe every partition, so
    # only ef can explain a difference. No refine_factor: reranking with exact distances is
    # exactly what would hide the effect being measured.
    #
    # Whether ef changes the answer at all depends on the index, not just on the parameter:
    # measured on this fixture, IVF_HNSW_SQ loses a true neighbour at ef=5 while
    # IVF_HNSW_FLAT and IVF_HNSW_PQ still return the exact rows - their traversal simply
    # does not need the extra candidates on 1024 collinear vectors. So the discriminator is
    # asserted where it holds and recorded everywhere else; `assert_it` is the spec flag,
    # and the regression suite must query the same table this asserts on.
    narrow = topk(dataset, EF_QUERY, EF_TOP_K, use_index=True, ef=EF_NARROW)
    wide = topk(dataset, EF_QUERY, EF_TOP_K, use_index=True, ef=EF_WIDE)
    # Compare distances rather than row ids: rows 512-d and 512+d tie, so the row order
    # inside a tie group is arbitrary while a missed neighbour always changes a distance.
    differs = [d for _, d in narrow] != [d for _, d in wide]
    if assert_it:
        assert differs, (
            f"{name}: ef={EF_NARROW} and ef={EF_WIDE} return the same distances, so the "
            "regression suite can no longer prove ef reached the index. The retrained graph "
            f"made the narrow search good enough at row {EF_DISCRIMINATOR_ROW}; find a row "
            "that still discriminates and update the suite's ef queries together with this."
        )
    print(f"record: {name} ef={EF_NARROW} rows {[row for row, _ in narrow]} vs ef={EF_WIDE} "
          f"rows {[row for row, _ in wide]} (differs={differs}, asserted={assert_it})")


def check_nested_dataset(location: str):
    dataset = lance.dataset(location)
    assert dataset.count_rows() == NESTED_ROWS, f"{NESTED_TABLE}: expected {NESTED_ROWS} rows"
    fragments = dataset.get_fragments()
    assert len(fragments) == 1, f"{NESTED_TABLE}: deferred compaction must leave 1 fragment"
    schema = dataset.schema
    assert schema.field("row_id").type == pa.int64(), f"{NESTED_TABLE}: row_id type"
    attributes = schema.field("attributes")
    assert pa.types.is_struct(attributes.type), f"{NESTED_TABLE}: attributes type"
    child_names = [field.name for field in attributes.type]
    assert child_names == ["source", "child.with.dot"], (
        f"{NESTED_TABLE}: attributes children {child_names}"
    )
    indices = {index["name"]: index["type"] for index in dataset.list_indices()}
    assert indices.get(NESTED_INDEX_NAME) == "BTree", (
        f"{NESTED_TABLE}: missing BTREE {NESTED_INDEX_NAME}: {indices}"
    )
    # The reserved system entry is part of the contract: the Doris FE must filter it out of
    # SHOW INDEX and lance_index_entries instead of failing the whole read on it.
    assert "__lance_frag_reuse" in indices, (
        f"{NESTED_TABLE}: deferred-remap compaction left no __lance_frag_reuse: {indices}"
    )
    # The BTREE must be usable, not just present: one exact-match lookup through it.
    probe = (
        dataset.scanner(filter="attributes.`child.with.dot` = 'item-0007'")
        .to_table()
        .column("row_id")
        .to_pylist()
    )
    assert probe == [7], f"{NESTED_TABLE}: BTREE probe returned {probe}"


def check_catalog(root: Path) -> None:
    namespace = lance_namespace.connect("dir", {"root": str(root)})
    tables = namespace.list_tables(ListTablesRequest(id=[NAMESPACE]))
    expected_tables = sorted(list(VECTOR_TABLES) + [NESTED_TABLE])
    assert sorted(tables.tables) == expected_tables, (
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
        search = search_params_of(spec)
        dataset = check_vector_dataset(
            table_name, described.location, spec["index_type"], search
        )
        if spec.get("exact"):
            check_exact_results(table_name, dataset, search)
        else:
            check_lossy_results(table_name, dataset, search)
        check_boundary_discriminator(table_name, dataset, search)
        if search.get("ef"):
            check_ef_discriminator(table_name, dataset, spec.get("ef_discriminator", False))

    nested = namespace.describe_table(DescribeTableRequest(id=[NAMESPACE, NESTED_TABLE]))
    nested_path = Path(nested.location.removeprefix("file://"))
    assert nested_path.is_dir(), f"{NESTED_TABLE} location missing: {nested.location}"
    check_nested_dataset(nested.location)
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
