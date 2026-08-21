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
import math
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

# The boundary query is symmetric for the ladder profiles - rows r-d and r+d are
# equidistant - so a top-k that lands mid-pair would pin an arbitrary choice of tie winner
# in the goldens. 9 is the last cut that ends on a complete pair. This is the regression
# contract, so the self-check probes at exactly this k: checking 10 here while the suites
# query 9 would let a retrained index pass the generator and fail the suites.
BOUNDARY_TOP_K = 9
# Graph candidate widths for the ef discriminator: with a narrow ef the traversal has to
# settle for worse candidates than with a wide one. Same purpose as nprobes=1 for IVF - it
# proves the parameter reached the index instead of being quietly dropped.
EF_DISCRIMINATOR_ROW = 518
EF_TOP_K = 5
EF_NARROW = 5
EF_WIDE = 50


# ---------------------------------------------------------------------------
# Vector data profiles
# ---------------------------------------------------------------------------
# A profile is the vector data a table stores, together with the query vectors and the
# closed-form distance ladder that make its goldens hand-checkable. Element type and metric
# are per table; the data shape is not, because a shape that discriminates under one metric
# can be degenerate under another. Measured on the collinear data below with pylance 7.0.0:
#
#   dot     every query returns the same top-9 (the highest-norm rows), because dot(q, v_r)
#           grows linearly in r for any positive query - the answer ignores the query.
#   cosine  all row directions converge on the all-ones vector, so at rows 250+ the top-9
#           distances are all 0.0 and the ranking is arbitrary tie-breaking.
#
# So L2 tables keep the collinear ladder, cosine and dot tables use DIRECTIONAL, and the
# uint8 table uses BINARY. Every profile is closed-form: no library RNG, whose stream can
# change under us between versions.


class DataProfile:
    """The data shape of a vector table, and the queries its goldens are built from."""

    def __init__(self, name, dim, dtype, vector, ladder=None, self_match=True,
                 boundary_row=256):
        self.name = name
        self.dim = dim
        self.dtype = dtype
        self.vector = vector
        # The row whose true neighbourhood straddles an IVF partition edge, so that a real
        # nprobes=1 probe must miss part of it while a silent flat fallback returns the flat
        # answer. Which rows have this property is decided by kmeans and moves on every
        # retrain, so it is asserted per table rather than assumed; see
        # check_boundary_discriminator.
        self.boundary_row = boundary_row
        # ladder(step) -> the exact distance between two rows `step` apart, when the data
        # shape has a closed form for it. None where no such form exists, in which case the
        # self-check verifies ordering properties instead of exact distances.
        self.ladder = ladder
        # dot is not a proper metric: the nearest row to a query is not necessarily the row
        # the query was taken from, so the "distance 0 to itself" check does not apply.
        self.self_match = self_match
        # A ladder profile is symmetric: rows r-d and r+d tie, so either may fill the last
        # slot of a top-k and only the distances are stable enough to compare. The
        # directional profile has no ladder and was verified to have no ties at all, so
        # there the row ids are the stable thing and the distances carry float noise.
        self.symmetric_ties = ladder is not None
        self.head_query = vector(0)
        self.tail_query = vector(ROWS - 1)

    def query_of(self, row):
        """The stored vector of a 1-based row id."""
        return self.vector(row - 1)


# Row r holds (r, r+1, ..., r+15): the exact squared L2 distance between rows r and n is
# 16*(n-r)^2, which is what every L2 golden and suite comment encodes.
def _collinear_vector(r):
    return [float(r + j) for j in range(DIM)]


# Directions from an irrational rotation - frac(sqrt(prime)) per dimension - so they spread
# quasi-uniformly instead of converging, and an independently varying norm so that cosine
# and dot cannot rank the same way. Verified on this data: l2, cosine and dot each return a
# different top-9 at every probe query, and no top-9 has a tie, in float32 and in float16.
# That is what lets a suite tell a respected metric from an ignored one.
_DIRECTION_ALPHA = [math.sqrt(p) % 1.0 for p in
                    (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53)]
_NORM_ALPHA = math.sqrt(59) % 1.0


def _directional_vector(r):
    raw = [math.sin(2.0 * math.pi * (((r + 1) * alpha) % 1.0)) for alpha in _DIRECTION_ALPHA]
    length = math.sqrt(sum(x * x for x in raw)) or 1.0
    scale = 1.0 + 3.0 * (((r + 1) * _NORM_ALPHA) % 1.0)
    # Round so the stored values stay short and identical across platforms, and small
    # enough that float16 reproduces the float32 row ordering exactly.
    return [round(x / length * scale, 4) for x in raw]


# Lance reads uint8 vectors as binary vectors and counts hamming in BITS, not bytes
# (measured: one byte differing by one bit is distance 1.0, by eight bits is 8.0). A
# thermometer code - row r sets its first r bits - gives hamming(a, b) == |a - b|, the same
# symmetric ladder the collinear data produces under L2, so BOUNDARY_TOP_K still ends on a
# complete pair. It needs one bit per row, hence 128 bytes for 1024 rows: this is the one
# profile whose dimension differs from DIM, and it has to, because a pseudo-random byte
# pattern puts every distance in a narrow band around 64 where the top-k ties are unstable.
BINARY_DIM = 128


def _binary_vector(r):
    return [(1 << max(0, min(8, r - j * 8))) - 1 for j in range(BINARY_DIM)]


COLLINEAR = DataProfile(
    "collinear", DIM, pa.float32(), _collinear_vector,
    ladder=lambda step: 16.0 * step * step)
DIRECTIONAL = DataProfile("directional", DIM, pa.float32(), _directional_vector)
# Measured on the thermometer data: the four partitions come out as contiguous row ranges
# split near 256, 512 and 768, and only rows within about four of an edge discriminate - on
# the committed index those are 255-262, 513-520 and 767-774. Which of them holds moves on
# every retrain, so check_boundary_discriminator reports the current ones when this breaks.
BINARY = DataProfile(
    "binary", BINARY_DIM, pa.uint8(), _binary_vector, ladder=lambda step: float(step),
    boundary_row=513)

# One table per matrix cell: algorithm x element type x metric, identical data within a
# profile, exactly one index named embedding_<the table name without its vs_ prefix>.
# Naming is vs_<algorithm>_<element type>[_<metric>], L2 keeping the suffix-free form, so a
# missing combination is visible from the table list alone. The build loop and the
# self-check are driven entirely by these specs.
#
# `exact` marks the algorithm whose full-partition probe is guaranteed to reproduce the flat
# search: IVF_FLAT stores the original vectors, so probing every partition is an exhaustive
# scan by another name. Everything else either quantizes the vectors (PQ, SQ) or reaches
# candidates through a graph that may miss neighbours (IVF_HNSW_*), and for those, agreement
# with flat search is only ever recorded - never asserted.
# `search` carries the per-query parameters an algorithm cannot be searched without.
# `element_type` and `metric` default to the Float32 + L2 cells that came first.
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
    # Metric coverage. Doris only plans an indexed split when the query metric equals the
    # index metric, so cosine and dot each need an index built with that metric; the L2
    # tables above cannot stand in for them.
    "vs_ivf_flat_f32_cosine": {
        "index_type": "IVF_FLAT", "params": {}, "exact": True,
        "metric": "cosine", "profile": DIRECTIONAL,
    },
    "vs_ivf_pq_f32_cosine": {
        "index_type": "IVF_PQ", "params": PQ_BUILD_PARAMS,
        "metric": "cosine", "profile": DIRECTIONAL,
    },
    "vs_ivf_pq_f32_dot": {
        "index_type": "IVF_PQ", "params": PQ_BUILD_PARAMS,
        "metric": "dot", "profile": DIRECTIONAL,
    },
    # Element type coverage, one indexed table each. Float16 uses cosine because building a
    # float16 L2 index does not complete in the embedded Lance version, and uint8 uses
    # hamming with IVF_FLAT because that is the only index type Lance accepts for it.
    "vs_ivf_flat_f64": {
        "index_type": "IVF_FLAT", "params": {}, "exact": True,
        "element_type": pa.float64(),
    },
    "vs_ivf_flat_f16_cosine": {
        "index_type": "IVF_FLAT", "params": {}, "exact": True,
        "element_type": pa.float16(), "metric": "cosine", "profile": DIRECTIONAL,
    },
    "vs_ivf_flat_u8": {
        "index_type": "IVF_FLAT", "params": {}, "exact": True,
        "metric": "hamming", "profile": BINARY,
    },
}


def profile_of(spec: dict) -> DataProfile:
    return spec.get("profile", COLLINEAR)


def element_type_of(spec: dict):
    return spec.get("element_type", profile_of(spec).dtype)


def metric_of(spec: dict) -> str:
    return spec.get("metric", "l2")


def distance_tolerance_of(spec: dict):
    """How far the indexed and flat distances may drift apart for this element type.

    The two paths accumulate in a different order, which is exact for the integer L2 and
    hamming ladders but not for cosine and dot. The drift is bounded by the element type's
    precision: about 6e-8 relative for float32, and about 5e-4 for float16's 11-bit
    mantissa, so the tolerance has to follow the type rather than be one global number.
    """
    if element_type_of(spec) == pa.float16():
        return 2e-3, 1e-5
    return 1e-5, 1e-6


def make_fragment_table(profile: DataProfile, element_type,
                        row_offset_start: int, row_offset_end: int) -> pa.Table:
    offsets = list(range(row_offset_start, row_offset_end))
    embedding = pa.FixedSizeListArray.from_arrays(
        pa.array(
            [value for offset in offsets for value in profile.vector(offset)],
            type=element_type,
        ),
        profile.dim,
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


def create_vector_table(namespace, table_name: str, spec: dict) -> str:
    profile = profile_of(spec)
    element_type = element_type_of(spec)
    first = make_fragment_table(profile, element_type, 0, FRAGMENT_ROWS)
    buffer = io.BytesIO()
    with ipc.new_stream(buffer, first.schema) as writer:
        writer.write_table(first)
    response = namespace.create_table(
        CreateTableRequest(id=[NAMESPACE, table_name]), buffer.getvalue()
    )
    # Never predict the hashed storage path; always use the location the namespace returns.
    location = response.location
    lance.write_dataset(
        make_fragment_table(profile, element_type, FRAGMENT_ROWS, ROWS),
        location, mode="append")
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
    # The pinned writer maintains the optional latest-version hint itself, but the version
    # cleanup and the index-directory pruning above both run after it last did. Rewrite it
    # from the reopened manifest so it names the version that actually survived; check_catalog
    # asserts the two agree, because a hint pointing at a deleted version would send every
    # reader to a manifest that is no longer there.
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
        location = create_vector_table(namespace, table_name, spec)
        # DirectoryNamespace.create_table_index exists but raises UnsupportedOperationError,
        # so open the physical dataset at the location the namespace returned and index it
        # there. index_file_version V3 is what the Doris BE reads through lance-c; the
        # default would produce an index the backend cannot open.
        lance.dataset(location).create_index(
            "embedding",
            spec["index_type"],
            name=index_name_of(table_name),
            metric=metric_of(spec),
            num_partitions=NUM_PARTITIONS,
            sample_rate=256,
            index_file_version="V3",
            **spec["params"],
        )
    compact_manifest(root)


def topk(dataset, query, k: int, use_index: bool, metric: str = "l2", **nearest_kwargs):
    # The metric is never left implicit. Lance defaults to L2, and asking an index built
    # with another metric for an L2 search is a silent fall back to brute force, not an
    # error - exactly the failure this fixture exists to detect.
    nearest = {"column": "embedding", "q": query, "k": k, "use_index": use_index,
               "metric": metric}
    if use_index:
        nearest.setdefault("nprobes", NUM_PARTITIONS)
    nearest.update(nearest_kwargs)
    table = dataset.scanner(nearest=nearest).to_table()
    return list(zip(table["row_id"].to_pylist(), table["_distance"].to_pylist()))


def search_params_of(spec: dict) -> dict:
    """Per-query parameters this algorithm cannot be searched without (HNSW's ef)."""
    return dict(spec.get("search", {}))


def check_vector_dataset(name: str, location: str, spec: dict, search: dict):
    profile = profile_of(spec)
    element_type = element_type_of(spec)
    metric = metric_of(spec)
    index_type = spec["index_type"]
    dataset = lance.dataset(location)
    assert dataset.count_rows() == ROWS, f"{name}: expected {ROWS} rows"
    fragments = dataset.get_fragments()
    assert len(fragments) == 2, f"{name}: expected 2 fragments"
    embedding_type = dataset.schema.field("embedding").type
    assert pa.types.is_fixed_size_list(embedding_type), f"{name}: embedding type"
    assert embedding_type.list_size == profile.dim, f"{name}: embedding dimension"
    assert embedding_type.value_type == element_type, f"{name}: embedding element type"
    for field in dataset.schema:
        assert not field.nullable, f"{name}: column {field.name} must be NOT NULL"

    # The regression goldens are hand-checkable only because the data shape has a closed
    # form: the collinear profile makes the exact squared L2 distance between rows r and n
    # equal 16*(n-r)^2, and the binary profile makes hamming equal |n-r|. Every distance in
    # those .out files and every comment in the suites encodes the ladder, so assert it
    # against a flat scan rather than trusting the row/dimension counts above: a change to
    # the data shape would otherwise leave the self-check green and surface only as an
    # opaque golden diff after a full docker regression run.
    head = topk(dataset, profile.head_query, 10, use_index=False, metric=metric)
    if profile.ladder is not None:
        expected = [(row, profile.ladder(step)) for step, row in enumerate(range(1, 11))]
        assert head == expected, (
            f"{name}: flat top-10 from row 1 is not the {profile.name} distance ladder; "
            "the fixture data shape changed and every dependent golden and comment is now "
            f"stale: {head}"
        )
    else:
        # No closed form for this profile, so pin the property the goldens actually need
        # instead: strictly increasing distances, i.e. no tie can reorder the top-10 when
        # the index is retrained or a different Lance version reads the fixture.
        distances = [distance for _, distance in head]
        assert all(a < b for a, b in zip(distances, distances[1:])), (
            f"{name}: flat top-10 has tied distances, so the golden row order is arbitrary "
            f"and will not survive a rebuild: {head}"
        )

    indices = dataset.list_indices()
    assert len(indices) == 1, f"{name}: expected exactly one index"
    index = indices[0]
    assert index["name"] == index_name_of(name), f"{name}: index name {index['name']}"
    assert index["type"] == index_type, f"{name}: index type {index['type']}"
    indexed_fragments = set(index["fragment_ids"])
    all_fragments = {fragment.fragment_id for fragment in fragments}
    assert indexed_fragments == all_fragments, f"{name}: index does not cover all fragments"

    for query in (profile.head_query, profile.tail_query):
        indexed_plan = dataset.scanner(
            nearest={"column": "embedding", "q": query, "k": 5, "metric": metric,
                     "nprobes": NUM_PARTITIONS, **search}
        ).explain_plan(True)
        assert "ANNSubIndex" in indexed_plan, f"{name}: indexed plan lacks ANNSubIndex"
        assert "ANNIvfPartition" in indexed_plan, f"{name}: plan lacks ANNIvfPartition"
        flat_plan = dataset.scanner(
            nearest={"column": "embedding", "q": query, "k": 5, "metric": metric,
                     "use_index": False}
        ).explain_plan(True)
        assert "ANNSubIndex" not in flat_plan, f"{name}: flat plan uses ANN"
        assert "KNNVectorDistance" in flat_plan, f"{name}: flat plan lacks KNN node"
    return dataset


def check_exact_results(name: str, dataset, spec: dict, search: dict) -> None:
    # IVF_FLAT keeps the original vectors, so probing every partition visits every row with
    # its true distance: equality with the flat search is the algorithm's guarantee, not a
    # property of this fixture, and it needs no refine_factor to hold. The regression suite
    # asserts the same thing against Doris, which is what turns "the query returned rows"
    # into "the query returned the right rows".
    profile = profile_of(spec)
    metric = metric_of(spec)
    rel_tol, abs_tol = distance_tolerance_of(spec)
    for query, nearest_row in ((profile.head_query, 1), (profile.tail_query, ROWS)):
        indexed = topk(dataset, query, 10, use_index=True, metric=metric, **search)
        flat = topk(dataset, query, 10, use_index=False, metric=metric)
        # IVF_FLAT's guarantee is about which rows a full probe visits, so compare row ids
        # exactly. The distances are only compared to a tolerance: the indexed and flat
        # paths accumulate in a different order, which is exact for the integer L2 and
        # hamming ladders but differs in the last ulp or two for cosine and dot.
        assert [row for row, _ in indexed] == [row for row, _ in flat], (
            f"{name}: full-probe top-10 rows differ from flat search, which IVF_FLAT "
            f"guarantees it cannot: indexed={indexed} flat={flat}"
        )
        assert all(math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)
                   for (_, a), (_, b) in zip(indexed, flat)), (
            f"{name}: full-probe top-10 distances differ from flat search by more than "
            f"{element_type_of(spec)} rounding: indexed={indexed} flat={flat}"
        )
        if profile.self_match:
            # The query is a stored row, so that row must come back first at distance zero -
            # to within the element type's rounding, since a cosine distance is computed
            # from normalized values rather than read off the ladder.
            nearest_id, nearest_distance = indexed[0]
            assert nearest_id == nearest_row, (
                f"{name}: nearest row is {indexed[0]}, expected row {nearest_row}"
            )
            assert math.isclose(nearest_distance, 0.0, abs_tol=abs_tol), (
                f"{name}: row {nearest_row} is at distance {nearest_distance}, expected 0"
            )
        distances = [distance for _, distance in indexed]
        if profile.ladder is not None:
            assert distances == [profile.ladder(step) for step in range(10)], (
                f"{name}: indexed distances are not the {profile.name} ladder: {distances}"
            )
    print(f"record: {name} full-probe top-10 equals flat search at both endpoints")


def check_lossy_results(name: str, dataset, spec: dict, search: dict) -> None:
    # Quantizing (PQ, SQ) and graph (HNSW) indexes do not promise the flat result:
    # agreement is an observed property of this frozen fixture and the pinned Lance
    # version, never a guarantee. The regression suites therefore query them with
    # refine_factor, which reranks candidates with exact distances; record what those
    # suites will observe.
    profile = profile_of(spec)
    metric = metric_of(spec)
    raw = topk(dataset, profile.head_query, 5, use_index=True, metric=metric, **search)
    flat = topk(dataset, profile.head_query, 5, use_index=False, metric=metric)
    assert len(raw) == 5, f"{name}: indexed search must return k rows"
    raw_agreement = "matches" if raw == flat else "differs from"
    print(f"record: {name} full-probe top-5 {raw_agreement} flat search: {raw}")
    refined = topk(dataset, profile.head_query, 5, use_index=True, metric=metric,
                   refine_factor=10, **search)
    refined_agreement = "matches" if refined == flat else "differs from"
    print(f"record: {name} refined top-5 {refined_agreement} flat search: {refined}")


def check_boundary_discriminator(name: str, dataset, spec: dict, search: dict) -> None:
    # See DataProfile.boundary_row. The lossy index uses refine_factor so the comparison
    # against flat runs on exact distances, exactly like the regression suite does.
    profile = profile_of(spec)
    metric = metric_of(spec)
    boundary_row = profile.boundary_row
    boundary_query = profile.query_of(boundary_row)
    single_rows = topk(
        dataset, boundary_query, BOUNDARY_TOP_K, use_index=True, metric=metric, nprobes=1,
        refine_factor=10, **search)
    flat_rows = topk(dataset, boundary_query, BOUNDARY_TOP_K, use_index=False, metric=metric)
    # Compare whichever quantity is stable for this profile. On a symmetric ladder rows r-d
    # and r+d tie and either may fill the last slot, so only the distances mean anything. On
    # the tie-free directional profile it is the other way round: the row ids are exact
    # while the distances carry the float noise that would make any two result sets differ.
    if profile.symmetric_ties:
        single = [distance for _, distance in single_rows]
        flat = [distance for _, distance in flat_rows]
    else:
        single = [row for row, _ in single_rows]
        flat = [row for row, _ in flat_rows]
    assert len(single) == BOUNDARY_TOP_K, (
        f"{name}: boundary nprobes=1 must still return k rows"
    )
    if single == flat:
        # Which rows straddle a partition edge is decided by kmeans and moves on every
        # rebuild, so search for the ones that still do rather than leaving the next person
        # to guess. Same shape as the ef discriminator's report below.
        def discriminates(row):
            q = profile.query_of(row)
            probe = topk(dataset, q, BOUNDARY_TOP_K, use_index=True, metric=metric,
                         nprobes=1, refine_factor=10, **search)
            straight = topk(dataset, q, BOUNDARY_TOP_K, use_index=False, metric=metric)
            if profile.symmetric_ties:
                return [d for _, d in probe] != [d for _, d in straight]
            return [r for r, _ in probe] != [r for r, _ in straight]

        candidates = [row for row in range(1, ROWS + 1) if discriminates(row)]
        raise AssertionError(
            f"{name}: row {boundary_row} no longer discriminates nprobes=1 from flat search; "
            "the IVF partition boundaries moved. "
            f"{len(candidates)} rows still discriminate on this build; pick one, set the "
            "profile's boundary_row to it and update the boundary queries in the regression "
            f"suites together with it. Candidates (first 30): {candidates[:30]}"
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
        dataset, boundary_query, ROWS, use_index=True, metric=metric, nprobes=1,
        refine_factor=1, **partition_search))
    contiguous = probed == list(range(probed[0], probed[-1] + 1))
    print(f"record: {name} partition holding row {boundary_row}: rows "
          f"{probed[0]}-{probed[-1]} ({len(probed)} rows, contiguous={contiguous})")


def check_ef_discriminator(name: str, dataset, spec: dict, assert_it: bool) -> None:
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
    profile = profile_of(spec)
    metric = metric_of(spec)
    ef_query = profile.query_of(EF_DISCRIMINATOR_ROW)
    narrow = topk(dataset, ef_query, EF_TOP_K, use_index=True, metric=metric, ef=EF_NARROW)
    wide = topk(dataset, ef_query, EF_TOP_K, use_index=True, metric=metric, ef=EF_WIDE)
    # Compare distances rather than row ids: rows 512-d and 512+d tie, so the row order
    # inside a tie group is arbitrary while a missed neighbour always changes a distance.
    differs = [d for _, d in narrow] != [d for _, d in wide]
    if assert_it and not differs:
        # Which rows react to ef is decided by the graph draw and changes on every rebuild,
        # so do the search here rather than making the next person write it: report the rows
        # that still discriminate on this freshly built index, and they can pin one.
        candidates = [
            row for row in range(1, ROWS + 1)
            if [d for _, d in topk(dataset, profile.query_of(row), EF_TOP_K,
                                   use_index=True, metric=metric, ef=EF_NARROW)]
            != [d for _, d in topk(dataset, profile.query_of(row), EF_TOP_K,
                                   use_index=True, metric=metric, ef=EF_WIDE)]
        ]
        raise AssertionError(
            f"{name}: ef={EF_NARROW} and ef={EF_WIDE} return the same distances at row "
            f"{EF_DISCRIMINATOR_ROW}, so the regression suite can no longer prove ef reached "
            "the index - the retrained graph made the narrow search good enough there. "
            f"{len(candidates)} rows still discriminate on this build; pick one, set "
            "EF_DISCRIMINATOR_ROW to it and update the suite's ef queries together with it. "
            f"Candidates (first 30): {candidates[:30]}"
        )
    print(f"record: {name} ef={EF_NARROW} rows {[row for row, _ in narrow]} vs ef={EF_WIDE} "
          f"rows {[row for row, _ in wide]} (differs={differs}, asserted={assert_it})")


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
        search = search_params_of(spec)
        dataset = check_vector_dataset(table_name, described.location, spec, search)
        if spec.get("exact"):
            check_exact_results(table_name, dataset, spec, search)
        else:
            check_lossy_results(table_name, dataset, spec, search)
        check_boundary_discriminator(table_name, dataset, spec, search)
        if search.get("ef"):
            check_ef_discriminator(
                table_name, dataset, spec, spec.get("ef_discriminator", False))
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
