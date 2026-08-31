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
  - The `doris` namespace with two full-text-search fixtures, one indexed vector table per cell of the
    algorithm x element type x metric matrix (hash-prefixed directories), listed in
    VECTOR_TABLES below; BREADTH_TABLE, one table carrying the remaining cells at plan
    level; and NESTED_TABLE, a nested-field scalar index fixture used by the SHOW INDEX /
    index-inspection suites.

Every vector table holds 1024 rows in two 512-row fragments, and takes its data from one of
the three DataProfiles below - the shape is per profile, the element type and metric are per
table:
  - collinear    16-dim, embedding[j] = (row_id - 1) + j. The exact squared L2 distance
                 between rows r and n is 16 * (n - r)^2. All values are integers below 2^11
                 and therefore exact in every float type used here. Used by the L2 tables.
  - directional  16-dim quasi-uniform directions with an independently varying norm, so
                 cosine and dot rank differently and neither degenerates. Used by the cosine
                 and dot tables, because the collinear shape is degenerate under both.
  - binary       128-byte thermometer code read as a binary vector, giving hamming == |n - r|.
                 Used by the uint8 table.

Environment: python3 with the exact pins in lance_fixture_requirements.txt. The self-check
asserts the pins that decide what lands on disk (see PINNED_WRITERS); the pyarrow pin is not
asserted because the stored values are verified directly instead. Index training (IVF kmeans,
HNSW graph construction) is not bit-reproducible across runs, so regenerating this fixture
changes the binary output and requires regenerating the dependent regression .out files.
Reproducible properties are asserted by the self-check below instead: the data-shape digest,
logical metadata, the index metric, the exact distance ladder the L2 and hamming goldens
encode, plan shape, the partition-boundary discriminator, and - for the graph indexes - the
ef discriminator. Indexed-vs-flat agreement is an algorithm guarantee only for IVF_FLAT;
every quantizing or graph-traversing index has its behaviour recorded rather than asserted.

The self-check is the entire contract for a fixture whose bytes cannot be reproduced, so
this script refuses to run under python -O, where assert statements are stripped.

Usage:
  python3 lance_build_preinstalled_catalog.py            # rebuild in place
  python3 lance_build_preinstalled_catalog.py --check    # self-check the existing fixture
  python3 lance_build_preinstalled_catalog.py --repin    # rebuild, downgrading a stale
                                                         # discriminator row to a warning so
                                                         # it can be re-measured afterwards
"""

import argparse
import hashlib
import importlib.metadata
import io
import json
import math
import re
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
# A boundary row discriminates by however many of its BOUNDARY_TOP_K positions differ from the
# flat answer. At or below this, say so: the check still passes, but the row is one retrain
# away from failing it, and that is worth knowing while the goldens are open rather than on
# the rebuild after next. Measured on the committed fixture: five tables sit at 3, three at
# 7, three at 8 and one at 9, so nothing warns today. The five at 3 are the collinear
# tables whose partition edge falls nearest boundary_row, and they move first.
BOUNDARY_MARGIN_WARN = 2
# When a discriminator row stops discriminating, the failure message reports replacements.
# Both searches for them cost two ANN queries per row scanned, so they stop here rather than
# sweeping all ROWS to build a list that is then truncated to the same length anyway.
REPORTED_CANDIDATES = 30
# Graph candidate widths for the ef discriminator: with a narrow ef the traversal has to
# settle for worse candidates than with a wide one. Same purpose as nprobes=1 for IVF - it
# proves the parameter reached the index instead of being quietly dropped. The row that
# discriminates lives on DataProfile.ef_row, next to boundary_row: both are decided by
# training that runs per table, so neither can be a single global number.
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

    def __init__(self, name, dim, dtype, vector, digest, ladder=None,
                 boundary_row=256, ef_row=518):
        self.name = name
        self.dim = dim
        self.dtype = dtype
        self.vector = vector
        # sha256 of every stored vector, truncated. Pinned because the whole point of a
        # closed-form shape is that it does not move: the ordering assertions below are
        # derived from `vector` and so cannot notice `vector` itself changing, which would
        # leave the self-check green while every dependent golden and hardcoded suite query
        # went stale. See _digest_of.
        self.digest = digest
        # The row whose true neighbourhood straddles an IVF partition edge, so that a real
        # nprobes=1 probe must miss part of it while a silent flat fallback returns the flat
        # answer. Which rows have this property is decided by kmeans and moves on every
        # retrain, so it is asserted per table rather than assumed; see
        # check_boundary_discriminator.
        self.boundary_row = boundary_row
        # The row whose graph neighbourhood is thin enough that a narrow ef misses a true
        # neighbour. Decided by the graph draw and moves on every retrain, so it lives here
        # rather than being one global number - but unlike boundary_row, which is checked for
        # every table, this is only reached by the graph indexes and only asserted for the one
        # spec carrying ef_discriminator; see check_ef_discriminator. The default is the row
        # measured on the collinear data; a profile that later backs a graph index needs its
        # own, and the check reports candidates when it does.
        self.ef_row = ef_row
        # ladder(step) -> the exact distance between two rows `step` apart, when the data
        # shape has a closed form for it. None where no such form exists, in which case the
        # self-check verifies ordering properties instead of exact distances.
        self.ladder = ladder
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


def _digest_of(vector) -> str:
    """A stable fingerprint of every vector a profile stores.

    repr() of a float round-trips exactly in Python 3, so this pins the data itself rather
    than any property derived from it. The directional shape goes through math.sin, whose
    last ulp is libm-dependent, but every value is then rounded to 4 decimals and the closest
    one sits ~1e-4 (in units of the last kept digit) away from a rounding boundary - about
    eight orders of magnitude more than a 1-ulp sin difference can move it. So this digest is
    portable in practice, and if it ever does differ across platforms that is exactly the
    condition that would silently desync the query vectors hardcoded in the suites.
    """
    h = hashlib.sha256()
    for r in range(ROWS):
        h.update(";".join(repr(x) for x in vector(r)).encode())
        h.update(b"\n")
    return h.hexdigest()[:16]


# boundary_row 257 rather than the 256 this used to use. Both discriminate, but on the
# committed indexes 256 does so by only 1 of BOUNDARY_TOP_K positions on five of the seven
# collinear tables, i.e. one partition-edge shift away from failing
# check_boundary_discriminator and making the suites report a silent flat fallback that is not
# actually happening. 257 was measured
# at a minimum margin of 3 across all seven - the widest minimum among the rows near every
# partition edge - and three of the seven sit well above it, at 7.
# Picking a row costs nothing at build time - it only selects a query vector, not the index -
# so it can be re-measured against the committed fixture whenever this gets thin again.
COLLINEAR = DataProfile(
    "collinear", DIM, pa.float32(), _collinear_vector, "7c90dc26ea2628ae",
    ladder=lambda step: 16.0 * step * step, boundary_row=257)
DIRECTIONAL = DataProfile("directional", DIM, pa.float32(), _directional_vector,
                          "91957d7c118b531e")
# Measured on the thermometer data: the four partitions come out as contiguous row ranges
# split near 256, 512 and 768, and only rows within about eight of an edge discriminate - on
# the committed index those are 248-255, 502-509 and 760-767. Which side of an edge they land
# on moves on every retrain: the previous build had 255-262, 513-520 and 767-774, i.e. just
# above each split where this one has them just below, which is why this row is 505 and not
# the 513 it used to be. check_boundary_discriminator reports the current set when this
# breaks; take the new row from the middle of a run so it has the most room either way.
BINARY = DataProfile(
    "binary", BINARY_DIM, pa.uint8(), _binary_vector, "53fe7e3eacded1f5",
    ladder=lambda step: float(step), boundary_row=505)

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
    # Element type coverage, one indexed table each, all on IVF_FLAT so each can carry the
    # exact indexed-equals-flat assertion rather than only recording what it returned.
    #
    # Float16 uses cosine because a float16 L2 index over the *collinear* data does not finish
    # training: that ladder's squared distances reach 16 * 1023^2, far past float16's 65504
    # ceiling. float16 + l2 is fine on a bounded shape - the breadth tier builds it on the
    # directional data - so this is a property of the pairing, not of float16 + l2 itself.
    #
    # uint8 uses hamming because Lance reads uint8 as a binary vector and rejects every other
    # distance for it. IVF_FLAT is a choice, not a constraint: measured on pylance 7.0.0, the
    # PQ and SQ builders reject uint8 ("PQ|SQ builder: unsupported data type: UInt8"), which
    # leaves IVF_FLAT *and* IVF_HNSW_FLAT. The graph variant is covered by the breadth tier.
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


# ---------------------------------------------------------------------------
# Breadth tier
# ---------------------------------------------------------------------------
# The 12 tables above are the depth tier: every one of them carries goldens, a closed-form
# distance ladder where the shape allows, and a discriminator that proves the parameter
# reached the index. That costs roughly 190KB per cell, so the tier deliberately covers one
# representative cell per axis rather than the whole matrix.
#
# The breadth tier covers everything the depth tier leaves out, at plan level only. It is a
# single table carrying one vector column per remaining cell, each with exactly one index -
# one column per cell, never several indexes on one column, because only the first index
# built on a column is reachable. Measured on Lance: with a cosine and a dot index on one
# column, whichever was created first answers its metric from the index and the other falls
# back to a silent brute-force scan. Doris lands in the same place by a different route -
# LanceScanNode.selectIndexSegments keeps only the segments of the first index it finds for
# the column's field id, so the second index is invisible to the planner and metricMatches
# then rejects the query whose metric it does not carry. Either way a column is the unit that
# can hold a testable index, and 64 rows is enough to train one.
#
# What this tier proves is narrower than the depth tier's, and the documentation must not
# conflate them: it shows Doris plans an indexed split and the backend answers it, NOT that
# the rows returned are the right rows. Only the depth tier shows that.
BREADTH_TABLE = "vs_index_matrix"
BREADTH_ROWS = 64
BREADTH_FRAGMENT_ROWS = 32
BREADTH_SEARCH_K = 5
# refine_factor candidates for the result check, tried narrowest first. It reranks
# BREADTH_SEARCH_K * this many rows with exact distances, so it is also how much of the table
# the index may hand back before the answer stops depending on the index's own ranking: 2 is
# 16% of this table, 10 is 78%.
#
# The assertion is on the widest value; the narrowest that worked is recorded rather than
# pinned. Pinning the tightest passing value is pinning a zero margin - 5 held on one set of
# indexes and failed on the next retrain for a single f64 cosine cell, which is ANN recall
# moving rather than a defect. Recording keeps the diagnostic (a cell that suddenly needs 10
# where it used to need 2 has lost recall) without letting it break a rebuild.
BREADTH_REFINE_FACTORS = (2, 3, 5, 10)
# Query rows for the nprobes discriminator, one per partition-sized stretch of the table.
# Whether restricting the probe changes the answer depends on where the query sits relative
# to a partition edge, so a single query row is one sample; these four are tried in turn and
# the first that discriminates is enough.
BREADTH_PROBE_ROWS = (1, 17, 33, 49)
# Set by --repin. The discriminator rows (boundary_row, ef_row, and the breadth probe rows)
# are chosen against one training run, and every rebuild retrains, so a rebuild routinely
# invalidates one of them. The candidates the failure reports belong to the staging fixture,
# which a failed build then throws away - so acting on them means rebuilding, which retrains
# again, and the advice is stale before it can be used. --repin breaks that loop: it
# downgrades those assertions to warnings so the rebuild completes and the fixture is
# promoted, after which the rows can be re-measured against the fixture that was actually
# committed. Picking a row costs no rebuild, since a row only selects a query vector.
REPIN = False
# The uint8 columns carry a thermometer code sized to THIS table, not the 1024-row one the
# BINARY profile builds. Reusing that width here left 120 of its 128 bytes identical across
# every row: the data was effectively 8-dimensional with 120 constant dimensions, kmeans had
# nothing to separate, and nprobes could not restrict anything - the cell looked covered while
# testing almost nothing. One bit per row is the invariant that matters.
BREADTH_BINARY_DIM = BREADTH_ROWS // 8
# 4-bit PQ so the codebook trains on 64 rows; the depth tier's IVF_HNSW_PQ uses 8 bits, which
# needs 256 training points per sub-vector and cannot train here. These are the parameters
# that let a small index exist at all, not a recommendation - read the depth tier for those.
BREADTH_PQ_PARAMS = {"num_sub_vectors": 4, "num_bits": 4}
BREADTH_HNSW_PARAMS = {"max_level": 7, "m": 20, "ef_construction": 100}
BREADTH_ELEMENT_TYPES = {
    "f32": pa.float32(), "f64": pa.float64(), "f16": pa.float16(), "u8": pa.uint8(),
}
ALGORITHMS = ("IVF_FLAT", "IVF_SQ", "IVF_PQ",
              "IVF_HNSW_FLAT", "IVF_HNSW_SQ", "IVF_HNSW_PQ")

# Nested-field scalar index fixture for #66497's SHOW INDEX / lance_index_entries suites.
# The indexed child name deliberately contains a dot so the canonical field path can only
# be written with backtick quoting; the table also carries a __lance_frag_reuse system
# entry (deferred-remap compaction) so every inspection surface proves it filters reserved
# system indexes instead of bricking on them.
NESTED_TABLE = "nested_index"
NESTED_INDEX_NAME = "nested_label_btree"
NESTED_COLUMN = "attributes.`child.with.dot`"
NESTED_ROWS = 16

# Full-text-search fixtures. full_text_search indexes both fragments. The partial table
# deliberately appends its second fragment after index creation so Doris can exercise the
# STRICT and INDEX_ONLY coverage modes against the same committed index segment.
FTS_TABLE = "full_text_search"
FTS_PARTIAL_TABLE = "full_text_search_partial"
FTS_INDEX_NAME = "body_fts"
FTS_PARTIAL_INDEX_NAME = "body_fts_partial"
FTS_ROWS = (
    (1, "Lance search", "lance search engine", "tech"),
    (2, "Lance search twice", "lance lance search engine", "tech"),
    (3, "Lance search three times", "lance lance lance search engine", "tech"),
    (4, "Vector search", "vector search engine", "vector"),
    (5, "Full text search", "full text search engine", "search"),
    (6, "Doris lakehouse", "apache doris lakehouse", "database"),
    (7, "Lance storage", "lance columnar storage format analytics", "storage"),
    (8, "Unrelated", "unrelated document", "other"),
)
FTS_PARTIAL_ROWS = (
    (101, "lance indexed document", "indexed"),
    (102, "another lance indexed document", "indexed"),
    (103, "unrelated indexed document", "indexed"),
    (104, "lance appended after indexing", "unindexed"),
    (105, "second lance appended after indexing", "unindexed"),
)
FTS_INDEX_PARAMS = {
    "base_tokenizer": "simple",
    "language": "English",
    "max_token_length": 40,
    "lower_case": True,
    "stem": False,
    "remove_stop_words": False,
    "ascii_folding": False,
    "with_position": True,
}


def buildable_combos():
    """Every (element type, metric, algorithm) cell the embedded Lance actually accepts.

    This list is a claim about Lance, so it was checked against Lance rather than assumed: all
    4 x 4 x 6 = 96 cells of the full matrix were built, one per subprocess, on pylance 7.0.0
    with this table's data shapes at 64 rows. 56 built and answered a search, 40 failed, and
    the 56 are exactly what this function yields - nothing extra, nothing missing. The
    failures fall into three groups, each matching what Lance enforces in
    rust/lance/src/index/vector/utils.rs (validate_distance_type_for) and
    rust/lance-index/src/vector/hnsw/builder.rs:

      float16/32/64 + hamming   18 cells. Rust panic, "KMeans::find_partitions: hamming is
                                not supported".
      uint8 + l2/dot/cosine     18 cells. "Unsupported data type UInt8 with distance type
                                ..." for l2 and dot, "Normalize only supports float array"
                                for cosine - uint8 is read as a binary vector.
      uint8 under PQ or SQ      4 cells. "PQ|SQ builder: unsupported data type: UInt8",
                                which is what leaves IVF_FLAT and IVF_HNSW_FLAT.

    Those 40 are deliberately not attempted at build time. Eighteen of them are Rust panics
    rather than raised errors, and this script writes the fixture that gets committed;
    provoking panics inside it on every rebuild costs more than the diagnostic is worth. Run
    the full matrix by hand when the Lance generation changes, which is the only thing that
    moves this list.
    """
    for tag in BREADTH_ELEMENT_TYPES:
        if tag == "u8":
            metrics, algorithms = ("hamming",), ("IVF_FLAT", "IVF_HNSW_FLAT")
        else:
            metrics, algorithms = ("l2", "cosine", "dot"), ALGORITHMS
        for metric in metrics:
            for algorithm in algorithms:
                yield tag, metric, algorithm


def element_tag_of(element_type) -> str:
    for tag, candidate in BREADTH_ELEMENT_TYPES.items():
        if candidate == element_type:
            return tag
    raise AssertionError(f"no breadth tag for element type {element_type}")


def depth_combos() -> set:
    """The cells the depth tier already covers, derived from VECTOR_TABLES.

    Derived rather than listed so the breadth tier cannot drift: add a table above and its
    cell leaves this tier automatically, with no second list to keep in step.
    """
    return {(element_tag_of(element_type_of(spec)), metric_of(spec), spec["index_type"])
            for spec in VECTOR_TABLES.values()}


def breadth_combos() -> list:
    return sorted(set(buildable_combos()) - depth_combos())


def breadth_column_of(tag: str, metric: str, algorithm: str) -> str:
    # Double underscore between the parts, because an algorithm name contains single ones:
    # emb__f32__cosine__ivf_hnsw_flat splits cleanly, and the regression suite derives the
    # metric and algorithm it must query from exactly this split rather than from a second
    # hardcoded list that could drift away from the fixture.
    return f"emb__{tag}__{metric}__{algorithm.lower()}"


def _breadth_binary_vector(r):
    # The BINARY profile's thermometer, re-cut to BREADTH_BINARY_DIM. See that constant for
    # why the 1024-row width cannot be reused at this table's size.
    return [(1 << max(0, min(8, r - j * 8))) - 1 for j in range(BREADTH_BINARY_DIM)]


def breadth_vector_of(tag: str, row_offset: int) -> list:
    """The vector a breadth column stores at a 0-based row offset.

    Every float column uses the directional shape, including the l2 ones. The collinear
    ladder is not usable here: its squared L2 distances reach 16 * 1023^2, far past float16's
    65504 ceiling, and a float16 L2 index over it does not finish training. Directional data
    is bounded and trains in well under a second for every cell.
    """
    return (_breadth_binary_vector(row_offset) if tag == "u8"
            else DIRECTIONAL.vector(row_offset))


def breadth_dim_of(tag: str) -> int:
    return BREADTH_BINARY_DIM if tag == "u8" else DIM


def breadth_query_of(tag: str, row: int) -> list:
    """The stored vector of a 1-based row id, as a query."""
    return breadth_vector_of(tag, row - 1)


def make_breadth_table(row_start: int, row_end: int) -> pa.Table:
    offsets = list(range(row_start, row_end))
    columns = {"row_id": pa.array([offset + 1 for offset in offsets], type=pa.int64())}
    for tag, metric, algorithm in breadth_combos():
        columns[breadth_column_of(tag, metric, algorithm)] = (
            pa.FixedSizeListArray.from_arrays(
                pa.array([value for offset in offsets
                          for value in breadth_vector_of(tag, offset)],
                         type=BREADTH_ELEMENT_TYPES[tag]),
                breadth_dim_of(tag),
            )
        )
    table = pa.table(columns)
    return table.cast(
        pa.schema([pa.field(f.name, f.type, nullable=False) for f in table.schema])
    )


def breadth_index_params(algorithm: str) -> dict:
    params = {}
    if "PQ" in algorithm:
        params.update(BREADTH_PQ_PARAMS)
    if "HNSW" in algorithm:
        params.update(BREADTH_HNSW_PARAMS)
    return params


# Graph candidate width for the breadth tier's HNSW cells. Lance's rule is not "ef >= k" but
# "ef >= k * refine_factor" - a reranked search asks the graph for that many candidates - so
# this has to cover the widest refine factor the result check tries. It currently equals that
# product exactly, with no headroom: raising BREADTH_SEARCH_K or the tail of
# BREADTH_REFINE_FACTORS without raising this turns every HNSW cell into "ef must be greater
# than or equal to k", in the generator and in the regression suite alike. The assertion
# below is here so that surfaces as this sentence rather than as two dozen opaque Lance
# errors. test_lance_vector_search_index_matrix.groovy hardcodes the same number.
BREADTH_EF = 50
assert BREADTH_EF >= BREADTH_SEARCH_K * max(BREADTH_REFINE_FACTORS), (
    f"BREADTH_EF={BREADTH_EF} is below BREADTH_SEARCH_K * max(BREADTH_REFINE_FACTORS) = "
    f"{BREADTH_SEARCH_K * max(BREADTH_REFINE_FACTORS)}; every HNSW breadth cell would fail "
    "with 'ef must be greater than or equal to k'"
)


def breadth_search_params(algorithm: str) -> dict:
    return {"ef": BREADTH_EF} if "HNSW" in algorithm else {}


def create_breadth_table(namespace) -> str:
    first = make_breadth_table(0, BREADTH_FRAGMENT_ROWS)
    buffer = io.BytesIO()
    with ipc.new_stream(buffer, first.schema) as writer:
        writer.write_table(first)
    response = namespace.create_table(
        CreateTableRequest(id=[NAMESPACE, BREADTH_TABLE]), buffer.getvalue()
    )
    location = response.location
    lance.write_dataset(
        make_breadth_table(BREADTH_FRAGMENT_ROWS, BREADTH_ROWS), location, mode="append")
    for tag, metric, algorithm in breadth_combos():
        column = breadth_column_of(tag, metric, algorithm)
        lance.dataset(location).create_index(
            column,
            algorithm,
            name=f"idx_{column}",
            metric=metric,
            num_partitions=NUM_PARTITIONS,
            sample_rate=256,
            index_file_version="V3",
            **breadth_index_params(algorithm),
        )
    # One create_index per cell means one dataset version per cell, and their superseded
    # manifests dominate the committed size - 1.2MB before this call, 0.6MB after.
    lance.dataset(location).cleanup_old_versions(
        older_than=timedelta(0), delete_unverified=True)
    return location


def profile_of(spec: dict) -> DataProfile:
    return spec.get("profile", COLLINEAR)


def element_type_of(spec: dict):
    return spec.get("element_type", profile_of(spec).dtype)


def metric_of(spec: dict) -> str:
    return spec.get("metric", "l2")


def distance_tolerance_of(spec: dict):
    """How far the indexed and flat distances may drift apart for this table.

    The two paths accumulate in a different order, so in general the results differ in the
    last ulp or two. What decides whether that can happen at all is the data shape, not the
    element type: a ladder profile stores small integers and both paths land on exactly the
    same value, so those tables get no tolerance and keep the bit-exact comparison that makes
    a real regression in the distance kernel impossible to hide.

    Only the tie-free directional profile needs slack, and there it is bounded by the element
    type's precision: about 6e-8 relative for float32, and about 5e-4 for float16's 11-bit
    mantissa.
    """
    if profile_of(spec).ladder is not None:
        return 0.0, 0.0
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


def make_fts_table(rows, *, include_title: bool) -> pa.Table:
    if include_title:
        schema = pa.schema(
            [
                pa.field("row_id", pa.int64(), nullable=False),
                pa.field("title", pa.string(), nullable=False),
                pa.field("body", pa.string(), nullable=False),
                pa.field("category", pa.string(), nullable=False),
            ]
        )
        columns = list(zip(*rows))
        return pa.Table.from_arrays(
            [
                pa.array(columns[0], type=pa.int64()),
                pa.array(columns[1], type=pa.string()),
                pa.array(columns[2], type=pa.string()),
                pa.array(columns[3], type=pa.string()),
            ],
            schema=schema,
        )

    schema = pa.schema(
        [
            pa.field("row_id", pa.int64(), nullable=False),
            pa.field("body", pa.string(), nullable=False),
            pa.field("category", pa.string(), nullable=False),
        ]
    )
    columns = list(zip(*rows))
    return pa.Table.from_arrays(
        [
            pa.array(columns[0], type=pa.int64()),
            pa.array(columns[1], type=pa.string()),
            pa.array(columns[2], type=pa.string()),
        ],
        schema=schema,
    )


def create_fts_table(namespace, table_name: str, rows, *, indexed_rows: int,
                     index_name: str, include_title: bool) -> str:
    first = make_fts_table(rows[:indexed_rows], include_title=include_title)
    buffer = io.BytesIO()
    with ipc.new_stream(buffer, first.schema) as writer:
        writer.write_table(first)
    response = namespace.create_table(
        CreateTableRequest(id=[NAMESPACE, table_name]), buffer.getvalue()
    )
    location = response.location
    dataset = lance.dataset(location)
    if table_name == FTS_TABLE:
        # Make the fully indexed fixture cover two physical fragments.
        lance.write_dataset(
            make_fts_table(rows[indexed_rows:], include_title=include_title),
            location,
            mode="append",
        )
        dataset = lance.dataset(location)
    dataset.create_scalar_index(
        "body", "INVERTED", name=index_name, **FTS_INDEX_PARAMS
    )
    if table_name == FTS_PARTIAL_TABLE:
        # Keep these rows outside the committed segment for coverage-mode tests.
        lance.write_dataset(
            make_fts_table(rows[indexed_rows:], include_title=include_title),
            location,
            mode="append",
        )
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
    # The pinned writer maintains the optional latest-version hint itself, but the version
    # cleanup and the index-directory pruning above both run after it last did. Rewrite it
    # from the reopened manifest so it names the version that actually survived; check_catalog
    # asserts the two agree, because a hint pointing at a deleted version would send every
    # reader to a manifest that is no longer there.
    hint = root / MANIFEST_DIR / "_versions" / "latest_version_hint.json"
    hint.write_text(f'{{"version":{manifest.version}}}')
    # This version is a commit count that compaction then collapses, so it is NOT monotonic
    # across rebuilds - it went 35 -> 27 when the matrix grew from 6 tables to 12. Lance names
    # a version file u64::MAX - version so that a listing returns the newest first, which
    # means a stale higher-versioned file left next to this one would win any listing-based
    # resolution and point readers at the previous catalog. Nothing in the committed fixture
    # can prevent that; whatever publishes it has to replace the destination rather than merge
    # into it. iceberg.yaml.tpl does exactly that for s3://warehouse/lance.
    print(f"record: __manifest committed at version {manifest.version}")


def build(root: Path, all_types_source: Path) -> None:
    shutil.copytree(all_types_source, root / ALL_TYPES_DIR)
    namespace = lance_namespace.connect("dir", {"root": str(root)})
    namespace.register_table(
        RegisterTableRequest(id=["all_types"], location=ALL_TYPES_DIR)
    )
    namespace.create_namespace(CreateNamespaceRequest(id=[NAMESPACE]))
    create_fts_table(
        namespace,
        FTS_TABLE,
        FTS_ROWS,
        indexed_rows=4,
        index_name=FTS_INDEX_NAME,
        include_title=True,
    )
    create_fts_table(
        namespace,
        FTS_PARTIAL_TABLE,
        FTS_PARTIAL_ROWS,
        indexed_rows=3,
        index_name=FTS_PARTIAL_INDEX_NAME,
        include_title=False,
    )
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
    create_breadth_table(namespace)
    create_nested_index_table(namespace)
    compact_manifest(root)


def topk(dataset, query, k: int, use_index: bool, *, metric: str, **nearest_kwargs):
    # The metric is never left implicit, so it is keyword-only with no default. Lance
    # defaults to L2, and asking an index built with another metric for an L2 search is a
    # silent fall back to brute force, not an error - exactly the failure this fixture
    # exists to detect. A default here would let a future check forget the argument and
    # then compare one brute-force scan against another, passing unconditionally.
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


def index_metric_of(dataset, index_name: str) -> set:
    """The metrics an index was actually built with, from stats["indices"][*]["metric_type"].

    Read that exact path rather than searching the stats for any metric-shaped key: a
    quantizing sub-index reports a metric of its own and it is not always the index's. An
    IVF_PQ built for cosine normalizes the vectors and then trains the PQ codebook under L2,
    so its stats carry metric_type "cosine" on the index and "l2" on the sub-index - only the
    first is the metric a query has to match, and collecting both would fail every cosine PQ
    table. A stats shape that no longer carries the key must fail loudly here rather than
    quietly skip the one assertion that catches an index built with the wrong metric.
    """
    stats = dataset.stats.index_stats(index_name)
    indices = stats.get("indices")
    assert indices, (
        f"index stats for {index_name} carry no 'indices' entry, so the metric cannot be "
        f"checked - Lance changed the stats shape. Top-level keys: {sorted(stats)}"
    )
    metrics = set()
    for entry in indices:
        assert "metric_type" in entry, (
            f"index stats for {index_name} carry no 'metric_type', so the metric cannot be "
            f"checked - Lance changed the stats shape. Entry keys: {sorted(entry)}"
        )
        metrics.add(str(entry["metric_type"]).lower())
    return metrics


def _discriminator_failure(message: str) -> None:
    """Fail on a stale discriminator row, or warn when --repin is re-measuring them."""
    if REPIN:
        print(f"REPIN: {message}")
        return
    raise AssertionError(message)


def check_data_shapes() -> None:
    """Pin the data every profile generates, before anything derived from it is checked.

    Every other assertion in this file is computed from `profile.vector`, so none of them can
    notice `profile.vector` itself changing - tweak _DIRECTION_ALPHA, _NORM_ALPHA or the
    round() precision and the whole self-check stays green while every regression golden and
    every query vector hardcoded in the suites silently goes stale.
    """
    for profile in (COLLINEAR, DIRECTIONAL, BINARY):
        actual = _digest_of(profile.vector)
        assert actual == profile.digest, (
            f"{profile.name}: data-shape digest is {actual}, expected {profile.digest}. The "
            "vector function changed, so every dependent golden in regression-test/data and "
            "every hardcoded query vector in the lance suites is now stale. If the change is "
            "intended, rebuild the fixture, regenerate those goldens, update the suites' "
            "query literals and set this digest to the new value."
        )


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

    # Tie the bytes on disk back to the profile that check_data_shapes just pinned, so the
    # chain "pinned digest -> profile.vector -> what this table actually stores" is closed.
    # Casting through the element type is what makes this exact for float16 and uint8 too.
    for row in (1, ROWS):
        expected = pa.array(profile.query_of(row), type=element_type).to_pylist()
        # row_id stays in the projection even though only embedding is read back, so this
        # does not depend on Lance being able to filter on a column it was not asked to
        # return.
        selected = dataset.to_table(columns=["row_id", "embedding"],
                                    filter=f"row_id = {row}")
        assert selected.num_rows == 1, (
            f"{name}: expected exactly one row with row_id {row}, got {selected.num_rows}"
        )
        stored = selected["embedding"][0].as_py()
        assert stored == expected, (
            f"{name}: row {row} stores {stored}, but the {profile.name} profile says "
            f"{expected}"
        )

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
    # The metric the index was built with, not the one the spec asked for. Doris only plans
    # an indexed split when the query metric equals the index metric, and Lance answers a
    # mismatched query by silently falling back to brute force, so an index that quietly
    # came out L2 would still return plausible rows - and surface only as
    # lanceSearchIndexSegments=0 in the regression suite, an opaque failure this check
    # exists to turn into a loud one.
    index_metrics = index_metric_of(dataset, index["name"])
    assert index_metrics == {metric}, (
        f"{name}: index reports metric {sorted(index_metrics)}, expected [{metric!r}]"
    )
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
        # dot is not a proper metric - a longer vector scores better than a closer one - so
        # the nearest row need not be the row the query was taken from, and this assertion
        # does not apply. That is a property of the metric, not of the data shape, so it is
        # decided here rather than on the profile: an exact dot table would otherwise have to
        # reuse a profile whose flag says the opposite.
        if metric != "dot":
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

        candidates = []
        for row in range(1, ROWS + 1):
            if discriminates(row):
                candidates.append(row)
                if len(candidates) == REPORTED_CANDIDATES:
                    break
        _discriminator_failure(
            f"{name}: row {boundary_row} no longer discriminates nprobes=1 from flat search; "
            "the IVF partition boundaries moved. Rows that still discriminate on this build: "
            f"pick one, set the {profile.name} profile's boundary_row to it and update the "
            "boundary queries in the regression suites together with it. "
            f"Candidates (up to {REPORTED_CANDIDATES}): {candidates}"
        )
    # How far this row is from stopping to discriminate. The assertion above is binary - the
    # sequences differ or they do not - but the distance to failure is not: a margin of 1
    # means one more partition-edge shift makes nprobes=1 return exactly the flat answer, and
    # then this check fails and the regression suite reports a silent flat fallback that is
    # not happening. It is a false alarm that costs a fixture rebuild to clear, so print the
    # number rather than leaving it invisible until it hits zero.
    margin = sum(1 for a, b in zip(single, flat) if a != b)
    print(f"record: {name} boundary nprobes=1 top-{BOUNDARY_TOP_K} rows "
          f"{[row for row, _ in single_rows]} vs flat rows {[row for row, _ in flat_rows]} "
          f"(margin={margin}/{BOUNDARY_TOP_K})")
    if margin <= BOUNDARY_MARGIN_WARN:
        # Not an assertion: a thin margin is a real, working discriminator, and failing here
        # would block a fixture that is fine. Choosing a wider row is only worth doing when
        # the goldens are being regenerated anyway, which is what this line is here to inform.
        state = ("no longer discriminates at all" if margin == 0
                 else f"discriminates by only {margin} of {BOUNDARY_TOP_K} positions")
        print(f"WARNING: {name} boundary row {boundary_row} {state}. "
              + ("It proves nothing about reaching the index; only --repin got you past the "
                 "assertion above, and the row must be re-measured before this fixture is "
                 "used." if margin == 0 else
                 "It still works, but the next retrain may erase it and fail "
                 "check_boundary_discriminator. If you are regenerating goldens anyway, "
                 "consider moving this profile's boundary_row to a wider-margin row."))
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
    ef_row = profile.ef_row
    ef_query = profile.query_of(ef_row)
    narrow = topk(dataset, ef_query, EF_TOP_K, use_index=True, metric=metric, ef=EF_NARROW)
    wide = topk(dataset, ef_query, EF_TOP_K, use_index=True, metric=metric, ef=EF_WIDE)
    # Compare distances rather than row ids: on a ladder profile rows ef_row-d and ef_row+d
    # tie, so the row order inside a tie group is arbitrary while a missed neighbour always
    # changes a distance.
    differs = [d for _, d in narrow] != [d for _, d in wide]
    if assert_it and not differs:
        # Which rows react to ef is decided by the graph draw and changes on every rebuild,
        # so do the search here rather than making the next person write it: report the rows
        # that still discriminate on this freshly built index, and they can pin one. Stop at
        # REPORTED_CANDIDATES - this runs two ANN searches per row scanned, and the report
        # only ever shows that many.
        candidates = []
        for row in range(1, ROWS + 1):
            query = profile.query_of(row)
            if ([d for _, d in topk(dataset, query, EF_TOP_K, use_index=True,
                                    metric=metric, ef=EF_NARROW)]
                    != [d for _, d in topk(dataset, query, EF_TOP_K, use_index=True,
                                           metric=metric, ef=EF_WIDE)]):
                candidates.append(row)
                if len(candidates) == REPORTED_CANDIDATES:
                    break
        _discriminator_failure(
            f"{name}: ef={EF_NARROW} and ef={EF_WIDE} return the same distances at row "
            f"{ef_row}, so the regression suite can no longer prove ef reached "
            "the index - the retrained graph made the narrow search good enough there. "
            f"Rows that still discriminate on this build: pick one, set the {profile.name} "
            "profile's ef_row to it and update the suite's ef queries together with it. "
            f"Candidates (up to {REPORTED_CANDIDATES}): {candidates}"
        )
    # Same reasoning as the boundary margin: how many positions separate the narrow search
    # from the wide one is how much room the discriminator has before a retrained graph makes
    # the narrow one good enough and this stops proving anything.
    margin = sum(1 for a, b in zip([d for _, d in narrow], [d for _, d in wide]) if a != b)
    print(f"record: {name} ef={EF_NARROW} rows {[row for row, _ in narrow]} vs ef={EF_WIDE} "
          f"rows {[row for row, _ in wide]} (differs={differs}, margin={margin}/{EF_TOP_K}, "
          f"asserted={assert_it})")
    if assert_it and margin <= BOUNDARY_MARGIN_WARN:
        state = ("no longer discriminates at all" if margin == 0
                 else f"discriminates by only {margin} of {EF_TOP_K} positions")
        print(f"WARNING: {name} ef row {ef_row} {state}. "
              + ("It proves nothing about ef reaching the index; only --repin got you past "
                 "the assertion above, and the row must be re-measured before this fixture "
                 "is used." if margin == 0 else
                 "It still works, but the next retrain may erase it and fail "
                 "check_ef_discriminator. If you are regenerating goldens anyway, consider "
                 "moving this profile's ef_row to a wider-margin row."))


REQUIREMENTS_FILE = Path(__file__).resolve().parent / "lance_fixture_requirements.txt"
# The distributions whose version decides what lands on disk, and which are therefore
# asserted: pylance writes the data and index files, lance-namespace writes the __manifest
# and picks the hash-prefixed directory names. pyarrow is deliberately absent - it only
# supplies the in-memory Arrow buffers handed to Lance, and whether it changed a stored value
# is checked directly by check_data_shapes plus the per-row readback in check_vector_dataset.
# Verifying the bytes beats pinning a proxy for them, and it keeps a pyarrow that differs in
# no observable way from blocking a rebuild.
PINNED_WRITERS = ("pylance", "lance-namespace")


def check_pinned_writer() -> None:
    """Refuse to build or verify with a writer other than the pinned one.

    The pins are not a formality: pylance names the Lance generation the BE's lance-c reads,
    and the fixture is readable by the older lance-java that Spark uses only because the
    regression run says so, not because anything about the format guarantees it. A rebuild on
    a machine that resolved something else can therefore emit a fixture no other reader opens,
    and the rest of this self-check would not notice - it opens the fixture with the very
    writer that produced it, so it passes by construction.
    """
    requirements = REQUIREMENTS_FILE.read_text()
    for distribution in PINNED_WRITERS:
        pinned = re.search(rf"^{re.escape(distribution)}==(\S+)$", requirements, re.MULTILINE)
        assert pinned, f"no {distribution} pin found in {REQUIREMENTS_FILE.name}"
        # The pin names a *distribution*, so read the distribution's metadata rather than a
        # module attribute: it is what pip actually resolved, and it exists whatever the
        # module chooses to expose.
        try:
            installed = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:  # pragma: no cover - env problem
            raise AssertionError(
                f"{distribution} is importable but has no distribution metadata, so its "
                f"version cannot be checked against {REQUIREMENTS_FILE.name}. Install it "
                "with pip using that file rather than putting a source checkout on sys.path."
            )
        assert installed == pinned.group(1), (
            f"{distribution} {installed} is installed but {REQUIREMENTS_FILE.name} pins "
            f"{pinned.group(1)}. Install the pins before touching the fixture: a fixture "
            "written by an unpinned writer can be unreadable by the BE's lance-c or by the "
            "lance-java Spark uses, and this self-check cannot detect that on its own "
            "because it reads the fixture back with the same writer that produced it."
        )


def check_breadth_table(location: str) -> None:
    """Result coverage for every matrix cell the depth tier does not carry.

    Asserts that each cell reaches an indexed plan and that a refined indexed search returns
    exactly what an exhaustive scan returns - the same standard check_exact_results holds
    IVF_FLAT to, reached here by reranking with refine_factor rather than by the algorithm
    keeping the original vectors. That needs no goldens and no closed-form distance ladder,
    which is what lets this tier cover 44 cells for the price of one small table.

    What it still does not establish, and the documentation must not claim: it compares the
    index path against the scan path inside the same backend, so a distance kernel that is
    wrong in both directions passes. The depth tier is what pins absolute values, through a
    closed-form ladder and committed goldens.
    """
    combos = breadth_combos()
    unrefined = []
    late_probes = []
    undiscriminated = []
    needed = {}
    dataset = lance.dataset(location)
    all_fragments = {fragment.fragment_id for fragment in dataset.get_fragments()}
    assert dataset.count_rows() == BREADTH_ROWS, (
        f"{BREADTH_TABLE}: expected {BREADTH_ROWS} rows"
    )
    assert len(dataset.get_fragments()) == 2, f"{BREADTH_TABLE}: expected 2 fragments"
    indices = {index["name"] for index in dataset.list_indices()}
    assert len(indices) == len(combos), (
        f"{BREADTH_TABLE}: {len(indices)} indexes for {len(combos)} cells. Either a "
        "create_index failed silently, or buildable_combos() moved - a Lance upgrade that "
        "widens or narrows the matrix changes the expected cell count without touching the "
        "committed fixture, and then the fixture has to be rebuilt"
    )
    for tag, metric, algorithm in combos:
        column = breadth_column_of(tag, metric, algorithm)
        assert f"idx_{column}" in indices, f"{BREADTH_TABLE}: {column} has no index"
        built = index_metric_of(dataset, f"idx_{column}")
        assert built == {metric}, (
            f"{BREADTH_TABLE}.{column}: index reports metric {sorted(built)}, expected "
            f"[{metric!r}]"
        )
        indexed_fragments = {frag for index in dataset.list_indices()
                             if index["name"] == f"idx_{column}"
                             for frag in index["fragment_ids"]}
        assert indexed_fragments == all_fragments, (
            f"{BREADTH_TABLE}.{column}: index covers fragments {sorted(indexed_fragments)}, "
            f"not {sorted(all_fragments)} - part of the column would be scanned unindexed"
        )

        query = breadth_query_of(tag, 1)
        search = breadth_search_params(algorithm)
        nearest = {"column": column, "q": query, "k": BREADTH_SEARCH_K, "metric": metric,
                   "nprobes": NUM_PARTITIONS, **search}
        nearest_flat = {"column": column, "q": query, "k": BREADTH_SEARCH_K,
                        "metric": metric, "use_index": False}
        plan = dataset.scanner(nearest=nearest).explain_plan(True)
        assert "ANNSubIndex" in plan, (
            f"{BREADTH_TABLE}.{column}: {algorithm}/{metric} does not reach an indexed plan, "
            "so Lance is answering it with a brute-force scan"
        )

        # Execution-level proof that the index is really what answered. A flat scan has no
        # partitions, so nprobes means nothing to it and cannot change its answer: observing
        # a difference proves the parameter reached partition-based code. The converse does
        # not hold - the true top-k can simply all live in the one probed partition - so try
        # several query rows and take the first that discriminates.
        def probe_rows(probe_query, nprobes):
            # topk() is hardcoded to the depth tier's "embedding" column, so go through the
            # scanner directly here.
            return dataset.scanner(nearest={
                "column": column, "q": probe_query, "k": BREADTH_SEARCH_K, "metric": metric,
                "nprobes": nprobes, **search}).to_table()["row_id"].to_pylist()

        probes = []
        for row in BREADTH_PROBE_ROWS:
            probe_query = breadth_query_of(tag, row)
            probes.append(row)
            if probe_rows(probe_query, 1) != probe_rows(probe_query, NUM_PARTITIONS):
                break
        else:
            undiscriminated.append(column)
            _discriminator_failure(
                f"{BREADTH_TABLE}.{column}: nprobes=1 returns what "
                f"nprobes={NUM_PARTITIONS} "
                f"returns at every row in {BREADTH_PROBE_ROWS}, so nothing here proves the "
                "search reached the index rather than falling back to a scan. Check the data "
                "shape first: a column whose dimensions are largely constant gives kmeans "
                "nothing to separate, and then no nprobes can restrict anything - that is "
                "what a mis-sized thermometer code did to the uint8 columns once."
            )

        # The result check. refine_factor reranks the candidates the index proposed using
        # exact distances, which is what lets a quantizing or graph index be held to the same
        # equality IVF_FLAT satisfies outright. Unrefined, only about half these cells agree
        # with the scan; that difference is ordinary ANN behaviour, not a defect, so it is
        # recorded below rather than asserted.
        flat = dataset.scanner(nearest=nearest_flat).to_table()
        flat_rows = flat["row_id"].to_pylist()
        rel_tol = 2e-3 if tag == "f16" else 1e-5
        assert len(flat_rows) == BREADTH_SEARCH_K, (
            f"{BREADTH_TABLE}.{column}: an exhaustive scan returned {len(flat_rows)} rows, "
            f"expected {BREADTH_SEARCH_K}"
        )
        refined = None
        for factor in BREADTH_REFINE_FACTORS:
            candidate = dataset.scanner(
                nearest={**nearest, "refine_factor": factor}).to_table()
            if candidate["row_id"].to_pylist() == flat_rows:
                refined, needed[column] = candidate, factor
                break
        assert refined is not None, (
            f"{BREADTH_TABLE}.{column}: even reranking "
            f"{BREADTH_SEARCH_K * BREADTH_REFINE_FACTORS[-1]} of {BREADTH_ROWS} rows, the "
            f"indexed search does not reproduce the exhaustive scan. indexed="
            f"{candidate['row_id'].to_pylist()} flat={flat_rows}"
        )
        assert all(math.isclose(a, b, rel_tol=rel_tol, abs_tol=1e-5)
                   for a, b in zip(refined["_distance"].to_pylist(),
                                   flat["_distance"].to_pylist())), (
            f"{BREADTH_TABLE}.{column}: refined indexed distances differ from the scan by "
            f"more than {tag} rounding"
        )
        if dataset.scanner(nearest=nearest).to_table()["row_id"].to_pylist() != flat_rows:
            unrefined.append(column)
        if len(probes) > 1 and column not in undiscriminated:
            late_probes.append(f"{column}@{probes[-1]}")
    # Only claim the full result under --repin if it is actually true. The whole point of
    # --repin is that a maintainer reads this log and re-measures from it, so a tail-of-log
    # summary contradicting the REPIN lines above it is the one place a false claim costs
    # something. Without --repin an undiscriminated cell has already raised.
    if undiscriminated:
        print(f"record: {BREADTH_TABLE} covers {len(combos)} matrix cells; "
              f"{len(undiscriminated)} of them no longer respond to nprobes at any row in "
              f"{BREADTH_PROBE_ROWS} and prove nothing about reaching the index: "
              f"{undiscriminated[:10]}")
    else:
        print(f"record: {BREADTH_TABLE} covers {len(combos)} matrix cells; every one reaches "
              "an indexed plan, responds to nprobes, and matches an exhaustive scan when "
              "refined")
    spread = {f: sum(1 for v in needed.values() if v == f) for f in BREADTH_REFINE_FACTORS}
    print(f"record: {BREADTH_TABLE} needs refine_factor on {len(unrefined)} of {len(combos)} "
          f"cells to reach that agreement; narrowest that sufficed, by count: {spread}")
    if late_probes:
        # Not a failure - the discriminator holds. Worth printing because a cell that only
        # discriminates at the last probe row is the one that will stop discriminating first.
        print(f"record: {BREADTH_TABLE} needed a later probe row on {len(late_probes)} "
              f"cells: {late_probes[:10]}")


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


def check_fts_dataset(location: str, *, table_name: str, index_name: str,
                      expected_rows: int, expected_indexed_fragments: int) -> None:
    dataset = lance.dataset(location)
    assert dataset.count_rows() == expected_rows, (
        f"{table_name}: expected {expected_rows} rows"
    )
    fragments = dataset.get_fragments()
    assert len(fragments) == 2, f"{table_name}: expected 2 fragments"
    indices = {index["name"]: index for index in dataset.list_indices()}
    index = indices.get(index_name)
    assert index is not None, f"{table_name}: missing FTS index {index_name}: {indices}"
    assert index["type"] == "Inverted", (
        f"{table_name}: {index_name} has type {index['type']}, expected Inverted"
    )
    indexed_fragments = set(index["fragment_ids"])
    assert len(indexed_fragments) == expected_indexed_fragments, (
        f"{table_name}: {index_name} covers fragments {sorted(indexed_fragments)}, expected "
        f"{expected_indexed_fragments} fragments"
    )
    if table_name == FTS_TABLE:
        ranked = dataset.scanner(
            columns=["row_id", "_score"],
            full_text_query={"query": "lance", "columns": ["body"]},
            limit=4,
        ).to_table()
        assert ranked["row_id"].to_pylist() == [3, 2, 1, 7], (
            f"{table_name}: indexed BM25 probe returned {ranked.to_pydict()}"
        )


def check_catalog(root: Path) -> None:
    check_data_shapes()
    namespace = lance_namespace.connect("dir", {"root": str(root)})
    tables = namespace.list_tables(ListTablesRequest(id=[NAMESPACE]))
    expected_tables = sorted(
        [*VECTOR_TABLES, BREADTH_TABLE, NESTED_TABLE, FTS_TABLE, FTS_PARTIAL_TABLE]
    )
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
        dataset = check_vector_dataset(table_name, described.location, spec, search)
        if spec.get("exact"):
            check_exact_results(table_name, dataset, spec, search)
        else:
            check_lossy_results(table_name, dataset, spec, search)
        check_boundary_discriminator(table_name, dataset, spec, search)
        if search.get("ef"):
            check_ef_discriminator(
                table_name, dataset, spec, spec.get("ef_discriminator", False))

    breadth = namespace.describe_table(DescribeTableRequest(id=[NAMESPACE, BREADTH_TABLE]))
    assert Path(breadth.location.removeprefix("file://")).is_dir(), (
        f"{BREADTH_TABLE} location missing: {breadth.location}"
    )
    check_breadth_table(breadth.location)

    nested = namespace.describe_table(DescribeTableRequest(id=[NAMESPACE, NESTED_TABLE]))
    nested_path = Path(nested.location.removeprefix("file://"))
    assert nested_path.is_dir(), f"{NESTED_TABLE} location missing: {nested.location}"
    check_nested_dataset(nested.location)

    full_fts = namespace.describe_table(DescribeTableRequest(id=[NAMESPACE, FTS_TABLE]))
    check_fts_dataset(
        full_fts.location,
        table_name=FTS_TABLE,
        index_name=FTS_INDEX_NAME,
        expected_rows=len(FTS_ROWS),
        expected_indexed_fragments=2,
    )
    partial_fts = namespace.describe_table(
        DescribeTableRequest(id=[NAMESPACE, FTS_PARTIAL_TABLE])
    )
    check_fts_dataset(
        partial_fts.location,
        table_name=FTS_PARTIAL_TABLE,
        index_name=FTS_PARTIAL_INDEX_NAME,
        expected_rows=len(FTS_PARTIAL_ROWS),
        expected_indexed_fragments=1,
    )
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
    parser.add_argument(
        "--repin",
        action="store_true",
        help="downgrade discriminator-row failures to warnings so a rebuild can complete; "
             "re-measure the rows against the promoted fixture, then re-run --check",
    )
    args = parser.parse_args()
    global REPIN
    REPIN = args.repin
    output: Path = args.output

    # Every verification in this script is an assert, and the self-check is the whole
    # contract for a fixture whose bytes are not reproducible. Under -O the rebuild
    # below would replace the committed fixture having verified nothing at all.
    if not __debug__:
        print("refusing to run with assertions disabled (python -O)", file=sys.stderr)
        return 1

    # Before either branch, so this really does refuse to build as well as to verify: a
    # wrong writer would otherwise get all the way through create_index and die there with
    # an opaque API error, never reaching the diagnostic below.
    check_pinned_writer()

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
