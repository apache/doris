<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lance multi-vector search

A Lance `List<FixedSizeList<T, D>>` column stores a variable number of D-dimensional
subvectors in each table row. Doris exposes it as `ARRAY<ARRAY<FLOAT>>` for Float16
and Float32, or `ARRAY<ARRAY<DOUBLE>>` for Float64.

`vector_search` accepts a non-empty JSON matrix for such a column. Every inner
array must contain exactly D finite numeric values representable in the column's
element type. A matrix with one inner array is still one multi-vector query.
This is not a batch of independent searches or a search across several columns.
Ordinary `FixedSizeList` columns continue to accept a one-dimensional JSON array.

```sql
SELECT id, _distance
FROM vector_search(
    "table" = "lance_catalog.default.documents",
    "column" = "embeddings",
    "query_vector" = "[[1,0],[0,1]]",
    "top_k" = "10",
    "metric" = "cosine",
    "filter" = "id > 100"
)
ORDER BY _distance, id;
```

## Row-level score and filtering

For query subvectors Q and a row's subvectors V, Doris reports:

`_distance = sum(q in Q, min(v in V, distance(q, v)))`

Smaller is better. The base distance is squared Euclidean distance for `l2`,
`1 - cosine_similarity` for `cosine`, or `1 - dot_product` for `dot`. Each query
subvector contributes once; the same stored subvector may match several query
subvectors. The score is a sum, not an average, so duplicating a query subvector
changes the score. `_distance` uses Doris FLOAT, including for Float64 inputs.

`top_k` and `offset` count table rows, not subvectors. The TVF `filter` is applied
before candidate search; an outer SQL WHERE filters the search's results.
Distributed fragments return row candidates for the global TopK. Ties at the
TopK boundary have no guaranteed row order.

`use_index=false` computes exact scores over the selected rows. With indexes,
candidate selection is approximate. Doris always refines multi-vector candidates
against their original values so indexed and unindexed rows use the same score.
`refine_factor` can increase the candidate pool beyond this default refinement.
It does not turn ANN candidate selection into an exhaustive search.

Omitting `metric` selects L2 consistently on all fragments, including partially
indexed datasets. The pinned Lance version supports multi-vector indexes with
the cosine metric; specify `metric="cosine"` to use those indexes. L2 and dot
searches use the exact path when no compatible index exists. `nprobes`, `ef`, and `refine_factor`
retain their usual index-specific meaning. Rows appended after index creation
are searched together with indexed rows.

## Data and compatibility requirements

- Supported element types: Float16, Float32, Float64. Integer multi-vector columns
  and Hamming multi-vector queries are rejected.
- Outer null rows and empty outer arrays have no matching subvector and do not rank.
- Subvectors must be declared non-nullable. Stored subvectors must contain only
  finite, non-null elements. The pinned Lance distance kernels do not support
  null elements. The scoring path rejects actual null/non-finite elements before
  computing scores; the persisted nullable schema flag alone is allowed because
  Lance reconstructs this flag as nullable.
- Cosine pairs with zero norm have undefined distance and do not contribute a
  match. A row is excluded if any query subvector has no defined match, so a
  zero-norm query subvector produces no results. Valid rows continue to rank.
- Query matrices reject empty matrices, ragged dimensions, nulls, nonnumeric
  values, and numbers outside the element type's finite range.
- At most 128 query subvectors are accepted. Both
  `num_vectors * (top_k + offset)` and `refine_factor * (top_k + offset)` must be
  at most 100,000, with a default refinement factor of 1. These limits bound
  per-query ANN plan expansion and candidate allocation, independently of wire size.
- Extension and dictionary vector encodings are not supported.
- Multi-vector requests use a new protocol version. Old BEs reject these requests;
  finish the BE upgrade before enabling multi-vector searches. Ordinary vector
  requests retain their previous protocol.

For example, a compatible PyArrow schema is:

```python
import pyarrow as pa

vector = pa.list_(pa.float32(), 2)
embeddings = pa.field(
    "embeddings",
    pa.list_(pa.field("item", vector, nullable=False)),
    nullable=True,
)
```

The regression fixture generator `lance_build_multivector.py` creates typed
multi-vector data, empty/null rows, cosine IVF_FLAT indexes, and a subsequent
append. Additional fixtures cover small-distance TopK, batch-independent indexed
TopK, and actual invalid stored elements. Its distance oracle computes scores
independently of Lance.

## Regression coverage

`test_lance_multivector_coverage` supplements the basic search suite with:

- Float16/Float32/Float64 columns at dimensions 1, 3, 8, and 128, unequal subvector
  counts, empty/null outer rows, and rejected nullable-subvector/integer schemas.
- Independent L2, cosine, and dot scoring oracles, including repeated query
  subvectors, Float16 overflow rejection, and nested payload round trips with
  TopN lazy materialization enabled and disabled.
- 768-row, 128-dimensional IVF_FLAT and IVF_PQ datasets with four partitions,
  two separately committed index segments, and one unindexed fragment. IDs are
  interleaved across fragments to exercise global TopK/offset, filters, empty
  results, and row-ID materialization of vectors and nullable payload columns.

The representative index tests probe all four partitions and overfetch before
refinement to compare this fixed fixture with an independent exact oracle.
They do not assert that arbitrary ANN settings guarantee exhaustive recall.
The fixture generator checks both original payloads and physical index coverage.

The third-party build applies the Lance v11 community patch chain followed by
merged lance-c PR #83. Its patch records the upstream commit and the context
adaptation needed to retain the scalar-segment execution path from PR #79.
Previously extracted lance-c sources carrying the older patch chain must be
removed before rebuilding; the patch driver rejects the stale marker instead
of silently linking an old library.
