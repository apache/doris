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

# Lance vector search approximation modes

`vector_search` accepts the optional `approx_mode` property to select Lance's
speed/accuracy policy for approximate vector search. The accepted values are
`fast`, `normal`, and `accurate`, ignoring case and surrounding whitespace.
Other values, including numeric strings, are rejected during SQL analysis.

```sql
SELECT id, _distance
FROM vector_search(
    "table" = "lance_catalog.default.documents",
    "column" = "embedding",
    "query_vector" = "[1,0,0]",
    "top_k" = "10",
    "metric" = "l2",
    "approx_mode" = "normal"
)
ORDER BY _distance, id;
```

The vector dimension must match the source column. `EXPLAIN` includes
`lanceApproxMode=NORMAL` when the property is explicitly specified. Doris sends
the mode to each Lance scanner through `lance_scanner_set_approx_mode`; it does
not implement a separate approximation algorithm. Omitting the property leaves
the native option unset, preserving Lance's default, currently `normal`.
The option also applies to multi-vector queries through the same scanner setup.

## What changes between modes

The behavior depends on the index and execution path. These details describe
the pinned lance-c `a51ab47a9c814a746cb1714c99fad93dd3a188b8`, which uses Lance
`68c12dfd7efe02f90ad2d7f3a239a7eb64884e57`.

| Path | `fast` | `normal` | `accurate` |
| --- | --- | --- | --- |
| RabitQ raw-query scoring | Uses one-bit scores even for a multi-bit index, skipping extension bits and the corresponding lower-bound gate. | Uses the standard quantized lookup table, with extension-bit refinement when available. | Uses a higher-precision lookup table, retaining extension-bit refinement when available. |
| HNSW with prefiltering | Can select ACORN filtered graph traversal. | Uses the regular filtered graph traversal. | Uses the same traversal policy as `normal`. |
| IVF_FLAT or exhaustive flat scoring | Does not add quantization or reduce the configured probe count. | Same scoring policy. | Same scoring policy. |

RabitQ's standard and higher-precision lookup tables use 8-bit and 16-bit
quantization respectively. This changes distance estimation, not the number of
bits stored in the index. Other RabitQ paths, including residual-query scoring,
can exhibit different or smaller differences. In HNSW, an all-pass filter uses
ordinary traversal; a very selective filter can trigger a flat fallback. ACORN
can also fall back when it does not find enough candidates. Consequently,
neither the speed ordering nor the recall ordering is a universal guarantee.

`accurate` still performs approximate candidate selection. It does not read and
score every original vector, and does not guarantee exact Top-K or exact
`_distance` on a quantized index.

## Relationship to other options

| Option | What it controls |
| --- | --- |
| `approx_mode` | The index algorithm's approximation policy inside the chosen search path. |
| `nprobes` | IVF partitions searched; unchanged by the mode. |
| `ef` | HNSW search width; unchanged by the mode. |
| `refine_factor` | The candidate pool reranked using original vectors. It can improve scores and recall, but cannot recover every candidate missed earlier. |
| `use_index=false` | Exhaustive flat search over the selected rows. The approximation mode does not turn this into ANN. |
| `query_parallelism` | Partition-search concurrency; independent of the approximation mode. |

Multi-vector queries retain Doris's existing mandatory refinement for consistent
row-level scores across indexed and unindexed splits. Their candidate selection
can still be approximate. Distributed merging and index coverage are unchanged.
For rolling upgrades, all participating BEs must understand the new option to
apply an explicitly requested mode consistently; older BEs ignore the optional
Thrift field.

## Local validation

The following experiment exercises the pinned **native lance-c scanner API**,
including the setter used by Doris. It is not an end-to-end Doris SQL benchmark.
The native library used an unoptimized development build; timings include
scanner creation, result conversion through the Arrow C Data Interface, and
scanner close. Treat them as evidence of behavior on this fixture, not expected
production latency or a general speedup claim.

### Method

- Generate 16,384 independent 128-dimensional Float32 Gaussian vectors, followed
  by 128 independent query vectors. Use a xorshift64 generator seeded with
  `20261029` and a Box-Muller transform. Set `row_id` to `0..16383` and
  `category = row_id % 10`.
- Build three fully covering indexes on the same `embedding` column: IVF_RQ
  with 32 partitions and 5 bits, IVF_FLAT with 32 partitions, and IVF_HNSW_FLAT
  with 32 partitions, 16 graph edges, and construction `ef=100`.
- Explicitly select each index's segment UUIDs so that index ordering in the
  manifest cannot change which algorithm is tested.
- Use squared L2, `k=10`, `nprobes=32`, and `query_parallelism=1`. The HNSW case
  additionally sets `ef=64`, `prefilter=true`, and `category < 5`, retaining
  about 50% of rows. Test IVF_RQ both without refinement and with
  `refine_factor=4`.
- Compute exact Top-10 ground truth independently with NumPy, including the
  predicate for the filtered case. Recall@10 is the number of returned IDs in
  the exact Top-10 divided by 10, averaged over all queries.
- Warm up each mode with eight queries. Run three rounds of all 128 queries
  per mode, rotating the mode order per query and round. Report the median
  and p95 of 384 samples per mode.
- For each case, rerun all 128 queries with the mode omitted and compare both
  ordered IDs and Float32 distances against explicit `normal`.

The data seed fixes the input vectors; rebuilding the ANN indexes can change
their training and graph layout. The measurements below describe one built
fixture, not bit-for-bit reproducibility across index rebuilds.

### Results

| Index / refinement | Mode | Recall@10 | Median (ms) | p95 (ms) |
| --- | --- | ---: | ---: | ---: |
| IVF_RQ / none | fast | 26.80% | 14.57 | 16.03 |
| IVF_RQ / none | normal | 91.72% | 15.90 | 18.12 |
| IVF_RQ / none | accurate | 91.72% | 20.52 | 22.91 |
| IVF_RQ / 4 | fast | 54.30% | 20.50 | 22.07 |
| IVF_RQ / 4 | normal | 99.92% | 23.44 | 25.64 |
| IVF_RQ / 4 | accurate | 99.92% | 28.19 | 30.58 |
| IVF_FLAT / none | fast | 100% | 25.71 | 29.62 |
| IVF_FLAT / none | normal | 100% | 25.78 | 29.82 |
| IVF_FLAT / none | accurate | 100% | 25.71 | 30.74 |
| IVF_HNSW_FLAT / 50% prefilter | fast | 100% | 40.45 | 44.04 |
| IVF_HNSW_FLAT / 50% prefilter | normal | 100% | 44.51 | 49.86 |
| IVF_HNSW_FLAT / 50% prefilter | accurate | 100% | 44.48 | 50.05 |

Every search returned ten rows. The omitted mode matched explicit `normal`
exactly for all 128 queries in each case. IVF_FLAT and the filtered HNSW case
returned identical ordered IDs and distances across all three modes.

Without refinement, `normal` and `accurate` had the same mean recall but
different ordered IDs on 89 of 128 queries; their distances differed as well.
An additional pass computed the mean absolute error between each returned
distance and independently recomputed original-vector squared L2: `fast`
30.7325, `normal` 0.7986, and `accurate` 0.7783. The higher-precision lookup
slightly reduced score error on this fixture without increasing mean recall.
With refinement factor 4, `normal` and `accurate` produced identical results,
while `fast` still missed many exact neighbors. Refinement therefore does not
make the three candidate-selection policies interchangeable.

To compare modes through Doris, run the SQL example with each mode, keeping
the snapshot, query, filter, probe count, search width, concurrency, and
refinement fixed. Use the same query with `use_index=false` as the exact
baseline, and compare returned IDs separately from distances. Measure on
representative data before changing the default policy.

### Source references

- [Native approximation mode API](https://github.com/lance-format/lance-c/blob/a51ab47a9c814a746cb1714c99fad93dd3a188b8/include/lance/lance.h)
- [RabitQ distance kernels and mode tests](https://github.com/lance-format/lance/blob/68c12dfd7efe02f90ad2d7f3a239a7eb64884e57/rust/lance-index/src/vector/bq/storage.rs)
- [HNSW filtered traversal policy](https://github.com/lance-format/lance/blob/68c12dfd7efe02f90ad2d7f3a239a7eb64884e57/rust/lance-index/src/vector/hnsw/builder.rs)
