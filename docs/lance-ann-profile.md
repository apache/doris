<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lance ANN profile timings

`FileScannerV2` accumulates scanner initialization, open, block-read, and close
wall time. Range acquisition and split preparation are nested within those
calls, so their counters are not additional time. Scanner worker scheduling
wait is reported separately. `LanceScannerReadTime` measures time spent calling
the Lance scanner, including Rust execution and waits. Doris scanner CPU time
does not include work done on Lance's CPU pool.

The following counters expose the work within an indexed vector search:

| Counter | Scope |
| --- | --- |
| `LanceIndexOpenTime` | Index-handle lookup/open, including metadata reads on a miss. |
| `LanceIVFPartitionRankingTime` | Partition ranking, including its CPU dispatch wait. |
| `LanceIndexPartitionLoadTime` | Partition cache lookup, coalesced-load wait, and read/decode on a miss. Also measured on cache hits. |
| `LanceIndexPartitionPrepareTime` | Partition load and per-partition filter preparation. On the streaming path, shared-filter waiting overlaps loading. |
| `LanceIndexPrefilterWaitTime` | Waiting for the shared prefilter to become ready. This is distinct from building the filter. |
| `LanceIndexCpuQueueWaitTime` | Delay before a dispatched search or result-materialization CPU task starts. |
| `LanceIndexSearchTime` | Search of prepared partitions on the CPU pool, including query preparation and any per-partition result construction. |
| `LanceIndexQueryPrepareTime` | Distance-calculator / lookup-table construction in the IVF flat sub-index (including quantized storage). |
| `LanceIndexDistanceTopKTime` | Candidate filtering, distance evaluation, and heap updates in that sub-index. These operations are fused in fast-scan paths. |
| `LanceIndexResultMaterializeTime` | Converting result heaps into Arrow arrays and batches, excluding final global sorting. |
| `LanceANNPartitionExecTime`, `LanceANNSubIndexExecTime`, `LanceANNBatchExecTime` | Baseline elapsed times reported by the corresponding Lance ANN operators. These include asynchronous waits. |
| `LanceSortComputeTime`, `LanceSortMergeComputeTime` | DataFusion sort / sort-preserving merge operator compute times. |
| `LanceTakeExecTime` | Baseline time reported by Lance's take operator within the scan plan. Doris second-phase row-ID fetch has separate counters. |
| `LanceVectorDistanceComputeTime` | Baseline reported by the vector-distance operator, e.g. refinement or an unindexed tail. |

Timings accumulate across partitions, tasks, and index segments. They are
**nested and may overlap**, so summing them does not reconstruct query wall time.
In particular, partition preparation contains loading; search contains query
preparation and distance/TopK work; ANN operator baselines contain downstream
search stages and waits. A zero counter can mean the corresponding operator or
path was not used. Detailed sub-index timers currently cover IVF flat sub-indices;
other sub-indices are visible through the encompassing search timer.

`LancePrefilterLoadTime` includes `LancePrefilterInputTime` and
`LancePrefilterBuildTime`; do not add these three together. A segment-scoped
search without a predicate can avoid constructing the row-ID allowlist, while
still respecting deletions and fragment visibility. Filter-readiness waiting can
therefore remain nonzero even when the row-ID materialization counters are zero.

For a warm query with no execution I/O and no prefilter materialization, inspect
CPU queue wait, query preparation, distance/TopK, and result/sort timers. For cold
queries, inspect partition load together with execution bytes, requests, and
partition cache misses. Use repeated queries and the operator-level elapsed
times to assess latency; cumulative parallel stage times alone are not a critical
path trace.

## Query parallelism

`vector_search` accepts an optional `"query_parallelism"` integer:

- `0` (also the default when omitted): let Lance choose the parallelism.
- `-1`: use the available Lance CPU parallelism.
- Positive values: request that many concurrent partition searches, capped by
  Lance's compute pool and execution-plan limits.

For example, add `"query_parallelism" = "4"` alongside `"nprobes" = "64"`.
This controls concurrency inside a Lance search, independently of Doris scan
instances. Increasing it can increase intermediate candidates and memory usage;
measure both single-query latency and concurrent throughput. EXPLAIN displays an
explicit setting as `lanceQueryParallelism`.

## Second-phase row-ID fetch

Each `RowIDFetcher: BackendId:...` profile also reports:

| Counter | Scope |
| --- | --- |
| `LanceRowIdFetchCalls` | Non-empty dataset `take_rows` calls. |
| `LanceRowIdFetchRows` | Rows converted from successful returned batches, including duplicate requested row IDs. |
| `LanceDataCacheBytesReadFromCache` | Logical data-file bytes served by Foyer for the dataset handles used by this fetch. |
| `LanceDataCacheBytesReadFromRemote` | Logical data-file bytes served through the Foyer origin path for those handles. |

Byte counts are collected after closing the reader and summed across dataset
handles in the fetch RPC. They exclude block-alignment amplification, metadata,
and index reads. With Foyer disabled or for paths outside its data-file wrapper,
zero values do not imply zero physical IO. The current take API does not expose
physical request counts or a separate decode timer; scanner-plan IO counters
must not be substituted for them.

`MATERIALIZATION_OPERATOR.ExecTime` includes its synchronous fetch wait once.
`MaxRpcTime` is nested within that execution time, not an additional duration.
