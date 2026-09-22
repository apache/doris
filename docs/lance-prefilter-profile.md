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

# Lance vector row-ID prefilter profiling

An explicit fragment list that covers every fragment in a fixed dataset snapshot does
not restrict an unfiltered vector query. Lance can omit the row-ID prefilter scan in
this case. The fragment selection remains attached to the scanner: indexed segment
selection, unindexed-fragment fallback, snapshot visibility, deletion masks and overlay
handling keep their existing semantics. A strict fragment subset or an actual filter
continues to use the normal prefilter path.

The following Doris counters describe Lance's ANN **row-ID prefilter loader**, not
returned TopK rows, HNSW comparisons, or the deletion mask. Scalar-index selection
vectors use a different loader and are not included in these counters.

| Counter | Meaning |
| --- | --- |
| `LancePrefilterLoads` | Number of row-ID prefilter loader executions started. |
| `LancePrefilterInputBatches` | Successfully consumed input batches. |
| `LancePrefilterInputRows` | Non-null input row IDs, including duplicates. |
| `LancePrefilterRowIds` | Sum of distinct row IDs in successfully completed allow sets. |
| `LancePrefilterLoadTime` | Total loader wall time, including input polling and set construction. |
| `LancePrefilterInputTime` | Wall time polling input batches, including upstream execution, I/O, decoding and scheduling. |
| `LancePrefilterBuildTime` | Wall time inserting row IDs into the allow set, measured once per batch. |

The timers overlap: do not add LoadTime to InputTime or BuildTime. They are not CPU
timers. Across multiple loaders or scanners they accumulate and can exceed query
wall time. RowIds is not peak resident memory and can count the same ID again when
separate loaders build separate sets. An interrupted or failed load may contribute
partial input counts without a completed set cardinality.

For a full-snapshot, unfiltered ANN query, no row-ID loader is needed and these
counters remain zero. Zero does not prove that no filtering occurred: native
visibility/deletion filtering and scalar-index selection vectors are independent.
The generic `LanceRowsScanned` counter can still include result materialization or
unindexed fallback work; it is not an exact count of distance comparisons.

To validate performance, hold the dataset version, query vectors, search parameters,
cache state and recall target constant. Compare serial and concurrent runs using QPS,
latency percentiles, process CPU and these counters. Removal of the redundant row-ID
scan does not by itself establish the size of the end-to-end latency improvement.
