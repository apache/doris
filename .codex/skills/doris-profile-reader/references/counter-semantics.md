# Counter Semantics

This file defines how to interpret every counter class. For individual counter names and source locations, use `source-profile-inventory.md`.

## Universal Priority

High-signal counters:

- `ExecTime`: time inside the operator. In merged profiles, check `max` and per-instance detail before comparing to query elapsed. Before calling it active CPU, qualify it with child wait evidence such as runtime-filter wait, data-arrival wait, dependency wait, scanner worker wait, or spill task queue wait. Do not mechanically subtract wait counters from `ExecTime`; scan-level `WaitForRuntimeFilter`, per-filter `RFx WaitTime`, scanner child timers, and merged operator timers may overlap or use different accumulation scopes.
- `InputRows`, `RowsProduced`, `RowsRead`, `ScanRows`, `ScanBytes`, bytes sent/received: data volume that explains active time.
- Operator-specific direct timers: scan CPU/I/O/decompression, hash table build/probe, aggregation build/merge, sort, analytic evaluation, serialization/compression, spill read/write.
- `MemoryUsage`, `MemoryUsagePeak`, `MemoryUsageHashTable`, `MemoryUsageBuildBlocks`, `MemoryUsageSortBlocks`: resource footprint.
- Spill counters: direct evidence that memory pressure changed the execution path.

Low-directness counters:

- `WaitForDependencyTime` and `WaitForDependency[...]Time`: dependency wait. Important for pipeline causality, not direct compute.
- `WaitForData0` and `WaitForDataN`: exchange receiver queue data wait. Usually upstream did not produce data yet.
- `WaitForRpcBufferQueue`, `WaitForBroadcastBuffer`, `WaitForLocalExchangeBufferN`: downstream or channel backpressure.
- `PendingFinishDependency`: sink finish dependency. Do not call it sink work.
- `WaitWorkerTime`, `BlockedByDependency`, `MemoryReserveTimes`, `MemoryReserveFailedTimes`: scheduler/dependency/memory state.
- `FirstBatchArrivalWaitTime`, `DataArrivalWaitTime`: data-arrival symptom; use with upstream operator evidence.
- FE `Plan Time`, `Schedule Time`, `Wait and Fetch Result Time`, and client/result fetch time: query lifecycle or client-facing timing. They can explain end-to-end latency, but do not rank them as BE operator active work.

## Common Operator Counters

- `InitTime`: local state/profile initialization. Usually not the bottleneck unless large in per-instance detail.
- `OpenTime`: opening child/local state/resources. Can matter for external scans/sinks or cold metadata, but verify with custom counters.
- `ExecTime`: primary active operator timer.
- `CloseTime`: close/finalization. For sinks it may include writer close, publish, commit, or file close; inspect custom sink timers.
- `ProjectionTime`: expression/projection after core operator work. Important if high with many output columns or expensive expressions.
- `RowsProduced`: source/operator output rows.
- `InputRows`: sink input rows.
- `BlocksProduced`, `OutputBlockBytes`, `MaxOutputBlockBytes`, `MinOutputBlockBytes`: vectorized block output volume and block-size skew.
- `MemoryUsage`, `MemoryUsagePeak`: current/peak memory tracked by the operator.
- `IsShuffled`, `IsColocate`, `FollowedByShuffledOperator`: plan/execution metadata, not cost by itself.
- `PendingFinishDependency`: wait for sink finalization dependency, not active work.

## Pipeline Task Counters

- `TaskCpuTime`: accumulated CPU attributed to a pipeline task. It can exceed wall time when summed.
- `ExecuteTime`: task execution loop time.
- `PrepareTime`, `OpenTime`, `GetBlockTime`, `SinkTime`, `CloseTime`: task lifecycle partitions.
- `WaitWorkerTime`: time waiting for worker scheduling.
- `NumScheduleTimes`, `NumYieldTimes`, `CoreChangeTimes`: scheduling behavior and task migration/yield signals.
- `MemoryReserveTimes`, `MemoryReserveFailedTimes`: memory reservation pressure; pair with blocked time and memory peaks.
- `BlockedByDependency`: count/state of dependency blocking, not CPU.

## Time Counter Name Patterns

- `*Time`, `*Timer`, `*Costs`: elapsed or accumulated time for the named action. Determine whether it is direct work or wait by the action name.
- `Wait*`, `*Wait*`, `Blocked*`, `Pending*`: wait/dependency/backpressure unless source code proves otherwise.
- `*CpuTime`: CPU-side work. Can be accumulated across threads.
- `*IOTimer`, `*ReadFileTime`, `*WriteFileTime`, `FileWriteTime`: I/O path work or I/O wait. Use bytes and cache counters to interpret.
- `*Serialize*`, `*Deserialize*`, `CompressTime`, `DecompressorTimer`: CPU transformation cost.
- `*EvalTime`, `ExprTime`, `ProjectionTime`, `VectorPredEvalTime`: expression or predicate CPU.

## Count/Volume Counter Name Patterns

- `*Rows`, `*RowsRead`, `*InputRows`, `*RowsProduced`: row volume.
- `*Bytes`, `*MemBytes`, `ScanBytes`, `CompressedBytesRead`, `UncompressedBytesRead`: byte volume or memory/storage size.
- `*Count`, `*Num`, `*Times`: event count.
- `*Filtered`, `*FilteredRows`, `Rows*Filtered`: rows skipped or filtered. High values can be good if they avoid downstream work.
- `*CacheHit*`, `*CacheMiss*`: cache effectiveness. Interpret with I/O timers.
- `*Peak`: peak observed value.

## Scan Counters

Common scan:

- `RowsRead`: rows returned by the scanner after scan-side pruning/filtering.
- `ScanRows`: rows scanned/read from storage or source. Larger than `RowsRead` means scan-side filtering/pruning removed rows.
- `ScanBytes`: bytes scanned. In OLAP scan source comments this represents uncompressed bytes from local plus remote reads.
- `NumScanners`: scanner instances created.
- `ScannerCpuTime`: direct scanner CPU. Primary scan bottleneck timer.
- `ScannerGetBlockTime`: time getting blocks from scanners.
- `ScannerFilterTime`: scan-side filter time.
- `ScannerWorkerWaitTime`: time scanner work waited in the scan worker pool. High value indicates scanner scheduling/thread-pool contention or too much scan concurrency, not scan CPU.
- `WaitForRuntimeFilter`, `AcquireRuntimeFilter`, and `RuntimeFilterInfo/RFx WaitTime`: scan-side wait before or during scan startup/filter application. These can make scan `ExecTime` look large even when storage CPU/I/O is small.
- `RFx InputRows`, `RFx FilterRows`, and `RFx AlwaysTrueFilterRows`: per-filter counters, often accumulated across parallel scan instances and filters. They may exceed a table's final scan rows when summed across filters/instances. Compare them per filter and against `ScanRows`, `RowsRead`, and target-side output; do not add them across filters as if they were disjoint.
- `MaxScanConcurrency`, `MinScanConcurrency`, `RunningScanner`: concurrency and skew.
- `ConditionCacheHit`, `ConditionCacheFilteredRows`: condition cache effectiveness.
- `PerScannerRunningTime`, `PerScannerRowsRead`, `PerScannerWaitTime`, `PerScannerProjectionTime`, `PerScannerPrepareTime`, `PerScannerOpenTime`: per-scanner skew and phase evidence.

OLAP scan storage path:

- `ReaderInitTime`, `ScannerInitTime`, `ProcessConjunctTime`: scan setup and predicate preparation.
- `BlockLoadTime`, `BlocksLoad`, `BlockFetchTime`, `BlockInitTime`, `BlockInitSeekTime`, `BlockInitSeekCount`: segment/block loading and seek overhead.
- `CompressedBytesRead`, `UncompressedBytesRead`, `IOTimer`, `DecompressorTimer`: storage I/O and decompression cost.
- `PredicateColumnReadTime`, `NonPredicateColumnReadTime`, `OutputColumnTime`, `LazyReadTime`, `LazyReadSeekTime`: column read and late materialization cost.
- `VectorPredEvalTime`, `ShortPredEvalTime`, `ExprFilterEvalTime`: predicate expression CPU.
- `RowsStatsFiltered`, `RowsKeyRangeFiltered`, `RowsConditionsFiltered`, `RowsZoneMapRuntimePredicateFiltered`, `RowsBloomFilterFiltered`, `RowsDelFiltered`, `SegmentDictFiltered`: pruning and delete/filter effectiveness. Runtime filters, especially min/max or storage-pushed filters, can show their benefit here even when a per-filter `FilterRows` counter is zero or misleading.
- `GenerateRowRangeByKeysTime`, `GenerateRowRangeByColumnConditionsTime`, `GenerateRowRangeByBloomFilterIndexTime`, `GenerateRowRangeByZoneMapIndexTime`, `GenerateRowRangeByDictTime`: index/range construction cost.
- Inverted-index counters (`InvertedIndex*`, `RowsInvertedIndexFiltered`): index reader/search/filtering cost and effectiveness.
- ANN counters (`AnnIndex*`, `AnnIvf*`): vector index search, conversion, cache, fallback, and filtering.
- Variant counters (`Variant*`, sparse-column counters): variant/sparse subcolumn materialization and filtering.
- `AdaptiveBatchPredictMaxRows`, `AdaptiveBatchPredictMinRows`: adaptive batch sizing metadata, not direct cost.

File/external scan:

- `FileScannerGetBlockTime`, `FileScannerPreFilterTimer`, `ApplyAllRuntimeFilters`: file scanner work and RF application.
- `FileNumber`, `EmptyFileNum`, `BatchSplitMode`: split/file shape.
- Connector-specific read/open/list counters should be treated as remote I/O or metadata cost; verify with bytes and file count.

## Join Counters

Hash join build sink:

- `BuildTime`, `BuildHashTableTime`, `BuildTableInsertTime`, `BuildExprCallTime`, `MergeBuildBlockTime`: direct build-side work.
- `InputRows`, `HashTableSize`, `MemoryUsageHashTable`, `MemoryUsageBuildBlocks`, `MemoryUsageBuildKeyArena`: build-side size and memory.
- `PublishTime`, `RuntimeFilterInfo`: runtime filter publication.
- `BroadcastJoin`, `JoinType`, `ShareHashTableEnabled`, `BuildShareHashTable`, `SkipProcess`: plan/state metadata.
- `AsofIndexExprTime`, `AsofIndexSortTime`, `AsofIndexGroupTime`: ASOF join index preparation cost.

Hash join probe:

- `ProbeRows`, `ProbeIntermediateRows`: probe-side volume and intermediate expansion.
- `ProbeExprCallTime`, `ProbeWhenSearchHashTableTime`, `ProbeWhenProbeSideOutputTime`, `ProbeWhenBuildSideOutputTime`, `NonEqualJoinConjunctEvaluationTime`, `JoinFilterTimer`, `BuildOutputBlock`, `FinishProbePhaseTime`: direct probe/output/filter work.
- `MemoryUsageProbeKeyArena`: probe key memory.
- `WaitForDependency[HASH_JOIN_BUILD_DEPENDENCY]Time` on probe is waiting for build readiness. It is not probe CPU.

Partitioned hash join:

- `Spilled`, `SpillBuildTime`, `SpillProbeTime`, `SpillRePartitionTime`, `SpillBuildRows`, `SpillProbeRows`, `SpillRecovery*`, `SpillMaxPartitionLevel`, `SpillTotalPartitions`: partitioned join spill path and recovery.
- `BuildRows`, `ProbeBlocksBytesInMem`, `SpillInMemRow`: partitioned memory/volume shape.

Nested loop/cross join:

- `LoopGenerateJoin`, `JoinConjunctsEvaluationTime`, `FilteredByJoinConjunctsTime`, `OutputTempBlocksTime`, `UpdateVisitedFlagsTime`: direct nested-loop work.
- Large probe/intermediate rows are usually more important than wait counters.

## Aggregation and Set Counters

Aggregation sink/source:

- `BuildTime`, `HashTableComputeTime`, `HashTableEmplaceTime`, `HashTableInputCount`, `HashTableSize`: primary group-by/hash aggregation cost.
- `MergeTime`, `DeserializeAndMergeTime`, `GetResultsTime`, `HashTableIterateTime`, `InsertKeysToColumnTime`, `InsertValuesToColumnTime`: merge/output phase.
- `ExprTime`, `DoLimitComputeTime`: expression and limit work.
- `MemoryUsageHashTable`, `MemoryUsageArena`, `MemoryUsageContainer`, `MemoryUsageSerializeKeyArena`: aggregation memory.
- `Spilled`, `SpillSerializeHashTableTime`, `SpillTotalTime`, `SpillTotalPartitions`, `SpillMaxPartitionLevel`: spill path.

Streaming/distinct aggregation:

- `StreamingAggTime`: streaming aggregation work.
- `BuildTime`, `HashTableComputeTime`, `HashTableEmplaceTime`, `HashTableInputCount`, `HashTableSize`: distinct/group state.

Set operators:

- `BuildTime` on `SET_SINK_OPERATOR`: build set/hash table.
- `ProbeTime`, `ExtractProbeDataTime` on `SET_PROBE_SINK_OPERATOR`: probe/extract set results.
- `GetDataTime`, `FilterTime`, `GetDataFromHashTableRows` on intersect/except: set output/filter work.

## Sort, Window, and Materialization Counters

Sort:

- `SortTime`, `AppendBatchTime`, `AppendBlockTime`, `MergeGetNext`, `MergeGetNextBlock`, `MemoryUsageSortBlocks`: direct sort and output work.
- Spill sort uses `SpillSort*`, `SpillMergeSortTime`, and common spill counters.
- `WaitForDependency[SORT_SOURCE_DEPENDENCY]Time` generally means waiting for the sink to finish sorting.

Partition sort:

- `HashTableBuildTime`, `SortedDataTime`, `SortedPartitionInputRows`, `SortedPartitionOutputRows`, `GetSortedTime`: partitioned/window sort setup and output.

Analytic/window:

- `ComputePartitionByTime`, `ComputeOrderByTime`, `ComputeAggDataTime`, `ComputeRangeBetweenTime`, `EvaluationTime`, `PartitionSearchTime`, `OrderSearchTime`, `RemoveRowsTime`: direct window function work.
- `Blocks`, `BlocksPeak`, `RemoveRows`, `RemoveCount`, `streaming mode`: window state and mode.

Materialization/select/repeat/table function:

- `ProjectionTime`, expression/evaluation timers, `RowsProduced`, and output bytes are the main proof.
- `MATERIALIZATION_OPERATOR` can be a top/lazy materialization node that fetches deferred columns by row id through `multiget_data_v2` RPCs. `MaxRpcTime` is the max elapsed RPC round-trip for a batch, not local projection CPU. Treat it as late row-id fetch latency unless child `RowIDFetcher` profiles, fetched rows/bytes, `MergeResponseTime`, and output row count prove a materialization-heavy workload.
- Do not rank a top/lazy materialization node above upstream scan/join/aggregation/memory evidence when it only materializes a small final row set. Trace `row_ids` or row-id table info back to the scan/join branch that produced those row ids, then report materialization separately as final-column fetch latency.
- `Repeat` expands grouping sets; compare input/output rows.
- `TableFunction` can multiply rows; compare produced rows and expression time.

## Exchange and Network Counters

Exchange source:

- `GetDataFromRecvrTime`, `DeserializeRowBatchTimer`, `DecompressTime`, `FilterTime`, `CreateMergerTime`, `MergeGetNext`, `MergeGetNextBlock`: direct receive/merge/decode work.
- `LocalBytesReceived`, `RemoteBytesReceived`, `DecompressBytes`, `BlocksProduced`: data volume.
- `WaitForData0`, `WaitForDataN`, `DataArrivalWaitTime`, `FirstBatchArrivalWaitTime`, `SendersBlockedTotalTimer(*)`, `MaxWaitForWorkerTime`, `MaxWaitToProcessTime`: wait/backpressure/scheduling signals.

Exchange sink:

- `SerializeBatchTime`, `CompressTime`, `DistributeRowsIntoChannelsTime`, `SplitBlockHashComputeTime`, `LocalSendTime`, `MergeBlockTime`, `SendNewPartitionTime`, `AddPartitionRequestTime`: direct send/shuffle work.
- `BytesSent`, `LocalBytesSent`, `LocalSentRows`, `UncompressedRowBatchSize`, `BlocksProduced`, `OverallThroughput`: volume/throughput.
- `RpcMaxTime`, `RpcAvgTime`, `RpcMinTime`, `RpcSumTime`, `RpcCount` on `DATA_STREAM_SINK_OPERATOR`: remote send RPC latency. A high max/avg with meaningful bytes and small serialize/compress time points to network, receiver, or RPC path latency, not local sink CPU.
- `WaitForRpcBufferQueue`, `WaitForBroadcastBuffer`, `WaitForLocalExchangeBufferN`: downstream backpressure or queue capacity wait.

Local exchange and multicast:

- `ComputeHashValueTime`, copy/channel push/pop timers, local bytes/rows, and buffer waits explain in-BE reshuffle or fanout.
- Long local exchange wait usually reflects receiver/sender imbalance, not compute in the local exchange itself.

## Sink and Writer Counters

Result/file/mysql/flight sinks:

- `AppendBatchTime`, `BytesSent`, `FileWriteTime`, `FileWriterCloseTime`, `ConvertBlockToArrowBatchTime`: direct output cost.
- Result sinks can be slow because clients fetch slowly; look for send/write wait and bytes.

OLAP/table/load sinks:

- `AppendNodeChannelTime`, `CloseWaitTime`, `CloseWriterTime`, `CloseLoadTime`, `CommitTxnTime`, `BuildRowsetTime`, `AddPartitionRequestTime`: write/load/finalization phases.
- High close/commit is sink-side only if custom writer timers agree.

External table sinks:

- Hive/Iceberg/MaxCompute/JDBC/TVF counters represent connector write, file write, delete-file write, and close/commit costs. Use file counts, bytes, writer close/write timers, and connector-specific errors/logs.
- Iceberg delete/merge/sort spill counters show delete file generation, merge output, and spill path.

Blackhole/cache/memory scratch/dictionary sinks:

- Blackhole `BytesProcessed` is volume consumed, not output I/O.
- Cache counters show cache fill/read size and tablet id metadata.
- Memory scratch conversion timers show in-memory Arrow/result conversion.
- Dictionary sink counters should be read as dictionary build/publish/write path.

## Runtime Filter Counters

See `runtime-filters.md`. In short:

- `RuntimeFilterInfo` on join/build/sink shows produced filters and target metadata.
- `WaitForRuntimeFilter` and `AcquireRuntimeFilter` on scans/sources are waits for filters.
- `RFx InputRows`, `RFx FilterRows`, `RFx AlwaysTrueFilterRows`, `RFx WaitTime` tell whether each RF was useful.
- Waiting for a runtime filter is worthwhile only if it filters enough scan rows or prevents expensive downstream work.
- Runtime-filter wait counters can appear on scans, multicast/source operators, or dynamic/top-N filter paths. Treat them as wait/synchronization evidence first, then use the plan and detail profile to find the producer and target.

## Spill Counters

Common spill counters are direct evidence of a different execution path:

- `SpillTotalTime`, `SpillWriteTime`, `SpillRecoverTime`: overall spill cost.
- `SpillWriteTaskWaitInQueueTime`, `SpillReadTaskWaitInQueueTime`: spill task queue wait.
- `SpillWriteFileTime`, `SpillReadFileTime`: spill file I/O.
- `SpillWriteSerializeBlockTime`, `SpillReadDeserializeBlockTime`: CPU transform cost.
- `SpillWriteBlockCount`, `SpillWriteBlockBytes`, `SpillWriteRows`, `SpillWriteFileBytes`, `SpillWriteFileTotalCount`: write volume.
- `SpillReadBlockCount`, `SpillReadBlockBytes`, `SpillReadRows`, `SpillReadFileBytes`, `SpillReadFileCount`: read volume.
- `SpillMaxRowsOfPartition`, `SpillMinRowsOfPartition`, `SpillMaxPartitionLevel`, `SpillTotalPartitions`: partition skew and spill fanout.

If spill exists, it is usually more important than generic wait counters.
