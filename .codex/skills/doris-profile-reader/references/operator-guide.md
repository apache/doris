# Operator Guide

This guide covers the major Doris runtime profile operator families. Use `source-profile-inventory.md` for exact source registration lines and counter/source anchors.

## Scan Operators

`OLAP_SCAN_OPERATOR` reads Doris internal storage. First check `ScanRows`, `RowsRead`, `ScanBytes`, `ScannerCpuTime`, `ScannerGetBlockTime`, `ScannerWorkerWaitTime`, `IOTimer`, `DecompressorTimer`, predicate timers, lazy read timers, runtime-filter wait/filter counters, and row-filter counters. A long scan is important when active scanner timers and large bytes/rows agree. If `ExecTime` is mostly explained by `RuntimeFilterInfo/RFx WaitTime`, `AcquireRuntimeFilter`, or `WaitForRuntimeFilter`, report it as RF wait latency rather than storage CPU. If the scan includes `TOPN OPT`, `TopNFilterSourceNodeIds`, or a `SharedPredicate`, check DetailProfile for `WaitForRuntimeFilter`; merged `RFx WaitTime` can be zero while a Top-N/dynamic filter wait still inflates scan `ExecTime`. A long `WaitForDependency[...]Time` is not scan CPU. High `ScannerWorkerWaitTime` means scan task queue/thread-pool pressure.

`FILE_SCAN_OPERATOR` reads external files through file scanner and connector code. Prioritize file count/split shape, file read/open/list timers, `FileScannerGetBlockTime`, `ApplyAllRuntimeFilters`, scan rows/bytes, and connector I/O. Metadata/listing time can dominate small queries; data read/decode dominates large ones.

`JDBC_SCAN_OPERATOR` reads remote JDBC sources. Prioritize remote query/read/fetch timers, rows, bytes, and network/serialization. A slow JDBC scan may be remote database latency rather than Doris CPU.

`GROUP_COMMIT_SCAN_OPERATOR` scans group-commit buffered data. Prioritize rows, blocks, group-commit state, and wait/dependency only as queue state.

`SCHEMA_SCAN_OPERATOR` reads schema/system metadata. Usually small. If slow, check metadata service/catalog lookup timers and rows produced.

`META_SCAN_OPERATOR` reads internal metadata. Treat it like a metadata scan; long active time points to metadata lookup or serialization, not table data scan.

`DATA_GEN_SCAN_OPERATOR` generates rows. Prioritize generated row count and expression/generation active timers.

`REC_CTE_SCAN_OPERATOR` reads recursive CTE state. Use recursive CTE source/sink counters and rows per iteration to find expansion.

`CACHE_SOURCE_OPERATOR` reads cached intermediate data. Prioritize cache tablet/id metadata, cache hit/read counters, rows/bytes, and active read time.

`EMPTY_SET_OPERATOR` or `EMPTY_SET_SOURCE_OPERATOR` produces no data. It should not be the bottleneck unless profile setup/open time is anomalously high.

## Exchange Operators

`EXCHANGE_OPERATOR` receives data from sender queues. Prioritize `GetDataFromRecvrTime`, deserialize/decompress/filter/merge timers, local/remote bytes received, and produced rows. `WaitForData0`/`WaitForDataN`, `DataArrivalWaitTime`, and `FirstBatchArrivalWaitTime` mean the receiver waited for data. They usually point to upstream work or network/backpressure, not exchange CPU.

`DATA_STREAM_SINK_OPERATOR` sends data across fragments. Prioritize `SerializeBatchTime`, `CompressTime`, `DistributeRowsIntoChannelsTime`, `SplitBlockHashComputeTime`, `LocalSendTime`, `BytesSent`, `LocalBytesSent`, `LocalSentRows`, `RpcMaxTime`/`RpcAvgTime`, and throughput. High RPC time with non-trivial bytes is exchange/network evidence even when local serialization is small. `WaitForRpcBufferQueue`, `WaitForBroadcastBuffer`, and `WaitForLocalExchangeBufferN` are buffer/backpressure waits.

`LOCAL_EXCHANGE_OPERATOR` and `LOCAL_EXCHANGE_SINK_OPERATOR` reshuffle data inside one BE. Prioritize hash computation, copy/push/pop timers, local bytes/rows, and skew. Local exchange waits indicate sender/receiver imbalance or downstream pressure.

`LOCAL_MERGE_SORT_SOURCE_OPERATOR` receives locally sorted streams. Prioritize merge/get-next/sorted output active timers and rows. A large dependency wait usually means it waited for sort sink completion.

`MULTI_CAST_DATA_STREAM_SINK_OPERATOR` and `MULTI_CAST_DATA_STREAM_SOURCE_OPERATOR` fan out data to multiple consumers. Prioritize copied bytes/rows, channel buffer state, source wait for runtime filters when present, and downstream imbalance. Wait counters identify which consumer/channel delayed progress.

## Aggregation Operators

`AGGREGATION_SINK_OPERATOR`, `AGGREGATION_OPERATOR`, `BUCKETED_AGGREGATION_SINK_OPERATOR`, `BUCKETED_AGGREGATION_OPERATOR`, `PARTITIONED_AGGREGATION_SINK_OPERATOR`, and `PARTITIONED_AGGREGATION_OPERATOR` perform hash/merge aggregation. Prioritize `InputRows`, `RowsProduced`, `BuildTime`, `HashTableComputeTime`, `HashTableEmplaceTime`, `HashTableInputCount`, `HashTableSize`, `MergeTime`, `DeserializeAndMergeTime`, `GetResultsTime`, `HashTableIterateTime`, memory counters, and spill counters. Large dependency waits on aggregation source often mean it waited for sink/build completion.

`STREAMING_AGGREGATION_OPERATOR` aggregates when input order/streaming mode allows less state. Prioritize `StreamingAggTime`, build/hash table counters, input/output rows, and whether memory/hash table is unexpectedly high.

`DISTINCT_STREAMING_AGGREGATION_OPERATOR` handles distinct streaming aggregation. Prioritize distinct hash table compute/emplace, input rows, hash table size, and rows produced.

## Join Operators

`HASH_JOIN_SINK_OPERATOR` builds hash join state and runtime filters. Prioritize `InputRows`, `BuildTime`, `BuildHashTableTime`, `BuildTableInsertTime`, `BuildExprCallTime`, `MergeBuildBlockTime`, `MemoryUsageHashTable`, build memory arenas, `PublishTime`, and `RuntimeFilterInfo`. Build dependency waits are sequencing state, not build CPU.

`HASH_JOIN_OPERATOR` probes hash join state. Prioritize `ProbeRows`, `ProbeIntermediateRows`, `ProbeExprCallTime`, `ProbeWhenSearchHashTableTime`, non-equi conjunct time, output block build time, rows produced, and probe memory. Long wait for build dependency means probe waited for the build side.

`PARTITIONED_HASH_JOIN_SINK_OPERATOR` and `PARTITIONED_HASH_JOIN_PROBE_OPERATOR` are partitioned/spill-capable join paths. Prioritize build/probe rows, in-memory bytes, spill repartition/build/probe/recovery timers, spill rows/blocks, partition level/count, and memory.

`CROSS_JOIN_SINK_OPERATOR` and `CROSS_JOIN_OPERATOR` are nested-loop style join paths. Prioritize build rows, probe rows, intermediate rows, `LoopGenerateJoin`, join conjunct evaluation/filter time, and output temp block time. Large output cardinality is often the root cause. If the cross join is only stitching together one-row scalar-subquery results, large dependency waits on the cross join are branch fan-in latency; inspect the child branch scans/joins/aggregations before blaming cross-join compute.

`NestedLoopJoinBuildSinkOperatorX` and `NestedLoopJoinProbeOperatorX` appear as cross join operators in profiles. Interpret them with the cross-join counters.

## Sort and Analytic Operators

`SORT_SINK_OPERATOR` receives rows and sorts them. Prioritize input rows, sort/append timers, memory usage for sort blocks, and spill counters.

`SORT_OPERATOR` outputs sorted rows. Prioritize merge/get-next timers, rows produced, output bytes, and source dependency waits only as sink-completion waits.

`SPILL_SORT_SINK_OPERATOR` and `SPILL_SORT_SOURCE_OPERATOR` are sort paths with spill. Prioritize common spill counters, merge sort time, read/write file times, spilled bytes/rows, and memory.

`PARTITION_SORT_SINK_OPERATOR` and `PARTITION_SORT_OPERATOR` sort by partition for analytic/window work. Prioritize hash table build, sorted data time, sorted partition input/output rows, and get-sorted time.

`ANALYTIC_EVAL_SINK_OPERATOR` and `ANALYTIC_EVAL_OPERATOR` evaluate window functions. Prioritize input rows, partition/order/range compute timers, evaluation time, search timers, remove rows/time, blocks peak, and rows produced. Wait for dependency usually means waiting for buffered window state.

## Set, Union, Repeat, and Projection Operators

`UNION_SINK_OPERATOR` and `UNION_OPERATOR` combine child outputs. Prioritize input/output rows, copy/projection time, and child imbalance. Union is rarely the bottleneck unless it moves many rows or projects expensive expressions.

`INTERSECT_OPERATOR`, `EXCEPT_OPERATOR`, `SET_SINK_OPERATOR`, and `SET_PROBE_SINK_OPERATOR` build/probe set state. Prioritize build/probe/get-data/filter timers, hash table rows, extracted rows, memory, and rows produced.

`REPEAT_OPERATOR` expands rows for grouping sets/rollups. Compare input rows to rows produced and output bytes before blaming later exchange or aggregation. A repeat/rollup can create hundreds of millions of rows; the repeat operator's own active time may be moderate, while the real cost appears immediately after it as local hash shuffle, data stream exchange, and aggregation over the expanded rows. In that pattern, downstream wait counters are symptoms and the proof is repeat output volume plus post-repeat active work and skew.

`MATERIALIZATION_OPERATOR` materializes slots/projections. Prioritize projection/expression time, output rows/bytes, child `RowIDFetcher` profile info, `MergeResponseTime`, and fetched row-id volume. A high materialization time can also be top/lazy materialization: the operator sends row-id based `multiget_data_v2` RPCs to fetch deferred columns after upstream pruning/top-N. In that case `MaxRpcTime` is remote row-id fetch latency for a batch, not local materialization CPU. Do not promote a top materialization node above upstream scan/join/aggregation evidence when it handles only a small final row set; trace the `row_ids` table back to the branch that produced those rows and report materialization as final fetch latency unless fetched rows/bytes dominate the workload.

`SELECT_OPERATOR` applies predicates/projections. Prioritize predicate/expression/projection active timers and rows filtered/produced.

`TABLE_FUNCTION_OPERATOR` expands rows through table functions. Compare input rows to output rows and expression/table-function evaluation time.

`ASSERT_NUM_ROWS_OPERATOR` verifies scalar-subquery row count. Usually not a performance bottleneck; check rows produced and assertion metadata if it appears.

## Recursive CTE Operators

`REC_CTE_SOURCE_OPERATOR`, `REC_CTE_SINK_OPERATOR`, `REC_CTE_ANCHOR_SINK_OPERATOR`, and `REC_CTE_SCAN_OPERATOR` implement recursive CTE data flow. Prioritize iteration counts if present, rows in/out per stage, materialized state size, and waits between recursive producer/consumer dependencies. Long waits usually indicate iteration sequencing.

## Result and Output Sinks

`RESULT_SINK_OPERATOR` and `ResultSink` return rows to the client. Prioritize bytes/rows sent, append/serialize/send timers, and client/network effects. A slow client can make the result sink look slow.

`RESULT_FILE_SINK_OPERATOR` writes query results to files. Prioritize `FileWriteTime`, `FileWriterCloseTime`, bytes, file count, and serialization/compression.

`MEMORY_SCRATCH_SINK_OPERATOR` writes to in-memory scratch/results. Prioritize conversion timers such as Arrow batch conversion, rows, and bytes.

`BLACKHOLE_SINK_OPERATOR` consumes output without writing. Prioritize `BytesProcessed`/rows only as volume; it should not be I/O-bound.

## Table and Connector Sinks

`OLAP_TABLE_SINK_OPERATOR`, `OlapTableSinkOperatorX`, and `OlapTableSinkV2OperatorX` write into Doris storage. Prioritize append/channel timers, rowset build, close/load/commit timers, tablet/channel skew, rows/bytes, and memory. Close/commit only proves sink cost when writer timers agree.

`GROUP_COMMIT_OLAP_TABLE_SINK_OPERATOR` and `GROUP_COMMIT_BLOCK_SINK_OPERATOR` write through group commit. Prioritize append blocks, group commit flush/queue/commit timers, rows/bytes, and close/wait states.

`HIVE_TABLE_SINK_OPERATOR`, `ICEBERG_TABLE_SINK_OPERATOR`, `SPILL_ICEBERG_TABLE_SINK_OPERATOR`, `ICEBERG_DELETE_SINK_OPERATOR`, `ICEBERG_MERGE_SINK_OPERATOR`, `MAXCOMPUTE_TABLE_SINK_OPERATOR`, `JDBC_TABLE_SINK_OPERATOR`, and `TVF_TABLE_SINK_OPERATOR` write to external systems. Prioritize connector write time, file write/close time, delete-file/write-file counts, commit/close timers, bytes/rows, and spill counters for spill-capable paths. External system latency may dominate.

`DICTIONARY_SINK_OPERATOR` builds/publishes dictionary data. Prioritize build/publish/write timers, rows, bytes, and memory.

`CACHE_SINK_OPERATOR` writes cacheable intermediate data. Prioritize bytes written, cache write timers, tablet/cache metadata, and memory.

## Source Coverage Index

The narrative sections above cover these factory-created plan nodes and data sinks from the local source inventory:

Plan nodes:

`OLAP_SCAN_NODE`, `GROUP_COMMIT_SCAN_NODE`, `JDBC_SCAN_NODE`, `FILE_SCAN_NODE`, `EXCHANGE_NODE`, `AGGREGATION_NODE`, `BUCKETED_AGGREGATION_NODE`, `HASH_JOIN_NODE`, `CROSS_JOIN_NODE`, `UNION_NODE`, `SORT_NODE`, `PARTITION_SORT_NODE`, `ANALYTIC_EVAL_NODE`, `MATERIALIZATION_NODE`, `INTERSECT_NODE`, `EXCEPT_NODE`, `REPEAT_NODE`, `TABLE_FUNCTION_NODE`, `ASSERT_NUM_ROWS_NODE`, `EMPTY_SET_NODE`, `DATA_GEN_SCAN_NODE`, `SCHEMA_SCAN_NODE`, `META_SCAN_NODE`, `SELECT_NODE`, `REC_CTE_NODE`, `REC_CTE_SCAN_NODE`.

Data sinks:

`DATA_STREAM_SINK`, `RESULT_SINK`, `DICTIONARY_SINK`, `GROUP_COMMIT_OLAP_TABLE_SINK`, `GROUP_COMMIT_BLOCK_SINK`, `HIVE_TABLE_SINK`, `ICEBERG_TABLE_SINK`, `ICEBERG_DELETE_SINK`, `ICEBERG_MERGE_SINK`, `MAXCOMPUTE_TABLE_SINK`, `JDBC_TABLE_SINK`, `MEMORY_SCRATCH_SINK`, `RESULT_FILE_SINK`, `MULTI_CAST_DATA_STREAM_SINK`, `BLACKHOLE_SINK`, `TVF_TABLE_SINK`, `OLAP_TABLE_SINK`.

Operator classes:

`AggSinkOperatorX`, `AggSourceOperatorX`, `AnalyticSinkOperatorX`, `AnalyticSourceOperatorX`, `AssertNumRowsOperatorX`, `BucketedAggSinkOperatorX`, `BucketedAggSourceOperatorX`, `CacheSourceOperatorX`, `DataGenSourceOperatorX`, `DictSinkOperatorX`, `DistinctStreamingAggOperatorX`, `EmptySetSourceOperatorX`, `ExchangeSinkOperatorX`, `ExchangeSourceOperatorX`, `FileScanOperatorX`, `GroupCommitBlockSinkOperatorX`, `GroupCommitOperatorX`, `HashJoinBuildSinkOperatorX`, `HashJoinProbeOperatorX`, `HiveTableSinkOperatorX`, `IcebergDeleteSinkOperatorX`, `IcebergMergeSinkOperatorX`, `IcebergTableSinkOperatorX`, `JDBCScanOperatorX`, `JdbcTableSinkOperatorX`, `LocalExchangeSinkOperatorX`, `LocalExchangeSourceOperatorX`, `LocalMergeSortSourceOperatorX`, `MCTableSinkOperatorX`, `MemoryScratchSinkOperatorX`, `MetaScanOperatorX`, `MultiCastDataStreamSinkOperatorX`, `MultiCastDataStreamerSourceOperatorX`, `NestedLoopJoinBuildSinkOperatorX`, `NestedLoopJoinProbeOperatorX`, `OlapScanOperatorX`, `OlapTableSinkOperatorX`, `OlapTableSinkV2OperatorX`, `PartitionSortSinkOperatorX`, `PartitionSortSourceOperatorX`, `PartitionedAggSinkOperatorX`, `PartitionedAggSourceOperatorX`, `PartitionedHashJoinProbeOperatorX`, `PartitionedHashJoinSinkOperatorX`, `RecCTEAnchorSinkOperatorX`, `RecCTEScanOperatorX`, `RecCTESinkOperatorX`, `RecCTESourceOperatorX`, `RepeatOperatorX`, `ResultFileSinkOperatorX`, `ResultSinkOperatorX`, `SchemaScanOperatorX`, `SelectOperatorX`, `SortSinkOperatorX`, `SortSourceOperatorX`, `SpillIcebergTableSinkOperatorX`, `SpillSortSinkOperatorX`, `SpillSortSourceOperatorX`, `StreamingAggOperatorX`, `TVFTableSinkOperatorX`, `TableFunctionOperatorX`, `UnionSinkOperatorX`, `UnionSourceOperatorX`.

Named operators outside the factory list but present in profile/source registration:

`CACHE_SINK_OPERATOR`, `CACHE_SOURCE_OPERATOR`, `DISTINCT_STREAMING_AGGREGATION_OPERATOR`, `LOCAL_EXCHANGE_OPERATOR`, `LOCAL_EXCHANGE_SINK_OPERATOR`, `LOCAL_MERGE_SORT_SOURCE_OPERATOR`, `MULTI_CAST_DATA_STREAM_SOURCE_OPERATOR`, `PARTITIONED_AGGREGATION_OPERATOR`, `PARTITIONED_AGGREGATION_SINK_OPERATOR`, `PARTITIONED_HASH_JOIN_PROBE_OPERATOR`, `PARTITIONED_HASH_JOIN_SINK_OPERATOR`, `REC_CTE_ANCHOR_SINK_OPERATOR`, `SET_PROBE_SINK_OPERATOR`, `SET_SINK_OPERATOR`, `SPILL_ICEBERG_TABLE_SINK_OPERATOR`, `SPILL_SORT_SINK_OPERATOR`, `SPILL_SORT_SOURCE_OPERATOR`, `STREAMING_AGGREGATION_OPERATOR`.

## Fallback for Unknown Operator Names

If a profile contains an operator not listed above:

1. Search `source-profile-inventory.md` for the exact operator name or class.
2. Use its source path to identify the family: scan, exchange, join, aggregation, sort, sink, set, metadata, or utility.
3. Classify each counter by `counter-semantics.md`.
4. Do not infer bottleneck from wait counters without active work or volume evidence.
