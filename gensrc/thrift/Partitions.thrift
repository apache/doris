// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp doris
namespace java org.apache.doris.thrift

include "Exprs.thrift"
include "Types.thrift"

enum TPartitionType {
  UNPARTITIONED = 0,

  // round-robin partition
  RANDOM = 1,

  // unordered partition on a set of exprs
  // (partition bounds overlap)
  HASH_PARTITIONED = 2,

  // ordered partition on a list of exprs
  // (partition bounds don't overlap)
  RANGE_PARTITIONED = 3,
  
  // partition on a list of exprs
  LIST_PARTITIONED = 4,

  // unordered partition on a set of exprs
  // (only use in bucket shuffle join)
  BUCKET_SHFFULE_HASH_PARTITIONED = 5,

  // used for shuffle data by parititon and tablet
  OLAP_TABLE_SINK_HASH_PARTITIONED = 6,

  // used for shuffle data by hive parititon
  HIVE_TABLE_SINK_HASH_PARTITIONED = 7,

  // used for hive unparititoned table
  HIVE_TABLE_SINK_UNPARTITIONED = 8,

  // used for merge partitioning: insert by partition columns, delete by row_id
  MERGE_PARTITIONED = 9
}

enum TLocalPartitionType {
  // NOOP: no local exchange. the consumer keeps the producer's existing data distribution and
  // instance count. `DataDistribution::need_local_exchange()` returns false for NOOP, so the
  // planner inserts no LocalExchangeNode at all; in the FE-planned path NOOP never reaches BE's
  // exchanger factory (it is filtered out earlier).
  NOOP = 0,
  // used to resume the global hash distribution because other distribution break the global hash distribution,
  // such as PASSTHROUGH. and then JoinNode can shuffle data by the same hash distribution.
  //
  // for example:                                   look here, need resume to GLOBAL_EXECUTION_HASH_SHUFFLE
  //                                                                            ↓
  //   Node -> LocalExchangeNode(PASSTHROUGH) → JoinNode →  LocalExchangeNode(GLOBAL_EXECUTION_HASH_SHUFFLE) → JoinNode
  //                  ExchangeNode(BROADCAST) ↗                                                                  ↑
  //                                                                         ExchangeNode(GLOBAL_EXECUTION_HASH_SHUFFLE)
  GLOBAL_EXECUTION_HASH_SHUFFLE = 1,
  // used to rebalance data within a backend to add parallelism. this is a performance rebalance,
  // NOT a correctness requirement.
  //
  // for example:          look here, need use LOCAL_EXECUTION_HASH_SHUFFLE to rebalance data
  //                                         ↓
  //  Scan(hash(id)) -> LocalExchangeNode(LOCAL_EXECUTION_HASH_SHUFFLE(id, name)) → AggregationNode(group by(id,name))
  //
  // group by (id, name) is already correct on a scan that is hash-distributed by id, because id is a subset
  // of the grouping keys: every (id, name) group is fully contained in the backend that owns its id, so no
  // reshuffle is required for correctness. but when there are few distinct id values the data is concentrated
  // on a few local instances; LOCAL_EXECUTION_HASH_SHUFFLE re-partitions by the full key (id, name) and
  // spreads the aggregation across all local instances of the backend (hash mod local instance count, mapping
  // instance i -> i), purely to add parallelism.
  //
  // conversely, the reverse plan scan(hash(id, name)) -> agg(group by id) is NOT a local-exchange case:
  // hash(id, name) spreads the rows of a single id across different backends (e.g. (5, A) on be1 and
  // (5, B) on be2), so re-partitioning them by id requires a cross-backend shuffle
  // (GLOBAL_EXECUTION_HASH_SHUFFLE / a network exchange), which a within-backend local exchange cannot do.
  // and we can not use GLOBAL_EXECUTION_HASH_SHUFFLE(id, name) here, because
  // `TPipelineFragmentParams.shuffle_idx_to_instance_idx` is used to mapping partial global instance index to local
  // instance index, and discard the other backend's instance index, the data not belong to the local instance will be
  // discarded, which cause data loss.
  LOCAL_EXECUTION_HASH_SHUFFLE = 2,
  // BUCKET_HASH_SHUFFLE: hash distribute_expr_lists and route each row to the instance that owns its
  // bucket, via `TPipelineFragmentParams.bucket_seq_to_instance_idx` (implemented by
  // BucketShuffleExchanger, a ShuffleExchanger specialization). this preserves the table's
  // tablet/bucket layout so operators that must agree on bucket->instance placement line up without a
  // global reshuffle -- i.e. colocate joins and bucket-shuffle joins.
  //
  // for example, a bucket-shuffle join on a table bucketed by id:
  //   Scan(bucketed by id) -> LocalExchangeNode(BUCKET_HASH_SHUFFLE(id)) -> HashJoin(... on id = ...)
  // each id-bucket lands on the same instance on both inputs, so matching rows meet locally.
  BUCKET_HASH_SHUFFLE = 3,
  // PASSTHROUGH: round-robin whole blocks across the local instances WITHOUT re-partitioning the data
  // (PassthroughExchanger sends block N to instance `N % local_instance_count`; rows are never
  // re-hashed or split). used only to even out work / add parallelism when the consumer does not care
  // how rows are partitioned -- e.g. fanning a serial (1-task) producer out to N instances, or feeding
  // an operator with no distribution requirement.
  //
  // for example, fan a serial source out to 3 instances:
  //   SerialNode(1 task) -> LocalExchangeNode(PASSTHROUGH) -> Project(3 tasks)
  //   block0->inst0, block1->inst1, block2->inst2, block3->inst0, ...
  PASSTHROUGH = 4,
  // ADAPTIVE_PASSTHROUGH: starts by round-robining each block's ROWS evenly across all instances (so
  // they fill up evenly even when there are only a few large blocks), then -- once it has seen
  // >= local_instance_count blocks -- switches to cheap whole-block PASSTHROUGH for the rest
  // (AdaptivePassthroughExchanger). this is round-robin, NOT a hash shuffle, and the switch is driven
  // by block count, not data content. used where we want an even initial spread plus low steady-state
  // overhead, e.g. the input of a non-grouping / streaming aggregation.
  ADAPTIVE_PASSTHROUGH = 5,
  // BROADCAST: copy every incoming block to ALL local instances (BroadcastExchanger enqueues the same
  // block to every channel), so each instance sees the full input. used for the build side of a
  // broadcast join -- every probe instance needs the complete build input to build its own hash table.
  //
  // for example:
  //   Scan(build side) -> LocalExchangeNode(BROADCAST) -> HashJoin(build)
  BROADCAST = 6,
  // PASS_TO_ONE: funnel all rows to a single local instance (channel 0); every other instance gets EOS
  // immediately and produces nothing (PassToOneExchanger). used for a broadcast join with a shared
  // hash table, where only instance 0 needs the build data and the others share its hash table.
  // NOTE: BE only uses PassToOneExchanger when `enable_share_hash_table_for_broadcast_join` is on;
  // when it is off the same PASS_TO_ONE type degrades to BROADCAST (each instance keeps its own copy).
  PASS_TO_ONE = 7,
  // LOCAL_MERGE_SORT: k-way merge of several already-sorted local inputs into one globally sorted
  // stream on a single instance (paired with LocalMergeSortSourceOperator, for a SortNode with
  // use_local_merge). only the legacy BE-side local-exchange planner emits this; the FE-planned path
  // never produces it, so BE's FE-planned exchanger factory rejects it as a protocol violation.
  LOCAL_MERGE_SORT = 8
}

enum TDistributionType {
  UNPARTITIONED = 0,

  // round-robin partition
  RANDOM = 1,

  // unordered partition on a set of exprs
  // (partition bounds overlap)
  HASH_PARTITIONED = 2
}

// TODO(zc): Refine
// Move the following to Partitions
struct TPartitionKey {
    1: required i16 sign
    2: optional Types.TPrimitiveType type
    3: optional string key
}

struct TPartitionRange {
    1: required TPartitionKey start_key
    2: required TPartitionKey end_key
    3: required bool include_start_key
    4: required bool include_end_key
}

// Partition info
struct TRangePartition {
    1: required i64 partition_id
    2: required TPartitionRange range

    // what distribute information in this partition.
    3: optional list<Exprs.TExpr> distributed_exprs
    4: optional i32 distribute_bucket
}

// Merge partitioning info for Iceberg update/delete.
struct TIcebergPartitionField {
  1: required string transform
  2: optional i32 param
  3: required Exprs.TExpr source_expr
  4: optional string name
  5: optional i32 source_id
}

struct TMergePartitionInfo {
  1: required Exprs.TExpr operation_expr
  2: optional list<Exprs.TExpr> insert_partition_exprs
  3: optional list<Exprs.TExpr> delete_partition_exprs
  4: required bool insert_random
  5: optional list<TIcebergPartitionField> insert_partition_fields
  6: optional i32 partition_spec_id
}

// Specification of how a single logical data stream is partitioned.
// This leaves out the parameters that determine the physical partition (for hash
// partitions, the number of partitions; for range partitions, the partitions'
// boundaries), which need to be specified by the enclosing structure/context.
struct TDataPartition {
  1: required TPartitionType type
  2: optional list<Exprs.TExpr> partition_exprs
  3: optional list<TRangePartition> partition_infos
  4: optional TMergePartitionInfo merge_partition_info
}
