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
  UNPARTITIONED,

  // round-robin partition
  RANDOM,

  // unordered partition on a set of exprs
  // (partition bounds overlap)
  HASH_PARTITIONED,

  // ordered partition on a list of exprs
  // (partition bounds don't overlap)
  RANGE_PARTITIONED,
  
  // partition on a list of exprs
  LIST_PARTITIONED,

  // unordered partition on a set of exprs
  // (only use in bucket shuffle join)
  BUCKET_SHFFULE_HASH_PARTITIONED,

  // used for shuffle data by parititon and tablet
  TABLET_SINK_SHUFFLE_PARTITIONED,

  // used for shuffle data by hive parititon
  TABLE_SINK_HASH_PARTITIONED,

  // used for hive unparititoned table
  TABLE_SINK_RANDOM_PARTITIONED
}

enum TDistributionType {
  UNPARTITIONED,

  // round-robin partition
  RANDOM,

  // unordered partition on a set of exprs
  // (partition bounds overlap)
  HASH_PARTITIONED
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

// Specification of how a single logical data stream is partitioned.
// This leaves out the parameters that determine the physical partition (for hash
// partitions, the number of partitions; for range partitions, the partitions'
// boundaries), which need to be specified by the enclosing structure/context.
struct TDataPartition {
  1: required TPartitionType type
  2: optional list<Exprs.TExpr> partition_exprs
  3: optional list<TRangePartition> partition_infos
}


