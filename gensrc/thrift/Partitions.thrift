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

  // used for shuffle data by external table sink ownership key. BE execution
  // versions before 12 reject this type because value 7 had different semantics.
  EXTERNAL_TABLE_SINK_HASH_PARTITIONED = 7,

  // adaptive writer distribution for external tables without an ownership key
  EXTERNAL_TABLE_SINK_UNPARTITIONED = 8,

  // used for merge partitioning: insert by partition columns, delete by row_id
  MERGE_PARTITIONED = 9
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

// The routing algorithm implemented by an external table sink hash exchange.
// Connector-specific FE distribution specs map to one of these algorithms.
enum TExternalTableSinkHashAlgorithm {
  DIRECT_HASH = 0,
  ICEBERG_TRANSFORM = 1,

  // Paimon HASH_FIXED routing using the default bucket function and
  // ChannelComputer. The partition expressions carry their Doris types.
  PAIMON_FIXED_BUCKET = 2
}

// Maps the logical partitions produced by the hash algorithm to Doris exchange writers.
// IDENTITY preserves one-writer ownership; SKEWED retains ScaleWriter behavior for formats
// which allow a hot partition to be written by multiple writers.
enum TExternalTableSinkWriterAssignment {
  IDENTITY = 0,
  SKEWED = 1
}

// Minimal metadata for Paimon's stateless HASH_FIXED route. Field indexes refer
// to TDataPartition.partition_exprs, not to physical Block column positions.
struct TPaimonFixedBucketInfo {
  1: required i32 num_buckets
  2: required list<i32> partition_field_indexes
  3: required list<i32> bucket_field_indexes
}

// Connector-independent routing metadata for an external table sink hash
// exchange. Algorithm-specific fields are optional for protocol evolution, but
// each algorithm validates the fields it requires before processing rows.
struct TExternalTableSinkHashPartitionInfo {
  1: required TExternalTableSinkHashAlgorithm algorithm

  // Positional Iceberg transforms. Required by ICEBERG_TRANSFORM and must have
  // the same size as partition_exprs.
  2: optional list<string> partition_transforms

  // Kept optional on the wire so a new BE can reject an old FE with a clear
  // status instead of silently relying on the enum's default value.
  3: optional TExternalTableSinkWriterAssignment writer_assignment

  // Required only by PAIMON_FIXED_BUCKET.
  4: optional TPaimonFixedBucketInfo paimon_fixed_bucket_info
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
  5: optional TExternalTableSinkHashPartitionInfo external_table_sink_hash_partition_info
}
