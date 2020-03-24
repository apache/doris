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

include "Types.thrift"
include "Exprs.thrift"

struct TSlotDescriptor {
  1: required Types.TSlotId id
  2: required Types.TTupleId parent
  3: required Types.TTypeDesc slotType
  4: required i32 columnPos   // in originating table
  5: required i32 byteOffset  // into tuple
  6: required i32 nullIndicatorByte
  7: required i32 nullIndicatorBit
  8: required string colName;
  9: required i32 slotIdx
  10: required bool isMaterialized
}

struct TTupleDescriptor {
  1: required Types.TTupleId id
  2: required i32 byteSize
  3: required i32 numNullBytes
  4: optional Types.TTableId tableId
  5: optional i32 numNullSlots
}

enum THdfsFileFormat {
  TEXT,
  LZO_TEXT,
  RC_FILE,
  SEQUENCE_FILE,
  AVRO,
  PARQUET
}

enum TSchemaTableType {
    SCH_AUTHORS= 0,
    SCH_CHARSETS,
    SCH_COLLATIONS,
    SCH_COLLATION_CHARACTER_SET_APPLICABILITY,
    SCH_COLUMNS,
    SCH_COLUMN_PRIVILEGES,
    SCH_CREATE_TABLE,
    SCH_ENGINES,
    SCH_EVENTS,
    SCH_FILES,
    SCH_GLOBAL_STATUS,
    SCH_GLOBAL_VARIABLES,
    SCH_KEY_COLUMN_USAGE,
    SCH_OPEN_TABLES,
    SCH_PARTITIONS,
    SCH_PLUGINS,
    SCH_PROCESSLIST,
    SCH_PROFILES,
    SCH_REFERENTIAL_CONSTRAINTS,
    SCH_PROCEDURES,
    SCH_SCHEMATA,
    SCH_SCHEMA_PRIVILEGES,
    SCH_SESSION_STATUS,
    SCH_SESSION_VARIABLES,
    SCH_STATISTICS,
    SCH_STATUS,
    SCH_TABLES,
    SCH_TABLE_CONSTRAINTS,
    SCH_TABLE_NAMES,
    SCH_TABLE_PRIVILEGES,
    SCH_TRIGGERS,
    SCH_USER_PRIVILEGES,
    SCH_VARIABLES,
    SCH_VIEWS,
    SCH_INVALID
}

enum THdfsCompression {
  NONE,
  DEFAULT,
  GZIP,
  DEFLATE,
  BZIP2,
  SNAPPY,
  SNAPPY_BLOCKED // Used by sequence and rc files but not stored in the metadata.
}

enum TIndexType {
  BITMAP
}

// Mapping from names defined by Avro to the enum.
// We permit gzip and bzip2 in addition.
const map<string, THdfsCompression> COMPRESSION_MAP = {
  "": THdfsCompression.NONE,
  "none": THdfsCompression.NONE,
  "deflate": THdfsCompression.DEFAULT,
  "gzip": THdfsCompression.GZIP,
  "bzip2": THdfsCompression.BZIP2,
  "snappy": THdfsCompression.SNAPPY
}

struct TOlapTableIndexTablets {
    1: required i64 index_id
    2: required list<i64> tablets
}

// its a closed-open range
struct TOlapTablePartition {
    1: required i64 id
    // deprecated, use 'start_keys' and 'end_keys' instead
    2: optional Exprs.TExprNode start_key
    3: optional Exprs.TExprNode end_key

    // how many tablets in one partition
    4: required i32 num_buckets

    5: required list<TOlapTableIndexTablets> indexes

    6: optional list<Exprs.TExprNode> start_keys
    7: optional list<Exprs.TExprNode> end_keys
}

struct TOlapTablePartitionParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version

    // used to split a logical table to multiple paritions
    // deprecated, use 'partition_columns' instead
    4: optional string partition_column

    // used to split a partition to multiple tablets
    5: optional list<string> distributed_columns

    // partitions
    6: required list<TOlapTablePartition> partitions

    7: optional list<string> partition_columns
}

struct TOlapTableIndexSchema {
    1: required i64 id
    2: required list<string> columns
    3: required i32 schema_hash
}

struct TOlapTableSchemaParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version

    // Logical columns, contain all column that in logical table
    4: required list<TSlotDescriptor> slot_descs
    5: required TTupleDescriptor tuple_desc
    6: required list<TOlapTableIndexSchema> indexes
}

struct TOlapTableIndex {
  1: optional string index_name
  2: optional list<string> columns
  3: optional TIndexType index_type
  4: optional string comment
}

struct TTabletLocation {
    1: required i64 tablet_id
    2: required list<i64> node_ids
}

struct TOlapTableLocationParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version
    4: required list<TTabletLocation> tablets
}

struct TNodeInfo {
    1: required i64 id
    2: required i64 option
    3: required string host
    // used to transfer data between nodes
    4: required i32 async_internal_port
}

struct TPaloNodesInfo {
    1: required i64 version
    2: required list<TNodeInfo> nodes
}

struct TOlapTable {
    1: required string tableName
}

struct TMySQLTable {
  1: required string host
  2: required string port
  3: required string user
  4: required string passwd
  5: required string db
  6: required string table
}

struct TEsTable {
}

struct TSchemaTable {
  1: required TSchemaTableType tableType
}

struct TBrokerTable {
}

// "Union" of all table types.
struct TTableDescriptor {
  1: required Types.TTableId id
  2: required Types.TTableType tableType
  3: required i32 numCols
  4: required i32 numClusteringCols

  // Unqualified name of table
  7: required string tableName;

  // Name of the database that the table belongs to
  8: required string dbName;
  10: optional TMySQLTable mysqlTable
  11: optional TOlapTable olapTable
  12: optional TSchemaTable schemaTable
  14: optional TBrokerTable BrokerTable
  15: optional TEsTable esTable
}

struct TDescriptorTable {
  1: optional list<TSlotDescriptor> slotDescriptors;
  2: required list<TTupleDescriptor> tupleDescriptors;

  // all table descriptors referenced by tupleDescriptors
  3: optional list<TTableDescriptor> tableDescriptors;
}
