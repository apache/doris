// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

namespace cpp palo
namespace java com.baidu.palo.thrift

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

// Parameters needed for hash partitioning
struct TKuduPartitionByHashParam {
  1: required list<string> columns
  2: required i32 num_partitions
}

struct TKuduRangePartition {
  1: optional list<Exprs.TExpr> lower_bound_values
  2: optional bool is_lower_bound_inclusive
  3: optional list<Exprs.TExpr> upper_bound_values
  4: optional bool is_upper_bound_inclusive
}

// A range partitioning is identified by a list of columns and a list of range partitions.
struct TKuduPartitionByRangeParam {
  1: required list<string> columns
  2: optional list<TKuduRangePartition> range_partitions
}

// Parameters for the PARTITION BY clause.
struct TKuduPartitionParam {
  1: optional TKuduPartitionByHashParam by_hash_param;
  2: optional TKuduPartitionByRangeParam by_range_param;
}

// Represents a Kudu table
struct TKuduTable {
  1: required string table_name

  // Network address of a master host in the form of 0.0.0.0:port
  2: required list<string> master_addresses

  // Name of the key columns
  3: required list<string> key_columns

  // Partitioning
  4: required list<TKuduPartitionParam> partition_by
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
  13: optional TKuduTable kuduTable
  14: optional TBrokerTable BrokerTable
}

struct TTupleDescriptor {
  1: required Types.TTupleId id
  2: required i32 byteSize
  3: required i32 numNullBytes
  4: optional Types.TTableId tableId
  5: optional i32 numNullSlots
}

struct TDescriptorTable {
  1: optional list<TSlotDescriptor> slotDescriptors;
  2: required list<TTupleDescriptor> tupleDescriptors;

  // all table descriptors referenced by tupleDescriptors
  3: optional list<TTableDescriptor> tableDescriptors;
}
