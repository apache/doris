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
include "Descriptors.thrift"
include "Partitions.thrift"
include "PlanNodes.thrift"

enum TDataSinkType {
    DATA_STREAM_SINK,
    RESULT_SINK,
    DATA_SPLIT_SINK,
    MYSQL_TABLE_SINK,
    EXPORT_SINK,
    OLAP_TABLE_SINK,
    MEMORY_SCRATCH_SINK,
    ODBC_TABLE_SINK,
    VRESULT_SINK,
    VDATA_STREAM_SINK,
}

enum TResultSinkType {
    MYSQL_PROTOCAL,
    FILE
}

struct TResultFileSinkOptions {
    1: required string file_path
    2: required PlanNodes.TFileFormatType file_format
    3: optional string column_separator    // only for csv
    4: optional string line_delimiter  // only for csv
    5: optional i64 max_file_size_bytes
    6: optional list<Types.TNetworkAddress> broker_addresses; // only for remote file
    7: optional map<string, string> broker_properties // only for remote file
    8: optional string success_file_name
    9: optional list<list<string>> schema            // for parquet/orc file
    10: optional map<string, string> file_properties // for parquet/orc file
}

struct TMemoryScratchSink {

}

// Sink which forwards data to a remote plan fragment,
// according to the given output partition specification
// (ie, the m:1 part of an m:n data stream)
struct TDataStreamSink {
  // destination node id
  1: required Types.TPlanNodeId dest_node_id

  // Specification of how the output of a fragment is partitioned.
  // If the partitioning type is UNPARTITIONED, the output is broadcast
  // to each destination host.
  2: required Partitions.TDataPartition output_partition

  3: optional bool ignore_not_found
}

struct TResultSink {
    1: optional TResultSinkType type;
    2: optional TResultFileSinkOptions file_options;
}

struct TMysqlTableSink {
    1: required string host
    2: required i32 port
    3: required string user
    4: required string passwd
    5: required string db
    6: required string table
}

struct TOdbcTableSink {
    1: optional string connect_string
    2: optional string table
    3: optional bool use_transaction
}

// Following is used to split data read from 
// Used to describe rollup schema
struct TRollupSchema {
    1: required list<Exprs.TExpr> keys
    2: required list<Exprs.TExpr> values
    3: required list<Types.TAggregationType> value_ops
    4: optional string keys_type 
}

struct TDataSplitSink {
    1: required list<Exprs.TExpr> partition_exprs
    2: required list<Partitions.TRangePartition> partition_infos
    4: required map<string, TRollupSchema> rollup_schemas
}

struct TExportSink {
    1: required Types.TFileType file_type
    2: required string export_path
    3: required string column_separator
    4: required string line_delimiter
    // properties need to access broker.
    5: optional list<Types.TNetworkAddress> broker_addresses
    6: optional map<string, string> properties;
}

struct TOlapTableSink {
    1: required Types.TUniqueId load_id
    2: required i64 txn_id
    3: required i64 db_id
    4: required i64 table_id
    5: required i32 tuple_id
    6: required i32 num_replicas
    7: required bool need_gen_rollup
    8: optional string db_name
    9: optional string table_name
    10: required Descriptors.TOlapTableSchemaParam schema
    11: required Descriptors.TOlapTablePartitionParam partition
    12: required Descriptors.TOlapTableLocationParam location
    13: required Descriptors.TPaloNodesInfo nodes_info
    14: optional i64 load_channel_timeout_s // the timeout of load channels in second
}

struct TDataSink {
  1: required TDataSinkType type
  2: optional TDataStreamSink stream_sink
  3: optional TResultSink result_sink
  4: optional TDataSplitSink split_sink
  5: optional TMysqlTableSink mysql_table_sink
  6: optional TExportSink export_sink
  7: optional TOlapTableSink olap_table_sink
  8: optional TMemoryScratchSink memory_scratch_sink
  9: optional TOdbcTableSink odbc_table_sink
}

