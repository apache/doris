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
    DATA_SPLIT_SINK, // deprecated
    MYSQL_TABLE_SINK,
    EXPORT_SINK,
    OLAP_TABLE_SINK,
    MEMORY_SCRATCH_SINK,
    ODBC_TABLE_SINK,
    RESULT_FILE_SINK,
    JDBC_TABLE_SINK,
}

enum TResultSinkType {
    MYSQL_PROTOCAL,
    FILE,    // deprecated, should not be used any more. FileResultSink is covered by TRESULT_FILE_SINK for concurrent purpose.
}

enum TParquetCompressionType {
    SNAPPY,
    GZIP,
    BROTLI,
    ZSTD,
    LZ4,
    LZO,
    BZ2,
    UNCOMPRESSED,
}

enum TParquetVersion {
    PARQUET_1_0,
    PARQUET_2_LATEST,
}

enum TParquetDataType {
    BOOLEAN,
    INT32,
    INT64,
    INT96,
    BYTE_ARRAY,
    FLOAT,
    DOUBLE,
    FIXED_LEN_BYTE_ARRAY,
}

enum TParquetRepetitionType {
    REQUIRED,
    REPEATED,
    OPTIONAL,
}

struct TParquetSchema {
    1: optional TParquetRepetitionType schema_repetition_type
    2: optional TParquetDataType schema_data_type
    3: optional string schema_column_name    
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
    9: optional list<list<string>> schema            // for orc file
    10: optional map<string, string> file_properties // for orc file

    //note: use outfile with parquet format, have deprecated 9:schema and 10:file_properties
    //because when this info thrift to BE, BE hava to find useful info in string,
    //have to check by use string directly, and maybe not so efficient
    11: optional list<TParquetSchema> parquet_schemas
    12: optional TParquetCompressionType parquet_compression_type
    13: optional bool parquet_disable_dictionary
    14: optional TParquetVersion parquet_version
    15: optional string orc_schema
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
    2: optional TResultFileSinkOptions file_options // deprecated
}

struct TResultFileSink {
    1: optional TResultFileSinkOptions file_options;
    2: optional Types.TStorageBackendType storage_backend_type;
    3: optional Types.TPlanNodeId dest_node_id;
    4: optional Types.TTupleId output_tuple_id;
    5: optional string header;
    6: optional string header_type;
}

struct TMysqlTableSink {
    1: required string host
    2: required i32 port
    3: required string user
    4: required string passwd
    5: required string db
    6: required string table
    7: required string charset
}

struct TOdbcTableSink {
    1: optional string connect_string
    2: optional string table
    3: optional bool use_transaction
}

struct TJdbcTableSink {
    1: optional Descriptors.TJdbcTable jdbc_table
    2: optional bool use_transaction
    3: optional Types.TOdbcTableType table_type
}

struct TExportSink {
    1: required Types.TFileType file_type
    2: required string export_path
    3: required string column_separator
    4: required string line_delimiter
    // properties need to access broker.
    5: optional list<Types.TNetworkAddress> broker_addresses
    6: optional map<string, string> properties
    7: optional string header
}

struct TOlapTableSink {
    1: required Types.TUniqueId load_id
    2: required i64 txn_id
    3: required i64 db_id
    4: required i64 table_id
    5: required i32 tuple_id
    6: required i32 num_replicas
    7: required bool need_gen_rollup    // Deprecated, not used since alter job v2
    8: optional string db_name
    9: optional string table_name
    10: required Descriptors.TOlapTableSchemaParam schema
    11: required Descriptors.TOlapTablePartitionParam partition
    12: required Descriptors.TOlapTableLocationParam location
    13: required Descriptors.TPaloNodesInfo nodes_info
    14: optional i64 load_channel_timeout_s // the timeout of load channels in second
    15: optional i32 send_batch_parallelism
    16: optional bool load_to_single_tablet
    17: optional bool write_single_replica
    18: optional Descriptors.TOlapTableLocationParam slave_location
}

struct TDataSink {
  1: required TDataSinkType type
  2: optional TDataStreamSink stream_sink
  3: optional TResultSink result_sink
  5: optional TMysqlTableSink mysql_table_sink
  6: optional TExportSink export_sink
  7: optional TOlapTableSink olap_table_sink
  8: optional TMemoryScratchSink memory_scratch_sink
  9: optional TOdbcTableSink odbc_table_sink
  10: optional TResultFileSink result_file_sink
  11: optional TJdbcTableSink jdbc_table_sink
}

