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

include "Exprs.thrift"
include "Types.thrift"
include "Descriptors.thrift"
include "Partitions.thrift"

enum TDataSinkType {
    DATA_STREAM_SINK,
    RESULT_SINK,
    DATA_SPLIT_SINK,
    MYSQL_TABLE_SINK,
    EXPORT_SINK,
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

// Reserved for 
struct TResultSink {
}

struct TMysqlTableSink {
    1: required string host
    2: required i32 port
    3: required string user
    4: required string passwd
    5: required string db
    6: required string table
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

struct TDataSink {
  1: required TDataSinkType type
  2: optional TDataStreamSink stream_sink
  3: optional TResultSink result_sink
  4: optional TDataSplitSink split_sink
  5: optional TMysqlTableSink mysql_table_sink
  6: optional TExportSink export_sink
}

