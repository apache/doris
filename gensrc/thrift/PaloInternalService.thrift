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

include "Status.thrift"
include "Types.thrift"
include "Exprs.thrift"
include "Descriptors.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "DataSinks.thrift"
include "Data.thrift"
include "RuntimeProfile.thrift"
include "PaloService.thrift"

// constants for TQueryOptions.num_nodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1

// constants for TPlanNodeId
const i32 INVALID_PLAN_NODE_ID = -1

// Constant default partition ID, must be < 0 to avoid collisions
const i64 DEFAULT_PARTITION_ID = -1;

enum TQueryType {
    SELECT,
    LOAD,
}

enum TErrorHubType {
    MYSQL,
    NULL_TYPE 
}

struct TMysqlErrorHubInfo {
    1: required string host;
    2: required i32 port;
    3: required string user;
    4: required string passwd;
    5: required string db;
    6: required string table;
}

struct TLoadErrorHubInfo {
    1: required TErrorHubType type = TErrorHubType.NULL_TYPE;
    2: optional TMysqlErrorHubInfo mysql_info;
}

// Query options that correspond to PaloService.PaloQueryOptions,
// with their respective defaults
struct TQueryOptions {
  1: optional bool abort_on_error = 0
  2: optional i32 max_errors = 0
  3: optional bool disable_codegen = 1
  4: optional i32 batch_size = 0
  5: optional i32 num_nodes = NUM_NODES_ALL
  6: optional i64 max_scan_range_length = 0
  7: optional i32 num_scanner_threads = 0
  8: optional i32 max_io_buffers = 0
  9: optional bool allow_unsupported_formats = 0
  10: optional i64 default_order_by_limit = -1
  11: optional string debug_action = ""
  12: optional i64 mem_limit = 2147483648
  13: optional bool abort_on_default_limit_exceeded = 0
  14: optional i32 query_timeout = 3600
  15: optional bool is_report_success = 0
  16: optional i32 codegen_level = 0
  // INT64::MAX
  17: optional i64 kudu_latest_observed_ts = 9223372036854775807
  18: optional TQueryType query_type = TQueryType.SELECT
}

// A scan range plus the parameters needed to execute that scan.
struct TScanRangeParams {
  1: required PlanNodes.TScanRange scan_range
  2: optional i32 volume_id = -1
}

// Specification of one output destination of a plan fragment
struct TPlanFragmentDestination {
  // the globally unique fragment instance id
  1: required Types.TUniqueId fragment_instance_id

  // ... which is being executed on this server
  2: required Types.TNetworkAddress server
}

// Parameters for a single execution instance of a particular TPlanFragment
// TODO: for range partitioning, we also need to specify the range boundaries
struct TPlanFragmentExecParams {
  // a globally unique id assigned to the entire query
  1: required Types.TUniqueId query_id
  
  // a globally unique id assigned to this particular execution instance of
  // a TPlanFragment
  2: required Types.TUniqueId fragment_instance_id

  // initial scan ranges for each scan node in TPlanFragment.plan_tree
  3: required map<Types.TPlanNodeId, list<TScanRangeParams>> per_node_scan_ranges

  // number of senders for ExchangeNodes contained in TPlanFragment.plan_tree;
  // needed to create a DataStreamRecvr
  4: required map<Types.TPlanNodeId, i32> per_exch_num_senders

  // Output destinations, one per output partition.
  // The partitioning of the output is specified by
  // TPlanFragment.output_sink.output_partition.
  // The number of output partitions is destinations.size().
  5: list<TPlanFragmentDestination> destinations

  // Debug options: perform some action in a particular phase of a particular node
  6: optional Types.TPlanNodeId debug_node_id
  7: optional PlanNodes.TExecNodePhase debug_phase
  8: optional PlanNodes.TDebugAction debug_action

  // Id of this fragment in its role as a sender.
  9: optional i32 sender_id
}

// Global query parameters assigned by the coordinator.
struct TQueryGlobals {
  // String containing a timestamp set as the current time.
  1: required string now_string
}


// Service Protocol Details

enum PaloInternalServiceVersion {
  V1
}


// ExecPlanFragment

struct TExecPlanFragmentParams {
  1: required PaloInternalServiceVersion protocol_version

  // required in V1
  2: optional Planner.TPlanFragment fragment

  // required in V1
  3: optional Descriptors.TDescriptorTable desc_tbl

  // required in V1
  4: optional TPlanFragmentExecParams params

  // Initiating coordinator.
  // TODO: determine whether we can get this somehow via the Thrift rpc mechanism.
  // required in V1
  5: optional Types.TNetworkAddress coord

  // backend number assigned by coord to identify backend
  // required in V1
  6: optional i32 backend_num

  // Global query parameters assigned by coordinator.
  // required in V1
  7: optional TQueryGlobals query_globals
  
  // options for the query
  // required in V1
  8: optional TQueryOptions query_options

  // Whether reportd when the backend fails
  // required in V1
  9: optional bool is_report_success

  // required in V1
  10: optional Types.TResourceInfo resource_info

  // load job related
  11: optional string import_label
  12: optional string db_name
  13: optional i64 load_job_id
  14: optional TLoadErrorHubInfo load_error_hub_info
}

struct TExecPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}

// CancelPlanFragment
struct TCancelPlanFragmentParams {
  1: required PaloInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId fragment_instance_id
}

struct TCancelPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}


// TransmitData

struct TTransmitDataParams {
  1: required PaloInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId dest_fragment_instance_id

  // for debugging purposes; currently ignored
  //3: optional Types.TUniqueId src_fragment_instance_id

  // required in V1
  4: optional Types.TPlanNodeId dest_node_id

  // required in V1
  5: optional Data.TRowBatch row_batch

  // if set to true, indicates that no more row batches will be sent
  // for this dest_node_id
  6: optional bool eos

  7: optional i32 be_number
  8: optional i64 packet_seq

  // Id of this fragment in its role as a sender.
  9: optional i32 sender_id
}

struct TTransmitDataResult {
  // required in V1
  1: optional Status.TStatus status
  2: optional i64 packet_seq
  3: optional Types.TUniqueId dest_fragment_instance_id
  4: optional Types.TPlanNodeId dest_node_id
}

struct TFetchDataParams {
  1: required PaloInternalServiceVersion protocol_version
  // required in V1
  // query id which want to fetch data
  2: required Types.TUniqueId fragment_instance_id
}

struct TFetchDataResult {
    // result batch 
    1: required Data.TResultBatch result_batch
    // end of stream flag
    2: required bool eos
    // packet num used check lost of packet
    3: required i32 packet_num
    // Operation result
    4: optional Status.TStatus status
}

struct TFetchStartKey {
    1: required list<string> key
}

struct TFetchEndKey {
    1: required list<string> key
}

struct TCondition {
    1:  required string column_name
    2:  required string condition_op
    3:  required list<string> condition_values
}

struct TFetchRequest {
    1: required bool use_compression
    2: optional i32 num_rows
    3: required i32 schema_hash
    4: required Types.TTabletId tablet_id
    5: required i32 version
    6: required i64 version_hash
    7: required list<string> field
    8: optional string user
    9: optional string output
    10: optional string range
    11: required list<TFetchStartKey> start_key
	12: required list<TFetchEndKey> end_key
    13: required list<TCondition> where
    14: optional string end_range
    15: optional bool aggregation
}

struct TShowHintsRequest {
    1: required Types.TTabletId tablet_id
    2: required i32 schema_hash
    3: required i32 block_row_count
    4: optional string end_range = "lt"
    5: required list<TFetchStartKey> start_key
	6: required list<TFetchEndKey> end_key
}

struct TExportStatusResult {
    1: required Status.TStatus status
    2: required Types.TExportState state
    3: optional list<string> files
}

