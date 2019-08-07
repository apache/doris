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

namespace java org.apache.doris.thrift
namespace cpp doris

include "Types.thrift"
include "Status.thrift"
include "Planner.thrift"
include "Descriptors.thrift"

struct TScanColumnDesc {
  // The column name
  1: optional string name
  // The column type. Always set.
  2: optional Types.TPrimitiveType type
}

struct TScanColumnData {
  // One element in the list for every row in the column indicating if there is
  // a value in the vals list or a null.
  1: required list<bool> is_null;

  // Only one is set, only non-null values are set. this indicates one column data for a row batch
  2: optional list<bool> bool_vals;
  3: optional list<byte> byte_vals;
  4: optional list<i16> short_vals;
  5: optional list<i32> int_vals;
  // for date and long value
  6: optional list<i64> long_vals;
  // for float and double
  7: optional list<double> double_vals;
  // for char, varchar, decimal
  8: optional list<string> string_vals;
  9: optional list<binary> binary_vals;
}

// Serialized batch of rows returned by getNext().
// one row batch contains mult rows, and the result is arranged in column style
struct TScanRowBatch {
  // selected_columns + cols = data frame
  // Each TScanColumnData contains the data for an entire column. Always set.
  1: optional list<TScanColumnData> cols
  // The number of rows returned.
  2: optional i32 num_rows
}

struct TTabletVersionInfo {
  1: required i64 tablet_id
  2: required i64 version
  3: required i64 versionHash
}

struct TQueryPlanInfo {
  1: required Planner.TPlanFragment plan_fragment
  // tablet_id -> TTabletVersionInfo
  2: required map<i64, TTabletVersionInfo> tablet_info
  3: required Descriptors.TDescriptorTable desc_tbl
  // all tablet scan should share one query_id
  4: required Types.TUniqueId query_id
}


// Parameters to open().
struct TScanOpenParams {

  1: required string cluster

  2: required string database

  3: required string table

  // tablets to scan
  4: required list<i64> tablet_ids

  // base64 encoded binary plan fragment
  5: required string opaqued_query_plan

  // A string specified for the table that is passed to the external data source.
  // Always set, may be an empty string.
  6: optional i32 batch_size

  // reserved params for use
  7: optional map<string,string> properties

  // The query limit, if specified.
  8: optional i64 limit

  // The authenticated user name. Always set.
  // maybe usefullless
  9: optional string user

  10: optional string passwd
}


// Returned by open().
struct TScanOpenResult {
  1: required Status.TStatus status
  // An opaque context_id used in subsequent getNext()/close() calls. Required.
  2: optional string context_id
  // selected fields
  3: optional list<TScanColumnDesc> selected_columns

}

// Parameters to getNext()
struct TScanNextBatchParams {
  // The opaque handle returned by the previous open() call. Always set.
  1: optional string context_id    // doris olap engine context id
  2: optional i64 offset            // doris should check the offset to prevent duplicate rpc calls
}

// Returned by getNext().
struct TScanBatchResult {
  1: required Status.TStatus status

  // If true, reached the end of the result stream; subsequent calls to
  // getNext() won’t return any more results. Required.
  2: optional bool eos

  // A batch of rows to return, if any exist. The number of rows in the batch
  // should be less than or equal to the batch_size specified in TOpenParams.
  3: optional TScanRowBatch rows
}

// Parameters to close()
struct TScanCloseParams {
  // The opaque handle returned by the previous open() call. Always set.
  1: optional string context_id
}

// Returned by close().
struct TScanCloseResult {
  1: required Status.TStatus status
}

// scan service expose ability of scanning data ability to other compute system
service TDorisExternalService {
    // doris will build  a scan context for this session, context_id returned if success
    TScanOpenResult open(1: TScanOpenParams params);

    // return the batch_size of data
    TScanBatchResult getNext(1: TScanNextBatchParams params);

    // release the context resource associated with the context_id
    TScanCloseResult close(1: TScanCloseParams params);
}
