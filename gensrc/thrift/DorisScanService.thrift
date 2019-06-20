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


enum TScanStatusCode {
    OK,
    INVALID_TABLE,
    INVALID_CONTEXT,
    INTERNAL_EROOR
}

struct TScanStatus {
  1: required TScanStatusCode status_code
  2: optional string error_msg
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
  6: optional list<i64> long_vals;
  7: optional list<double> double_vals;
  8: optional list<string> string_vals;
  9: optional list<binary> binary_vals;
}

// Serialized batch of rows returned by getNext().
// one row batch contains mult rows, and the result is arranged in column style
struct TScanRowBatch {
  // selected_columns + cols = data frame
  // Each TScanColumnData contains the data for an entire column. Always set.
  1: optional list<TScanColumnData> cols
  2: optional list<string> selected_columns
  // The number of rows returned.
  3: optional i32 num_rows
}

// Parameters to open().
struct TScanOpenParams {

  // tablets to scan
  1: required list<i64> tablet_ids

  // base64 encoded binary plan fragment
  2: required string opaqued_query_plan

  // A string specified for the table that is passed to the external data source.
  // Always set, may be an empty string.
  3: optional i32 batch_size

  // reserved params for use
  4: optional map<string,string> payloads

  // The query limit, if specified.
  5: optional i64 limit

  // The authenticated user name. Always set.
  // maybe usefullless
  6: optional string user_name
}


// Returned by open().
struct TScanOpenResult {
  1: required TScanStatus status
  // An opaque context_id used in subsequent getNext()/close() calls. Required.
  2: optional string context_id
}

// Parameters to getNext()
struct TScanNextBatchParams {
  // The opaque handle returned by the previous open() call. Always set.
  1: optional string context_id    // doris olap engine context id
  2: optional i64 offset            // doris should check the offset to prevent duplicate rpc calls
}

// Returned by getNext().
struct TScanBatchResult {
  1: required TScanStatus status

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
  1: required TScanStatus status
}

// scan service expose ability of scanning data ability to other compute system
service TDorisScanService {
    // doris will build  a scan context for this session, context_id returned if success
    TScanOpenResult open(1: TScanOpenParams params);
    // 1. palo be will send a search context id to es 
    // 2. es will find the search context and find a batch rows and send to palo
    // 3. palo will run the remaining predicates when receving data
    // 4. es should check the offset when receive the request
    TScanBatchResult getNext(1: TScanNextBatchParams params);

    // release the context resource associated with the context_id
    TScanCloseResult close(1: TScanCloseParams params);
}
