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
    // max keep alive time min
  11: optional i16 keep_alive_min

  12: optional i32 query_timeout
  
  // memory limit for a single query
  13: optional i64 mem_limit
}

struct TScanColumnDesc {
  // The column name
  1: optional string name
  // The column type. Always set.
  2: optional Types.TPrimitiveType type
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

  // A batch of rows of arrow format to return, if any exist. The number of rows in the batch
  // should be less than or equal to the batch_size specified in TOpenParams.
  3: optional binary rows
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
    TScanOpenResult open_scanner(1: TScanOpenParams params);

    // return the batch_size of data
    TScanBatchResult get_next(1: TScanNextBatchParams params);

    // release the context resource associated with the context_id
    TScanCloseResult close_scanner(1: TScanCloseParams params);
}
