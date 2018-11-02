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

include "Status.thrift"

// PaloService accepts query execution options through beeswax.Query.configuration in
// key:value form. For example, the list of strings could be:
//     "num_nodes:1", "abort_on_error:false"
// The valid keys are listed in this enum. They map to TQueryOptions.
// Note: If you add an option or change the default, you also need to update:
// - PaloService.DEFAULT_QUERY_OPTIONS
// - PaloInternalService.thrift: TQueryOptions
// - PalodClientExecutor.getBeeswaxQueryConfigurations()
// - PaloServer::SetQueryOptions()
// - PaloServer::TQueryOptionsToMap()
enum TPaloQueryOptions {
  // if true, abort execution on the first error
  ABORT_ON_ERROR,
  
  // maximum # of errors to be reported; Unspecified or 0 indicates backend default
  MAX_ERRORS,
  
  // if true, disable llvm codegen
  DISABLE_CODEGEN,
  
  // batch size to be used by backend; Unspecified or a size of 0 indicates backend
  // default
  BATCH_SIZE,

  // a per-machine approximate limit on the memory consumption of this query;
  // unspecified or a limit of 0 means no limit;
  // otherwise specified either as:
  // a) an int (= number of bytes);
  // b) a float followed by "M" (MB) or "G" (GB)
  MEM_LIMIT,
   
  // specifies the degree of parallelism with which to execute the query;
  // 1: single-node execution
  // NUM_NODES_ALL: executes on all nodes that contain relevant data
  // NUM_NODES_ALL_RACKS: executes on one node per rack that holds relevant data
  // > 1: executes on at most that many nodes at any point in time (ie, there can be
  //      more nodes than numNodes with plan fragments for this query, but at most
  //      numNodes would be active at any point in time)
  // Constants (NUM_NODES_ALL, NUM_NODES_ALL_RACKS) are defined in JavaConstants.thrift.
  NUM_NODES,
  
  // maximum length of the scan range; only applicable to HDFS scan range; Unspecified or
  // a length of 0 indicates backend default;  
  MAX_SCAN_RANGE_LENGTH,
  
  // Maximum number of io buffers (per disk)
  MAX_IO_BUFFERS,

  // Number of scanner threads.
  NUM_SCANNER_THREADS,

  QUERY_TIMEOUT,

  // If true, Palo will try to execute on file formats that are not fully supported yet
  ALLOW_UNSUPPORTED_FORMATS,

  // if set and > -1, specifies the default limit applied to a top-level SELECT statement
  // with an ORDER BY but without a LIMIT clause (ie, if the SELECT statement also has
  // a LIMIT clause, this default is ignored)
  DEFAULT_ORDER_BY_LIMIT,

  // DEBUG ONLY:
  // If set to
  //   "[<backend number>:]<node id>:<TExecNodePhase>:<TDebugAction>",
  // the exec node with the given id will perform the specified action in the given
  // phase. If the optional backend number (starting from 0) is specified, only that
  // backend instance will perform the debug action, otherwise all backends will behave
  // in that way.
  // If the string doesn't have the required format or if any of its components is
  // invalid, the option is ignored. 
  DEBUG_ACTION,
  
  // If true, raise an error when the DEFAULT_ORDER_BY_LIMIT has been reached.
  ABORT_ON_DEFAULT_LIMIT_EXCEEDED,

  // If false, the backend dosn't report the success status to coordiator
  IS_REPORT_SUCCESS,
}
