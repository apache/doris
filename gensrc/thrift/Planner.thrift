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
include "DataSinks.thrift"
include "PlanNodes.thrift"
include "Partitions.thrift"
include "QueryCache.thrift"

// TPlanFragment encapsulates info needed to execute a particular
// plan fragment, including how to produce and how to partition its output.
// It leaves out node-specific parameters neede for the actual execution.
struct TPlanFragment {
  // no plan or descriptor table: query without From clause
  2: optional PlanNodes.TPlan plan

  // exprs that produce values for slots of output tuple (one expr per slot);
  // if not set, plan fragment materializes full rows of plan_tree
  4: optional list<Exprs.TExpr> output_exprs
  
  // Specifies the destination of this plan fragment's output rows.
  // For example, the destination could be a stream sink which forwards 
  // the data to a remote plan fragment, or a sink which writes to a table (for
  // insert stmts).
  5: optional DataSinks.TDataSink output_sink

  // Partitioning of the data created by all instances of this plan fragment;
  // partitioning.type has the following meaning:
  // - UNPARTITIONED: there is only one instance of the plan fragment
  // - RANDOM: a particular output row is randomly assigned to any of the instances
  // - HASH_PARTITIONED: output row r is produced by
  //   hash_value(partitioning.partitioning_exprs(r)) % #partitions
  // - RANGE_PARTITIONING: currently not supported
  // This is distinct from the partitioning of each plan fragment's
  // output, which is specified by output_sink.output_partitioning.
  6: required Partitions.TDataPartition partition

  // The minimum reservation size (in bytes) required for an instance of this plan
  // fragment to execute on a single host.
  7: optional i64 min_reservation_bytes

  // Total of the initial buffer reservations that we expect to be claimed by this
  // fragment. I.e. the sum of the min reservations over all operators (including the
  // sink) in a single instance of this fragment. This is used for an optimization in
  // InitialReservation. Measured in bytes. required in V1
  8: optional i64 initial_reservation_total_claims

  9: optional QueryCache.TQueryCacheParam query_cache_param
}

// location information for a single scan range
struct TScanRangeLocation {
  1: required Types.TNetworkAddress server

  // disk volume identifier of a particular scan range at 'server';
  // -1 indicates an unknown volume id;
  // only set for TScanRange.hdfs_file_split
  2: optional i32 volume_id = -1
  3: optional i64 backend_id
}

// A single scan range plus the hosts that serve it
struct TScanRangeLocations {
  1: required PlanNodes.TScanRange scan_range
  // non-empty list
  2: list<TScanRangeLocation> locations
}