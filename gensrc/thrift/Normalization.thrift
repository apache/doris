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

include "Exprs.thrift"
include "Types.thrift"
include "Opcodes.thrift"
include "Descriptors.thrift"
include "Partitions.thrift"
include "PlanNodes.thrift"

struct TNormalizedOlapScanNode {
  1: optional i64 table_id
  2: optional i64 index_id
  3: optional bool is_preaggregation
  4: optional list<string> key_column_names
  5: optional list<Types.TPrimitiveType> key_column_types
  6: optional string rollup_name
  7: optional string sort_column
  8: optional list<string> select_columns
}

struct TNormalizedAggregateNode {
  1: optional list<Exprs.TExpr> grouping_exprs
  2: optional list<Exprs.TExpr> aggregate_functions
  3: optional Types.TTupleId intermediate_tuple_id
  4: optional Types.TTupleId output_tuple_id
  5: optional bool is_finalize
  6: optional bool use_streaming_preaggregation
  7: optional list<Exprs.TExpr> projectToAggIntermediateTuple
  8: optional list<Exprs.TExpr> projectToAggOutputTuple
}

struct TNormalizedPlanNode {
  1: optional Types.TPlanNodeId node_id
  2: optional PlanNodes.TPlanNodeType node_type
  3: optional i32 num_children
  5: optional set<Types.TTupleId> tuple_ids
  6: optional set<Types.TTupleId> nullable_tuples
  7: optional list<Exprs.TExpr> conjuncts
  8: optional list<Exprs.TExpr> projects
  9: optional i64 limit

  10: optional TNormalizedOlapScanNode olap_scan_node
  11: optional TNormalizedAggregateNode aggregation_node
}