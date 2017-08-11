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
include "Partitions.thrift"

enum TPlanNodeType {
  OLAP_SCAN_NODE,
  MYSQL_SCAN_NODE,
  CSV_SCAN_NODE,
  SCHEMA_SCAN_NODE,
  HASH_JOIN_NODE,
  MERGE_JOIN_NODE,
  AGGREGATION_NODE,
  PRE_AGGREGATION_NODE,
  SORT_NODE,
  EXCHANGE_NODE,
  MERGE_NODE,
  SELECT_NODE,
  CROSS_JOIN_NODE,
  META_SCAN_NODE,
  ANALYTIC_EVAL_NODE,
  OLAP_REWRITE_NODE,
  KUDU_SCAN_NODE
  BROKER_SCAN_NODE
  EMPTY_SET_NODE    
  UNION_NODE
}

// phases of an execution node
enum TExecNodePhase {
  PREPARE,
  OPEN,
  GETNEXT,
  CLOSE,
  INVALID
}

// what to do when hitting a debug point (TPaloQueryOptions.DEBUG_ACTION)
enum TDebugAction {
  WAIT,
  FAIL
}

struct TKeyRange {
  1: required i64 begin_key
  2: required i64 end_key
  3: required Types.TPrimitiveType column_type
  4: required string column_name
}

// The information contained in subclasses of ScanNode captured in two separate
// Thrift structs:
// - TScanRange: the data range that's covered by the scan (which varies with the
//   particular partition of the plan fragment of which the scan node is a part)
// - T<subclass>: all other operational parameters that are the same across
//   all plan fragments

struct TPaloScanRange {
  1: required list<Types.TNetworkAddress> hosts
  2: required string schema_hash
  3: required string version
  4: required string version_hash
  5: required Types.TTabletId tablet_id
  6: required string db_name
  7: optional list<TKeyRange> partition_column_ranges
  8: optional string index_name
  9: optional string table_name
}

enum TFileFormatType {
    FORMAT_CSV_PLAIN,
    FORMAT_CSV_GZ,
    FORMAT_CSV_LZO,
    FORMAT_CSV_BZ2,
    FORMAT_CSV_LZ4FRAME,
    FORMAT_CSV_LZOP
}

// One broker range information.
struct TBrokerRangeDesc {
    1: required Types.TFileType file_type
    2: required TFileFormatType format_type
    3: required bool splittable;
    // Path of this range
    4: required string path
    // Offset of this file start
    5: required i64 start_offset;
    // Size of this range, if size = -1, this means that will read to then end of file
    6: required i64 size
}

struct TBrokerScanRangeParams {
    1: required byte column_separator;
    2: required byte line_delimiter;

    // We construct one line in file to a tuple. And each field of line 
    // correspond to a slot in this tuple. 
    // src_tuple_id is the tuple id of the input file
    3: required Types.TTupleId src_tuple_id
    // src_slot_ids is the slot_ids of the input file
    // we use this id to find the slot descriptor
    4: required list<Types.TSlotId> src_slot_ids

    // dest_tuple_id is the tuple id that need by scan node
    5: required Types.TTupleId dest_tuple_id
    // This is expr that convert the content read from file
    // the format that need by the compute layer.
    6: optional map<Types.TSlotId, Exprs.TExpr> expr_of_dest_slot

    // properties need to access broker.
    7: optional map<string, string> properties;

    // If partition_ids is set, data that doesn't in this partition will be filtered.
    8: optional list<i64> partition_ids
}

// Broker scan range
struct TBrokerScanRange {
    1: required list<TBrokerRangeDesc> ranges
    2: required TBrokerScanRangeParams params
    3: required list<Types.TNetworkAddress> broker_addresses
}

// Specification of an individual data range which is held in its entirety
// by a storage server
struct TScanRange {
  // one of these must be set for every TScanRange2
  4: optional TPaloScanRange palo_scan_range
  5: optional binary kudu_scan_token
    6: optional TBrokerScanRange broker_scan_range
}

struct TMySQLScanNode {
  1: required Types.TTupleId tuple_id
  2: required string table_name
  3: required list<string> columns
  4: required list<string> filters
}

struct TBrokerScanNode {
    1: required Types.TTupleId tuple_id

    // Partition info used to process partition select in broker load
    2: optional list<Exprs.TExpr> partition_exprs
    3: optional list<Partitions.TRangePartition> partition_infos
}

struct TMiniLoadEtlFunction {
  1: required string function_name
  2: required i32 param_column_index
}

struct TCsvScanNode {
  1: required Types.TTupleId tuple_id
  2: required list<string> file_paths

  3: optional string column_separator
  4: optional string line_delimiter

  // <column_name, ColumnType>
  5: optional map<string, Types.TColumnType> column_type_mapping

  // columns specified in load command
  6: optional list<string> columns
  // <column_name, default_value_in_string>
  7: optional list<string> unspecified_columns
  // always string type, and only contain columns which are not specified
  8: optional list<string> default_values

  9: optional double max_filter_ratio
  10:optional map<string, TMiniLoadEtlFunction> column_function_mapping
}

struct TSchemaScanNode {
  1: required Types.TTupleId tuple_id

  2: required string table_name
  3: optional string db
  4: optional string table
  5: optional string wild
  6: optional string user
  7: optional string ip
  8: optional i32 port
  9: optional i64 thread_id
}

struct TMetaScanNode {
  1: required Types.TTupleId tuple_id

  2: required string table_name
  3: optional string db
  4: optional string table
  5: optional string user
}

struct TOlapScanNode {
  1: required Types.TTupleId tuple_id
  2: required list<string> key_column_name
  3: required list<Types.TPrimitiveType> key_column_type
  4: required bool is_preaggregation
  5: optional string sort_column
}
struct TEqJoinCondition {
  // left-hand side of "<a> = <b>"
  1: required Exprs.TExpr left;
  // right-hand side of "<a> = <b>"
  2: required Exprs.TExpr right;
}

enum TJoinOp {
  INNER_JOIN,
  LEFT_OUTER_JOIN,
  LEFT_SEMI_JOIN,
  RIGHT_OUTER_JOIN,
  FULL_OUTER_JOIN,
  CROSS_JOIN,
  MERGE_JOIN,

  RIGHT_SEMI_JOIN,
  LEFT_ANTI_JOIN,
  RIGHT_ANTI_JOIN,

  // Similar to LEFT_ANTI_JOIN with special handling for NULLs for the join conjuncts
  // on the build side. Those NULLs are considered candidate matches, and therefore could
  // be rejected (ANTI-join), based on the other join conjuncts. This is in contrast
  // to LEFT_ANTI_JOIN where NULLs are not matches and therefore always returned.
  NULL_AWARE_LEFT_ANTI_JOIN
}

struct THashJoinNode {
  1: required TJoinOp join_op

  // anything from the ON, USING or WHERE clauses that's an equi-join predicate
  2: required list<TEqJoinCondition> eq_join_conjuncts

  // anything from the ON or USING clauses (but *not* the WHERE clause) that's not an
  // equi-join predicate
  3: optional list<Exprs.TExpr> other_join_conjuncts
  4: optional bool is_push_down

  // If true, this join node can (but may choose not to) generate slot filters
  // after constructing the build side that can be applied to the probe side.
  5: optional bool add_probe_filters
}

struct TMergeJoinNode {
  // anything from the ON, USING or WHERE clauses that's an equi-join predicate
  1: required list<TEqJoinCondition> cmp_conjuncts

  // anything from the ON or USING clauses (but *not* the WHERE clause) that's not an
  // equi-join predicate
  2: optional list<Exprs.TExpr> other_join_conjuncts
}

enum TAggregationOp {
  INVALID,
  COUNT,
  MAX,
  DISTINCT_PC,
  DISTINCT_PCSA,
  MIN,
  SUM,
  GROUP_CONCAT,
  HLL,
  COUNT_DISTINCT,
  SUM_DISTINCT,
  LEAD,
  FIRST_VALUE,
  LAST_VALUE,
  RANK,
  DENSE_RANK,
  ROW_NUMBER,
  LAG,
  HLL_C, 
}

//struct TAggregateFunctionCall {
  // The aggregate function to call.
//  1: required Types.TFunction fn

  // The input exprs to this aggregate function
//  2: required list<Exprs.TExpr> input_exprs

  // If set, this aggregate function udf has varargs and this is the index for the
  // first variable argument.
//  3: optional i32 vararg_start_idx
//}

struct TAggregationNode {
  1: optional list<Exprs.TExpr> grouping_exprs
  // aggregate exprs. The root of each expr is the aggregate function. The
  // other exprs are the inputs to the aggregate function.
  2: required list<Exprs.TExpr> aggregate_functions

  // Tuple id used for intermediate aggregations (with slots of agg intermediate types)
  3: required Types.TTupleId intermediate_tuple_id

  // Tupld id used for the aggregation output (with slots of agg output types)
  // Equal to intermediate_tuple_id if intermediate type == output type for all
  // aggregate functions.
  4: required Types.TTupleId output_tuple_id

  // Set to true if this aggregation function requires finalization to complete after all
  // rows have been aggregated, and this node is not an intermediate node.
  5: required bool need_finalize
}

struct TPreAggregationNode {
  1: required list<Exprs.TExpr> group_exprs
  2: required list<Exprs.TExpr> aggregate_exprs
}

struct TSortInfo {
  1: required list<Exprs.TExpr> ordering_exprs
  2: required list<bool> is_asc_order
  // Indicates, for each expr, if nulls should be listed first or last. This is
  // independent of is_asc_order.
  3: required list<bool> nulls_first
  // Expressions evaluated over the input row that materialize the tuple to be sorted.
  // Contains one expr per slot in the materialized tuple.
  4: optional list<Exprs.TExpr> sort_tuple_slot_exprs
}

struct TSortNode {
  1: required TSortInfo sort_info
  // Indicates whether the backend service should use topn vs. sorting
  2: required bool use_top_n;
  // This is the number of rows to skip before returning results
  3: optional i64 offset

  // TODO(lingbin): remove blew, because duplaicate with TSortInfo
  4: optional list<Exprs.TExpr> ordering_exprs                                   
  5: optional list<bool> is_asc_order                                            
  // Indicates whether the imposed limit comes DEFAULT_ORDER_BY_LIMIT.           
  6: optional bool is_default_limit                                              
  // Indicates, for each expr, if nulls should be listed first or last. This is  
  // independent of is_asc_order.                                                
  7: optional list<bool> nulls_first                                             
  // Expressions evaluated over the input row that materialize the tuple to be so
  // Contains one expr per slot in the materialized tuple.                       
  8: optional list<Exprs.TExpr> sort_tuple_slot_exprs                            
}

enum TAnalyticWindowType {
  // Specifies the window as a logical offset
  RANGE,

  // Specifies the window in physical units
  ROWS
}

enum TAnalyticWindowBoundaryType {
  // The window starts/ends at the current row.
  CURRENT_ROW,

  // The window starts/ends at an offset preceding current row.
  PRECEDING,

  // The window starts/ends at an offset following current row.
  FOLLOWING
}

struct TAnalyticWindowBoundary {
  1: required TAnalyticWindowBoundaryType type

  // Predicate that checks: child tuple '<=' buffered tuple + offset for the orderby expr
  2: optional Exprs.TExpr range_offset_predicate

  // Offset from the current row for ROWS windows.
  3: optional i64 rows_offset_value
}

struct TAnalyticWindow {
  // Specifies the window type for the start and end bounds.
  1: required TAnalyticWindowType type

  // Absence indicates window start is UNBOUNDED PRECEDING.
  2: optional TAnalyticWindowBoundary window_start

  // Absence indicates window end is UNBOUNDED FOLLOWING.
  3: optional TAnalyticWindowBoundary window_end
}

// Defines a group of one or more analytic functions that share the same window,
// partitioning expressions and order-by expressions and are evaluated by a single
// ExecNode.
struct TAnalyticNode {
  // Exprs on which the analytic function input is partitioned. Input is already sorted
  // on partitions and order by clauses, partition_exprs is used to identify partition
  // boundaries. Empty if no partition clause is specified.
  1: required list<Exprs.TExpr> partition_exprs

  // Exprs specified by an order-by clause for RANGE windows. Used to evaluate RANGE
  // window boundaries. Empty if no order-by clause is specified or for windows
  // specifying ROWS.
  2: required list<Exprs.TExpr> order_by_exprs

  // Functions evaluated over the window for each input row. The root of each expr is
  // the aggregate function. Child exprs are the inputs to the function.
  3: required list<Exprs.TExpr> analytic_functions

  // Window specification
  4: optional TAnalyticWindow window

  // Tuple used for intermediate results of analytic function evaluations
  // (with slots of analytic intermediate types)
  5: required Types.TTupleId intermediate_tuple_id

  // Tupld used for the analytic function output (with slots of analytic output types)
  // Equal to intermediate_tuple_id if intermediate type == output type for all
  // analytic functions.
  6: required Types.TTupleId output_tuple_id

  // id of the buffered tuple (identical to the input tuple, which is assumed
  // to come from a single SortNode); not set if both partition_exprs and
  // order_by_exprs are empty
  7: optional Types.TTupleId buffered_tuple_id

  // predicate that checks: child tuple is in the same partition as the buffered tuple,
  // i.e. each partition expr is equal or both are not null. Only set if
  // buffered_tuple_id is set; should be evaluated over a row that is composed of the
  // child tuple and the buffered tuple
  8: optional Exprs.TExpr partition_by_eq

  // predicate that checks: the order_by_exprs are equal or both NULL when evaluated
  // over the child tuple and the buffered tuple. only set if buffered_tuple_id is set;
  // should be evaluated over a row that is composed of the child tuple and the buffered
  // tuple
  9: optional Exprs.TExpr order_by_eq
}

struct TMergeNode {
  // A MergeNode could be the left input of a join and needs to know which tuple to write.
  1: required Types.TTupleId tuple_id
  // List or expr lists materialized by this node.
  // There is one list of exprs per query stmt feeding into this merge node.
  2: required list<list<Exprs.TExpr>> result_expr_lists
  // Separate list of expr lists coming from a constant select stmts.
  3: required list<list<Exprs.TExpr>> const_expr_lists
}

struct TUnionNode {
    // A UnionNode materializes all const/result exprs into this tuple.
    1: required Types.TTupleId tuple_id
    // List or expr lists materialized by this node.
    // There is one list of exprs per query stmt feeding into this union node.
    2: required list<list<Exprs.TExpr>> result_expr_lists
    // Separate list of expr lists coming from a constant select stmts.
    3: required list<list<Exprs.TExpr>> const_expr_lists
    // Index of the first child that needs to be materialized.
    4: required i64 first_materialized_child_idx
}

struct TExchangeNode {
  // The ExchangeNode's input rows form a prefix of the output rows it produces;
  // this describes the composition of that prefix
  1: required list<Types.TTupleId> input_row_tuples
  // For a merging exchange, the sort information.
  2: optional TSortInfo sort_info
  // This is tHe number of rows to skip before returning results
  3: optional i64 offset
}

struct TOlapRewriteNode {
    1: required list<Exprs.TExpr> columns
    2: required list<Types.TColumnType> column_types
    3: required Types.TTupleId output_tuple_id
}

struct TKuduScanNode {
  1: required Types.TTupleId tuple_id
}

// This is essentially a union of all messages corresponding to subclasses
// of PlanNode.
struct TPlanNode {
  // node id, needed to reassemble tree structure
  1: required Types.TPlanNodeId node_id
  2: required TPlanNodeType node_type
  3: required i32 num_children
  4: required i64 limit
  5: required list<Types.TTupleId> row_tuples

  // nullable_tuples[i] is true if row_tuples[i] is nullable
  6: required list<bool> nullable_tuples
  7: optional list<Exprs.TExpr> conjuncts

  // Produce data in compact format.
  8: required bool compact_data

  // one field per PlanNode subclass
  11: optional THashJoinNode hash_join_node
  12: optional TAggregationNode agg_node
  13: optional TSortNode sort_node
  14: optional TMergeNode merge_node
  15: optional TExchangeNode exchange_node
  17: optional TMySQLScanNode mysql_scan_node
  18: optional TOlapScanNode olap_scan_node  
  19: optional TCsvScanNode csv_scan_node  
  20: optional TBrokerScanNode broker_scan_node  
  21: optional TPreAggregationNode pre_agg_node
  22: optional TSchemaScanNode schema_scan_node
  23: optional TMergeJoinNode merge_join_node
  24: optional TMetaScanNode meta_scan_node
  25: optional TAnalyticNode analytic_node
  26: optional TOlapRewriteNode olap_rewrite_node
  27: optional TKuduScanNode kudu_scan_node
  28: optional TUnionNode union_node
}

// A flattened representation of a tree of PlanNodes, obtained by depth-first
// traversal.
struct TPlan {
  1: required list<TPlanNode> nodes
}
