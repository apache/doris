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
include "Opcodes.thrift"

enum TExprNodeType {
  AGG_EXPR = 0,
  ARITHMETIC_EXPR = 1,
  BINARY_PRED = 2,
  BOOL_LITERAL = 3,
  CASE_EXPR = 4,
  CAST_EXPR = 5,
  COMPOUND_PRED = 6,
  DATE_LITERAL = 7,
  FLOAT_LITERAL = 8,
  INT_LITERAL = 9,
  DECIMAL_LITERAL = 10,
  IN_PRED = 11,
  IS_NULL_PRED = 12,
  LIKE_PRED = 13,
  LITERAL_PRED = 14,
  NULL_LITERAL = 15,
  SLOT_REF = 16,
  STRING_LITERAL = 17,
  TUPLE_IS_NULL_PRED = 18,
  INFO_FUNC = 19,
  FUNCTION_CALL = 20,
  ARRAY_LITERAL = 21,
  
  // TODO: old style compute functions. this will be deprecated
  COMPUTE_FUNCTION_CALL = 22,
  LARGE_INT_LITERAL = 23,
  
  // only used in runtime filter
  BLOOM_PRED = 24,
  
  // for josn
  JSON_LITERAL = 25,
  
  // only used in runtime filter
  BITMAP_PRED = 26,
  
  // for fulltext search
  MATCH_PRED = 27,
  
  // for map 
  MAP_LITERAL = 28,
  
  // for struct
  STRUCT_LITERAL = 29,
  
  // for schema change
  SCHEMA_CHANGE_EXPR = 30,
  // for lambda function expr
  LAMBDA_FUNCTION_EXPR = 31,
  LAMBDA_FUNCTION_CALL_EXPR = 32,
  // for column_ref expr
  COLUMN_REF = 33,
  
  IPV4_LITERAL = 34,
  IPV6_LITERAL = 35,
  
  // only used in runtime filter
  // to prevent push to storage layer
  NULL_AWARE_IN_PRED = 36,
  NULL_AWARE_BINARY_PRED = 37,
  TIMEV2_LITERAL = 38,
  VIRTUAL_SLOT_REF = 39,
}

//enum TAggregationOp {
//  INVALID = 0,
//  COUNT = 1,
//  MAX = 2,
//  DISTINCT_PC = 3,
//  MERGE_PC = 4,
//  DISTINCT_PCSA = 5,
//  MERGE_PCSA = 6,
//  MIN = 7,
//  SUM = 8,
//}
//
//struct TAggregateExpr {
//  1: required bool is_star
//  2: required bool is_distinct
//  3: required TAggregationOp op
//}
struct TAggregateExpr {
  // Indicates whether this expr is the merge() of an aggregation.
  1: required bool is_merge_agg
  2: optional list<Types.TTypeDesc> param_types
}
struct TBoolLiteral {
  1: required bool value
}

struct TCaseExpr {
  1: required bool has_case_expr
  2: required bool has_else_expr
}

struct TDateLiteral {
  1: required string value
}

struct TTimeV2Literal {
  1: required double value
}

struct TFloatLiteral {
  1: required double value
}

struct TDecimalLiteral {
  1: required string value
}

struct TIntLiteral {
  1: required i64 value
}

struct TLargeIntLiteral {
  1: required string value
}

struct TIPv4Literal {
  1: required i64 value
}

struct TIPv6Literal {
  1: required string value
}

struct TInPredicate {
  1: required bool is_not_in
}

struct TIsNullPredicate {
  1: required bool is_not_null
}

struct TLikePredicate {
  1: required string escape_char;
}

struct TMatchPredicate {
  1: required string parser_type;
  2: required string parser_mode;
  3: optional map<string, string> char_filter_map;
  4: optional bool parser_lowercase = true;
  5: optional string parser_stopwords = "";
  6: optional string custom_analyzer = "";
}

struct TLiteralPredicate {
  1: required bool value
  2: required bool is_null
}

enum TNullSide {
   LEFT = 0,
   RIGHT = 1
}

struct TTupleIsNullPredicate {
  1: required list<Types.TTupleId> tuple_ids
  2: optional TNullSide null_side
}

struct TSlotRef {
  1: required Types.TSlotId slot_id
  2: required Types.TTupleId tuple_id
  3: optional i32 col_unique_id
  4: optional bool is_virtual_slot
}

struct TColumnRef {
  1: optional Types.TSlotId column_id
  2: optional string column_name
}

struct TStringLiteral {
  1: required string value;
}

struct TNullableStringLiteral {
  1: optional string value;
  2: optional bool is_null = false;
}

struct TJsonLiteral {
  1: required string value;
}

struct TInfoFunc {
  1: required i64 int_value;
  2: required string str_value;
}

struct TFunctionCallExpr {
  // The aggregate function to call.
  1: required Types.TFunction fn

  // If set, this aggregate function udf has varargs and this is the index for the
  // first variable argument.
  2: optional i32 vararg_start_idx
}

struct TSchemaChangeExpr {
  // target schema change table
  1: optional i64 table_id 
}

// This is essentially a union over the subclasses of Expr.
struct TExprNode {
  1: required TExprNodeType node_type
  2: required Types.TTypeDesc type
  3: optional Opcodes.TExprOpcode opcode
  4: required i32 num_children

  5: optional TAggregateExpr agg_expr
  6: optional TBoolLiteral bool_literal
  7: optional TCaseExpr case_expr
  8: optional TDateLiteral date_literal
  9: optional TFloatLiteral float_literal
  10: optional TIntLiteral int_literal
  11: optional TInPredicate in_predicate
  12: optional TIsNullPredicate is_null_pred
  13: optional TLikePredicate like_pred
  14: optional TLiteralPredicate literal_pred
  15: optional TSlotRef slot_ref
  16: optional TStringLiteral string_literal
  17: optional TTupleIsNullPredicate tuple_is_null_pred
  18: optional TInfoFunc info_func
  19: optional TDecimalLiteral decimal_literal

  20: required i32 output_scale
  21: optional TFunctionCallExpr fn_call_expr
  22: optional TLargeIntLiteral large_int_literal

  23: optional i32 output_column
  24: optional Types.TColumnType output_type
  25: optional Opcodes.TExprOpcode vector_opcode
  // The function to execute. Not set for SlotRefs and Literals.
  26: optional Types.TFunction fn
  // If set, child[vararg_start_idx] is the first vararg child.
  27: optional i32 vararg_start_idx
  28: optional Types.TPrimitiveType child_type // Deprecated

  // For vectorized engine
  29: optional bool is_nullable
  
  30: optional TJsonLiteral json_literal
  31: optional TSchemaChangeExpr schema_change_expr 

  32: optional TColumnRef column_ref 
  33: optional TMatchPredicate match_predicate
  34: optional TIPv4Literal ipv4_literal
  35: optional TIPv6Literal ipv6_literal
  36: optional string label // alias name, a/b in `select xxx as a, count(1) as b`
  37: optional TTimeV2Literal timev2_literal
}

// A flattened representation of a tree of Expr nodes, obtained by depth-first
// traversal.
struct TExpr {
  1: required list<TExprNode> nodes
}

struct TExprList {
  1: required list<TExpr> exprs
}


