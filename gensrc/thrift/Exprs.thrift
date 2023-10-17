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
  AGG_EXPR,
  ARITHMETIC_EXPR,
  BINARY_PRED,
  BOOL_LITERAL,
  CASE_EXPR,
  CAST_EXPR,
  COMPOUND_PRED,
  DATE_LITERAL,
  FLOAT_LITERAL,
  INT_LITERAL,
  DECIMAL_LITERAL,
  IN_PRED,
  IS_NULL_PRED,
  LIKE_PRED,
  LITERAL_PRED,
  NULL_LITERAL,
  SLOT_REF,
  STRING_LITERAL,
  TUPLE_IS_NULL_PRED,
  INFO_FUNC,
  FUNCTION_CALL,
  ARRAY_LITERAL,
  
  // TODO: old style compute functions. this will be deprecated
  COMPUTE_FUNCTION_CALL,
  LARGE_INT_LITERAL,

  // only used in runtime filter
  BLOOM_PRED,

  // for josn
  JSON_LITERAL,

  // only used in runtime filter
  BITMAP_PRED,

  // for fulltext search
  MATCH_PRED,

  // for map 
  MAP_LITERAL,

  // for struct
  STRUCT_LITERAL,

  // for schema change
  SCHEMA_CHANGE_EXPR,
  // for lambda function expr
  LAMBDA_FUNCTION_EXPR,
  LAMBDA_FUNCTION_CALL_EXPR,
  // for column_ref expr
  COLUMN_REF,
}

//enum TAggregationOp {
//  INVALID,
//  COUNT,
//  MAX,
//  DISTINCT_PC,
//  MERGE_PC,
//  DISTINCT_PCSA,
//  MERGE_PCSA,
//  MIN,
//  SUM,
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
}

struct TLiteralPredicate {
  1: required bool value
  2: required bool is_null
}

enum TNullSide {
   LEFT,
   RIGHT
}

struct TTupleIsNullPredicate {
  1: required list<Types.TTupleId> tuple_ids
  2: optional TNullSide null_side
}

struct TSlotRef {
  1: required Types.TSlotId slot_id
  2: required Types.TTupleId tuple_id
  3: optional i32 col_unique_id
}

struct TColumnRef {
  1: optional Types.TSlotId column_id
  2: optional string column_name
}

struct TStringLiteral {
  1: required string value;
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
  28: optional Types.TPrimitiveType child_type

  // For vectorized engine
  29: optional bool is_nullable
  
  30: optional TJsonLiteral json_literal
  31: optional TSchemaChangeExpr schema_change_expr 

  32: optional TColumnRef column_ref 
  33: optional TMatchPredicate match_predicate
}

// A flattened representation of a tree of Expr nodes, obtained by depth-first
// traversal.
struct TExpr {
  1: required list<TExprNode> nodes
}

struct TExprList {
  1: required list<TExpr> exprs
}


