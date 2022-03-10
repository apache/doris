# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exprs")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exprs")

add_library(Exprs
  encryption_functions.cpp
  aggregate_functions.cpp
  agg_fn_evaluator.cpp
  anyval_util.cpp
  arithmetic_expr.cpp
  binary_predicate.cpp
  case_expr.cpp
  cast_expr.cpp
  cast_functions.cpp
  compound_predicate.cpp
  conditional_functions.cpp
  decimalv2_operators.cpp
  time_operators.cpp
  es_functions.cpp
  hash_functions.cpp
  literal.cpp
  expr.cpp
  expr_context.cpp
  in_predicate.cpp
  new_in_predicate.cpp
  bloomfilter_predicate.cpp
  block_bloom_filter_avx_impl.cc
  block_bloom_filter_impl.cc
  runtime_filter.cpp
  runtime_filter_rpc.cpp
  is_null_predicate.cpp
  like_predicate.cpp
  math_functions.cpp
  null_literal.cpp  
  scalar_fn_call.cpp
  rpc_fn.cpp
  rpc_fn_call.cpp
  slot_ref.cpp
  string_functions.cpp
  array_functions.cpp
  timestamp_functions.cpp
  tuple_is_null_predicate.cpp
  udf_builtins.cpp
  utility_functions.cpp
  info_func.cpp
  json_functions.cpp
  operators.cpp
  hll_hash_function.cpp
  agg_fn.cpp
  new_agg_fn_evaluator.cc
  bitmap_function.cpp
  hll_function.cpp
  quantile_function.cpp
  grouping_sets_functions.cpp
  topn_function.cpp
  table_function/explode_split.cpp
  table_function/explode_bitmap.cpp
  table_function/explode_json_array.cpp
  table_function/table_function_factory.cpp
  table_function/dummy_table_functions.cpp
)
