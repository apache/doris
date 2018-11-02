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

#ifdef IR_COMPILE
struct __float128;
#include "codegen/codegen_anyval_ir.cpp"
#include "exec/aggregation_node_ir.cpp"
#include "exec/hash_join_node_ir.cpp"
#include "exprs/aggregate_functions.cpp"
#include "exprs/cast_functions.cpp"
#include "exprs/conditional_functions_ir.cpp"
#include "exprs/decimal_operators.cpp"
#include "exprs/expr_ir.cpp"
#include "exprs/is_null_predicate.cpp"
#include "exprs/like_predicate.cpp"
#include "exprs/math_functions.cpp"
#include "exprs/operators.cpp"
#include "exprs/string_functions.cpp"
#include "exprs/timestamp_functions.cpp"
#include "exprs/utility_functions.cpp"
#include "runtime/raw_value_ir.cpp"
#include "runtime/string_value_ir.cpp"
#include "udf/udf_ir.cpp"
#include "util/hash_util_ir.cpp"
#else
#error "This file should only be used for cross compiling to IR."
#endif

