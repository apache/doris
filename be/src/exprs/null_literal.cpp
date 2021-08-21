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

#include "null_literal.h"

#include "gen_cpp/Exprs_types.h"
#include "runtime/runtime_state.h"

namespace doris {

NullLiteral::NullLiteral(const TExprNode& node) : Expr(node) {}

// NullLiteral::NullLiteral(PrimitiveType type) : Expr(TypeDescriptor(type)) {
// }

BooleanVal NullLiteral::get_boolean_val(ExprContext*, TupleRow*) {
    return BooleanVal::null();
}

TinyIntVal NullLiteral::get_tiny_int_val(ExprContext*, TupleRow*) {
    return TinyIntVal::null();
}

SmallIntVal NullLiteral::get_small_int_val(ExprContext*, TupleRow*) {
    return SmallIntVal::null();
}

IntVal NullLiteral::get_int_val(ExprContext*, TupleRow*) {
    return IntVal::null();
}

BigIntVal NullLiteral::get_big_int_val(ExprContext*, TupleRow*) {
    return BigIntVal::null();
}

FloatVal NullLiteral::get_float_val(ExprContext*, TupleRow*) {
    return FloatVal::null();
}

DoubleVal NullLiteral::get_double_val(ExprContext*, TupleRow*) {
    return DoubleVal::null();
}

StringVal NullLiteral::get_string_val(ExprContext*, TupleRow*) {
    return StringVal::null();
}

DateTimeVal NullLiteral::get_datetime_val(ExprContext*, TupleRow*) {
    return DateTimeVal::null();
}

DecimalV2Val NullLiteral::get_decimalv2_val(ExprContext*, TupleRow*) {
    return DecimalV2Val::null();
}

CollectionVal NullLiteral::get_array_val(ExprContext* context, TupleRow*) {
    return CollectionVal::null();
}
} // namespace doris
