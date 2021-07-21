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

#ifndef DORIS_BE_SRC_EXPRS_TIME_OPERATORS_H
#define DORIS_BE_SRC_EXPRS_TIME_OPERATORS_H

#include <stdint.h>

#include "udf/udf.h"

namespace doris {
class Expr;
struct ExprValue;
class TupleRow;

/// Implementation of the time operators. These include the cast,
/// arithmetic and binary operators.
class TimeOperators {
public:
    static void init();

    static BooleanVal cast_to_boolean_val(FunctionContext*, const DoubleVal&);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext*, const DoubleVal&);
    static SmallIntVal cast_to_small_int_val(FunctionContext*, const DoubleVal&);
    static IntVal cast_to_int_val(FunctionContext*, const DoubleVal&);
    static BigIntVal cast_to_big_int_val(FunctionContext*, const DoubleVal&);
    static LargeIntVal cast_to_large_int_val(FunctionContext*, const DoubleVal&);
    static FloatVal cast_to_float_val(FunctionContext*, const DoubleVal&);
    static DoubleVal cast_to_double_val(FunctionContext*, const DoubleVal&);
    static StringVal cast_to_string_val(FunctionContext*, const DoubleVal&);
    static DateTimeVal cast_to_datetime_val(FunctionContext*, const DoubleVal&);
};
} // namespace doris
#endif
