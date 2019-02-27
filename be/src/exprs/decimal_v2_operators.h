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

#ifndef DORIS_BE_SRC_EXPRS_DECIMAL_OPERATORS_H
#define DORIS_BE_SRC_EXPRS_DECIMAL_OPERATORS_H

#include <stdint.h>
#include "runtime/decimal_v2_value.h"
#include "udf/udf.h"

namespace doris {

class Expr;
struct ExprValue;
class TupleRow;

/// Implementation of the decimal operators. These include the cast,
/// arithmetic and binary operators.
class Decimal_V2Operators {
public:
    static void init();
    
    static Decimal_V2Val cast_to_decimal_v2_val(FunctionContext*, const TinyIntVal&);
    static Decimal_V2Val cast_to_decimal_v2_val(FunctionContext*, const SmallIntVal&);
    static Decimal_V2Val cast_to_decimal_v2_val(FunctionContext*, const IntVal&);
    static Decimal_V2Val cast_to_decimal_v2_val(FunctionContext*, const BigIntVal&);
    static Decimal_V2Val cast_to_decimal_v2_val(FunctionContext*, const LargeIntVal&);
    static Decimal_V2Val cast_to_decimal_v2_val(FunctionContext*, const FloatVal&);
    static Decimal_V2Val cast_to_decimal_v2_val(FunctionContext*, const DoubleVal&);
    static Decimal_V2Val cast_to_decimal_v2_val(FunctionContext*, const StringVal&);

    static BooleanVal cast_to_boolean_val(FunctionContext*, const Decimal_V2Val&);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext*, const Decimal_V2Val&);
    static SmallIntVal cast_to_small_int_val(FunctionContext*, const Decimal_V2Val&);
    static IntVal cast_to_int_val(FunctionContext*, const Decimal_V2Val&);
    static BigIntVal cast_to_big_int_val(FunctionContext*, const Decimal_V2Val&);
    static LargeIntVal cast_to_large_int_val(FunctionContext*, const Decimal_V2Val&);
    static FloatVal cast_to_float_val(FunctionContext*, const Decimal_V2Val&);
    static DoubleVal cast_to_double_val(FunctionContext*, const Decimal_V2Val&);
    static StringVal cast_to_string_val(FunctionContext*, const Decimal_V2Val&);
    static DateTimeVal cast_to_datetime_val(FunctionContext*, const Decimal_V2Val&);
    static DecimalVal cast_to_decimal_val(FunctionContext*, const Decimal_V2Val&);

    static Decimal_V2Val add_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);
    static Decimal_V2Val subtract_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);
    static Decimal_V2Val multiply_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);
    static Decimal_V2Val divide_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);
    static Decimal_V2Val mod_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);

    static BooleanVal eq_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);
    static BooleanVal ne_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);
    static BooleanVal gt_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);
    static BooleanVal lt_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);
    static BooleanVal ge_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);
    static BooleanVal le_decimal_v2_val_decimal_v2_val(
        FunctionContext*, const Decimal_V2Val&, const Decimal_V2Val&);
};

}

#endif
