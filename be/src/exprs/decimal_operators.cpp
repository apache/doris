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

#include "exprs/decimal_operators.h"

#include <math.h>

#include <iomanip>
#include <sstream>

#include "exprs/anyval_util.h"
#include "exprs/case_expr.h"
#include "exprs/expr.h"
#include "runtime/tuple_row.h"
// #include "util/decimal_util.h"
#include "util/string_parser.hpp"

namespace doris {

void DecimalOperators::init() {}

#define CAST_INT_TO_DECIMAL(from_type)                                         \
    DecimalVal DecimalOperators::cast_to_decimal_val(FunctionContext* context, \
                                                     const from_type& val) {   \
        if (val.is_null) return DecimalVal::null();                            \
        DecimalValue dv = val.val;                                             \
        DecimalVal result;                                                     \
        dv.to_decimal_val(&result);                                            \
        return result;                                                         \
    }

#define CAST_INT_TO_DECIMALS()        \
    CAST_INT_TO_DECIMAL(TinyIntVal);  \
    CAST_INT_TO_DECIMAL(SmallIntVal); \
    CAST_INT_TO_DECIMAL(IntVal);      \
    CAST_INT_TO_DECIMAL(BigIntVal);   \
    CAST_INT_TO_DECIMAL(LargeIntVal);

CAST_INT_TO_DECIMALS();

DecimalVal DecimalOperators::cast_to_decimal_val(FunctionContext* context, const FloatVal& val) {
    if (val.is_null) {
        return DecimalVal::null();
    }
    DecimalValue dv;
    dv.assign_from_float(val.val);
    DecimalVal result;
    dv.to_decimal_val(&result);
    return result;
}

DecimalVal DecimalOperators::cast_to_decimal_val(FunctionContext* context, const DoubleVal& val) {
    if (val.is_null) {
        return DecimalVal::null();
    }
    DecimalValue dv;
    dv.assign_from_double(val.val);
    DecimalVal result;
    dv.to_decimal_val(&result);
    return result;
}

DecimalVal DecimalOperators::cast_to_decimal_val(FunctionContext* context, const DateTimeVal& val) {
    if (val.is_null) {
        return DecimalVal::null();
    }

    DateTimeValue dt_value = DateTimeValue::from_datetime_val(val);

    DecimalValue dv = dt_value.to_int64();
    DecimalVal result;
    dv.to_decimal_val(&result);
    return result;
}

DecimalVal DecimalOperators::cast_to_decimal_val(FunctionContext* context, const StringVal& val) {
    if (val.is_null) {
        return DecimalVal::null();
    }
    DecimalValue dv;
    if (dv.parse_from_str((const char*)val.ptr, val.len)) {
        return DecimalVal::null();
    }
    DecimalVal result;
    dv.to_decimal_val(&result);
    return result;
}

#define CAST_DECIMAL_TO_INT(to_type, type_name)                             \
    to_type DecimalOperators::cast_to_##type_name(FunctionContext* context, \
                                                  const DecimalVal& val) {  \
        if (val.is_null) return to_type::null();                            \
        DecimalValue dv = DecimalValue::from_decimal_val(val);              \
        return to_type(dv);                                                 \
    }

#define CAST_FROM_DECIMAL()                          \
    CAST_DECIMAL_TO_INT(BooleanVal, boolean_val);    \
    CAST_DECIMAL_TO_INT(TinyIntVal, tiny_int_val);   \
    CAST_DECIMAL_TO_INT(SmallIntVal, small_int_val); \
    CAST_DECIMAL_TO_INT(IntVal, int_val);            \
    CAST_DECIMAL_TO_INT(BigIntVal, big_int_val);     \
    CAST_DECIMAL_TO_INT(LargeIntVal, large_int_val); \
    CAST_DECIMAL_TO_INT(FloatVal, float_val);        \
    CAST_DECIMAL_TO_INT(DoubleVal, double_val);

CAST_FROM_DECIMAL();

StringVal DecimalOperators::cast_to_string_val(FunctionContext* ctx, const DecimalVal& val) {
    if (val.is_null) {
        return StringVal::null();
    }
    const DecimalValue& dv = DecimalValue::from_decimal_val(val);
    return AnyValUtil::from_string_temp(ctx, dv.to_string());
}

DateTimeVal DecimalOperators::cast_to_datetime_val(FunctionContext* context,
                                                   const DecimalVal& val) {
    if (val.is_null) {
        return DateTimeVal::null();
    }
    const DecimalValue& dv = DecimalValue::from_decimal_val(val);
    DateTimeValue dt;
    if (!dt.from_date_int64(dv)) {
        return DateTimeVal::null();
    }
    DateTimeVal result;
    dt.to_datetime_val(&result);
    return result;
}

#define DECIMAL_ARITHMETIC_OP(FN_NAME, OP)                                          \
    DecimalVal DecimalOperators::FN_NAME##_decimal_val_decimal_val(                 \
            FunctionContext* context, const DecimalVal& v1, const DecimalVal& v2) { \
        if (v1.is_null || v2.is_null) return DecimalVal::null();                    \
        DecimalValue iv1 = DecimalValue::from_decimal_val(v1);                      \
        DecimalValue iv2 = DecimalValue::from_decimal_val(v2);                      \
        DecimalValue ir = iv1 OP iv2;                                               \
        DecimalVal result;                                                          \
        ir.to_decimal_val(&result);                                                 \
        return result;                                                              \
    }

#define DECIMAL_ARITHMETIC_OPS()        \
    DECIMAL_ARITHMETIC_OP(add, +);      \
    DECIMAL_ARITHMETIC_OP(subtract, -); \
    DECIMAL_ARITHMETIC_OP(multiply, *); \
    DECIMAL_ARITHMETIC_OP(divide, /);   \
    DECIMAL_ARITHMETIC_OP(mod, %);

DECIMAL_ARITHMETIC_OPS();

#define DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(NAME, OP)                      \
    BooleanVal DecimalOperators::NAME##_decimal_val_decimal_val(              \
            FunctionContext* c, const DecimalVal& v1, const DecimalVal& v2) { \
        if (v1.is_null || v2.is_null) return BooleanVal::null();              \
        DecimalValue iv1 = DecimalValue::from_decimal_val(v1);                \
        DecimalValue iv2 = DecimalValue::from_decimal_val(v2);                \
        return BooleanVal(iv1 OP iv2);                                        \
    }

#define BINARY_PREDICATE_NONNUMERIC_FNS()           \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(eq, ==); \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(ne, !=); \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(gt, >);  \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(lt, <);  \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(ge, >=); \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(le, <=);

BINARY_PREDICATE_NONNUMERIC_FNS();

} // namespace doris
