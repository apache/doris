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

#include "exprs/decimal_v2_operators.h"

#include <iomanip>
#include <sstream>
#include <math.h>

#include "exprs/anyval_util.h"
#include "exprs/case_expr.h"
#include "exprs/expr.h"
#include "runtime/tuple_row.h"
// #include "util/decimal_util.h"
#include "util/string_parser.hpp"

namespace doris {

void Decimal_V2Operators::init() {
}

#define CAST_INT_TO_DECIMAL(from_type) \
    Decimal_V2Val Decimal_V2Operators::cast_to_decimal_v2_val( \
            FunctionContext* context, const from_type& val) { \
        if (val.is_null) return Decimal_V2Val::null(); \
        Decimal_V2Value dv = val.val;\
        Decimal_V2Val result;\
        dv.to_decimal_val(&result);\
        return result;\
    }

#define CAST_INT_TO_DECIMALS() \
    CAST_INT_TO_DECIMAL(TinyIntVal);\
    CAST_INT_TO_DECIMAL(SmallIntVal);\
    CAST_INT_TO_DECIMAL(IntVal);\
    CAST_INT_TO_DECIMAL(BigIntVal);\
    CAST_INT_TO_DECIMAL(LargeIntVal);\

CAST_INT_TO_DECIMALS();

Decimal_V2Val Decimal_V2Operators::cast_to_decimal_v2_val(
        FunctionContext* context, const FloatVal& val) {
    if (val.is_null) {
        return Decimal_V2Val::null();
    }
    Decimal_V2Value dv;
    dv.assign_from_float(val.val);
    Decimal_V2Val result;
    dv.to_decimal_val(&result);
    return result;
}

Decimal_V2Val Decimal_V2Operators::cast_to_decimal_v2_val(
        FunctionContext* context, const DoubleVal& val) {
    if (val.is_null) {
        return Decimal_V2Val::null();
    }
    Decimal_V2Value dv;
    dv.assign_from_double(val.val);
    Decimal_V2Val result;
    dv.to_decimal_val(&result);
    return result;
}

Decimal_V2Val Decimal_V2Operators::cast_to_decimal_v2_val(
        FunctionContext* context, const StringVal& val) {
    if (val.is_null) {
        return Decimal_V2Val::null();
    }
    Decimal_V2Value dv;
    if (dv.parse_from_str((const char*)val.ptr, val.len)) {
        return Decimal_V2Val::null();
    }
    Decimal_V2Val result;
    dv.to_decimal_val(&result);
    return result;
}

#define CAST_DECIMAL_TO_INT(to_type, type_name) \
    to_type Decimal_V2Operators::cast_to_##type_name( \
            FunctionContext* context, const Decimal_V2Val& val) { \
        if (val.is_null) return to_type::null(); \
        Decimal_V2Value dv = Decimal_V2Value::from_decimal_val(val); \
        return to_type(dv);\
    }

#define CAST_FROM_DECIMAL() \
    CAST_DECIMAL_TO_INT(BooleanVal, boolean_val);\
    CAST_DECIMAL_TO_INT(TinyIntVal, tiny_int_val);\
    CAST_DECIMAL_TO_INT(SmallIntVal, small_int_val);\
    CAST_DECIMAL_TO_INT(IntVal, int_val);\
    CAST_DECIMAL_TO_INT(BigIntVal, big_int_val);\
    CAST_DECIMAL_TO_INT(LargeIntVal, large_int_val);\
    CAST_DECIMAL_TO_INT(FloatVal, float_val);\
    CAST_DECIMAL_TO_INT(DoubleVal, double_val);

CAST_FROM_DECIMAL();

StringVal Decimal_V2Operators::cast_to_string_val(
        FunctionContext* ctx, const Decimal_V2Val& val) {
    if (val.is_null) {
        return StringVal::null();
    }
    const Decimal_V2Value& dv = Decimal_V2Value::from_decimal_val(val);
    return AnyValUtil::from_string_temp(ctx, dv.to_string());
}

DateTimeVal Decimal_V2Operators::cast_to_datetime_val(
        FunctionContext* context, const Decimal_V2Val& val) {
    if (val.is_null) {
        return DateTimeVal::null();
    }
    const Decimal_V2Value& dv = Decimal_V2Value::from_decimal_val(val);
    DateTimeValue dt;
    if (!dt.from_date_int64(dv)) {
        return DateTimeVal::null();
    }
    DateTimeVal result;
    dt.to_datetime_val(&result);
    return result;
}

#define DECIMAL_ARITHMETIC_OP(FN_NAME, OP) \
    Decimal_V2Val Decimal_V2Operators::FN_NAME##_decimal_v2_val_decimal_v2_val( \
            FunctionContext* context, const Decimal_V2Val& v1, const Decimal_V2Val& v2) { \
        if (v1.is_null || v2.is_null) return Decimal_V2Val::null(); \
        Decimal_V2Value iv1 = Decimal_V2Value::from_decimal_val(v1); \
        Decimal_V2Value iv2 = Decimal_V2Value::from_decimal_val(v2); \
        Decimal_V2Value ir = iv1 OP iv2; \
        Decimal_V2Val result;\
        ir.to_decimal_val(&result); \
        return result; \
    }

#define DECIMAL_ARITHMETIC_OPS() \
    DECIMAL_ARITHMETIC_OP(add, +);\
    DECIMAL_ARITHMETIC_OP(subtract, -);\
    DECIMAL_ARITHMETIC_OP(multiply, *);\
    DECIMAL_ARITHMETIC_OP(divide, /);\
    DECIMAL_ARITHMETIC_OP(mod, %);\

DECIMAL_ARITHMETIC_OPS();

#define DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(NAME, OP) \
    BooleanVal Decimal_V2Operators::NAME##_decimal_v2_val_decimal_v2_val(\
            FunctionContext* c, const Decimal_V2Val& v1, const Decimal_V2Val& v2) {\
        if (v1.is_null || v2.is_null) return BooleanVal::null();\
        Decimal_V2Value iv1 = Decimal_V2Value::from_decimal_val(v1);\
        Decimal_V2Value iv2 = Decimal_V2Value::from_decimal_val(v2);\
        return BooleanVal(iv1 OP iv2);\
    }

#define BINARY_PREDICATE_NONNUMERIC_FNS() \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(eq, ==); \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(ne, !=); \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(gt, >); \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(lt, <); \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(ge, >=); \
    DECIMAL_BINARY_PREDICATE_NONNUMERIC_FN(le, <=);

BINARY_PREDICATE_NONNUMERIC_FNS();

}

