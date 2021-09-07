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

#include "exprs/time_operators.h"

#include <math.h>

#include <iomanip>
#include <sstream>

#include "exprs/anyval_util.h"
#include "exprs/case_expr.h"
#include "exprs/expr.h"
#include "runtime/tuple_row.h"
#include "util/date_func.h"
#include "util/string_parser.hpp"

namespace doris {
void TimeOperators::init() {}

#define CAST_TIME_TO_INT(to_type, type_name)                                                     \
    to_type TimeOperators::cast_to_##type_name(FunctionContext* context, const DoubleVal& val) { \
        if (val.is_null) return to_type::null();                                                 \
        int time = (int)val.val;                                                                 \
        int second = time % 60;                                                                  \
        int minute = time / 60 % 60;                                                             \
        int hour = time / 3600;                                                                  \
        return to_type(hour * 10000 + minute * 100 + second);                                    \
    }

#define CAST_FROM_TIME()                          \
    CAST_TIME_TO_INT(BooleanVal, boolean_val);    \
    CAST_TIME_TO_INT(TinyIntVal, tiny_int_val);   \
    CAST_TIME_TO_INT(SmallIntVal, small_int_val); \
    CAST_TIME_TO_INT(IntVal, int_val);            \
    CAST_TIME_TO_INT(BigIntVal, big_int_val);     \
    CAST_TIME_TO_INT(LargeIntVal, large_int_val); \
    CAST_TIME_TO_INT(FloatVal, float_val);        \
    CAST_TIME_TO_INT(DoubleVal, double_val);

CAST_FROM_TIME();

StringVal TimeOperators::cast_to_string_val(FunctionContext* ctx, const DoubleVal& val) {
    if (val.is_null) {
        return StringVal::null();
    }
    char buffer[MAX_TIME_WIDTH];
    int len = time_to_buffer_from_double(val.val, buffer);
    return AnyValUtil::from_buffer_temp(ctx, buffer, len);
}

DateTimeVal TimeOperators::cast_to_datetime_val(FunctionContext* context, const DoubleVal& val) {
    return DateTimeVal::null();
}
} // namespace doris
