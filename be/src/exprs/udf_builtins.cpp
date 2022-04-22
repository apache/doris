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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/udf-builtins.cc
// and modified by Doris

#include "exprs/udf_builtins.h"

#include <ctype.h>
#include <math.h>

#include "common/logging.h"

namespace doris {
using doris_udf::FunctionContext;
using doris_udf::BooleanVal;
using doris_udf::TinyIntVal;
using doris_udf::SmallIntVal;
using doris_udf::IntVal;
using doris_udf::BigIntVal;
using doris_udf::LargeIntVal;
using doris_udf::FloatVal;
using doris_udf::DoubleVal;
using doris_udf::DecimalV2Val;
using doris_udf::StringVal;
using doris_udf::AnyVal;

DoubleVal UdfBuiltins::abs(FunctionContext* context, const DoubleVal& v) {
    if (v.is_null) {
        return v;
    }

    return DoubleVal(fabs(v.val));
}

DecimalV2Val UdfBuiltins::decimal_abs(FunctionContext* context, const DecimalV2Val& v) {
    if (v.is_null) {
        return v;
    }
    DecimalV2Val result = v;
    result.set_to_abs_value();
    return result;
}

//for test
BigIntVal UdfBuiltins::add_two_number(FunctionContext* context, const BigIntVal& v1,
                                      const BigIntVal& v2) {
    if (v1.is_null || v2.is_null) {
        return BigIntVal::null();
    }

    return BigIntVal(v1.val + v2.val);
}

//for test
StringVal UdfBuiltins::sub_string(FunctionContext* context, const StringVal& v1,
                                  const IntVal& begin, const IntVal& len) {
    if (v1.is_null || begin.is_null || len.is_null) {
        return StringVal::null();
    }

    int substring_len = (len.val > v1.len) ? v1.len : len.val;
    StringVal v = StringVal(context, substring_len);
    memcpy(v.ptr, v1.ptr + begin.val, substring_len);
    return v;
}

DoubleVal UdfBuiltins::pi(FunctionContext* context) {
    return DoubleVal(M_PI);
}

StringVal UdfBuiltins::lower(FunctionContext* context, const StringVal& v) {
    if (v.is_null) {
        return v;
    }

    StringVal result(context, v.len);

    for (int i = 0; i < v.len; ++i) {
        result.ptr[i] = tolower(v.ptr[i]);
    }

    return result;
}

} // namespace doris
