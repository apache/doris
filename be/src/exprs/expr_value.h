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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/expr-value.h
// and modified by Doris

#pragma once

#include "runtime/collection_value.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/string_value.h"
#include "runtime/types.h"

namespace doris {

// The materialized value returned by Expr::get_value().
// Some exprs may set multiple fields of this value at once
// for maintaining state across evaluations.
// For example, the rand() math function uses double_val as its return value,
// and int_val as the state for the random number generator.
struct ExprValue {
    bool bool_val;
    int8_t tinyint_val;
    int16_t smallint_val;
    int32_t int_val;
    int64_t bigint_val;
    __int128 large_int_val;
    float float_val;
    double double_val;
    std::string string_data;
    StringValue string_val;
    DateTimeValue datetime_val;
    doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType> datev2_val;
    doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType> datetimev2_val;
    DecimalV2Value decimalv2_val;
    CollectionValue array_val;

    ExprValue()
            : bool_val(false),
              tinyint_val(0),
              smallint_val(0),
              int_val(0),
              bigint_val(0),
              large_int_val(0),
              float_val(0.0),
              double_val(0.0),
              string_data(),
              string_val(),
              datetime_val(),
              decimalv2_val(0),
              array_val() {}

    ExprValue(bool v) : bool_val(v) {}
    ExprValue(int8_t v) : tinyint_val(v) {}
    ExprValue(int16_t v) : smallint_val(v) {}
    ExprValue(int32_t v) : int_val(v) {}
    ExprValue(int64_t v) : bigint_val(v) {}
    ExprValue(__int128 value) : large_int_val(value) {}
    ExprValue(float v) : float_val(v) {}
    ExprValue(double v) : double_val(v) {}
    ExprValue(int64_t i, int32_t f) : decimalv2_val(i, f) {}

    // c'tor for string values
    ExprValue(const std::string& str)
            : string_data(str),
              string_val(const_cast<char*>(string_data.data()), string_data.size()) {}

    // Set string value to copy of str
    void set_string_val(const StringValue& str) {
        string_data = std::string(str.ptr, str.len);
        sync_string_val();
    }

    void set_string_val(const std::string& str) {
        string_data = str;
        sync_string_val();
    }

    // Updates string_val ptr / len pair to reflect any changes in
    // string_data. If not called after mutating string_data,
    // string_val->ptr may point at garbage.
    void sync_string_val() {
        string_val.ptr = const_cast<char*>(string_data.data());
        string_val.len = string_data.size();
    }

    // Sets the value for type to '0' and returns a pointer to the data
    void* set_to_zero(const TypeDescriptor& type) {
        switch (type.type) {
        case TYPE_NULL:
            return nullptr;

        case TYPE_BOOLEAN:
            bool_val = false;
            return &bool_val;

        case TYPE_TINYINT:
            tinyint_val = 0;
            return &tinyint_val;

        case TYPE_SMALLINT:
            smallint_val = 0;
            return &smallint_val;

        case TYPE_INT:
            int_val = 0;
            return &int_val;

        case TYPE_BIGINT:
            bigint_val = 0;
            return &bigint_val;

        case TYPE_LARGEINT:
            large_int_val = 0;
            return &large_int_val;

        case TYPE_FLOAT:
            float_val = 0;
            return &float_val;

        case TYPE_DOUBLE:
            double_val = 0;
            return &double_val;

        case TYPE_DECIMALV2:
            decimalv2_val.set_to_zero();
            return &decimalv2_val;

        default:
            DCHECK(false);
            return nullptr;
        }
    }

    // Sets the value for type to min and returns a pointer to the data
    void* set_to_min(const TypeDescriptor& type) {
        switch (type.type) {
        case TYPE_NULL:
            return nullptr;

        case TYPE_BOOLEAN:
            bool_val = false;
            return &bool_val;

        case TYPE_TINYINT:
            tinyint_val = std::numeric_limits<int8_t>::min();
            return &tinyint_val;

        case TYPE_SMALLINT:
            smallint_val = std::numeric_limits<int16_t>::min();
            return &smallint_val;

        case TYPE_INT:
            int_val = std::numeric_limits<int32_t>::min();
            return &int_val;

        case TYPE_BIGINT:
            bigint_val = std::numeric_limits<int64_t>::min();
            return &bigint_val;

        case TYPE_LARGEINT:
            large_int_val = std::numeric_limits<int128_t>::min();
            return &large_int_val;

        case TYPE_FLOAT:
            float_val = std::numeric_limits<float>::lowest();
            return &float_val;

        case TYPE_DOUBLE:
            double_val = std::numeric_limits<double>::lowest();
            return &double_val;

        case TYPE_DECIMALV2:
            decimalv2_val = DecimalV2Value::get_min_decimal();
            return &decimalv2_val;

        default:
            DCHECK(false);
            return nullptr;
        }
    }

    // Sets the value for type to max and returns a pointer to the data
    void* set_to_max(const TypeDescriptor& type) {
        switch (type.type) {
        case TYPE_NULL:
            return nullptr;

        case TYPE_BOOLEAN:
            bool_val = true;
            return &bool_val;

        case TYPE_TINYINT:
            tinyint_val = std::numeric_limits<int8_t>::max();
            return &tinyint_val;

        case TYPE_SMALLINT:
            smallint_val = std::numeric_limits<int16_t>::max();
            return &smallint_val;

        case TYPE_INT:
            int_val = std::numeric_limits<int32_t>::max();
            return &int_val;

        case TYPE_BIGINT:
            bigint_val = std::numeric_limits<int64_t>::max();
            return &bigint_val;

        case TYPE_LARGEINT:
            large_int_val = std::numeric_limits<int128_t>::max();
            return &large_int_val;

        case TYPE_FLOAT:
            float_val = std::numeric_limits<float>::max();
            return &float_val;

        case TYPE_DOUBLE:
            double_val = std::numeric_limits<double>::max();
            return &double_val;

        case TYPE_DECIMALV2:
            decimalv2_val = DecimalV2Value::get_max_decimal();
            return &decimalv2_val;

        default:
            DCHECK(false);
            return nullptr;
        }
    }
};

} // namespace doris
