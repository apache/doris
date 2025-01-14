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

#pragma once

#include <gen_cpp/internal_service.pb.h>

#include "runtime/large_int_value.h"
#include "vec/common/string_ref.h"
#include "vec/core/wide_integer.h"

namespace doris {

template <typename T>
auto get_convertor() {
    if constexpr (std::is_same_v<T, bool>) {
        return [](PColumnValue* value, const T& data) { value->set_boolval(data); };
    } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                         std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t> ||
                         std::is_same_v<T, vectorized::Decimal32>) {
        return [](PColumnValue* value, const T& data) { value->set_intval(data); };
    } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, vectorized::Decimal64>) {
        return [](PColumnValue* value, const T& data) { value->set_longval(data); };
    } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
        return [](PColumnValue* value, const T& data) { value->set_doubleval(data); };
    } else if constexpr (std::is_same_v<T, int128_t> || std::is_same_v<T, uint128_t> ||
                         std::is_same_v<T, vectorized::Decimal128V3>) {
        return [](PColumnValue* value, const T& data) {
            value->set_stringval(LargeIntValue::to_string(data));
        };
    } else if constexpr (std::is_same_v<T, vectorized::Decimal256>) {
        return [](PColumnValue* value, const T& data) {
            value->set_stringval(wide::to_string(wide::Int256(data)));
        };
    } else if constexpr (std::is_same_v<T, std::string>) {
        return [](PColumnValue* value, const T& data) { value->set_stringval(data); };
    } else if constexpr (std::is_same_v<T, StringRef> ||
                         std::is_same_v<T, vectorized::Decimal128V2> ||
                         std::is_same_v<T, DecimalV2Value>) {
        return [](PColumnValue* value, const T& data) { value->set_stringval(data.to_string()); };
    } else if constexpr (std::is_same_v<T, VecDateTimeValue>) {
        return [](PColumnValue* value, const T& data) {
            char convert_buffer[30];
            data.to_string(convert_buffer);
            value->set_stringval(convert_buffer);
        };
    } else if constexpr (std::is_same_v<T, DateV2Value<DateV2ValueType>>) {
        return [](PColumnValue* value, const T& data) {
            value->set_intval(data.to_date_int_val());
        };
    } else if constexpr (std::is_same_v<T, DateV2Value<DateTimeV2ValueType>>) {
        return [](PColumnValue* value, const T& data) {
            value->set_longval(data.to_date_int_val());
        };
    } else {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "runtime filter data convertor meet invalid type {}", typeid(T).name());
        return [](PColumnValue* value, const T& data) {};
    }
}

} // namespace doris
