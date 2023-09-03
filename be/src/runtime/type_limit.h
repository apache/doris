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

#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "vec/common/string_ref.h"

namespace doris {

template <typename T>
struct type_limit {
    static T min() { return std::numeric_limits<T>::lowest(); }
    static T max() { return std::numeric_limits<T>::max(); }
};

template <>
struct type_limit<StringRef> {
    static StringRef min() { return StringRef::min_string_val(); }
    static StringRef max() { return StringRef::max_string_val(); }
};

template <>
struct type_limit<DecimalV2Value> {
    static DecimalV2Value min() { return DecimalV2Value::get_min_decimal(); }
    static DecimalV2Value max() { return DecimalV2Value::get_max_decimal(); }
};

template <>
struct type_limit<vectorized::Decimal32> {
    static vectorized::Decimal32 max() { return 999999999; }
    static vectorized::Decimal32 min() { return -max(); }
};

template <>
struct type_limit<vectorized::Decimal64> {
    static vectorized::Decimal64 max() { return int64_t(999999999999999999ll); }
    static vectorized::Decimal64 min() { return -max(); }
};

template <>
struct type_limit<vectorized::Decimal128I> {
    static vectorized::Decimal128I max() {
        return (static_cast<int128_t>(999999999999999999ll) * 100000000000000000ll * 1000ll +
                static_cast<int128_t>(99999999999999999ll) * 1000ll + 999ll);
    }
    static vectorized::Decimal128I min() { return -max(); }
};

template <>
struct type_limit<vectorized::Decimal128> {
    static vectorized::Decimal128 max() {
        return (static_cast<int128_t>(999999999999999999ll) * 100000000000000000ll * 1000ll +
                static_cast<int128_t>(99999999999999999ll) * 1000ll + 999ll);
    }
    static vectorized::Decimal128 min() { return -max(); }
};

template <>
struct type_limit<vectorized::VecDateTimeValue> {
    static vectorized::VecDateTimeValue min() {
        return vectorized::VecDateTimeValue::datetime_min_value();
    }
    static vectorized::VecDateTimeValue max() {
        return vectorized::VecDateTimeValue::datetime_max_value();
    }
};

template <>
struct type_limit<doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>> {
    static doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType> min() {
        uint32_t min = doris::vectorized::MIN_DATE_V2;
        return binary_cast<uint32_t,
                           doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>>(min);
    }
    static doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType> max() {
        uint32_t max = doris::vectorized::MAX_DATE_V2;
        return binary_cast<uint32_t,
                           doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>>(max);
    }
};

template <>
struct type_limit<doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>> {
    static doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType> min() {
        uint64_t min = doris::vectorized::MIN_DATETIME_V2;
        return binary_cast<uint64_t,
                           doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>>(
                min);
    }
    static doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType> max() {
        uint64_t max = doris::vectorized::MAX_DATETIME_V2;
        return binary_cast<uint64_t,
                           doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>>(
                max);
    }
};

} // namespace doris
