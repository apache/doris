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

#ifndef DORIS_BE_RUNTIME_TYPE_LIMIT_H
#define DORIS_BE_RUNTIME_TYPE_LIMIT_H

#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/string_value.h"

namespace doris {

template <typename T>
struct type_limit {
    static T min() { return std::numeric_limits<T>::lowest(); }
    static T max() { return std::numeric_limits<T>::max(); }
};

template <>
struct type_limit<StringValue> {
    static StringValue min() { return StringValue::min_string_val(); }
    static StringValue max() { return StringValue::max_string_val(); }
};

template <>
struct type_limit<DecimalV2Value> {
    static DecimalV2Value min() { return DecimalV2Value::get_min_decimal(); }
    static DecimalV2Value max() { return DecimalV2Value::get_max_decimal(); }
};

template <>
struct type_limit<DateTimeValue> {
    static DateTimeValue min() { return DateTimeValue::datetime_min_value(); }
    static DateTimeValue max() { return DateTimeValue::datetime_max_value(); }
};

} // namespace doris

#endif
