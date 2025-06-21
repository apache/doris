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

#include "jsonb_utils.h"

#include <cstdint>

#include "common/status.h"
#include "util/date_func.h"
#include "util/string_parser.hpp"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
template <JsonbDecimalType T>
void JsonbToJson::decimal_to_json(const T& value, const uint32_t precision, const uint32_t scale) {
    auto value_str = value.to_string(precision, scale);
    os_.write(value_str.data(), value_str.size());
}

template void JsonbToJson::decimal_to_json<vectorized::Decimal32>(
        const vectorized::Decimal32& value, const uint32_t precision, const uint32_t scale);
template void JsonbToJson::decimal_to_json<vectorized::Decimal64>(
        const vectorized::Decimal64& value, const uint32_t precision, const uint32_t scale);
template void JsonbToJson::decimal_to_json<vectorized::Decimal128V3>(
        const vectorized::Decimal128V3& value, const uint32_t precision, const uint32_t scale);
template void JsonbToJson::decimal_to_json<vectorized::Decimal256>(
        const vectorized::Decimal256& value, const uint32_t precision, const uint32_t scale);
} // namespace doris