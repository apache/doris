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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <fmt/format.h>

#include <map>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/types.h"
#include "core/value/decimalv2_value.h"

namespace doris {

class RuntimeState;

// State shared by recursive tablet validation of one output block.
struct TabletValidateContext {
    explicit TabletValidateContext(std::vector<char>& filter_map) : filter_map(filter_map) {}

    Status reject(size_t row, const fmt::memory_buffer& message) const;

    RuntimeState* state = nullptr;
    fmt::memory_buffer error_prefix;
    std::vector<char>& filter_map;

    std::map<std::pair<int, int>, DecimalV2Value> min_decimalv2_val;
    std::map<std::pair<int, int>, DecimalV2Value> max_decimalv2_val;
    std::map<int, int32_t> min_decimal32_val;
    std::map<int, int32_t> max_decimal32_val;
    std::map<int, int64_t> min_decimal64_val;
    std::map<int, int64_t> max_decimal64_val;
    std::map<int, int128_t> min_decimal128_val;
    std::map<int, int128_t> max_decimal128_val;
    std::map<int, wide::Int256> min_decimal256_val;
    std::map<int, wide::Int256> max_decimal256_val;
};

// Validate a full (non-const) column without modifying its input tree. Scalar
// checks reuse input storage; complex types build shells around validated children.
Result<ColumnPtr> tablet_validate_column(const ColumnPtr& column, const DataTypePtr& type,
                                         TabletValidateContext& context);

} // namespace doris
