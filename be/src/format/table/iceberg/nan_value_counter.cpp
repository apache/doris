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

#include "format/table/iceberg/nan_value_counter.h"

#include <cmath>
#include <unordered_set>

#include "common/check.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "format/table/iceberg/types.h"

namespace doris::iceberg {

namespace {

// Branchless so the compiler can vectorize it: this runs over every floating value written.
template <typename Container>
int64_t count_nan_values(const Container& data, const NullMap* null_map) {
    int64_t nan_count = 0;
    const size_t rows = data.size();
    if (null_map == nullptr) {
        for (size_t i = 0; i < rows; ++i) {
            nan_count += static_cast<int64_t>(std::isnan(data[i]));
        }
        return nan_count;
    }
    for (size_t i = 0; i < rows; ++i) {
        nan_count += static_cast<int64_t>((*null_map)[i] == 0 && std::isnan(data[i]));
    }
    return nan_count;
}

} // namespace

NanValueCounter::NanValueCounter(const Schema& schema,
                                 const std::vector<int32_t>& requested_field_ids) {
    const std::unordered_set<int32_t> requested(requested_field_ids.begin(),
                                                requested_field_ids.end());
    const auto& columns = schema.columns();
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto type_id = columns[i].field_type()->type_id();
        if (type_id != TypeID::FLOAT && type_id != TypeID::DOUBLE) {
            continue;
        }
        const int32_t field_id = columns[i].field_id();
        if (requested.contains(field_id)) {
            _columns.emplace_back(i, field_id);
            _counts[field_id] = 0;
        }
    }
}

void NanValueCounter::count(const Block& block) {
    for (const auto& [column_position, field_id] : _columns) {
        const IColumn* column = block.get_by_position(column_position).column.get();
        const NullMap* null_map = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
            null_map = &nullable->get_null_map_data();
            column = &nullable->get_nested_column();
        }
        if (const auto* float64 = check_and_get_column<ColumnFloat64>(column)) {
            _counts[field_id] += count_nan_values(float64->get_data(), null_map);
            continue;
        }
        const auto* float32 = check_and_get_column<ColumnFloat32>(column);
        DORIS_CHECK(float32 != nullptr);
        _counts[field_id] += count_nan_values(float32->get_data(), null_map);
    }
}

} // namespace doris::iceberg
