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

#include "core/column/column_equal_flags.h"

#include <glog/logging.h>

#include <type_traits>

#include "common/compare.h"
#include "core/call_on_type_index.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/primitive_type.h"

namespace doris {

namespace {

// The types a key column can have.
constexpr uint32_t EQUAL_FLAGS_KEY_TYPES =
        DispatchDataTypeMask::INT | DispatchDataTypeMask::DECIMAL | DispatchDataTypeMask::DATETIME |
        DispatchDataTypeMask::IP | DispatchDataTypeMask::STRING;

// flags[row] &= (row `row` of `column` equals row `row` - 1) for the rows from 1 on, by the
// comparison of ColumnType::compare_at(). `null_map` is the null map of the nullable column that
// `column` is nested in, nullptr when there is none: two NULLs are equal and a NULL and a value
// are not, as in ColumnNullable::compare_at().
template <typename ColumnType>
void and_equal_to_previous_row(const ColumnType& column, const uint8_t* null_map, size_t rows,
                               uint8_t* flags) {
    auto equal = [&column](size_t row) -> uint8_t {
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            // The same size and bytes, as ColumnStr::compare_at() finds them equal.
            return column.get_data_at(row - 1) == column.get_data_at(row);
        } else {
            const auto& data = column.get_data();
            return Compare::compare(data[row - 1], data[row]) == 0;
        }
    };
    if (null_map == nullptr) {
        for (size_t row = 1; row < rows; ++row) {
            flags[row] &= equal(row);
        }
        return;
    }
    for (size_t row = 1; row < rows; ++row) {
        flags[row] &= (null_map[row - 1] == null_map[row]) & (null_map[row] | equal(row));
    }
}

// Run and_equal_to_previous_row() for `column` of `type` when it is a column of a key type;
// false when it is not.
bool and_equal_to_previous_row(const IColumn& column, PrimitiveType type, size_t rows,
                               uint8_t* flags) {
    const IColumn* values = &column;
    const uint8_t* null_map = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        values = &nullable->get_nested_column();
        null_map = nullable->get_null_map_data().data();
    }
    auto typed_loop = [&](auto dispatch) {
        using ColumnType =
                typename PrimitiveTypeTraits<std::decay_t<decltype(dispatch)>::PType>::ColumnType;
        const auto* typed = check_and_get_column<ColumnType>(*values);
        if (typed == nullptr) {
            return false;
        }
        and_equal_to_previous_row(*typed, null_map, rows, flags);
        return true;
    };
    return dispatch_type_base<decltype(typed_loop)&, EQUAL_FLAGS_KEY_TYPES>(type, typed_loop);
}

} // namespace

void build_equal_flags_for_column(const IColumn& column, PrimitiveType type, EqualFlags* flags) {
    const size_t rows = column.size();
    DCHECK_EQ(flags->size(), rows);
    uint8_t* data = flags->data();
    if (and_equal_to_previous_row(column, type, rows, data)) {
        return;
    }
    for (size_t row = 1; row < rows; ++row) {
        if (data[row] && column.compare_at(row, row - 1, column, -1) != 0) {
            data[row] = 0;
        }
    }
}

} // namespace doris
