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

#include "storage/iterator/same_key_flags.h"

#include <glog/logging.h>

#include <type_traits>

#include "common/compare.h"
#include "core/block/block.h"
#include "core/call_on_type_index.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/primitive_type.h"

namespace doris {

namespace {

// The types a key column can have.
constexpr uint32_t KEY_TYPES = DispatchDataTypeMask::INT | DispatchDataTypeMask::DECIMAL |
                               DispatchDataTypeMask::DATETIME | DispatchDataTypeMask::IP |
                               DispatchDataTypeMask::STRING;

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
    return dispatch_type_base<decltype(typed_loop)&, KEY_TYPES>(type, typed_loop);
}

} // namespace

void SameKeyFlags::compute(const Block& block, std::vector<uint8_t>* flags) {
    const size_t rows = block.rows();
    DCHECK_GT(rows, 0);
    flags->assign(rows, 1);
    uint8_t* data = flags->data();
    data[0] = !_last_key.empty();
    for (size_t i = 0; i < _key_ordinals.size(); ++i) {
        const ColumnWithTypeAndName& key = block.get_by_position(_key_ordinals[i]);
        const IColumn& column = *key.column;
        if (data[0] && column.compare_at(0, 0, *_last_key[i], -1) != 0) {
            data[0] = 0;
        }
        if (and_equal_to_previous_row(column, remove_nullable(key.type)->get_primitive_type(), rows,
                                      data)) {
            continue;
        }
        for (size_t row = 1; row < rows; ++row) {
            if (data[row] && column.compare_at(row, row - 1, column, -1) != 0) {
                data[row] = 0;
            }
        }
    }
    if (_last_key.empty()) {
        for (uint32_t ordinal : _key_ordinals) {
            _last_key.push_back(block.get_by_position(ordinal).column->clone_empty());
        }
    }
    for (size_t i = 0; i < _key_ordinals.size(); ++i) {
        _last_key[i]->clear();
        _last_key[i]->insert_from(*block.get_by_position(_key_ordinals[i]).column, rows - 1);
    }
}

} // namespace doris
