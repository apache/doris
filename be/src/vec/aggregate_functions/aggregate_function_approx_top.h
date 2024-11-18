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

#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

class AggregateFunctionApproxTop {
public:
    AggregateFunctionApproxTop(const std::vector<std::string>& column_names)
            : _column_names(column_names) {}

    static int32_t is_valid_const_columns(const std::vector<bool>& is_const_columns) {
        int32_t true_count = 0;
        bool found_false_after_true = false;
        for (int32_t i = is_const_columns.size() - 1; i >= 0; --i) {
            if (is_const_columns[i]) {
                true_count++;
                if (found_false_after_true) {
                    return false;
                }
            } else {
                if (true_count > 2) {
                    return false;
                }
                found_false_after_true = true;
            }
        }
        if (true_count > 2) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid is_const_columns configuration");
        }
        return true_count;
    }

protected:
    void lazy_init(const IColumn** columns, ssize_t row_num,
                   const DataTypes& argument_types) const {
        auto get_param = [](size_t idx, const DataTypes& data_types,
                            const IColumn** columns) -> uint64_t {
            const auto& data_type = data_types.at(idx);
            const IColumn* column = columns[idx];

            const auto* type = data_type.get();
            if (type->is_nullable()) {
                type = assert_cast<const DataTypeNullable*, TypeCheckOnRelease::DISABLE>(type)
                               ->get_nested_type()
                               .get();
            }
            int64_t value = 0;
            WhichDataType which(type);
            if (which.idx == TypeIndex::Int8) {
                value = assert_cast<const ColumnInt8*, TypeCheckOnRelease::DISABLE>(column)
                                ->get_element(0);
            } else if (which.idx == TypeIndex::Int16) {
                value = assert_cast<const ColumnInt16*, TypeCheckOnRelease::DISABLE>(column)
                                ->get_element(0);
            } else if (which.idx == TypeIndex::Int32) {
                value = assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(column)
                                ->get_element(0);
            }
            if (value <= 0) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "The parameter cannot be less than or equal to 0.");
            }
            return value;
        };

        _threshold =
                std::min(get_param(_column_names.size(), argument_types, columns), (uint64_t)4096);
        _reserved = std::min(
                std::max(get_param(_column_names.size() + 1, argument_types, columns), _threshold),
                (uint64_t)4096);

        if (_threshold == 0 || _reserved == 0 || _threshold > 4096 || _reserved > 4096) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "approx_top_sum param error, _threshold: {}, _reserved: {}", _threshold,
                            _reserved);
        }

        _init_flag = true;
    }

    static inline constexpr UInt64 TOP_K_MAX_SIZE = 0xFFFFFF;

    mutable std::vector<std::string> _column_names;
    mutable bool _init_flag = false;
    mutable uint64_t _threshold = 10;
    mutable uint64_t _reserved = 30;
};

} // namespace doris::vectorized