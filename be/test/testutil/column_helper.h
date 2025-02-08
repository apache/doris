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

#include <type_traits>
#include <vector>

#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
struct ColumnHelper {
public:
    template <typename DataType>
    static ColumnPtr create_column(const std::vector<typename DataType::FieldType>& datas) {
        auto column = DataType::ColumnType::create();
        if constexpr (std::is_same_v<DataTypeString, DataType>) {
            for (const auto& data : datas) {
                column->insert_data(data.data(), data.size());
            }
        } else {
            for (const auto& data : datas) {
                column->insert_value(data);
            }
        }
        return std::move(column);
    }

    static bool column_equal(const ColumnPtr& column1, const ColumnPtr& column2) {
        if (column1->size() != column2->size()) {
            return false;
        }
        for (size_t i = 0; i < column1->size(); i++) {
            if (column1->compare_at(i, i, *column2, 1) != 0) {
                return false;
            }
        }
        return true;
    }
};
} // namespace doris::vectorized
