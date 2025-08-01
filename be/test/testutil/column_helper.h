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

#include <memory>
#include <type_traits>
#include <vector>

#include "runtime/define_primitive_type.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
struct ColumnHelper {
public:
    template <typename DataType>
    static ColumnPtr create_column(const std::vector<typename DataType::FieldType>& data) {
        auto column = DataType::ColumnType::create();
        if constexpr (std::is_same_v<DataTypeString, DataType>) {
            for (const auto& datum : data) {
                column->insert_data(datum.data(), datum.size());
            }
        } else {
            for (const auto& datum : data) {
                column->insert_value(datum);
            }
        }
        return std::move(column);
    }

    template <PrimitiveType Offset>
    static ColumnPtr create_column_offsets(
            const std::vector<typename PrimitiveTypeTraits<Offset>::CppType>& data) {
        auto column = ColumnVector<Offset>::create();
        for (const auto& datum : data) {
            column->insert_value(datum);
        }
        return std::move(column);
    }

    template <typename DataType>
    static ColumnPtr create_nullable_column(
            const std::vector<typename DataType::FieldType>& data,
            const std::vector<typename NullMap::value_type>& null_map) {
        auto null_col = ColumnUInt8::create();
        for (const auto& datum : null_map) {
            null_col->insert_value(datum);
        }
        auto ptr = create_column<DataType>(data);
        return ColumnNullable::create(std::move(ptr), std::move(null_col));
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

    static bool block_equal(const Block& block1, const Block& block2) {
        if (block1.columns() != block2.columns()) {
            return false;
        }
        for (size_t i = 0; i < block1.columns(); i++) {
            if (!column_equal(block1.get_by_position(i).column, block2.get_by_position(i).column)) {
                return false;
            }
        }
        return true;
    }

    static bool block_equal_with_sort(const Block& block1, const Block& block2) {
        if (block1.columns() != block2.columns()) {
            return false;
        }
        for (size_t i = 0; i < block1.columns(); i++) {
            auto column1 = block1.get_by_position(i).column;
            auto column2 = block2.get_by_position(i).column;
            auto type1 = block1.get_by_position(i).type;
            auto type2 = block2.get_by_position(i).type;

            std::vector<std::string> output_string1;
            std::vector<std::string> output_string2;

            for (size_t j = 0; j < column1->size(); j++) {
                output_string1.push_back(type1->to_string(*column1, j));
                output_string2.push_back(type2->to_string(*column2, j));
            }

            std::sort(output_string1.begin(), output_string1.end());
            std::sort(output_string2.begin(), output_string2.end());
            if (output_string1 != output_string2) {
                return false;
            }
        }
        return true;
    }

    template <typename DataType>
    static Block create_block(const std::vector<typename DataType::FieldType>& data) {
        auto column = create_column<DataType>(data);
        auto data_type = std::make_shared<DataType>();
        Block block({ColumnWithTypeAndName(column, data_type, "column")});
        return block;
    }

    template <typename DataType>
    static Block create_block(const std::vector<typename DataType::FieldType>& data1,
                              const std::vector<typename DataType::FieldType>& data2) {
        auto column1 = create_column<DataType>(data1);
        auto column2 = create_column<DataType>(data2);
        auto data_type = std::make_shared<DataType>();
        Block block({ColumnWithTypeAndName(column1, data_type, "column1"),
                     ColumnWithTypeAndName(column2, data_type, "column2")});
        return block;
    }

    template <typename DataType>
    static Block create_nullable_block(const std::vector<typename DataType::FieldType>& data,
                                       const std::vector<typename NullMap::value_type>& null_map) {
        auto column = create_nullable_column<DataType>(data, null_map);
        auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataType>());
        Block block({ColumnWithTypeAndName(column, data_type, "column")});
        return block;
    }

    template <typename DataType>
    static ColumnWithTypeAndName create_column_with_name(
            const std::vector<typename DataType::FieldType>& datas) {
        auto column = create_column<DataType>(datas);
        auto data_type = std::make_shared<DataType>();
        return ColumnWithTypeAndName(column, data_type, "column");
    }

    template <typename DataType>
    static ColumnWithTypeAndName create_nullable_column_with_name(
            const std::vector<typename DataType::FieldType>& datas,
            const std::vector<typename NullMap::value_type>& null_map) {
        auto column = create_nullable_column<DataType>(datas, null_map);
        auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataType>());
        return ColumnWithTypeAndName(column, data_type, "column");
    }
};
} // namespace doris::vectorized
