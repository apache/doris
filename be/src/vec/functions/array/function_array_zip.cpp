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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayZip.cpp
// and modified by Doris

#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "util/simd/bits.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

// Combines multiple arrays into a single array
// array_zip(['d', 'o', 'r', 'i', 's'], [1, 2, 3, 4, 5]) -> [('d', 1), ('o', 2), ('r', 3), ('i', 4), ('s', 5)]
class FunctionArrayZip : public IFunction {
public:
    static constexpr auto name = "array_zip";
    static FunctionPtr create() { return std::make_shared<FunctionArrayZip>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(!arguments.empty())
                << "function: " << get_name() << ", arguments should not be empty";

        DataTypes res_data_types;
        size_t num_elements = arguments.size();
        bool has_nullable_type = false;
        for (size_t i = 0; i < num_elements; ++i) {
            auto remove_nullable_type = arguments[i];
            if (arguments[i]->is_nullable()) {
                has_nullable_type = true;
                remove_nullable_type = remove_nullable(arguments[i]);
            }

            DCHECK(is_array(remove_nullable_type)) << i << "-th element is not array type";

            const auto* array_type =
                    check_and_get_data_type<DataTypeArray>(remove_nullable_type.get());
            DCHECK(array_type) << "function: " << get_name() << " " << i + 1
                               << "-th argument is not array";

            res_data_types.emplace_back(
                    make_nullable(remove_nullable((array_type->get_nested_type()))));
        }

        auto res = std::make_shared<DataTypeArray>(
                make_nullable(std::make_shared<DataTypeStruct>(res_data_types)));
        return has_nullable_type ? make_nullable(res) : res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        size_t column_size = arguments.size();
        //res: Nullable(Array(Nullable(Struct(1:Nullable(String), 2:Nullable(String), 3:Nullable(Int8)))))
        auto result_type = block.get_by_position(result).type;
        auto res_null_map = ColumnUInt8::create(input_rows_count, 0); //outside
        auto res_array_null_map = ColumnUInt8::create();              //nested of nullable
        auto res_array_offset_column = ColumnArray::ColumnOffsets::create();
        // all the columns must have the same size as the first column
        ColumnPtr first_column;
        MutableColumns tuple_columns(column_size);
        ColumnPtr argument_columns[column_size];
        ColumnPtr nullmap_columns[column_size];

        for (size_t i = 0; i < column_size; ++i) {
            argument_columns[i] = block.get_by_position(arguments[i]).column;
            argument_columns[i] = argument_columns[i]->convert_to_full_column_if_const();
            if (const auto* nullable_column =
                        check_and_get_column<const ColumnNullable>(*argument_columns[i])) {
                VectorizedUtils::update_null_map(res_null_map->get_data(),
                                                 nullable_column->get_null_map_data());
                argument_columns[i] = nullable_column->get_nested_column_ptr();
                nullmap_columns[i] = nullable_column->get_null_map_column_ptr();
            }

            const auto* column_array = check_and_get_column<ColumnArray>(argument_columns[i].get());
            if (!column_array) {
                return Status::RuntimeError(fmt::format(
                        "execute failed, function {}'s {}-th argument should be array bet get {}",
                        get_name(), i + 1, block.get_by_position(arguments[i]).type->get_name()));
            }
            tuple_columns[i] = column_array->get_data_ptr()->clone_empty();
            if (i == 0) {
                first_column = argument_columns[0];
            }
        }

        auto& res_array_offset_column_data = res_array_offset_column->get_data();
        for (size_t row = 0; row < input_rows_count; ++row) {
            auto last_size = res_array_offset_column_data.back();
            if (res_null_map->get_data()[row]) { //eg: (['a', 'b'], NULL);
                for (size_t col = 0; col < column_size; ++col) {
                    tuple_columns[col]->insert_default();
                }
                res_array_offset_column_data.push_back(last_size + 1);
                res_array_null_map->get_data().push_back(1);
            } else {
                size_t current_column_row_length = 0;
                const auto& first_array_column = static_cast<const ColumnArray&>(*first_column);
                const auto& first_array_offset_data =
                        static_cast<const ColumnArray::ColumnOffsets&>(
                                first_array_column.get_offsets_column())
                                .get_data();
                auto first_offset_row_length = first_array_offset_data.data()[row] -
                                               first_array_offset_data.data()[row - 1];
                for (size_t col = 0; col < column_size; ++col) {
                    const auto& current_array_col =
                            static_cast<const ColumnArray&>(*argument_columns[col]);
                    const auto& current_array_offset_data =
                            static_cast<const ColumnArray::ColumnOffsets&>(
                                    current_array_col.get_offsets_column())
                                    .get_data();
                    current_column_row_length = current_array_offset_data.data()[row] -
                                                current_array_offset_data.data()[row - 1];
                    if (first_offset_row_length != current_column_row_length) {
                        return Status::RuntimeError(fmt::format(
                                "execute failed, function {}'s {}-th argument should have same "
                                "offsets with first argument on rows: {}. {} vs {}",
                                get_name(), col + 1, row, first_offset_row_length,
                                current_column_row_length));
                    }
                    auto nested_nullable_column = current_array_col.get_data_ptr();
                    tuple_columns[col]->insert_range_from(*nested_nullable_column,
                                                          current_array_offset_data.data()[row - 1],
                                                          current_column_row_length);
                }
                res_array_offset_column_data.push_back(last_size + current_column_row_length);
                res_array_null_map->insert_many_defaults(current_column_row_length);
            }
        }

        auto res_struct_column = ColumnStruct::create(std::move(tuple_columns));
        auto res_array_nullable =
                ColumnNullable::create(std::move(res_struct_column), std::move(res_array_null_map));
        auto res_column = ColumnArray::create(std::move(res_array_nullable),
                                              std::move(res_array_offset_column));
        if (result_type->is_nullable()) {
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(res_column), std::move(res_null_map)));
        } else {
            block.replace_by_position(result, std::move(res_column));
        }
        return Status::OK();
    }
};

void register_function_array_zip(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayZip>();
}

} // namespace doris::vectorized