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

#include "common/status.h"
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

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments.size() > 0)
                << "function: " << get_name() << ", arguments should not be empty";

        DataTypes res_data_types;
        size_t num_elements = arguments.size();
        for (size_t i = 0; i < num_elements; ++i) {
            DCHECK(is_array(arguments[i])) << i << "-th element is not array type";

            const auto* array_type = check_and_get_data_type<DataTypeArray>(arguments[i].get());
            DCHECK(array_type) << "function: " << get_name() << " " << i + 1
                               << "-th argument is not array";

            res_data_types.emplace_back(
                    make_nullable(remove_nullable((array_type->get_nested_type()))));
        }

        auto res = std::make_shared<DataTypeArray>(
                make_nullable(std::make_shared<DataTypeStruct>(res_data_types)));
        return res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        size_t num_element = arguments.size();

        // all the columns must have the same size as the first column
        ColumnPtr first_array_column;
        Columns tuple_columns(num_element);

        for (size_t i = 0; i < num_element; ++i) {
            auto col = block.get_by_position(arguments[i]).column;
            col = col->convert_to_full_column_if_const();

            const auto* column_array = check_and_get_column<ColumnArray>(col.get());
            if (!column_array) {
                return Status::RuntimeError(fmt::format(
                        "execute failed, function {}'s {}-th argument should be array bet get {}",
                        get_name(), i + 1, block.get_by_position(arguments[i]).type->get_name()));
            }

            if (i == 0) {
                first_array_column = col;
            } else if (!column_array->has_equal_offsets(
                               static_cast<const ColumnArray&>(*first_array_column))) {
                return Status::RuntimeError(
                        fmt::format("execute failed, function {}'s {}-th argument should have same "
                                    "offsets with first argument",
                                    get_name(), i + 1));
            }

            tuple_columns[i] = column_array->get_data_ptr();
        }

        auto tuples = ColumnStruct::create(tuple_columns);
        auto nullable_tuples =
                ColumnNullable::create(tuples, ColumnUInt8::create(tuples->size(), 0));
        auto res_column = ColumnArray::create(
                nullable_tuples,
                static_cast<const ColumnArray&>(*first_array_column).get_offsets_ptr());
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

void register_function_array_zip(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayZip>();
}

} // namespace doris::vectorized