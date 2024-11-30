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

#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class FunctionArrayEnumerate : public IFunction {
public:
    static constexpr auto name = "array_enumerate";
    static FunctionPtr create() { return std::make_shared<FunctionArrayEnumerate>(); }
    String get_name() const override { return name; }
    bool use_default_implementation_for_nulls() const override { return true; }
    size_t get_number_of_arguments() const override { return 1; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const DataTypeArray* array_type =
                check_and_get_data_type<DataTypeArray>(remove_nullable(arguments[0]).get());
        if (!array_type) {
            throw doris::Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    "First argument for function {} .must be an array but it type is {}",
                    get_name(), arguments[0]->get_name());
        }

        auto nested_type = assert_cast<const DataTypeArray&>(*array_type).get_nested_type();
        bool is_nested_nullable = nested_type->is_nullable();
        bool is_nullable = arguments[0]->is_nullable();
        auto return_nested_type = std::make_shared<DataTypeInt64>();
        DataTypePtr return_type = std::make_shared<DataTypeArray>(
                is_nested_nullable ? make_nullable(return_nested_type) : return_nested_type);
        if (is_nullable) {
            return_type = make_nullable(return_type);
        }
        return return_type;
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnArray* array =
                check_and_get_column<ColumnArray>(remove_nullable(left_column->get_ptr()));
        if (!array) {
            return Status::RuntimeError(
                    fmt::format("Illegal column {}, of first argument of function {}",
                                left_column->get_name(), get_name()));
        }
        const ColumnArray::Offsets64& offsets = array->get_offsets();
        auto res_nested = ColumnInt64::create();
        ColumnInt64::Container& res_values = res_nested->get_data();
        res_values.resize(array->get_data().size());
        ColumnArray::Offset64 prev_off = 0;
        for (auto off : offsets) {
            for (ColumnArray::Offset64 j = prev_off; j < off; ++j) res_values[j] = j - prev_off + 1;
            prev_off = off;
        }

        ColumnPtr nested_column = res_nested->get_ptr();
        if (array->get_data().is_nullable()) {
            nested_column = ColumnNullable::create(nested_column,
                                                   ColumnUInt8::create(nested_column->size(), 0));
        }
        ColumnPtr res_column =
                ColumnArray::create(std::move(nested_column), array->get_offsets_ptr());
        if (block.get_by_position(arguments[0]).column->is_nullable()) {
            const ColumnNullable* nullable = check_and_get_column<ColumnNullable>(left_column);
            res_column = ColumnNullable::create(
                    res_column, nullable->get_null_map_column().clone_resized(nullable->size()));
        }
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

void register_function_array_enumerate(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayEnumerate>();
}

} // namespace doris::vectorized
