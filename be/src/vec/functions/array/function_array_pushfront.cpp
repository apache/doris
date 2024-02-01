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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayIndex.h
// and modified by Doris

#include <stddef.h>

#include <memory>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class FunctionArrayPushfront : public IFunction {
public:
    static constexpr auto name = "array_pushfront";

    static FunctionPtr create() { return std::make_shared<FunctionArrayPushfront>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        // the type of arguments[0] could be Array(Nullable(xxx)) or Nullable(Array(Nullable(xxx))),
        // and we always return Nullable(Array(Nullable(xxx)))
        return std::make_shared<DataTypeNullable>(remove_nullable(arguments[0]));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        // extract src array column
        const ColumnArray* array_column = nullptr;
        const UInt8* array_null_map = nullptr;
        if (src_column->is_nullable()) {
            auto nullable_array = static_cast<const ColumnNullable*>(src_column.get());
            array_column = static_cast<const ColumnArray*>(&nullable_array->get_nested_column());
            array_null_map = nullable_array->get_null_map_column().get_data().data();
        } else {
            array_column = static_cast<const ColumnArray*>(src_column.get());
        }
        auto& src_nested_data_col = array_column->get_data();
        auto& src_offset_col = array_column->get_offsets();

        auto right_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        // prepare dst nullable array column
        auto result_col = block.get_by_position(result).type->create_column();
        auto result_nullable_col = static_cast<ColumnNullable*>(result_col.get());
        auto& result_null_map = result_nullable_col->get_null_map_data();
        auto result_array_col =
                static_cast<ColumnArray*>(result_nullable_col->get_nested_column_ptr().get());

        auto& result_nested_data_col = result_array_col->get_data();
        auto& result_offset_col = result_array_col->get_offsets();

        result_null_map.resize(input_rows_count);
        result_offset_col.resize(input_rows_count);
        result_nested_data_col.reserve(src_nested_data_col.size() + input_rows_count);

        size_t off = 0;
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (array_null_map && array_null_map[i]) {
                result_null_map[i] = 1;
                result_offset_col[i] = off;
                continue;
            }

            size_t length = src_offset_col[i] - src_offset_col[i - 1];
            result_nested_data_col.insert((*right_column)[i]);
            result_nested_data_col.insert_range_from(src_nested_data_col, src_offset_col[i - 1],
                                                     length);

            off += length + 1;
            result_null_map[i] = 0;
            result_offset_col[i] = off;
        }

        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }
};

void register_function_array_pushfront(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayPushfront>();
}

} // namespace doris::vectorized
