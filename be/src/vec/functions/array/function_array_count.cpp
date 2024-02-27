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

#include <vec/columns/column_array.h>
#include <vec/columns/column_nullable.h>
#include <vec/columns/columns_number.h>
#include <vec/data_types/data_type_array.h>
#include <vec/data_types/data_type_number.h>
#include <vec/functions/function.h>
#include <vec/functions/function_helpers.h>
#include <vec/functions/simple_function_factory.h>

namespace doris::vectorized {

// array_count([0, 1, 1, 1, 0, 0]) -> [3]
class FunctionArrayCount : public IFunction {
public:
    static constexpr auto name = "array_count";

    static FunctionPtr create() { return std::make_shared<FunctionArrayCount>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& [src_column, src_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const ColumnArray* array_column = nullptr;
        const UInt8* array_null_map = nullptr;
        if (src_column->is_nullable()) {
            auto nullable_array = assert_cast<const ColumnNullable*>(src_column.get());
            array_column = assert_cast<const ColumnArray*>(&nullable_array->get_nested_column());
            array_null_map = nullable_array->get_null_map_column().get_data().data();
        } else {
            array_column = assert_cast<const ColumnArray*>(src_column.get());
        }

        if (!array_column) {
            return Status::RuntimeError("unsupported types for function {}({})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name());
        }

        const auto& offsets = array_column->get_offsets();
        ColumnPtr nested_column = nullptr;
        const UInt8* nested_null_map = nullptr;
        if (array_column->get_data().is_nullable()) {
            const auto& nested_null_column =
                    assert_cast<const ColumnNullable&>(array_column->get_data());
            nested_null_map = nested_null_column.get_null_map_column().get_data().data();
            nested_column = nested_null_column.get_nested_column_ptr();
        } else {
            nested_column = array_column->get_data_ptr();
        }

        const auto& nested_data = assert_cast<const ColumnUInt8&>(*nested_column).get_data();

        auto dst_column = ColumnInt64::create(offsets.size());
        auto& dst_data = dst_column->get_data();

        for (size_t row = 0; row < offsets.size(); ++row) {
            Int64 res = 0;
            if (array_null_map && array_null_map[row]) {
                dst_data[row] = res;
                continue;
            }
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;
            for (size_t pos = 0; pos < len; ++pos) {
                if (nested_null_map && nested_null_map[pos + off]) {
                    continue;
                }
                if (nested_data[pos + off] != 0) {
                    ++res;
                }
            }
            dst_data[row] = res;
        }

        block.replace_by_position(result, std::move(dst_column));
        return Status::OK();
    }
};

void register_function_array_count(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayCount>();
}
} // namespace doris::vectorized
