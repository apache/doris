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

#include <vec/functions/simple_function_factory.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

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
#include "vec/utils/util.hpp"

namespace doris::vectorized {

///* bool array_match_all/any(array<boolean>) *///
template <bool MATCH_ALL>
class ArrayMatchFunction : public IFunction {
public:
    static constexpr auto name = MATCH_ALL ? "array_match_all" : "array_match_any";
    static FunctionPtr create() { return std::make_shared<ArrayMatchFunction>(); }

    std::string get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    bool is_use_default_implementation_for_constants() const override { return false; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeUInt8>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        // here is executed by array_map filtered and arg[0] is bool result column
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

        if (!nested_column) {
            return Status::RuntimeError("unsupported types for function {}({})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name());
        }

        const auto& nested_data = assert_cast<const ColumnUInt8&>(*nested_column).get_data();

        // result is nullable bool column for every array column
        auto result_data_column = ColumnUInt8::create(input_rows_count, 1);
        auto result_null_column = ColumnUInt8::create(input_rows_count, 0);

        // iterate over all arrays with bool elements
        for (int row = 0; row < input_rows_count; ++row) {
            if (array_null_map && array_null_map[row]) {
                // current array is null, this is always null
                result_null_column->get_data()[row] = 1;
                result_data_column->get_data()[row] = 0;
            } else {
                // we should calculate the bool result for current array
                // has_null in current array
                bool has_null_elem = false;
                // res for current array
                bool res_for_array = MATCH_ALL;
                for (auto off = offsets[row - 1]; off < offsets[row]; ++off) {
                    if (nested_null_map && nested_null_map[off]) {
                        has_null_elem = true;
                    } else {
                        if (nested_data[off] != MATCH_ALL) { // not match
                            res_for_array = !MATCH_ALL;
                            break;
                        } // default is MATCH_ALL
                    }
                }
                result_null_column->get_data()[row] = has_null_elem && res_for_array == MATCH_ALL;
                result_data_column->get_data()[row] = res_for_array;
            }
        }

        // insert the result column to block
        DCHECK(block.get_by_position(result).type->is_nullable());
        ColumnPtr dst_column = ColumnNullable::create(std::move(result_data_column),
                                                      std::move(result_null_column));
        block.replace_by_position(result, std::move(dst_column));
        return Status::OK();
    }
};

void register_function_array_match(SimpleFunctionFactory& factory) {
    factory.register_function<ArrayMatchFunction<true>>(); // MATCH_ALL = true means array_match_all
    factory.register_function<
            ArrayMatchFunction<false>>(); // MATCH_ALL = false means array_match_any
}
} // namespace doris::vectorized
