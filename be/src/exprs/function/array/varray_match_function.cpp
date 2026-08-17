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

#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_array_view.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h" // IWYU pragma: keep
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

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
                        uint32_t result, size_t input_rows_count) const override {
        // here is executed by array_map filtered and arg[0] is bool result column
        auto array_view =
                ColumnArrayView<TYPE_BOOLEAN>::create(block.get_by_position(arguments[0]).column);

        // result is nullable bool column for every array column
        auto result_data_column = ColumnUInt8::create(input_rows_count, 1);
        auto result_null_column = ColumnUInt8::create(input_rows_count, 0);

        // iterate over all arrays with bool elements
        for (int row = 0; row < input_rows_count; ++row) {
            if (array_view.is_null_at(row)) {
                // current array is null, this is always null
                result_null_column->get_data()[row] = 1;
                result_data_column->get_data()[row] = 0;
            } else {
                // we should calculate the bool result for current array
                // has_null in current array
                bool has_null_elem = false;
                // res for current array
                bool res_for_array = MATCH_ALL;
                auto array_data = array_view[row];
                const auto* data = array_data.get_data();
                const auto* null_map = array_data.get_null_map_data();
                for (size_t pos = 0; pos < array_data.size(); ++pos) {
                    if (null_map[pos]) {
                        has_null_elem = true;
                    } else {
                        if (data[pos] != MATCH_ALL) { // not match
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
} // namespace doris
