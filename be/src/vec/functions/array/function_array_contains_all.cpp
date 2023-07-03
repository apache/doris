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

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionArrayContainsAll : public IFunction {
public:
    static constexpr auto name {"array_contains_all"};

    static FunctionPtr create() { return std::make_shared<FunctionArrayContainsAll>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeUInt8>());
    }

    // the semantics of this function is to check if the left array contains all of the right elements.
    // it's important to note that the arrays are interpreted as sets, and hence the order of the elements
    // and the number of occurrences of each element are not taken into account.
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto result_col = ColumnUInt8::create(input_rows_count, 0);
        auto result_null_map = ColumnUInt8::create(input_rows_count, 0);

        const auto& left_input_col = block.get_by_position(arguments[0]).column;
        const auto& right_input_col = block.get_by_position(arguments[1]).column;

        // since the input maybe literal, we have to remove constness accordingly.
        // since the input maybe null, we make it nullable to unify the processing.
        const auto left_col = make_nullable(unpack_if_const(left_input_col).first);
        const auto right_col = make_nullable(unpack_if_const(right_input_col).first);

        const ColumnNullable* left_col_nullable = check_and_get_column<ColumnNullable>(left_col);
        const ColumnNullable* right_col_nullable = check_and_get_column<ColumnNullable>(right_col);

        const ColumnArray* left_col_array =
                check_and_get_column<ColumnArray>(left_col_nullable->get_nested_column());
        const ColumnArray* right_col_array =
                check_and_get_column<ColumnArray>(right_col_nullable->get_nested_column());

        // data columns are single-dimension columns which stores elements of all arrays.
        const ColumnNullable* left_data_col_nullable =
                check_and_get_column<ColumnNullable>(left_col_array->get_data());
        const ColumnNullable* right_data_col_nullable =
                check_and_get_column<ColumnNullable>(right_col_array->get_data());

        for (size_t row = 0; row < input_rows_count; ++row) {
            if (left_col_nullable->is_null_at(row) || right_col_nullable->is_null_at(row)) {
                result_null_map->get_data()[row] = 1;
                continue;
            }

            const auto& left_offsets = _get_offsets_of_row(left_col_array->get_offsets(), row);
            const auto& right_offsets = _get_offsets_of_row(right_col_array->get_offsets(), row);

            const bool left_has_nulls = _has_nulls(left_data_col_nullable, left_offsets);
            const bool right_has_nulls = _has_nulls(right_data_col_nullable, right_offsets);

            if (right_has_nulls && !left_has_nulls) {
                continue;
            }

            // for each element in the right array, check if it is contained in the left array.
            // if any element is not contained, then the left array does not contain all of the right elements.
            bool contains_all = true;

            for (size_t ri = right_offsets.first; ri <= right_offsets.second; ++ri) {
                // skip null elements in the right array.
                if (right_data_col_nullable->is_null_at(ri)) {
                    continue;
                }

                // true if the left array contains this element.
                bool contained = false;

                for (size_t li = left_offsets.first; li <= left_offsets.second; ++li) {
                    // skip null elements in the left array.
                    if (left_data_col_nullable->is_null_at(li)) {
                        continue;
                    }

                    // ColumnNullable::compare_at will invoke the `compare_at` of the nested column.
                    if (left_data_col_nullable->compare_at(li, ri, *right_data_col_nullable, -1) ==
                        0) {
                        contained = true;
                        break;
                    }
                }

                if (!contained) {
                    contains_all = false;
                    break;
                }
            }

            if (contains_all) {
                result_col->get_data()[row] = 1;
            }
        }

        auto result_col_nullable =
                ColumnNullable::create(std::move(result_col), std::move(result_null_map));
        block.replace_by_position(result, std::move(result_col_nullable));

        return Status::OK();
    }

private:
    // get the start and end offsets of the array at the given row.
    std::pair<size_t, size_t> _get_offsets_of_row(const ColumnArray::Offsets64& offsets,
                                                  const size_t row) {
        const size_t start_offset = row == 0 ? 0 : offsets[row - 1] + 1;
        const size_t end_offset = offsets[row];
        return {start_offset, end_offset};
    }

    bool _has_nulls(const ColumnNullable* col_nullable, const std::pair<size_t, size_t>& offsets) {
        for (size_t i = offsets.first; i <= offsets.second; ++i) {
            if (col_nullable->is_null_at(i)) {
                return true;
            }
        }
        return false;
    }
};

// register this function so it's regarded as a built-in function.
void register_function_array_contains_all(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayContainsAll>();
}

} // namespace doris::vectorized