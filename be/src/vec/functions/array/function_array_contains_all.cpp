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
        return std::make_shared<DataTypeUInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        // perpare the result column.
        auto result_col = ColumnUInt8::create(input_rows_count, 0);

        // fetch the input columns.
        const auto& left_input_col = block.get_by_position(arguments[0]).column;
        const auto& right_input_col = block.get_by_position(arguments[1]).column;

        // remove the constness of the input columns if necessary.
        const auto& [left_col_nullable, is_left_const] = unpack_if_const(left_input_col);
        const auto& [right_col_nullable, is_right_const] = unpack_if_const(right_input_col);

        // casts the columns in advance to avoid the repeated casting in the for loop.
        // we won't access the cells until we're sure that they're not null,
        // so it's safe to cast the columns in advance.
        const ColumnArray* left_col_array = check_and_get_column<ColumnArray>(left_col_nullable);
        const ColumnArray* right_col_array = check_and_get_column<ColumnArray>(right_col_nullable);

        for (size_t i = 0; i < input_rows_count; ++i) {
            // FIXME(niebayes): the null checking seems already done in the frontend.
            if (left_col_nullable->is_null_at(i) || right_col_nullable->is_null_at(i)) {
                continue;
            }

            // each array is a cell in a column array.
            // however, arrays in a column are flattened to reduce storage overhead.
            // therefore, we need to use offsets to delimit among arrays and
            // to locate the elements in an array.
            const ColumnNullable* left_nested_col_nullable =
                    check_and_get_column<ColumnNullable>(left_col_array->get_data());
            const ColumnNullable* right_nested_col_nullable =
                    check_and_get_column<ColumnNullable>(right_col_array->get_data());

            const ColumnArray::Offsets64& left_offsets = left_col_array->get_offsets();
            const ColumnArray::Offsets64& right_offsets = right_col_array->get_offsets();

            // construct arrays.
            const Array left_array = make_array(left_nested_col_nullable, left_offsets, i);
            const Array right_array = make_array(right_nested_col_nullable, right_offsets, i);

            // check if the left array contains all of the right elements.
            auto result_data = &result_col->get_data()[i];
            _check_left_contains_all_right(left_array, right_array, result_data);
        }

        // store the result column in the specified `result` column of the block.
        block.replace_by_position(result, std::move(result_col));

        return Status::OK();
    }

private:
    // the internal array type.
    using Offset = ColumnArray::Offset64;
    struct Array {
        const ColumnPtr& data;     // data[i] is the i-th element of the array.
        const NullMap& null_map;   // null_map[i] = true if data[i] is null.
        const Offset start_offset; // the offset of the first element in the array.
        const Offset end_offset;   // the offset of the last element in the array.

        Array(const ColumnPtr& data_, const NullMap& null_map_, const Offset start_offset_,
              const Offset end_offset_)
                : data {data_},
                  null_map {null_map_},
                  start_offset {start_offset_},
                  end_offset {end_offset_} {}
    };

    // construct an `Array` instance from a ColumnArray.
    static Array make_array(const ColumnNullable* col_nullable,
                            const ColumnArray::Offsets64& offsets, const size_t cur_row) {
        const ColumnPtr& data = col_nullable->get_nested_column_ptr();
        const NullMap& null_map = col_nullable->get_null_map_data();
        const Offset start_offset = cur_row == 0 ? 0 : offsets[cur_row - 1] + 1;
        const Offset end_offset = offsets[cur_row];

        return Array(data, null_map, start_offset, end_offset);
    }

    // the semantics of this function is to check if the left array contains all of the right elements.
    // it's important to note that the arrays are interpreted as sets, and hence the order of the elements
    // and the number of occurrences of each element are not taken into account.
    void _check_left_contains_all_right(const Array& left_array, const Array& right_array,
                                        UInt8* result) {
        static constexpr UInt8 CONTAINS_ALL = 1;
        static constexpr UInt8 NOT_CONTAINS_ALL = 0;

        // set the default result to NOT_CONTAINS_ALL.
        *result = NOT_CONTAINS_ALL;

        const bool left_has_nulls = !left_array.null_map.empty();
        const bool right_has_nulls = !right_array.null_map.empty();

        // the left array cannot contain all of the right elements if the right array has nulls while the left does not.
        if (right_has_nulls && !left_has_nulls) {
            return;
        }

        // for each element in the right array, check if it is contained in the left array.
        // if any element is not contained, then the left array does not contain all of the right elements.
        for (size_t j = right_array.start_offset; j <= right_array.end_offset; ++j) {
            // skip null elements in the right array.
            if (right_has_nulls && right_array.null_map[j]) {
                continue;
            }

            // true if the current element is contained in the left array.
            bool contained = false;
            for (size_t i = left_array.start_offset; i <= left_array.end_offset; ++i) {
                // skip null elements in the left array.
                if (left_has_nulls && left_array.null_map[i]) {
                    continue;
                }

                if (left_array.data->compare_at(i, j, *right_array.data, -1) == 0) {
                    contained = true;
                    break;
                }
            }
            if (!contained) {
                return;
            }
        }

        // all elements in the right array are contained in the left array.
        *result = CONTAINS_ALL;
    }
};

// register this function so it's regarded as a built-in function.
void register_function_array_contains_all(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayContainsAll>();
}

} // namespace doris::vectorized