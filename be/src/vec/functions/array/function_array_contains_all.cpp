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

    bool use_default_implementation_for_nulls() const override { return false; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        // construct arrays from the input columns.
        const Array left_array = from_input_column(block.get_by_position(arguments[0]).column);
        const Array right_array = from_input_column(block.get_by_position(arguments[1]).column);

        // construct a column to store the execution result.
        auto result_column = ColumnUInt8::create(input_rows_count);
        UInt8* result_data = result_column->get_data().data();

        // check if the left array contains all of the right elements.
        _execute_internal(left_array, right_array, result_data);

        // store the result column in the specified `result` column of the block.
        block.replace_by_position(result, std::move(result_column));

        return Status::OK();
    }

private:
    // the internal array type.
    struct Array {
        ColumnPtr data {nullptr};
        const NullMap& null_map;
        const size_t num_elements;

        Array(ColumnPtr data_, const NullMap& null_map_, const size_t num_elements_)
                : data {data_}, null_map {null_map_}, num_elements {num_elements_} {}
    };

    // construct an `Array` instance from the input column.
    static Array from_input_column(ColumnPtr column) {
        const auto& [nullable_column, _] = unpack_if_const(column);

        // applying NULL checking on the nullable columns is somewhat the canonical way.
        // however, the NULL checking is already performed in the frontend.
        // so we simply fetch the nested array column without any further checking.
        const ColumnArray* array_column = assert_cast<const ColumnArray*>(nullable_column.get());

        // fetch the data and the corresponding null map.
        const auto& nested_nullable_column =
                assert_cast<const ColumnNullable&>(array_column->get_data());
        ColumnPtr data = nested_nullable_column.get_nested_column_ptr();
        const NullMap& null_map = nested_nullable_column.get_null_map_data();

        // count the number of elements in the array.
        const auto& offsets = array_column->get_offsets();
        // FIXME(niebayes): the usage pattern of `offsets` is somewhat confusing.
        // maybe we can find another more elegant way to count the number of elements.
        const size_t num_elements = offsets[0] - offsets[-1];

        return Array(data, null_map, num_elements);
    }

    // the semantics of this function is to check if the left array contains all of the right elements.
    // it's important to note that the arrays are interpreted as sets, and hence the order of the elements
    // and the number of occurrences of each element are not taken into account.
    void _execute_internal(const Array& left_array, const Array& right_array, UInt8* result) {
        static constexpr UInt8 CONTAINS_ALL {1};
        static constexpr UInt8 NOT_CONTAINS_ALL {0};

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
        for (size_t j = 0; j < right_array.num_elements; ++j) {
            // skip null elements in the right array.
            if (right_has_nulls && right_array.null_map[j]) {
                continue;
            }

            // true if the current element is contained in the left array.
            bool contained {false};
            for (size_t i = 0; i < left_array.num_elements; ++i) {
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