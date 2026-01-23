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

#include "common/logging.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
// array_combinations([1, 2, 3],2) -> [[1,2], [1,3], [2,3]]
// array_combinations([1, NULL, 3, NULL, 5]，4) -> [[1,NULL,3,NULL], [1,NULL,3,5], [NULL,3,NULL,5]]

class FunctionArrayCombinations : public IFunction {
public:
    static constexpr auto name = "array_combinations";
    static FunctionPtr create() { return std::make_shared<FunctionArrayCombinations>(); }
    bool is_variadic() const override { return false; }
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const auto* array_type = check_and_get_data_type<DataTypeArray>(arguments[0].get());
        auto elem_t = make_nullable(remove_nullable(array_type->get_nested_type()));
        auto res = std::make_shared<DataTypeArray>(
                make_nullable(std::make_shared<DataTypeArray>(elem_t)));
        return res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto left = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto* src_arr = assert_cast<ColumnArray*>(remove_nullable(left)->assume_mutable().get());

        ColumnPtr k_col =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        Int64 k = k_col->get_int(0);

        const auto& offsets =
                assert_cast<ColumnArray::ColumnOffsets&>(src_arr->get_offsets_column());

        ColumnPtr res = _execute_combination(src_arr, input_rows_count, offsets, k);
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

private:
    size_t _combination_count(size_t array_length, size_t k) const {
        size_t combinations = 1;
        for (int i = 1; i <= k; i++) {
            combinations = combinations * (array_length - k + i) / i;
        }
        return combinations;
    }

    std::vector<size_t> _first_combination(Int64 k, size_t length) const {
        std::vector<size_t> comb(k + 1);
        for (size_t i = 0; i < static_cast<size_t>(k); ++i) {
            comb[i] = i;
        }
        comb[k] = length;
        return comb;
    }

    bool _next_combination(std::vector<size_t>& comb, Int64 k) const {
        for (size_t i = 0; i < static_cast<size_t>(k); ++i) {
            if (comb[i] + 1 < comb[i + 1]) {
                ++comb[i];
                for (size_t j = 0; j < i; ++j) {
                    comb[j] = j;
                }
                return true;
            }
        }
        return false;
    }

    ColumnPtr _execute_combination(const ColumnArray* nested, size_t input_rows_count,
                                   const ColumnArray::ColumnOffsets& offsets, Int64 k) const {
        const auto& data_col = nested->get_data();
        const auto& in_offs = offsets.get_data();

        auto inner_data = data_col.clone_empty();
        auto inner_offsets = ColumnArray::ColumnOffsets::create();
        auto inner_arr = ColumnArray::create(std::move(inner_data), std::move(inner_offsets));
        auto* inner = assert_cast<ColumnArray*>(inner_arr.get());

        auto outer_offsets = ColumnArray::ColumnOffsets::create();
        auto& outer_offs = outer_offsets->get_data();
        outer_offs.resize(input_rows_count);

        Field element;
        size_t prev_off = 0, outer_off = 0;

        for (size_t row = 0; row < input_rows_count; ++row) {
            size_t curr_off = in_offs[row];
            size_t row_len = curr_off - prev_off;

            if (k <= 0 || static_cast<size_t>(k) > row_len) {
                outer_offs[row] = outer_off;
                prev_off = curr_off;
                continue;
            }

            std::vector comb = _first_combination(k, row_len);

            for (int i = 0; i < static_cast<size_t>(k); ++i) {
                size_t idx = prev_off + comb[i];
                data_col.get(idx, element);
                inner->get_data().insert(element);
            }
            inner->get_offsets().push_back(inner->get_data().size());

            while (_next_combination(comb, k)) {
                for (int i = 0; i < static_cast<size_t>(k); ++i) {
                    size_t idx = prev_off + comb[i];
                    data_col.get(idx, element);
                    inner->get_data().insert(element);
                }
                inner->get_offsets().push_back(inner->get_data().size());
            }

            outer_off += _combination_count(row_len, k);
            outer_offs[row] = outer_off;
            prev_off = curr_off;
        }
        auto nullable_arr = ColumnNullable::create(std::move(inner_arr),
                                                   ColumnUInt8::create(inner_arr->size(), 0));
        return ColumnArray::create(std::move(nullable_arr), std::move(outer_offsets));
    }
};

void register_function_array_combinations(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayCombinations>();
}
} // namespace doris::vectorized
