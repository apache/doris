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

#include "common/compiler_util.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
// array_combinations([1, 2, 3],2) -> [[1,2], [1,3], [2,3]]
// array_combinations([1, NULL, 3, NULL, 5]，4) -> [[1,NULL,3,NULL], [1,NULL,3,5], [NULL,3,NULL,5]]

class FunctionArrayCombinations : public IFunction {
public:
    static constexpr auto name = "array_combinations";
    static FunctionPtr create() { return std::make_shared<FunctionArrayCombinations>(); }
    bool is_variadic() const override { return false; }
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const auto* array_type = assert_cast<const DataTypeArray*>(arguments[0].get());
        auto elem_t = make_nullable(array_type->get_nested_type());
        auto res = std::make_shared<DataTypeArray>(
                make_nullable(std::make_shared<DataTypeArray>(elem_t)));
        return res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto array = block.get_by_position(arguments[0]).column;
        ColumnPtr num = block.get_by_position(arguments[1]).column;
        Int64 combination_length = num->get_int(0);

        if (combination_length > MAX_COMBINATION_LENGTH || combination_length < 1) {
            return Status::InvalidArgument(
                    fmt::format("execute failed, function {}'s second argument must be bigger than "
                                "0 and not bigger than 5",
                                get_name()));
        }

        ColumnPtr res;
        const auto* src_arr = assert_cast<const ColumnArray*>(remove_nullable(array).get());
        const auto& offsets =
                assert_cast<const ColumnArray::ColumnOffsets&>(src_arr->get_offsets_column());
        Status status = vector_const(src_arr, input_rows_count, res, offsets, combination_length);
        if (!status.ok()) {
            return status;
        }
        block.replace_by_position(result, std::move(res));
        return status;
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

private:
    static const size_t MAX_COMBINATION_LENGTH = 5;
    static const size_t MAX_COMBINATION_COUNT = 100000;

    // Then combinationCount(n, k) = combinationCount(n-1, k-1) * n/k (https://en.wikipedia.org/wiki/Combination#Number_of_k-combinations)
    // The formula is recursive. Here, instead of starting with k=combinationCount, n=arrayLength and recursing,
    // we start with k=0 n=(arrayLength-combinationLength) and proceed "bottom up".
    size_t _combination_count(size_t array_length, size_t combination_length) const {
        size_t combinations = 1;

        for (size_t i = 1; i <= combination_length; i++) {
            combinations = combinations * (array_length - combination_length + i) / i;
        }

        return combinations;
    }

    ALWAYS_INLINE std::vector<size_t> _first_combination(Int64 combination_length,
                                                         size_t length) const {
        std::vector<size_t> comb(combination_length + 1);
        std::iota(comb.begin(), comb.begin() + combination_length, 0);
        comb[combination_length] = length;
        return comb;
    }

    // Generates the next k-combination in colex order.
    //
    // scan from the lowest index upward and increment the first
    // position that can be increased without breaking the strictly
    // increasing invariant. Resetting all lower positions to their
    // minimal values ensures the result is the smallest combination
    // greater than the current one.
    bool _next_combination(std::vector<size_t>& comb, Int64 combination_length) const {
        for (size_t i = 0; i < combination_length; ++i) {
            if (comb[i] + 1 < comb[i + 1]) {
                ++comb[i];
                std::iota(comb.begin(), comb.begin() + i, 0);
                return true;
            }
        }
        return false;
    }

    Status vector_const(const ColumnArray* nested_src_column_ptr, size_t input_rows_count,
                        ColumnPtr& res, const ColumnArray::ColumnOffsets& offsets,
                        Int64 combination_length) const {
        const auto& src_data = nested_src_column_ptr->get_data();
        const auto& src_offsets = offsets.get_data();

        auto inner_data = src_data.clone_empty();
        auto inner_offsets = ColumnArray::ColumnOffsets::create();
        auto inner_arr = ColumnArray::create(std::move(inner_data), std::move(inner_offsets));
        auto* inner = assert_cast<ColumnArray*>(inner_arr.get());

        auto outer_offsets = ColumnArray::ColumnOffsets::create();
        auto& outer_offsets_data = outer_offsets->get_data();
        outer_offsets_data.resize(input_rows_count);

        size_t prev_off = 0, outer_off = 0;

        for (size_t row = 0; row < input_rows_count; ++row) {
            size_t curr_off = src_offsets[row];
            size_t row_len = curr_off - prev_off;

            if (combination_length <= 0 || combination_length > row_len) {
                outer_offsets_data[row] = outer_off;
                prev_off = curr_off;
                continue;
            }

            size_t combination_count = _combination_count(row_len, combination_length);
            if (combination_count > MAX_COMBINATION_COUNT) {
                return Status::InvalidArgument(
                        fmt::format("execute failed, function {}'s total size of sub-groups "
                                    "generated must be smaller than 100,000",
                                    get_name()));
            }
            std::vector comb = _first_combination(combination_length, row_len);
            inner->get_data().reserve(inner->get_data().size() +
                                      combination_count * combination_length);
            inner->get_offsets().reserve(inner->get_offsets().size() + combination_count);
            outer_off += combination_count;
            do {
                for (int i = 0; i < combination_length; ++i) {
                    size_t idx = prev_off + comb[i];
                    inner->get_data().insert_from(src_data, idx);
                }
                inner->get_offsets().push_back(inner->get_data().size());
            } while (_next_combination(comb, combination_length));

            outer_offsets_data[row] = outer_off;
            prev_off = curr_off;
        }
        size_t inner_size = inner_arr->size();
        auto nullable_arr =
                ColumnNullable::create(std::move(inner_arr), ColumnUInt8::create(inner_size, 0));
        res = ColumnArray::create(std::move(nullable_arr), std::move(outer_offsets));

        return Status::OK();
    }
};

void register_function_array_combinations(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayCombinations>();
}
} // namespace doris