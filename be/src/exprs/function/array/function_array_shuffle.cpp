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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

class FunctionArrayShuffle : public IFunction {
public:
    static constexpr auto name = "array_shuffle";
    static FunctionPtr create() { return std::make_shared<FunctionArrayShuffle>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments[0]->get_primitive_type() == TYPE_ARRAY)
                << "first argument for function: " << name << " should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        return arguments[0];
    }

    // Shuffle a constant array on each row too, so every row gets its own order.
    bool use_default_implementation_for_constants() const override { return false; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        // All rows share one random sequence that starts from the seed, so a row cannot use
        // its own seed. Reject a non-constant seed instead of silently using the first one.
        if (scope == FunctionContext::THREAD_LOCAL && context->get_num_args() == 2 &&
            !context->is_col_constant(1)) {
            return Status::InvalidArgument("The seed of {} must be a constant", get_name());
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src_column_array = assert_cast<const ColumnArray&>(*src_column);

        uint32_t seed = 0;
        if (arguments.size() == 2) {
            // open() makes sure the seed is a constant, so every row has the same seed.
            // Use its low 32 bits, so any BIGINT works, a negative one too.
            const ColumnPtr& seed_column =
                    unpack_if_const(block.get_by_position(arguments[1]).column).first;
            seed = static_cast<uint32_t>(
                    assert_cast<const ColumnInt64&>(*seed_column).get_element(0));
        } else {
            // Give each block its own random seed, so blocks do not repeat the same orders.
            seed = std::random_device()();
        }

        std::mt19937 g(seed);
        auto dest_column_ptr = _execute(src_column_array, g);
        if (!dest_column_ptr) {
            return Status::RuntimeError(
                    fmt::format("execute failed or unsupported types for function {}({})",
                                get_name(), block.get_by_position(arguments[0]).type->get_name()));
        }

        block.replace_by_position(result, std::move(dest_column_ptr));
        return Status::OK();
    }

private:
    ColumnPtr _execute(const ColumnArray& src_column_array, std::mt19937& g) const {
        const auto& src_offsets = src_column_array.get_offsets();
        const auto src_nested_column = src_column_array.get_data_ptr();

        ColumnArray::Offset64 src_offsets_size = src_offsets.size();
        IColumn::Permutation permutation(src_nested_column->size());

        for (size_t i = 0; i < src_nested_column->size(); ++i) {
            permutation[i] = i;
        }

        for (size_t i = 0; i < src_offsets_size; ++i) {
            auto last_offset = src_offsets[i - 1];
            auto src_offset = src_offsets[i];

            std::shuffle(&permutation[last_offset], &permutation[src_offset], g);
        }
        return ColumnArray::create(src_nested_column->permute(permutation, 0),
                                   src_column_array.get_offsets_ptr());
    }
};

void register_function_array_shuffle(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayShuffle>();
    factory.register_alias("array_shuffle", "shuffle");
}

} // namespace doris