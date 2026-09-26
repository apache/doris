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
#include <stdint.h>
#include <time.h>

#include <algorithm>
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src_column_array = assert_cast<const ColumnArray&>(*src_column);

        ColumnPtr dest_column_ptr;
        if (arguments.size() == 2) {
            const auto [seed_column, seed_const] =
                    unpack_if_const(block.get_by_position(arguments[1]).column);
            const auto& seeds = assert_cast<const ColumnInt64&>(*seed_column).get_data();
            // Each row re-seeds with its own seed, so the result of a row only
            // depends on its array and seed, not on the rows before it.
            std::mt19937_64 g;
            dest_column_ptr = _execute(src_column_array, [&](size_t row) -> std::mt19937_64& {
                // Use all 64 bits of the seed, so any BIGINT works, negative too.
                g.seed(static_cast<uint64_t>(seeds[index_check_const(row, seed_const)]));
                return g;
            });
        } else {
            std::mt19937_64 g(static_cast<uint64_t>(time(nullptr)));
            dest_column_ptr =
                    _execute(src_column_array, [&](size_t) -> std::mt19937_64& { return g; });
        }
        if (!dest_column_ptr) {
            return Status::RuntimeError(
                    fmt::format("execute failed or unsupported types for function {}({})",
                                get_name(), block.get_by_position(arguments[0]).type->get_name()));
        }

        block.replace_by_position(result, std::move(dest_column_ptr));
        return Status::OK();
    }

private:
    // get_generator(row) returns the random generator used to shuffle that row.
    template <typename GetGenerator>
    ColumnPtr _execute(const ColumnArray& src_column_array, GetGenerator&& get_generator) const {
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
            // An array with 0 or 1 element does not change. Skip it, so we also
            // skip seeding the generator for it.
            if (src_offset - last_offset < 2) {
                continue;
            }
            std::shuffle(&permutation[last_offset], &permutation[src_offset], get_generator(i));
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