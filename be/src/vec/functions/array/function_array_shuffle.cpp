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
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class FunctionArrayShuffle : public IFunction {
public:
    static constexpr auto name = "array_shuffle";
    static FunctionPtr create() { return std::make_shared<FunctionArrayShuffle>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src_column_array = assert_cast<const ColumnArray&>(*src_column);

        uint32_t seed = time(nullptr);
        if (arguments.size() == 2) {
            ColumnPtr seed_column =
                    block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
            seed = assert_cast<const ColumnInt64*>(seed_column.get())->get_element(0);
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

} // namespace doris::vectorized