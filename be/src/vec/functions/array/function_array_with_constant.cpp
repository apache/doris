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

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/thread_context.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h" // IWYU pragma: keep
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/* array_with_constant(num, T) / array_repeat(T, num)  - return array of constants with length num.
 * array_with_constant(2, 'xxx') = ['xxx', 'xxx']
 * array_repeat('xxx', 2) = ['xxx', 'xxx']
 */
template <typename FunctionType>
class FunctionArrayWithConstant : public IFunction {
public:
    static constexpr auto name = FunctionType::name;
    static FunctionPtr create() { return std::make_shared<FunctionArrayWithConstant>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    // need handle null cases
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeArray>(
                make_nullable(arguments[FunctionType::param_val_idx]));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto num = block.get_by_position(arguments[FunctionType::param_num_idx])
                           .column->convert_to_full_column_if_const();
        num = num->is_nullable()
                      ? assert_cast<const ColumnNullable*>(num.get())->get_nested_column_ptr()
                      : num;
        auto value = block.get_by_position(arguments[FunctionType::param_val_idx])
                             .column->convert_to_full_column_if_const();
        auto offsets_col = ColumnVector<ColumnArray::Offset64>::create();
        ColumnArray::Offsets64& offsets = offsets_col->get_data();
        offsets.reserve(input_rows_count);
        ColumnArray::Offset64 offset = 0;
        std::vector<uint32_t> array_sizes;
        array_sizes.reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto array_size = num->get_int(i);
            if (UNLIKELY(array_size < 0)) {
                return Status::RuntimeError("Array size can not be negative in function:" +
                                            get_name());
            }
            offset += array_size;
            offsets.push_back(offset);
            array_sizes.resize(array_sizes.size() + array_size, i);
        }
        auto clone = value->clone_empty();
        clone->reserve(input_rows_count);
        RETURN_IF_CATCH_EXCEPTION(
                value->replicate(array_sizes.data(), offset, *clone->assume_mutable().get()));
        if (!clone->is_nullable()) {
            clone = ColumnNullable::create(std::move(clone), ColumnUInt8::create(clone->size(), 0));
        }
        auto array = ColumnArray::create(std::move(clone), std::move(offsets_col));
        block.replace_by_position(result, std::move(array));
        return Status::OK();
    }
};

struct NameArrayWithConstant {
    static constexpr auto name = "array_with_constant";

    static constexpr auto param_num_idx = 0;

    static constexpr auto param_val_idx = 1;
};

struct NameArrayRepeat {
    static constexpr auto name = "array_repeat";

    static constexpr auto param_num_idx = 1;

    static constexpr auto param_val_idx = 0;
};

void register_function_array_with_constant(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayWithConstant<NameArrayWithConstant>>();
    factory.register_function<FunctionArrayWithConstant<NameArrayRepeat>>();
}

} // namespace doris::vectorized
