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

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

/* array_with_constant(num, T) - return array of constants with length num.
 * array_with_constant(2, 'xxx') = ['xxx', 'xxx']
 */
class FunctionArrayWithConstant : public IFunction {
public:
    static constexpr auto name = "array_with_constant";
    static FunctionPtr create() { return std::make_shared<FunctionArrayWithConstant>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    // need handle null cases
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeArray>(make_nullable(arguments[1]));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto num = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto value = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
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
            array_sizes.push_back(array_size);
        }
        auto clone = value->clone_empty();
        clone->reserve(input_rows_count);
        value->replicate(array_sizes.data(), offset, *clone->assume_mutable().get());
        if (!clone->is_nullable()) {
            clone = ColumnNullable::create(std::move(clone), ColumnUInt8::create(clone->size(), 0));
        }
        auto array = ColumnArray::create(std::move(clone), std::move(offsets_col));
        block.replace_by_position(result, std::move(array));
        return Status::OK();
    }
};

void register_function_array_with_constant(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayWithConstant>();
}

} // namespace doris::vectorized
