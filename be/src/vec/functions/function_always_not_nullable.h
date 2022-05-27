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

#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

template <typename Function>
class FunctionAlwaysNotNullable : public IFunction {
public:
    static constexpr auto name = Function::name;

    static FunctionPtr create() { return std::make_shared<FunctionAlwaysNotNullable>(); }

    String get_name() const override { return Function::name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<typename Function::ReturnType>();
    }

    bool use_default_implementation_for_constants() const override { return true; }
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto column = block.get_by_position(arguments[0]).column;

        MutableColumnPtr column_result = get_return_type_impl({})->create_column();
        column_result->resize(input_rows_count);

        if (const ColumnNullable* col_nullable =
                    check_and_get_column<ColumnNullable>(column.get())) {
            const ColumnString* col =
                    check_and_get_column<ColumnString>(col_nullable->get_nested_column_ptr().get());
            const ColumnUInt8* col_nullmap = check_and_get_column<ColumnUInt8>(
                    col_nullable->get_null_map_column_ptr().get());

            if (col != nullptr && col_nullmap != nullptr) {
                Function::vector_nullable(col->get_chars(), col->get_offsets(),
                                          col_nullmap->get_data(), column_result);

                block.replace_by_position(result, std::move(column_result));
                return Status::OK();
            }
        } else if (const ColumnString* col = check_and_get_column<ColumnString>(column.get())) {
            Function::vector(col->get_chars(), col->get_offsets(), column_result);

            block.replace_by_position(result, std::move(column_result));
            return Status::OK();
        } else {
            return Status::RuntimeError(fmt::format(
                    "Illegal column {} of argument of function {}",
                    block.get_by_position(arguments[0]).column->get_name(), get_name()));
        }

        block.replace_by_position(result, std::move(column_result));
        return Status::OK();
    }
};

} // namespace doris::vectorized
