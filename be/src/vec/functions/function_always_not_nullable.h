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

namespace doris::vectorized {

template <typename Function, bool WithReturn = false>
class FunctionAlwaysNotNullable : public IFunction {
public:
    static constexpr auto name = Function::name;

    static FunctionPtr create() { return std::make_shared<FunctionAlwaysNotNullable>(); }

    String get_name() const override { return Function::name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<typename Function::ReturnType>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    template <typename ColumnType, bool is_nullable>
    Status execute_internal(const ColumnPtr& column, const DataTypePtr& data_type,
                            MutableColumnPtr& column_result) const {
        auto type_error = [&]() {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        column->get_name(), get_name());
        };
        if constexpr (is_nullable) {
            const ColumnNullable* col_nullable = check_and_get_column<ColumnNullable>(column.get());
            const ColumnType* col =
                    check_and_get_column<ColumnType>(col_nullable->get_nested_column_ptr().get());
            const ColumnUInt8* col_nullmap = check_and_get_column<ColumnUInt8>(
                    col_nullable->get_null_map_column_ptr().get());

            if (col != nullptr && col_nullmap != nullptr) {
                if constexpr (WithReturn) {
                    RETURN_IF_ERROR(
                            Function::vector_nullable(col, col_nullmap->get_data(), column_result));
                } else {
                    Function::vector_nullable(col, col_nullmap->get_data(), column_result);
                }
            } else {
                return type_error();
            }
        } else {
            const ColumnType* col = check_and_get_column<ColumnType>(column.get());
            if constexpr (WithReturn) {
                RETURN_IF_ERROR(Function::vector(col, column_result));
            } else {
                Function::vector(col, column_result);
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnPtr& column = block.get_by_position(arguments[0]).column;
        const DataTypePtr& data_type = block.get_by_position(arguments[0]).type;
        WhichDataType which(data_type);

        MutableColumnPtr column_result = get_return_type_impl({})->create_column();
        column_result->resize(input_rows_count);

        auto type_error = [&]() {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        };
        Status status = Status::OK();
        if (which.is_nullable()) {
            const DataTypePtr& nested_data_type =
                    static_cast<const DataTypeNullable*>(data_type.get())->get_nested_type();
            WhichDataType nested_which(nested_data_type);
            if (nested_which.is_string_or_fixed_string()) {
                status = execute_internal<ColumnString, true>(column, data_type, column_result);
            } else if (nested_which.is_int64()) {
                status = execute_internal<ColumnInt64, true>(column, data_type, column_result);
            } else {
                return type_error();
            }
        } else if (which.is_string_or_fixed_string()) {
            status = execute_internal<ColumnString, false>(column, data_type, column_result);
        } else if (which.is_int64()) {
            status = execute_internal<ColumnInt64, false>(column, data_type, column_result);
        } else {
            return type_error();
        }
        if (status.ok()) {
            block.replace_by_position(result, std::move(column_result));
        }
        return status;
    }
};

} // namespace doris::vectorized
