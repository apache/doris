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

#pragma once
#include <utility>

#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

template <typename ToDataType, typename Impl>
class FunctionVariadicArgumentsBase : public IFunction {
public:
    static constexpr auto name = Impl::name;
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionVariadicArgumentsBase>(); }
    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        DataTypePtr res;
        if constexpr (IsDataTypeDecimalV2<ToDataType>) {
            res = create_decimal(27, 9, true);
            if (!res) {
                LOG(FATAL) << "Someting wrong with toDecimalNNOrZero() or toDecimalNNOrNull()";
                __builtin_unreachable();
            }
        } else
            res = std::make_shared<ToDataType>();
        return res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ToDataType to_type;
        auto column = to_type.create_column();
        column->reserve(input_rows_count);

        if (arguments.empty()) {
            RETURN_IF_ERROR(Impl::empty_apply(column->assume_mutable_ref(), input_rows_count));
        } else {
            const ColumnWithTypeAndName& first_col = block.get_by_position(arguments[0]);
            RETURN_IF_ERROR(Impl::first_apply(first_col.type.get(), first_col.column.get(),
                                              input_rows_count, column->assume_mutable_ref()));

            for (size_t i = 1; i < arguments.size(); ++i) {
                const ColumnWithTypeAndName& col = block.get_by_position(arguments[i]);
                RETURN_IF_ERROR(Impl::combine_apply(col.type.get(), col.column.get(),
                                                    input_rows_count,
                                                    column->assume_mutable_ref()));
            }
        }

        block.get_by_position(result).column = std::move(column);
        return Status::OK();
    }
};
} // namespace doris::vectorized
