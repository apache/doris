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

#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

template <typename Transform>
class FunctionDateTimeStringToString : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create() { return std::make_shared<FunctionDateTimeStringToString>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;

        const auto* nullable_column = check_and_get_column<ColumnNullable>(source_col.get());
        const auto* sources = check_and_get_column<ColumnVector<typename Transform::FromType>>(
                nullable_column ? nullable_column->get_nested_column_ptr().get()
                                : source_col.get());

        if (sources) {
            auto col_res = ColumnString::create();
            ColumnUInt8::MutablePtr col_null_map_to;
            col_null_map_to = ColumnUInt8::create(sources->size());
            auto& vec_null_map_to = col_null_map_to->get_data();

            if (arguments.size() == 2) {
                const IColumn& source_col1 = *block.get_by_position(arguments[1]).column;
                if (const auto* delta_const_column =
                            typeid_cast<const ColumnConst*>(&source_col1)) {
                    TransformerToStringTwoArgument<Transform>::vector_constant(
                            sources->get_data(), delta_const_column->get_field().get<String>(),
                            col_res->get_chars(), col_res->get_offsets(), vec_null_map_to);
                } else {
                    return Status::InternalError(
                            "Illegal column " +
                            block.get_by_position(arguments[1]).column->get_name() +
                            " is not const" + name);
                }
            } else {
                TransformerToStringTwoArgument<Transform>::vector_constant(
                        sources->get_data(), "yyyy-MM-dd HH:mm:ss", col_res->get_chars(),
                        col_res->get_offsets(), vec_null_map_to);
            }

            if (nullable_column) {
                const auto& origin_null_map = nullable_column->get_null_map_column().get_data();
                for (int i = 0; i < origin_null_map.size(); ++i) {
                    vec_null_map_to[i] |= origin_null_map[i];
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
        } else {
            return Status::InternalError("Illegal column " +
                                         block.get_by_position(arguments[0]).column->get_name() +
                                         " of first argument of function " + name);
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
