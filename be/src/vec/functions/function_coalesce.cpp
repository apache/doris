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

#include "udf/udf.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
class FunctionCoalesce : public IFunction {
public:
    static constexpr auto name = "coalesce";

    static FunctionPtr create() { return std::make_shared<FunctionCoalesce>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        for (const auto& arg : arguments) {
            if (!arg->is_nullable()) {
                return arg;
            }
        }
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK_GE(arguments.size(), 1);
        ColumnNumbers filtered_args;
        filtered_args.reserve(arguments.size());
        for (const auto& arg : arguments) {
            const auto& type = block.get_by_position(arg).type;
            if (type->only_null()) {
                continue;
            }
            filtered_args.push_back(arg);
            if (!type->is_nullable()) {
                break;
            }
        }

        size_t remaining_rows = input_rows_count;
        size_t argument_size = filtered_args.size();
        std::vector<int> record_idx(input_rows_count, -1); //used to save column idx
        MutableColumnPtr result_column;

        DataTypePtr type = block.get_by_position(result).type;
        if (!type->is_nullable()) {
            result_column = type->create_column();
        } else {
            result_column = remove_nullable(type)->create_column();
        }

        result_column->reserve(input_rows_count);
        auto return_type = std::make_shared<DataTypeUInt8>();
        auto null_map = ColumnUInt8::create(input_rows_count, 1);
        auto& null_map_data = null_map->get_data();
        ColumnPtr argument_columns[argument_size];

        for (size_t i = 0; i < argument_size; ++i) {
            block.get_by_position(filtered_args[i]).column =
                    block.get_by_position(filtered_args[i])
                            .column->convert_to_full_column_if_const();
            argument_columns[i] = block.get_by_position(filtered_args[i]).column;
            if (auto* nullable = check_and_get_column<const ColumnNullable>(*argument_columns[i])) {
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        for (size_t i = 0; i < argument_size && remaining_rows; ++i) {
            const ColumnsWithTypeAndName is_not_null_col {block.get_by_position(filtered_args[i])};
            Block temporary_block(is_not_null_col);
            temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
            auto func_is_not_null = SimpleFunctionFactory::instance().get_function("is_not_null_pred", is_not_null_col, return_type);
            func_is_not_null->execute(context, temporary_block, {0}, 1, input_rows_count);

            auto res_column = std::move(*temporary_block.get_by_position(1).column->convert_to_full_column_if_const()).mutate();
            auto& res_map = assert_cast<ColumnVector<UInt8>*>(res_column.get())->get_data();
            auto* res = res_map.data();

            //TODO: if need to imporve performance in the feature, here it's could SIMD
            for (size_t j = 0; j < input_rows_count && remaining_rows; ++j) {
                if (res[j] && null_map_data[j]) {
                    null_map_data[j] = 0;
                    remaining_rows--;
                    record_idx[j] = i;
                }
            }
        }
        //TODO: According to the record results, fill in result one by one, 
        //that's fill in result use different methods for different types,
        //because now the string types does not support random position writing,
        //But other type could, so could check one column, and fill the not null value in result column,
        //and then next column, this method may result in higher CPU cache
        for (int row = 0; row < input_rows_count; ++row) {
            if (record_idx[row] == -1) {
                result_column->insert_default();
            } else {
                result_column->insert_from(*argument_columns[record_idx[row]].get(), row);
            }
        }

        if (type->is_nullable()) {
            block.replace_by_position(result, ColumnNullable::create(std::move(result_column), std::move(null_map)));
        } else {
            block.replace_by_position(result, std::move(result_column));
        }

        return Status::OK();
    }
};

void register_function_coalesce(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionCoalesce>();
}

} // namespace doris::vectorized
