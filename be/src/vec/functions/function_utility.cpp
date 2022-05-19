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
#include <thread>

#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionSleep : public IFunction {
public:
    static constexpr auto name = "sleep";
    static FunctionPtr create() { return std::make_shared<FunctionSleep>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments[0].get()->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeUInt8>());
        }
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_constants() const override { return true; }
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        ColumnPtr argument_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();

        auto res_column = ColumnUInt8::create();

        if (auto* nullable_column = check_and_get_column<ColumnNullable>(*argument_column)) {
            auto null_map_column = ColumnUInt8::create();

            auto nested_column = nullable_column->get_nested_column_ptr();
            auto data_column = assert_cast<const ColumnVector<Int32>*>(nested_column.get());

            for (int i = 0; i < input_rows_count; i++) {
                if (nullable_column->is_null_at(i)) {
                    res_column->insert(0);
                    null_map_column->insert(1);
                } else {
                    int seconds = data_column->get_data()[i];
                    std::this_thread::sleep_for(std::chrono::seconds(seconds));
                    res_column->insert(1);
                    null_map_column->insert(0);
                }
            }

            block.replace_by_position(result, ColumnNullable::create(std::move(res_column),
                                                                     std::move(null_map_column)));
        } else {
            auto data_column = assert_cast<const ColumnVector<Int32>*>(argument_column.get());

            for (int i = 0; i < input_rows_count; i++) {
                int seconds = data_column->get_element(i);
                std::this_thread::sleep_for(std::chrono::seconds(seconds));
                res_column->insert(1);
            }

            block.replace_by_position(result, std::move(res_column));
        }
        return Status::OK();
    }
};

class FunctionVersion : public IFunction {
public:
    static constexpr auto name = "version";

    static const std::string version;

    static FunctionPtr create() { return std::make_shared<FunctionVersion>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto res_column = ColumnString::create();
        res_column->insert_data(version.c_str(), version.length());
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

const std::string FunctionVersion::version = "5.1.0";

void register_function_utility(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSleep>();
    factory.register_function<FunctionVersion>();
}

} // namespace doris::vectorized