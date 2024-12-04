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

#include <memory>
#include <variant>

#include "common/status.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_time.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/time_value.h"
#include "vec/utils/template_helpers.hpp"
namespace doris::vectorized {

template <typename ToDataType, typename Transform>
class FunctionTimeValueToField : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create() { return std::make_shared<FunctionTimeValueToField>(); }
    String get_name() const override { return name; }

    // is_variadic and get_number_of_arguments are consistent with FunctionDateOrDateTimeToSomething,
    // as FunctionDateOrDateTimeToSomething supports Date and DateTime types as arguments.
    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeTimeV2>()};
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<ToDataType>();
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 1);

        const auto* column_time = assert_cast<const TimeValue::ColumnTime*>(
                block.get_by_position(arguments[0]).column.get());

        auto col_res = ToDataType::ColumnType::create();

        col_res->resize(input_rows_count);
        auto& col_res_data = col_res->get_data();

        for (size_t i = 0; i < input_rows_count; i++) {
            col_res_data[i] = Transform::execute(column_time->get_element(i));
        }

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }
};

struct HourImpl {
    constexpr static auto name = "hour";
    static inline auto execute(const TimeValue::TimeType& t) { return TimeValue::hour(t); }
};

struct MintuImpl {
    constexpr static auto name = "minute";
    static inline auto execute(const TimeValue::TimeType& t) { return TimeValue::minute(t); }
};

struct SecondImpl {
    constexpr static auto name = "second";
    static inline auto execute(const TimeValue::TimeType& t) { return TimeValue::second(t); }
};

void register_function_time_value_field(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionTimeValueToField<DataTypeInt32, HourImpl>>();
    factory.register_function<FunctionTimeValueToField<DataTypeInt8, MintuImpl>>();
    factory.register_function<FunctionTimeValueToField<DataTypeInt8, SecondImpl>>();
}

} // namespace doris::vectorized