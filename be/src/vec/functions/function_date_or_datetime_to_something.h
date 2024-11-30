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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionDateOrDatetimeToSomething.h
// and modified by Doris

#pragma once

#include "vec/data_types/data_type_date.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

/// See DateTimeTransforms.h
template <typename ToDataType, typename Transform>
class FunctionDateOrDateTimeToSomething : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;

    static FunctionPtr create() { return std::make_shared<FunctionDateOrDateTimeToSomething>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) {
            return Transform::get_variadic_argument_types();
        }
        return {};
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        if (arguments.size() == 1) {
            if (!is_date_or_datetime(remove_nullable(arguments[0].type)) &&
                !is_date_v2_or_datetime_v2(remove_nullable(arguments[0].type))) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "Illegal type {} of argument of function {}. Should be a "
                                       "date or a date with time",
                                       arguments[0].type->get_name(), get_name());
            }
        } else if (arguments.size() == 2) {
            if (!is_date_or_datetime(remove_nullable(arguments[0].type)) &&
                !is_date_v2_or_datetime_v2(remove_nullable(arguments[0].type))) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "Illegal type {} of argument of function {}. Should be a "
                                       "date or a date with time",
                                       arguments[0].type->get_name(), get_name());
            }
            if (!is_string(remove_nullable(arguments[1].type))) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Function {} supports 1 or 2 arguments. The 1st argument must be of type "
                        "Date or DateTime. The 2nd argument (optional) must be a constant string "
                        "with timezone name",
                        get_name());
            }
            if (is_date(remove_nullable(arguments[0].type)) &&
                std::is_same_v<ToDataType, DataTypeDate>) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "The timezone argument of function {} is allowed only when the 1st "
                        "argument has the type DateTime",
                        get_name());
            }
        } else {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Number of arguments for function {} doesn't match: passed {}, "
                                   "should be 1 or 2",
                                   get_name(), arguments.size());
        }

        RETURN_REAL_TYPE_FOR_DATEV2_FUNCTION(ToDataType);
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return DateTimeTransformImpl<typename Transform::OpArgType, typename ToDataType::FieldType,
                                     Transform>::execute(block, arguments, result,
                                                         input_rows_count);
    }
};

} // namespace doris::vectorized
