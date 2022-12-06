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

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionRunningDifference : public IFunction {
public:
    static constexpr auto name = "running_difference";

    static FunctionPtr create() { return std::make_shared<FunctionRunningDifference>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool use_default_implementation_for_constants() const override { return true; }

    template <typename SrcFieldType>
    using DstFieldType =
            typename NumberTraits::ResultOfSubtraction<SrcFieldType, SrcFieldType>::Type;
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        bool is_nullable = arguments[0]->is_nullable();
        auto nested_type = remove_nullable(arguments[0]);
        WhichDataType which(nested_type);
        //return type is promoted to prevent result overflow
        //like: input is int32 ---> return type will be int64
        DataTypePtr return_type = nullptr;
        if (which.is_uint8() || which.is_int8()) {
            return_type = std::make_shared<DataTypeInt16>();
        } else if (which.is_uint16() || which.is_int16()) {
            return_type = std::make_shared<DataTypeInt32>();
        } else if (which.is_uint32() || which.is_uint64() || which.is_int32()) {
            return_type = std::make_shared<DataTypeInt64>();
        } else if (which.is_int64() || which.is_int128()) {
            return_type = std::make_shared<DataTypeInt128>();
        } else if (which.is_float32() || which.is_float64()) {
            return_type = std::make_shared<DataTypeFloat64>();
        } else if (which.is_decimal()) {
            return_type = nested_type;
        } else if (which.is_date_time() || which.is_date_time_v2()) {
            return_type = std::make_shared<DataTypeFloat64>();
        } else if (which.is_date() || which.is_date_v2()) {
            return_type = std::make_shared<DataTypeInt32>();
        }

        return_type = is_nullable ? make_nullable(return_type) : return_type;
        const ColumnsWithTypeAndName subtract_cols {{nullptr, arguments[0], "first_arg"},
                                                    {nullptr, arguments[0], "second_arg"}};
        if (which.is_date_time() || which.is_date_time_v2()) {
            func_subtract = SimpleFunctionFactory::instance().get_function(
                    "timediff", subtract_cols, return_type);
        } else if (which.is_date() || which.is_date_v2()) {
            func_subtract = SimpleFunctionFactory::instance().get_function(
                    "datediff", subtract_cols, return_type);
        } else {
            func_subtract = SimpleFunctionFactory::instance().get_function(
                    "subtract", subtract_cols, return_type);
        }
        func_return_type = return_type;
        return return_type;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        if (is_column_const(*block.get_by_position(arguments[0]).column)) {
            auto res = func_return_type->create_column_const_with_default_value(input_rows_count);
            block.replace_by_position(result, std::move(res));
            return Status::OK();
        }

        auto arg_first = block.get_by_position(arguments[0]).column;
        auto arg_type = block.get_by_position(arguments[0]).type;
        auto arg_second = arg_type->create_column();

        if (is_first_block) {
            arg_second->insert_from(*arg_first, 0);
            is_first_block = false;
        } else {
            arg_second->insert_data(last_value.c_str(), last_value.length());
        }
        arg_second->insert_range_from(*arg_first, 0, input_rows_count - 1);
        last_value = arg_first->get_data_at(input_rows_count - 1).to_string();

        Block temporary_block {
                ColumnsWithTypeAndName {block.get_by_position(arguments[0]),
                                        {std::move(arg_second), arg_type, "second_arg"},
                                        block.get_by_position(result)}};

        func_subtract->execute(context, temporary_block, {0, 1}, 2, input_rows_count);
        block.get_by_position(result).column = temporary_block.get_by_position(2).column;
        return Status::OK();
    }

private:
    mutable FunctionBasePtr func_subtract;
    mutable DataTypePtr func_return_type;
    bool is_first_block = true;
    std::string last_value;
};

} // namespace doris::vectorized