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

#include "common/exception.h"
#include "common/status.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_varbinary.h"
#include "vec/exprs/function_context.h"
#include "vec/functions/function.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <int NumArgs>
class FunctionLpadSparkString : public IFunction {
    static_assert(NumArgs == 2 || NumArgs == 3);

public:
    static constexpr auto name = "lpad_spark";
    static FunctionPtr create() { return std::make_shared<FunctionLpadSparkString>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return static_cast<size_t>(NumArgs); }
    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (NumArgs == 2) {
            return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()};
        }
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>(),
                std::make_shared<DataTypeString>()};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != static_cast<size_t>(NumArgs)) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Function {} requires {} arguments",
                                   get_name(), NumArgs);
        }
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override;
};

template <int NumArgs>
class FunctionLpadSparkVarbinary : public IFunction {
    static_assert(NumArgs == 2 || NumArgs == 3);

public:
    static constexpr auto name = "lpad_spark";
    static FunctionPtr create() { return std::make_shared<FunctionLpadSparkVarbinary>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return static_cast<size_t>(NumArgs); }
    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (NumArgs == 2) {
            return {std::make_shared<DataTypeVarbinary>(), std::make_shared<DataTypeInt32>()};
        }
        return {std::make_shared<DataTypeVarbinary>(), std::make_shared<DataTypeInt32>(),
                std::make_shared<DataTypeVarbinary>()};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != static_cast<size_t>(NumArgs)) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Function {} requires {} arguments",
                                   get_name(), NumArgs);
        }
        return make_nullable(std::make_shared<DataTypeVarbinary>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
