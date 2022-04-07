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
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

struct FunctionEsqueryImpl {
    static constexpr auto name = "esquery";
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DataTypeUInt8>();
    }
};

struct FunctionExplodeSplitImpl {
    static constexpr auto name = "explode_split";
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DataTypeString>();
    }
};

struct FunctionExplodeNumbersImpl {
    static constexpr auto name = "explode_numbers";
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DataTypeInt32>();
    }
};

struct FunctionExplodeJsonArrayIntImpl {
    static constexpr auto name = "explode_json_array_int";
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DataTypeInt64>();
    }
};

struct FunctionExplodeJsonArrayStringImpl {
    static constexpr auto name = "explode_json_array_string";
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DataTypeString>();
    }
};

struct FunctionExplodeJsonArrayDoubleImpl {
    static constexpr auto name = "explode_json_array_double";
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DataTypeFloat64>();
    }
};

struct FunctionExplodeBitmapImpl {
    static constexpr auto name = "explode_bitmap";
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DataTypeInt64>();
    }
};

//FunctionFake is use for some function call expr only work at prepare/open phase, do not support execute().
template <typename Impl>
class FunctionFake : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionFake>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        return Status::NotSupported(fmt::format("Fake function {} do not support execute", name));
    }
};

} // namespace doris::vectorized
