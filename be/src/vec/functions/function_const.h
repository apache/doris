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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionConst.h
// and modified by Doris

#pragma once

#include "vec/columns/column_const.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

template <typename Impl, bool use_field = true>
class FunctionConst : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionConst>(); }

public:
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return Impl::get_return_type();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        block.get_by_position(result).column =
                block.get_by_position(result).type->create_column_const(input_rows_count,
                                                                        Impl::init_value());
        return Status::OK();
    }
};

template <typename Impl>
class FunctionConst<Impl, false> : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionConst>(); }
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return Impl::get_return_type();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto column = Impl::ReturnColVec::create();
        column->get_data().emplace_back(Impl::init_value());
        block.replace_by_position(result, ColumnConst::create(std::move(column), input_rows_count));
        return Status::OK();
    }
};

template <typename Impl>
class FunctionMathConstFloat64 : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionMathConstFloat64>(); }

private:
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return std::make_shared<DataTypeFloat64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        block.get_by_position(result).column =
                block.get_by_position(result).type->create_column_const(
                        input_rows_count == 0 ? 1 : input_rows_count, Impl::value);
        return Status::OK();
    }
};

} // namespace doris::vectorized
