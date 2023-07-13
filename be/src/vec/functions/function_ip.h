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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsCodingIP.cpp
// and modified by Doris

#pragma once
#include <glog/logging.h>

#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/format_ip.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct NameFunctionIPv4StringToNum;
struct NameFunctionIPv4StringToNumOrDefault;

/** If mask_tail_octets > 0, the last specified number of octets will be filled with "xxx".
  */
template <size_t mask_tail_octets, typename Name>
class FunctionIPv4NumToString : public IFunction {
private:
    template <typename ArgType>
    Status execute_type(Block& block, const ColumnWithTypeAndName& argument, size_t result) const {
        using ColumnType = ColumnVector<ArgType>;
        const ColumnPtr& column = argument.column;

        if (const ColumnType* col_src = typeid_cast<const ColumnType*>(column.get())) {
            const typename ColumnType::Container& vec_in = col_src->get_data();
            auto col_res = ColumnString::create();
            auto null_map = ColumnUInt8::create(vec_in.size(), 0);

            for (size_t i = 0; i < vec_in.size(); ++i) {
                auto value = vec_in[i];
                if (value < IPV4_MIN_NUM_VALUE || value > IPV4_MAX_NUM_VALUE) {
                    col_res->insert_default();
                    null_map->get_data()[i] = 1;
                } else {
                    vectorized::IPv4 ipv4_val = static_cast<vectorized::IPv4>(value);
                    std::string ipv4_str = IPv4Value::to_string(ipv4_val);
                    col_res->insert_data(ipv4_str.c_str(), ipv4_str.size());
                }
            }

            DCHECK_EQ(col_res->size(), col_src->size());
            DCHECK_EQ(col_res->size(), null_map->size());

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(null_map)));
            return Status::OK();
        } else
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        argument.column->get_name(), get_name());
    }

public:
    static constexpr auto name = "ipv4numtostring";
    static FunctionPtr create() {
        return std::make_shared<FunctionIPv4NumToString<mask_tail_octets, Name>>();
    }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        ColumnWithTypeAndName& argument = block.get_by_position(arguments[0]);

        switch (argument.type->get_type_id()) {
        case TypeIndex::Int8:
            return execute_type<Int8>(block, argument, result);
        case TypeIndex::Int16:
            return execute_type<Int16>(block, argument, result);
        case TypeIndex::Int32:
            return execute_type<Int32>(block, argument, result);
        case TypeIndex::Int64:
            return execute_type<Int64>(block, argument, result);
        default:
            break;
        }

        return Status::RuntimeError(
                "Illegal column {} of argument of function {}, expected Int8 or Int16 or Int32 or "
                "Int64",
                argument.name, get_name());
    }
};

template <typename Name>
class FunctionIPv4StringToNum : public IFunction {
private:
    Status execute_type(Block& block, const ColumnWithTypeAndName& argument, size_t result) const {
        const ColumnPtr& column = argument.column;

        if (const ColumnString* col_src = typeid_cast<const ColumnString*>(column.get())) {
            auto col_res = ColumnInt64::create();
            auto null_map = ColumnUInt8::create(col_src->size(), 0);

            for (size_t i = 0; i < col_src->size(); ++i) {
                auto ipv4_str = col_src->get_data_at(i).to_string();
                vectorized::IPv4 ipv4_val;
                if (!IPv4Value::from_string(ipv4_val, ipv4_str) || ipv4_str.size() > IPV4_MAX_TEXT_LENGTH) {
                    null_map->get_data()[i] = 1;
                    col_res->insert_default();
                } else {
                    col_res->insert_value(ipv4_val);
                }
            }

            DCHECK_EQ(col_res->size(), col_src->size());
            DCHECK_EQ(col_res->size(), null_map->size());

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(null_map)));
            return Status::OK();
        } else
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        argument.column->get_name(), get_name());
    }

public:
    static constexpr auto name = "ipv4stringtonum";
    static FunctionPtr create() {
        return std::make_shared<FunctionIPv4StringToNum<NameFunctionIPv4StringToNum>>();
    }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeInt64>());
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        ColumnWithTypeAndName& argument = block.get_by_position(arguments[0]);

        DCHECK(argument.type->get_type_id() == TypeIndex::String);

        return execute_type(block, argument, result);
    }
};

template <typename Name>
class FunctionIPv4StringToNumOrDefault : public IFunction {
private:
    Status execute_type(Block& block, const ColumnWithTypeAndName& argument, size_t result) const {
        const ColumnPtr& column = argument.column;

        if (const ColumnString* col_src = typeid_cast<const ColumnString*>(column.get())) {
            auto col_res = ColumnInt64::create();

            for (size_t i = 0; i < col_src->size(); ++i) {
                auto ipv4_str = col_src->get_data_at(i).to_string();
                vectorized::IPv4 ipv4_val;
                if (!IPv4Value::from_string(ipv4_val, ipv4_str) || ipv4_str.size() > IPV4_MAX_TEXT_LENGTH) {
                    col_res->insert_value(0);
                } else {
                    col_res->insert_value(ipv4_val);
                }
            }

            DCHECK_EQ(col_res->size(), col_src->size());

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), ColumnUInt8::create(col_src->size(), 0)));
            return Status::OK();
        } else
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        argument.column->get_name(), get_name());
    }

public:
    static constexpr auto name = "ipv4stringtonum_or_default";
    static FunctionPtr create() {
        return std::make_shared<FunctionIPv4StringToNumOrDefault<NameFunctionIPv4StringToNumOrDefault>>();
    }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeInt64>());
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        ColumnWithTypeAndName& argument = block.get_by_position(arguments[0]);

        DCHECK(argument.type->get_type_id() == TypeIndex::String);

        return execute_type(block, argument, result);
    }
};
} // namespace doris::vectorized