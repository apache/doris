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
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

/** If mask_tail_octets > 0, the last specified number of octets will be filled with "xxx".
  */
template <size_t mask_tail_octets, typename Name>
class FunctionIPv4NumToString : public IFunction {
private:
    template <typename ArgType>
    Status execute_type(Block& block, const ColumnWithTypeAndName& argument, size_t result) const {
        using ColumnType = ColumnVector<ArgType>;
        const ColumnPtr& column = argument.column;

        if (const ColumnType* col = typeid_cast<const ColumnType*>(column.get())) {
            const typename ColumnType::Container& vec_in = col->get_data();
            auto col_res = ColumnString::create();

            ColumnString::Chars& vec_res = col_res->get_chars();
            ColumnString::Offsets& offsets_res = col_res->get_offsets();

            vec_res.resize(vec_in.size() *
                           (IPV4_MAX_TEXT_LENGTH + 1)); /// the longest value is: 255.255.255.255\0
            offsets_res.resize(vec_in.size());
            char* begin = reinterpret_cast<char*>(vec_res.data());
            char* pos = begin;

            auto null_map = ColumnUInt8::create(vec_in.size(), 0);
            size_t src_size = std::min(sizeof(ArgType), (unsigned long)4);
            for (size_t i = 0; i < vec_in.size(); ++i) {
                auto value = vec_in[i];
                if (value < IPV4_MIN_NUM_VALUE || value > IPV4_MAX_NUM_VALUE) {
                    offsets_res[i] = pos - begin;
                    null_map->get_data()[i] = 1;
                } else {
                    formatIPv4(reinterpret_cast<const unsigned char*>(&vec_in[i]), src_size, pos,
                               mask_tail_octets, "xxx");
                    offsets_res[i] = pos - begin;
                }
            }

            vec_res.resize(pos - begin);
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
                        size_t result, size_t input_rows_count) const override {
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
    template <typename ResType>
    Status execute_type(Block& block, const ColumnWithTypeAndName& argument, size_t result) const {
        using ColumnType = ColumnString;
        const ColumnPtr& column = argument.column;

        if (const ColumnType* col = typeid_cast<const ColumnType*>(column.get())) {
            const typename ColumnType::Chars& vec_in = col->get_chars();
            const typename ColumnType::Offsets& offsets_in = col->get_offsets();
            auto col_res = ColumnVector<ResType>::create();

            typename ColumnVector<ResType>::Container& vec_res = col_res->get_data();
            vec_res.resize(offsets_in.size());

            for (size_t i = 0; i < offsets_in.size(); ++i) {
                StringRef str_ref = col->get_string(i);
                const char* begin = str_ref.data;
                const char* end = begin + str_ref.size;

                uint32_t value = 0;
                if (parseIPv4(begin, end, value)) {
                    vec_res[i] = value;
                } else {
                    return Status::RuntimeError("Invalid IPv4 address: {}", String(begin, end));
                }
            }

            block.replace_by_position(result, std::move(col_res));
            return Status::OK();
        } else
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        argument.column->get_name(), get_name());
    }

public:
    static constexpr auto name = "ipv4stringtonum";
    static FunctionPtr create() { return std::make_shared<FunctionIPv4StringToNum<Name>>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt32>();
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnWithTypeAndName& argument = block.get_by_position(arguments[0]);

        if (argument.type->is_string()) {
            return execute_type<UInt32>(block, argument, result);
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}, expected String",
                                        argument.name, get_name());
        }
    }
};
} // namespace doris::vectorized