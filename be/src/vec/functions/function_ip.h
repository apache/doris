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
#include "vec/data_types/data_type_number.h"
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

enum class IPStringToNumExceptionMode : uint8_t { Throw, Default, Null };

static inline bool tryParseIPv4(const char* pos, Int64& result_value) {
    return parseIPv4whole(pos, reinterpret_cast<unsigned char*>(&result_value));
}

template <IPStringToNumExceptionMode exception_mode, typename ToColumn>
ColumnPtr convertToIPv4(ColumnPtr column, const PaddedPODArray<UInt8>* null_map = nullptr) {
    const ColumnString* column_string = check_and_get_column<ColumnString>(column.get());

    if (!column_string) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Illegal column {} of argument of function {}, expected String",
                        column->get_name());
    }

    size_t column_size = column_string->size();

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container* vec_null_map_to = nullptr;

    if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
        col_null_map_to = ColumnUInt8::create(column_size, false);
        vec_null_map_to = &col_null_map_to->get_data();
    }

    auto col_res = ToColumn::create();

    auto& vec_res = col_res->get_data();
    vec_res.resize(column_size);

    const ColumnString::Chars& vec_src = column_string->get_chars();
    const ColumnString::Offsets& offsets_src = column_string->get_offsets();
    size_t prev_offset = 0;

    for (size_t i = 0; i < vec_res.size(); ++i) {
        if (null_map && (*null_map)[i]) {
            vec_res[i] = 0;
            prev_offset = offsets_src[i];
            if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
                (*vec_null_map_to)[i] = true;
            continue;
        }

        bool parse_result =
                tryParseIPv4(reinterpret_cast<const char*>(&vec_src[prev_offset]), vec_res[i]);

        if (!parse_result) {
            if constexpr (exception_mode == IPStringToNumExceptionMode::Throw) {
                throw Exception(ErrorCode::RUNTIME_ERROR, "Invalid IPv4 value");
            } else if constexpr (exception_mode == IPStringToNumExceptionMode::Default) {
                vec_res[i] = 0;
            } else if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
                (*vec_null_map_to)[i] = true;
                vec_res[i] = 0;
            }
        }

        prev_offset = offsets_src[i];
    }

    if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
        return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));

    return col_res;
}

template <IPStringToNumExceptionMode exception_mode>
class FunctionIPv4StringToNum : public IFunction {
public:
    static constexpr auto name = exception_mode == IPStringToNumExceptionMode::Throw
                                         ? "ipv4stringtonum"
                                         : (exception_mode == IPStringToNumExceptionMode::Default
                                                    ? "ipv4stringtonumordefault"
                                                    : "ipv4stringtonumornull");

    static FunctionPtr create() {
        return std::make_shared<FunctionIPv4StringToNum<exception_mode>>();
    }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (!is_string(remove_nullable(arguments[0]))) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Illegal type {} of argument of function {}", arguments[0]->get_name(),
                            get_name());
        }
        auto result_type = std::make_shared<DataTypeInt64>();

        if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
            return make_nullable(result_type);
        }

        return arguments[0]->is_nullable() ? make_nullable(result_type) : result_type;
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr column = block.get_by_position(arguments[0]).column;
        ColumnPtr null_map_column;
        const NullMap* null_map = nullptr;
        if (column->is_nullable()) {
            const auto* column_nullable = assert_cast<const ColumnNullable*>(column.get());
            column = column_nullable->get_nested_column_ptr();
            null_map_column = column_nullable->get_null_map_column_ptr();
            null_map = &column_nullable->get_null_map_data();
        }

        auto col_res = convertToIPv4<exception_mode, ColumnInt64>(column, null_map);

        if (null_map && !col_res->is_nullable())
            block.replace_by_position(result,
                                      ColumnNullable::create(IColumn::mutate(col_res),
                                                             IColumn::mutate(null_map_column)));

        block.replace_by_position(result, col_res);
        return Status::OK();
    }
};

} // namespace doris::vectorized