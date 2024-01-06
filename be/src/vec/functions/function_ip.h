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
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/ip_address_cidr.h"

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
    static constexpr auto name = "ipv4_num_to_string";
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
            if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
                (*vec_null_map_to)[i] = true;
            }
            continue;
        }

        const char* src_start = reinterpret_cast<const char*>(&vec_src[prev_offset]);
        size_t src_length = (i < vec_res.size() - 1) ? (offsets_src[i] - prev_offset)
                                                     : (vec_src.size() - prev_offset);
        std::string src(src_start, src_length);
        bool parse_result = tryParseIPv4(src.c_str(), vec_res[i]);

        if (!parse_result) {
            if constexpr (exception_mode == IPStringToNumExceptionMode::Throw) {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid IPv4 value");
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
                                         ? "ipv4_string_to_num"
                                         : (exception_mode == IPStringToNumExceptionMode::Default
                                                    ? "ipv4_string_to_num_or_default"
                                                    : "ipv4_string_to_num_or_null");

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

    bool use_default_implementation_for_nulls() const override { return false; }

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

        if (null_map && !col_res->is_nullable()) {
            block.replace_by_position(result,
                                      ColumnNullable::create(IColumn::mutate(col_res),
                                                             IColumn::mutate(null_map_column)));
            return Status::OK();
        }

        block.replace_by_position(result, col_res);
        return Status::OK();
    }
};

template <typename T>
void process_ipv6_column(const ColumnPtr& column, size_t input_rows_count,
                         ColumnString::Chars& vec_res, ColumnString::Offsets& offsets_res,
                         ColumnUInt8::MutablePtr& null_map, unsigned char* ipv6_address_data) {
    auto* begin = reinterpret_cast<char*>(vec_res.data());
    auto* pos = begin;

    const auto* col = check_and_get_column<T>(column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        bool is_empty = false;

        if constexpr (std::is_same_v<T, ColumnIPv6>) {
            const auto& vec_in = col->get_data();
            memcpy(ipv6_address_data, reinterpret_cast<const unsigned char*>(&vec_in[i]),
                   IPV6_BINARY_LENGTH);
        } else {
            const auto str_ref = col->get_data_at(i);
            const char* value = str_ref.data;
            size_t value_size = str_ref.size;

            if (value_size > IPV6_BINARY_LENGTH || value == nullptr || value_size == 0) {
                is_empty = true;
            } else {
                memcpy(ipv6_address_data, value, value_size);
                memset(ipv6_address_data + value_size, 0, IPV6_BINARY_LENGTH - value_size);
            }
        }

        if (is_empty) {
            offsets_res[i] = pos - begin;
            null_map->get_data()[i] = 1;
        } else {
            formatIPv6(ipv6_address_data, pos);
            offsets_res[i] = pos - begin;
        }
    }
}

class FunctionIPv6NumToString : public IFunction {
public:
    static constexpr auto name = "ipv6_num_to_string";
    static FunctionPtr create() { return std::make_shared<FunctionIPv6NumToString>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const auto* arg_string = check_and_get_data_type<DataTypeString>(arguments[0].get());
        const auto* arg_ipv6 = check_and_get_data_type<DataTypeIPv6>(arguments[0].get());
        if (!arg_ipv6 && !(arg_string))
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Illegal type {} of argument of function {}, expected IPv6 or String",
                            arguments[0]->get_name(), get_name());

        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnPtr& column = block.get_by_position(arguments[0]).column;
        const auto* col_ipv6 = check_and_get_column<ColumnIPv6>(column.get());
        const auto* col_string = check_and_get_column<ColumnString>(column.get());

        if (!col_ipv6 && !col_string)
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Illegal column {} of argument of function {}, expected IPv6 or String",
                            column->get_name(), get_name());

        auto col_res = ColumnString::create();
        ColumnString::Chars& vec_res = col_res->get_chars();
        ColumnString::Offsets& offsets_res = col_res->get_offsets();
        vec_res.resize(input_rows_count * (IPV6_MAX_TEXT_LENGTH + 1));
        offsets_res.resize(input_rows_count);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        unsigned char ipv6_address_data[IPV6_BINARY_LENGTH];

        if (col_ipv6) {
            process_ipv6_column<ColumnIPv6>(column, input_rows_count, vec_res, offsets_res,
                                            null_map, ipv6_address_data);
        } else {
            process_ipv6_column<ColumnString>(column, input_rows_count, vec_res, offsets_res,
                                              null_map, ipv6_address_data);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(col_res), std::move(null_map)));
        return Status::OK();
    }
};

namespace detail {
template <IPStringToNumExceptionMode exception_mode, typename ToColumn = ColumnIPv6,
          typename StringColumnType>
ColumnPtr convertToIPv6(const StringColumnType& string_column,
                        const PaddedPODArray<UInt8>* null_map = nullptr) {
    if constexpr (!std::is_same_v<ToColumn, ColumnString> &&
                  !std::is_same_v<ToColumn, ColumnIPv6>) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Illegal return column type {}. Expected IPv6 or String",
                        TypeName<typename ToColumn::ValueType>::get());
    }

    const size_t column_size = string_column.size();

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container* vec_null_map_to = nullptr;

    if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
        col_null_map_to = ColumnUInt8::create(column_size, false);
        vec_null_map_to = &col_null_map_to->get_data();
    }

    /// This is a special treatment for source column of type String
    /// to preserve previous behavior when IPv6 was a domain type of String
    if constexpr (std::is_same_v<StringColumnType, ColumnString>) {
        if (string_column.get_offsets()[0] - 1 == IPV6_BINARY_LENGTH) {
            if constexpr (std::is_same_v<ToColumn, ColumnString>) {
                auto col_res = ColumnString::create();

                if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
                    col_null_map_to = ColumnUInt8::create(column_size, false);
                    if (null_map) {
                        memcpy(col_null_map_to->get_data().data(), null_map->data(), column_size);
                    }

                    return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
                }

                return col_res;
            } else {
                auto col_res = ColumnIPv6::create();
                auto& vec_res = col_res->get_data();

                vec_res.resize(column_size);
                memcpy(vec_res.data(), string_column.get_chars().data(),
                       column_size * IPV6_BINARY_LENGTH);

                if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
                    col_null_map_to = ColumnUInt8::create(column_size, false);
                    if (null_map) {
                        memcpy(col_null_map_to->get_data().data(), null_map->data(), column_size);
                    }
                    return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
                }

                return col_res;
            }
        }
    }

    auto column_create = [](size_t column_size) -> typename ToColumn::MutablePtr {
        if constexpr (std::is_same_v<ToColumn, ColumnString>) {
            auto column_string = ColumnString::create();
            column_string->get_chars().reserve(column_size * IPV6_BINARY_LENGTH);
            column_string->get_offsets().reserve(column_size);
            return column_string;
        } else {
            return ColumnIPv6::create();
        }
    };

    auto get_vector = [](auto& col_res, size_t col_size) -> decltype(auto) {
        if constexpr (std::is_same_v<ToColumn, ColumnString>) {
            auto& vec_res = col_res->get_chars();
            vec_res.resize(col_size * IPV6_BINARY_LENGTH);
            return (vec_res);
        } else {
            auto& vec_res = col_res->get_data();
            vec_res.resize(col_size);
            return (vec_res);
        }
    };

    auto col_res = column_create(column_size);
    auto& vec_res = get_vector(col_res, column_size);

    using Chars = typename StringColumnType::Chars;
    const Chars& vec_src = string_column.get_chars();

    size_t src_offset = 0;
    char src_ipv4_buf[sizeof("::ffff:") + IPV4_MAX_TEXT_LENGTH + 1] = "::ffff:";

    /// ColumnString contains not null terminated strings. But functions parseIPv6, parseIPv4 expect null terminated string.
    /// TODO fix this - now parseIPv6/parseIPv4 accept end iterator, so can be parsed in-place
    std::string string_buffer;

    int offset_inc = 1;
    if constexpr (std::is_same_v<ToColumn, ColumnString>) {
        offset_inc = IPV6_BINARY_LENGTH;
    }

    for (size_t out_offset = 0, i = 0; i < column_size; out_offset += offset_inc, ++i) {
        size_t src_next_offset = src_offset;

        const char* src_value = nullptr;
        auto* res_value = reinterpret_cast<unsigned char*>(&vec_res[out_offset]);

        if constexpr (std::is_same_v<StringColumnType, ColumnString>) {
            src_value = reinterpret_cast<const char*>(&vec_src[src_offset]);
            src_next_offset = string_column.get_offsets()[i];

            string_buffer.assign(src_value, src_next_offset - src_offset);
            src_value = string_buffer.c_str();
        }

        if (null_map && (*null_map)[i]) {
            if (exception_mode == IPStringToNumExceptionMode::Throw) {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid IPv6 value");
            } else if (exception_mode == IPStringToNumExceptionMode::Default) {
                std::fill_n(&vec_res[out_offset], offset_inc, 0);
            } else {
                std::fill_n(&vec_res[out_offset], offset_inc, 0);
                (*vec_null_map_to)[i] = true;
                if constexpr (std::is_same_v<ToColumn, ColumnString>) {
                    auto* column_string = assert_cast<ColumnString*>(col_res.get());
                    column_string->get_offsets().push_back((i + 1) * IPV6_BINARY_LENGTH);
                }
            }
            src_offset = src_next_offset;
            continue;
        }

        bool parse_result = false;
        Int64 dummy_result = 0;

        /// For both cases below: In case of failure, the function parseIPv6 fills vec_res with zero bytes.

        /// If the source IP address is parsable as an IPv4 address, then transform it into a valid IPv6 address.
        /// Keeping it simple by just prefixing `::ffff:` to the IPv4 address to represent it as a valid IPv6 address.
        size_t string_length = src_next_offset - src_offset;
        if (string_length != 0) {
            if (tryParseIPv4(src_value, dummy_result)) {
                strcat(src_ipv4_buf, src_value);
                parse_result = parseIPv6whole(src_ipv4_buf, res_value);
            } else {
                parse_result = parseIPv6whole(src_value, res_value);
            }
        }

        if (parse_result && string_length != 0) {
            if constexpr (std::is_same_v<ToColumn, ColumnString>) {
                auto* column_string = assert_cast<ColumnString*>(col_res.get());
                std::copy(res_value, res_value + IPV6_BINARY_LENGTH,
                          column_string->get_chars().begin() + i * IPV6_BINARY_LENGTH);
                column_string->get_offsets().push_back((i + 1) * IPV6_BINARY_LENGTH);
            } else {
                col_res->insert_data(reinterpret_cast<const char*>(res_value), IPV6_BINARY_LENGTH);
            }
        } else {
            if (exception_mode == IPStringToNumExceptionMode::Throw) {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid IPv6 value");
            }
            std::fill_n(&vec_res[out_offset], offset_inc, 0);
            if constexpr (std::is_same_v<ToColumn, ColumnString>) {
                auto* column_string = assert_cast<ColumnString*>(col_res.get());
                column_string->get_offsets().push_back((i + 1) * IPV6_BINARY_LENGTH);
            }
            if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
                (*vec_null_map_to)[i] = true;
            }
        }
        src_offset = src_next_offset;
    }

    if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
        return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
    }
    return col_res;
}
} // namespace detail

template <IPStringToNumExceptionMode exception_mode, typename ToColumn = ColumnIPv6>
ColumnPtr convertToIPv6(ColumnPtr column, const PaddedPODArray<UInt8>* null_map = nullptr) {
    if (const auto* column_input_string = check_and_get_column<ColumnString>(column.get())) {
        auto result =
                detail::convertToIPv6<exception_mode, ToColumn>(*column_input_string, null_map);
        return result;
    } else {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Illegal column type {}. Expected String",
                        column->get_name());
    }
}

template <IPStringToNumExceptionMode exception_mode>
class FunctionIPv6StringToNum : public IFunction {
public:
    static constexpr auto name = exception_mode == IPStringToNumExceptionMode::Throw
                                         ? "ipv6_string_to_num"
                                         : (exception_mode == IPStringToNumExceptionMode::Default
                                                    ? "ipv6_string_to_num_or_default"
                                                    : "ipv6_string_to_num_or_null");

    static FunctionPtr create() {
        return std::make_shared<FunctionIPv6StringToNum<exception_mode>>();
    }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (!is_string(remove_nullable(arguments[0]))) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Illegal type {} of argument of function {}", arguments[0]->get_name(),
                            get_name());
        }

        auto result_type = std::make_shared<DataTypeString>();

        if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
            return make_nullable(result_type);
        }

        return result_type;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr column = block.get_by_position(arguments[0]).column;
        ColumnPtr null_map_column;
        const NullMap* null_map = nullptr;

        if (column->is_nullable()) {
            const auto* column_nullable = assert_cast<const ColumnNullable*>(column.get());
            column = column_nullable->get_nested_column_ptr();
            if constexpr (exception_mode == IPStringToNumExceptionMode::Null) {
                null_map_column = column_nullable->get_null_map_column_ptr();
                null_map = &column_nullable->get_null_map_data();
            }
        }

        auto col_res = convertToIPv6<exception_mode, ColumnString>(column, null_map);

        if (null_map && !col_res->is_nullable()) {
            block.replace_by_position(result,
                                      ColumnNullable::create(IColumn::mutate(col_res),
                                                             IColumn::mutate(null_map_column)));
            return Status::OK();
        }

        block.replace_by_position(result, col_res);
        return Status::OK();
    }
};

class FunctionIsIPv4String : public IFunction {
private:
    Status execute_type(Block& block, const ColumnWithTypeAndName& argument, size_t result) const {
        const ColumnPtr& column = argument.column;

        if (const auto* nullable_src = typeid_cast<const ColumnNullable*>(column.get())) {
            size_t col_size = nullable_src->size();
            auto col_res = ColumnUInt8::create(col_size, 0);
            auto null_map = ColumnUInt8::create(col_size, 0);
            auto& col_res_data = col_res->get_data();
            auto& null_map_data = null_map->get_data();

            for (size_t i = 0; i < col_size; ++i) {
                if (nullable_src->is_null_at(i)) {
                    null_map_data[i] = 1;
                } else {
                    StringRef ipv4_str = nullable_src->get_data_at(i);
                    if (IPv4Value::is_valid_string(ipv4_str.data, ipv4_str.size)) {
                        col_res_data[i] = 1;
                    }
                }
            }

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(null_map)));
            return Status::OK();
        } else if (const auto* col_src = typeid_cast<const ColumnString*>(column.get())) {
            size_t col_size = col_src->size();
            auto col_res = ColumnUInt8::create(col_size, 0);
            auto null_map = ColumnUInt8::create(col_size, 0);
            auto& col_res_data = col_res->get_data();

            for (size_t i = 0; i < col_size; ++i) {
                StringRef ipv4_str = col_src->get_data_at(i);
                if (IPv4Value::is_valid_string(ipv4_str.data, ipv4_str.size)) {
                    col_res_data[i] = 1;
                }
            }

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(null_map)));
            return Status::OK();
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        argument.column->get_name(), get_name());
        }
    }

public:
    static constexpr auto name = "isipv4string";
    static FunctionPtr create() { return std::make_shared<FunctionIsIPv4String>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeUInt8>());
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnWithTypeAndName& argument = block.get_by_position(arguments[0]);
        DCHECK(argument.type->get_type_id() == TypeIndex::String);
        return execute_type(block, argument, result);
    }
};

class FunctionIsIPv6String : public IFunction {
private:
    Status execute_type(Block& block, const ColumnWithTypeAndName& argument, size_t result) const {
        const ColumnPtr& column = argument.column;

        if (const auto* nullable_src = typeid_cast<const ColumnNullable*>(column.get())) {
            size_t col_size = nullable_src->size();
            auto col_res = ColumnUInt8::create(col_size, 0);
            auto null_map = ColumnUInt8::create(col_size, 0);
            auto& col_res_data = col_res->get_data();
            auto& null_map_data = null_map->get_data();

            for (size_t i = 0; i < col_size; ++i) {
                if (nullable_src->is_null_at(i)) {
                    null_map_data[i] = 1;
                } else {
                    StringRef ipv6_str = nullable_src->get_data_at(i);
                    if (IPv6Value::is_valid_string(ipv6_str.data, ipv6_str.size)) {
                        col_res_data[i] = 1;
                    }
                }
            }

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(null_map)));
            return Status::OK();
        } else if (const auto* col_src = typeid_cast<const ColumnString*>(column.get())) {
            size_t col_size = col_src->size();
            auto col_res = ColumnUInt8::create(col_size, 0);
            auto null_map = ColumnUInt8::create(col_size, 0);
            auto& col_res_data = col_res->get_data();

            for (size_t i = 0; i < col_size; ++i) {
                StringRef ipv6_str = col_src->get_data_at(i);
                if (IPv6Value::is_valid_string(ipv6_str.data, ipv6_str.size)) {
                    col_res_data[i] = 1;
                }
            }

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(null_map)));
            return Status::OK();
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        argument.column->get_name(), get_name());
        }
    }

public:
    static constexpr auto name = "isipv6string";
    static FunctionPtr create() { return std::make_shared<FunctionIsIPv6String>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeUInt8>());
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnWithTypeAndName& argument = block.get_by_position(arguments[0]);
        DCHECK(argument.type->get_type_id() == TypeIndex::String);
        return execute_type(block, argument, result);
    }
};

class FunctionIsIPAddressInRange : public IFunction {
public:
    static constexpr auto name = "isipaddressinrange";
    static FunctionPtr create() { return std::make_shared<FunctionIsIPAddressInRange>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != 2) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Number of arguments for function {} doesn't match: passed {}, should be 2",
                            get_name(), arguments.size());
        }
        const auto& addr_type = arguments[0];
        const auto& cidr_type = arguments[1];
        if (!is_string(remove_nullable(addr_type)) || !is_string(remove_nullable(cidr_type))) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "The arguments of function {} must be String", get_name());
        }
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnPtr& addr_column = block.get_by_position(arguments[0]).column;
        const ColumnPtr& cidr_column = block.get_by_position(arguments[1]).column;
        const auto* col_addr = check_and_get_column<ColumnString>(addr_column.get());
        const auto* col_cidr = check_and_get_column<ColumnString>(cidr_column.get());

        if (!col_addr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Illegal column {} of argument of function {}, expected String",
                            addr_column->get_name(), get_name());
        }

        if (!col_cidr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Illegal column {} of argument of function {}, expected String",
                            cidr_column->get_name(), get_name());
        }

        auto col_res = ColumnUInt8::create(input_rows_count, 0);
        auto& col_res_data = col_res->get_data();

        for (size_t i = 0; i < input_rows_count; ++i) {
            const auto addr = IPAddressVariant(col_addr->get_data_at(i).to_string_view());
            const auto cidr = parse_ip_with_cidr(col_cidr->get_data_at(i).to_string_view());
            col_res_data[i] = is_address_in_range(addr, cidr) ? 1 : 0;
        }

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }
};

} // namespace doris::vectorized