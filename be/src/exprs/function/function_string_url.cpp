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

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/string_ref.h"
#include "exec/common/stringop_substring.h"
#include "exec/common/template_helpers.hpp"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "util/url_coding.h"
#include "util/url_parser.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

class FunctionExtractURLParameter : public IFunction {
public:
    static constexpr auto name = "extract_url_parameter";
    static FunctionPtr create() { return std::make_shared<FunctionExtractURLParameter>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto col_url =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto col_parameter =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto url_col = assert_cast<const ColumnString*>(col_url.get());
        auto parameter_col = assert_cast<const ColumnString*>(col_parameter.get());

        ColumnString::MutablePtr col_res = ColumnString::create();

        for (int i = 0; i < input_rows_count; ++i) {
            auto source = url_col->get_data_at(i);
            auto param = parameter_col->get_data_at(i);
            auto res = extract_url(source, param);

            col_res->insert_data(res.data, res.size);
        }

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

private:
    StringRef extract_url(StringRef url, StringRef parameter) const {
        if (url.size == 0 || parameter.size == 0) {
            return StringRef("", 0);
        }
        return UrlParser::extract_url(url, parameter);
    }
};

class FunctionStringParseUrl : public IFunction {
public:
    static constexpr auto name = "parse_url";
    static FunctionPtr create() { return std::make_shared<FunctionStringParseUrl>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto& null_map_data = null_map->get_data();
        DCHECK_GE(3, arguments.size());
        auto res = ColumnString::create();
        auto& res_offsets = res->get_offsets();
        auto& res_chars = res->get_chars();
        res_offsets.resize(input_rows_count);

        size_t argument_size = arguments.size();
        const bool has_key = argument_size == 3;

        std::vector<ColumnPtr> argument_columns(argument_size);
        std::vector<UInt8> col_const(argument_size);
        for (size_t i = 0; i < argument_size; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }

        const auto* url_col = assert_cast<const ColumnString*>(argument_columns[0].get());
        const auto* part_col = assert_cast<const ColumnString*>(argument_columns[1].get());
        const bool part_const = col_const[1];
        std::vector<UrlParser::UrlPart> url_parts;
        const int part_nums = part_const ? 1 : input_rows_count;

        url_parts.resize(part_nums);
        for (int i = 0; i < part_nums; i++) {
            StringRef part = part_col->get_data_at(i);
            UrlParser::UrlPart url_part = UrlParser::get_url_part(part);
            if (url_part == UrlParser::INVALID) {
                return Status::RuntimeError("Invalid URL part: {}\n{}",
                                            std::string(part.data, part.size),
                                            "(Valid URL parts are 'PROTOCOL', 'HOST', "
                                            "'PATH', 'REF', 'AUTHORITY', "
                                            "'FILE', 'USERINFO', 'PORT' and 'QUERY')");
            }
            url_parts[i] = url_part;
        }

        if (has_key) {
            const bool url_const = col_const[0];
            const bool key_const = col_const[2];
            const auto* key_col = assert_cast<const ColumnString*>(argument_columns[2].get());
            RETURN_IF_ERROR(std::visit(
                    [&](auto url_const, auto part_const, auto key_const) {
                        return vector_parse_key<url_const, part_const, key_const>(
                                url_col, url_parts, key_col, input_rows_count, null_map_data,
                                res_chars, res_offsets);
                    },
                    make_bool_variant(url_const), make_bool_variant(part_const),
                    make_bool_variant(key_const)));
        } else {
            const bool url_const = col_const[0];
            RETURN_IF_ERROR(std::visit(
                    [&](auto url_const, auto part_const) {
                        return vector_parse<url_const, part_const>(url_col, url_parts,
                                                                   input_rows_count, null_map_data,
                                                                   res_chars, res_offsets);
                    },
                    make_bool_variant(url_const), make_bool_variant(part_const)));
        }
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
    template <bool url_const, bool part_const>
    static Status vector_parse(const ColumnString* url_col,
                               std::vector<UrlParser::UrlPart>& url_parts, const int size,
                               ColumnUInt8::Container& null_map_data,
                               ColumnString::Chars& res_chars, ColumnString::Offsets& res_offsets) {
        for (size_t i = 0; i < size; ++i) {
            UrlParser::UrlPart& url_part = url_parts[index_check_const<part_const>(i)];
            StringRef url_val = url_col->get_data_at(index_check_const<url_const>(i));
            StringRef parse_res;
            if (UrlParser::parse_url(url_val, url_part, &parse_res)) {
                if (parse_res.empty()) [[unlikely]] {
                    StringOP::push_empty_string(i, res_chars, res_offsets);
                    continue;
                }
                StringOP::push_value_string(std::string_view(parse_res.data, parse_res.size), i,
                                            res_chars, res_offsets);
            } else {
                StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
            }
        }
        return Status::OK();
    }
    template <bool url_const, bool part_const, bool key_const>
    static Status vector_parse_key(const ColumnString* url_col,
                                   std::vector<UrlParser::UrlPart>& url_parts,
                                   const ColumnString* key_col, const int size,
                                   ColumnUInt8::Container& null_map_data,
                                   ColumnString::Chars& res_chars,
                                   ColumnString::Offsets& res_offsets) {
        for (size_t i = 0; i < size; ++i) {
            UrlParser::UrlPart& url_part = url_parts[index_check_const<part_const>(i)];
            StringRef url_val = url_col->get_data_at(index_check_const<url_const>(i));
            StringRef url_key = key_col->get_data_at(index_check_const<key_const>(i));
            StringRef parse_res;
            if (UrlParser::parse_url_key(url_val, url_part, url_key, &parse_res)) {
                StringOP::push_value_string(std::string_view(parse_res.data, parse_res.size), i,
                                            res_chars, res_offsets);
            } else {
                StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                continue;
            }
        }
        return Status::OK();
    }
};

class FunctionUrlDecode : public IFunction {
public:
    static constexpr auto name = "url_decode";
    static FunctionPtr create() { return std::make_shared<FunctionUrlDecode>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res = ColumnString::create();
        res->get_offsets().reserve(input_rows_count);

        const auto* url_col =
                assert_cast<const ColumnString*>(block.get_by_position(arguments[0]).column.get());

        std::string decoded_url;
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto url = url_col->get_data_at(i);
            if (!url_decode(url.to_string(), &decoded_url)) {
                return Status::InternalError("Decode url failed");
            }
            res->insert_data(decoded_url.data(), decoded_url.size());
            decoded_url.clear();
        }

        block.get_by_position(result).column = std::move(res);
        return Status::OK();
    }
};

class FunctionUrlEncode : public IFunction {
public:
    static constexpr auto name = "url_encode";
    static FunctionPtr create() { return std::make_shared<FunctionUrlEncode>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res = ColumnString::create();
        res->get_offsets().reserve(input_rows_count);

        const auto* url_col =
                assert_cast<const ColumnString*>(block.get_by_position(arguments[0]).column.get());

        std::string encoded_url;
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto url = url_col->get_data_at(i);
            url_encode(url.to_string_view(), &encoded_url);
            res->insert_data(encoded_url.data(), encoded_url.size());
            encoded_url.clear();
        }

        block.get_by_position(result).column = std::move(res);
        return Status::OK();
    }
};

void register_function_string_url(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionExtractURLParameter>();
    factory.register_function<FunctionStringParseUrl>();
    factory.register_function<FunctionUrlDecode>();
    factory.register_function<FunctionUrlEncode>();
}

#include "common/compile_check_avoid_end.h"
} // namespace doris
