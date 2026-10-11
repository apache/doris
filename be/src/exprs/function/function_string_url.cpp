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

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_const.h"
#include "core/column/column_execute_util.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/string_ref.h"
#include "exec/common/stringop_substring.h"
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
    bool use_default_implementation_for_nulls() const override { return false; }

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

        const auto url_col =
                ColumnView<TYPE_STRING>::create(block.get_by_position(arguments[0]).column);
        const auto part_col =
                ColumnView<TYPE_STRING>::create(block.get_by_position(arguments[1]).column);
        if (has_key) {
            const auto key_col =
                    ColumnView<TYPE_STRING>::create(block.get_by_position(arguments[2]).column);
            for (size_t i = 0; i < input_rows_count; ++i) {
                if (url_col.is_null_at(i) || part_col.is_null_at(i) || key_col.is_null_at(i)) {
                    StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                    continue;
                }
                const auto part = part_col.value_at(i);
                const auto url_part = UrlParser::get_url_part(part);
                if (url_part == UrlParser::INVALID) {
                    return Status::RuntimeError("Invalid URL part: {}\n{}",
                                                std::string(part.data, part.size),
                                                "(Valid URL parts are 'PROTOCOL', 'HOST', "
                                                "'PATH', 'REF', 'AUTHORITY', "
                                                "'FILE', 'USERINFO', 'PORT' and 'QUERY')");
                }
                StringRef parse_res;
                if (UrlParser::parse_url_key(url_col.value_at(i), url_part, key_col.value_at(i),
                                             &parse_res)) {
                    StringOP::push_value_string(std::string_view(parse_res.data, parse_res.size), i,
                                                res_chars, res_offsets);
                } else {
                    StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                }
            }
        } else {
            for (size_t i = 0; i < input_rows_count; ++i) {
                if (url_col.is_null_at(i) || part_col.is_null_at(i)) {
                    StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                    continue;
                }
                const auto part = part_col.value_at(i);
                const auto url_part = UrlParser::get_url_part(part);
                if (url_part == UrlParser::INVALID) {
                    return Status::RuntimeError("Invalid URL part: {}\n{}",
                                                std::string(part.data, part.size),
                                                "(Valid URL parts are 'PROTOCOL', 'HOST', "
                                                "'PATH', 'REF', 'AUTHORITY', "
                                                "'FILE', 'USERINFO', 'PORT' and 'QUERY')");
                }
                StringRef parse_res;
                if (UrlParser::parse_url(url_col.value_at(i), url_part, &parse_res)) {
                    if (parse_res.empty()) {
                        StringOP::push_empty_string(i, res_chars, res_offsets);
                    } else {
                        StringOP::push_value_string(
                                std::string_view(parse_res.data, parse_res.size), i, res_chars,
                                res_offsets);
                    }
                } else {
                    StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                }
            }
        }
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
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
