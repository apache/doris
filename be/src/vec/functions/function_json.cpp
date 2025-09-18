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

#include <glog/logging.h>
#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/pointer.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <re2/re2.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/token_functions.hpp>
#include <boost/tokenizer.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "exprs/json_functions.h"
#include "runtime/jsonb_value.h"
#include "util/string_parser.hpp"
#include "util/string_util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/io/io_helper.h"
#include "vec/utils/stringop_substring.h"
#include "vec/utils/template_helpers.hpp"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
static const re2::RE2 JSON_PATTERN("^([^\\\"\\[\\]]*)(?:\\[([0-9]+|\\*)\\])?");

template <typename T, typename U>
void char_split(std::vector<T>& res, const U& var, char p) {
    int start = 0;
    int pos = start;
    int end = var.length();
    while (pos < end) {
        while (var[pos] != p && pos < end) {
            pos++;
        }
        res.emplace_back(&var[start], pos - start);
        pos++;
        start = pos;
    }
}

// T = std::vector<std::string>
// TODO: update RE2 to support std::vector<std::string_view>
template <typename T>
void get_parsed_paths(const T& path_exprs, std::vector<JsonPath>* parsed_paths) {
    if (path_exprs.empty()) {
        return;
    }

    if (path_exprs[0] != "$") {
        parsed_paths->emplace_back("", -1, false);
    } else {
        parsed_paths->emplace_back("$", -1, true);
    }

    for (int i = 1; i < path_exprs.size(); i++) {
        std::string col;
        std::string index;
        if (UNLIKELY(!RE2::FullMatch(path_exprs[i], JSON_PATTERN, &col, &index))) {
            parsed_paths->emplace_back("", -1, false);
        } else {
            int idx = -1;
            if (!index.empty()) {
                if (index == "*") {
                    idx = -2;
                } else {
                    idx = atoi(index.c_str());
                }
            }
            parsed_paths->emplace_back(col, idx, true);
        }
    }
}

rapidjson::Value* NO_SANITIZE_UNDEFINED
match_value(const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
            rapidjson::Document::AllocatorType& mem_allocator, bool is_insert_null = false) {
    rapidjson::Value* root = document;
    rapidjson::Value* array_obj = nullptr;
    for (int i = 1; i < parsed_paths.size(); i++) {
        if (root == nullptr || root->IsNull()) {
            return nullptr;
        }

        if (UNLIKELY(!parsed_paths[i].is_valid)) {
            return nullptr;
        }

        const std::string& col = parsed_paths[i].key;
        int index = parsed_paths[i].idx;
        if (LIKELY(!col.empty())) {
            if (root->IsObject()) {
                if (!root->HasMember(col.c_str())) {
                    return nullptr;
                } else {
                    root = &((*root)[col.c_str()]);
                }
            } else {
                // root is not a nested type, return NULL
                return nullptr;
            }
        }

        if (UNLIKELY(index != -1)) {
            // judge the rapidjson:Value, which base the top's result,
            // if not array return NULL;else get the index value from the array
            if (root->IsArray()) {
                if (root->IsNull()) {
                    return nullptr;
                } else if (index == -2) {
                    // [*]
                    array_obj = static_cast<rapidjson::Value*>(
                            mem_allocator.Malloc(sizeof(rapidjson::Value)));
                    array_obj->SetArray();

                    for (int j = 0; j < root->Size(); j++) {
                        rapidjson::Value v;
                        v.CopyFrom((*root)[j], mem_allocator);
                        array_obj->PushBack(v, mem_allocator);
                    }
                    root = array_obj;
                } else if (index >= root->Size()) {
                    return nullptr;
                } else {
                    root = &((*root)[index]);
                }
            } else {
                return nullptr;
            }
        }
    }
    return root;
}

template <JsonFunctionType fntype>
rapidjson::Value* get_json_object(std::string_view json_string, std::string_view path_string,
                                  rapidjson::Document* document) {
    std::vector<JsonPath>* parsed_paths;
    std::vector<JsonPath> tmp_parsed_paths;

    //Cannot use '\' as the last character, return NULL
    if (path_string.back() == '\\') {
        return nullptr;
    }

    std::string fixed_string;
    if (path_string.size() >= 2 && path_string[0] == '$' && path_string[1] != '.') {
        // Boost tokenizer requires explicit "." after "$" to correctly extract JSON path tokens.
        // Without this, expressions like "$[0].key" cannot be properly split.
        // This commit ensures a "." is automatically added after "$" to maintain consistent token parsing behavior.
        fixed_string = "$.";
        fixed_string += path_string.substr(1);
        path_string = fixed_string;
    }

    try {
#ifdef USE_LIBCPP
        std::string s(path_string);
        auto tok = get_json_token(s);
#else
        auto tok = get_json_token(path_string);
#endif
        std::vector<std::string> paths(tok.begin(), tok.end());
        get_parsed_paths(paths, &tmp_parsed_paths);
        if (tmp_parsed_paths.empty()) {
            return document;
        }
    } catch (boost::escaped_list_error&) {
        // meet unknown escape sequence, example '$.name\k'
        return nullptr;
    }

    parsed_paths = &tmp_parsed_paths;

    if (!(*parsed_paths)[0].is_valid) {
        return nullptr;
    }

    if (UNLIKELY((*parsed_paths).size() == 1)) {
        if (fntype == JSON_FUN_STRING) {
            document->SetString(json_string.data(),
                                cast_set<rapidjson::SizeType>(json_string.size()),
                                document->GetAllocator());
        } else {
            return document;
        }
    }

    document->Parse(json_string.data(), json_string.size());
    if (UNLIKELY(document->HasParseError())) {
        // VLOG_CRITICAL << "Error at offset " << document->GetErrorOffset() << ": "
        //         << GetParseError_En(document->GetParseError());
        return nullptr;
    }

    return match_value(*parsed_paths, document, document->GetAllocator());
}

template <int flag>
struct JsonParser {
    //string
    static void update_value(StringParser::ParseResult& result, rapidjson::Value& value,
                             StringRef data, rapidjson::Document::AllocatorType& allocator) {
        value.SetString(data.data, cast_set<rapidjson::SizeType>(data.size), allocator);
    }
};

template <>
struct JsonParser<'0'> {
    // null
    static void update_value(StringParser::ParseResult& result, rapidjson::Value& value,
                             StringRef data, rapidjson::Document::AllocatorType& allocator) {
        value.SetNull();
    }
};

template <>
struct JsonParser<'1'> {
    // bool
    static void update_value(StringParser::ParseResult& result, rapidjson::Value& value,
                             StringRef data, rapidjson::Document::AllocatorType& allocator) {
        DCHECK(data.size == 1 || strncmp(data.data, "true", 4) == 0 ||
               strncmp(data.data, "false", 5) == 0);
        value.SetBool(*data.data == '1' || *data.data == 't');
    }
};

template <>
struct JsonParser<'2'> {
    // int
    static void update_value(StringParser::ParseResult& result, rapidjson::Value& value,
                             StringRef data, rapidjson::Document::AllocatorType& allocator) {
        value.SetInt(StringParser::string_to_int<int32_t>(data.data, data.size, &result));
    }
};

template <>
struct JsonParser<'3'> {
    // double
    static void update_value(StringParser::ParseResult& result, rapidjson::Value& value,
                             StringRef data, rapidjson::Document::AllocatorType& allocator) {
        value.SetDouble(StringParser::string_to_float<double>(data.data, data.size, &result));
    }
};

template <>
struct JsonParser<'4'> {
    // time
    static void update_value(StringParser::ParseResult& result, rapidjson::Value& value,
                             StringRef data, rapidjson::Document::AllocatorType& allocator) {
        // remove double quotes, "xxx" -> xxx
        value.SetString(data.data + 1, cast_set<rapidjson::SizeType>(data.size - 2), allocator);
    }
};

template <>
struct JsonParser<'5'> {
    // bigint
    static void update_value(StringParser::ParseResult& result, rapidjson::Value& value,
                             StringRef data, rapidjson::Document::AllocatorType& allocator) {
        value.SetInt64(StringParser::string_to_int<int64_t>(data.data, data.size, &result));
    }
};

template <>
struct JsonParser<'7'> {
    // json string
    static void update_value(StringParser::ParseResult& result, rapidjson::Value& value,
                             StringRef data, rapidjson::Document::AllocatorType& allocator) {
        rapidjson::Document document;
        JsonbValue* json_val = JsonbDocument::createValue(data.data, data.size);
        convert_jsonb_to_rapidjson(*json_val, document, allocator);
        value.CopyFrom(document, allocator);
    }
};

template <int flag, typename Impl>
struct ExecuteReducer {
    template <typename... TArgs>
    static void run(TArgs&&... args) {
        Impl::template execute_type<JsonParser<flag>>(std::forward<TArgs>(args)...);
    }
};

struct FunctionJsonQuoteImpl {
    static constexpr auto name = "json_quote";

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        if (!arguments.empty() && arguments[0] && arguments[0]->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeString>());
        }
        return std::make_shared<DataTypeString>();
    }
    static void execute(const std::vector<const ColumnString*>& data_columns,
                        ColumnString& result_column, size_t input_rows_count) {
        rapidjson::Document document;
        rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

        rapidjson::Value value;

        rapidjson::StringBuffer buf;

        for (int i = 0; i < input_rows_count; i++) {
            StringRef data = data_columns[0]->get_data_at(i);
            value.SetString(data.data, cast_set<rapidjson::SizeType>(data.size), allocator);

            buf.Clear();
            rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
            value.Accept(writer);
            result_column.insert_data(buf.GetString(), buf.GetSize());
        }
    }
};

template <typename Impl>
class FunctionJson : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionJson<Impl>>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto result_column = ColumnString::create();

        std::vector<ColumnPtr> column_ptrs; // prevent converted column destruct
        std::vector<const ColumnString*> data_columns;
        for (int i = 0; i < arguments.size(); i++) {
            column_ptrs.push_back(
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const());
            data_columns.push_back(assert_cast<const ColumnString*>(column_ptrs.back().get()));
        }

        Impl::execute(data_columns, *assert_cast<ColumnString*>(result_column.get()),
                      input_rows_count);
        block.get_by_position(result).column = std::move(result_column);
        return Status::OK();
    }
};

template <typename Impl>
class FunctionJsonNullable : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionJsonNullable<Impl>>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto result_column = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        std::vector<const ColumnString*> data_columns;
        std::vector<bool> column_is_consts;
        for (int i = 0; i < arguments.size(); i++) {
            ColumnPtr arg_col;
            bool arg_const;
            std::tie(arg_col, arg_const) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
            column_is_consts.push_back(arg_const);
            data_columns.push_back(assert_cast<const ColumnString*>(arg_col.get()));
        }
        Impl::execute(data_columns, *assert_cast<ColumnString*>(result_column.get()),
                      null_map->get_data(), input_rows_count, column_is_consts);
        block.replace_by_position(
                result, ColumnNullable::create(std::move(result_column), std::move(null_map)));
        return Status::OK();
    }
};

class FunctionJsonValid : public IFunction {
public:
    static constexpr auto name = "json_valid";
    static FunctionPtr create() { return std::make_shared<FunctionJsonValid>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeInt32>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const IColumn& col_from = *(block.get_by_position(arguments[0]).column);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        const ColumnUInt8::Container* input_null_map = nullptr;
        const ColumnString* col_from_string = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(col_from)) {
            input_null_map = &nullable->get_null_map_data();
            col_from_string =
                    check_and_get_column<ColumnString>(*nullable->get_nested_column_ptr());
        } else {
            col_from_string = check_and_get_column<ColumnString>(col_from);
        }

        if (!col_from_string) {
            return Status::RuntimeError("Illegal column {} should be ColumnString",
                                        col_from.get_name());
        }

        auto col_to = ColumnInt32::create();
        auto& vec_to = col_to->get_data();
        size_t size = col_from.size();
        vec_to.resize(size);

        // parser can be reused for performance

        auto input_type = block.get_by_position(arguments[0]).type->get_primitive_type();

        if (input_type == PrimitiveType::TYPE_VARCHAR || input_type == PrimitiveType::TYPE_CHAR ||
            input_type == PrimitiveType::TYPE_STRING) {
            JsonBinaryValue jsonb_value;
            for (size_t i = 0; i < input_rows_count; ++i) {
                if (input_null_map && (*input_null_map)[i]) {
                    null_map->get_data()[i] = 1;
                    vec_to[i] = 0;
                    continue;
                }

                const auto& val = col_from_string->get_data_at(i);
                if (jsonb_value.from_json_string(val.data, cast_set<unsigned int>(val.size)).ok()) {
                    vec_to[i] = 1;
                } else {
                    vec_to[i] = 0;
                }
            }

        } else {
            DCHECK(input_type == PrimitiveType::TYPE_JSONB);
            for (size_t i = 0; i < input_rows_count; ++i) {
                if (input_null_map && (*input_null_map)[i]) {
                    null_map->get_data()[i] = 1;
                    vec_to[i] = 0;
                    continue;
                }
                const auto& val = col_from_string->get_data_at(i);
                if (val.size == 0) {
                    vec_to[i] = 0;
                    continue;
                }
                JsonbDocument* doc = nullptr;
                auto st = JsonbDocument::checkAndCreateDocument(val.data, val.size, &doc);
                if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
                    vec_to[i] = 0;
                    continue;
                }
                JsonbValue* value = doc->getValue();
                if (UNLIKELY(!value)) {
                    vec_to[i] = 0;
                    continue;
                }
                vec_to[i] = 1;
            }
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(col_to), std::move(null_map)));

        return Status::OK();
    }
};
class FunctionJsonUnquote : public IFunction {
public:
    static constexpr auto name = "json_unquote";
    static FunctionPtr create() { return std::make_shared<FunctionJsonUnquote>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const IColumn& col_from = *(block.get_by_position(arguments[0]).column);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        const ColumnString* col_from_string = check_and_get_column<ColumnString>(col_from);
        if (auto* nullable = check_and_get_column<ColumnNullable>(col_from)) {
            col_from_string =
                    check_and_get_column<ColumnString>(*nullable->get_nested_column_ptr());
        }

        if (!col_from_string) {
            return Status::RuntimeError("Illegal column {} should be ColumnString",
                                        col_from.get_name());
        }

        auto col_to = ColumnString::create();
        col_to->reserve(input_rows_count);

        // parser can be reused for performance
        rapidjson::Document document;
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (col_from.is_null_at(i)) {
                null_map->get_data()[i] = 1;
                col_to->insert_data(nullptr, 0);
                continue;
            }

            const auto& json_str = col_from_string->get_data_at(i);
            if (json_str.size < 2 || json_str.data[0] != '"' ||
                json_str.data[json_str.size - 1] != '"') {
                // non-quoted string
                col_to->insert_data(json_str.data, json_str.size);
            } else {
                document.Parse(json_str.data, json_str.size);
                if (document.HasParseError() || !document.IsString()) {
                    return Status::RuntimeError(
                            fmt::format("Invalid JSON text in argument 1 to function {}: {}", name,
                                        std::string_view(json_str.data, json_str.size)));
                }
                col_to->insert_data(document.GetString(), document.GetStringLength());
            }
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(col_to), std::move(null_map)));

        return Status::OK();
    }
};

void register_function_json(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionJsonUnquote>();

    factory.register_function<FunctionJson<FunctionJsonQuoteImpl>>();

    factory.register_function<FunctionJsonValid>();
}

} // namespace doris::vectorized
