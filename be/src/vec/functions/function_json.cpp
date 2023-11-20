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

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "exprs/json_functions.h"
#ifdef __AVX2__
#include "util/jsonb_parser_simd.h"
#else
#include "util/jsonb_parser.h"
#endif
#include "util/string_parser.hpp"
#include "util/string_util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
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
#include "vec/functions/function_string.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/template_helpers.hpp"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
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

rapidjson::Value* match_value(const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
                              rapidjson::Document::AllocatorType& mem_allocator,
                              bool is_insert_null = false) {
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
            if (root->IsArray()) {
                array_obj = static_cast<rapidjson::Value*>(
                        mem_allocator.Malloc(sizeof(rapidjson::Value)));
                array_obj->SetArray();
                bool is_null = true;

                // if array ,loop the array,find out all Objects,then find the results from the objects
                for (int j = 0; j < root->Size(); j++) {
                    rapidjson::Value* json_elem = &((*root)[j]);

                    if (json_elem->IsArray() || json_elem->IsNull()) {
                        continue;
                    } else {
                        if (!json_elem->IsObject()) {
                            continue;
                        }
                        if (!json_elem->HasMember(col.c_str())) {
                            if (is_insert_null) { // not found item, then insert a null object.
                                is_null = false;
                                rapidjson::Value nullObject(rapidjson::kNullType);
                                array_obj->PushBack(nullObject, mem_allocator);
                            }
                            continue;
                        }
                        rapidjson::Value* obj = &((*json_elem)[col.c_str()]);
                        if (obj->IsArray()) {
                            is_null = false;
                            for (int k = 0; k < obj->Size(); k++) {
                                array_obj->PushBack((*obj)[k], mem_allocator);
                            }
                        } else if (!obj->IsNull()) {
                            is_null = false;
                            array_obj->PushBack(*obj, mem_allocator);
                        }
                    }
                }

                root = is_null ? &(array_obj->SetNull()) : array_obj;
            } else if (root->IsObject()) {
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

    parsed_paths = &tmp_parsed_paths;

    if (!(*parsed_paths)[0].is_valid) {
        return document;
    }

    if (UNLIKELY((*parsed_paths).size() == 1)) {
        if (fntype == JSON_FUN_STRING) {
            document->SetString(json_string.data(), json_string.size(), document->GetAllocator());
        } else {
            return document;
        }
    }

    document->Parse(json_string.data(), json_string.size());
    if (UNLIKELY(document->HasParseError())) {
        // VLOG_CRITICAL << "Error at offset " << document->GetErrorOffset() << ": "
        //         << GetParseError_En(document->GetParseError());
        document->SetNull();
        return document;
    }

    return match_value(*parsed_paths, document, document->GetAllocator());
}

template <typename NumberType>
struct GetJsonNumberType {
    using ReturnType = typename NumberType::ReturnType;
    using ColumnType = typename NumberType::ColumnType;
    using Container = typename ColumnType::Container;

    static DataTypes get_variadic_argument_types_impl() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    static void get_json_impl(rapidjson::Value*& root, const std::string_view& json_string,
                              const std::string_view& path_string, rapidjson::Document& document,
                              typename NumberType::T& res, UInt8& null_map) {
        if constexpr (std::is_same_v<double, typename NumberType::T>) {
            root = get_json_object<JSON_FUN_DOUBLE>(json_string, path_string, &document);
            handle_result<double>(root, res, null_map);
        } else if constexpr (std::is_same_v<int32_t, typename NumberType::T>) {
            root = get_json_object<JSON_FUN_DOUBLE>(json_string, path_string, &document);
            handle_result<int32_t>(root, res, null_map);
        } else if constexpr (std::is_same_v<int64_t, typename NumberType::T>) {
            root = get_json_object<JSON_FUN_DOUBLE>(json_string, path_string, &document);
            handle_result<int64_t>(root, res, null_map);
        }
    }
    static void vector_vector(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, Container& res,
                              NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];
            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];
            if (null_map[i]) {
                res[i] = 0;
                continue;
            }

            std::string_view json_string(l_raw_str, l_str_size);
            std::string_view path_string(r_raw_str, r_str_size);
            rapidjson::Document document;
            rapidjson::Value* root = nullptr;

            get_json_impl(root, json_string, path_string, document, res[i], null_map[i]);
        }
    }
    static void vector_scalar(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets, const StringRef& rdata,
                              Container& res, NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);
        std::string_view path_string(rdata.data, rdata.size);
        for (size_t i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];
            if (null_map[i]) {
                res[i] = 0;
                continue;
            }
            std::string_view json_string(l_raw_str, l_str_size);

            rapidjson::Document document;
            rapidjson::Value* root = nullptr;

            get_json_impl(root, json_string, path_string, document, res[i], null_map[i]);
        }
    }
    static void scalar_vector(FunctionContext* context, const StringRef& ldata,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, Container& res,
                              NullMap& null_map) {
        size_t size = roffsets.size();
        res.resize(size);
        std::string_view json_string(ldata.data, ldata.size);
        for (size_t i = 0; i < size; ++i) {
            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];
            if (null_map[i]) {
                res[i] = 0;
                continue;
            }
            std::string_view path_string(r_raw_str, r_str_size);
            rapidjson::Document document;
            rapidjson::Value* root = nullptr;

            get_json_impl(root, json_string, path_string, document, res[i], null_map[i]);
        }
    }

    template <typename T>
        requires std::is_same_v<double, T>
    static void handle_result(rapidjson::Value* root, T& res, uint8_t& res_null) {
        if (root == nullptr || root->IsNull()) {
            res = 0;
            res_null = 1;
        } else if (root->IsInt()) {
            res = root->GetInt();
        } else if (root->IsInt64()) {
            res = root->GetInt64();
        } else if (root->IsDouble()) {
            res = root->GetDouble();
        } else {
            res_null = 1;
        }
    }

    template <typename T>
        requires std::is_same_v<T, int32_t>
    static void handle_result(rapidjson::Value* root, int32_t& res, uint8_t& res_null) {
        if (root != nullptr && root->IsInt()) {
            res = root->GetInt();
        } else {
            res_null = 1;
        }
    }

    template <typename T>
        requires std::is_same_v<T, int64_t>
    static void handle_result(rapidjson::Value* root, int64_t& res, uint8_t& res_null) {
        if (root != nullptr && root->IsInt64()) {
            res = root->GetInt64();
        } else {
            res_null = 1;
        }
    }
};

// Helper Class
struct JsonNumberTypeDouble {
    using T = Float64;
    using ReturnType = DataTypeFloat64;
    using ColumnType = ColumnVector<T>;
};

struct JsonNumberTypeInt {
    using T = int32_t;
    using ReturnType = DataTypeInt32;
    using ColumnType = ColumnVector<T>;
};

struct JsonNumberTypeBigInt {
    using T = int64_t;
    using ReturnType = DataTypeInt64;
    using ColumnType = ColumnVector<T>;
};

struct GetJsonDouble : public GetJsonNumberType<JsonNumberTypeDouble> {
    static constexpr auto name = "get_json_double";
    using ReturnType = typename JsonNumberTypeDouble::ReturnType;
    using ColumnType = typename JsonNumberTypeDouble::ColumnType;
};

struct GetJsonInt : public GetJsonNumberType<JsonNumberTypeInt> {
    static constexpr auto name = "get_json_int";
    using ReturnType = typename JsonNumberTypeInt::ReturnType;
    using ColumnType = typename JsonNumberTypeInt::ColumnType;
};

struct GetJsonBigInt : public GetJsonNumberType<JsonNumberTypeBigInt> {
    static constexpr auto name = "get_json_bigint";
    using ReturnType = typename JsonNumberTypeBigInt::ReturnType;
    using ColumnType = typename JsonNumberTypeBigInt::ColumnType;
};

struct GetJsonString {
    static constexpr auto name = "get_json_string";
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;
    using Chars = ColumnString::Chars;
    using Offsets = ColumnString::Offsets;
    static void vector_vector(FunctionContext* context, const Chars& ldata, const Offsets& loffsets,
                              const Chars& rdata, const Offsets& roffsets, Chars& res_data,
                              Offsets& res_offsets, NullMap& null_map) {
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }
            int l_size = loffsets[i] - loffsets[i - 1];
            const auto l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int r_size = roffsets[i] - roffsets[i - 1];
            const auto r_raw = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);

            std::string_view json_string(l_raw, l_size);
            std::string_view path_string(r_raw, r_size);

            execute_impl(json_string, path_string, res_data, res_offsets, null_map, i);
        }
    }
    static void vector_scalar(FunctionContext* context, const Chars& ldata, const Offsets& loffsets,
                              const StringRef& rdata, Chars& res_data, Offsets& res_offsets,
                              NullMap& null_map) {
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }
            int l_size = loffsets[i] - loffsets[i - 1];
            const auto l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);

            std::string_view json_string(l_raw, l_size);
            std::string_view path_string(rdata.data, rdata.size);

            execute_impl(json_string, path_string, res_data, res_offsets, null_map, i);
        }
    }
    static void scalar_vector(FunctionContext* context, const StringRef& ldata, const Chars& rdata,
                              const Offsets& roffsets, Chars& res_data, Offsets& res_offsets,
                              NullMap& null_map) {
        size_t input_rows_count = roffsets.size();
        res_offsets.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }
            int r_size = roffsets[i] - roffsets[i - 1];
            const auto r_raw = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);

            std::string_view json_string(ldata.data, ldata.size);
            std::string_view path_string(r_raw, r_size);

            execute_impl(json_string, path_string, res_data, res_offsets, null_map, i);
        }
    }

    static void execute_impl(const std::string_view& json_string,
                             const std::string_view& path_string, Chars& res_data,
                             Offsets& res_offsets, NullMap& null_map, size_t index_now) {
        rapidjson::Document document;
        rapidjson::Value* root = nullptr;

        root = get_json_object<JSON_FUN_STRING>(json_string, path_string, &document);
        const int max_string_len = 65535;

        if (root == nullptr || root->IsNull()) {
            StringOP::push_null_string(index_now, res_data, res_offsets, null_map);
        } else if (root->IsString()) {
            const auto ptr = root->GetString();
            size_t len = strnlen(ptr, max_string_len);
            StringOP::push_value_string(std::string_view(ptr, len), index_now, res_data,
                                        res_offsets);
        } else {
            rapidjson::StringBuffer buf;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
            root->Accept(writer);

            const auto ptr = buf.GetString();
            size_t len = strnlen(ptr, max_string_len);
            StringOP::push_value_string(std::string_view(ptr, len), index_now, res_data,
                                        res_offsets);
        }
    }
    static DataTypes get_variadic_argument_types_impl() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }
};

template <int flag>
struct JsonParser {
    //string
    static void update_value(StringParser::ParseResult& result, rapidjson::Value& value,
                             StringRef data, rapidjson::Document::AllocatorType& allocator) {
        value.SetString(data.data, data.size, allocator);
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
        value.SetBool((*data.data == '1' || *data.data == 't') ? true : false);
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
        value.SetString(data.data + 1, data.size - 2, allocator);
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

template <int flag, typename Impl>
struct ExecuteReducer {
    template <typename... TArgs>
    static void run(TArgs&&... args) {
        Impl::template execute_type<JsonParser<flag>>(std::forward<TArgs>(args)...);
    }
};

struct FunctionJsonArrayImpl {
    static constexpr auto name = "json_array";

    template <int flag>
    using Reducer = ExecuteReducer<flag, FunctionJsonArrayImpl>;

    static void execute_parse(const std::string& type_flags,
                              const std::vector<const ColumnString*>& data_columns,
                              std::vector<rapidjson::Value>& objects,
                              rapidjson::Document::AllocatorType& allocator,
                              const std::vector<const ColumnUInt8*>& nullmaps) {
        for (int i = 0; i < data_columns.size() - 1; i++) {
            constexpr_int_match<'0', '6', Reducer>::run(type_flags[i], objects, allocator,
                                                        data_columns[i], nullmaps[i]);
        }
    }

    template <typename TypeImpl>
    static void execute_type(std::vector<rapidjson::Value>& objects,
                             rapidjson::Document::AllocatorType& allocator,
                             const ColumnString* data_column, const ColumnUInt8* nullmap) {
        StringParser::ParseResult result;
        rapidjson::Value value;

        for (int i = 0; i < objects.size(); i++) {
            if (nullmap != nullptr && nullmap->get_data()[i]) {
                JsonParser<'0'>::update_value(result, value, data_column->get_data_at(i),
                                              allocator);
            } else {
                TypeImpl::update_value(result, value, data_column->get_data_at(i), allocator);
            }
            objects[i].PushBack(value, allocator);
        }
    }
};

struct FunctionJsonObjectImpl {
    static constexpr auto name = "json_object";

    template <int flag>
    using Reducer = ExecuteReducer<flag, FunctionJsonObjectImpl>;

    static void execute_parse(std::string type_flags,
                              const std::vector<const ColumnString*>& data_columns,
                              std::vector<rapidjson::Value>& objects,
                              rapidjson::Document::AllocatorType& allocator,
                              const std::vector<const ColumnUInt8*>& nullmaps) {
        for (auto& array_object : objects) {
            array_object.SetObject();
        }

        for (int i = 0; i + 1 < data_columns.size() - 1; i += 2) {
            constexpr_int_match<'0', '6', Reducer>::run(type_flags[i + 1], objects, allocator,
                                                        data_columns[i], data_columns[i + 1],
                                                        nullmaps[i + 1]);
        }
    }

    template <typename TypeImpl>
    static void execute_type(std::vector<rapidjson::Value>& objects,
                             rapidjson::Document::AllocatorType& allocator,
                             const ColumnString* key_column, const ColumnString* value_column,
                             const ColumnUInt8* nullmap) {
        StringParser::ParseResult result;
        rapidjson::Value key;
        rapidjson::Value value;
        for (int i = 0; i < objects.size(); i++) {
            JsonParser<'6'>::update_value(result, key, key_column->get_data_at(i),
                                          allocator); // key always is string
            if (nullmap != nullptr && nullmap->get_data()[i]) {
                JsonParser<'0'>::update_value(result, value, value_column->get_data_at(i),
                                              allocator);
            } else {
                TypeImpl::update_value(result, value, value_column->get_data_at(i), allocator);
            }
            objects[i].AddMember(key, value, allocator);
        }
    }
};

template <typename SpecificImpl>
class FunctionJsonAlwaysNotNullable : public IFunction {
public:
    using IFunction::execute;

    static constexpr auto name = SpecificImpl::name;

    static FunctionPtr create() {
        return std::make_shared<FunctionJsonAlwaysNotNullable<SpecificImpl>>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto result_column = ColumnString::create();

        std::vector<ColumnPtr> column_ptrs; // prevent converted column destruct
        std::vector<const ColumnString*> data_columns;
        std::vector<const ColumnUInt8*> nullmaps;
        for (int i = 0; i < arguments.size(); i++) {
            auto column = block.get_by_position(arguments[i]).column;
            column_ptrs.push_back(column->convert_to_full_column_if_const());
            const ColumnNullable* col_nullable =
                    check_and_get_column<ColumnNullable>(column_ptrs.back().get());
            if (col_nullable) {
                const ColumnUInt8* col_nullmap = check_and_get_column<ColumnUInt8>(
                        col_nullable->get_null_map_column_ptr().get());
                nullmaps.push_back(col_nullmap);
                const ColumnString* col = check_and_get_column<ColumnString>(
                        col_nullable->get_nested_column_ptr().get());
                data_columns.push_back(col);
            } else {
                nullmaps.push_back(nullptr);
                data_columns.push_back(assert_cast<const ColumnString*>(column_ptrs.back().get()));
            }
        }
        execute(data_columns, *assert_cast<ColumnString*>(result_column.get()), input_rows_count,
                nullmaps);
        block.get_by_position(result).column = std::move(result_column);
        return Status::OK();
    }

    static void execute(const std::vector<const ColumnString*>& data_columns,
                        ColumnString& result_column, size_t input_rows_count,
                        const std::vector<const ColumnUInt8*> nullmaps) {
        std::string type_flags = data_columns.back()->get_data_at(0).to_string();

        rapidjson::Document document;
        rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

        std::vector<rapidjson::Value> objects;
        for (int i = 0; i < input_rows_count; i++) {
            objects.emplace_back(rapidjson::kArrayType);
        }

        SpecificImpl::execute_parse(type_flags, data_columns, objects, allocator, nullmaps);

        rapidjson::StringBuffer buf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

        for (int i = 0; i < input_rows_count; i++) {
            buf.Clear();
            writer.Reset(buf);
            objects[i].Accept(writer);
            result_column.insert_data(buf.GetString(), buf.GetSize());
        }
    }
};

struct FunctionJsonQuoteImpl {
    static constexpr auto name = "json_quote";

    static void execute(const std::vector<const ColumnString*>& data_columns,
                        ColumnString& result_column, size_t input_rows_count) {
        rapidjson::Document document;
        rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

        rapidjson::Value value;

        rapidjson::StringBuffer buf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

        for (int i = 0; i < input_rows_count; i++) {
            StringRef data = data_columns[0]->get_data_at(i);
            value.SetString(data.data, data.size, allocator);

            buf.Clear();
            value.Accept(writer);
            result_column.insert_data(buf.GetString(), buf.GetSize());
        }
    }
};

struct FunctionJsonExtractImpl {
    static constexpr auto name = "json_extract";

    static rapidjson::Value parse_json(const ColumnString* json_col, const ColumnString* path_col,
                                       rapidjson::Document::AllocatorType& allocator,
                                       const int row) {
        rapidjson::Value value;
        rapidjson::Document document;

        const auto obj = json_col->get_data_at(row);
        std::string_view json_string(obj.data, obj.size);
        const auto path = path_col->get_data_at(row);
        std::string_view path_string(path.data, path.size);

        auto root = get_json_object<JSON_FUN_STRING>(json_string, path_string, &document);
        if (root != nullptr) {
            value.CopyFrom(*root, allocator);
        }
        return value;
    }

    static void execute(const std::vector<const ColumnString*>& data_columns,
                        ColumnString& result_column, NullMap& null_map, size_t input_rows_count) {
        rapidjson::Document document;
        rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
        rapidjson::StringBuffer buf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

        const auto json_col = data_columns[0];
        for (size_t row = 0; row < input_rows_count; row++) {
            rapidjson::Value value;
            if (data_columns.size() == 2) {
                value = parse_json(json_col, data_columns[1], allocator, row);
            } else {
                value.SetArray();
                value.Reserve(data_columns.size() - 1, allocator);
                for (size_t col = 1; col < data_columns.size(); ++col) {
                    value.PushBack(parse_json(json_col, data_columns[col], allocator, row),
                                   allocator);
                }
            }

            if (value.IsNull()) {
                null_map[row] = 1;
                result_column.insert_default();
            } else {
                // write value as string
                buf.Clear();
                writer.Reset(buf);
                value.Accept(writer);
                result_column.insert_data(buf.GetString(), buf.GetSize());
            }
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
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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
    bool use_default_implementation_for_constants() const override { return true; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto result_column = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        std::vector<ColumnPtr> column_ptrs; // prevent converted column destruct
        std::vector<const ColumnString*> data_columns;
        for (int i = 0; i < arguments.size(); i++) {
            column_ptrs.push_back(
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const());
            data_columns.push_back(assert_cast<const ColumnString*>(column_ptrs.back().get()));
        }
        Impl::execute(data_columns, *assert_cast<ColumnString*>(result_column.get()),
                      null_map->get_data(), input_rows_count);
        block.replace_by_position(
                result, ColumnNullable::create(std::move(result_column), std::move(null_map)));
        return Status::OK();
    }
};

using FunctionGetJsonDouble = FunctionBinaryStringOperateToNullType<GetJsonDouble>;
using FunctionGetJsonInt = FunctionBinaryStringOperateToNullType<GetJsonInt>;
using FunctionGetJsonBigInt = FunctionBinaryStringOperateToNullType<GetJsonBigInt>;
using FunctionGetJsonString = FunctionBinaryStringOperateToNullType<GetJsonString>;

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
                        size_t result, size_t input_rows_count) const override {
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

        auto col_to = ColumnVector<vectorized::Int32>::create();
        auto& vec_to = col_to->get_data();
        size_t size = col_from.size();
        vec_to.resize(size);

        // parser can be reused for performance
        JsonbParser parser;
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (col_from.is_null_at(i)) {
                null_map->get_data()[i] = 1;
                vec_to[i] = 0;
                continue;
            }

            const auto& val = col_from_string->get_data_at(i);
            if (parser.parse(val.data, val.size)) {
                vec_to[i] = 1;
            } else {
                vec_to[i] = 0;
            }
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(col_to), std::move(null_map)));

        return Status::OK();
    }
};

class FunctionJsonContains : public IFunction {
public:
    static constexpr auto name = "json_contains";
    static FunctionPtr create() { return std::make_shared<FunctionJsonContains>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeUInt8>());
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeString>()};
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool json_contains_object(const rapidjson::Value& target,
                              const rapidjson::Value& search_value) const {
        if (!target.IsObject() || !search_value.IsObject()) {
            return false;
        }

        for (auto itr = search_value.MemberBegin(); itr != search_value.MemberEnd(); ++itr) {
            if (!target.HasMember(itr->name) || !json_contains(target[itr->name], itr->value)) {
                return false;
            }
        }

        return true;
    }

    bool json_contains_array(const rapidjson::Value& target,
                             const rapidjson::Value& search_value) const {
        if (!target.IsArray() || !search_value.IsArray()) {
            return false;
        }

        for (auto itr = search_value.Begin(); itr != search_value.End(); ++itr) {
            bool found = false;
            for (auto target_itr = target.Begin(); target_itr != target.End(); ++target_itr) {
                if (json_contains(*target_itr, *itr)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }

        return true;
    }

    bool json_contains(const rapidjson::Value& target, const rapidjson::Value& search_value) const {
        if (target == search_value) {
            return true;
        }

        if (target.IsObject() && search_value.IsObject()) {
            return json_contains_object(target, search_value);
        }

        if (target.IsArray() && search_value.IsArray()) {
            return json_contains_array(target, search_value);
        }

        return false;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const IColumn& col_json = *(block.get_by_position(arguments[0]).column);
        const IColumn& col_search = *(block.get_by_position(arguments[1]).column);
        const IColumn& col_path = *(block.get_by_position(arguments[2]).column);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        const ColumnString* col_json_string = check_and_get_column<ColumnString>(col_json);
        const ColumnString* col_search_string = check_and_get_column<ColumnString>(col_search);
        const ColumnString* col_path_string = check_and_get_column<ColumnString>(col_path);

        if (!col_json_string || !col_search_string || !col_path_string) {
            return Status::RuntimeError("Illegal column should be ColumnString");
        }

        auto col_to = ColumnVector<vectorized::UInt8>::create();
        auto& vec_to = col_to->get_data();
        size_t size = col_json.size();
        vec_to.resize(size);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (col_json.is_null_at(i) || col_search.is_null_at(i) || col_path.is_null_at(i)) {
                null_map->get_data()[i] = 1;
                vec_to[i] = 0;
                continue;
            }

            const auto& json_val = col_json_string->get_data_at(i);
            const auto& search_val = col_search_string->get_data_at(i);
            const auto& path_val = col_path_string->get_data_at(i);

            std::string_view json_string(json_val.data, json_val.size);
            std::string_view search_string(search_val.data, search_val.size);
            std::string_view path_string(path_val.data, path_val.size);

            rapidjson::Document document;
            auto target_val = get_json_object<JSON_FUN_STRING>(json_string, path_string, &document);
            if (target_val == nullptr || target_val->IsNull()) {
                vec_to[i] = 0;
            } else {
                rapidjson::Document search_doc;
                search_doc.Parse(search_string.data(), search_string.size());
                if (json_contains(*target_val, search_doc)) {
                    vec_to[i] = 1;
                } else {
                    vec_to[i] = 0;
                }
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
                        size_t result, size_t input_rows_count) const override {
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

enum class JsonModifyType {
    JSON_INSERT = 0,
    JSON_REPLACE,
    JSON_SET,
};

struct FunctionJsonInsert {
    static constexpr auto name = "json_insert";
    static constexpr auto modify_type = JsonModifyType::JSON_INSERT;
};

struct FunctionJsonReplace {
    static constexpr auto name = "json_replace";
    static constexpr auto modify_type = JsonModifyType::JSON_REPLACE;
};
struct FunctionJsonSet {
    static constexpr auto name = "json_set";
    static constexpr auto modify_type = JsonModifyType::JSON_SET;
};

template <typename Kind>
class FunctionJsonModifyImpl : public IFunction {
private:
    // T = std::vector<std::string>
    // TODO: update RE2 to support std::vector<std::string_view>
    // if path is not a valid path expression or contains
    // a * wildcard, return runtime error.
    template <typename T>
    Status get_parsed_paths_with_status(const T& path_exprs,
                                        std::vector<JsonPath>* parsed_paths) const {
        if (UNLIKELY(path_exprs.empty())) {
            return Status::RuntimeError("json path empty function {}", get_name());
        }

        if (path_exprs[0] != "$") {
            // keep same behaviour with get_parsed_paths(),
            // '$[0]' is invalid path, '$.[0]' is valid
            return Status::RuntimeError(
                    "Invalid JSON path expression. The error is around character position 1");
        }
        parsed_paths->emplace_back("$", -1, true);

        for (int i = 1; i < path_exprs.size(); i++) {
            std::string col;
            std::string index;
            if (UNLIKELY(!RE2::FullMatch(path_exprs[i], JSON_PATTERN, &col, &index))) {
                return Status::RuntimeError(
                        "Invalid JSON path expression. The error is around character position {}",
                        i + 1);
            } else {
                int idx = -1;
                if (!index.empty()) {
                    if (index == "*") {
                        return Status::RuntimeError(
                                "In this situation, path expressions may not contain the * token");
                    } else {
                        idx = atoi(index.c_str());
                    }
                }
                parsed_paths->emplace_back(col, idx, true);
            }
        }
        return Status::OK();
    }

    Status get_parsed_path_columns(std::vector<std::vector<std::vector<JsonPath>>>& json_paths,
                                   const std::vector<const ColumnString*>& data_columns,
                                   size_t input_rows_count) const {
        for (auto col = 1; col + 1 < data_columns.size() - 1; col += 2) {
            json_paths.emplace_back(std::vector<std::vector<JsonPath>>());
            for (auto row = 0; row < input_rows_count; row++) {
                const auto path = data_columns[col]->get_data_at(row);
                std::string_view path_string(path.data, path.size);
                std::vector<JsonPath> parsed_paths;

#ifdef USE_LIBCPP
                std::string s(path_string);
                auto tok = get_json_token(s);
#else
                auto tok = get_json_token(path_string);
#endif
                std::vector<std::string> paths(tok.begin(), tok.end());
                RETURN_IF_ERROR(get_parsed_paths_with_status(paths, &parsed_paths));
                json_paths[col / 2].emplace_back(parsed_paths);
            }
        }
        return Status::OK();
    }

public:
    static constexpr auto name = Kind::name;

    static FunctionPtr create() { return std::make_shared<FunctionJsonModifyImpl<Kind>>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        bool is_nullable = false;
        // arguments: (json_str, path, val[, path, val...], type_flag)
        for (auto col = 2; col < arguments.size() - 1; col += 2) {
            if (arguments[col]->is_nullable()) {
                is_nullable = true;
                break;
            }
        }
        return is_nullable ? make_nullable(std::make_shared<DataTypeString>())
                           : std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto result_column = ColumnString::create();
        bool is_nullable = false;
        auto ret_null_map = ColumnUInt8::create(0, 0);

        std::vector<ColumnPtr> column_ptrs; // prevent converted column destruct
        std::vector<const ColumnString*> data_columns;
        std::vector<const ColumnUInt8*> nullmaps;
        for (int i = 0; i < arguments.size(); i++) {
            auto column = block.get_by_position(arguments[i]).column;
            column_ptrs.push_back(column->convert_to_full_column_if_const());
            const ColumnNullable* col_nullable =
                    check_and_get_column<ColumnNullable>(column_ptrs.back().get());
            if (col_nullable) {
                if (!is_nullable) {
                    is_nullable = true;
                    ret_null_map = ColumnUInt8::create(input_rows_count, 0);
                }
                const ColumnUInt8* col_nullmap = check_and_get_column<ColumnUInt8>(
                        col_nullable->get_null_map_column_ptr().get());
                nullmaps.push_back(col_nullmap);
                const ColumnString* col = check_and_get_column<ColumnString>(
                        col_nullable->get_nested_column_ptr().get());
                data_columns.push_back(col);
            } else {
                nullmaps.push_back(nullptr);
                data_columns.push_back(assert_cast<const ColumnString*>(column_ptrs.back().get()));
            }
        }

        RETURN_IF_ERROR(execute_process(
                data_columns, *assert_cast<ColumnString*>(result_column.get()), input_rows_count,
                nullmaps, is_nullable, *assert_cast<ColumnUInt8*>(ret_null_map.get())));

        if (is_nullable) {
            block.replace_by_position(result, ColumnNullable::create(std::move(result_column),
                                                                     std::move(ret_null_map)));
        } else {
            block.get_by_position(result).column = std::move(result_column);
        }
        return Status::OK();
    }

    Status execute_process(const std::vector<const ColumnString*>& data_columns,
                           ColumnString& result_column, size_t input_rows_count,
                           const std::vector<const ColumnUInt8*> nullmaps, bool is_nullable,
                           ColumnUInt8& ret_null_map) const {
        std::string type_flags = data_columns.back()->get_data_at(0).to_string();

        std::vector<rapidjson::Document> objects;
        for (auto row = 0; row < input_rows_count; row++) {
            objects.emplace_back(rapidjson::kNullType);
            const auto json_doc = data_columns[0]->get_data_at(row);
            std::string_view json_str(json_doc.data, json_doc.size);
            objects[row].Parse(json_str.data(), json_str.size());
            if (UNLIKELY(objects[row].HasParseError())) {
                return Status::RuntimeError("invalid json str {}: function {}", json_str,
                                            get_name());
            }
        }

        std::vector<std::vector<std::vector<JsonPath>>> json_paths;
        RETURN_IF_ERROR(get_parsed_path_columns(json_paths, data_columns, input_rows_count));

        execute_parse(type_flags, data_columns, objects, json_paths, nullmaps);

        rapidjson::StringBuffer buf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

        for (int i = 0; i < input_rows_count; i++) {
            buf.Clear();
            writer.Reset(buf);
            objects[i].Accept(writer);
            if (is_nullable && objects[i].IsNull()) {
                ret_null_map.get_data()[i] = 1;
            }
            result_column.insert_data(buf.GetString(), buf.GetSize());
        }
        return Status::OK();
    }

    template <int flag>
    using Reducer = ExecuteReducer<flag, FunctionJsonModifyImpl<Kind>>;

    static void execute_parse(std::string type_flags,
                              const std::vector<const ColumnString*>& data_columns,
                              std::vector<rapidjson::Document>& objects,
                              std::vector<std::vector<std::vector<JsonPath>>>& json_paths,
                              const std::vector<const ColumnUInt8*>& nullmaps) {
        for (auto col = 1; col + 1 < data_columns.size() - 1; col += 2) {
            constexpr_int_match<'0', '6', Reducer>::run(type_flags[col + 1], objects,
                                                        json_paths[col / 2], data_columns[col + 1],
                                                        nullmaps[col + 1]);
        }
    }

    static void modify_value(const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
                             rapidjson::Document::AllocatorType& mem_allocator, bool is_insert,
                             bool is_replace, rapidjson::Value* value) {
        rapidjson::Value* root = document;
        rapidjson::Value key;

        auto i = 1;
        for (; i < parsed_paths.size(); i++) {
            if (root->IsNull()) {
                return;
            }
            const std::string& col = parsed_paths[i].key;
            int index = parsed_paths[i].idx;
            if (LIKELY(!col.empty())) {
                if (root->IsObject()) {
                    if (!root->HasMember(col.c_str())) {
                        break;
                    } else {
                        root = &((*root)[col.c_str()]);
                    }
                } else {
                    // not object
                    return;
                }
            }
            if (UNLIKELY(index != -1)) {
                if (root->IsArray()) {
                    if (index >= root->Size()) {
                        // array append new value
                        if (is_insert && i + 1 == parsed_paths.size()) {
                            root->PushBack(*value, mem_allocator);
                        }
                        return;
                    } else {
                        root = &((*root)[index]);
                    }
                } else {
                    if (i + 1 == parsed_paths.size()) {
                        // replace, example:
                        // json_replace({"a": 1}, '$.[0]', null);
                        // output: null
                        if (is_replace && index == 0) {
                            *root = *value;
                            return;
                        }
                        // convert to array, example:
                        // json_insert({"a": 1}, '$.[1]', 3);
                        // output: [{"a": 1}, 3]
                        if (is_insert && index > 0) {
                            key.SetArray();
                            root->Swap(key);
                            root->PushBack(key, mem_allocator);
                            root->PushBack(*value, mem_allocator);
                        }
                    }
                    return;
                }
            }
        }

        if (is_insert && i + 1 == parsed_paths.size()) {
            if (LIKELY(root->IsObject())) {
                // object insert new value
                const std::string& col = parsed_paths[i].key;
                int index = parsed_paths[i].idx;
                if (LIKELY(!col.empty() && index == -1)) {
                    key.SetString(col.c_str(), mem_allocator);
                    root->AddMember(key, *value, mem_allocator);
                }
            }
        } else if (is_replace && i == parsed_paths.size()) {
            *root = *value;
        }
    }

    template <typename TypeImpl>
    static void execute_type(std::vector<rapidjson::Document>& objects,
                             std::vector<std::vector<JsonPath>>& paths_column,
                             const ColumnString* value_column, const ColumnUInt8* nullmap) {
        StringParser::ParseResult result;
        rapidjson::Value value;
        for (auto row = 0; row < objects.size(); row++) {
            std::vector<JsonPath>* parsed_paths = &paths_column[row];

            if (nullmap != nullptr && nullmap->get_data()[row]) {
                JsonParser<'0'>::update_value(result, value, value_column->get_data_at(row),
                                              objects[row].GetAllocator());
            } else {
                TypeImpl::update_value(result, value, value_column->get_data_at(row),
                                       objects[row].GetAllocator());
            }

            switch (Kind::modify_type) {
            case JsonModifyType::JSON_INSERT:
                modify_value(*parsed_paths, &objects[row], objects[row].GetAllocator(), true, false,
                             &value);
                break;
            case JsonModifyType::JSON_REPLACE:
                modify_value(*parsed_paths, &objects[row], objects[row].GetAllocator(), false, true,
                             &value);
                break;
            case JsonModifyType::JSON_SET:
                modify_value(*parsed_paths, &objects[row], objects[row].GetAllocator(), true, true,
                             &value);
                break;
            }
        }
    }
};

void register_function_json(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionGetJsonInt>();
    factory.register_function<FunctionGetJsonBigInt>();
    factory.register_function<FunctionGetJsonDouble>();
    factory.register_function<FunctionGetJsonString>();
    factory.register_function<FunctionJsonUnquote>();

    factory.register_function<FunctionJsonAlwaysNotNullable<FunctionJsonArrayImpl>>();
    factory.register_function<FunctionJsonAlwaysNotNullable<FunctionJsonObjectImpl>>();
    factory.register_function<FunctionJson<FunctionJsonQuoteImpl>>();
    factory.register_function<FunctionJsonNullable<FunctionJsonExtractImpl>>();

    factory.register_function<FunctionJsonValid>();
    factory.register_function<FunctionJsonContains>();

    factory.register_function<FunctionJsonModifyImpl<FunctionJsonInsert>>();
    factory.register_function<FunctionJsonModifyImpl<FunctionJsonReplace>>();
    factory.register_function<FunctionJsonModifyImpl<FunctionJsonSet>>();
}

} // namespace doris::vectorized
