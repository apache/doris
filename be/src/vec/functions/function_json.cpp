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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "exprs/json_functions.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
static const re2::RE2 JSON_PATTERN("^([^\\\"\\[\\]]*)(?:\\[([0-9]+|\\*)\\])?");

template <typename T, typename U>
void char_split(std::vector<T>& res, const U& var, char p) {
    int start = 0;
    int pos = start;
    int end = var.length();
    while (pos < end) {
        while (var[pos] != p && pos < end) pos++;
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
rapidjson::Value* get_json_object(const std::string_view& json_string,
                                  const std::string_view& path_string,
                                  rapidjson::Document* document) {
    std::vector<JsonPath>* parsed_paths;
    std::vector<JsonPath> tmp_parsed_paths;

    // TODO: update RE2 to support RE2
    // std::vector<std::string_view> paths;
    std::vector<std::string> paths;
    std::string pt(path_string.data(), path_string.length());
    char_split(paths, path_string, '.');

    get_parsed_paths(paths, &tmp_parsed_paths);
    parsed_paths = &tmp_parsed_paths;

    if (!(*parsed_paths)[0].is_valid) {
        return document;
    }

    if (UNLIKELY((*parsed_paths).size() == 1)) {
        if (fntype == JSON_FUN_STRING) {
            document->SetString(json_string.data(), document->GetAllocator());
        } else {
            return document;
        }
    }

    document->Parse(json_string.data());
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
    static void vector_vector(const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, Container& res,
                              NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1] - 1;

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1] - 1;

            if (null_map[i]) {
                res[i] = 0;
                continue;
            }

            std::string_view json_string(l_raw_str, l_str_size);
            std::string_view path_string(r_raw_str, r_str_size);

            rapidjson::Document document;
            rapidjson::Value* root = nullptr;

            if constexpr (std::is_same_v<double, typename NumberType::T>) {
                root = get_json_object<JSON_FUN_DOUBLE>(json_string, path_string, &document);
                handle_result<double>(root, res[i], null_map[i]);
            } else if constexpr (std::is_same_v<int32_t, typename NumberType::T>) {
                root = get_json_object<JSON_FUN_DOUBLE>(json_string, path_string, &document);
                handle_result<int32_t>(root, res[i], null_map[i]);
            }
        }
    }

    template <typename T, std::enable_if_t<std::is_same_v<double, T>, T>* = nullptr>
    static void handle_result(rapidjson::Value* root, T& res, uint8_t& res_null) {
        if (root == nullptr || root->IsNull()) {
            res = 0;
            res_null = 1;
        } else if (root->IsInt()) {
            res = root->GetInt();
        } else if (root->IsDouble()) {
            res = root->GetDouble();
        } else {
            res = 1;
        }
    }

    template <typename T, std::enable_if_t<std::is_same_v<int32_t, T>, T>* = nullptr>
    static void handle_result(rapidjson::Value* root, int32_t& res, uint8_t& res_null) {
        if (root != nullptr && root->IsInt()) {
            res = root->GetInt();
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

struct GetJsonString {
    static constexpr auto name = "get_json_string";
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;
    using Chars = ColumnString::Chars;
    using Offsets = ColumnString::Offsets;
    static void vector_vector(const Chars& ldata, const Offsets& loffsets, const Chars& rdata,
                              const Offsets& roffsets, Chars& res_data, Offsets& res_offsets,
                              NullMap& null_map) {
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            int l_size = loffsets[i] - loffsets[i - 1] - 1;
            const auto l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);

            int r_size = roffsets[i] - roffsets[i - 1] - 1;
            const auto r_raw = reinterpret_cast<const char*>(&rdata[loffsets[i - 1]]);

            if (null_map[i]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }

            std::string_view json_string(l_raw, l_size);
            std::string_view path_string(r_raw, r_size);

            rapidjson::Document document;
            rapidjson::Value* root = nullptr;

            root = get_json_object<JSON_FUN_STRING>(json_string, path_string, &document);
            const int max_string_len = 65535;

            if (root == nullptr || root->IsNull()) {
                null_map[i] = 1;
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
            } else if (root->IsString()) {
                const auto ptr = root->GetString();
                size_t len = strnlen(ptr, max_string_len);
                StringOP::push_value_string(std::string_view(ptr, len), i, res_data, res_offsets);
            } else {
                rapidjson::StringBuffer buf;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
                root->Accept(writer);

                const auto ptr = buf.GetString();
                size_t len = strnlen(ptr, max_string_len);
                StringOP::push_value_string(std::string_view(ptr, len), i, res_data, res_offsets);
            }
        }
    }
};

using FunctionGetJsonDouble = FunctionBinaryStringOperateToNullType<GetJsonDouble>;
using FunctionGetJsonInt = FunctionBinaryStringOperateToNullType<GetJsonInt>;
using FunctionGetJsonString = FunctionBinaryStringOperateToNullType<GetJsonString>;

void register_function_json(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionGetJsonInt>();
    factory.register_function<FunctionGetJsonDouble>();
    factory.register_function<FunctionGetJsonString>();
}

} // namespace doris::vectorized