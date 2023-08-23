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

#include "exprs/json_functions.h"

#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <re2/re2.h>
#include <simdjson/simdjson.h> // IWYU pragma: keep
#include <stdlib.h>

#include <boost/iterator/iterator_facade.hpp>
#include <boost/token_functions.hpp>
#include <boost/tokenizer.hpp>
#include <sstream>
#include <string>
#include <vector>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"

namespace doris {

// static const re2::RE2 JSON_PATTERN("^([a-zA-Z0-9_\\-\\:\\s#\\|\\.]*)(?:\\[([0-9]+)\\])?");
// json path cannot contains: ", [, ]
static const re2::RE2 JSON_PATTERN("^([^\\\"\\[\\]]*)(?:\\[([0-9]+|\\*)\\])?");

rapidjson::Value* JsonFunctions::match_value(const std::vector<JsonPath>& parsed_paths,
                                             rapidjson::Value* document,
                                             rapidjson::Document::AllocatorType& mem_allocator,
                                             bool is_insert_null) {
    rapidjson::Value* root = document;
    rapidjson::Value* array_obj = nullptr;
    for (int i = 1; i < parsed_paths.size(); i++) {
        VLOG_TRACE << "parsed_paths: " << parsed_paths[i].debug_string();

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
                // root is not a nested type, return nullptr
                return nullptr;
            }
        }

        if (UNLIKELY(index != -1)) {
            // judge the rapidjson:Value, which base the top's result,
            // if not array return nullptr;else get the index value from the array
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

rapidjson::Value* JsonFunctions::get_json_array_from_parsed_json(
        const std::string& json_path, rapidjson::Value* document,
        rapidjson::Document::AllocatorType& mem_allocator, bool* wrap_explicitly) {
    std::vector<JsonPath> vec;
    parse_json_paths(json_path, &vec);
    return get_json_array_from_parsed_json(vec, document, mem_allocator, wrap_explicitly);
}

rapidjson::Value* JsonFunctions::get_json_array_from_parsed_json(
        const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
        rapidjson::Document::AllocatorType& mem_allocator, bool* wrap_explicitly) {
    *wrap_explicitly = false;
    if (!parsed_paths[0].is_valid) {
        return nullptr;
    }

    if (parsed_paths.size() == 1) {
        // the json path is "$", just return entire document
        // wrapper an array
        rapidjson::Value* array_obj = nullptr;
        array_obj = static_cast<rapidjson::Value*>(mem_allocator.Malloc(sizeof(rapidjson::Value)));
        array_obj->SetArray();
        array_obj->PushBack(*document, mem_allocator);
        return array_obj;
    }

    rapidjson::Value* root = match_value(parsed_paths, document, mem_allocator, true);
    if (root == nullptr || root == document) { // not found
        return nullptr;
    } else if (!root->IsArray() && wrap_explicitly) {
        rapidjson::Value* array_obj = nullptr;
        array_obj = static_cast<rapidjson::Value*>(mem_allocator.Malloc(sizeof(rapidjson::Value)));
        array_obj->SetArray();
        rapidjson::Value copy;
        copy.CopyFrom(*root, mem_allocator);
        array_obj->PushBack(std::move(copy), mem_allocator);
        // set `wrap_explicitly` to true, so that the caller knows that this Array is wrapped actively.
        *wrap_explicitly = true;
        return array_obj;
    }
    return root;
}

rapidjson::Value* JsonFunctions::get_json_object_from_parsed_json(
        const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
        rapidjson::Document::AllocatorType& mem_allocator) {
    if (!parsed_paths[0].is_valid) {
        return nullptr;
    }

    if (parsed_paths.size() == 1) {
        // the json path is "$", just return entire document
        return document;
    }

    rapidjson::Value* root = match_value(parsed_paths, document, mem_allocator, true);
    if (root == nullptr || root == document) { // not found
        return nullptr;
    }
    return root;
}

void JsonFunctions::parse_json_paths(const std::string& path_string,
                                     std::vector<JsonPath>* parsed_paths) {
    // split path by ".", and escape quota by "\"
    // eg:
    //    '$.text#abc.xyz'  ->  [$, text#abc, xyz]
    //    '$."text.abc".xyz'  ->  [$, text.abc, xyz]
    //    '$."text.abc"[1].xyz'  ->  [$, text.abc[1], xyz]
    boost::tokenizer<boost::escaped_list_separator<char>> tok(
            path_string, boost::escaped_list_separator<char>("\\", ".", "\""));
    std::vector<std::string> paths(tok.begin(), tok.end());
    get_parsed_paths(paths, parsed_paths);
}

void JsonFunctions::get_parsed_paths(const std::vector<std::string>& path_exprs,
                                     std::vector<JsonPath>* parsed_paths) {
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
            parsed_paths->emplace_back(std::move(col), idx, true);
        }
    }
}

Status JsonFunctions::extract_from_object(simdjson::ondemand::object& obj,
                                          const std::vector<JsonPath>& jsonpath,
                                          simdjson::ondemand::value* value) noexcept {
// Return DataQualityError when it's a malformed json.
// Otherwise the path was not found, due to array out of bound or not exist
#define HANDLE_SIMDJSON_ERROR(err, msg)                                                        \
    do {                                                                                       \
        const simdjson::error_code& _err = err;                                                \
        const std::string& _msg = msg;                                                         \
        if (UNLIKELY(_err)) {                                                                  \
            if (_err == simdjson::NO_SUCH_FIELD || _err == simdjson::INDEX_OUT_OF_BOUNDS) {    \
                return Status::NotFound(                                                       \
                        fmt::format("err: {}, msg: {}", simdjson::error_message(_err), _msg)); \
            }                                                                                  \
            return Status::DataQualityError(                                                   \
                    fmt::format("err: {}, msg: {}", simdjson::error_message(_err), _msg));     \
        }                                                                                      \
    } while (false);

    if (jsonpath.size() <= 1) {
        // The first elem of json path should be '$'.
        // A valid json path's size is >= 2.
        return Status::DataQualityError("empty json path");
    }

    simdjson::ondemand::value tvalue;

    // Skip the first $.
    for (int i = 1; i < jsonpath.size(); i++) {
        if (UNLIKELY(!jsonpath[i].is_valid)) {
            return Status::DataQualityError(fmt::format("invalid json path: {}", jsonpath[i].key));
        }

        const std::string& col = jsonpath[i].key;
        int index = jsonpath[i].idx;

        // Since the simdjson::ondemand::object cannot be converted to simdjson::ondemand::value,
        // we have to do some special treatment for the second elem of json path.
        // If the key is not found in json object, simdjson::NO_SUCH_FIELD would be returned.
        if (i == 1) {
            HANDLE_SIMDJSON_ERROR(obj.find_field_unordered(col).get(tvalue),
                                  fmt::format("unable to find field: {}", col));
        } else {
            HANDLE_SIMDJSON_ERROR(tvalue.find_field_unordered(col).get(tvalue),
                                  fmt::format("unable to find field: {}", col));
        }

        // TODO support [*] which idex == -2
        if (index != -1) {
            // try to access tvalue as array.
            // If the index is beyond the length of array, simdjson::INDEX_OUT_OF_BOUNDS would be returned.
            simdjson::ondemand::array arr;
            HANDLE_SIMDJSON_ERROR(tvalue.get_array().get(arr),
                                  fmt::format("failed to access field as array, field: {}", col));

            HANDLE_SIMDJSON_ERROR(
                    arr.at(index).get(tvalue),
                    fmt::format("failed to access array field: {}, index: {}", col, index));
        }
    }

    std::swap(*value, tvalue);

    return Status::OK();
}

std::string JsonFunctions::print_json_value(const rapidjson::Value& value) {
    rapidjson::StringBuffer buffer;
    buffer.Clear();
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return std::string(buffer.GetString());
}

void JsonFunctions::merge_objects(rapidjson::Value& dst_object, rapidjson::Value& src_object,
                                  rapidjson::Document::AllocatorType& allocator) {
    if (!src_object.IsObject()) {
        return;
    }
    for (auto src_it = src_object.MemberBegin(); src_it != src_object.MemberEnd(); ++src_it) {
        auto dst_it = dst_object.FindMember(src_it->name);
        if (dst_it != dst_object.MemberEnd()) {
            if (src_it->value.IsObject()) {
                merge_objects(dst_it->value, src_it->value, allocator);
            } else {
                if (dst_it->value.IsNull()) {
                    dst_it->value = src_it->value;
                }
            }
        } else {
            dst_object.AddMember(src_it->name, src_it->value, allocator);
        }
    }
}

} // namespace doris
