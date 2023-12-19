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

#pragma once

#include <fmt/format.h>
#include <rapidjson/document.h>
#include <simdjson.h> // IWYU pragma: keep

#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"

namespace simdjson {
namespace fallback {
namespace ondemand {
class object;
class value;
} // namespace ondemand
} // namespace fallback
} // namespace simdjson

namespace doris {

enum JsonFunctionType {
    JSON_FUN_INT = 0,
    JSON_FUN_DOUBLE,
    JSON_FUN_STRING,

    JSON_FUN_UNKNOWN //The last
};

struct JsonPath {
    std::string key; // key of a json object
    int idx;         // array index of a json array, -1 means not set, -2 means *
    bool is_valid;   // true if the path is successfully parsed

    JsonPath(const std::string& key_, int idx_, bool is_valid_)
            : key(key_), idx(idx_), is_valid(is_valid_) {}

    JsonPath(std::string&& key_, int idx_, bool is_valid_)
            : key(std::move(key_)), idx(idx_), is_valid(is_valid_) {}

    std::string to_string() const {
        std::stringstream ss;
        if (!is_valid) {
            return "INVALID";
        }
        if (!key.empty()) {
            ss << key;
        }
        if (idx == -2) {
            ss << "[*]";
        } else if (idx > -1) {
            ss << "[" << idx << "]";
        }
        return ss.str();
    }

    std::string debug_string() const {
        return fmt::format("key:{}, idx:{}, valid:{}", key, idx, is_valid);
    }
};

class JsonFunctions {
public:
    /**
     * The `document` parameter must be has parsed.
     * return Value Is Array object
     * wrap_explicitly is set to true when the returned Array is wrapped actively.
     */
    static rapidjson::Value* get_json_array_from_parsed_json(
            const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
            rapidjson::Document::AllocatorType& mem_allocator, bool* wrap_explicitly);

    // this is only for test, it will parse the json path inside,
    // so that we can easily pass a json path as string.
    static rapidjson::Value* get_json_array_from_parsed_json(
            const std::string& jsonpath, rapidjson::Value* document,
            rapidjson::Document::AllocatorType& mem_allocator, bool* wrap_explicitly);

    static rapidjson::Value* get_json_object_from_parsed_json(
            const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
            rapidjson::Document::AllocatorType& mem_allocator);

    static void parse_json_paths(const std::string& path_strings,
                                 std::vector<JsonPath>* parsed_paths);
    // extract_from_object extracts value from object according to the json path.
    // Now, we do not support complete functions of json path.
    // Eg. city[*].id is not supported in this function
    static Status extract_from_object(simdjson::ondemand::object& obj,
                                      const std::vector<JsonPath>& jsonpath,
                                      simdjson::ondemand::value* value) noexcept;
    // src:    {"a" : "b" {"c" : 1}, "e" : 123}
    // dst:    {"a" : "b" {"d" : 1}}
    // merged: {"a" : "b" : {"c" : 1, "d" : 1}, "e" : 123}
    static void merge_objects(rapidjson::Value& dst_object, rapidjson::Value& src_object,
                              rapidjson::Document::AllocatorType& allocator);

    static std::string print_json_value(const rapidjson::Value& value);

private:
    static rapidjson::Value* match_value(const std::vector<JsonPath>& parsed_paths,
                                         rapidjson::Value* document,
                                         rapidjson::Document::AllocatorType& mem_allocator,
                                         bool is_insert_null = false);
    static void get_parsed_paths(const std::vector<std::string>& path_exprs,
                                 std::vector<JsonPath>* parsed_paths);
};
} // namespace doris
