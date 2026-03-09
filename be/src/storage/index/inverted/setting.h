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

#include <unicode/utf8.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/regex.hpp>
#include <unordered_map>
#include <utility>

#include "common/exception.h"

namespace doris::segment_v2::inverted_index {

class Settings {
public:
    Settings() = default;
    Settings(std::unordered_map<std::string, std::string> args) : _args(std::move(args)) {}
    Settings(const Settings&) = default;
    Settings(Settings&&) = default;
    ~Settings() = default;

    void set(const std::string& key, const std::string& value) {
        _args.insert_or_assign(key, value);
    }

    bool empty() const { return _args.empty(); }

    bool get_bool(const std::string& key, bool default_value) const {
        auto it = _args.find(key);
        if (it != _args.end()) {
            std::string value = it->second;
            std::transform(value.begin(), value.end(), value.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            if (value == "true" || value == "1") {
                return true;
            } else if (value == "false" || value == "0") {
                return false;
            }
        }
        return default_value;
    }

    int32_t get_int(const std::string& key, int32_t default_value) const {
        auto it = _args.find(key);
        if (it != _args.end()) {
            try {
                size_t pos;
                int32_t num = std::stoi(it->second, &pos);
                if (pos == it->second.size()) {
                    return num;
                }
            } catch (...) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "stoi failed (invalid argument or out of range): " + it->second);
            }
        }
        return default_value;
    }

    std::string get_string(const std::string& key, const std::string& default_value = "") const {
        auto it = _args.find(key);
        if (it != _args.end()) {
            return it->second;
        }
        return default_value;
    }

    std::vector<std::string> get_entry_list(const std::string& key) const {
        static const boost::regex sep(R"((?<=\])\s*,\s*(?=\[))");
        std::vector<std::string> lists;
        auto it = _args.find(key);
        if (it != _args.end()) {
            std::string trimmed_input = boost::algorithm::trim_copy(it->second);
            if (trimmed_input.empty()) {
                return lists;
            }

            auto validate_single = [&](const std::string& item, const std::string& prefix) {
                if (item.size() < 2 || item.front() != '[' || item.back() != ']') {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    prefix + key + " must be enclosed in []");
                }
                int depth = 0;
                for (size_t i = 0; i + 1 < item.size(); ++i) {
                    char c = item[i];
                    if (c == '[') {
                        ++depth;
                    } else if (c == ']') {
                        --depth;
                        if (depth == 0) {
                            throw Exception(ErrorCode::INVALID_ARGUMENT,
                                            prefix + key + " must be enclosed in []");
                        }
                    }
                }
            };

            if (boost::regex_search(trimmed_input, sep)) {
                boost::sregex_token_iterator regex_it(trimmed_input.begin(), trimmed_input.end(),
                                                      sep, -1);
                boost::sregex_token_iterator end;
                for (; regex_it != end; ++regex_it) {
                    std::string item = boost::algorithm::trim_copy(regex_it->str());
                    validate_single(item, "Each item in ");
                    std::string content = item.substr(1, item.size() - 2);
                    if (!content.empty()) {
                        lists.emplace_back(content);
                    }
                }
            } else {
                if (trimmed_input.size() < 2 || trimmed_input.front() != '[' ||
                    trimmed_input.back() != ']') {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "Item in " + key + " must be enclosed in []");
                }
                std::string content = trimmed_input.substr(1, trimmed_input.size() - 2);
                if (!content.empty()) {
                    lists.emplace_back(content);
                }
            }
        }
        return lists;
    }

    std::unordered_set<std::string> get_word_set(const std::string& key) const {
        std::unordered_set<std::string> sets;
        auto it = _args.find(key);
        if (it != _args.end()) {
            std::vector<std::string> lists;
            boost::split(lists, it->second, boost::is_any_of(","));
            for (auto& str : lists) {
                boost::trim(str);
                if (!str.empty()) {
                    sets.insert(str);
                }
            }
        }
        return sets;
    }

    std::string to_string() {
        std::string result;
        for (const auto& [key, value] : _args) {
            if (!result.empty()) {
                result += ", ";
            }
            result += key + "=" + value;
        }
        return result;
    }

private:
    std::unordered_map<std::string, std::string> _args;
};

} // namespace doris::segment_v2::inverted_index