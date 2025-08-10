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
#include <memory>
#include <regex>
#include <unordered_map>
#include <utility>

#include "common/exception.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

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

    std::string get_string(const std::string& key) const {
        auto it = _args.find(key);
        if (it != _args.end()) {
            return it->second;
        }
        return "";
    }

    std::vector<std::string> get_entry_list(const std::string& key) const {
        std::vector<std::string> lists;
        auto it = _args.find(key);
        if (it != _args.end()) {
            static std::regex pattern(R"(\[([^\]]+)\])");
            std::smatch match;
            std::sregex_iterator iter(it->second.begin(), it->second.end(), pattern);
            std::sregex_iterator end;
            for (; iter != end; ++iter) {
                if (iter->size() > 1) {
                    lists.emplace_back((*iter)[1].str());
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

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index