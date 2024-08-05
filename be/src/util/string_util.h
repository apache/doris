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

#include <strings.h>

#include <algorithm>
#include <boost/token_functions.hpp>
#include <boost/tokenizer.hpp>
#include <cctype>
#include <cstddef>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/exception.h"
#include "common/status.h"

namespace doris {

inline std::string to_lower(const std::string& input) {
    std::string output;
    output.resize(input.size());
    std::transform(input.begin(), input.end(), output.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return output;
}

inline std::string to_upper(const std::string& input) {
    std::string output;
    output.resize(input.size());
    std::transform(input.begin(), input.end(), output.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return output;
}

inline bool iequal(const std::string& lhs, const std::string& rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    return to_lower(lhs) == to_lower(rhs);
}

inline bool starts_with(const std::string& value, const std::string& beginning) {
    return value.find(beginning) == 0;
}

inline bool ends_with(std::string const& value, std::string const& ending) {
    if (ending.size() > value.size()) {
        return false;
    }
    return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

inline std::vector<std::string> split(const std::string& s, const std::string& delim) {
    std::vector<std::string> out;
    size_t pos {};

    for (size_t find = 0; (find = s.find(delim, pos)) != std::string::npos;
         pos = find + delim.size()) {
        out.emplace_back(s.data() + pos, s.data() + find);
    }

    out.emplace_back(s.data() + pos, s.data() + s.size());
    return out;
}

template <typename T>
std::string join(const std::vector<T>& elems, const std::string& delim) {
    std::stringstream ss;
    for (size_t i = 0; i < elems.size(); ++i) {
        if (i != 0) {
            ss << delim.c_str();
        }
        ss << elems[i];
    }
    return ss.str();
}

struct StringCaseHasher {
public:
    std::size_t operator()(const std::string& value) const {
        std::string lower_value = to_lower(value);
        return std::hash<std::string>()(lower_value);
    }
};

struct StringCaseEqual {
public:
    bool operator()(const std::string& lhs, const std::string& rhs) const {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        return strncasecmp(lhs.c_str(), rhs.c_str(), lhs.size()) == 0;
    }
};

struct StringCaseLess {
public:
    bool operator()(const std::string& lhs, const std::string& rhs) const {
        size_t common_size = std::min(lhs.size(), rhs.size());
        auto cmp = strncasecmp(lhs.c_str(), rhs.c_str(), common_size);
        if (cmp == 0) {
            return lhs.size() < rhs.size();
        }
        return cmp < 0;
    }
};

size_t hash_of_path(const std::string& identifier, const std::string& path);

using StringCaseSet = std::set<std::string, StringCaseLess>;
using StringCaseUnorderedSet = std::unordered_set<std::string, StringCaseHasher, StringCaseEqual>;
template <class T>
using StringCaseMap = std::map<std::string, T, StringCaseLess>;
template <class T>
using StringCaseUnorderedMap =
        std::unordered_map<std::string, T, StringCaseHasher, StringCaseEqual>;

template <typename T>
auto get_json_token(T& path_string) {
    try {
        return boost::tokenizer<boost::escaped_list_separator<char>>(
                path_string, boost::escaped_list_separator<char>("\\", ".", "\""));
    } catch (const boost::escaped_list_error& err) {
        throw doris::Exception(ErrorCode::INVALID_JSON_PATH, "meet error {}", err.what());
    }
}

#ifdef USE_LIBCPP
template <>
auto get_json_token(std::string_view& path_string) = delete;
#endif

} // namespace doris
