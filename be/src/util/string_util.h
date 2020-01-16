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

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp> // to_lower_copy
#include <boost/functional/hash.hpp>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace doris {

struct StringCaseHasher {
public:
    std::size_t operator()(const std::string& value) const {
        std::string lower_value = boost::algorithm::to_lower_copy(value);
        return std::hash<std::string>()(lower_value);
    }
};

struct StringCaseEqual {
public:
    bool operator()(const std::string& lhs, const std::string& rhs) const {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        return strncasecmp(lhs.c_str(), rhs.c_str(), 0) == 0;
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

} // namespace doris
