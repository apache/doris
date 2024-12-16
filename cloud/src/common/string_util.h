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

#include <string>
#include <vector>

namespace doris::cloud {

// Removes any trailing charactor in `to_drop` in `str`
static inline void strip_trailing(std::string& str, std::string_view to_drop) {
    str.erase(str.find_last_not_of(to_drop) + 1);
}

// Removes any leading charactor in `to_drop` in `str`
static inline void strip_leading(std::string& str, std::string_view to_drop) {
    str.erase(0, str.find_first_not_of(to_drop));
}

static inline void trim(std::string& str) {
    constexpr std::string_view drop = "/ \t";
    strip_trailing(str, drop);
    strip_leading(str, drop);
}

static inline std::vector<std::string> split(const std::string& str, const char delim) {
    std::vector<std::string> result;
    size_t start = 0;
    size_t pos = str.find(delim);
    while (pos != std::string::npos) {
        if (pos > start) {
            result.push_back(str.substr(start, pos - start));
        }
        start = pos + 1;
        pos = str.find(delim, start);
    }

    if (start < str.length()) result.push_back(str.substr(start));

    return result;
}

} // namespace doris::cloud
