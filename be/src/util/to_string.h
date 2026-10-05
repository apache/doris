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
//

#pragma once

#include <absl/strings/ascii.h>
#include <fmt/compile.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <cfloat>
#include <map>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

namespace doris {

template <typename T>
std::string to_string(const T& t) {
    return fmt::format("{}", t);
}

template <typename K, typename V>
std::string to_string(const std::map<K, V>& m);

template <typename T>
std::string to_string(const std::set<T>& s);

template <typename T>
std::string to_string(const std::vector<T>& t);

template <typename K, typename V>
std::string to_string(const typename std::pair<K, V>& v) {
    return fmt::format("{}: {}", to_string(v.first), to_string(v.second));
}

template <typename T>
std::string to_string(const T& beg, const T& end) {
    std::string out;
    for (T it = beg; it != end; ++it) {
        if (it != beg) out += ", ";
        out += to_string(*it);
    }
    return out;
}

template <typename T>
std::string to_string(const std::vector<T>& t) {
    return "[" + to_string(t.begin(), t.end()) + "]";
}

template <typename K, typename V>
std::string to_string(const std::map<K, V>& m) {
    return "{" + to_string(m.begin(), m.end()) + "}";
}

template <typename T>
std::string to_string(const std::set<T>& s) {
    return "{" + to_string(s.begin(), s.end()) + "}";
}

} // namespace doris
