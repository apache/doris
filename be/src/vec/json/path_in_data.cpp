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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/Serializations/PathInData.cpp
// and modified by Doris

#include "vec/json/path_in_data.h"

#include <assert.h>

#include "vec/common/sip_hash.h"

namespace doris::vectorized {
PathInData::PathInData(std::string_view path_) : path(path_) {
    const char* begin = path.data();
    const char* end = path.data() + path.size();
    for (const char* it = path.data(); it != end; ++it) {
        if (*it == '.') {
            size_t size = static_cast<size_t>(it - begin);
            parts.emplace_back(std::string_view {begin, size}, false, 0);
            begin = it + 1;
        }
    }
    size_t size = static_cast<size_t>(end - begin);
    parts.emplace_back(std::string_view {begin, size}, false, 0.);
}
PathInData::PathInData(const Parts& parts_) {
    build_path(parts_);
    build_parts(parts_);
}
PathInData::PathInData(const PathInData& other) : path(other.path) {
    build_parts(other.get_parts());
}
PathInData& PathInData::operator=(const PathInData& other) {
    if (this != &other) {
        path = other.path;
        build_parts(other.parts);
    }
    return *this;
}
UInt128 PathInData::get_parts_hash(const Parts& parts_) {
    SipHash hash;
    hash.update(parts_.size());
    for (const auto& part : parts_) {
        hash.update(part.key.data(), part.key.length());
        hash.update(part.is_nested);
        hash.update(part.anonymous_array_level);
    }
    UInt128 res;
    hash.get128(res);
    return res;
}

void PathInData::build_path(const Parts& other_parts) {
    if (other_parts.empty()) {
        return;
    }
    path.clear();
    auto it = other_parts.begin();
    path += it->key;
    ++it;
    for (; it != other_parts.end(); ++it) {
        path += ".";
        path += it->key;
    }
}
void PathInData::build_parts(const Parts& other_parts) {
    if (other_parts.empty()) {
        return;
    }
    parts.clear();
    parts.reserve(other_parts.size());
    const char* begin = path.data();
    for (const auto& part : other_parts) {
        has_nested |= part.is_nested;
        parts.emplace_back(std::string_view {begin, part.key.length()}, part.is_nested,
                           part.anonymous_array_level);
        begin += part.key.length() + 1;
    }
}
size_t PathInData::Hash::operator()(const PathInData& value) const {
    auto hash = get_parts_hash(value.parts);
    return hash.low ^ hash.high;
}
PathInDataBuilder& PathInDataBuilder::append(std::string_view key, bool is_array) {
    if (parts.empty()) {
        current_anonymous_array_level += is_array;
    }
    if (!key.empty()) {
        if (!parts.empty()) {
            parts.back().is_nested = is_array;
        }
        parts.emplace_back(key, false, current_anonymous_array_level);
        current_anonymous_array_level = 0;
    }
    return *this;
}
PathInDataBuilder& PathInDataBuilder::append(const PathInData::Parts& path, bool is_array) {
    if (parts.empty()) {
        current_anonymous_array_level += is_array;
    }
    if (!path.empty()) {
        if (!parts.empty()) {
            parts.back().is_nested = is_array;
        }
        auto it = parts.insert(parts.end(), path.begin(), path.end());
        for (; it != parts.end(); ++it) {
            it->anonymous_array_level += current_anonymous_array_level;
        }
        current_anonymous_array_level = 0;
    }
    return *this;
}
void PathInDataBuilder::pop_back() {
    parts.pop_back();
}
void PathInDataBuilder::pop_back(size_t n) {
    assert(n <= parts.size());
    parts.resize(parts.size() - n);
}
} // namespace doris::vectorized
