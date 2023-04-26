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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/Serializations/PathInData.h
// and modified by Doris

#pragma once

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "vec/common/uint128.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

namespace doris::vectorized {

/// Class that represents path in document, e.g. JSON.
class PathInData {
public:
    struct Part {
        Part() = default;
        Part(std::string_view key_, bool is_nested_, vectorized::UInt8 anonymous_array_level_)
                : key(key_), is_nested(is_nested_), anonymous_array_level(anonymous_array_level_) {}
        bool operator==(const Part& other) const {
            return this->key == other.key && this->is_nested == other.is_nested &&
                   this->anonymous_array_level == other.anonymous_array_level;
        }
        /// Name of part of path.
        std::string_view key;
        /// If this part is Nested, i.e. element
        /// related to this key is the array of objects.
        bool is_nested = false;
        /// Number of array levels between current key and previous key.
        /// E.g. in JSON {"k1": [[[{"k2": 1, "k3": 2}]]]}
        /// "k1" is nested and has anonymous_array_level = 0.
        /// "k2" and "k3" are not nested and have anonymous_array_level = 2.
        UInt8 anonymous_array_level = 0;
    };
    using Parts = std::vector<Part>;
    PathInData() = default;
    explicit PathInData(std::string_view path_);
    explicit PathInData(const Parts& parts_);
    PathInData(const PathInData& other);
    PathInData& operator=(const PathInData& other);
    static UInt128 get_parts_hash(const Parts& parts_);
    bool empty() const { return parts.empty(); }
    const vectorized::String& get_path() const { return path; }
    const Parts& get_parts() const { return parts; }
    bool is_nested(size_t i) const { return parts[i].is_nested; }
    bool has_nested_part() const { return has_nested; }
    bool operator==(const PathInData& other) const { return parts == other.parts; }
    struct Hash {
        size_t operator()(const PathInData& value) const;
    };

private:
    /// Creates full path from parts.
    void build_path(const Parts& other_parts);
    /// Creates new parts full from full path with correct string pointers.
    void build_parts(const Parts& other_parts);
    /// The full path. Parts are separated by dots.
    vectorized::String path;
    /// Parts of the path. All string_view-s in parts must point to the @path.
    Parts parts;
    /// True if at least one part is nested.
    /// Cached to avoid linear complexity at 'has_nested'.
    bool has_nested = false;
};
class PathInDataBuilder {
public:
    const PathInData::Parts& get_parts() const { return parts; }
    PathInDataBuilder& append(std::string_view key, bool is_array);
    PathInDataBuilder& append(const PathInData::Parts& path, bool is_array);
    void pop_back();
    void pop_back(size_t n);

private:
    PathInData::Parts parts;
    /// Number of array levels without key to which
    /// next non-empty key will be nested.
    /// Example: for JSON { "k1": [[{"k2": 1, "k3": 2}] }
    // `k2` and `k3` has anonymous_array_level = 1 in that case.
    size_t current_anonymous_array_level = 0;
};
using PathsInData = std::vector<PathInData>;
/// Result of parsing of a document.
/// Contains all paths extracted from document
/// and values which are related to them.
struct ParseResult {
    std::vector<PathInData> paths;
    std::vector<Field> values;
};
} // namespace doris::vectorized
