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

#include <algorithm>

#include "util/json/path_in_data.h"

namespace doris {

inline int compare_variant_shredded_path_part(const PathInData::Part& left,
                                              const PathInData::Part& right) {
    const int key_comparison = left.key.compare(right.key);
    if (key_comparison != 0) {
        return key_comparison;
    }
    if (left.is_nested != right.is_nested) {
        return left.is_nested ? 1 : -1;
    }
    return (left.anonymous_array_level > right.anonymous_array_level) -
           (left.anonymous_array_level < right.anonymous_array_level);
}

// Shredded layout identity deliberately uses every physical path part while ignoring the
// PathInData typed marker, which is storage metadata rather than JSON path identity.
inline bool variant_shredded_path_less(const PathInData& left, const PathInData& right) {
    const auto& left_parts = left.get_parts();
    const auto& right_parts = right.get_parts();
    return std::ranges::lexicographical_compare(
            left_parts, right_parts, [](const auto& lhs, const auto& rhs) {
                return compare_variant_shredded_path_part(lhs, rhs) < 0;
            });
}

inline bool variant_shredded_path_is_prefix(const PathInData& prefix, const PathInData& path) {
    const auto& prefix_parts = prefix.get_parts();
    const auto& path_parts = path.get_parts();
    return prefix_parts.size() <= path_parts.size() &&
           std::ranges::equal(prefix_parts.begin(), prefix_parts.end(), path_parts.begin(),
                              path_parts.begin() + prefix_parts.size());
}

} // namespace doris
