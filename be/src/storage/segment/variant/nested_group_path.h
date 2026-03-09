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
#include <string_view>

namespace doris::segment_v2 {

inline constexpr std::string_view kNestedGroupMarker = "__D0_ng__";
inline constexpr std::string_view kRootNestedGroupPath = "__D0_root__";
inline constexpr std::string_view kNestedGroupOffsetsSuffix = ".__offsets";

inline bool ends_with(std::string_view value, std::string_view suffix) {
    return value.size() >= suffix.size() &&
           value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

inline bool contains_nested_group_marker(std::string_view path) {
    return path.find(kNestedGroupMarker) != std::string_view::npos;
}

inline std::string nested_group_marker_token() {
    return "." + std::string(kNestedGroupMarker) + ".";
}

inline std::string strip_nested_group_marker(std::string_view path) {
    std::string out(path);
    const std::string marker = nested_group_marker_token();
    for (;;) {
        auto pos = out.find(marker);
        if (pos == std::string::npos) {
            break;
        }
        out.replace(pos, marker.size(), ".");
    }
    return out;
}

inline std::string build_nested_group_offsets_column_name(std::string_view variant_name,
                                                          std::string_view full_path) {
    std::string name;
    name.reserve(variant_name.size() + full_path.size() + 32);
    name.append(variant_name);
    name.append(".");
    name.append(kNestedGroupMarker);
    name.append(".");
    name.append(full_path);
    name.append(kNestedGroupOffsetsSuffix);
    return name;
}

} // namespace doris::segment_v2
