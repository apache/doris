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

#include <charconv>
#include <cstdint>
#include <optional>
#include <string_view>

namespace doris {

// Interpret the complete relative name of an internal Variant bucket.
inline std::optional<uint32_t> parse_variant_bucket(std::string_view path,
                                                    std::string_view marker) {
    if (!path.starts_with(marker)) {
        return std::nullopt;
    }
    path.remove_prefix(marker.size());
    if (!path.starts_with(".b")) {
        return std::nullopt;
    }
    path.remove_prefix(2);
    // The writer uses std::to_string, so a noncanonical suffix cannot be an internal bucket.
    if (path.empty() || (path.size() > 1 && path.front() == '0')) {
        return std::nullopt;
    }
    uint32_t bucket = 0;
    const char* end = path.data() + path.size();
    const auto result = std::from_chars(path.data(), end, bucket);
    if (result.ec != std::errc() || result.ptr != end) {
        return std::nullopt;
    }
    return bucket;
}

} // namespace doris
