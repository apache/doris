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

#include "common/tls_protocol_config.h"

#include <algorithm>
#include <cctype>
#include <string_view>

#include "common/config.h"

namespace doris {
namespace {

std::string_view trim_ascii_whitespace(std::string_view value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    return value;
}

bool equals_ignore_ascii_case(std::string_view lhs, std::string_view rhs) {
    return lhs.size() == rhs.size() &&
           std::equal(lhs.begin(), lhs.end(), rhs.begin(),
                      [](unsigned char left, unsigned char right) {
                          return std::tolower(left) == std::tolower(right);
                      });
}

bool comma_separated_protocols_contains(std::string_view raw, std::string_view protocol) {
    while (true) {
        size_t separator = raw.find(',');
        std::string_view token = trim_ascii_whitespace(raw.substr(0, separator));
        if (equals_ignore_ascii_case(token, protocol)) {
            return true;
        }
        if (separator == std::string_view::npos) {
            return false;
        }
        raw.remove_prefix(separator + 1);
    }
}

bool is_unified_http_tls_enabled() {
    return config::enable_tls &&
           !comma_separated_protocols_contains(config::tls_excluded_protocols, "http");
}

} // namespace

const char* get_internal_http_scheme() {
    // Keep the legacy HTTPS-only mode working while unified TLS is adopted by internal callers.
    return (config::enable_https || is_unified_http_tls_enabled()) ? "https://" : "http://";
}

} // namespace doris
