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
#include <regex>
#include <sstream>
#include <string>

#include "util/string_parser.hpp"
#include "vec/common/format_ip.h"
#include "vec/common/string_ref.h"

namespace doris {

class IPv4Value {
public:
    IPv4Value() = default;

    explicit IPv4Value(IPv4 ipv4) { _value = ipv4; }

    const IPv4& value() const { return _value; }

    IPv4& value() { return _value; }

    void set_value(IPv4 ipv4) { _value = ipv4; }

    bool from_string(const std::string& ipv4_str) { return from_string(_value, ipv4_str); }

    std::string to_string() const { return to_string(_value); }

    static bool from_string(IPv4& value, const char* ipv4_str, size_t len) {
        if (len == 0) {
            return false;
        }
        int64_t parse_value = 0;
        size_t begin = 0;
        size_t end = len - 1;
        while (begin < len && std::isspace(ipv4_str[begin])) {
            ++begin;
        }
        while (end > begin && std::isspace(ipv4_str[end])) {
            --end;
        }
        if (!vectorized::parse_ipv4_whole(ipv4_str + begin, ipv4_str + end + 1,
                                          reinterpret_cast<unsigned char*>(&parse_value))) {
            return false;
        }
        value = static_cast<IPv4>(parse_value);
        return true;
    }

    static bool from_string(IPv4& value, const std::string& ipv4_str) {
        return from_string(value, ipv4_str.c_str(), ipv4_str.size());
    }

    static std::string to_string(IPv4 value) {
        char buf[IPV4_MAX_TEXT_LENGTH + 1];
        char* start = buf;
        char* end = buf;
        const auto* src = reinterpret_cast<const unsigned char*>(&value);
        vectorized::format_ipv4(src, end);
        size_t len = end - start;
        return {buf, len};
    }

    static bool is_valid_string(const char* ipv4_str, size_t len) {
        if (len == 0) {
            return false;
        }
        int64_t parse_value = 0;
        size_t begin = 0;
        size_t end = len - 1;
        while (begin < len && std::isspace(ipv4_str[begin])) {
            ++begin;
        }
        while (end > begin && std::isspace(ipv4_str[end])) {
            --end;
        }
        return vectorized::parse_ipv4_whole(ipv4_str + begin, ipv4_str + end + 1,
                                            reinterpret_cast<unsigned char*>(&parse_value));
    }

private:
    IPv4 _value;
};

} // namespace doris
