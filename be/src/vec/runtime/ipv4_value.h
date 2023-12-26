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

namespace doris {

class IPv4Value {
public:
    IPv4Value() = default;

    explicit IPv4Value(vectorized::IPv4 ipv4) { _value = ipv4; }

    const vectorized::IPv4& value() const { return _value; }

    vectorized::IPv4& value() { return _value; }

    void set_value(vectorized::IPv4 ipv4) { _value = ipv4; }

    bool from_string(const std::string& ipv4_str) { return from_string(_value, ipv4_str); }

    std::string to_string() const { return to_string(_value); }

    static bool from_string(vectorized::IPv4& value, const std::string& ipv4_str) {
        if (ipv4_str.empty()) {
            return false;
        }
        int64_t parse_value;
        const char* src = ipv4_str.c_str();
        const char* end = ipv4_str.c_str() + ipv4_str.size() - 1;
        while (std::isspace(*src)) ++src;
        while (std::isspace(*end)) --end;
        if (!vectorized::parseIPv4whole(src, ++end,
                                        reinterpret_cast<unsigned char*>(&parse_value))) {
            return false;
        }
        value = static_cast<vectorized::IPv4>(parse_value);
        return true;
    }

    static std::string to_string(vectorized::IPv4 value) {
        char buf[IPV4_MAX_TEXT_LENGTH + 1];
        char* start = buf;
        char* end = buf;
        const auto* src = reinterpret_cast<const unsigned char*>(&value);
        vectorized::formatIPv4(src, end);
        size_t len = end - start;
        return {buf, len};
    }

private:
    vectorized::IPv4 _value;
};

} // namespace doris
