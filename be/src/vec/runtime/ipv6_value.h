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

#include <regex>
#include <sstream>
#include <string>

#include "vec/common/format_ip.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number_base.h"

namespace doris {

class IPv6Value {
public:
    IPv6Value() { _value = 0; }

    explicit IPv6Value(vectorized::IPv6 ipv6) { _value = ipv6; }

    const vectorized::IPv6& value() const { return _value; }

    vectorized::IPv6& value() { return _value; }

    void set_value(vectorized::IPv6 ipv6) { _value = ipv6; }

    bool from_string(const std::string& ipv6_str) { return from_string(_value, ipv6_str); }

    static bool from_string(vectorized::IPv6& value, const char* ipv6_str, size_t len) {
        if (len == 0) {
            return false;
        }
        size_t begin = 0;
        size_t end = len - 1;
        while (begin < len && std::isspace(ipv6_str[begin])) {
            ++begin;
        }
        while (end > begin && std::isspace(ipv6_str[end])) {
            --end;
        }
        // parse and store in little-endian
        return vectorized::parse_ipv6_whole(ipv6_str + begin, ipv6_str + end + 1,
                                            reinterpret_cast<unsigned char*>(&value));
    }

    static bool from_string(vectorized::IPv6& value, const std::string& ipv6_str) {
        return from_string(value, ipv6_str.c_str(), ipv6_str.size());
    }

    std::string to_string() const { return to_string(_value); }

    static std::string to_string(vectorized::IPv6 value) {
        char buf[IPV6_MAX_TEXT_LENGTH + 1];
        char* start = buf;
        char* end = buf;
        auto* src = reinterpret_cast<unsigned char*>(&value);
        // load and format in little-endian
        vectorized::format_ipv6(src, end);
        size_t len = end - start;
        return {buf, len};
    }

    static bool is_valid_string(const char* ipv6_str, size_t len) {
        if (len == 0 || len > IPV6_MAX_TEXT_LENGTH) {
            return false;
        }
        vectorized::IPv6 value;
        size_t begin = 0;
        size_t end = len - 1;
        while (begin < len && std::isspace(ipv6_str[begin])) {
            ++begin;
        }
        while (end > begin && std::isspace(ipv6_str[end])) {
            --end;
        }
        return vectorized::parse_ipv6_whole(ipv6_str + begin, ipv6_str + end + 1,
                                            reinterpret_cast<unsigned char*>(&value));
    }

private:
    vectorized::IPv6 _value;
};

} // namespace doris
