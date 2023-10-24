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

#include <stdint.h>

#include <algorithm>
#include <regex>
#include <sstream>
#include <string>

#include "util/string_parser.hpp"

namespace doris {

class IPv4Value {
public:
    IPv4Value() = default;

    explicit IPv4Value(vectorized::IPv4 ipv4) { _value = ipv4; }

    explicit IPv4Value(std::string ipv4) {}

    [[nodiscard]] const vectorized::IPv4& value() const { return _value; }

    vectorized::IPv4& value() { return _value; }

    void set_value(vectorized::IPv4 ipv4) { _value = ipv4; }

    bool from_string(std::string ipv4) { return from_string(_value, ipv4); }

    [[nodiscard]] std::string to_string() const { return to_string(_value); }

    static bool from_string(vectorized::IPv4& value, std::string ipv4) {
        remove_ipv4_space(ipv4);

        // shortest ipv4 string is `0.0.0.0` whose length is 7
        if (ipv4.size() < 7 || !is_valid_string(ipv4)) {
            return false;
        }

        vectorized::IPv4 octets[4] = {0};
        std::istringstream iss(ipv4);
        std::string octet;
        uint8_t octet_index = 0;

        while (getline(iss, octet, '.')) {
            if (octet_index >= 4) {
                return false;
            }

            StringParser::ParseResult result;
            vectorized::IPv4 val = StringParser::string_to_unsigned_int<vectorized::IPv4>(
                    octet.c_str(), octet.length(), &result);
            if (result != StringParser::PARSE_SUCCESS || val > 255) {
                return false;
            }

            octets[octet_index++] = val;
        }

        if (octet_index != 4) {
            return false;
        }

        value = (octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3];
        return true;
    }

    static std::string to_string(vectorized::IPv4 value) {
        std::stringstream ss;
        ss << ((value >> 24) & 0xFF) << '.' << ((value >> 16) & 0xFF) << '.'
           << ((value >> 8) & 0xFF) << '.' << (value & 0xFF);
        return ss.str();
    }

    static void remove_ipv4_space(std::string& ipv4) {
        if (ipv4.empty()) {
            return;
        }

        std::string special_chars = "\r\n\t ";

        size_t pos = ipv4.find_first_not_of(special_chars);
        if (pos != std::string::npos) {
            ipv4.erase(0, pos);
        }

        pos = ipv4.find_last_not_of(special_chars);
        if (pos != std::string::npos) {
            ipv4.erase(pos + 1);
        }
    }

    static bool is_valid_string(std::string ipv4) {
        static std::regex IPV4_STD_REGEX(
                "^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-"
                "9]?)$");
        if (ipv4.size() > 15 || !std::regex_match(ipv4, IPV4_STD_REGEX)) {
            return false;
        }
        return true;
    }

private:
    vectorized::IPv4 _value;
};

}