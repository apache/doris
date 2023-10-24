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

#include <regex>
#include <sstream>
#include <string>

#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number_base.h"

namespace doris {

class IPv6Value {
public:
    IPv6Value() {
        _value.high = 0;
        _value.low = 0;
    }

    explicit IPv6Value(vectorized::IPv6 ipv6) { _value = ipv6; }

    [[nodiscard]] const vectorized::IPv6& value() const { return _value; }

    vectorized::IPv6& value() { return _value; }

    void set_value(vectorized::IPv6 ipv6) { _value = ipv6; }

    bool from_string(std::string ipv6) { return from_string(_value, ipv6); }

    bool from_binary_string(std::string ipv6_binary) {
        return from_binary_string(_value, ipv6_binary);
    }

    static bool from_string(vectorized::IPv6& x, std::string ipv6) {
        remove_ipv6_space(ipv6);

        if (ipv6.empty() || !is_valid_string(ipv6)) {
            return false;
        }

        std::transform(ipv6.begin(), ipv6.end(), ipv6.begin(),
                       [](unsigned char ch) { return std::tolower(ch); });
        std::istringstream iss(ipv6);
        std::string field;
        uint16_t fields[8] = {0};
        uint8_t zero_index = 0;
        uint8_t num_field = 0;
        uint8_t right_field_num = 0;

        while (num_field < 8) {
            if (!getline(iss, field, ':')) {
                break;
            }

            if (field.empty()) {
                zero_index = num_field;
                fields[num_field++] = 0;
            } else {
                try {
                    if (field.size() > 4 || field > "ffff") {
                        return false;
                    }

                    fields[num_field++] = std::stoi(field, nullptr, 16);
                } catch (const std::exception& /*e*/) {
                    return false;
                }
            }
        }

        if (zero_index != 0) {
            right_field_num = num_field - zero_index - 1;

            for (uint8_t i = 7; i > 7 - right_field_num; --i) {
                fields[i] = fields[zero_index + right_field_num + i - 7];
                fields[zero_index + right_field_num + i - 7] = 0;
            }
        }

        x.high = (static_cast<uint64_t>(fields[0]) << 48) |
                 (static_cast<uint64_t>(fields[1]) << 32) |
                 (static_cast<uint64_t>(fields[2]) << 16) | static_cast<uint64_t>(fields[3]);
        x.low = (static_cast<uint64_t>(fields[4]) << 48) |
                (static_cast<uint64_t>(fields[5]) << 32) |
                (static_cast<uint64_t>(fields[6]) << 16) | static_cast<uint64_t>(fields[7]);

        return true;
    }

    static bool from_binary_string(vectorized::IPv6& x, std::string ipv6_binary_str) {
        // Accepts a FixedString(16) value containing the IPv6 address in binary format
        if (ipv6_binary_str.size() != 16) {
            return false;
        }

        x.high = 0;
        x.low = 0;

        const uint8_t* ipv6_binary = reinterpret_cast<const uint8_t*>(ipv6_binary_str.c_str());

        for (int i = 0; i < 8; ++i) {
            x.high |= (static_cast<uint64_t>(ipv6_binary[i]) << (56 - i * 8));
        }

        for (int i = 8; i < 16; ++i) {
            x.low |= (static_cast<uint64_t>(ipv6_binary[i]) << (56 - (i - 8) * 8));
        }

        return true;
    }

    [[nodiscard]] std::string to_string() const { return to_string(_value); }

    static std::string to_string(vectorized::IPv6 x) {
        // "0000:0000:0000:0000:0000:0000:0000:0000"
        if (x.high == 0 && x.low == 0) {
            return "::";
        }

        uint16_t fields[8] = {static_cast<uint16_t>((x.high >> 48) & 0xFFFF),
                              static_cast<uint16_t>((x.high >> 32) & 0xFFFF),
                              static_cast<uint16_t>((x.high >> 16) & 0xFFFF),
                              static_cast<uint16_t>(x.high & 0xFFFF),
                              static_cast<uint16_t>((x.low >> 48) & 0xFFFF),
                              static_cast<uint16_t>((x.low >> 32) & 0xFFFF),
                              static_cast<uint16_t>((x.low >> 16) & 0xFFFF),
                              static_cast<uint16_t>(x.low & 0xFFFF)};

        uint8_t zero_start = 0, zero_end = 0;

        while (zero_start < 8 && zero_end < 8) {
            if (fields[zero_start] != 0) {
                zero_start++;
                zero_end = zero_start;
                continue;
            }

            while (zero_end < 7 && fields[zero_end + 1] == 0) {
                zero_end++;
            }

            if (zero_end > zero_start) {
                break;
            }

            zero_start++;
            zero_end = zero_start;
        }

        std::stringstream ss;

        if (zero_start == zero_end) {
            for (uint8_t i = 0; i < 7; ++i) {
                ss << std::hex << fields[i] << ":";
            }
            ss << std::hex << fields[7];
        } else {
            for (uint8_t i = 0; i < zero_start; ++i) {
                ss << std::hex << fields[i] << ":";
            }

            if (zero_end == 7) {
                ss << ":";
            } else {
                for (uint8_t j = zero_end + 1; j < 8; ++j) {
                    ss << std::hex << ":" << fields[j];
                }
            }
        }

        return ss.str();
    }

    [[nodiscard]] std::string to_binary_string() const { return to_binary_string(_value); }

    static std::string to_binary_string(vectorized::IPv6 x) {
        uint8_t fields[16] = {static_cast<uint8_t>((x.high >> 56) & 0xFF),
                              static_cast<uint8_t>((x.high >> 48) & 0xFF),
                              static_cast<uint8_t>((x.high >> 40) & 0xFF),
                              static_cast<uint8_t>((x.high >> 32) & 0xFF),
                              static_cast<uint8_t>((x.high >> 24) & 0xFF),
                              static_cast<uint8_t>((x.high >> 16) & 0xFF),
                              static_cast<uint8_t>((x.high >> 8) & 0xFF),
                              static_cast<uint8_t>(x.high & 0xFF),
                              static_cast<uint8_t>((x.low >> 56) & 0xFF),
                              static_cast<uint8_t>((x.low >> 48) & 0xFF),
                              static_cast<uint8_t>((x.low >> 40) & 0xFF),
                              static_cast<uint8_t>((x.low >> 32) & 0xFF),
                              static_cast<uint8_t>((x.low >> 24) & 0xFF),
                              static_cast<uint8_t>((x.low >> 16) & 0xFF),
                              static_cast<uint8_t>((x.low >> 8) & 0xFF),
                              static_cast<uint8_t>(x.low & 0xFF)};

        std::stringstream ss;

        for (int i = 0; i < 16; ++i) {
            ss << (char)fields[i];
        }

        return ss.str();
    }

    static void remove_ipv6_space(std::string& ipv6) {
        if (ipv6.empty()) {
            return;
        }

        std::string special_chars = "\r\n\t ";

        size_t pos = ipv6.find_first_not_of(special_chars);
        if (pos != std::string::npos) {
            ipv6.erase(0, pos);
        }

        pos = ipv6.find_last_not_of(special_chars);
        if (pos != std::string::npos) {
            ipv6.erase(pos + 1);
        }
    }

    static bool is_valid_string(std::string ipv6) {
        static std::regex IPV6_STD_REGEX("^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$");
        static std::regex IPV6_COMPRESS_REGEX(
                "^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4})*)?)::((([0-9A-Fa-f]{1,4}:)*[0-9A-Fa-f]{1,4}"
                ")?)$");

        if (ipv6.size() > 39 || !(std::regex_match(ipv6, IPV6_STD_REGEX) ||
                                  std::regex_match(ipv6, IPV6_COMPRESS_REGEX))) {
            return false;
        }
        return true;
    }

private:
    vectorized::IPv6 _value;
};

} // namespace doris
