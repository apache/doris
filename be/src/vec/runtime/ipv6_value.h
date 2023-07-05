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

#include <sstream>
#include <string>

#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number_base.h"


namespace doris {

namespace vectorized {

class IPv6Value {
public:
    IPv6Value() = default;

    explicit IPv6Value(UInt128 ipv6) {
        _value = ipv6;
    }

    const UInt128& value() const {
        return _value;
    }

    UInt128& value() {
        return _value;
    }

    void set_value(UInt128 ipv6) {
        _value = ipv6;
    }

    bool from_string(std::string ipv6) {
        return from_string(_value, ipv6);
    }

    static bool from_string(IPv6& x, std::string ipv6) {
        remove_ipv6_space(ipv6);
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
                    fields[num_field++] = std::stoi(field, nullptr, 16);
                } catch (const std::exception& e) {
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

        x.high = (static_cast<uint64_t>(fields[0]) << 48) | (static_cast<uint64_t>(fields[1]) << 32) |
                 (static_cast<uint64_t>(fields[2]) << 16) | static_cast<uint64_t>(fields[3]);
        x.low = (static_cast<uint64_t>(fields[4]) << 48) | (static_cast<uint64_t>(fields[5]) << 32) |
                (static_cast<uint64_t>(fields[6]) << 16) | static_cast<uint64_t>(fields[7]);

        return true;
    }

    std::string to_string() const {
        return to_string(_value);
    }

    static std::string to_string(const IPv6& x) {
        uint16_t fields[8] = {
                static_cast<uint16_t>((x.high >> 48) & 0xFFFF),
                static_cast<uint16_t>((x.high >> 32) & 0xFFFF),
                static_cast<uint16_t>((x.high >> 16) & 0xFFFF),
                static_cast<uint16_t>(x.high & 0xFFFF),
                static_cast<uint16_t>((x.low >> 48) & 0xFFFF),
                static_cast<uint16_t>((x.low >> 32) & 0xFFFF),
                static_cast<uint16_t>((x.low >> 16) & 0xFFFF),
                static_cast<uint16_t>(x.low & 0xFFFF)
        };

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

    static void remove_ipv6_space(std::string& ipv6) {
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

    static IPv6Value create_from_olap_ipv6(UInt128 value) {
        IPv6Value ipv6;
        ipv6.set_value(value);
        return ipv6;
    }

private:
    UInt128 _value;
};

}

}



