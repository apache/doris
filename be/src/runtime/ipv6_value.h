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

    std::string to_string() const {
        uint16_t fields[8] = {
                static_cast<uint16_t>((_value.high >> 48) & 0xFFFF),
                static_cast<uint16_t>((_value.high >> 32) & 0xFFFF),
                static_cast<uint16_t>((_value.high >> 16) & 0xFFFF),
                static_cast<uint16_t>(_value.high & 0xFFFF),
                static_cast<uint16_t>((_value.low >> 48) & 0xFFFF),
                static_cast<uint16_t>((_value.low >> 32) & 0xFFFF),
                static_cast<uint16_t>((_value.low >> 16) & 0xFFFF),
                static_cast<uint16_t>(_value.low & 0xFFFF)
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



