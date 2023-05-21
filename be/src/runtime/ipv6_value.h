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
        std::stringstream ss;
        ss << std::hex << std::setfill('0');

        for (int i = 7; i >= 0; i--) {
            UInt16 field = (i < 4) ? ((_value.high >> (16 * i)) & 0xFFFF) : ((_value.low >> (16 * (i - 4))) & 0xFFFF);
            ss << std::setw(4) << field;
            if (i != 0) {
                ss << ":";
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



