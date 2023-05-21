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

namespace doris {

namespace vectorized {

class IPv4Value {
public:
    IPv4Value() = default;

    explicit IPv4Value(uint32_t ipv4) {
        _value = ipv4;
    }

    const uint32_t& value() const {
        return _value;
    }

    uint32_t& value() {
        return _value;
    }

    void set_value(uint32_t ipv4) {
        _value = ipv4;
    }

    std::string to_string() const {
        std::stringstream ss;
        ss << ((_value >> 24) & 0xFF) << '.'
           << ((_value >> 16) & 0xFF) << '.'
           << ((_value >> 8) & 0xFF) << '.'
           << (_value & 0xFF);
        return ss.str();
    }

    static IPv4Value create_from_olap_ipv4(uint32_t value) {
        IPv4Value ipv4;
        ipv4.set_value(value);
        return ipv4;
    }

private:
    uint32_t _value;
};

}

}



