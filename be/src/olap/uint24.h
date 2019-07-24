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

#include <cstdint>
#include <string>
#include <iostream>

namespace doris {

// 24bit int type, used to store date type in storage
struct uint24_t {
public:
    uint24_t() {
        memset(data, 0, sizeof(data));
    }

    uint24_t(const uint24_t& value) {
        data[0] = value.data[0];
        data[1] = value.data[1];
        data[2] = value.data[2];
    }

    uint24_t(const int32_t& value) {
        data[0] = static_cast<uint8_t>(value);
        data[1] = static_cast<uint8_t>(value >> 8);
        data[2] = static_cast<uint8_t>(value >> 16);
    }

    uint24_t& operator+=(const uint24_t& value) {
        *this = static_cast<int>(*this) + static_cast<int>(value);
        return *this;
    }

    operator int() const {
        int value = static_cast<uint8_t>(data[0]);
        value += (static_cast<uint32_t>(static_cast<uint8_t>(data[1]))) << 8;
        value += (static_cast<uint32_t>(static_cast<uint8_t>(data[2]))) << 16;
        return value;
    }

    uint24_t& operator=(const int& value) {
        data[0] = static_cast<uint8_t>(value);
        data[1] = static_cast<uint8_t>(value >> 8);
        data[2] = static_cast<uint8_t>(value >> 16);
        return *this;
    }

    uint24_t& operator=(const int64_t& value) {
        data[0] = static_cast<uint8_t>(value);
        data[1] = static_cast<uint8_t>(value >> 8);
        data[2] = static_cast<uint8_t>(value >> 16);
        return *this;
    }

    bool operator==(const uint24_t& value) const {
        return cmp(value) == 0;
    }

    bool operator!=(const uint24_t& value) const {
        return cmp(value) != 0;
    }

    bool operator<(const uint24_t& value) const {
        return cmp(value) < 0;
    }

    bool operator<=(const uint24_t& value) const {
        return cmp(value) <= 0;
    }

    bool operator>(const uint24_t& value) const {
        return cmp(value) > 0;
    }

    bool operator>=(const uint24_t& value) const {
        return cmp(value) >= 0;
    }

    int32_t cmp(const uint24_t& other) const {
        if (data[2] > other.data[2]) {
            return 1;
        } else if (data[2] < other.data[2]) {
            return -1;
        }

        if (data[1] > other.data[1]) {
            return 1;
        } else if (data[1] < other.data[1]) {
            return -1;
        }

        if (data[0] > other.data[0]) {
            return 1;
        } else if (data[0] < other.data[0]) {
            return -1;
        }

        return 0;
    }

private:
    uint8_t data[3];
} __attribute__((packed));

inline std::ostream& operator<<(std::ostream& os, const uint24_t& val) {
    return os;
}

}  // namespace doris
