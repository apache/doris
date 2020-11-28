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

#include "exec/read_write_util.h"

namespace doris {

int ReadWriteUtil::put_zint(int32_t integer, uint8_t* buf) {
    // Move the sign bit to the first bit.
    uint32_t uinteger = (integer << 1) ^ (integer >> 31);
    const int mask = 0x7f;
    const int cont = 0x80;
    buf[0] = uinteger & mask;
    int len = 1;

    while ((uinteger >>= 7) != 0) {
        // Set the continuation bit.
        buf[len - 1] |= cont;
        buf[len] = uinteger & mask;
        ++len;
    }

    return len;
}

int ReadWriteUtil::put_zlong(int64_t longint, uint8_t* buf) {
    // Move the sign bit to the first bit.
    uint64_t ulongint = (longint << 1) ^ (longint >> 63);
    const int mask = 0x7f;
    const int cont = 0x80;
    buf[0] = ulongint & mask;
    int len = 1;

    while ((ulongint >>= 7) != 0) {
        // Set the continuation bit.
        buf[len - 1] |= cont;
        buf[len] = ulongint & mask;
        ++len;
    }

    return len;
}

std::string ReadWriteUtil::hex_dump(const uint8_t* buf, int64_t length) {
    std::stringstream ss;
    ss << std::hex;

    for (int i = 0; i < length; ++i) {
        ss << static_cast<int>(buf[i]) << " ";
    }

    return ss.str();
}

std::string ReadWriteUtil::hex_dump(const char* buf, int64_t length) {
    return hex_dump(reinterpret_cast<const uint8_t*>(buf), length);
}
} // namespace doris
