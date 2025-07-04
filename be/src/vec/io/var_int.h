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

#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"

namespace doris::vectorized {
/** Write Int64 in variable length format (base128) */
template <typename OUT>
void write_var_int(Int64 x, OUT& ostr) {
    write_var_uint(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

/** Read Int64, written in variable length format (base128) */
template <typename IN>
void read_var_int(Int64& x, IN& istr) {
    read_var_uint(*reinterpret_cast<UInt64*>(&x), istr);
    x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
}

// TODO: do real implement in the future
inline void read_var_uint(UInt64& x, BufferReadable& buf) {
    x = 0;
    // get length from first byte firstly
    uint8_t len = 0;
    buf.read((char*)&len, 1);
    auto ref = buf.read(len);
    // read data and set it to x per byte.
    char* bytes = const_cast<char*>(ref.data);
    for (size_t i = 0; i < 9; ++i) {
        UInt64 byte = bytes[i];
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80)) {
            return;
        }
    }
}

inline void write_var_uint(UInt64 x, BufferWritable& ostr) {
    char bytes[9];
    uint8_t i = 0;
    while (i < 9) {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F) {
            byte |= 0x80;
        }

        bytes[i++] = byte;

        x >>= 7;
        if (!x) {
            break;
        }
    }
    ostr.write((char*)&i, 1);
    ostr.write(bytes, i);
}

} // namespace doris::vectorized
