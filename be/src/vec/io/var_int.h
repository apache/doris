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

#include <iostream>

#include "vec/core/types.h"

namespace doris::vectorized {
/** Write UInt64 in variable length format (base128) NOTE Only up to 2^63 - 1 are supported. */
void write_var_uint(UInt64 x, std::ostream& ostr);
char* write_var_uint(UInt64 x, char* ostr);

/** Read UInt64, written in variable length format (base128) */
void read_var_uint(UInt64& x, std::istream& istr);
const char* read_var_uint(UInt64& x, const char* istr, size_t size);

/** Get the length of UInt64 in VarUInt format */
size_t get_length_of_var_uint(UInt64 x);

/** Get the Int64 length in VarInt format */
size_t get_length_of_var_int(Int64 x);

/** Write Int64 in variable length format (base128) */
template <typename OUT>
inline void write_var_int(Int64 x, OUT& ostr) {
    write_var_uint(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

inline char* write_var_int(Int64 x, char* ostr) {
    return write_var_uint(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

/** Read Int64, written in variable length format (base128) */
template <typename IN>
inline void read_var_int(Int64& x, IN& istr) {
    read_var_uint(*reinterpret_cast<UInt64*>(&x), istr);
    x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
}

inline const char* read_var_int(Int64& x, const char* istr, size_t size) {
    const char* res = read_var_uint(*reinterpret_cast<UInt64*>(&x), istr, size);
    x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
    return res;
}

inline void write_var_t(UInt64 x, std::ostream& ostr) {
    write_var_uint(x, ostr);
}
inline void write_var_t(Int64 x, std::ostream& ostr) {
    write_var_int(x, ostr);
}

inline char* write_var_t(UInt64 x, char*& ostr) {
    return write_var_uint(x, ostr);
}
inline char* write_var_t(Int64 x, char*& ostr) {
    return write_var_int(x, ostr);
}

inline void read_var_t(UInt64& x, std::istream& istr) {
    read_var_uint(x, istr);
}
inline void read_var_t(Int64& x, std::istream& istr) {
    read_var_int(x, istr);
}

inline const char* read_var_t(UInt64& x, const char* istr, size_t size) {
    return read_var_uint(x, istr, size);
}
inline const char* read_var_t(Int64& x, const char* istr, size_t size) {
    return read_var_int(x, istr, size);
}

inline void read_var_uint(UInt64& x, std::istream& istr) {
    x = 0;
    for (size_t i = 0; i < 9; ++i) {
        UInt64 byte = istr.get();
        x |= (byte & 0x7F) << (7 * i);
        if (!(byte & 0x80)) return;
    }
}

inline void write_var_uint(UInt64 x, std::ostream& ostr) {
    for (size_t i = 0; i < 9; ++i) {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F) byte |= 0x80;

        ostr.put(byte);

        x >>= 7;
        if (!x) return;
    }
}

// TODO: do real implement in the future
inline void read_var_uint(UInt64& x, BufferReadable& buf) {
    x = 0;
    uint8_t len = 0;
    buf.read((char*)&len, 1);
    auto ref = buf.read(len);

    char* bytes = const_cast<char *>(ref.data);
    for (size_t i = 0; i < 9; ++i) {
        UInt64 byte = bytes[i];
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80)) return;
    }
}

inline void write_var_uint(UInt64 x, BufferWritable& ostr) {
    char bytes[9];
    uint8_t i = 0;
    while (i < 9) {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F) byte |= 0x80;

        bytes[i++] = byte;

        x >>= 7;
        if (!x) break;
    }
    ostr.write((char*)&i, 1);
    ostr.write(bytes, i);
}

inline char* write_var_uint(UInt64 x, char* ostr) {
    for (size_t i = 0; i < 9; ++i) {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F) byte |= 0x80;

        *ostr = byte;
        ++ostr;

        x >>= 7;
        if (!x) return ostr;
    }

    return ostr;
}

// clang-format off
inline size_t get_length_of_var_uint(UInt64 x) {
    return x < (1ULL << 7) ? 1
        : (x < (1ULL << 14) ? 2
        : (x < (1ULL << 21) ? 3
        : (x < (1ULL << 28) ? 4
        : (x < (1ULL << 35) ? 5
        : (x < (1ULL << 42) ? 6
        : (x < (1ULL << 49) ? 7
        : (x < (1ULL << 56) ? 8
        : 9)))))));
}
// clang-format on

inline size_t get_length_of_var_int(Int64 x) {
    return get_length_of_var_uint(static_cast<UInt64>((x << 1) ^ (x >> 63)));
}

} // namespace doris::vectorized
