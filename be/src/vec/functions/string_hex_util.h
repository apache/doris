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

#include "util/simd/vstring_function.h"
#include "vec/columns/column_string.h"

namespace doris::vectorized::string_hex {

static constexpr int MAX_STACK_CIPHER_LEN = 1024 * 64;

inline bool check_and_decode_one(char& c, const char src_c, bool flag) {
    int k = flag ? 16 : 1;
    int value = src_c - '0';
    // 9 = ('9'-'0')
    if (value >= 0 && value <= 9) {
        c += value * k;
        return true;
    }

    value = src_c - 'A';
    // 5 = ('F'-'A')
    if (value >= 0 && value <= 5) {
        c += (value + 10) * k;
        return true;
    }

    value = src_c - 'a';
    // 5 = ('f'-'a')
    if (value >= 0 && value <= 5) {
        c += (value + 10) * k;
        return true;
    }
    // not in ( ['0','9'], ['a','f'], ['A','F'] )
    return false;
}

inline int hex_decode(const char* src_str, ColumnString::Offset src_len, char* dst_str) {
    // if str length is odd or 0, return empty string like mysql dose.
    if ((src_len & 1) != 0 or src_len == 0) {
        return 0;
    }
    //check and decode one character at the same time
    // character in ( ['0','9'], ['a','f'], ['A','F'] ), return 'NULL' like mysql dose.
    for (auto i = 0, dst_index = 0; i < src_len; i += 2, dst_index++) {
        char c = 0;
        // combine two character into dst_str one character
        bool left_4bits_flag = check_and_decode_one(c, *(src_str + i), true);
        bool right_4bits_flag = check_and_decode_one(c, *(src_str + i + 1), false);

        if (!left_4bits_flag || !right_4bits_flag) {
            return 0;
        }
        *(dst_str + dst_index) = c;
    }
    return src_len / 2;
}

inline void hex_encode(const unsigned char* source, size_t srclen, unsigned char*& dst_data_ptr,
                       size_t& offset) {
    if (srclen != 0) {
        doris::simd::VStringFunctions::hex_encode(source, srclen,
                                                  reinterpret_cast<char*>(dst_data_ptr));
        dst_data_ptr += (srclen * 2);
        offset += (srclen * 2);
    }
}

} // namespace doris::vectorized::string_hex
