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

#ifndef DORIS_BE_SRC_COMMON_UTIL_SSE_UTIL_H
#define DORIS_BE_SRC_COMMON_UTIL_SSE_UTIL_H

#include <nmmintrin.h>
#include <smmintrin.h>

namespace doris {

// This class contains constants useful for text processing with SSE4.2
// intrinsics.
namespace sse_util {
// Number of characters that fit in 64/128 bit register.
// SSE provides instructions for loading 64 or 128 bits into a register
// at a time.
static const int CHARS_PER_64_BIT_REGISTER = 8;
static const int CHARS_PER_128_BIT_REGISTER = 16;

// SSE4.2 adds instructions for textprocessing.  The instructions accept
// a flag to control what text operation to do.
//   - SIDD_CMP_EQUAL_ANY ~ strchr
//   - SIDD_CMP_EQUAL_EACH ~ strcmp
//   - SIDD_UBYTE_OPS - 8 bit chars (as opposed to 16 bit)
//   - SIDD_NEGATIVE_POLARITY - toggles whether to set result to 1 or 0 when a
//     match is found.

// In this mode, sse text processing functions will return a mask of all the characters that
// matched
static const int STRCHR_MODE = _SIDD_CMP_EQUAL_ANY | _SIDD_UBYTE_OPS;

// In this mode, sse text processing functions will return the number of bytes that match
// consecutively from the beginning.
static const int STRCMP_MODE = _SIDD_CMP_EQUAL_EACH | _SIDD_UBYTE_OPS
                               | _SIDD_NEGATIVE_POLARITY;

// Precomputed mask values up to 16 bits.
static const int SSE_BITMASK[CHARS_PER_128_BIT_REGISTER] = {
    1 << 0,
    1 << 1,
    1 << 2,
    1 << 3,
    1 << 4,
    1 << 5,
    1 << 6,
    1 << 7,
    1 << 8,
    1 << 9,
    1 << 10,
    1 << 11,
    1 << 12,
    1 << 13,
    1 << 14,
    1 << 15,
};

}
}

#endif
