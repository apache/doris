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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/sse-util.hpp
// and modified by Doris

#pragma once

#if defined(__aarch64__)
#include <sse2neon.h> // IWYU pragma: export
#elif defined(__x86_64__)
#include <emmintrin.h> // IWYU pragma: export
#include <immintrin.h> // IWYU pragma: export
#include <mm_malloc.h> // IWYU pragma: export
#include <smmintrin.h> // IWYU pragma: export
#endif

namespace doris {

// This class contains constants useful for text processing with SSE4.2
// intrinsics.
namespace sse_util {
// Number of characters that fit in 64/128 bit register.
// SSE provides instructions for loading 64 or 128 bits into a register
// at a time.
static const int CHARS_PER_128_BIT_REGISTER = 16;

// SSE4.2 adds instructions for textprocessing.  The instructions accept
// a flag to control what text operation to do.
//   - SIDD_CMP_EQUAL_ANY ~ strchr
//   - SIDD_CMP_EQUAL_EACH ~ strcmp
//   - SIDD_UBYTE_OPS - 8 bit chars (as opposed to 16 bit)
//   - SIDD_NEGATIVE_POLARITY - toggles whether to set result to 1 or 0 when a
//     match is found.

// In this mode, sse text processing functions will return the number of bytes that match
// consecutively from the beginning.
static const int STRCMP_MODE = _SIDD_CMP_EQUAL_EACH | _SIDD_UBYTE_OPS | _SIDD_NEGATIVE_POLARITY;

} // namespace sse_util
} // namespace doris
