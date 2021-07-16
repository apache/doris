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

#include <string.h>

#ifdef __SSE2__
#include <emmintrin.h>

/** memcpy function could work suboptimal if all the following conditions are met:
  * 1. Size of memory region is relatively small (approximately, under 50 bytes).
  * 2. Size of memory region is not known at compile-time.
  *
  * In that case, memcpy works suboptimal by following reasons:
  * 1. Function is not inlined.
  * 2. Much time/instructions are spend to process "tails" of data.
  *
  * There are cases when function could be implemented in more optimal way, with help of some assumptions.
  * One of that assumptions - ability to read and write some number of bytes after end of passed memory regions.
  * Under that assumption, it is possible not to implement difficult code to process tails of data and do copy always by big chunks.
  *
  * This case is typical, for example, when many small pieces of data are gathered to single contiguous piece of memory in a loop.
  * - because each next copy will overwrite excessive data after previous copy.
  *
  * Assumption that size of memory region is small enough allows us to not unroll the loop.
  * This is slower, when size of memory is actually big.
  *
  * Use with caution.
  */

namespace detail {
inline void memcpy_small_allow_read_write_overflow15_impl(char* __restrict dst,
                                                    const char* __restrict src, ssize_t n) {
    while (n > 0) {
        _mm_storeu_si128(reinterpret_cast<__m128i*>(dst),
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(src)));

        dst += 16;
        src += 16;
        n -= 16;
    }
}
} // namespace detail

/** Works under assumption, that it's possible to read up to 15 excessive bytes after end of 'src' region
  *  and to write any garbage into up to 15 bytes after end of 'dst' region.
  */
inline void memcpy_small_allow_read_write_overflow15(void* __restrict dst, const void* __restrict src,
                                                size_t n) {
    detail::memcpy_small_allow_read_write_overflow15_impl(reinterpret_cast<char*>(dst),
                                                    reinterpret_cast<const char*>(src), n);
}

/** NOTE There was also a function, that assumes, that you could read any bytes inside same memory page of src.
  * This function was unused, and also it requires special handling for Valgrind and ASan.
  */

#else /// Implementation for other platforms.

inline void memcpy_small_allow_read_write_overflow15(void* __restrict dst, const void* __restrict src,
                                                size_t n) {
    memcpy(dst, src, n);
}

#endif
