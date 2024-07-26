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

#pragma once
#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "common/compiler_util.h"
#include "gutil/integral_types.h"
#include "gutil/port.h"

namespace doris {

ALWAYS_INLINE inline void memcpy_inlined(void* __restrict _dst, const void* __restrict _src,
                                         size_t size) {
    auto dst = static_cast<uint8_t*>(_dst);
    auto src = static_cast<const uint8_t*>(_src);

    [[maybe_unused]] tail :
            /// Small sizes and tails after the loop for large sizes.
            /// The order of branches is important but in fact the optimal order depends on the distribution of sizes in your application.
            /// This order of branches is from the disassembly of glibc's code.
            /// We copy chunks of possibly uneven size with two overlapping movs.
            /// Example: to copy 5 bytes [0, 1, 2, 3, 4] we will copy tail [1, 2, 3, 4] first and then head [0, 1, 2, 3].
            if (size <= 16) {
        if (size >= 8) {
            /// Chunks of 8..16 bytes.
            __builtin_memcpy(dst + size - 8, src + size - 8, 8);
            __builtin_memcpy(dst, src, 8);
        } else if (size >= 4) {
            /// Chunks of 4..7 bytes.
            __builtin_memcpy(dst + size - 4, src + size - 4, 4);
            __builtin_memcpy(dst, src, 4);
        } else if (size >= 2) {
            /// Chunks of 2..3 bytes.
            __builtin_memcpy(dst + size - 2, src + size - 2, 2);
            __builtin_memcpy(dst, src, 2);
        } else if (size >= 1) {
            /// A single byte.
            *dst = *src;
        }
        /// No bytes remaining.
    }
    else {
#ifdef __AVX2__
        if (size <= 256) {
            if (size <= 32) {
                __builtin_memcpy(dst, src, 8);
                __builtin_memcpy(dst + 8, src + 8, 8);
                size -= 16;
                dst += 16;
                src += 16;
                goto tail;
            }

            /// Then we will copy every 16 bytes from the beginning in a loop.
            /// The last loop iteration will possibly overwrite some part of already copied last 32 bytes.
            /// This is Ok, similar to the code for small sizes above.
            while (size > 32) {
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst),
                                    _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src)));
                dst += 32;
                src += 32;
                size -= 32;
            }

            _mm256_storeu_si256(
                    reinterpret_cast<__m256i*>(dst + size - 32),
                    _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + size - 32)));
        } else {
            if (size >= 512 * 1024 && size <= 2048 * 1024) {
                asm volatile("rep movsb"
                             : "=D"(dst), "=S"(src), "=c"(size)
                             : "0"(dst), "1"(src), "2"(size)
                             : "memory");
            } else {
                size_t padding = (32 - (reinterpret_cast<size_t>(dst) & 31)) & 31;

                if (padding > 0) {
                    __m256i head = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), head);
                    dst += padding;
                    src += padding;
                    size -= padding;
                }

                /// Aligned unrolled copy. We will use half of available AVX registers.
                /// It's not possible to have both src and dst aligned.
                /// So, we will use aligned stores and unaligned loads.
                __m256i c0, c1, c2, c3, c4, c5, c6, c7;

                while (size >= 256) {
                    c0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
                    c1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 32));
                    c2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 64));
                    c3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 96));
                    c4 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 128));
                    c5 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 160));
                    c6 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 192));
                    c7 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 224));
                    src += 256;

                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst)), c0);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 32)), c1);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 64)), c2);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 96)), c3);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 128)), c4);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 160)), c5);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 192)), c6);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 224)), c7);
                    dst += 256;

                    size -= 256;
                }

                goto tail;
            }
        }
#else
        memcpy(dst, src, size);
#endif
    }
}
} // namespace doris