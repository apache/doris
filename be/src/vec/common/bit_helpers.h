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

#include <cstddef>
#include <type_traits>

/** Returns log2 of number, rounded down.
  * Compiles to single 'bsr' instruction on x86.
  * For zero argument, result is unspecified.
  */
inline unsigned int bit_scan_reverse(unsigned int x) {
    return sizeof(unsigned int) * 8 - 1 - __builtin_clz(x);
}

/** For zero argument, result is zero.
  * For arguments with most significand bit set, result is zero.
  * For other arguments, returns value, rounded up to power of two.
  */
inline size_t round_up_to_power_of_two_or_zero(size_t n) {
    --n;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    ++n;

    return n;
}

template <typename T>
inline size_t get_leading_zero_bits(T x) {
    if (!x) return sizeof(x) * 8;

    if constexpr (sizeof(T) <= sizeof(unsigned int)) {
        return __builtin_clz(x);
    } else if constexpr (sizeof(T) <= sizeof(unsigned long int)) {
        return __builtin_clzl(x);
    } else {
        return __builtin_clzll(x);
    }
}

template <typename T>
inline size_t get_trailing_zero_bits(T x) {
    if (!x) return sizeof(x) * 8;

    if constexpr (sizeof(T) <= sizeof(unsigned int)) {
        return __builtin_ctz(x);
    } else if constexpr (sizeof(T) <= sizeof(unsigned long int)) {
        return __builtin_ctzl(x);
    } else {
        return __builtin_ctzll(x);
    }
}

/** Returns a mask that has '1' for `bits` LSB set:
 * mask_low_bits<UInt8>(3) => 00000111
 */
template <typename T>
inline T mask_low_bits(unsigned char bits) {
    if (bits == 0) {
        return 0;
    }

    T result = static_cast<T>(~T {0});
    if (bits < sizeof(T) * 8) {
        result = static_cast<T>(result >> (sizeof(T) * 8 - bits));
    }

    return result;
}
