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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/bit-util.h
// and modified by Doris

#pragma once

#include <type_traits>

#include "vec/core/extended_types.h"
#ifndef __APPLE__
#include <endian.h>
#endif

#include "common/compiler_util.h" // IWYU pragma: keep
#include "util/cpu_info.h"
#include "util/sse_util.hpp"

namespace doris {

// Utility class to do standard bit tricks
// TODO: is this in boost or something else like that?
class BitUtil {
public:
    // Returns the ceil of value/divisor
    static inline int64_t ceil(int64_t value, int64_t divisor) {
        return value / divisor + (value % divisor != 0);
    }

    // Returns 'value' rounded up to the nearest multiple of 'factor'
    static inline int64_t round_up(int64_t value, int64_t factor) {
        return (value + (factor - 1)) / factor * factor;
    }

    // Returns the smallest power of two that contains v. Taken from
    // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    // TODO: Pick a better name, as it is not clear what happens when the input is
    // already a power of two.
    static inline int64_t next_power_of_two(int64_t v) {
        --v;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        ++v;
        return v;
    }

    // Non hw accelerated pop count.
    // TODO: we don't use this in any perf sensitive code paths currently.  There
    // might be a much faster way to implement this.
    static inline int popcount_no_hw(uint64_t x) {
        int count = 0;

        for (; x != 0; ++count) {
            x &= x - 1;
        }

        return count;
    }

    // Returns the number of set bits in x
    static inline int popcount(uint64_t x) {
        if (LIKELY(CpuInfo::is_supported(CpuInfo::POPCNT))) {
            return __builtin_popcountl(x);
        } else {
            return popcount_no_hw(x);
        }
    }

    // Returns the 'num_bits' least-significant bits of 'v'.
    static inline uint64_t trailing_bits(uint64_t v, int num_bits) {
        if (UNLIKELY(num_bits == 0)) {
            return 0;
        }

        if (UNLIKELY(num_bits >= 64)) {
            return v;
        }

        int n = 64 - num_bits;
        return (v << n) >> n;
    }

    template <typename T>
    static std::string IntToByteBuffer(T input) {
        std::string buffer;
        T value = input;
        for (int i = 0; i < sizeof(value); ++i) {
            // Applies a mask for a byte range on the input.
            signed char value_to_save = value & 0XFF;
            buffer.push_back(value_to_save);
            // Remove the just processed part from the input so that we can exit early if there
            // is nothing left to process.
            value >>= 8;
            if (value == 0 && value_to_save >= 0) {
                break;
            }
            if (value == -1 && value_to_save < 0) {
                break;
            }
        }
        std::reverse(buffer.begin(), buffer.end());
        return buffer;
    }

    // Returns ceil(log2(x)).
    // TODO: this could be faster if we use __builtin_clz.  Fix this if this ever shows up
    // in a hot path.
    static inline int log2(uint64_t x) {
        DCHECK_GT(x, 0);

        if (x == 1) {
            return 0;
        }

        // Compute result = ceil(log2(x))
        //                = floor(log2(x - 1)) + 1, for x > 1
        // by finding the position of the most significant bit (1-indexed) of x - 1
        // (floor(log2(n)) = MSB(n) (0-indexed))
        --x;
        int result = 1;

        while (x >>= 1) {
            ++result;
        }

        return result;
    }

    // Returns the rounded up to 64 multiple. Used for conversions of bits to i64.
    static inline uint32_t round_up_numi64(uint32_t bits) { return (bits + 63) >> 6; }

    // Returns the rounded up to 32 multiple. Used for conversions of bits to i32.
    constexpr static inline uint32_t round_up_numi32(uint32_t bits) { return (bits + 31) >> 5; }

    /// Returns the smallest power of two that contains v. If v is a power of two, v is
    /// returned. Taken from
    /// http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    static inline int64_t RoundUpToPowerOfTwo(int64_t v) {
        --v;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        ++v;
        return v;
    }

    // Wrap the gutil/ version for convenience.
    static inline int Log2FloorNonZero64(uint64_t n) { return 63 ^ __builtin_clzll(n); }

    // Wrap the gutil/ version for convenience.
    static inline int Log2Floor64(uint64_t n) { return n == 0 ? -1 : 63 ^ __builtin_clzll(n); }

    static inline int Log2Ceiling64(uint64_t n) {
        int floor = Log2Floor64(n);
        // Check if zero or a power of two. This pattern is recognised by gcc and optimised
        // into branch-free code.
        if (0 == (n & (n - 1))) {
            return floor;
        } else {
            return floor + 1;
        }
    }

    static inline int Log2CeilingNonZero64(uint64_t n) {
        int floor = Log2FloorNonZero64(n);
        // Check if zero or a power of two. This pattern is recognised by gcc and optimised
        // into branch-free code.
        if (0 == (n & (n - 1))) {
            return floor;
        } else {
            return floor + 1;
        }
    }

    // Returns the rounded up to 64 multiple. Used for conversions of bits to i64.
    static inline uint32_t round_up_numi_64(uint32_t bits) { return (bits + 63) >> 6; }

    constexpr static inline int64_t Ceil(int64_t value, int64_t divisor) {
        return value / divisor + (value % divisor != 0);
    }

    constexpr static inline bool IsPowerOf2(int64_t value) { return (value & (value - 1)) == 0; }

    constexpr static inline int64_t RoundDown(int64_t value, int64_t factor) {
        return (value / factor) * factor;
    }

    /// Specialized round up and down functions for frequently used factors,
    /// like 8 (bits->bytes), 32 (bits->i32), and 64 (bits->i64)
    /// Returns the rounded up number of bytes that fit the number of bits.
    constexpr static inline uint32_t RoundUpNumBytes(uint32_t bits) { return (bits + 7) >> 3; }

    template <typename T>
    static inline T RoundDownToPowerOf2(T value, T factor) {
        static_assert(std::is_integral<T>::value, "T must be an integral type");
        DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
        return value & ~(factor - 1);
    }

    // Returns the ceil of value/divisor
    static inline int Ceil(int value, int divisor) {
        return value / divisor + (value % divisor != 0);
    }

    // Returns the 'num_bits' least-significant bits of 'v'.
    static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
        if (num_bits == 0) [[unlikely]] {
            return 0;
        }
        if (num_bits >= 64) [[unlikely]] {
            return v;
        }
        int n = 64 - num_bits;
        return (v << n) >> n;
    }

    static inline uint64_t ShiftLeftZeroOnOverflow(uint64_t v, int num_bits) {
        if (num_bits >= 64) [[unlikely]] {
            return 0;
        }
        return v << num_bits;
    }

    static inline uint64_t ShiftRightZeroOnOverflow(uint64_t v, int num_bits) {
        if (num_bits >= 64) [[unlikely]] {
            return 0;
        }
        return v >> num_bits;
    }
};

} // namespace doris
