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

#ifndef DORIS_BE_SRC_COMMON_UTIL_BIT_UTIL_H
#define DORIS_BE_SRC_COMMON_UTIL_BIT_UTIL_H

#include <endian.h>

#include "common/compiler_util.h"
#include "gutil/bits.h"
#include "gutil/port.h"
#include "util/cpu_info.h"

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

    // Swaps the byte order (i.e. endianess)
    static inline int64_t byte_swap(int64_t value) { return __builtin_bswap64(value); }
    static inline uint64_t byte_swap(uint64_t value) {
        return static_cast<uint64_t>(__builtin_bswap64(value));
    }
    static inline int32_t byte_swap(int32_t value) { return __builtin_bswap32(value); }
    static inline uint32_t byte_swap(uint32_t value) {
        return static_cast<uint32_t>(__builtin_bswap32(value));
    }
    static inline int16_t byte_swap(int16_t value) {
        return (((value >> 8) & 0xff) | ((value & 0xff) << 8));
    }
    static inline uint16_t byte_swap(uint16_t value) {
        return static_cast<uint16_t>(byte_swap(static_cast<int16_t>(value)));
    }

    // Write the swapped bytes into dst. len must be 1, 2, 4 or 8.
    static inline void byte_swap(void* dst, void* src, int len) {
        switch (len) {
        case 1:
            *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<int8_t*>(src);
            break;

        case 2:
            *reinterpret_cast<int16_t*>(dst) = byte_swap(*reinterpret_cast<int16_t*>(src));
            break;

        case 4:
            *reinterpret_cast<int32_t*>(dst) = byte_swap(*reinterpret_cast<int32_t*>(src));
            break;

        case 8:
            *reinterpret_cast<int64_t*>(dst) = byte_swap(*reinterpret_cast<int64_t*>(src));
            break;

        default:
            DCHECK(false);
        }
    }

    // Returns the rounded up to 64 multiple. Used for conversions of bits to i64.
    static inline uint32_t round_up_numi64(uint32_t bits) { return (bits + 63) >> 6; }

#if __BYTE_ORDER == __LITTLE_ENDIAN
    // Converts to big endian format (if not already in big endian).
    static inline int64_t big_endian(int64_t value) { return byte_swap(value); }
    static inline uint64_t big_endian(uint64_t value) { return byte_swap(value); }
    static inline int32_t big_endian(int32_t value) { return byte_swap(value); }
    static inline uint32_t big_endian(uint32_t value) { return byte_swap(value); }
    static inline int16_t big_endian(int16_t value) { return byte_swap(value); }
    static inline uint16_t big_endian(uint16_t value) { return byte_swap(value); }
#else
    static inline int64_t big_endian(int64_t val) { return val; }
    static inline uint64_t big_endian(uint64_t val) { return val; }
    static inline int32_t big_endian(int32_t val) { return val; }
    static inline uint32_t big_endian(uint32_t val) { return val; }
    static inline int16_t big_endian(int16_t val) { return val; }
    static inline uint16_t big_endian(uint16_t val) { return val; }
#endif

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
    static inline int Log2FloorNonZero64(uint64_t n) { return Bits::Log2FloorNonZero64(n); }

    // Wrap the gutil/ version for convenience.
    static inline int Log2Floor64(uint64_t n) { return Bits::Log2Floor64(n); }

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

    /// Non hw accelerated pop count.
    /// TODO: we don't use this in any perf sensitive code paths currently.  There
    /// might be a much faster way to implement this.
    static inline int PopcountNoHw(uint64_t x) {
        int count = 0;
        for (; x != 0; ++count) x &= x - 1;
        return count;
    }

    /// Returns the number of set bits in x
    static inline int Popcount(uint64_t x) {
        //if (LIKELY(CpuInfo::is_supported(CpuInfo::POPCNT))) {
        //  return POPCNT_popcnt_u64(x);
        //} else {
        return PopcountNoHw(x);
        // }
    }

    // Compute correct population count for various-width signed integers
    template <typename T>
    static inline int PopcountSigned(T v) {
        // Converting to same-width unsigned then extending preserves the bit pattern.
        return BitUtil::Popcount(static_cast<typename std::make_unsigned<T>::type>(v));
    }

    /// Logical right shift for signed integer types
    /// This is needed because the C >> operator does arithmetic right shift
    /// Negative shift amounts lead to undefined behavior
    template <typename T>
    constexpr static T ShiftRightLogical(T v, int shift) {
        // Conversion to unsigned ensures most significant bits always filled with 0's
        return static_cast<typename std::make_unsigned<T>::type>(v) >> shift;
    }

    /// Get an specific bit of a numeric type
    template <typename T>
    static inline int8_t GetBit(T v, int bitpos) {
        T masked = v & (static_cast<T>(0x1) << bitpos);
        return static_cast<int8_t>(ShiftRightLogical(masked, bitpos));
    }

    /// Set a specific bit to 1
    /// Behavior when bitpos is negative is undefined
    template <typename T>
    constexpr static T SetBit(T v, int bitpos) {
        return v | (static_cast<T>(0x1) << bitpos);
    }

    /// Set a specific bit to 0
    /// Behavior when bitpos is negative is undefined
    template <typename T>
    constexpr static T UnsetBit(T v, int bitpos) {
        return v & ~(static_cast<T>(0x1) << bitpos);
    }

    /// Returns 'value' rounded up to the nearest multiple of 'factor' when factor is
    /// a power of two
    static inline int64_t RoundUpToPowerOf2(int64_t value, int64_t factor) {
        DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
        return (value + (factor - 1)) & ~(factor - 1);
    }

    // Returns the ceil of value/divisor
    static inline int Ceil(int value, int divisor) {
        return value / divisor + (value % divisor != 0);
    }

    // Returns the 'num_bits' least-significant bits of 'v'.
    static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
        if (PREDICT_FALSE(num_bits == 0)) return 0;
        if (PREDICT_FALSE(num_bits >= 64)) return v;
        int n = 64 - num_bits;
        return (v << n) >> n;
    }

    static inline uint64_t ShiftLeftZeroOnOverflow(uint64_t v, int num_bits) {
        if (PREDICT_FALSE(num_bits >= 64)) return 0;
        return v << num_bits;
    }

    static inline uint64_t ShiftRightZeroOnOverflow(uint64_t v, int num_bits) {
        if (PREDICT_FALSE(num_bits >= 64)) return 0;
        return v >> num_bits;
    }
};

} // namespace doris

#endif
