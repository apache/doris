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

#include <iomanip>
#include <sstream>
#include <tuple>

#include "gutil/hash/city.h"
#include "gutil/hash/hash128to64.h"
#include "vec/core/types.h"

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

namespace doris::vectorized {

/// For aggregation by SipHash, UUID type or concatenation of several fields.
struct UInt128 {
/// Suppress gcc7 warnings: 'prev_key.doris::vectorized::UInt128::low' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    /// This naming assumes little endian.
    UInt64 low;
    UInt64 high;

    UInt128() = default;
    explicit UInt128(const UInt64 low_, const UInt64 high_) : low(low_), high(high_) {}
    explicit UInt128(const UInt64 rhs) : low(rhs), high() {}

    auto tuple() const { return std::tie(high, low); }

    String to_hex_string() const {
        std::ostringstream os;
        os << std::setw(16) << std::setfill('0') << std::hex << high << low;
        return String(os.str());
    }

    bool inline operator==(const UInt128 rhs) const { return tuple() == rhs.tuple(); }
    bool inline operator!=(const UInt128 rhs) const { return tuple() != rhs.tuple(); }
    bool inline operator<(const UInt128 rhs) const { return tuple() < rhs.tuple(); }
    bool inline operator<=(const UInt128 rhs) const { return tuple() <= rhs.tuple(); }
    bool inline operator>(const UInt128 rhs) const { return tuple() > rhs.tuple(); }
    bool inline operator>=(const UInt128 rhs) const { return tuple() >= rhs.tuple(); }

    template <typename T>
    bool inline operator==(const T rhs) const {
        return *this == UInt128(rhs);
    }
    template <typename T>
    bool inline operator!=(const T rhs) const {
        return *this != UInt128(rhs);
    }
    template <typename T>
    bool inline operator>=(const T rhs) const {
        return *this >= UInt128(rhs);
    }
    template <typename T>
    bool inline operator>(const T rhs) const {
        return *this > UInt128(rhs);
    }
    template <typename T>
    bool inline operator<=(const T rhs) const {
        return *this <= UInt128(rhs);
    }
    template <typename T>
    bool inline operator<(const T rhs) const {
        return *this < UInt128(rhs);
    }

    template <typename T>
    explicit operator T() const {
        return static_cast<T>(low);
    }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    UInt128& operator=(const UInt64 rhs) {
        low = rhs;
        high = 0;
        return *this;
    }
};

template <typename T>
bool inline operator==(T a, const UInt128 b) {
    return UInt128(a) == b;
}
template <typename T>
bool inline operator!=(T a, const UInt128 b) {
    return UInt128(a) != b;
}
template <typename T>
bool inline operator>=(T a, const UInt128 b) {
    return UInt128(a) >= b;
}
template <typename T>
bool inline operator>(T a, const UInt128 b) {
    return UInt128(a) > b;
}
template <typename T>
bool inline operator<=(T a, const UInt128 b) {
    return UInt128(a) <= b;
}
template <typename T>
bool inline operator<(T a, const UInt128 b) {
    return UInt128(a) < b;
}

template <>
inline constexpr bool IsNumber<UInt128> = true;
template <>
struct TypeName<UInt128> {
    static const char* get() { return "UInt128"; }
};
template <>
struct TypeId<UInt128> {
    static constexpr const TypeIndex value = TypeIndex::UInt128;
};

struct UInt128Hash {
    size_t operator()(UInt128 x) const { return Hash128to64({x.low, x.high}); }
};

#ifdef __SSE4_2__

struct UInt128HashCRC32 {
    size_t operator()(UInt128 x) const {
        UInt64 crc = -1ULL;
        crc = _mm_crc32_u64(crc, x.low);
        crc = _mm_crc32_u64(crc, x.high);
        return crc;
    }
};

#else

/// On other platforms we do not use CRC32. NOTE This can be confusing.
struct UInt128HashCRC32 : public UInt128Hash {};

#endif

struct UInt128TrivialHash {
    size_t operator()(UInt128 x) const { return x.low; }
};

/** Used for aggregation, for putting a large number of constant-length keys in a hash table.
  */
struct UInt256 {
/// Suppress gcc7 warnings: 'prev_key.doris::vectorized::UInt256::a' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    UInt64 a;
    UInt64 b;
    UInt64 c;
    UInt64 d;

    bool operator==(const UInt256 rhs) const {
        return a == rhs.a && b == rhs.b && c == rhs.c && d == rhs.d;
    }

    bool operator!=(const UInt256 rhs) const { return !operator==(rhs); }

    bool operator==(const UInt64 rhs) const { return a == rhs && b == 0 && c == 0 && d == 0; }
    bool operator!=(const UInt64 rhs) const { return !operator==(rhs); }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    UInt256& operator=(const UInt64 rhs) {
        a = rhs;
        b = 0;
        c = 0;
        d = 0;
        return *this;
    }
};

struct UInt256Hash {
    size_t operator()(UInt256 x) const {
        /// NOTE suboptimal
        return Hash128to64({Hash128to64({x.a, x.b}), Hash128to64({x.c, x.d})});
    }
};

#ifdef __SSE4_2__

struct UInt256HashCRC32 {
    size_t operator()(UInt256 x) const {
        UInt64 crc = -1ULL;
        crc = _mm_crc32_u64(crc, x.a);
        crc = _mm_crc32_u64(crc, x.b);
        crc = _mm_crc32_u64(crc, x.c);
        crc = _mm_crc32_u64(crc, x.d);
        return crc;
    }
};

#else

/// We do not need to use CRC32 on other platforms. NOTE This can be confusing.
struct UInt256HashCRC32 : public UInt256Hash {};

#endif
} // namespace doris::vectorized

/// Overload hash for type casting
namespace std {
template <>
struct hash<doris::vectorized::UInt128> {
    size_t operator()(const doris::vectorized::UInt128& u) const {
        return Hash128to64({u.low, u.high});
    }
};

template <>
struct is_signed<doris::vectorized::UInt128> {
    static constexpr bool value = false;
};

template <>
struct is_unsigned<doris::vectorized::UInt128> {
    static constexpr bool value = true;
};

template <>
struct is_integral<doris::vectorized::UInt128> {
    static constexpr bool value = true;
};

// Operator +, -, /, *, % aren't implemented so it's not an arithmetic type
template <>
struct is_arithmetic<doris::vectorized::UInt128> {
    static constexpr bool value = false;
};
} // namespace std
