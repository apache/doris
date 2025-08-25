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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/Uint128.h
// and modified by Doris

#pragma once

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <tuple>

#include "util/hash/city.h"
#include "util/sse_util.hpp"
#include "vec/core/extended_types.h"
#include "vec/core/types.h"

namespace doris::vectorized {

using UInt128 = wide::UInt128;

template <>
inline constexpr bool IsNumber<UInt128> = true;

#if defined(__SSE4_2__) || defined(__aarch64__)

struct UInt128HashCRC32 {
    size_t operator()(const UInt128& x) const {
        UInt64 crc = -1ULL;
        crc = _mm_crc32_u64(crc, x.low());
        crc = _mm_crc32_u64(crc, x.high());
        return crc;
    }
};

#else

/// On other platforms we do not use CRC32. NOTE This can be confusing.
struct UInt128HashCRC32 : public UInt128Hash {};

#endif

struct UInt128TrivialHash {
    size_t operator()(UInt128 x) const { return x.low(); }
};

using UInt256 = wide::UInt256;

#pragma pack(1)
struct UInt136 {
    UInt8 a;
    UInt64 b;
    UInt64 c;

    bool operator==(const UInt136& rhs) const { return a == rhs.a && b == rhs.b && c == rhs.c; }
};
#pragma pack()

} // namespace doris::vectorized

/// Overload hash for type casting
template <>
struct std::hash<doris::vectorized::UInt128> {
    size_t operator()(const doris::vectorized::UInt128& u) const {
        return doris::util_hash::HashLen16(u.low(), u.high());
    }
};
