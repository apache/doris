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
#include <cstdint>
#include <type_traits>

#include "runtime/decimalv2_value.h"
#include "util/types.h"

namespace doris {
union TypeConverter {
    uint64_t u64;
    int64_t i64;
    uint32_t u32[2];
    int32_t i32[2];
    uint8_t u8[8];
    float flt[2];
    double dbl;
};

template <typename C0, typename C1, typename T0, typename T1>
inline constexpr bool match_v = std::is_same_v<C0, C1>&& std::is_same_v<T0, T1>;

union DecimalInt128Union {
    DecimalV2Value decimal;
    PackedInt128 packed128;
};

static_assert(sizeof(DecimalV2Value) == sizeof(PackedInt128));

// similar to reinterpret_cast but won't break strict-aliasing rules
template <typename From, typename To>
To binary_cast(From from) {
    constexpr bool from_u64_to_db = match_v<From, uint64_t, To, double>;
    constexpr bool from_i64_to_db = match_v<From, int64_t, To, double>;
    constexpr bool from_db_to_i64 = match_v<From, double, To, int64_t>;
    constexpr bool from_db_to_u64 = match_v<From, double, To, uint64_t>;
    constexpr bool from_decv2_to_packed128 = match_v<From, DecimalV2Value, To, PackedInt128>;
    
    static_assert(from_u64_to_db || from_i64_to_db || from_db_to_i64 || from_db_to_u64 ||
                  from_decv2_to_packed128);

    if constexpr (from_u64_to_db) {
        TypeConverter conv;
        conv.u64 = from;
        return conv.dbl;
    } else if constexpr (from_i64_to_db) {
        TypeConverter conv;
        conv.i64 = from;
        return conv.dbl;
    } else if constexpr (from_db_to_i64) {
        TypeConverter conv;
        conv.dbl = from;
        return conv.i64;
    } else if constexpr (from_db_to_u64) {
        TypeConverter conv;
        conv.dbl = from;
        return conv.u64;
    } else if constexpr (from_decv2_to_packed128) {
        DecimalInt128Union conv;
        conv.decimal = from;
        return conv.packed128;
    } else {
        __builtin_unreachable();
    }
}

} // namespace doris
