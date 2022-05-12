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
#include "vec/common/hash_table/hash.h"

template <>
uint64_t Hash(doris::vectorized::UInt64 val) {
    return murmurhash64(val);
}

template <>
uint64_t Hash(doris::vectorized::Int64 val) {
    return murmurhash64((doris::vectorized::UInt64)val);
}

template <>
uint64_t Hash(doris::vectorized::Int128 val) {
    return murmurhash64((doris::vectorized::UInt64)val) ^
           murmurhash64((doris::vectorized::UInt64)(val >> 64));
}

template <>
uint64_t Hash(doris::vectorized::Float32 val) {
    return std::hash<doris::vectorized::Float32> {}(val);
}

template <>
uint64_t Hash(doris::vectorized::Float64 val) {
    return std::hash<doris::vectorized::Float64> {}(val);
}

// MIT License
// Copyright (c) 2018-2021 Martin Ankerl
// https://github.com/martinus/robin-hood-hashing/blob/3.11.5/LICENSE
uint64_t HashBytes(void* ptr, size_t len) noexcept {
    static constexpr doris::vectorized::UInt64 M = UINT64_C(0xc6a4a7935bd1e995);
    static constexpr doris::vectorized::UInt64 SEED = UINT64_C(0xe17a1465);
    static constexpr unsigned int R = 47;

    auto const* const data64 = static_cast<doris::vectorized::UInt64 const*>(ptr);
    doris::vectorized::UInt64 h = SEED ^ (len * M);

    size_t const n_blocks = len / 8;
    for (size_t i = 0; i < n_blocks; ++i) {
        doris::vectorized::UInt64 res;
        memcpy(&res, ptr, sizeof(res));
        auto k = res;

        k *= M;
        k ^= k >> R;
        k *= M;

        h ^= k;
        h *= M;
    }

    auto const* const data8 = reinterpret_cast<doris::vectorized::UInt8 const*>(data64 + n_blocks);
    switch (len & 7U) {
    case 7:
        h ^= static_cast<doris::vectorized::UInt64>(data8[6]) << 48U;
        [[fallthrough]];
    case 6:
        h ^= static_cast<doris::vectorized::UInt64>(data8[5]) << 40U;
        [[fallthrough]];
    case 5:
        h ^= static_cast<doris::vectorized::UInt64>(data8[4]) << 32U;
        [[fallthrough]];
    case 4:
        h ^= static_cast<doris::vectorized::UInt64>(data8[3]) << 24U;
        [[fallthrough]];
    case 3:
        h ^= static_cast<doris::vectorized::UInt64>(data8[2]) << 16U;
        [[fallthrough]];
    case 2:
        h ^= static_cast<doris::vectorized::UInt64>(data8[1]) << 8U;
        [[fallthrough]];
    case 1:
        h ^= static_cast<doris::vectorized::UInt64>(data8[0]);
        h *= M;
        [[fallthrough]];
    default:
        break;
    }
    h ^= h >> R;
    h *= M;
    h ^= h >> R;
    return static_cast<uint64_t>(h);
}

uint64_t Hash(const char* val, size_t size) {
    return HashBytes((void*)val, size);
}

template <>
uint64_t Hash(StringRef val) {
    return Hash(val.data, val.size);
}
