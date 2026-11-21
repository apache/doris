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

#include <bit>
#include <cstdint>
#include <cstring>

namespace doris {

// MurmurHash2, 64 bit version. It was modified in order to provide the same result in big and
// little endian archs (endian neutral).
//
// Its results are persisted (HLL values, path version 1 shards), so it MUST NOT change. Inline as
// BE calls it per row.
inline uint64_t murmur_hash64A(const void* key, int64_t len, unsigned int seed) {
    const uint64_t m = 0xc6a4a7935bd1e995ULL;
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const uint8_t* data = (const uint8_t*)key;
    const uint8_t* end = data + (len - (len & 7));

    while (data != end) {
        uint64_t k;
        if constexpr (std::endian::native == std::endian::big) {
            k = (uint64_t)data[0];
            k |= (uint64_t)data[1] << 8;
            k |= (uint64_t)data[2] << 16;
            k |= (uint64_t)data[3] << 24;
            k |= (uint64_t)data[4] << 32;
            k |= (uint64_t)data[5] << 40;
            k |= (uint64_t)data[6] << 48;
            k |= (uint64_t)data[7] << 56;
        } else if constexpr (std::endian::native == std::endian::little) {
            memcpy(&k, data, sizeof(k));
        } else {
            static_assert(std::endian::native == std::endian::big ||
                                  std::endian::native == std::endian::little,
                          "Unsupported endianness");
        }

        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        data += 8;
    }

    switch (len & 7) {
    case 7:
        h ^= (uint64_t)data[6] << 48;
        [[fallthrough]];
    case 6:
        h ^= (uint64_t)data[5] << 40;
        [[fallthrough]];
    case 5:
        h ^= (uint64_t)data[4] << 32;
        [[fallthrough]];
    case 4:
        h ^= (uint64_t)data[3] << 24;
        [[fallthrough]];
    case 3:
        h ^= (uint64_t)data[2] << 16;
        [[fallthrough]];
    case 2:
        h ^= (uint64_t)data[1] << 8;
        [[fallthrough]];
    case 1:
        h ^= (uint64_t)data[0];
        h *= m;
    }

    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

} // namespace doris
