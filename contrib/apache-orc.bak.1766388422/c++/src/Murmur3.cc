/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Murmur3.hh"
#include "Adaptor.hh"

#define ROTL64(x, r) ((x << r) | (x >> (64 - r)))

namespace orc {

  inline uint64_t rotl64(uint64_t x, int8_t r) {
    return (x << r) | (x >> (64 - r));
  }

  inline uint64_t Murmur3::fmix64(uint64_t value) {
    value ^= (value >> 33);
    value *= 0xff51afd7ed558ccdL;
    value ^= (value >> 33);
    value *= 0xc4ceb9fe1a85ec53L;
    value ^= (value >> 33);
    return value;
  }

  uint64_t Murmur3::hash64(const uint8_t* data, uint32_t len) {
    return hash64(data, len, DEFAULT_SEED);
  }

  DIAGNOSTIC_PUSH

#if defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wimplicit-fallthrough")
#endif

  uint64_t Murmur3::hash64(const uint8_t* data, uint32_t len, uint32_t seed) {
    uint64_t h = seed;
    uint32_t blocks = len >> 3;

    const uint64_t* src = reinterpret_cast<const uint64_t*>(data);
    uint64_t c1 = 0x87c37b91114253d5L;
    uint64_t c2 = 0x4cf5ad432745937fL;
    for (uint32_t i = 0; i < blocks; i++) {
      uint64_t k = src[i];
      k *= c1;
      k = ROTL64(k, 31);
      k *= c2;

      h ^= k;
      h = ROTL64(h, 27);
      h = h * 5 + 0x52dce729;
    }

    uint64_t k = 0;
    uint32_t idx = blocks << 3;
    switch (len - idx) {
      case 7:
        k ^= static_cast<uint64_t>(data[idx + 6]) << 48;
      case 6:
        k ^= static_cast<uint64_t>(data[idx + 5]) << 40;
      case 5:
        k ^= static_cast<uint64_t>(data[idx + 4]) << 32;
      case 4:
        k ^= static_cast<uint64_t>(data[idx + 3]) << 24;
      case 3:
        k ^= static_cast<uint64_t>(data[idx + 2]) << 16;
      case 2:
        k ^= static_cast<uint64_t>(data[idx + 1]) << 8;
      case 1:
        k ^= static_cast<uint64_t>(data[idx + 0]);

        k *= c1;
        k = ROTL64(k, 31);
        k *= c2;
        h ^= k;
    }

    h ^= len;
    h = fmix64(h);
    return h;
  }

  DIAGNOSTIC_POP

}  // namespace orc
