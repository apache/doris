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

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

#include "util/bit_packing.h"
#include "util/bit_packing.inline.h"

namespace doris {

// H2: UnpackValue() in bit_packing.inline.h loads 32/64-bit words through
// reinterpret_cast of an arbitrary byte pointer. Feed UnpackValues buffers at
// every possible misalignment (offset 1..7) and check decoded values against
// a straightforward reference implementation. Guards the unaligned_load
// hardening; also directly exercisable under -fsanitize=alignment.
namespace {

// Reference bit unpacker: reads the stream one bit at a time (LSB-first
// within each byte, matching the layout documented in bit_packing.h).
uint64_t ref_unpack(const uint8_t* in, int bit_width, int64_t value_idx) {
    uint64_t result = 0;
    int64_t first_bit = value_idx * bit_width;
    for (int b = 0; b < bit_width; ++b) {
        int64_t bit = first_bit + b;
        uint64_t v = (in[bit / 8] >> (bit % 8)) & 1;
        result |= v << b;
    }
    return result;
}

void run_unaligned_case(int bit_width, int offset, std::mt19937_64* rng) {
    constexpr int64_t kNumValues = 64;
    const int64_t in_bytes = (bit_width * kNumValues + 7) / 8;
    std::vector<uint8_t> backing(in_bytes + 16, 0);
    // vector<uint8_t> storage is not guaranteed 8-aligned; find an aligned
    // base inside it first so that 'in' lands exactly at 'offset' mod 8.
    uint8_t* aligned_base =
            reinterpret_cast<uint8_t*>((reinterpret_cast<uintptr_t>(backing.data()) + 7) & ~7ULL);
    for (int i = 0; i < in_bytes; ++i) {
        aligned_base[offset + i] = static_cast<uint8_t>((*rng)());
    }
    const uint8_t* in = aligned_base + offset;
    ASSERT_EQ(offset, static_cast<int>(reinterpret_cast<uintptr_t>(in) % 8));

    std::vector<uint64_t> out(kNumValues, 0);
    auto [end, read] =
            BitPacking::UnpackValues<uint64_t>(bit_width, in, in_bytes, kNumValues, out.data());
    ASSERT_EQ(kNumValues, read);
    for (int64_t i = 0; i < kNumValues; ++i) {
        uint64_t expected = ref_unpack(in, bit_width, i);
        EXPECT_EQ(expected, out[i]) << "bit_width=" << bit_width << " offset=" << offset
                                    << " value_idx=" << i;
    }
}

} // namespace

TEST(BitPackingUnalignedTest, UnpackFromMisalignedBuffers) {
    std::mt19937_64 rng(20260815);
    // Widths that exercise the 32-bit path, the 64-bit path and the
    // three-word path (e.g. width 63 spans words) in UnpackValue.
    for (int bit_width : {1, 3, 7, 8, 12, 16, 21, 31, 32, 33, 48, 63, 64}) {
        for (int offset = 1; offset < 8; ++offset) {
            run_unaligned_case(bit_width, offset, &rng);
        }
    }
}

} // namespace doris
