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

#include "snii/encoding/pfor.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/byte_source.h"

using namespace snii;

static void roundtrip(const std::vector<uint32_t>& v) {
    ByteSink sink;
    pfor_encode(v.data(), v.size(), &sink);
    ByteSource src(sink.view());
    std::vector<uint32_t> out(v.size());
    ASSERT_TRUE(pfor_decode(&src, v.size(), out.data()).ok());
    EXPECT_EQ(out, v);
}

TEST(SniiPfor, Uniform) {
    roundtrip(std::vector<uint32_t>(200, 5));
}

TEST(SniiPfor, Ramp) {
    std::vector<uint32_t> v;
    for (uint32_t i = 0; i < 256; ++i) {
        v.push_back(i);
    }
    roundtrip(v);
}

TEST(SniiPfor, WithOutliers) {
    std::vector<uint32_t> v(128, 3);
    v[10] = 1000000;
    v[77] = 999;
    roundtrip(v);
}

TEST(SniiPfor, AllZero) {
    roundtrip(std::vector<uint32_t>(64, 0));
}

TEST(SniiPfor, MaxValues) {
    std::vector<uint32_t> v(32, 0xFFFFFFFFU);
    v[0] = 0;
    roundtrip(v);
}

// Exhaustive: every bit width 0..32, capped random values, across a spread of n
// (including non-power-of-two and prime lengths) so the bit-unpacker's tail handling
// (partial trailing 64-bit word / partial byte) is exercised for each width. A
// deterministic LCG keeps it reproducible without Math.random.
TEST(SniiPfor, AllWidthsRandomLengths) {
    uint64_t rng = 0x9E3779B97F4A7C15ULL;
    auto next = [&rng]() {
        rng = rng * 6364136223846793005ULL + 1442695040888963407ULL;
        return static_cast<uint32_t>(rng >> 32);
    };
    const size_t lens[] = {1, 2, 3, 7, 8, 9, 31, 32, 33, 63, 64, 65, 127, 200, 257};
    for (int w = 0; w <= 32; ++w) {
        const uint32_t cap = (w >= 32) ? 0xFFFFFFFFU : ((1U << w) - 1U);
        for (size_t n : lens) {
            std::vector<uint32_t> v(n);
            for (size_t i = 0; i < n; ++i) {
                v[i] = (w == 0) ? 0U : (next() & cap);
            }
            roundtrip(v); // byte-identical decode required for every (w, n)
        }
    }
}

// A few exceptions sprinkled into otherwise-narrow data, at boundary indices, to
// cover the exception-patch path alongside the fast unpack.
TEST(SniiPfor, ExceptionsAtBoundaries) {
    std::vector<uint32_t> v(130, 2);
    v[0] = 0x00ABCDEF;  // first
    v[63] = 0x12345678; // around the 64-value word boundary
    v[64] = 0x0BADF00D;
    v[129] = 0xFFFFFFFF; // last
    roundtrip(v);
}
