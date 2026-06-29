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

#include "snii/encoding/varint.h"

#include <gtest/gtest.h>

#include <cstdint>

#include "common/status.h"

using namespace snii;

TEST(SniiVarint, RoundTrip32) {
    for (uint32_t v : {0U, 1U, 127U, 128U, 300U, 16384U, 0xFFFFFFFFU}) {
        uint8_t buf[10];
        size_t n = encode_varint32(v, buf);
        EXPECT_EQ(n, varint_len(v));
        uint32_t out;
        const uint8_t* next;
        ASSERT_TRUE(decode_varint32(buf, buf + n, &out, &next).ok());
        EXPECT_EQ(out, v);
        EXPECT_EQ(next, buf + n);
    }
}

TEST(SniiVarint, RoundTrip64) {
    for (uint64_t v : {0ULL, 1ULL, 127ULL, 128ULL, 1ULL << 35, 0xFFFFFFFFFFFFFFFFULL}) {
        uint8_t buf[10];
        size_t n = encode_varint64(v, buf);
        uint64_t out;
        const uint8_t* next;
        ASSERT_TRUE(decode_varint64(buf, buf + n, &out, &next).ok());
        EXPECT_EQ(out, v);
    }
}

TEST(SniiVarint, TruncatedFails) {
    uint8_t buf[1] = {0x80}; // continuation bit set but no subsequent byte
    uint32_t out;
    const uint8_t* next;
    EXPECT_FALSE(decode_varint32(buf, buf + 1, &out, &next).ok());
}

TEST(SniiVarint, Overflow32Fails) {
    // encode a value > 2^32-1, decoding with decode_varint32 should fail
    uint8_t buf[10];
    size_t n = encode_varint64((1ULL << 33), buf);
    uint32_t out;
    const uint8_t* next;
    EXPECT_FALSE(decode_varint32(buf, buf + n, &out, &next).ok());
}

TEST(SniiVarint, ZigzagRoundTrip) {
    const int64_t cases[] = {0, -1, 1, -1000, 1000, INT64_MIN, INT64_MAX};
    for (int64_t v : cases) {
        EXPECT_EQ(zigzag_decode(zigzag_encode(v)), v);
    }
}
