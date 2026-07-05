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

#include <iterator>
#include "storage/index/snii/encoding/byte_source.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"

using namespace doris::snii;

TEST(SniiByteSource, RoundTripWithSink) {
    ByteSink s;
    s.put_fixed32(0xDEADBEEF);
    s.put_varint64(123456789);
    s.put_zigzag(-42);
    ByteSource src(s.view());
    uint32_t a;
    uint64_t b;
    int64_t c;
    ASSERT_TRUE(src.get_fixed32(&a).ok());
    EXPECT_EQ(a, 0xDEADBEEFU);
    ASSERT_TRUE(src.get_varint64(&b).ok());
    EXPECT_EQ(b, 123456789U);
    ASSERT_TRUE(src.get_zigzag(&c).ok());
    EXPECT_EQ(c, -42);
    EXPECT_TRUE(src.eof());
}

TEST(SniiByteSource, GetBytesAdvances) {
    ByteSink s;
    const uint8_t p[] = {1, 2, 3, 4, 5};
    s.put_bytes(Slice(p, 5));
    ByteSource src(s.view());
    Slice got;
    ASSERT_TRUE(src.get_bytes(3, &got).ok());
    ASSERT_EQ(got.size(), 3U);
    EXPECT_EQ(got[0], 1U);
    EXPECT_EQ(src.remaining(), 2U);
}

TEST(SniiByteSource, OverrunFails) {
    uint8_t one[1] = {0x01};
    ByteSource src(Slice(one, 1));
    uint32_t a;
    EXPECT_FALSE(src.get_fixed32(&a).ok());
}

// skip_varints must advance the cursor EXACTLY as decoding the same varints
// would -- across 1..5-byte encodings and back-to-back values -- so a CSR
// selective decode can skip non-candidate docs' position deltas losslessly.
TEST(SniiByteSource, SkipVarintsAdvancesLikeDecode) {
    ByteSink s;
    const uint64_t vals[] = {0, 1, 127, 128, 300, 16384, 0xFFFFFFFFULL, 42, 1u << 21, 5};
    for (uint64_t v : vals) s.put_varint64(v);
    s.put_u8(0xAB); // sentinel after the varints

    // Decode-advance reference: consume all values, record end position.
    ByteSource ref(s.view());
    for (size_t i = 0; i < std::size(vals); ++i) {
        uint64_t v = 0;
        ASSERT_TRUE(ref.get_varint64(&v).ok());
        EXPECT_EQ(v, vals[i]);
    }
    const size_t decoded_pos = ref.position();

    // Skip-advance: skipping the same count must land at the identical position.
    ByteSource skp(s.view());
    ASSERT_TRUE(skp.skip_varints(std::size(vals)).ok());
    EXPECT_EQ(skp.position(), decoded_pos);
    uint8_t sentinel = 0;
    ASSERT_TRUE(skp.get_u8(&sentinel).ok());
    EXPECT_EQ(sentinel, 0xAB);

    // Partial skip then decode the rest interleaves correctly.
    ByteSource mix(s.view());
    ASSERT_TRUE(mix.skip_varints(3).ok()); // skip 0,1,127
    uint64_t v = 0;
    ASSERT_TRUE(mix.get_varint64(&v).ok());
    EXPECT_EQ(v, 128U); // 4th value decoded intact
}

TEST(SniiByteSource, SkipVarintsTruncationFails) {
    // A dangling continuation byte (high bit set, no terminator) must error, not
    // run off the buffer.
    uint8_t buf[2] = {0x80, 0x80};
    ByteSource src(Slice(buf, 2));
    EXPECT_FALSE(src.skip_varints(1).ok());
    // Zero-count skip is a no-op and always succeeds.
    ByteSource empty(Slice(buf, 0));
    EXPECT_TRUE(empty.skip_varints(0).ok());
}
