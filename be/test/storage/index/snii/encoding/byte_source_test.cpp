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

#include "storage/index/snii/encoding/byte_source.h"

#include <gtest/gtest.h>

#include <array>
#include <iterator>
#include <limits>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"

using namespace doris::snii;
using doris::Status;

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

// A long run of single-byte varints interleaved with multi-byte values.
// skip_varints must land exactly where a full decode would, for every prefix
// count -- covers long runs and mixed widths in one sweep.
TEST(SniiByteSource, SkipVarintsLongRunMatchesDecode) {
    ByteSink s;
    std::vector<uint64_t> vals;
    // 40 one-byte values, then a 5-byte value (forces a straddle), then more.
    for (int i = 0; i < 40; ++i) {
        vals.push_back(static_cast<uint64_t>(i % 100));
    }
    vals.push_back(0xFFFFFFFFULL); // 5 bytes
    for (int i = 0; i < 40; ++i) {
        vals.push_back(static_cast<uint64_t>((i * 7) % 120));
    }
    vals.push_back(300); // 2 bytes
    for (int i = 0; i < 20; ++i) {
        vals.push_back(1);
    }
    for (uint64_t v : vals) {
        s.put_varint64(v);
    }
    s.put_u8(0x3C); // sentinel

    // Reference decode end-position after consuming the first k values.
    for (size_t k = 0; k <= vals.size(); ++k) {
        ByteSource ref(s.view());
        for (size_t i = 0; i < k; ++i) {
            uint64_t v = 0;
            ASSERT_TRUE(ref.get_varint64(&v).ok());
        }
        const size_t want = ref.position();
        ByteSource skp(s.view());
        ASSERT_TRUE(skp.skip_varints(k).ok()) << "k=" << k;
        EXPECT_EQ(skp.position(), want) << "k=" << k;
    }
    // Full skip then read the sentinel.
    ByteSource full(s.view());
    ASSERT_TRUE(full.skip_varints(vals.size()).ok());
    uint8_t sentinel = 0;
    ASSERT_TRUE(full.get_u8(&sentinel).ok());
    EXPECT_EQ(sentinel, 0x3C);
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

// decode_delta_run must produce the SAME running prefix-sum values as the
// per-value get_varint32 loop it replaces in the CSR reader -- across 1..5-byte
// delta encodings -- and advance the cursor identically.
TEST(SniiByteSource, DecodeDeltaRunMatchesManualPrefixSum) {
    ByteSink s;
    // Deltas chosen to span every varint width and to sum to a >32-bit-looking
    // running value only if mis-summed; the running total stays within uint32.
    const uint32_t deltas[] = {5, 0, 1, 127, 128, 300, 16384, 1U << 21, 7, 2};
    for (uint32_t d : deltas) {
        s.put_varint64(d);
    }
    s.put_u8(0xCD); // sentinel

    // Manual reference: decode each delta, prefix-sum from 0.
    std::vector<uint32_t> expect;
    uint32_t running = 0;
    for (uint32_t d : deltas) {
        running += d;
        expect.push_back(running);
    }

    std::vector<uint32_t> got;
    ByteSource src(s.view());
    ASSERT_TRUE(src.decode_delta_run(std::size(deltas), &got).ok());
    EXPECT_EQ(got, expect);
    // Cursor lands exactly on the sentinel.
    uint8_t sentinel = 0;
    ASSERT_TRUE(src.get_u8(&sentinel).ok());
    EXPECT_EQ(sentinel, 0xCD);
}

TEST(SniiByteSource, DecodeDeltaRunAppendsAndZeroCount) {
    ByteSink s;
    s.put_varint64(10);
    s.put_varint64(20);
    std::vector<uint32_t> out {999}; // pre-existing content must be preserved
    ByteSource src(s.view());
    // Zero-count run is a no-op that leaves the cursor and output untouched.
    ASSERT_TRUE(src.decode_delta_run(0, &out).ok());
    EXPECT_EQ(out.size(), 1U);
    EXPECT_EQ(src.position(), 0U);
    // A real run APPENDS running values after the existing entry.
    ASSERT_TRUE(src.decode_delta_run(2, &out).ok());
    ASSERT_EQ(out.size(), 3U);
    EXPECT_EQ(out[0], 999U);
    EXPECT_EQ(out[1], 10U);
    EXPECT_EQ(out[2], 30U);
}

// get_varint32_fast must decode identically to get_varint32 across the 1-byte
// fast path, the multi-byte fallback, and the boundary at 127/128.
TEST(SniiByteSource, GetVarint32FastMatchesSlow) {
    ByteSink s;
    const uint32_t vals[] = {0, 1, 127, 128, 129, 255, 300, 16384, 0xFFFFFFFFU, 42};
    for (uint32_t v : vals) {
        s.put_varint64(v);
    }
    s.put_u8(0x5A); // sentinel

    ByteSource fast(s.view());
    for (uint32_t expect : vals) {
        uint32_t v = 0;
        ASSERT_TRUE(fast.get_varint32_fast(&v).ok());
        EXPECT_EQ(v, expect);
    }
    uint8_t sentinel = 0;
    ASSERT_TRUE(fast.get_u8(&sentinel).ok());
    EXPECT_EQ(sentinel, 0x5A);

    // Empty buffer -> fast path bails to the checked slow decoder, which errors.
    ByteSource empty(Slice(nullptr, 0));
    uint32_t v = 0;
    EXPECT_FALSE(empty.get_varint32_fast(&v).ok());
}

TEST(SniiByteSource, DecodeDeltaRunTruncationFails) {
    // Dangling continuation byte -> corruption, no buffer overrun.
    uint8_t buf[2] = {0x80, 0x80};
    ByteSource src(Slice(buf, 2));
    std::vector<uint32_t> out;
    EXPECT_FALSE(src.decode_delta_run(1, &out).ok());
    // Asking for more values than present also fails cleanly.
    ByteSink s;
    s.put_varint64(3);
    ByteSource src2(s.view());
    std::vector<uint32_t> out2;
    EXPECT_FALSE(src2.decode_delta_run(2, &out2).ok());
}

TEST(SniiByteSource, DecodeDeltaRunRejectsVarintAndPrefixSumOverflow) {
    ByteSink oversized;
    oversized.put_varint64(uint64_t {1} << 32);
    std::vector<uint32_t> out = {17};
    ByteSource oversized_source(oversized.view());
    const Status oversized_status = oversized_source.decode_delta_run(1, &out);
    EXPECT_TRUE(oversized_status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
            << oversized_status;
    EXPECT_EQ(out, std::vector<uint32_t>({17}));

    ByteSink overflowing_sum;
    overflowing_sum.put_varint32(std::numeric_limits<uint32_t>::max());
    overflowing_sum.put_varint32(1);
    ByteSource sum_source(overflowing_sum.view());
    const Status sum_status = sum_source.decode_delta_run(2, &out);
    EXPECT_TRUE(sum_status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << sum_status;
    EXPECT_EQ(out, std::vector<uint32_t>({17}));
}

TEST(ByteSourceTest, DecodeDeltaBatchCarriesPrefixAcrossBatches) {
    ByteSink sink;
    for (uint32_t delta : {7U, 2U, 3U, 5U}) {
        sink.put_varint32(delta);
    }
    ByteSource source(sink.view());
    std::array<uint32_t, 2> first {};
    std::array<uint32_t, 2> second {};
    uint32_t previous = 0;
    bool first_position = true;
    ASSERT_TRUE(source.decode_delta_batch(first, &previous, &first_position).ok());
    ASSERT_TRUE(source.decode_delta_batch(second, &previous, &first_position).ok());
    EXPECT_EQ(first, (std::array<uint32_t, 2> {7, 9}));
    EXPECT_EQ(second, (std::array<uint32_t, 2> {12, 17}));
    EXPECT_TRUE(source.eof());
}

TEST(ByteSourceTest, DecodeDeltaBatchTruncationLeavesStateUnchanged) {
    ByteSink sink;
    sink.put_varint32(4);
    sink.put_u8(0x80);
    ByteSource source(sink.view());
    std::array<uint32_t, 2> out {41, 43};
    const auto out_before = out;
    uint32_t previous = 7;
    bool first_position = false;

    const Status status = source.decode_delta_batch(out, &previous, &first_position);

    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_EQ(source.position(), 0U);
    EXPECT_EQ(out, out_before);
    EXPECT_EQ(previous, 7U);
    EXPECT_FALSE(first_position);
}

TEST(ByteSourceTest, DecodeDeltaBatchOverflowLeavesStateUnchanged) {
    ByteSink oversized;
    oversized.put_varint64(uint64_t {1} << 32);
    ByteSource oversized_source(oversized.view());
    std::array<uint32_t, 1> oversized_out {47};
    const auto oversized_out_before = oversized_out;
    uint32_t oversized_previous = 11;
    bool oversized_first_position = false;

    const Status oversized_status = oversized_source.decode_delta_batch(
            oversized_out, &oversized_previous, &oversized_first_position);

    EXPECT_TRUE(oversized_status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
            << oversized_status;
    EXPECT_EQ(oversized_source.position(), 0U);
    EXPECT_EQ(oversized_out, oversized_out_before);
    EXPECT_EQ(oversized_previous, 11U);
    EXPECT_FALSE(oversized_first_position);

    ByteSink overflowing_sum;
    overflowing_sum.put_varint32(std::numeric_limits<uint32_t>::max());
    overflowing_sum.put_varint32(1);
    ByteSource sum_source(overflowing_sum.view());
    std::array<uint32_t, 2> sum_out {53, 59};
    const auto sum_out_before = sum_out;
    uint32_t sum_previous = 0;
    bool sum_first_position = true;

    const Status sum_status =
            sum_source.decode_delta_batch(sum_out, &sum_previous, &sum_first_position);

    EXPECT_TRUE(sum_status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << sum_status;
    EXPECT_EQ(sum_source.position(), 0U);
    EXPECT_EQ(sum_out, sum_out_before);
    EXPECT_EQ(sum_previous, 0U);
    EXPECT_TRUE(sum_first_position);
}
