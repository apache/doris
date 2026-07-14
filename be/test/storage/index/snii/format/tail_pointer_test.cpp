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

#include "storage/index/snii/format/tail_pointer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/format_constants.h"

using namespace doris::snii;
using namespace doris::snii::format;
using doris::Status;

namespace {

// Encode then decode through the fixed-size on-disk layout and assert that every
// field round-trips and the encoded length matches the advertised fixed size.
void ExpectRoundTrip(const TailPointer& in) {
    ByteSink sink;
    ASSERT_TRUE(encode_tail_pointer(in, &sink).ok());
    EXPECT_EQ(sink.size(), tail_pointer_size());
    TailPointer out {};
    ASSERT_TRUE(decode_tail_pointer(sink.view(), &out).ok());
    EXPECT_EQ(out.meta_region_offset, in.meta_region_offset);
    EXPECT_EQ(out.meta_region_length, in.meta_region_length);
    EXPECT_EQ(out.hot_off, in.hot_off);
    EXPECT_EQ(out.meta_region_checksum, in.meta_region_checksum);
    EXPECT_EQ(out.bootstrap_header_checksum, in.bootstrap_header_checksum);
}

TailPointer MakeTypical() {
    TailPointer tp {};
    tp.meta_region_offset = 0x0011223344556677ULL;
    tp.meta_region_length = 4096;
    tp.hot_off = 0x00000000DEADBEEFULL;
    tp.meta_region_checksum = 0xABCD1234U;
    tp.bootstrap_header_checksum = 0x0BADC0DEU;
    return tp;
}

} // namespace

TEST(SniiTailPointer, RoundTripTypicalValues) {
    ExpectRoundTrip(MakeTypical());
}

TEST(SniiTailPointer, RoundTripZeroes) {
    TailPointer tp {}; // empty / absent hot region (hot_off == 0) must round-trip.
    ExpectRoundTrip(tp);
}

TEST(SniiTailPointer, RoundTripMaxValues) {
    TailPointer tp {};
    tp.meta_region_offset = UINT64_MAX;
    tp.meta_region_length = UINT64_MAX - 1;
    tp.hot_off = (1ULL << 63) | 7ULL; // high bit set must survive fixed64.
    tp.meta_region_checksum = UINT32_MAX;
    tp.bootstrap_header_checksum = UINT32_MAX - 3;
    ExpectRoundTrip(tp);
}

// The advertised fixed size must equal the actual encoded byte count, so a reader
// can read exactly the last tail_pointer_size() bytes of the file.
TEST(SniiTailPointer, FixedSizeMatchesEncodedLength) {
    ByteSink sink;
    ASSERT_TRUE(encode_tail_pointer(MakeTypical(), &sink).ok());
    EXPECT_EQ(sink.size(), tail_pointer_size());
}

// The on-disk tail_pointer_size byte must record the same fixed size value.
TEST(SniiTailPointer, EmbeddedSizeByteMatchesFixedSize) {
    ByteSink sink;
    ASSERT_TRUE(encode_tail_pointer(MakeTypical(), &sink).ok());
    auto bytes = sink.buffer();
    // Layout: ...[u8 tail_pointer_size][u32 tail_checksum]; the size byte is the
    // 5th byte from the end.
    ASSERT_GE(bytes.size(), 5U);
    EXPECT_EQ(bytes[bytes.size() - 5], static_cast<uint8_t>(tail_pointer_size()));
}

TEST(SniiTailPointer, WrongMagicRejected) {
    ByteSink sink;
    ASSERT_TRUE(encode_tail_pointer(MakeTypical(), &sink).ok());
    auto bytes = sink.buffer();
    ASSERT_GE(bytes.size(), 4U);
    bytes[0] ^= 0xFFU; // corrupt the magic.
    // Recompute the trailing tail_checksum is NOT done: but even if we wanted to
    // isolate the magic check, corrupting magic alone must be rejected.
    TailPointer out {};
    Status s = decode_tail_pointer(Slice(bytes), &out);
    EXPECT_FALSE(s.ok());
}

TEST(SniiTailPointer, TailChecksumMismatchRejected) {
    ByteSink sink;
    ASSERT_TRUE(encode_tail_pointer(MakeTypical(), &sink).ok());
    auto bytes = sink.buffer();
    // Flip a payload byte (the meta_region_offset region) without fixing the
    // trailing tail_checksum; the checksum must catch it.
    ASSERT_GE(bytes.size(), 10U);
    bytes[8] ^= 0xFFU;
    TailPointer out {};
    Status s = decode_tail_pointer(Slice(bytes), &out);
    EXPECT_FALSE(s.ok());
}

TEST(SniiTailPointer, TruncatedInputRejected) {
    ByteSink sink;
    ASSERT_TRUE(encode_tail_pointer(MakeTypical(), &sink).ok());
    auto bytes = sink.buffer();
    bytes.pop_back(); // one byte short of the fixed size.
    TailPointer out {};
    Status s = decode_tail_pointer(Slice(bytes), &out);
    EXPECT_FALSE(s.ok());
}

TEST(SniiTailPointer, EmptyInputRejected) {
    TailPointer out {};
    Status s = decode_tail_pointer(Slice(), &out);
    EXPECT_FALSE(s.ok());
}

// A reader may hand decode_tail_pointer more than the fixed size (e.g. when the
// file is short and it read extra leading bytes). Decoding the exact-size suffix
// must succeed; passing a longer buffer must be rejected as not the fixed size.
TEST(SniiTailPointer, LongerThanFixedSizeRejected) {
    ByteSink sink;
    ASSERT_TRUE(encode_tail_pointer(MakeTypical(), &sink).ok());
    auto bytes = sink.buffer();
    bytes.push_back(0x00); // one trailing extra byte.
    TailPointer out {};
    Status s = decode_tail_pointer(Slice(bytes), &out);
    EXPECT_FALSE(s.ok());
}
