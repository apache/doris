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

#include "snii/format/null_bitmap.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/section_framer.h"

using namespace snii;
using doris::Status;
using snii::format::NullBitmapReader;
using snii::format::NullBitmapWriter;
using snii::format::kNullBitmapSectionType;

namespace {

// Encode a set of null docids into a framed buffer using the writer.
std::vector<uint8_t> BuildBitmap(const std::vector<uint32_t>& nulls, uint32_t doc_count) {
    NullBitmapWriter writer;
    for (uint32_t d : nulls) {
        writer.add_null(d);
    }
    ByteSink sink;
    writer.finish(doc_count, &sink);
    return sink.buffer();
}

} // namespace

// After adding nulls, is_null(docid) must match the input set for every doc.
TEST(SniiNullBitmap, RoundTripPerDoc) {
    std::vector<uint32_t> nulls = {0, 3, 7, 11, 100, 4000};
    uint32_t doc_count = 5000;
    auto buf = BuildBitmap(nulls, doc_count);

    NullBitmapReader reader;
    ASSERT_TRUE(NullBitmapReader::open(Slice(buf), &reader).ok());
    EXPECT_EQ(reader.doc_count(), doc_count);
    EXPECT_EQ(reader.null_count(), nulls.size());

    std::vector<bool> expected(doc_count, false);
    for (uint32_t d : nulls) {
        expected[d] = true;
    }
    for (uint32_t docid = 0; docid < doc_count; ++docid) {
        EXPECT_EQ(reader.is_null(docid), expected[docid]) << "docid=" << docid;
    }
}

// Writer null_count reflects the number of distinct null docids added.
TEST(SniiNullBitmap, WriterNullCount) {
    NullBitmapWriter writer;
    EXPECT_EQ(writer.null_count(), 0U);
    writer.add_null(5);
    writer.add_null(9);
    writer.add_null(5); // duplicate is idempotent in a set
    EXPECT_EQ(writer.null_count(), 2U);
}

// Empty bitmap: no nulls. open succeeds, null_count == 0, nothing is null.
TEST(SniiNullBitmap, EmptyNoNulls) {
    auto buf = BuildBitmap({}, 1000);

    NullBitmapReader reader;
    ASSERT_TRUE(NullBitmapReader::open(Slice(buf), &reader).ok());
    EXPECT_EQ(reader.doc_count(), 1000U);
    EXPECT_EQ(reader.null_count(), 0U);
    EXPECT_FALSE(reader.is_null(0));
    EXPECT_FALSE(reader.is_null(999));
}

// All-null bitmap: every doc in [0, doc_count) is null.
TEST(SniiNullBitmap, AllNull) {
    uint32_t doc_count = 256;
    std::vector<uint32_t> nulls;
    for (uint32_t d = 0; d < doc_count; ++d) {
        nulls.push_back(d);
    }
    auto buf = BuildBitmap(nulls, doc_count);

    NullBitmapReader reader;
    ASSERT_TRUE(NullBitmapReader::open(Slice(buf), &reader).ok());
    EXPECT_EQ(reader.null_count(), doc_count);
    for (uint32_t docid = 0; docid < doc_count; ++docid) {
        EXPECT_TRUE(reader.is_null(docid)) << "docid=" << docid;
    }
}

// doc_count round-trips even when there are no nulls and a large doc_count.
TEST(SniiNullBitmap, DocCountRoundTrips) {
    auto buf = BuildBitmap({42}, 1234567);

    NullBitmapReader reader;
    ASSERT_TRUE(NullBitmapReader::open(Slice(buf), &reader).ok());
    EXPECT_EQ(reader.doc_count(), 1234567U);
    EXPECT_TRUE(reader.is_null(42));
}

// is_null beyond doc_count is false (docid not in the null set).
TEST(SniiNullBitmap, IsNullOutsideRangeIsFalse) {
    auto buf = BuildBitmap({1, 2, 3}, 10);

    NullBitmapReader reader;
    ASSERT_TRUE(NullBitmapReader::open(Slice(buf), &reader).ok());
    EXPECT_FALSE(reader.is_null(10));
    EXPECT_FALSE(reader.is_null(1000000));
}

// CRC corruption is detectable: flipping a payload byte fails open with a
// corruption error (SectionFramer stamps the crc over type+len+payload, so a
// flipped payload byte makes the recomputed crc disagree with the stored one).
TEST(SniiNullBitmap, DetectsCorruption) {
    std::vector<uint32_t> nulls = {2, 4, 6, 8, 10, 12, 14};
    auto buf = BuildBitmap(nulls, 100);
    // Flip a byte inside the roaring payload region (near the end, before the trailing CRC).
    buf[buf.size() - 5] ^= 0xFF;

    NullBitmapReader reader;
    Status s = NullBitmapReader::open(Slice(buf), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// Truncated input returns an error rather than crashing.
TEST(SniiNullBitmap, DetectsTruncation) {
    auto buf = BuildBitmap({1, 2, 3, 4, 5}, 100);
    buf.resize(buf.size() - 4); // chop trailing CRC region

    NullBitmapReader reader;
    Status s = NullBitmapReader::open(Slice(buf), &reader);
    EXPECT_FALSE(s.ok());
}

// An oversized declared roaring_size (larger than the remaining payload bytes) is rejected (anti-DoS).
TEST(SniiNullBitmap, RejectsOversizedRoaringSize) {
    // Manually construct a self-consistent frame whose declared roaring_size
    // exceeds the bytes actually present, to drive the guard branch.
    ByteSink payload;
    payload.put_varint64(100);           // doc_count
    payload.put_varint64(0xFFFFFFFFULL); // roaring_size: absurdly large
    payload.put_u8(0x00);                // only 1 byte of roaring data present

    ByteSink sink;
    SectionFramer::write(sink, kNullBitmapSectionType, payload.view());

    NullBitmapReader reader;
    Status s = NullBitmapReader::open(sink.view(), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// doc_count overflowing uint32 is rejected.
TEST(SniiNullBitmap, RejectsDocCountOverflow) {
    ByteSink payload;
    payload.put_varint64(0x1'0000'0000ULL); // doc_count > uint32 max
    payload.put_varint64(0);                // roaring_size

    ByteSink sink;
    SectionFramer::write(sink, kNullBitmapSectionType, payload.view());

    NullBitmapReader reader;
    Status s = NullBitmapReader::open(sink.view(), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// A CRC-valid frame carrying malformed roaring container bytes must be rejected
// gracefully (corruption error) without throwing or aborting. The roaring bytes are
// not a valid portable serialization; SectionFramer stamps a correct crc so the framer
// check passes and the roaring pre-validation (deserialize_size probe) must catch it.
TEST(SniiNullBitmap, RejectsMalformedRoaringContainer) {
    ByteSink payload;
    payload.put_varint64(10);                           // doc_count
    payload.put_varint64(4);                            // roaring_size
    const uint8_t garbage[] = {0xFF, 0xFF, 0xFF, 0xFF}; // invalid roaring cookie
    payload.put_bytes(Slice(garbage, 4));

    ByteSink sink;
    SectionFramer::write(sink, kNullBitmapSectionType, payload.view());

    NullBitmapReader reader;
    Status s = NullBitmapReader::open(sink.view(), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}
