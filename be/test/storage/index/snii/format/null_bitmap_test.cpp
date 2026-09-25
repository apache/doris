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

#include "storage/index/snii/format/null_bitmap.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
// clang-format off
// CRoaring's public header must precede internal container headers.
#include "roaring/roaring.hh"
#include "roaring/containers/array.h"
#include "roaring/containers/bitset.h"
#include "roaring/containers/run.h"
// clang-format on
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"

using namespace doris::snii;
using doris::Status;
using doris::snii::format::NullBitmapReader;
using doris::snii::format::NullBitmapWriter;
using doris::snii::format::kNullBitmapSectionType;

namespace {

// Encode a set of null docids into a framed buffer using the writer.
std::vector<uint8_t> BuildBitmap(const std::vector<uint32_t>& nulls, uint32_t doc_count) {
    NullBitmapWriter writer;
    for (uint32_t d : nulls) {
        writer.add_null(d);
    }
    ByteSink sink;
    EXPECT_TRUE(writer.finish(doc_count, &sink).ok());
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

TEST(SniiNullBitmap, DenseBuildPeakCoversArrayToBitsetConversion) {
    constexpr uint32_t kDenseContainerCount = 8;
    constexpr uint32_t kDenseCardinality = roaring::internal::DEFAULT_MAX_SIZE + 1;
    std::vector<uint32_t> nulls;
    nulls.reserve(kDenseContainerCount * kDenseCardinality);
    for (uint32_t key = 0; key < kDenseContainerCount; ++key) {
        for (uint32_t low = 0; low < kDenseCardinality; ++low) {
            nulls.push_back((key << 16) | low);
        }
    }

    constexpr uint64_t kObservedConversionCapacity = 4165;
    constexpr uint64_t kTopEntryBytes = sizeof(void*) + sizeof(uint16_t) + sizeof(uint8_t);
    constexpr uint64_t kBitsetBytes =
            roaring::internal::BITSET_CONTAINER_SIZE_IN_WORDS * sizeof(uint64_t);
    constexpr uint64_t kObservedPeak =
            sizeof(roaring::Roaring) + 3 * kDenseContainerCount * kTopEntryBytes +
            kDenseContainerCount * (kObservedConversionCapacity * sizeof(uint16_t) + kBitsetBytes +
                                    sizeof(roaring::internal::array_container_t) +
                                    sizeof(roaring::internal::bitset_container_t));
    EXPECT_GE(NullBitmapWriter::build_memory_upper_bound(std::span<const uint32_t>(nulls)),
              kObservedPeak);
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

TEST(SniiNullBitmap, RejectsNegativeNoRunContainerCount) {
    ByteSink roaring;
    roaring.put_fixed32(12346); // portable cookie without run containers
    roaring.put_fixed32(std::numeric_limits<uint32_t>::max());

    ByteSink payload;
    payload.put_varint64(1); // doc_count
    payload.put_varint64(roaring.size());
    payload.put_bytes(roaring.view());

    ByteSink sink;
    SectionFramer::write(sink, kNullBitmapSectionType, payload.view());

    uint64_t decoded_bytes = 0;
    Status status = NullBitmapReader::decoded_memory_bytes(sink.view(), &decoded_bytes);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    EXPECT_NE(status.to_string().find("portable container count out of range"), std::string::npos);
}

TEST(SniiNullBitmap, DecodedMemoryAccountsEachPortableContainer) {
    constexpr uint32_t kContainerCount = 3;
    auto buffer = BuildBitmap({0, 1U << 16, 2U << 16}, (2U << 16) + 1);

    ByteSource frame_source {Slice(buffer)};
    FramedSection frame;
    ASSERT_TRUE(SectionFramer::read(frame_source, &frame).ok());
    ByteSource payload(frame.payload);
    uint64_t doc_count = 0;
    uint64_t roaring_bytes = 0;
    ASSERT_TRUE(payload.get_varint64(&doc_count).ok());
    ASSERT_TRUE(payload.get_varint64(&roaring_bytes).ok());
    EXPECT_EQ(doc_count, (2U << 16) + 1);

    constexpr uint64_t kContainerObjectBytes =
            std::max({sizeof(roaring::internal::array_container_t),
                      sizeof(roaring::internal::bitset_container_t),
                      sizeof(roaring::internal::run_container_t)});
    constexpr uint64_t kContainerMetadataBytes =
            sizeof(void*) + sizeof(uint16_t) + sizeof(uint8_t) + kContainerObjectBytes;
    constexpr uint64_t kFixedBytes =
            sizeof(roaring::Roaring) + sizeof(roaring::api::roaring_bitmap_t);

    uint64_t decoded_bytes = 0;
    ASSERT_TRUE(NullBitmapReader::decoded_memory_bytes(Slice(buffer), &decoded_bytes).ok());
    EXPECT_EQ(decoded_bytes,
              roaring_bytes + kContainerCount * kContainerMetadataBytes + kFixedBytes);
}

namespace {

// Frames a bitmap exactly as the writers before run optimization did: addMany, then the portable
// serialization of whatever containers that produced.
std::vector<uint8_t> LegacyUnoptimizedSection(const std::vector<uint32_t>& nulls,
                                              uint32_t doc_count) {
    roaring::Roaring bitmap;
    bitmap.addMany(nulls.size(), nulls.data());
    std::vector<char> roaring_bytes(bitmap.getSizeInBytes());
    bitmap.write(roaring_bytes.data());
    ByteSink payload;
    payload.put_varint64(doc_count);
    payload.put_varint64(roaring_bytes.size());
    payload.put_bytes(
            Slice(reinterpret_cast<const uint8_t*>(roaring_bytes.data()), roaring_bytes.size()));
    ByteSink sink;
    SectionFramer::write(sink, kNullBitmapSectionType, payload.view());
    return sink.buffer();
}

uint16_t PortableCookie(const std::vector<uint8_t>& framed) {
    ByteSource src {Slice(framed)};
    FramedSection section;
    EXPECT_TRUE(SectionFramer::read(src, &section).ok());
    ByteSource payload(section.payload);
    uint64_t value = 0;
    EXPECT_TRUE(payload.get_varint64(&value).ok());
    EXPECT_TRUE(payload.get_varint64(&value).ok());
    uint16_t cookie = 0;
    EXPECT_TRUE(payload.get_fixed16(&cookie).ok());
    return cookie;
}

std::vector<uint32_t> MostlyNull(uint32_t doc_count) {
    std::vector<uint32_t> nulls;
    for (uint32_t docid = 0; docid < doc_count; ++docid) {
        const bool present = (docid >= 1000 && docid < 1100) ||
                             (docid >= 500000 && docid < 500010) || docid >= doc_count - 576;
        if (!present) {
            nulls.push_back(docid);
        }
    }
    return nulls;
}

} // namespace

// A mostly NULL path over 16 containers: without run containers every one of them is an 8 KiB
// bitset; run-optimized they take a few bytes each, and the section still round-trips.
TEST(SniiNullBitmap, RunOptimizedMostlyNullSectionIsSmallAndRoundTrips) {
    constexpr uint32_t kDocCount = 1U << 20;
    const std::vector<uint32_t> nulls = MostlyNull(kDocCount);
    const std::vector<uint8_t> legacy = LegacyUnoptimizedSection(nulls, kDocCount);

    NullBitmapWriter writer;
    writer.add_many(nulls);
    doris::snii::format::NullBitmapSerializationSizes sizes;
    ASSERT_TRUE(writer.serialization_sizes(kDocCount, &sizes).ok());
    ByteSink sink;
    ASSERT_TRUE(writer.finish(kDocCount, &sink).ok());
    const std::vector<uint8_t> optimized = sink.buffer();
    EXPECT_EQ(optimized.size(), sizes.framed_bytes);
    EXPECT_GT(legacy.size(), 16U * 8192);
    EXPECT_LT(optimized.size() * 100, legacy.size());
    EXPECT_EQ(PortableCookie(optimized), 12347); // SERIAL_COOKIE: run containers present
    EXPECT_EQ(PortableCookie(legacy), 12346);    // SERIAL_COOKIE_NO_RUNCONTAINER

    NullBitmapReader reader;
    ASSERT_TRUE(NullBitmapReader::open(Slice(optimized), &reader).ok());
    EXPECT_EQ(reader.doc_count(), kDocCount);
    EXPECT_EQ(reader.null_count(), nulls.size());
    std::vector<uint32_t> decoded;
    reader.append_docids(decoded);
    EXPECT_EQ(decoded, nulls);
    uint64_t decoded_bytes = 0;
    ASSERT_TRUE(NullBitmapReader::decoded_memory_bytes(Slice(optimized), &decoded_bytes).ok());
    EXPECT_LT(decoded_bytes, 16U * 8192);
}

// Sections written before run optimization (array and bitset containers only) stay readable.
TEST(SniiNullBitmap, ReadsLegacyUnoptimizedSections) {
    const std::vector<std::pair<uint32_t, std::vector<uint32_t>>> cases = {
            {1U << 20, MostlyNull(1U << 20)},
            {5000, {0, 3, 7, 11, 100, 4000}},
            {200000,
             [] {
                 std::vector<uint32_t> nulls;
                 for (uint32_t docid = 0; docid < 200000; ++docid) {
                     if (docid % 5 != 0) {
                         nulls.push_back(docid);
                     }
                 }
                 return nulls;
             }()},
    };
    for (const auto& [doc_count, nulls] : cases) {
        const std::vector<uint8_t> legacy = LegacyUnoptimizedSection(nulls, doc_count);
        ASSERT_EQ(PortableCookie(legacy), 12346);
        NullBitmapReader reader;
        ASSERT_TRUE(NullBitmapReader::open(Slice(legacy), &reader).ok());
        EXPECT_EQ(reader.doc_count(), doc_count);
        EXPECT_EQ(reader.null_count(), nulls.size());
        std::vector<uint32_t> decoded;
        reader.append_docids(decoded);
        EXPECT_EQ(decoded, nulls);
        roaring::Roaring copy;
        reader.copy_to(&copy);
        EXPECT_EQ(copy.cardinality(), nulls.size());
        uint64_t decoded_bytes = 0;
        EXPECT_TRUE(NullBitmapReader::decoded_memory_bytes(Slice(legacy), &decoded_bytes).ok());
    }
}

// Sizes are computed on the optimized bitmap, also after more docids arrive.
TEST(SniiNullBitmap, SerializationSizesFollowLaterAdds) {
    NullBitmapWriter writer;
    std::vector<uint32_t> run(70000);
    for (uint32_t i = 0; i < run.size(); ++i) {
        run[i] = i;
    }
    writer.add_many(run);
    doris::snii::format::NullBitmapSerializationSizes first;
    ASSERT_TRUE(writer.serialization_sizes(100000, &first).ok());
    writer.add_null(90000);
    doris::snii::format::NullBitmapSerializationSizes second;
    ASSERT_TRUE(writer.serialization_sizes(100000, &second).ok());
    EXPECT_GT(second.roaring_bytes, first.roaring_bytes);
    ByteSink sink;
    ASSERT_TRUE(writer.finish(100000, &sink).ok());
    EXPECT_EQ(sink.size(), second.framed_bytes);
    NullBitmapReader reader;
    ASSERT_TRUE(NullBitmapReader::open(sink.view(), &reader).ok());
    EXPECT_EQ(reader.null_count(), 70001U);
    EXPECT_TRUE(reader.is_null(69999));
    EXPECT_FALSE(reader.is_null(70000));
    EXPECT_TRUE(reader.is_null(90000));
}
