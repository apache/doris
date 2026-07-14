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

#include "storage/index/snii/format/logical_index_directory.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/format/format_constants.h"

using namespace doris::snii;
using namespace doris::snii::format;

namespace {

// Serialize a list of refs via the builder and return the framed section bytes.
std::vector<uint8_t> Build(const std::vector<LogicalIndexRef>& refs) {
    LogicalIndexDirectoryBuilder builder;
    for (const auto& r : refs) {
        builder.add(r);
    }
    ByteSink sink;
    builder.finish(&sink);
    return sink.buffer();
}

// Assert that two LogicalIndexRef structs are equal field by field.
void ExpectRefEq(const LogicalIndexRef& a, const LogicalIndexRef& b) {
    EXPECT_EQ(a.index_id, b.index_id);
    EXPECT_EQ(a.index_suffix, b.index_suffix);
    EXPECT_EQ(a.meta_off, b.meta_off);
    EXPECT_EQ(a.meta_len, b.meta_len);
}

} // namespace

TEST(SniiLogicalIndexDirectory, RoundTripMultipleEntries) {
    std::vector<LogicalIndexRef> refs = {
            {.index_id = 1, .index_suffix = "", .meta_off = 0, .meta_len = 4096},
            {.index_id = 2, .index_suffix = "fulltext", .meta_off = 4096, .meta_len = 8192},
            {.index_id = 7, .index_suffix = "phrase_v2", .meta_off = 12288, .meta_len = 2048},
    };
    auto bytes = Build(refs);

    LogicalIndexDirectoryReader reader;
    ASSERT_TRUE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.size(), 3U);
    for (uint32_t i = 0; i < refs.size(); ++i) {
        LogicalIndexRef out {};
        ASSERT_TRUE(reader.get(i, &out).ok());
        ExpectRefEq(out, refs[i]);
    }
}

TEST(SniiLogicalIndexDirectory, GetOrdinalOutOfRangeRejected) {
    std::vector<LogicalIndexRef> refs = {
            {.index_id = 1, .index_suffix = "a", .meta_off = 0, .meta_len = 100}};
    auto bytes = Build(refs);

    LogicalIndexDirectoryReader reader;
    ASSERT_TRUE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.size(), 1U);
    LogicalIndexRef out {};
    // ordinal == size is out of range
    EXPECT_TRUE(reader.get(1, &out).is<doris::ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND>());
    EXPECT_TRUE(reader.get(1000, &out).is<doris::ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND>());
}

TEST(SniiLogicalIndexDirectory, FindHit) {
    std::vector<LogicalIndexRef> refs = {
            {.index_id = 1, .index_suffix = "", .meta_off = 0, .meta_len = 4096},
            {.index_id = 2, .index_suffix = "fulltext", .meta_off = 4096, .meta_len = 8192},
            {.index_id = 7, .index_suffix = "phrase_v2", .meta_off = 12288, .meta_len = 2048},
    };
    auto bytes = Build(refs);

    LogicalIndexDirectoryReader reader;
    ASSERT_TRUE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader).ok());

    bool found = false;
    LogicalIndexRef out {};
    ASSERT_TRUE(reader.find(2, "fulltext", &found, &out).ok());
    EXPECT_TRUE(found);
    ExpectRefEq(out, refs[1]);

    // hit on the empty-suffix entry
    found = false;
    ASSERT_TRUE(reader.find(1, "", &found, &out).ok());
    EXPECT_TRUE(found);
    ExpectRefEq(out, refs[0]);
}

TEST(SniiLogicalIndexDirectory, FindMissByIdAndBySuffix) {
    std::vector<LogicalIndexRef> refs = {
            {.index_id = 1, .index_suffix = "", .meta_off = 0, .meta_len = 4096},
            {.index_id = 2, .index_suffix = "fulltext", .meta_off = 4096, .meta_len = 8192},
    };
    auto bytes = Build(refs);

    LogicalIndexDirectoryReader reader;
    ASSERT_TRUE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader).ok());

    bool found = true;
    LogicalIndexRef out {};
    // unknown index_id
    ASSERT_TRUE(reader.find(99, "fulltext", &found, &out).ok());
    EXPECT_FALSE(found);

    // known index_id but wrong suffix
    found = true;
    ASSERT_TRUE(reader.find(2, "wrong", &found, &out).ok());
    EXPECT_FALSE(found);

    // known index_id but suffix mismatch (empty vs non-empty)
    found = true;
    ASSERT_TRUE(reader.find(1, "fulltext", &found, &out).ok());
    EXPECT_FALSE(found);
}

TEST(SniiLogicalIndexDirectory, EmptyVersusNonEmptySuffixSameId) {
    // Same index_id, different suffix: both must be addressable distinctly.
    std::vector<LogicalIndexRef> refs = {
            {.index_id = 5, .index_suffix = "", .meta_off = 0, .meta_len = 100},
            {.index_id = 5, .index_suffix = "tokenized", .meta_off = 100, .meta_len = 200},
    };
    auto bytes = Build(refs);

    LogicalIndexDirectoryReader reader;
    ASSERT_TRUE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.size(), 2U);

    bool found = false;
    LogicalIndexRef out {};
    ASSERT_TRUE(reader.find(5, "", &found, &out).ok());
    EXPECT_TRUE(found);
    ExpectRefEq(out, refs[0]);

    found = false;
    ASSERT_TRUE(reader.find(5, "tokenized", &found, &out).ok());
    EXPECT_TRUE(found);
    ExpectRefEq(out, refs[1]);
}

TEST(SniiLogicalIndexDirectory, DuplicateIdDifferentSuffix) {
    // Multiple entries share index_id with distinct suffixes (Doris sub-column indexes).
    std::vector<LogicalIndexRef> refs = {
            {.index_id = 10, .index_suffix = "a", .meta_off = 0, .meta_len = 10},
            {.index_id = 10, .index_suffix = "b", .meta_off = 10, .meta_len = 20},
            {.index_id = 10, .index_suffix = "c", .meta_off = 30, .meta_len = 30},
    };
    auto bytes = Build(refs);

    LogicalIndexDirectoryReader reader;
    ASSERT_TRUE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.size(), 3U);

    for (const auto& want : refs) {
        bool found = false;
        LogicalIndexRef out {};
        ASSERT_TRUE(reader.find(10, want.index_suffix, &found, &out).ok());
        EXPECT_TRUE(found);
        ExpectRefEq(out, want);
    }
}

TEST(SniiLogicalIndexDirectory, EmptyDirectory) {
    std::vector<LogicalIndexRef> refs; // 0 entries
    auto bytes = Build(refs);

    LogicalIndexDirectoryReader reader;
    ASSERT_TRUE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader).ok());
    EXPECT_EQ(reader.size(), 0U);

    LogicalIndexRef out {};
    EXPECT_TRUE(reader.get(0, &out).is<doris::ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND>());

    bool found = true;
    ASSERT_TRUE(reader.find(1, "x", &found, &out).ok());
    EXPECT_FALSE(found);
}

TEST(SniiLogicalIndexDirectory, LargeOffsetsRoundTrip) {
    const uint64_t kBig = (1ULL << 48) - 1; // near 2^48
    std::vector<LogicalIndexRef> refs = {
            {.index_id = 0xFFFFFFFFFFFFFFFFULL,
             .index_suffix = "edge",
             .meta_off = kBig,
             .meta_len = kBig - 1},
            {.index_id = 12345, .index_suffix = "", .meta_off = kBig + 99, .meta_len = 1},
    };
    auto bytes = Build(refs);

    LogicalIndexDirectoryReader reader;
    ASSERT_TRUE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.size(), 2U);
    LogicalIndexRef out0 {};
    ASSERT_TRUE(reader.get(0, &out0).ok());
    ExpectRefEq(out0, refs[0]);
    LogicalIndexRef out1 {};
    ASSERT_TRUE(reader.get(1, &out1).ok());
    ExpectRefEq(out1, refs[1]);
}

TEST(SniiLogicalIndexDirectory, FramedAsLogicalIndexDirectoryType) {
    std::vector<LogicalIndexRef> refs = {
            {.index_id = 1, .index_suffix = "x", .meta_off = 0, .meta_len = 10}};
    auto bytes = Build(refs);
    ASSERT_GE(bytes.size(), 1U);
    // The first byte is the SectionFramer type and must be kLogicalIndexDirectory.
    EXPECT_EQ(bytes[0], static_cast<uint8_t>(SectionType::kLogicalIndexDirectory));
}

TEST(SniiLogicalIndexDirectory, DetectsCorruption) {
    std::vector<LogicalIndexRef> refs = {
            {.index_id = 1, .index_suffix = "fulltext", .meta_off = 0, .meta_len = 4096},
            {.index_id = 2, .index_suffix = "phrase", .meta_off = 4096, .meta_len = 8192},
    };
    auto bytes = Build(refs);
    ASSERT_GE(bytes.size(), 4U);
    // Flip one byte in the payload region; the section CRC must detect the corruption.
    bytes[3] ^= 0xFF;
    LogicalIndexDirectoryReader reader;
    EXPECT_TRUE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiLogicalIndexDirectory, DetectsTruncation) {
    std::vector<LogicalIndexRef> refs = {
            {.index_id = 1, .index_suffix = "fulltext", .meta_off = 0, .meta_len = 4096}};
    auto bytes = Build(refs);
    bytes.pop_back(); // truncate the last byte
    LogicalIndexDirectoryReader reader;
    EXPECT_FALSE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader).ok());
}

TEST(SniiLogicalIndexDirectory, WrongSectionTypeRejected) {
    // A section with a type other than kLogicalIndexDirectory must be rejected.
    ByteSink sink;
    const uint8_t p[] = {0, 1, 2};
    SectionFramer::write(sink, static_cast<uint8_t>(SectionType::kXFilter), Slice(p, 3));
    auto bytes = sink.buffer();
    LogicalIndexDirectoryReader reader;
    EXPECT_TRUE(LogicalIndexDirectoryReader::open(Slice(bytes), &reader)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
}

TEST(SniiLogicalIndexDirectory, TrailingBytesRejected) {
    // Extra trailing bytes after the declared entries must be detected.
    ByteSink payload;
    payload.put_varint32(1);  // n_entries = 1
    payload.put_varint64(1);  // index_id
    payload.put_varint32(1);  // suffix_len
    payload.put_u8('a');      // suffix bytes
    payload.put_varint64(0);  // meta_off
    payload.put_varint64(10); // meta_len
    payload.put_u8(0xEE);     // extra trailing byte
    ByteSink sink;
    SectionFramer::write(sink, static_cast<uint8_t>(SectionType::kLogicalIndexDirectory),
                         payload.view());
    LogicalIndexDirectoryReader reader;
    EXPECT_TRUE(LogicalIndexDirectoryReader::open(Slice(sink.buffer()), &reader)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiLogicalIndexDirectory, OversizedNEntriesRejected) {
    // n_entries declares far more entries than the remaining payload can hold.
    ByteSink payload;
    payload.put_varint32(0xFFFFFFFFU); // absurd n_entries
    payload.put_varint64(1);           // a tiny bit of real data
    ByteSink sink;
    SectionFramer::write(sink, static_cast<uint8_t>(SectionType::kLogicalIndexDirectory),
                         payload.view());
    LogicalIndexDirectoryReader reader;
    EXPECT_TRUE(LogicalIndexDirectoryReader::open(Slice(sink.buffer()), &reader)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiLogicalIndexDirectory, OversizedSuffixLenRejected) {
    // suffix_len declares more bytes than the payload contains.
    ByteSink payload;
    payload.put_varint32(1);           // n_entries = 1
    payload.put_varint64(1);           // index_id
    payload.put_varint32(0xFFFFFFFFU); // absurd suffix_len
    payload.put_u8('a');               // only 1 byte present
    ByteSink sink;
    SectionFramer::write(sink, static_cast<uint8_t>(SectionType::kLogicalIndexDirectory),
                         payload.view());
    LogicalIndexDirectoryReader reader;
    EXPECT_TRUE(LogicalIndexDirectoryReader::open(Slice(sink.buffer()), &reader)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}
