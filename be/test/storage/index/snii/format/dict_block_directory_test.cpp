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

#include "snii/format/dict_block_directory.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/section_framer.h"
#include "snii/format/format_constants.h"

using namespace snii;
using namespace snii::format;

namespace {

// Serialize a list of block_refs using the builder and return the framed section bytes.
std::vector<uint8_t> Build(const std::vector<BlockRef>& refs) {
    DictBlockDirectoryBuilder builder;
    for (const auto& r : refs) {
        builder.add(r);
    }
    ByteSink sink;
    builder.finish(&sink);
    return sink.buffer();
}

// Assert that two BlockRef structs are equal field by field.
void ExpectRefEq(const BlockRef& a, const BlockRef& b) {
    EXPECT_EQ(a.offset, b.offset);
    EXPECT_EQ(a.length, b.length);
    EXPECT_EQ(a.n_entries, b.n_entries);
    EXPECT_EQ(a.flags, b.flags);
    EXPECT_EQ(a.checksum, b.checksum);
    EXPECT_EQ(a.uncomp_len, b.uncomp_len);
}

} // namespace

// A zstd-compressed block ref carries uncomp_len; a raw ref does not. Both
// round-trip exactly, and the raw ref's directory bytes stay v1-compact (no
// trailing uncomp_len varint when the kZstd flag is clear).
TEST(SniiDictBlockDirectory, ZstdRefCarriesUncompLen) {
    std::vector<BlockRef> refs = {
            {.offset = 0,
             .length = 40000,
             .n_entries = 250,
             .flags = block_ref_flags::kZstd,
             .checksum = 0xABCDEF01U,
             .uncomp_len = 65536}, // compressed
            {.offset = 40000,
             .length = 4096,
             .n_entries = 64,
             .flags = 0,
             .checksum = 0x11223344U,
             .uncomp_len = 0}, // raw
    };
    auto bytes = Build(refs);
    DictBlockDirectoryReader reader;
    ASSERT_TRUE(DictBlockDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.n_blocks(), 2U);
    BlockRef z {}, r {};
    ASSERT_TRUE(reader.get(0, &z).ok());
    ASSERT_TRUE(reader.get(1, &r).ok());
    ExpectRefEq(z, refs[0]);
    ExpectRefEq(r, refs[1]);
    EXPECT_EQ(z.uncomp_len, 65536U);
    EXPECT_TRUE((z.flags & block_ref_flags::kZstd) != 0);
    EXPECT_EQ(r.uncomp_len, 0U);
    EXPECT_FALSE((r.flags & block_ref_flags::kZstd) != 0);
}

TEST(SniiDictBlockDirectory, RoundTripMultipleRefs) {
    std::vector<BlockRef> refs = {
            {.offset = 0, .length = 4096, .n_entries = 120, .flags = 0x01, .checksum = 0xDEADBEEFU},
            {.offset = 4096,
             .length = 8192,
             .n_entries = 300,
             .flags = 0x05,
             .checksum = 0x12345678U},
            {.offset = 12288,
             .length = 2048,
             .n_entries = 64,
             .flags = 0x00,
             .checksum = 0xCAFEBABEU},
    };
    auto bytes = Build(refs);

    DictBlockDirectoryReader reader;
    ASSERT_TRUE(DictBlockDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.n_blocks(), 3U);
    for (uint32_t i = 0; i < refs.size(); ++i) {
        BlockRef out {};
        ASSERT_TRUE(reader.get(i, &out).ok());
        ExpectRefEq(out, refs[i]);
    }
}

TEST(SniiDictBlockDirectory, GetOrdinalCorrectMapping) {
    std::vector<BlockRef> refs;
    for (uint32_t i = 0; i < 50; ++i) {
        refs.push_back(BlockRef {.offset = static_cast<uint64_t>(i) * 1000,
                                 .length = 1000,
                                 .n_entries = i + 1,
                                 .flags = static_cast<uint8_t>(i & 0xFF),
                                 .checksum = i * 7U + 3U});
    }
    auto bytes = Build(refs);

    DictBlockDirectoryReader reader;
    ASSERT_TRUE(DictBlockDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.n_blocks(), 50U);
    // Sample several ordinals and verify the mapping produces no cross-slot errors.
    for (uint32_t ord : {0U, 1U, 17U, 49U}) {
        BlockRef out {};
        ASSERT_TRUE(reader.get(ord, &out).ok());
        ExpectRefEq(out, refs[ord]);
    }
}

TEST(SniiDictBlockDirectory, OutOfRangeOrdinalRejected) {
    std::vector<BlockRef> refs = {
            {.offset = 0, .length = 100, .n_entries = 1, .flags = 0, .checksum = 0xAAU}};
    auto bytes = Build(refs);

    DictBlockDirectoryReader reader;
    ASSERT_TRUE(DictBlockDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.n_blocks(), 1U);
    BlockRef out {};
    // ordinal == n_blocks is out of range
    EXPECT_TRUE(reader.get(1, &out).is<doris::ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND>());
    // far beyond range
    EXPECT_TRUE(reader.get(1000, &out).is<doris::ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND>());
}

TEST(SniiDictBlockDirectory, EmptyDirectory) {
    std::vector<BlockRef> refs; // 0 blocks
    auto bytes = Build(refs);

    DictBlockDirectoryReader reader;
    ASSERT_TRUE(DictBlockDirectoryReader::open(Slice(bytes), &reader).ok());
    EXPECT_EQ(reader.n_blocks(), 0U);
    BlockRef out {};
    EXPECT_TRUE(reader.get(0, &out).is<doris::ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND>());
}

TEST(SniiDictBlockDirectory, LargeOffsetNear2Pow48) {
    const uint64_t kBig = (1ULL << 48) - 1; // near 2^48
    std::vector<BlockRef> refs = {
            {.offset = kBig,
             .length = kBig - 1,
             .n_entries = 0xFFFFFFFFU,
             .flags = 0xFF,
             .checksum = 0xFFFFFFFFU},
            {.offset = kBig + 12345, .length = 1, .n_entries = 1, .flags = 0x02, .checksum = 0U},
    };
    auto bytes = Build(refs);

    DictBlockDirectoryReader reader;
    ASSERT_TRUE(DictBlockDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.n_blocks(), 2U);
    BlockRef out0 {};
    ASSERT_TRUE(reader.get(0, &out0).ok());
    ExpectRefEq(out0, refs[0]);
    BlockRef out1 {};
    ASSERT_TRUE(reader.get(1, &out1).ok());
    ExpectRefEq(out1, refs[1]);
}

TEST(SniiDictBlockDirectory, FramedAsDictBlockDirectoryType) {
    std::vector<BlockRef> refs = {
            {.offset = 0, .length = 10, .n_entries = 1, .flags = 0, .checksum = 0}};
    auto bytes = Build(refs);
    ASSERT_GE(bytes.size(), 1U);
    // The first byte is the SectionFramer type and must be kDictBlockDirectory.
    EXPECT_EQ(bytes[0], static_cast<uint8_t>(SectionType::kDictBlockDirectory));
}

TEST(SniiDictBlockDirectory, DetectsCorruption) {
    std::vector<BlockRef> refs = {
            {.offset = 0, .length = 4096, .n_entries = 120, .flags = 0x01, .checksum = 0xDEADBEEFU},
            {.offset = 4096,
             .length = 8192,
             .n_entries = 300,
             .flags = 0x05,
             .checksum = 0x12345678U},
    };
    auto bytes = Build(refs);
    ASSERT_GE(bytes.size(), 4U);
    // Flip one byte in the payload region (skip the type+len prefix); the section CRC must detect the corruption.
    bytes[3] ^= 0xFF;
    DictBlockDirectoryReader reader;
    EXPECT_TRUE(DictBlockDirectoryReader::open(Slice(bytes), &reader)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiDictBlockDirectory, DetectsTruncation) {
    std::vector<BlockRef> refs = {{.offset = 0,
                                   .length = 4096,
                                   .n_entries = 120,
                                   .flags = 0x01,
                                   .checksum = 0xDEADBEEFU}};
    auto bytes = Build(refs);
    bytes.pop_back(); // truncate the last byte
    DictBlockDirectoryReader reader;
    EXPECT_FALSE(DictBlockDirectoryReader::open(Slice(bytes), &reader).ok());
}

TEST(SniiDictBlockDirectory, WrongSectionTypeRejected) {
    // Write a section with a type other than kDictBlockDirectory via the framer; open must reject it.
    ByteSink sink;
    const uint8_t p[] = {0, 1, 2};
    SectionFramer::write(sink, static_cast<uint8_t>(SectionType::kXFilter), Slice(p, 3));
    auto bytes = sink.buffer();
    DictBlockDirectoryReader reader;
    EXPECT_TRUE(DictBlockDirectoryReader::open(Slice(bytes), &reader)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
}

TEST(SniiDictBlockDirectory, TrailingBytesRejected) {
    // Extra trailing bytes at the end of the payload should be detected (n_blocks does not match actual data).
    ByteSink payload;
    payload.put_varint32(1);  // n_blocks = 1
    payload.put_varint64(0);  // offset
    payload.put_varint64(10); // length
    payload.put_varint32(1);  // n_entries
    payload.put_u8(0);        // flags
    payload.put_fixed32(0);   // checksum
    payload.put_u8(0xEE);     // extra trailing byte
    ByteSink sink;
    SectionFramer::write(sink, static_cast<uint8_t>(SectionType::kDictBlockDirectory),
                         payload.view());
    DictBlockDirectoryReader reader;
    EXPECT_TRUE(DictBlockDirectoryReader::open(Slice(sink.buffer()), &reader)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// An attacker-inflated n_blocks count must hit the reserve-bomb guard in
// decode_payload BEFORE the vector reserve, returning Corruption rather than
// attempting a multi-gigabyte allocation. The payload is framed through
// SectionFramer::write so the section crc is VALID over the crafted bytes:
// this proves the inner n_blocks-vs-capacity check fires, not an outer crc flip.
TEST(SniiDictBlockDirectory, InflatedNBlocksHitsReserveGuard) {
    ByteSink payload;
    payload.put_varint32(0xFFFFFFFFU); // n_blocks = ~4.29 billion (impossible)
    // Only a few real bytes follow; capacity is nowhere near 4.29B refs.
    payload.put_u8(0x00);
    payload.put_u8(0x00);
    payload.put_u8(0x00);
    payload.put_u8(0x00);
    ByteSink sink;
    SectionFramer::write(sink, static_cast<uint8_t>(SectionType::kDictBlockDirectory),
                         payload.view());
    DictBlockDirectoryReader reader;
    // remaining()/kMinRefBytes == 4/8 == 0, so n_blocks > 0 trips the guard.
    EXPECT_TRUE(DictBlockDirectoryReader::open(Slice(sink.buffer()), &reader)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// A kZstd ref whose trailing uncomp_len varint is missing (truncated) must be
// rejected as Corruption: decode_ref reads offset/length/n_entries/flags/checksum
// fine, sees the kZstd flag, then overruns when it tries to read uncomp_len.
// n_blocks == 1 with an ~8-byte ref body passes the reserve guard (1 > 8/8 is
// false), so the corruption surfaces from the inner uncomp_len read -- not the
// capacity cap and not the section crc (the frame is crc-valid by construction).
TEST(SniiDictBlockDirectory, ZstdRefTruncatedUncompLenRejected) {
    ByteSink payload;
    payload.put_varint32(1);                // n_blocks = 1
    payload.put_varint64(0);                // offset (1 byte)
    payload.put_varint64(10);               // length (1 byte)
    payload.put_varint32(1);                // n_entries (1 byte)
    payload.put_u8(block_ref_flags::kZstd); // flags: kZstd set
    payload.put_fixed32(0xDEADBEEFU);       // checksum (4 bytes)
    // INTENTIONALLY OMIT the trailing varint64 uncomp_len -> decode_ref overruns.
    ByteSink sink;
    SectionFramer::write(sink, static_cast<uint8_t>(SectionType::kDictBlockDirectory),
                         payload.view());
    DictBlockDirectoryReader reader;
    EXPECT_TRUE(DictBlockDirectoryReader::open(Slice(sink.buffer()), &reader)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// A kZstd ref round-trips through the builder/reader preserving uncomp_len
// exactly, including a large 64-bit value, proving the trailing varint is both
// written and read back on the kZstd path.
TEST(SniiDictBlockDirectory, ZstdRefRoundTripPreservesUncompLen) {
    const uint64_t kUncomp = (1ULL << 40) + 1234567U; // large, multi-byte varint
    std::vector<BlockRef> refs = {
            {.offset = 1024,
             .length = 999,
             .n_entries = 7,
             .flags = block_ref_flags::kZstd,
             .checksum = 0x0BADF00DU,
             .uncomp_len = kUncomp},
    };
    auto bytes = Build(refs);
    DictBlockDirectoryReader reader;
    ASSERT_TRUE(DictBlockDirectoryReader::open(Slice(bytes), &reader).ok());
    ASSERT_EQ(reader.n_blocks(), 1U);
    BlockRef out {};
    ASSERT_TRUE(reader.get(0, &out).ok());
    ExpectRefEq(out, refs[0]);
    EXPECT_EQ(out.uncomp_len, kUncomp);
    EXPECT_TRUE((out.flags & block_ref_flags::kZstd) != 0);
}
