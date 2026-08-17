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

#include "storage/index/snii/bkd/bkd_index_block.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/olap_common.h"

namespace doris::snii::bkd {
namespace {

constexpr uint32_t kBytesPerDim = sizeof(int64_t);
// Byte length of the companion bkd_data file the fixture's leaf offsets live in.
constexpr uint64_t kDataLength = 130;

// Unsigned big-endian sortable bytes for a BIGINT -- what
// KeyCoder::full_encode_ascending emits (sign bit flipped, then byte-swapped).
// The tests need ordered byte strings whose ordering is a plain unsigned memcmp
// from offset 0 (INV-1); feeding little-endian bytes would build a self
// consistent but semantically wrong index.
std::vector<uint8_t> sortable_bigint(int64_t v) {
    const uint64_t u = static_cast<uint64_t>(v) ^ (uint64_t {1} << 63);
    std::vector<uint8_t> out(kBytesPerDim);
    for (uint32_t i = 0; i < kBytesPerDim; ++i) {
        out[kBytesPerDim - 1 - i] = static_cast<uint8_t>(u >> (8 * i));
    }
    return out;
}

void append_bytes(std::vector<uint8_t>* dst, const std::vector<uint8_t>& src) {
    dst->insert(dst->end(), src.begin(), src.end());
}

// A hand-assembled bkd_index with one struct member per on-disk field, so a
// corruption test can break exactly ONE rule and leave the rest well formed.
// Deliberately independent of encode_bkd_index_block: the encoder DORIS_CHECKs
// its inputs (they are build-time invariants), so inconsistent bytes can only be
// produced here. EncoderMatchesDocumentedLayout pins the two against each other.
struct RawIndexBlock {
    uint8_t section_type = kBkdIndexSectionType;
    uint32_t magic = kBkdIndexMagic;
    uint32_t format_version = kFormatVersion;
    uint32_t flags = 0;
    uint32_t bytes_per_dim = kBytesPerDim;
    uint32_t field_type = static_cast<uint32_t>(FieldType::OLAP_FIELD_TYPE_BIGINT);
    uint64_t point_count = 0;
    uint32_t doc_count = 0;
    uint32_t leaf_count = 0;
    uint32_t points_per_leaf = kDefaultPointsPerLeaf;
    std::vector<uint8_t> min_value;
    std::vector<uint8_t> max_value;
    std::vector<uint8_t> split_values;
    // Deltas, exactly as stored; leaves_of() turns them into absolute offsets.
    std::vector<uint64_t> leaf_offset_deltas;
    std::vector<uint32_t> leaf_counts;
    // Extra payload bytes past the leaf directory.
    std::vector<uint8_t> trailing;
};

std::vector<uint8_t> frame(const RawIndexBlock& raw) {
    ByteSink payload;
    payload.put_fixed32(raw.magic);
    payload.put_varint32(raw.format_version);
    payload.put_varint32(raw.flags);
    payload.put_varint32(raw.bytes_per_dim);
    payload.put_varint32(raw.field_type);
    payload.put_varint64(raw.point_count);
    payload.put_varint32(raw.doc_count);
    payload.put_varint32(raw.leaf_count);
    payload.put_varint32(raw.points_per_leaf);
    payload.put_bytes(Slice(raw.min_value));
    payload.put_bytes(Slice(raw.max_value));
    payload.put_bytes(Slice(raw.split_values));
    for (uint64_t delta : raw.leaf_offset_deltas) {
        payload.put_varint64(delta);
    }
    for (uint32_t count : raw.leaf_counts) {
        payload.put_varint32(count);
    }
    payload.put_bytes(Slice(raw.trailing));

    ByteSink framed;
    SectionFramer::write(framed, raw.section_type, payload.view());
    return framed.take();
}

// Three BIGINT leaves: counts 4 + 4 + 2 == point_count, offsets 0 / 40 / 90 inside
// a 130-byte bkd_data. Every corruption test starts from this shape.
RawIndexBlock valid_three_leaf_block() {
    RawIndexBlock raw;
    raw.point_count = 10;
    raw.doc_count = 9;
    raw.leaf_count = 3;
    raw.min_value = sortable_bigint(-5);
    raw.max_value = sortable_bigint(300);
    append_bytes(&raw.split_values, sortable_bigint(100));
    append_bytes(&raw.split_values, sortable_bigint(200));
    raw.leaf_offset_deltas = {0, 40, 50};
    raw.leaf_counts = {4, 4, 2};
    return raw;
}

BkdIndexHeader header_of(const RawIndexBlock& raw) {
    BkdIndexHeader header;
    header.format_version = raw.format_version;
    header.flags = raw.flags;
    header.bytes_per_dim = raw.bytes_per_dim;
    header.field_type = static_cast<FieldType>(raw.field_type);
    header.point_count = raw.point_count;
    header.doc_count = raw.doc_count;
    header.leaf_count = raw.leaf_count;
    header.points_per_leaf = raw.points_per_leaf;
    return header;
}

std::vector<LeafRef> leaves_of(const RawIndexBlock& raw) {
    std::vector<LeafRef> leaves(raw.leaf_count);
    uint64_t offset = 0;
    for (uint32_t i = 0; i < raw.leaf_count; ++i) {
        offset += raw.leaf_offset_deltas[i];
        leaves[i].offset = offset;
        leaves[i].count = raw.leaf_counts[i];
    }
    return leaves;
}

std::vector<uint8_t> encode(const RawIndexBlock& raw) {
    ByteSink sink;
    const std::vector<LeafRef> leaves = leaves_of(raw);
    encode_bkd_index_block(header_of(raw), Slice(raw.min_value), Slice(raw.max_value),
                           Slice(raw.split_values), leaves, &sink);
    return sink.take();
}

bool bytes_equal(Slice actual, const std::vector<uint8_t>& expected) {
    return actual.size() == expected.size() &&
           std::memcmp(actual.data(), expected.data(), expected.size()) == 0;
}

// Every rejection below must be a Status, never a crash and never an out-of-bounds
// read -- that is the whole point of routing disk bytes through ByteSource and
// validating once at open (design 8).
::testing::AssertionResult IsCorrupted(const Status& status) {
    if (status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) {
        return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure()
           << "expected INVERTED_INDEX_FILE_CORRUPTED, got " << status;
}

Status open_raw(const RawIndexBlock& raw, BkdIndexBlockReader* reader,
                uint64_t data_length = kDataLength) {
    const std::vector<uint8_t> bytes = frame(raw);
    return BkdIndexBlockReader::open(Slice(bytes), data_length, reader);
}

// ---------------------------------------------------------------------------
// Round trip
// ---------------------------------------------------------------------------

TEST(SniiBkdIndexBlock, RoundTripPreservesHeaderBoundsSplitsAndDirectory) {
    const RawIndexBlock raw = valid_three_leaf_block();
    const std::vector<uint8_t> bytes = encode(raw);

    BkdIndexBlockReader reader;
    ASSERT_TRUE(BkdIndexBlockReader::open(Slice(bytes), kDataLength, &reader).ok());

    ASSERT_FALSE(reader.empty());
    ASSERT_EQ(reader.leaf_count(), 3U);
    const BkdIndexHeader& header = reader.header();
    EXPECT_EQ(header.format_version, kFormatVersion);
    EXPECT_EQ(header.flags, 0U);
    EXPECT_EQ(header.bytes_per_dim, kBytesPerDim);
    EXPECT_EQ(header.field_type, FieldType::OLAP_FIELD_TYPE_BIGINT);
    EXPECT_EQ(header.point_count, 10U);
    EXPECT_EQ(header.doc_count, 9U);
    EXPECT_EQ(header.leaf_count, 3U);
    EXPECT_EQ(header.points_per_leaf, kDefaultPointsPerLeaf);

    EXPECT_TRUE(bytes_equal(reader.min_value(), sortable_bigint(-5)));
    EXPECT_TRUE(bytes_equal(reader.max_value(), sortable_bigint(300)));

    ASSERT_EQ(reader.split_values().size(), 2 * kBytesPerDim);
    EXPECT_TRUE(bytes_equal(reader.split_value(0), sortable_bigint(100)));
    EXPECT_TRUE(bytes_equal(reader.split_value(1), sortable_bigint(200)));

    EXPECT_EQ(reader.leaf(0).offset, 0U);
    EXPECT_EQ(reader.leaf(0).count, 4U);
    EXPECT_EQ(reader.leaf(1).offset, 40U);
    EXPECT_EQ(reader.leaf(1).count, 4U);
    EXPECT_EQ(reader.leaf(2).offset, 90U);
    EXPECT_EQ(reader.leaf(2).count, 2U);

    // The arrays are decoded once and owned by the reader, so the resident charge
    // is real rather than the old ram_bytes_used() that omitted the packed index.
    EXPECT_GE(reader.heap_bytes(), 2 * kBytesPerDim + 2 * kBytesPerDim + 3 * sizeof(LeafRef));
}

// The encoder must emit exactly the layout documented in design 5.1. Pinning the
// bytes here is what keeps the format from drifting silently and what makes the
// hand-assembled corruption fixtures below a faithful mirror of real output.
TEST(SniiBkdIndexBlock, EncoderMatchesDocumentedLayout) {
    const RawIndexBlock raw = valid_three_leaf_block();
    EXPECT_EQ(encode(raw), frame(raw));
}

TEST(SniiBkdIndexBlock, RoundTripCarriesFlagsAndNonDefaultLeafCapacity) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.flags = index_flags::kBuiltWithSpill;
    raw.points_per_leaf = 512;

    const std::vector<uint8_t> bytes = encode(raw);
    BkdIndexBlockReader reader;
    ASSERT_TRUE(BkdIndexBlockReader::open(Slice(bytes), kDataLength, &reader).ok());
    EXPECT_EQ(reader.header().flags, index_flags::kBuiltWithSpill);
    EXPECT_EQ(reader.header().points_per_leaf, 512U);
}

// Design 5.3: an empty index is header-only with leaf_count == 0 and a zero-length
// bkd_data. It is a LEGAL state, not corruption.
TEST(SniiBkdIndexBlock, EmptyIndexRoundTrips) {
    RawIndexBlock raw;
    raw.points_per_leaf = 777;
    const std::vector<uint8_t> bytes = encode(raw);
    EXPECT_EQ(bytes, frame(raw));

    BkdIndexBlockReader reader;
    ASSERT_TRUE(BkdIndexBlockReader::open(Slice(bytes), /*data_length=*/0, &reader).ok());
    EXPECT_TRUE(reader.empty());
    EXPECT_EQ(reader.leaf_count(), 0U);
    EXPECT_EQ(reader.header().point_count, 0U);
    EXPECT_EQ(reader.header().doc_count, 0U);
    // The type the index was built with survives even with no points in it.
    EXPECT_EQ(reader.header().bytes_per_dim, kBytesPerDim);
    EXPECT_EQ(reader.header().field_type, FieldType::OLAP_FIELD_TYPE_BIGINT);
    EXPECT_EQ(reader.header().points_per_leaf, 777U);
    EXPECT_TRUE(reader.split_values().empty());
}

// Design 5.1: with a single leaf there is no boundary to record, so split_values is
// empty and the whole value range routes to leaf 0.
TEST(SniiBkdIndexBlock, SingleLeafHasNoSplitValues) {
    RawIndexBlock raw;
    raw.point_count = 4;
    raw.doc_count = 4;
    raw.leaf_count = 1;
    raw.min_value = sortable_bigint(-9);
    raw.max_value = sortable_bigint(9);
    raw.leaf_offset_deltas = {0};
    raw.leaf_counts = {4};

    const std::vector<uint8_t> bytes = encode(raw);
    EXPECT_EQ(bytes, frame(raw));

    BkdIndexBlockReader reader;
    ASSERT_TRUE(BkdIndexBlockReader::open(Slice(bytes), kDataLength, &reader).ok());
    ASSERT_FALSE(reader.empty());
    ASSERT_EQ(reader.leaf_count(), 1U);
    EXPECT_TRUE(reader.split_values().empty());
    EXPECT_TRUE(bytes_equal(reader.min_value(), sortable_bigint(-9)));
    EXPECT_TRUE(bytes_equal(reader.max_value(), sortable_bigint(9)));
    EXPECT_EQ(reader.leaf(0).offset, 0U);
    EXPECT_EQ(reader.leaf(0).count, 4U);
}

// Equal split values are LEGAL (non-decreasing, not strictly increasing): a value
// spanning more than one leaf makes consecutive leaves start at the same value.
TEST(SniiBkdIndexBlock, EqualSplitValuesAreAccepted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.min_value = sortable_bigint(42);
    raw.max_value = sortable_bigint(42);
    raw.split_values.clear();
    append_bytes(&raw.split_values, sortable_bigint(42));
    append_bytes(&raw.split_values, sortable_bigint(42));

    BkdIndexBlockReader reader;
    const Status status = open_raw(raw, &reader);
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader.leaf_count(), 3U);
}

// ---------------------------------------------------------------------------
// Capability boundary: a future format is NOT corruption
// ---------------------------------------------------------------------------

TEST(SniiBkdIndexBlock, FutureFormatVersionIsNotSupported) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.format_version = kSupportedVersion + 1;

    BkdIndexBlockReader reader;
    const Status status = open_raw(raw, &reader);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
    // Must NOT be reported as a damaged segment: the caller falls back to "index
    // unavailable" instead of flagging corruption (design 3 / 8).
    EXPECT_FALSE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

// A far-future version must take the same capability path, not trip a length or
// structure check first.
TEST(SniiBkdIndexBlock, FarFutureFormatVersionIsNotSupported) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.format_version = 4096;
    raw.leaf_count = 0;
    raw.min_value.clear();
    raw.max_value.clear();
    raw.split_values.clear();
    raw.leaf_offset_deltas.clear();
    raw.leaf_counts.clear();

    BkdIndexBlockReader reader;
    EXPECT_TRUE(open_raw(raw, &reader).is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>());
}

// Version 0 was never written by any binary, so it is damage rather than a newer
// format this binary does not know.
TEST(SniiBkdIndexBlock, ZeroFormatVersionIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.format_version = 0;

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

// ---------------------------------------------------------------------------
// Corruption: envelope
// ---------------------------------------------------------------------------

TEST(SniiBkdIndexBlock, BadMagicIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.magic = kBkdIndexMagic ^ 0xFFU;

    BkdIndexBlockReader reader;
    const Status status = open_raw(raw, &reader);
    EXPECT_TRUE(IsCorrupted(status));
    // The magic is checked BEFORE the version, so random bytes that happen to
    // decode a large version are not misreported as an unsupported format.
    EXPECT_FALSE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
}

TEST(SniiBkdIndexBlock, WrongSectionTypeIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.section_type = kBkdIndexSectionType + 1;

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

TEST(SniiBkdIndexBlock, SingleBitFlipIsCaughtByTheFramerChecksum) {
    const RawIndexBlock raw = valid_three_leaf_block();
    std::vector<uint8_t> bytes = frame(raw);
    ASSERT_GT(bytes.size(), 20U);

    for (size_t i = 0; i < bytes.size(); ++i) {
        std::vector<uint8_t> damaged = bytes;
        damaged[i] ^= 0x01U;
        BkdIndexBlockReader reader;
        const Status status = BkdIndexBlockReader::open(Slice(damaged), kDataLength, &reader);
        EXPECT_FALSE(status.ok()) << "byte " << i << " flip accepted";
    }
}

TEST(SniiBkdIndexBlock, TruncationAtAnyLengthIsRejected) {
    const RawIndexBlock raw = valid_three_leaf_block();
    const std::vector<uint8_t> bytes = frame(raw);

    for (size_t len = 0; len < bytes.size(); ++len) {
        BkdIndexBlockReader reader;
        const Status status =
                BkdIndexBlockReader::open(Slice(bytes.data(), len), kDataLength, &reader);
        EXPECT_TRUE(IsCorrupted(status)) << "truncation to " << len << " bytes";
    }
}

TEST(SniiBkdIndexBlock, TrailingBytesAfterTheFramedSectionAreCorrupted) {
    const RawIndexBlock raw = valid_three_leaf_block();
    std::vector<uint8_t> bytes = frame(raw);
    bytes.push_back(0x00);

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(BkdIndexBlockReader::open(Slice(bytes), kDataLength, &reader)));
}

TEST(SniiBkdIndexBlock, TrailingBytesInsideThePayloadAreCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.trailing = {0x00, 0x00};

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

// ---------------------------------------------------------------------------
// Corruption: header self-consistency
// ---------------------------------------------------------------------------

// INV-2: bytes_per_dim is fixed and equals sizeof(CppType) of the recorded type.
// A mismatch would make the fixed-width split array and the whole-record memcmp
// read at the wrong stride.
TEST(SniiBkdIndexBlock, BytesPerDimInconsistentWithFieldTypeIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.bytes_per_dim = 4; // BIGINT is 8 bytes.

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

TEST(SniiBkdIndexBlock, ZeroBytesPerDimIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.bytes_per_dim = 0;

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

// field_type drives the KeyCoder used at query time, so an unrecognised value must
// be rejected HERE and never reach a type-dispatch that assumes it is valid.
TEST(SniiBkdIndexBlock, UnknownFieldTypeIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.field_type = 0xFFFFU;

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

// A string type has no fixed-width sortable-bytes representation, so it can never
// have produced a BKD index (design 2: one dimension, numeric only).
TEST(SniiBkdIndexBlock, NonNumericFieldTypeIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.field_type = static_cast<uint32_t>(FieldType::OLAP_FIELD_TYPE_STRING);

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

// Design 5.3: leaf_count == 0 means no points at all.
TEST(SniiBkdIndexBlock, EmptyIndexWithNonZeroPointCountIsCorrupted) {
    RawIndexBlock raw;
    raw.point_count = 7;

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader, /*data_length=*/0)));
}

TEST(SniiBkdIndexBlock, EmptyIndexWithTrailingArraysIsCorrupted) {
    RawIndexBlock raw;
    raw.min_value = sortable_bigint(1);
    raw.max_value = sortable_bigint(2);

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader, /*data_length=*/0)));
}

// ---------------------------------------------------------------------------
// Corruption: leaf_count must agree with all three arrays
// ---------------------------------------------------------------------------

TEST(SniiBkdIndexBlock, LeafCountLargerThanTheDirectoryIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.leaf_count = 4; // The three arrays still describe three leaves.

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

TEST(SniiBkdIndexBlock, LeafCountSmallerThanTheDirectoryIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.leaf_count = 2;

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

// Anti-DoS: a huge leaf_count must be rejected against the REMAINING payload
// bytes, before anything sized by it is allocated.
TEST(SniiBkdIndexBlock, ImplausibleLeafCountIsRejectedWithoutAllocating) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.leaf_count = 1U << 30;

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

TEST(SniiBkdIndexBlock, SplitValueArrayShorterThanLeafCountIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.split_values.resize(kBytesPerDim); // one split value for three leaves

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

TEST(SniiBkdIndexBlock, LeafOffsetArrayShorterThanLeafCountIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.leaf_offset_deltas = {0, 40};

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

TEST(SniiBkdIndexBlock, LeafCountArrayShorterThanLeafCountIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.leaf_counts = {4, 4};

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

// ---------------------------------------------------------------------------
// Corruption: array ordering and bounds
// ---------------------------------------------------------------------------

// The split array is binary-searched, so an unordered array would silently route
// queries to the wrong leaf -- wrong results, no error. It must be rejected at open.
TEST(SniiBkdIndexBlock, SplitValuesNotAscendingIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.split_values.clear();
    append_bytes(&raw.split_values, sortable_bigint(200));
    append_bytes(&raw.split_values, sortable_bigint(100));

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

// Ordering is UNSIGNED byte comparison, MSB first (INV-1). 0x80.. sorts above
// 0x7F.. even though the same bytes read as a signed value would not.
TEST(SniiBkdIndexBlock, SplitValueOrderingIsUnsignedByteWise) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.min_value = std::vector<uint8_t>(kBytesPerDim, 0x00);
    raw.max_value = std::vector<uint8_t>(kBytesPerDim, 0xFF);
    raw.split_values.clear();
    // 0x80... then 0x7F...: descending under unsigned byte order.
    raw.split_values.push_back(0x80);
    raw.split_values.resize(kBytesPerDim, 0x00);
    raw.split_values.push_back(0x7F);
    raw.split_values.resize(2 * kBytesPerDim, 0x00);

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

TEST(SniiBkdIndexBlock, RepeatedLeafOffsetIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.leaf_offset_deltas = {0, 40, 0}; // offsets 0, 40, 40

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

TEST(SniiBkdIndexBlock, LastLeafOffsetBeyondDataLengthIsCorrupted) {
    const RawIndexBlock raw = valid_three_leaf_block(); // last offset == 90

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader, /*data_length=*/89)));
    // Exactly at the bound is accepted.
    BkdIndexBlockReader tight;
    EXPECT_TRUE(open_raw(raw, &tight, /*data_length=*/90).ok());
}

TEST(SniiBkdIndexBlock, LeafOffsetSumOverflowIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.leaf_offset_deltas = {0, UINT64_MAX, UINT64_MAX};

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader, /*data_length=*/UINT64_MAX)));
}

// sum(leaf_counts) == point_count ties the directory to the header; a mismatch
// means one of the two is damaged and neither can be trusted.
TEST(SniiBkdIndexBlock, LeafCountsNotSummingToPointCountIsCorrupted) {
    RawIndexBlock raw = valid_three_leaf_block();
    raw.point_count = 9; // counts still sum to 10

    BkdIndexBlockReader reader;
    EXPECT_TRUE(IsCorrupted(open_raw(raw, &reader)));
}

// The sum must accumulate in 64 bits: a uint32 accumulator would wrap and reject
// this otherwise consistent directory (or, worse, accept a damaged one whose
// wrapped sum lands on point_count).
//
// Every count here is a LEGAL one (== points_per_leaf), so the total is pushed
// past 2^32 by the number of leaves rather than by absurd per-leaf counts. That
// distinction matters: a directory whose counts exceed points_per_leaf is now
// rejected outright, because such a count is what sizes the leaf decode
// allocation. This test must exercise the accumulator, not the bomb.
TEST(SniiBkdIndexBlock, LeafCountSumAccumulatesIn64Bits) {
    constexpr uint32_t kLeaves = 5000; // 5000 * 2^20 > 2^32
    RawIndexBlock raw;
    raw.points_per_leaf = kMaxPointsPerLeaf;
    raw.leaf_count = kLeaves;
    raw.point_count = uint64_t {kLeaves} * kMaxPointsPerLeaf;
    raw.doc_count = UINT32_MAX;
    raw.min_value = sortable_bigint(0);
    raw.max_value = sortable_bigint(kLeaves);
    for (uint32_t i = 1; i < kLeaves; ++i) {
        append_bytes(&raw.split_values, sortable_bigint(i));
    }
    raw.leaf_offset_deltas.assign(kLeaves, 8);
    raw.leaf_offset_deltas[0] = 0;
    raw.leaf_counts.assign(kLeaves, kMaxPointsPerLeaf);

    BkdIndexBlockReader reader;
    const Status status = open_raw(raw, &reader, /*data_length=*/UINT64_MAX);
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader.header().point_count, uint64_t {kLeaves} * kMaxPointsPerLeaf);
    EXPECT_GT(reader.header().point_count, uint64_t {UINT32_MAX});
}

// ---------------------------------------------------------------------------
// Scale
// ---------------------------------------------------------------------------

// A directory big enough to exercise multi-byte varint deltas and a long
// non-decreasing split array.
TEST(SniiBkdIndexBlock, LargeDirectoryRoundTrips) {
    constexpr uint32_t kLeaves = 4096;
    constexpr uint32_t kPointsPerLeaf = 1024;

    RawIndexBlock raw;
    raw.leaf_count = kLeaves;
    // Stated explicitly: this case deliberately uses 1024-point leaves, which is
    // legal only while the header says so. Leaving it at the default made the
    // block self-inconsistent the moment the default moved, and open() rejected
    // it -- correctly, since a leaf count above points_per_leaf is exactly the
    // unbounded-allocation shape that check exists to catch.
    raw.points_per_leaf = kPointsPerLeaf;
    raw.point_count = uint64_t {kLeaves} * kPointsPerLeaf;
    raw.doc_count = kLeaves * kPointsPerLeaf;
    raw.min_value = sortable_bigint(0);
    raw.max_value = sortable_bigint(int64_t {kLeaves} * 1000);
    for (uint32_t i = 1; i < kLeaves; ++i) {
        append_bytes(&raw.split_values, sortable_bigint(int64_t {i} * 1000));
    }
    raw.leaf_offset_deltas.assign(kLeaves, 3000);
    raw.leaf_offset_deltas[0] = 0;
    raw.leaf_counts.assign(kLeaves, kPointsPerLeaf);

    const std::vector<uint8_t> bytes = encode(raw);
    EXPECT_EQ(bytes, frame(raw));

    BkdIndexBlockReader reader;
    ASSERT_TRUE(BkdIndexBlockReader::open(Slice(bytes), /*data_length=*/uint64_t {kLeaves} * 3000,
                                          &reader)
                        .ok());
    ASSERT_EQ(reader.leaf_count(), kLeaves);
    EXPECT_EQ(reader.header().point_count, uint64_t {kLeaves} * kPointsPerLeaf);
    ASSERT_EQ(reader.split_values().size(), (kLeaves - 1) * size_t {kBytesPerDim});
    EXPECT_TRUE(bytes_equal(reader.split_value(0), sortable_bigint(1000)));
    EXPECT_TRUE(bytes_equal(reader.split_value(kLeaves - 2),
                            sortable_bigint(int64_t {kLeaves - 1} * 1000)));
    EXPECT_EQ(reader.leaf(kLeaves - 1).offset, uint64_t {kLeaves - 1} * 3000);
    EXPECT_EQ(reader.leaf(kLeaves - 1).count, kPointsPerLeaf);
}

} // namespace
} // namespace doris::snii::bkd
