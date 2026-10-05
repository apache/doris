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

#include "storage/index/snii/bkd/leaf_codec.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/pfor.h"
#include "storage/index/snii/encoding/varint.h"

namespace doris::snii::bkd {
namespace {

constexpr uint32_t kBytesPerDim = sizeof(int64_t);
constexpr uint32_t kRecordSize = kBytesPerDim + kPointDocIdBytes;

// Unsigned big-endian sortable bytes for a BIGINT -- what
// KeyCoder::full_encode_ascending emits (sign bit flipped, then byte-swapped).
// The codec compares and prefix-compresses values with plain memcmp from offset 0
// (INV-1), so feeding little-endian bytes would produce a self-consistent but
// semantically wrong leaf.
std::vector<uint8_t> sortable_bigint(int64_t v) {
    const uint64_t u = static_cast<uint64_t>(v) ^ (uint64_t {1} << 63);
    std::vector<uint8_t> out(kBytesPerDim);
    for (uint32_t i = 0; i < kBytesPerDim; ++i) {
        out[kBytesPerDim - 1 - i] = static_cast<uint8_t>(u >> (8 * i));
    }
    return out;
}

struct Point {
    int64_t value = 0;
    uint32_t doc_id = 0;
};

// The build-time point array of design 6.2: fixed-width
// [value: bytes_per_dim][doc_id: 4 big-endian] records whose whole-record memcmp
// IS (value, doc_id) order.
std::vector<uint8_t> pack(const std::vector<Point>& points) {
    std::vector<uint8_t> records;
    records.reserve(points.size() * kRecordSize);
    for (const Point& point : points) {
        const std::vector<uint8_t> value = sortable_bigint(point.value);
        records.insert(records.end(), value.begin(), value.end());
        for (uint32_t i = 0; i < kPointDocIdBytes; ++i) {
            records.push_back(
                    static_cast<uint8_t>(point.doc_id >> (8 * (kPointDocIdBytes - 1 - i))));
        }
    }
    return records;
}

std::vector<uint8_t> encode(const std::vector<Point>& points) {
    const std::vector<uint8_t> records = pack(points);
    ByteSink sink;
    encode_leaf_block(Slice(records), kBytesPerDim, &sink);
    return sink.take();
}

// Every rejection below must be a Status, never a crash and never an out-of-bounds
// read -- that is the whole point of routing leaf bytes through ByteSource and
// checking each field as it is decoded (design 8).
::testing::AssertionResult IsCorrupted(const Status& status) {
    if (status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) {
        return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure()
           << "expected INVERTED_INDEX_FILE_CORRUPTED, got " << status;
}

// Rebuilds each point's full value from the decoded prefix + its run's suffix and
// compares the whole leaf against the points that went in.
::testing::AssertionResult LeafMatches(const std::vector<Point>& points,
                                       const DecodedLeafBlock& leaf) {
    if (leaf.point_count != points.size()) {
        return ::testing::AssertionFailure()
               << "point_count " << leaf.point_count << " != " << points.size();
    }
    if (leaf.doc_ids.size() != points.size()) {
        return ::testing::AssertionFailure()
               << "doc_ids.size() " << leaf.doc_ids.size() << " != " << points.size();
    }
    if (leaf.common_prefix.size() + leaf.suffix_width != kBytesPerDim) {
        return ::testing::AssertionFailure()
               << "common_prefix " << leaf.common_prefix.size() << " + suffix_width "
               << leaf.suffix_width << " != " << kBytesPerDim;
    }
    uint32_t covered = 0;
    for (const LeafValueRun& run : leaf.runs) {
        if (run.first_point != covered) {
            return ::testing::AssertionFailure()
                   << "run starts at " << run.first_point << ", expected " << covered;
        }
        if (run.count == 0) {
            return ::testing::AssertionFailure() << "run at " << run.first_point << " is empty";
        }
        if (run.suffix.size() != leaf.suffix_width) {
            return ::testing::AssertionFailure()
                   << "run suffix width " << run.suffix.size() << " != " << leaf.suffix_width;
        }
        for (uint32_t i = 0; i < run.count; ++i) {
            const uint32_t point = run.first_point + i;
            std::vector<uint8_t> value(leaf.common_prefix.data(),
                                       leaf.common_prefix.data() + leaf.common_prefix.size());
            value.insert(value.end(), run.suffix.data(), run.suffix.data() + run.suffix.size());
            if (value != sortable_bigint(points[point].value)) {
                return ::testing::AssertionFailure() << "value mismatch at point " << point;
            }
            if (leaf.doc_ids[point] != points[point].doc_id) {
                return ::testing::AssertionFailure()
                       << "doc id " << leaf.doc_ids[point] << " != " << points[point].doc_id
                       << " at point " << point;
            }
        }
        covered += run.count;
    }
    if (covered != points.size()) {
        return ::testing::AssertionFailure()
               << "runs cover " << covered << " points, expected " << points.size();
    }
    return ::testing::AssertionSuccess();
}

// Encodes, decodes both ways, and asserts the leaf came back intact in the
// expected value mode. The doc-id-only path must agree with the full decode --
// they are separate functions and a divergence would make whole-leaf hits return
// different rows than boundary-leaf hits.
void ExpectRoundTrip(const std::vector<Point>& points, LeafValueMode expected_mode) {
    const std::vector<uint8_t> block = encode(points);
    const uint32_t count = static_cast<uint32_t>(points.size());

    DecodedLeafBlock leaf;
    ASSERT_TRUE(decode_leaf_block(Slice(block), kBytesPerDim, count, &leaf).ok());
    EXPECT_EQ(leaf.value_mode, expected_mode);
    EXPECT_TRUE(LeafMatches(points, leaf));

    std::vector<uint32_t> doc_ids;
    ASSERT_TRUE(decode_leaf_doc_ids(Slice(block), kBytesPerDim, count, &doc_ids).ok());
    EXPECT_EQ(doc_ids, leaf.doc_ids);
}

// ---- byte-level surgery for the corruption cases -------------------------
// The offsets are derived from the documented layout rather than hard-coded, so a
// fixture with a different point count or prefix length stays valid.

size_t mode_byte_index(uint32_t point_count) {
    return varint_len(point_count);
}
size_t prefix_len_index(uint32_t point_count) {
    return varint_len(point_count) + 1;
}
size_t value_area_index(uint32_t point_count, uint32_t common_prefix_len) {
    return varint_len(point_count) + 1 + varint_len(common_prefix_len) + common_prefix_len;
}

// Rewrites the trailing { docid_block_offset varint32, offset_length u8 }.
void set_tail_offset(std::vector<uint8_t>* block, uint32_t offset) {
    const size_t length = block->back();
    block->resize(block->size() - 1 - length);
    ByteSink tail;
    tail.put_varint32(offset);
    const std::vector<uint8_t>& bytes = tail.buffer();
    block->insert(block->end(), bytes.begin(), bytes.end());
    block->push_back(static_cast<uint8_t>(bytes.size()));
}

uint32_t tail_offset_of(const std::vector<uint8_t>& block) {
    const size_t length = block.back();
    ByteSource src(Slice(block.data() + block.size() - 1 - length, length));
    uint32_t offset = 0;
    EXPECT_TRUE(src.get_varint32(&offset).ok());
    return offset;
}

// ---- fixtures ------------------------------------------------------------

// One value repeated: the common prefix covers the whole width and no suffix is
// stored at all.
std::vector<Point> all_equal_points() {
    return {{42, 3}, {42, 7}, {42, 9}, {42, 20}, {42, 21}, {42, 100}};
}

// Three values sharing a 7-byte prefix over 8 points (runs 3 / 3 / 2), the
// smallest shape for which run-length coding actually pays; see
// ModeSwitchesToRleOnlyWhenItIsStrictlySmaller.
std::vector<Point> rle_points() {
    return {{1, 0}, {1, 5}, {1, 6}, {2, 1}, {2, 4}, {2, 9}, {3, 2}, {3, 8}};
}

// Eight distinct values sharing a 7-byte prefix: one run per point, so run-length
// coding would only add a length byte per value.
std::vector<Point> raw_points() {
    return {{1, 40}, {2, 3}, {3, 90}, {4, 1}, {5, 77}, {6, 12}, {7, 5}, {8, 61}};
}

// ---------------------------------------------------------------------------
// Round trips, one per value mode
// ---------------------------------------------------------------------------

TEST(SniiBkdLeafCodec, AllEqualLeafRoundTrips) {
    const std::vector<Point> points = all_equal_points();
    ExpectRoundTrip(points, LeafValueMode::kAllEqual);

    DecodedLeafBlock leaf;
    const std::vector<uint8_t> block = encode(points);
    ASSERT_TRUE(decode_leaf_block(Slice(block), kBytesPerDim, 6, &leaf).ok());
    // Design 5.2: the prefix IS the value and there is no suffix data, so the
    // whole leaf is one run.
    EXPECT_EQ(leaf.common_prefix.size(), kBytesPerDim);
    EXPECT_EQ(leaf.suffix_width, 0U);
    ASSERT_EQ(leaf.runs.size(), 1U);
    EXPECT_EQ(leaf.runs[0].first_point, 0U);
    EXPECT_EQ(leaf.runs[0].count, 6U);
    EXPECT_TRUE(leaf.runs[0].suffix.empty());
    // Six points cost one value plus ascending doc id deltas, far below the
    // 6 * 8 bytes a raw value array would need.
    EXPECT_LT(block.size(), 6 * kBytesPerDim);
}

TEST(SniiBkdLeafCodec, RleLeafRoundTrips) {
    const std::vector<Point> points = rle_points();
    ExpectRoundTrip(points, LeafValueMode::kRle);

    DecodedLeafBlock leaf;
    const std::vector<uint8_t> block = encode(points);
    ASSERT_TRUE(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf).ok());
    EXPECT_EQ(leaf.common_prefix.size(), kBytesPerDim - 1);
    EXPECT_EQ(leaf.suffix_width, 1U);
    ASSERT_EQ(leaf.runs.size(), 3U);
    EXPECT_EQ(leaf.runs[0].count, 3U);
    EXPECT_EQ(leaf.runs[1].first_point, 3U);
    EXPECT_EQ(leaf.runs[1].count, 3U);
    EXPECT_EQ(leaf.runs[2].first_point, 6U);
    EXPECT_EQ(leaf.runs[2].count, 2U);
}

TEST(SniiBkdLeafCodec, RawLeafRoundTrips) {
    const std::vector<Point> points = raw_points();
    ExpectRoundTrip(points, LeafValueMode::kRaw);

    DecodedLeafBlock leaf;
    const std::vector<uint8_t> block = encode(points);
    ASSERT_TRUE(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf).ok());
    EXPECT_EQ(leaf.suffix_width, 1U);
    // Distinct values means one run per point.
    ASSERT_EQ(leaf.runs.size(), 8U);
    for (uint32_t i = 0; i < 8; ++i) {
        EXPECT_EQ(leaf.runs[i].first_point, i);
        EXPECT_EQ(leaf.runs[i].count, 1U);
    }
}

TEST(SniiBkdLeafCodec, SinglePointLeafIsAllEqual) {
    ExpectRoundTrip({{-7, 12345}}, LeafValueMode::kAllEqual);
}

// Design 6.4: only the last leaf is short, so a full leaf is the common case.
TEST(SniiBkdLeafCodec, FullLeafRoundTrips) {
    std::vector<Point> points;
    points.reserve(kDefaultPointsPerLeaf);
    for (uint32_t i = 0; i < kDefaultPointsPerLeaf; ++i) {
        // Distinct values, doc ids deliberately NOT ascending across values --
        // only inside a run are they ordered, and kRaw stores them as they are.
        points.push_back({static_cast<int64_t>(i) * 7 - 3000, (i * 37) % kDefaultPointsPerLeaf});
    }
    ExpectRoundTrip(points, LeafValueMode::kRaw);
}

// The two extremes of the common prefix. Zero shared bytes is what a leaf spanning
// the type's whole range looks like.
TEST(SniiBkdLeafCodec, ZeroCommonPrefixRoundTrips) {
    const std::vector<Point> points = {{std::numeric_limits<int64_t>::min(), 0},
                                       {-1, 1},
                                       {0, 2},
                                       {std::numeric_limits<int64_t>::max(), 3}};
    const std::vector<uint8_t> block = encode(points);

    DecodedLeafBlock leaf;
    ASSERT_TRUE(decode_leaf_block(Slice(block), kBytesPerDim, 4, &leaf).ok());
    EXPECT_EQ(leaf.common_prefix.size(), 0U);
    EXPECT_EQ(leaf.suffix_width, kBytesPerDim);
    EXPECT_TRUE(LeafMatches(points, leaf));
}

// An array column may repeat one value inside one row, which produces two
// identical (value, doc_id) records. The run delta for the second one is zero.
TEST(SniiBkdLeafCodec, RepeatedValueForOneDocRoundTrips) {
    ExpectRoundTrip({{5, 11}, {5, 11}, {5, 12}}, LeafValueMode::kAllEqual);
}

// ---------------------------------------------------------------------------
// Mode selection
// ---------------------------------------------------------------------------

// With S = 1 and every run length below 128, the value area costs
//   raw = point_count, rle = varint_len(run_count) + run_count * 2
// so three runs pay off from the eighth point on. The tie at seven points must go
// to kRaw: equal size, cheaper decode.
TEST(SniiBkdLeafCodec, ModeSwitchesToRleOnlyWhenItIsStrictlySmaller) {
    const std::vector<Point> tie = {{1, 0}, {1, 5}, {1, 6}, {2, 1}, {2, 4}, {3, 2}, {3, 8}};
    ASSERT_EQ(tie.size(), 7U);
    ExpectRoundTrip(tie, LeafValueMode::kRaw);

    // One more point in the same three runs, and run-length coding wins by a byte.
    const std::vector<Point> win = rle_points();
    ASSERT_EQ(win.size(), 8U);
    ExpectRoundTrip(win, LeafValueMode::kRle);
}

// One long run plus a tail of singletons: run-length coding is a large win even
// though most values are distinct.
TEST(SniiBkdLeafCodec, LongRunPrefersRle) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 60; ++i) {
        points.push_back({100, i});
    }
    for (uint32_t i = 0; i < 4; ++i) {
        points.push_back({101 + i, 200 + i});
    }
    ExpectRoundTrip(points, LeafValueMode::kRle);
}

// ---------------------------------------------------------------------------
// Format
// ---------------------------------------------------------------------------

// Pins the on-disk layout of design 5.2 byte for byte. Assembled from the
// encoding primitives directly, so nothing here depends on leaf_codec's own
// serialization; a silent format drift breaks this test and nothing else.
TEST(SniiBkdLeafCodec, EncoderMatchesDocumentedLayout) {
    const std::vector<Point> points = rle_points();
    const std::vector<uint8_t> first_value = sortable_bigint(1);
    const std::vector<uint8_t> prefix(first_value.begin(), first_value.end() - 1);

    ByteSink expected;
    expected.put_varint32(8);                                   // point_count
    expected.put_u8(static_cast<uint8_t>(LeafValueMode::kRle)); // value_mode
    expected.put_varint32(kBytesPerDim - 1);                    // common_prefix_len
    expected.put_bytes(Slice(prefix));                          // common_prefix
    expected.put_varint32(3);                                   // run_count
    const uint8_t suffixes[3] = {1, 2, 3};
    const uint32_t run_lengths[3] = {3, 3, 2};
    for (uint32_t run = 0; run < 3; ++run) {
        expected.put_bytes(Slice(&suffixes[run], 1));
        expected.put_varint32(run_lengths[run]);
    }
    const uint32_t docid_block_offset = static_cast<uint32_t>(expected.size());
    // Doc id deltas restart at every run, so each run's first code is absolute.
    const uint32_t codes[8] = {0, 5, 1, 1, 3, 5, 2, 6};
    pfor_encode(codes, 8, &expected);
    ByteSink tail;
    tail.put_varint32(docid_block_offset);
    expected.put_bytes(tail.view());
    expected.put_u8(static_cast<uint8_t>(tail.size()));

    EXPECT_EQ(encode(points), expected.buffer());
}

// ---------------------------------------------------------------------------
// Corruption: every one of these must come back as a Status
// ---------------------------------------------------------------------------

TEST(SniiBkdLeafCodec, EmptyBlockIsCorrupted) {
    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(), kBytesPerDim, 1, &leaf)));
    std::vector<uint32_t> doc_ids;
    EXPECT_TRUE(IsCorrupted(decode_leaf_doc_ids(Slice(), kBytesPerDim, 1, &doc_ids)));
}

// Design 5.2: a point_count that disagrees with the leaf directory is corruption.
// It is also what bounds the decode allocation.
TEST(SniiBkdLeafCodec, PointCountDisagreeingWithTheDirectoryIsCorrupted) {
    const std::vector<uint8_t> block = encode(raw_points());
    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 7, &leaf)));
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 9, &leaf)));
    std::vector<uint32_t> doc_ids;
    EXPECT_TRUE(IsCorrupted(decode_leaf_doc_ids(Slice(block), kBytesPerDim, 9, &doc_ids)));
}

TEST(SniiBkdLeafCodec, UnknownValueModeIsCorrupted) {
    std::vector<uint8_t> block = encode(raw_points());
    block[mode_byte_index(8)] = static_cast<uint8_t>(kMaxLeafValueMode) + 1;

    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf)));
    std::vector<uint32_t> doc_ids;
    EXPECT_TRUE(IsCorrupted(decode_leaf_doc_ids(Slice(block), kBytesPerDim, 8, &doc_ids)));
}

TEST(SniiBkdLeafCodec, CommonPrefixLongerThanTheValueIsCorrupted) {
    std::vector<uint8_t> block = encode(raw_points());
    block[prefix_len_index(8)] = static_cast<uint8_t>(kBytesPerDim + 1);

    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf)));
}

// kAllEqual means the prefix IS the value; a leaf claiming it while leaving suffix
// bytes unaccounted for would silently drop every value distinction.
TEST(SniiBkdLeafCodec, AllEqualWithAShortPrefixIsCorrupted) {
    std::vector<uint8_t> block = encode(raw_points());
    block[mode_byte_index(8)] = static_cast<uint8_t>(LeafValueMode::kAllEqual);

    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf)));
}

// The mirror case: a full-width prefix leaves no suffix for kRaw / kRle to store.
TEST(SniiBkdLeafCodec, SuffixModeWithAFullWidthPrefixIsCorrupted) {
    std::vector<uint8_t> block = encode(all_equal_points());
    block[mode_byte_index(6)] = static_cast<uint8_t>(LeafValueMode::kRaw);

    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 6, &leaf)));
}

TEST(SniiBkdLeafCodec, RunLengthsMustSumToPointCount) {
    std::vector<uint8_t> block = encode(rle_points());
    // First run's length varint sits right after run_count and the first suffix.
    const size_t first_run_length = value_area_index(8, kBytesPerDim - 1) + 1 + 1;
    ASSERT_EQ(block[first_run_length], 3U);
    block[first_run_length] = 4;

    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf)));
    std::vector<uint32_t> doc_ids;
    EXPECT_TRUE(IsCorrupted(decode_leaf_doc_ids(Slice(block), kBytesPerDim, 8, &doc_ids)));
}

TEST(SniiBkdLeafCodec, ZeroRunLengthIsCorrupted) {
    std::vector<uint8_t> block = encode(rle_points());
    const size_t first_run_length = value_area_index(8, kBytesPerDim - 1) + 1 + 1;
    block[first_run_length] = 0;

    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf)));
}

// Values out of order would silently break the boundary-leaf early exit (a range
// scan stops at the first value past the upper bound), so they must never decode.
TEST(SniiBkdLeafCodec, DescendingSuffixesAreCorrupted) {
    std::vector<uint8_t> block = encode(raw_points());
    const size_t suffixes = value_area_index(8, kBytesPerDim - 1);
    std::swap(block[suffixes], block[suffixes + 1]);

    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf)));
}

TEST(SniiBkdLeafCodec, DescendingRunSuffixesAreCorrupted) {
    std::vector<uint8_t> block = encode(rle_points());
    const size_t first_suffix = value_area_index(8, kBytesPerDim - 1) + 1;
    const size_t second_suffix = first_suffix + 2;
    std::swap(block[first_suffix], block[second_suffix]);

    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf)));
}

// A damaged tail offset must never let the doc-id-only path reinterpret value
// bytes as doc ids, so it is cross-checked against where the value area actually
// ends.
TEST(SniiBkdLeafCodec, TailOffsetDisagreeingWithTheValueAreaIsCorrupted) {
    for (const std::vector<Point>& points : {all_equal_points(), rle_points(), raw_points()}) {
        const uint32_t count = static_cast<uint32_t>(points.size());
        const std::vector<uint8_t> encoded = encode(points);
        const uint32_t offset = tail_offset_of(encoded);

        for (uint32_t damaged : {offset - 1, offset + 1}) {
            std::vector<uint8_t> block = encoded;
            set_tail_offset(&block, damaged);
            DecodedLeafBlock leaf;
            EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, count, &leaf)));
            std::vector<uint32_t> doc_ids;
            EXPECT_TRUE(
                    IsCorrupted(decode_leaf_doc_ids(Slice(block), kBytesPerDim, count, &doc_ids)));
        }
    }
}

TEST(SniiBkdLeafCodec, TailOffsetPastTheBlockIsCorrupted) {
    std::vector<uint8_t> block = encode(raw_points());
    set_tail_offset(&block, static_cast<uint32_t>(block.size()) + 1000);

    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf)));
    std::vector<uint32_t> doc_ids;
    EXPECT_TRUE(IsCorrupted(decode_leaf_doc_ids(Slice(block), kBytesPerDim, 8, &doc_ids)));
}

TEST(SniiBkdLeafCodec, BadTailOffsetLengthIsCorrupted) {
    const std::vector<uint8_t> encoded = encode(raw_points());
    for (uint8_t length : {uint8_t {0}, uint8_t {6}, uint8_t {255}}) {
        std::vector<uint8_t> block = encoded;
        block.back() = length;
        DecodedLeafBlock leaf;
        EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf)));
    }
}

TEST(SniiBkdLeafCodec, ExtraBytesBetweenDocIdsAndTailAreCorrupted) {
    std::vector<uint8_t> block = encode(raw_points());
    const size_t tail_length = block.back();
    block.insert(block.begin() + static_cast<ptrdiff_t>(block.size() - 1 - tail_length),
                 uint8_t {0});

    DecodedLeafBlock leaf;
    EXPECT_TRUE(IsCorrupted(decode_leaf_block(Slice(block), kBytesPerDim, 8, &leaf)));
    std::vector<uint32_t> doc_ids;
    EXPECT_TRUE(IsCorrupted(decode_leaf_doc_ids(Slice(block), kBytesPerDim, 8, &doc_ids)));
}

// Truncation is the classic damaged-tail-of-file shape. Every strict prefix of a
// good block must be rejected rather than half-decoded.
TEST(SniiBkdLeafCodec, EveryTruncationIsCorrupted) {
    const std::vector<uint8_t> encoded = encode(rle_points());
    for (size_t length = 0; length < encoded.size(); ++length) {
        const Slice block(encoded.data(), length);
        DecodedLeafBlock leaf;
        EXPECT_FALSE(decode_leaf_block(block, kBytesPerDim, 8, &leaf).ok())
                << "truncated to " << length;
        std::vector<uint32_t> doc_ids;
        EXPECT_FALSE(decode_leaf_doc_ids(block, kBytesPerDim, 8, &doc_ids).ok())
                << "truncated to " << length;
    }
}

// Bit rot sweep (design 12.4): every single-byte corruption of a good block must
// either decode to something structurally consistent or return a Status. Under
// ASAN this is also the assertion that no path reads out of bounds.
TEST(SniiBkdLeafCodec, SingleByteFlipNeverCrashes) {
    for (const std::vector<Point>& points : {all_equal_points(), rle_points(), raw_points()}) {
        const uint32_t count = static_cast<uint32_t>(points.size());
        const std::vector<uint8_t> encoded = encode(points);
        for (size_t i = 0; i < encoded.size(); ++i) {
            for (uint8_t mask : {uint8_t {0x01}, uint8_t {0x80}, uint8_t {0xFF}}) {
                std::vector<uint8_t> block = encoded;
                block[i] ^= mask;

                DecodedLeafBlock leaf;
                if (decode_leaf_block(Slice(block), kBytesPerDim, count, &leaf).ok()) {
                    EXPECT_EQ(leaf.point_count, count);
                    EXPECT_EQ(leaf.doc_ids.size(), count);
                }
                std::vector<uint32_t> doc_ids;
                if (decode_leaf_doc_ids(Slice(block), kBytesPerDim, count, &doc_ids).ok()) {
                    EXPECT_EQ(doc_ids.size(), count);
                }
            }
        }
    }
}

} // namespace
} // namespace doris::snii::bkd
