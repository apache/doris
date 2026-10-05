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

#include "storage/index/snii/bkd/point_sorter.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <random>
#include <string_view>
#include <vector>

#include "storage/index/snii/bkd/bkd_format.h"

namespace doris::snii::bkd {
namespace {

constexpr uint32_t kBytesPerDim = sizeof(int64_t);
constexpr uint32_t kRecordSize = kBytesPerDim + kPointDocIdBytes;

// ---- the build-time record layout of design 6.2 ---------------------------

struct Point {
    int64_t value = 0;
    uint32_t doc_id = 0;
};

void append_big_endian(uint64_t value, uint32_t width, std::vector<uint8_t>* out) {
    for (uint32_t i = 0; i < width; ++i) {
        out->push_back(static_cast<uint8_t>(value >> (8 * (width - 1 - i))));
    }
}

uint64_t read_big_endian(const uint8_t* bytes, uint32_t width) {
    uint64_t value = 0;
    for (uint32_t i = 0; i < width; ++i) {
        value = (value << 8) | bytes[i];
    }
    return value;
}

// Unsigned big-endian sortable bytes for a BIGINT -- what
// KeyCoder::full_encode_ascending emits (sign bit flipped, then byte-swapped).
// The sorter compares records with plain memcmp from offset 0 (INV-1), so this
// encoding is what makes the byte order agree with the numeric order; feeding it
// little-endian or un-flipped bytes would produce a self-consistently sorted but
// semantically wrong array.
std::vector<uint8_t> sortable_bigint(int64_t value) {
    std::vector<uint8_t> out;
    append_big_endian(static_cast<uint64_t>(value) ^ (uint64_t {1} << 63), kBytesPerDim, &out);
    return out;
}

// [value: 8 BE sortable][doc_id: 4 BE] per point, concatenated -- the builder's
// own buffer, which is exactly what the sorter is handed.
std::vector<uint8_t> pack(const std::vector<Point>& points) {
    std::vector<uint8_t> records;
    records.reserve(points.size() * kRecordSize);
    for (const Point& point : points) {
        const std::vector<uint8_t> value = sortable_bigint(point.value);
        records.insert(records.end(), value.begin(), value.end());
        append_big_endian(point.doc_id, kPointDocIdBytes, &records);
    }
    return records;
}

std::vector<Point> unpack(const std::vector<uint8_t>& records) {
    std::vector<Point> points;
    for (size_t offset = 0; offset < records.size(); offset += kRecordSize) {
        const uint64_t encoded = read_big_endian(records.data() + offset, kBytesPerDim);
        points.push_back({static_cast<int64_t>(encoded ^ (uint64_t {1} << 63)),
                          static_cast<uint32_t>(read_big_endian(
                                  records.data() + offset + kBytesPerDim, kPointDocIdBytes))});
    }
    return points;
}

// ---- assertions -----------------------------------------------------------

std::vector<std::string_view> records_of(const std::vector<uint8_t>& buffer, uint32_t record_size) {
    std::vector<std::string_view> out;
    for (size_t offset = 0; offset < buffer.size(); offset += record_size) {
        out.emplace_back(reinterpret_cast<const char*>(buffer.data() + offset), record_size);
    }
    return out;
}

bool record_less(std::string_view lhs, std::string_view rhs) {
    return std::memcmp(lhs.data(), rhs.data(), lhs.size()) < 0;
}

// Sorts a copy of `input` and pins the EXACT expected bytes against an independent
// reference sort. The sorted order is unique -- the whole record is the key, so
// records that compare equal are byte-identical -- which is what makes an exact
// comparison legitimate rather than over-specified. It also catches an in-place
// permutation that drops or duplicates a record, which a sortedness-only check
// would not.
void ExpectSorts(const std::vector<uint8_t>& input, uint32_t record_size) {
    ASSERT_EQ(input.size() % record_size, 0U);
    std::vector<std::string_view> expected = records_of(input, record_size);
    std::sort(expected.begin(), expected.end(), record_less);

    std::vector<uint8_t> records = input;
    point_sorter::sort(records.data(), records.size() / record_size, record_size);
    EXPECT_EQ(records_of(records, record_size), expected);
}

// ---- fixtures -------------------------------------------------------------

std::vector<uint8_t> random_records(size_t count, uint32_t record_size, uint32_t alphabet,
                                    std::mt19937* rng) {
    std::uniform_int_distribution<uint32_t> byte(0, alphabet - 1);
    std::vector<uint8_t> records;
    records.reserve(count * record_size);
    for (size_t i = 0; i < count * record_size; ++i) {
        records.push_back(static_cast<uint8_t>(byte(*rng)));
    }
    return records;
}

// Descending records: every adjacent pair is out of order, so nothing can come
// back right by accident.
std::vector<uint8_t> descending_records(size_t count, uint32_t record_size) {
    std::vector<uint8_t> records;
    for (size_t i = 0; i < count; ++i) {
        append_big_endian(count - 1 - i, record_size, &records);
    }
    return records;
}

// ---------------------------------------------------------------------------
// Degenerate sizes
// ---------------------------------------------------------------------------

TEST(SniiBkdPointSorter, EmptyInputTouchesNothing) {
    std::vector<uint8_t> records(kRecordSize, 0xAB);
    const std::vector<uint8_t> before = records;
    point_sorter::sort(records.data(), 0, kRecordSize);
    EXPECT_EQ(records, before);
}

TEST(SniiBkdPointSorter, SinglePointTouchesNothing) {
    std::vector<uint8_t> records = pack({{-7, 41}});
    const std::vector<uint8_t> before = records;
    point_sorter::sort(records.data(), 1, kRecordSize);
    EXPECT_EQ(records, before);
}

// ---------------------------------------------------------------------------
// The (value, doc_id) key
// ---------------------------------------------------------------------------

TEST(SniiBkdPointSorter, RandomPointsSortByValueThenDocId) {
    // A value range narrow enough that duplicates are common, so the doc id
    // tie-break inside a run is actually exercised rather than merely present.
    std::mt19937 rng(20260801);
    std::uniform_int_distribution<int64_t> value(-40, 40);
    std::uniform_int_distribution<uint32_t> doc_id(0, 100000);
    std::vector<Point> points;
    for (int i = 0; i < 5000; ++i) {
        points.push_back({value(rng), doc_id(rng)});
    }
    ExpectSorts(pack(points), kRecordSize);
}

TEST(SniiBkdPointSorter, EqualValuesEndUpWithAscendingDocIds) {
    // Four distinct values, every doc id distinct, shuffled. The whole-record
    // memcmp has to put equal values next to each other AND order them by doc id
    // -- that ascending-doc-id-per-run property is what leaf_codec's delta coding
    // depends on, so it is asserted on the decoded points, not on the bytes.
    std::vector<Point> points;
    for (uint32_t doc_id = 0; doc_id < 400; ++doc_id) {
        points.push_back({static_cast<int64_t>(doc_id % 4) - 2, doc_id});
    }
    std::mt19937 rng(7);
    std::shuffle(points.begin(), points.end(), rng);

    std::vector<uint8_t> records = pack(points);
    point_sorter::sort(records.data(), points.size(), kRecordSize);

    const std::vector<Point> sorted = unpack(records);
    ASSERT_EQ(sorted.size(), points.size());
    for (size_t i = 1; i < sorted.size(); ++i) {
        EXPECT_LE(sorted[i - 1].value, sorted[i].value) << "at point " << i;
        if (sorted[i - 1].value == sorted[i].value) {
            EXPECT_LT(sorted[i - 1].doc_id, sorted[i].doc_id) << "at point " << i;
        }
    }
}

TEST(SniiBkdPointSorter, NegativeValuesSortBelowPositiveOnes) {
    // The sign-bit flip in the sortable encoding only pays off if the comparison
    // is unsigned: a signed byte compare would rank 0x00.. (the encoding of
    // INT64_MIN) above 0xff.. and silently invert the whole ordering.
    const std::vector<Point> points = {{5, 1},         {-5, 2}, {0, 3}, {INT64_MIN, 4},
                                       {INT64_MAX, 5}, {-1, 6}, {1, 7}};
    std::vector<uint8_t> records = pack(points);
    point_sorter::sort(records.data(), points.size(), kRecordSize);

    const std::vector<Point> sorted = unpack(records);
    const std::vector<int64_t> expected = {INT64_MIN, -5, -1, 0, 1, 5, INT64_MAX};
    std::vector<int64_t> actual;
    for (const Point& point : sorted) {
        actual.push_back(point.value);
    }
    EXPECT_EQ(actual, expected);
}

TEST(SniiBkdPointSorter, DocIdTailIsComparedAsUnsignedBigEndian) {
    // One value, doc ids spanning the sign boundary of a 32-bit integer. Real doc
    // ids never reach 2^31, but the tail is compared as raw bytes and this pins
    // that it is treated as unsigned big-endian rather than as a native int.
    std::vector<Point> points = {
            {9, 0x80000000U}, {9, 1}, {9, 0xFFFFFFFFU}, {9, 0}, {9, 0x7FFFFFFFU}};
    std::vector<uint8_t> records = pack(points);
    point_sorter::sort(records.data(), points.size(), kRecordSize);

    std::vector<uint32_t> actual;
    for (const Point& point : unpack(records)) {
        actual.push_back(point.doc_id);
    }
    const std::vector<uint32_t> expected = {0, 1, 0x7FFFFFFFU, 0x80000000U, 0xFFFFFFFFU};
    EXPECT_EQ(actual, expected);
}

TEST(SniiBkdPointSorter, ComparisonIsUnsignedByteWise) {
    // Same property one level down, on bare records: a leading byte with the high
    // bit set must sort last.
    const std::vector<uint8_t> input = {0xFF, 0x00, 0x00, 0x80, 0x11, 0x22,
                                        0x00, 0xAA, 0xBB, 0x7F, 0xFF, 0xFF};
    ExpectSorts(input, 3);
}

// ---------------------------------------------------------------------------
// Input shapes
// ---------------------------------------------------------------------------

TEST(SniiBkdPointSorter, AlreadySortedInputIsUnchanged) {
    std::vector<Point> points;
    for (uint32_t doc_id = 0; doc_id < 2000; ++doc_id) {
        points.push_back({static_cast<int64_t>(doc_id) - 1000, doc_id});
    }
    const std::vector<uint8_t> input = pack(points);
    std::vector<uint8_t> records = input;
    point_sorter::sort(records.data(), points.size(), kRecordSize);
    EXPECT_EQ(records, input);
}

TEST(SniiBkdPointSorter, ReverseSortedInputIsReversed) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 2000; ++i) {
        points.push_back({1000 - static_cast<int64_t>(i), 2000 - i});
    }
    ExpectSorts(pack(points), kRecordSize);
}

TEST(SniiBkdPointSorter, IdenticalRecordsAreUnchanged) {
    // An array column may repeat one value inside one row, so exact duplicates are
    // legal input; they must survive with their multiplicity intact.
    std::vector<Point> points(500, Point {42, 17});
    const std::vector<uint8_t> input = pack(points);
    std::vector<uint8_t> records = input;
    point_sorter::sort(records.data(), points.size(), kRecordSize);
    EXPECT_EQ(records, input);
}

TEST(SniiBkdPointSorter, DuplicateRecordsKeepTheirMultiplicity) {
    std::mt19937 rng(99);
    std::vector<Point> points;
    for (uint32_t doc_id = 0; doc_id < 600; ++doc_id) {
        // Three points per doc, two of which are the same value: the exactly
        // duplicated record is the array-column case.
        points.push_back({static_cast<int64_t>(doc_id % 5), doc_id});
        points.push_back({static_cast<int64_t>(doc_id % 5), doc_id});
        points.push_back({static_cast<int64_t>(doc_id % 7), doc_id});
    }
    std::shuffle(points.begin(), points.end(), rng);
    ExpectSorts(pack(points), kRecordSize);
}

TEST(SniiBkdPointSorter, EveryCountAcrossTheFallbackBoundarySorts) {
    // The comparison fallback takes over below some bucket size; sweeping every
    // count from 0 to 200 crosses that boundary without the test having to know
    // where it is, and a descending input makes an untouched array impossible to
    // mistake for a sorted one.
    for (size_t count = 0; count <= 200; ++count) {
        ExpectSorts(descending_records(count, kRecordSize), kRecordSize);
    }
}

TEST(SniiBkdPointSorter, LongCommonPrefixSorts) {
    // Every record shares all eight value bytes, so the first distinguishing byte
    // is the fifth from the end: the radix pass has to walk down through eight
    // single-bucket levels before it can split anything.
    std::mt19937 rng(1234);
    std::uniform_int_distribution<uint32_t> doc_id(0, 1000000);
    std::vector<Point> points;
    for (int i = 0; i < 5000; ++i) {
        points.push_back({-1, doc_id(rng)});
    }
    ExpectSorts(pack(points), kRecordSize);
}

TEST(SniiBkdPointSorter, TwoSymbolAlphabetSorts) {
    // Two byte values only, so every level splits into two large buckets and the
    // recursion stays wide all the way down to the last byte -- the shape that
    // exercises the in-place bucket permutation hardest.
    std::mt19937 rng(555);
    ExpectSorts(random_records(20000, kRecordSize, 2, &rng), kRecordSize);
}

// ---------------------------------------------------------------------------
// Record widths
// ---------------------------------------------------------------------------

TEST(SniiBkdPointSorter, NarrowRecordsSort) {
    // TINYINT: one value byte plus the four-byte doc id tail.
    std::mt19937 rng(11);
    ExpectSorts(random_records(4000, 1 + kPointDocIdBytes, 256, &rng), 1 + kPointDocIdBytes);
}

TEST(SniiBkdPointSorter, WideRecordsSort) {
    // LARGEINT: sixteen value bytes plus the tail, the widest record the builder
    // can produce.
    std::mt19937 rng(12);
    ExpectSorts(random_records(4000, 16 + kPointDocIdBytes, 256, &rng), 16 + kPointDocIdBytes);
}

// ---------------------------------------------------------------------------
// Scale and idempotence
// ---------------------------------------------------------------------------

TEST(SniiBkdPointSorter, LargeInputSorts) {
    std::mt19937 rng(20260802);
    std::uniform_int_distribution<int64_t> value(-1000000, 1000000);
    std::vector<Point> points;
    points.reserve(150000);
    for (uint32_t doc_id = 0; doc_id < 150000; ++doc_id) {
        points.push_back({value(rng), doc_id});
    }
    ExpectSorts(pack(points), kRecordSize);
}

TEST(SniiBkdPointSorter, SortingTwiceChangesNothing) {
    std::mt19937 rng(31337);
    const std::vector<uint8_t> input = random_records(3000, kRecordSize, 256, &rng);
    std::vector<uint8_t> once = input;
    point_sorter::sort(once.data(), 3000, kRecordSize);
    std::vector<uint8_t> twice = once;
    point_sorter::sort(twice.data(), 3000, kRecordSize);
    EXPECT_EQ(twice, once);
}

} // namespace
} // namespace doris::snii::bkd
