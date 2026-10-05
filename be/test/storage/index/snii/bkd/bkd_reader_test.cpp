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

#include "storage/index/snii/bkd/bkd_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/bkd/bkd_builder.h"
#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/olap_common.h"

namespace doris::snii::bkd {
namespace {

constexpr uint32_t kBytesPerDim = sizeof(int64_t);
constexpr FieldType kFieldType = FieldType::OLAP_FIELD_TYPE_BIGINT;

// Unsigned big-endian sortable bytes for a BIGINT -- what
// KeyCoder::full_encode_ascending emits (sign bit flipped, then byte-swapped).
// Every comparison in the index is a memcmp from offset 0 (INV-1), so query
// bounds have to travel through exactly the same encoder the build used.
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

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// Collects the bkd_data bytes the builder appends.
class MemoryFileWriter final : public io::FileWriter {
public:
    Status append(Slice data) override {
        bytes_.insert(bytes_.end(), data.data(), data.data() + data.size());
        return Status::OK();
    }
    Status finalize() override { return Status::OK(); }
    uint64_t bytes_written() const override { return bytes_.size(); }

    const std::vector<uint8_t>& bytes() const { return bytes_; }

private:
    std::vector<uint8_t> bytes_;
};

// A whole container image, with a call counter. The counter is what makes
// design 7.2's "zero IO" claim for the global-bounds fast reject and its
// "O(touched leaves) positioned reads" cost model observable rather than
// asserted in prose.
class CountingFileReader final : public io::FileReader {
public:
    explicit CountingFileReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        out->resize(len);
        return read_into(offset, out->data(), len);
    }

    Status read_into(uint64_t offset, uint8_t* out, size_t out_len) override {
        reads_.fetch_add(1);
        if (out_len == 0) {
            return Status::OK();
        }
        if (offset > bytes_.size() || out_len > bytes_.size() - offset) {
            return Status::Error<ErrorCode::CORRUPTION, false>("read past EOF");
        }
        std::memcpy(out, bytes_.data() + offset, out_len);
        return Status::OK();
    }

    uint64_t size() const override { return bytes_.size(); }

    uint64_t reads() const { return reads_.load(); }

private:
    std::vector<uint8_t> bytes_;
    mutable std::atomic<uint64_t> reads_ {0};
};

// ---------------------------------------------------------------------------
// Fixture: build an index, then lay the two sub-files out inside a container
// image at non-zero offsets so nothing can accidentally depend on either
// sub-file starting at 0.
// ---------------------------------------------------------------------------

constexpr size_t kLeadingPad = 13;
constexpr size_t kMiddlePad = 5;
constexpr size_t kTrailingPad = 3;

struct Container {
    std::vector<uint8_t> image;
    BkdSections sections;
    // Where the two sub-files sit inside `image`, for the corruption tests.
    size_t data_begin = 0;
    size_t index_begin = 0;
    size_t index_size = 0;
};

Status build_container(const std::vector<Point>& points, uint32_t points_per_leaf, Container* out) {
    BkdBuilderOptions options;
    options.bytes_per_dim = kBytesPerDim;
    options.field_type = kFieldType;
    options.points_per_leaf = points_per_leaf;

    std::unique_ptr<BkdBuilder> builder;
    RETURN_IF_ERROR(BkdBuilder::create(options, &builder));
    for (const Point& point : points) {
        RETURN_IF_ERROR(builder->add(point.doc_id, Slice(sortable_bigint(point.value))));
    }
    MemoryFileWriter data;
    ByteSink index;
    BkdStats stats;
    RETURN_IF_ERROR(builder->finish(&data, &index, &stats));

    const std::vector<uint8_t> index_bytes = index.take();
    out->image.assign(kLeadingPad, 0xA5);
    out->data_begin = out->image.size();
    out->image.insert(out->image.end(), data.bytes().begin(), data.bytes().end());
    out->image.insert(out->image.end(), kMiddlePad, 0x5A);
    out->index_begin = out->image.size();
    out->image.insert(out->image.end(), index_bytes.begin(), index_bytes.end());
    out->image.insert(out->image.end(), kTrailingPad, 0xC3);
    out->index_size = index_bytes.size();

    out->sections.data_offset = out->data_begin;
    out->sections.data_length = data.bytes().size();
    out->sections.index_offset = out->index_begin;
    out->sections.index_length = index_bytes.size();
    return Status::OK();
}

// One opened index plus the reader it was opened over, kept together because the
// FileReader must outlive the BkdReader (design 9: no reference counting, the
// SNII segment reader owns the file).
struct OpenedIndex {
    std::unique_ptr<CountingFileReader> file;
    std::unique_ptr<BkdReader> reader;
    Container container;
};

Status open_index(const std::vector<Point>& points, uint32_t points_per_leaf, OpenedIndex* out) {
    RETURN_IF_ERROR(build_container(points, points_per_leaf, &out->container));
    out->file = std::make_unique<CountingFileReader>(out->container.image);
    return BkdReader::open(out->file.get(), out->container.sections, &out->reader);
}

// ---------------------------------------------------------------------------
// The oracle: a brute-force scan of the same point list (design 12.2). It is
// independent of every line of the index, so it catches errors the index and a
// differential baseline could share.
// ---------------------------------------------------------------------------

struct Interval {
    bool has_lower = false;
    int64_t lower = 0;
    bool lower_inclusive = true;
    bool has_upper = false;
    int64_t upper = 0;
    bool upper_inclusive = true;
};

std::string describe(const Interval& interval) {
    std::string text = interval.has_lower ? (interval.lower_inclusive ? "[" : "(") +
                                                    std::to_string(interval.lower)
                                          : std::string("(-inf");
    text += ", ";
    text += interval.has_upper
                    ? std::to_string(interval.upper) + (interval.upper_inclusive ? "]" : ")")
                    : std::string("+inf)");
    return text;
}

std::vector<uint32_t> brute_force(const std::vector<Point>& points, const Interval& interval) {
    std::vector<uint32_t> hits;
    for (const Point& point : points) {
        if (interval.has_lower) {
            if (interval.lower_inclusive ? point.value < interval.lower
                                         : point.value <= interval.lower) {
                continue;
            }
        }
        if (interval.has_upper) {
            if (interval.upper_inclusive ? point.value > interval.upper
                                         : point.value >= interval.upper) {
                continue;
            }
        }
        hits.push_back(point.doc_id);
    }
    std::ranges::sort(hits);
    hits.erase(std::ranges::unique(hits).begin(), hits.end());
    return hits;
}

std::vector<uint32_t> to_sorted_vector(const roaring::Roaring& bitmap) {
    std::vector<uint32_t> out(bitmap.cardinality());
    bitmap.toUint32Array(out.data());
    return out;
}

::testing::AssertionResult matches_brute_force(const BkdReader& reader,
                                               const std::vector<Point>& points,
                                               const Interval& interval) {
    const std::vector<uint8_t> lower = sortable_bigint(interval.lower);
    const std::vector<uint8_t> upper = sortable_bigint(interval.upper);
    roaring::Roaring hits;
    const Status status = reader.range(
            interval.has_lower ? Slice(lower) : Slice(), interval.lower_inclusive,
            interval.has_upper ? Slice(upper) : Slice(), interval.upper_inclusive, &hits);
    if (!status.ok()) {
        return ::testing::AssertionFailure() << describe(interval) << " failed: " << status;
    }
    const std::vector<uint32_t> got = to_sorted_vector(hits);
    const std::vector<uint32_t> expected = brute_force(points, interval);
    if (got != expected) {
        return ::testing::AssertionFailure() << describe(interval) << " returned " << got.size()
                                             << " docs, brute force says " << expected.size();
    }
    return ::testing::AssertionSuccess();
}

Interval closed(int64_t lower, int64_t upper) {
    return Interval {true, lower, true, true, upper, true};
}

// A deterministic 64-bit LCG so a failure is reproducible from the seed alone.
class Rng {
public:
    explicit Rng(uint64_t seed) : state_(seed) {}
    uint64_t next() {
        state_ = state_ * 6364136223846793005ULL + 1442695040888963407ULL;
        return state_ >> 11;
    }
    int64_t next_in(int64_t low, int64_t high) {
        return low + static_cast<int64_t>(next() % static_cast<uint64_t>(high - low + 1));
    }

private:
    uint64_t state_;
};

// ---------------------------------------------------------------------------
// open()
// ---------------------------------------------------------------------------

TEST(BkdReaderTest, OpenReportsTheHeaderAndTheGlobalBounds) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 37; ++i) {
        points.push_back(Point {static_cast<int64_t>(i) - 10, i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 8, &opened).ok());

    const BkdReader& reader = *opened.reader;
    EXPECT_EQ(reader.header().format_version, kFormatVersion);
    EXPECT_EQ(reader.header().bytes_per_dim, kBytesPerDim);
    EXPECT_EQ(reader.header().field_type, kFieldType);
    EXPECT_EQ(reader.point_count(), 37U);
    EXPECT_EQ(reader.doc_count(), 37U);
    EXPECT_EQ(reader.leaf_count(), 5U);
    // An empty index has no bounds to ask for at all, so stop here rather than trip
    // the reader's DORIS_CHECK below.
    ASSERT_FALSE(reader.empty());

    const std::vector<uint8_t> min = sortable_bigint(-10);
    const std::vector<uint8_t> max = sortable_bigint(26);
    ASSERT_EQ(reader.min_value().size(), kBytesPerDim);
    EXPECT_EQ(std::memcmp(reader.min_value().data(), min.data(), kBytesPerDim), 0);
    EXPECT_EQ(std::memcmp(reader.max_value().data(), max.data(), kBytesPerDim), 0);
    // The resident cost is the decoded directory, not an estimate, and it is real:
    // five leaves plus four split values cannot be free.
    EXPECT_GT(reader.memory_usage(), sizeof(BkdReader));

    // bkd_index is read in full exactly once at open and never again (design 5.1).
    EXPECT_EQ(opened.file->reads(), 1U);
}

TEST(BkdReaderTest, OpenRejectsSectionsThatRunPastTheEndOfTheFile) {
    Container container;
    ASSERT_TRUE(build_container({{1, 0}, {2, 1}}, 4, &container).ok());
    CountingFileReader file(container.image);

    // The section table is on-disk metadata, so an impossible extent is damage to
    // report (design 8) -- not something to assert on, and not something to hand
    // to read_into as a multi-gigabyte allocation.
    BkdSections beyond = container.sections;
    beyond.index_length = container.image.size() + 1;
    std::unique_ptr<BkdReader> reader;
    const Status status = BkdReader::open(&file, beyond, &reader);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_EQ(reader, nullptr);

    BkdSections data_beyond = container.sections;
    data_beyond.data_offset = container.image.size() - 1;
    data_beyond.data_length = 4;
    const Status data_status = BkdReader::open(&file, data_beyond, &reader);
    EXPECT_TRUE(data_status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << data_status;
}

TEST(BkdReaderTest, OpenRejectsATruncatedIndexSection) {
    Container container;
    ASSERT_TRUE(build_container({{1, 0}, {2, 1}, {3, 2}}, 2, &container).ok());
    CountingFileReader file(container.image);

    BkdSections truncated = container.sections;
    truncated.index_length -= 1;
    std::unique_ptr<BkdReader> reader;
    const Status status = BkdReader::open(&file, truncated, &reader);
    // A damaged hot section downgrades the query; it never crashes the node.
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(reader, nullptr);
}

// ---------------------------------------------------------------------------
// Degenerate indexes
// ---------------------------------------------------------------------------

TEST(BkdReaderTest, EmptyIndexAnswersEveryRangeWithAnEmptyBitmapAndNoIo) {
    OpenedIndex opened;
    ASSERT_TRUE(open_index({}, 4, &opened).ok());
    ASSERT_TRUE(opened.reader->empty());
    EXPECT_EQ(opened.reader->point_count(), 0U);
    EXPECT_EQ(opened.reader->leaf_count(), 0U);

    const uint64_t reads_after_open = opened.file->reads();
    const std::vector<uint8_t> lower = sortable_bigint(-100);
    const std::vector<uint8_t> upper = sortable_bigint(100);
    roaring::Roaring hits;
    // Design 10.4: "empty" must NOT be translated into an error -- the adapter is
    // supposed to see an empty result set, exactly as a non-empty index that
    // happens to match nothing would produce.
    const Status status = opened.reader->range(Slice(lower), true, Slice(upper), true, &hits);
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_TRUE(hits.isEmpty());
    EXPECT_EQ(opened.file->reads(), reads_after_open);

    // Both sides unbounded is still empty, still free.
    roaring::Roaring all;
    EXPECT_TRUE(opened.reader->range(Slice(), true, Slice(), true, &all).ok());
    EXPECT_TRUE(all.isEmpty());
    EXPECT_EQ(opened.file->reads(), reads_after_open);
}

TEST(BkdReaderTest, SinglePointIndex) {
    const std::vector<Point> points {{42, 7}};
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 4, &opened).ok());
    ASSERT_EQ(opened.reader->leaf_count(), 1U);

    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(42, 42)));
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(41, 43)));
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(43, 50)));
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(0, 41)));
    EXPECT_TRUE(matches_brute_force(*opened.reader, points,
                                    Interval {true, 42, false, true, 42, true}));
    EXPECT_TRUE(matches_brute_force(*opened.reader, points,
                                    Interval {true, 42, true, true, 42, false}));
}

TEST(BkdReaderTest, AllEqualValuesSpanningManyLeaves) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 20; ++i) {
        points.push_back(Point {5, i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 4, &opened).ok());
    ASSERT_EQ(opened.reader->leaf_count(), 5U);

    // The split array is non-decreasing rather than strictly increasing here --
    // every split is the one repeated value -- so the two binary searches must not
    // assume distinct splits.
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(5, 5)));
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(0, 10)));
    EXPECT_TRUE(
            matches_brute_force(*opened.reader, points, Interval {true, 5, false, false, 0, true}));
    EXPECT_TRUE(
            matches_brute_force(*opened.reader, points, Interval {false, 0, true, true, 5, false}));
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(6, 9)));
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(-3, 4)));
}

// ---------------------------------------------------------------------------
// Interval shapes
// ---------------------------------------------------------------------------

TEST(BkdReaderTest, AllFourInclusivityCombinationsOnLeafBoundaries) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 40; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 4, &opened).ok());
    ASSERT_EQ(opened.reader->leaf_count(), 10U);

    for (const bool lower_inclusive : {false, true}) {
        for (const bool upper_inclusive : {false, true}) {
            // 12 and 24 are both exactly split values (leaf 3 and leaf 6 start
            // there), so the open/closed distinction decides whether a whole leaf
            // joins the answer.
            EXPECT_TRUE(matches_brute_force(
                    *opened.reader, points,
                    Interval {true, 12, lower_inclusive, true, 24, upper_inclusive}));
            // Inside one leaf.
            EXPECT_TRUE(matches_brute_force(
                    *opened.reader, points,
                    Interval {true, 13, lower_inclusive, true, 14, upper_inclusive}));
            // Adjacent leaves.
            EXPECT_TRUE(matches_brute_force(
                    *opened.reader, points,
                    Interval {true, 15, lower_inclusive, true, 16, upper_inclusive}));
            // Degenerate: one value.
            EXPECT_TRUE(matches_brute_force(
                    *opened.reader, points,
                    Interval {true, 20, lower_inclusive, true, 20, upper_inclusive}));
        }
    }
}

TEST(BkdReaderTest, SingleSidedIntervals) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 40; ++i) {
        points.push_back(Point {static_cast<int64_t>(i) * 3 - 50, i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 6, &opened).ok());

    for (int64_t bound = -60; bound <= 80; bound += 3) {
        EXPECT_TRUE(matches_brute_force(*opened.reader, points,
                                        Interval {true, bound, true, false, 0, true}))
                << bound;
        EXPECT_TRUE(matches_brute_force(*opened.reader, points,
                                        Interval {true, bound, false, false, 0, true}))
                << bound;
        EXPECT_TRUE(matches_brute_force(*opened.reader, points,
                                        Interval {false, 0, true, true, bound, true}))
                << bound;
        EXPECT_TRUE(matches_brute_force(*opened.reader, points,
                                        Interval {false, 0, true, true, bound, false}))
                << bound;
    }
    // Both sides unbounded is every doc.
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, Interval {}));
}

TEST(BkdReaderTest, LowerAboveUpperIsAnEmptyIntervalNotAnError) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 40; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 4, &opened).ok());

    // Both bounds sit INSIDE [min_value, max_value], so the global fast reject
    // cannot fire; the leaf search itself has to conclude the interval is empty.
    const uint64_t reads_after_open = opened.file->reads();
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(30, 10)));
    EXPECT_EQ(opened.file->reads(), reads_after_open);
}

// ---------------------------------------------------------------------------
// IO cost (design 7.2)
// ---------------------------------------------------------------------------

TEST(BkdReaderTest, IntervalsEntirelyOutsideTheGlobalBoundsCostNoIo) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 40; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 4, &opened).ok());
    const uint64_t reads_after_open = opened.file->reads();

    // Below the global minimum, above the global maximum, and the two exclusive
    // touches of the bounds themselves.
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(-100, -1)));
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(40, 100)));
    EXPECT_TRUE(
            matches_brute_force(*opened.reader, points, Interval {false, 0, true, true, 0, false}));
    EXPECT_TRUE(matches_brute_force(*opened.reader, points,
                                    Interval {true, 39, false, false, 0, true}));
    // Design 7.2: the global-bounds check is answered from the resident header,
    // so a miss touches bkd_data zero times.
    EXPECT_EQ(opened.file->reads(), reads_after_open);
}

TEST(BkdReaderTest, ReadsExactlyOnePositionedReadPerTouchedLeaf) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 40; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 4, &opened).ok());
    ASSERT_EQ(opened.reader->leaf_count(), 10U);

    // Leaf j holds values [4j, 4j+3], so [10, 25] spans leaves 2..6 -- two
    // boundary leaves plus three whole-leaf hits.
    const uint64_t before = opened.file->reads();
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(10, 25)));
    EXPECT_EQ(opened.file->reads() - before, 5U);

    // One leaf, one read.
    const uint64_t before_single = opened.file->reads();
    EXPECT_TRUE(matches_brute_force(*opened.reader, points, closed(13, 14)));
    EXPECT_EQ(opened.file->reads() - before_single, 1U);
}

// ---------------------------------------------------------------------------
// Property test against brute force (design 12.2)
// ---------------------------------------------------------------------------

TEST(BkdReaderTest, RandomPointSetsAndRandomIntervalsMatchBruteForce) {
    // Value ranges chosen so consecutive shapes differ: dense (many duplicates and
    // many equal-value runs crossing leaves), sparse (nearly all distinct), and
    // one wide enough that the leaves share no common prefix.
    const std::vector<int64_t> spans {6, 60, 100000, 4000000000LL};
    for (size_t shape = 0; shape < spans.size(); ++shape) {
        Rng rng(0x243F6A8885A308D3ULL + shape);
        const int64_t span = spans[shape];
        const uint32_t point_count = 200 + static_cast<uint32_t>(rng.next() % 400);

        std::vector<Point> points;
        for (uint32_t doc = 0; doc < point_count; ++doc) {
            points.push_back(Point {rng.next_in(-span, span), doc});
            // Every few docs contribute a second point, the array-column shape.
            if (rng.next() % 5 == 0) {
                points.push_back(Point {rng.next_in(-span, span), doc});
            }
        }

        OpenedIndex opened;
        ASSERT_TRUE(open_index(points, 7, &opened).ok()) << "shape " << shape;

        for (uint32_t trial = 0; trial < 200; ++trial) {
            Interval interval;
            interval.has_lower = (rng.next() % 8) != 0;
            interval.has_upper = (rng.next() % 8) != 0;
            interval.lower = rng.next_in(-span - 5, span + 5);
            interval.upper = rng.next_in(-span - 5, span + 5);
            interval.lower_inclusive = (rng.next() % 2) == 0;
            interval.upper_inclusive = (rng.next() % 2) == 0;
            EXPECT_TRUE(matches_brute_force(*opened.reader, points, interval))
                    << "shape " << shape << " trial " << trial;
        }
        // Every value that is actually present, as a point lookup, so exact hits on
        // split values and on run boundaries are all covered.
        for (const Point& point : points) {
            ASSERT_TRUE(
                    matches_brute_force(*opened.reader, points, closed(point.value, point.value)))
                    << "shape " << shape << " value " << point.value;
        }
    }
}

// ---------------------------------------------------------------------------
// Robustness, reuse and sharing
// ---------------------------------------------------------------------------

TEST(BkdReaderTest, DamagedLeafBytesDowngradeToAStatusInsteadOfCrashing) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 20; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    Container container;
    ASSERT_TRUE(build_container(points, 4, &container).ok());
    // Leaves are read lazily, so they are outside the open-time validation: the
    // damage can only be caught by the leaf decoder, and it must come back as a
    // Status (design 8.3).
    for (size_t i = 0; i < 8; ++i) {
        container.image[container.data_begin + i] = 0xFF;
    }

    CountingFileReader file(container.image);
    std::unique_ptr<BkdReader> reader;
    ASSERT_TRUE(BkdReader::open(&file, container.sections, &reader).ok());

    const std::vector<uint8_t> lower = sortable_bigint(0);
    const std::vector<uint8_t> upper = sortable_bigint(19);
    roaring::Roaring hits;
    const Status status = reader->range(Slice(lower), true, Slice(upper), true, &hits);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(BkdReaderTest, HitsAreReplacedNotAccumulated) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 20; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 4, &opened).ok());

    roaring::Roaring hits;
    hits.add(9999);
    hits.add(3);
    const std::vector<uint8_t> lower = sortable_bigint(10);
    const std::vector<uint8_t> upper = sortable_bigint(12);
    ASSERT_TRUE(opened.reader->range(Slice(lower), true, Slice(upper), true, &hits).ok());
    EXPECT_EQ(to_sorted_vector(hits), (std::vector<uint32_t> {10, 11, 12}));
}

TEST(BkdReaderTest, ScratchIsReusedAcrossQueriesWithoutChangingResults) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 120; ++i) {
        points.push_back(Point {static_cast<int64_t>(i % 37), i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 5, &opened).ok());

    // Design 9: the per-query state is the caller's, so one scratch serves an
    // arbitrary sequence of queries and the answers are unaffected by what ran
    // before.
    BkdQueryScratch scratch;
    for (int64_t low = 0; low < 37; ++low) {
        const Interval interval = closed(low, low + 4);
        const std::vector<uint8_t> lower = sortable_bigint(interval.lower);
        const std::vector<uint8_t> upper = sortable_bigint(interval.upper);
        roaring::Roaring hits;
        ASSERT_TRUE(
                opened.reader->range(Slice(lower), true, Slice(upper), true, &hits, &scratch).ok());
        EXPECT_EQ(to_sorted_vector(hits), brute_force(points, interval)) << describe(interval);
    }
}

TEST(BkdReaderTest, ConcurrentQueriesShareOneImmutableReader) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 500; ++i) {
        points.push_back(Point {static_cast<int64_t>((i * 7919) % 1000), i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 9, &opened).ok());

    // Design 9: range() is const, every query keeps its state on its own stack, and
    // there is no clone() -- so a searcher cache may hand the same object to
    // concurrent queries with no locking at all.
    std::vector<std::thread> threads;
    std::atomic<int> failures {0};
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t]() {
            BkdQueryScratch scratch;
            for (int64_t low = 0; low < 1000; low += 37) {
                const Interval interval = closed(low + t, low + t + 120);
                const std::vector<uint8_t> lower = sortable_bigint(interval.lower);
                const std::vector<uint8_t> upper = sortable_bigint(interval.upper);
                roaring::Roaring hits;
                if (!opened.reader->range(Slice(lower), true, Slice(upper), true, &hits, &scratch)
                             .ok() ||
                    to_sorted_vector(hits) != brute_force(points, interval)) {
                    failures.fetch_add(1);
                }
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(failures.load(), 0);
}

// ---------------------------------------------------------------------------
// lookup_many (design 7.3)
// ---------------------------------------------------------------------------

namespace {

// The points every multi-value test runs over: a span far below the doc count,
// so values repeat heavily and a single value genuinely straddles leaf
// boundaries -- which is the case a per-value window has to get right.
std::vector<Point> lookup_points() {
    std::vector<Point> points;
    uint64_t state = 0x452821E638D01377ULL;
    for (uint32_t i = 0; i < 600; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        points.push_back(Point {static_cast<int64_t>(state % 40), i});
    }
    return points;
}

std::vector<std::vector<uint8_t>> encode_all(const std::vector<int64_t>& values) {
    std::vector<std::vector<uint8_t>> encoded;
    encoded.reserve(values.size());
    for (const int64_t value : values) {
        encoded.push_back(sortable_bigint(value));
    }
    return encoded;
}

std::vector<Slice> slices_of(const std::vector<std::vector<uint8_t>>& encoded) {
    std::vector<Slice> slices;
    slices.reserve(encoded.size());
    for (const std::vector<uint8_t>& bytes : encoded) {
        slices.push_back(Slice(bytes));
    }
    return slices;
}

} // namespace

// lookup_many exists to answer IN (...) in ONE pass instead of the N full
// traversals the old implementation ran (design 7.3). It must agree exactly with
// the union of the equality ranges it replaces -- that equivalence is the whole
// contract, and the union is computed by the already-tested range().
TEST(BkdReaderTest, LookupManyEqualsTheUnionOfEqualityRanges) {
    for (const uint32_t points_per_leaf : {1U, 4U, 32U, 1024U}) {
        SCOPED_TRACE("points_per_leaf " + std::to_string(points_per_leaf));
        OpenedIndex opened;
        ASSERT_TRUE(open_index(lookup_points(), points_per_leaf, &opened).ok());

        // Present values, absent values, and the two ends of the value space.
        const std::vector<int64_t> wanted = {-5, 0, 3, 7, 11, 12, 13, 25, 39, 40, 100};
        const auto encoded = encode_all(wanted);
        const std::vector<Slice> values = slices_of(encoded);

        roaring::Roaring expected;
        for (const Slice& value : values) {
            roaring::Roaring one;
            ASSERT_TRUE(opened.reader->range(value, true, value, true, &one).ok());
            expected |= one;
        }

        roaring::Roaring actual;
        ASSERT_TRUE(opened.reader->lookup_many(values, &actual).ok());
        EXPECT_TRUE(actual == expected);
    }
}

// The point of the one-pass shape: a leaf is READ once no matter how many of the
// query values land in it. The old implementation re-walked the whole tree per
// value, so this bound is the improvement, not an implementation detail.
TEST(BkdReaderTest, LookupManyReadsEachLeafAtMostOnce) {
    OpenedIndex opened;
    ASSERT_TRUE(open_index(lookup_points(), /*points_per_leaf=*/16, &opened).ok());
    const uint32_t leaf_count = opened.reader->leaf_count();
    ASSERT_GT(leaf_count, 1U);

    // Forty values over a value space of forty: every leaf is wanted, and every
    // leaf holds several of the values.
    std::vector<int64_t> wanted;
    for (int64_t v = 0; v < 40; ++v) {
        wanted.push_back(v);
    }
    const auto encoded = encode_all(wanted);
    const std::vector<Slice> values = slices_of(encoded);

    const uint64_t before = opened.file->reads();
    roaring::Roaring hits;
    ASSERT_TRUE(opened.reader->lookup_many(values, &hits).ok());
    const uint64_t reads = opened.file->reads() - before;

    // The read bound alone does not discriminate: an implementation that read
    // nothing and returned nothing would satisfy every upper bound below. The
    // query asks for the entire value space, so it must return EVERY doc.
    roaring::Roaring expected;
    for (const Point& point : lookup_points()) {
        expected.add(point.doc_id);
    }
    EXPECT_TRUE(hits == expected) << "the whole value space must return every doc";

    // Every leaf holds several wanted values, so each is read exactly once --
    // not merely "at most once", which zero reads would also satisfy.
    EXPECT_EQ(reads, leaf_count);
    // And strictly better than the per-value traversal it replaces.
    EXPECT_LT(reads, wanted.size());
}

// Values that are not in the index cost nothing beyond the binary search, and an
// empty value list is a legal query rather than a caller error.
TEST(BkdReaderTest, LookupManyOnAbsentValuesTouchesNothing) {
    OpenedIndex opened;
    ASSERT_TRUE(open_index(lookup_points(), /*points_per_leaf=*/16, &opened).ok());

    const auto encoded = encode_all({-1000, -999, 500, 501});
    const uint64_t before = opened.file->reads();
    roaring::Roaring hits;
    ASSERT_TRUE(opened.reader->lookup_many(slices_of(encoded), &hits).ok());
    EXPECT_TRUE(hits.isEmpty());
    EXPECT_EQ(opened.file->reads(), before) << "values outside the global bounds read a leaf";

    roaring::Roaring none;
    ASSERT_TRUE(opened.reader->lookup_many({}, &none).ok());
    EXPECT_TRUE(none.isEmpty());
    EXPECT_EQ(opened.file->reads(), before);
}

TEST(BkdReaderTest, LookupManyOnAnEmptyIndexIsEmpty) {
    OpenedIndex opened;
    ASSERT_TRUE(open_index({}, /*points_per_leaf=*/16, &opened).ok());
    const uint64_t before = opened.file->reads();

    const auto encoded = encode_all({0, 1, 2});
    roaring::Roaring hits;
    ASSERT_TRUE(opened.reader->lookup_many(slices_of(encoded), &hits).ok());
    EXPECT_TRUE(hits.isEmpty());
    EXPECT_EQ(opened.file->reads(), before);
}

// ---------------------------------------------------------------------------
// estimate_cardinality (design 7.4)
// ---------------------------------------------------------------------------

// The estimate feeds inverted_index_skip_threshold's bypass decision, so being
// cheap matters as much as being close: it must read NOTHING.
TEST(BkdReaderTest, EstimateReadsNoData) {
    OpenedIndex opened;
    ASSERT_TRUE(open_index(lookup_points(), /*points_per_leaf=*/8, &opened).ok());
    const uint64_t before = opened.file->reads();

    const std::vector<uint8_t> lower = sortable_bigint(5);
    const std::vector<uint8_t> upper = sortable_bigint(30);
    uint64_t estimate = 0;
    ASSERT_TRUE(
            opened.reader->estimate_cardinality(Slice(lower), true, Slice(upper), true, &estimate)
                    .ok());
    EXPECT_EQ(opened.file->reads(), before);
    EXPECT_GT(estimate, 0U);
}

// Only the two boundary leaves are guessed at, and only when a bound actually
// cuts into them. The old implementation estimated whole subtrees as FULL
// leaves, which over-counted a sparse tail by multiples.
//
// The bound is 2 x ceil(points_per_leaf / 2), not points_per_leaf as design 7.4
// states: halving is integer division, so a partial leaf of c points can be off
// by ceil(c / 2) -- for c == 1 the guess is 0 while the truth is 1. With two
// partial leaves that is 2 for a one-point leaf, which exceeds points_per_leaf.
// The doc's figure is right only for even capacities.
TEST(BkdReaderTest, EstimateErrorIsBoundedByOneLeaf) {
    for (const uint32_t points_per_leaf : {1U, 8U, 64U}) {
        SCOPED_TRACE("points_per_leaf " + std::to_string(points_per_leaf));
        const std::vector<Point> points = lookup_points();
        OpenedIndex opened;
        ASSERT_TRUE(open_index(points, points_per_leaf, &opened).ok());

        for (const auto& [low, high] : std::vector<std::pair<int64_t, int64_t>> {
                     {0, 39}, {5, 30}, {10, 12}, {20, 20}, {-50, 50}, {38, 39}, {-5, 4}}) {
            SCOPED_TRACE("range [" + std::to_string(low) + ", " + std::to_string(high) + "]");
            const std::vector<uint8_t> lower = sortable_bigint(low);
            const std::vector<uint8_t> upper = sortable_bigint(high);

            uint64_t estimate = 0;
            ASSERT_TRUE(opened.reader
                                ->estimate_cardinality(Slice(lower), true, Slice(upper), true,
                                                       &estimate)
                                .ok());

            // Truth is the number of POINTS in the range, which is what the
            // estimate approximates -- not the number of docs, which a bitmap
            // would collapse.
            uint64_t truth = 0;
            for (const Point& point : points) {
                if (point.value >= low && point.value <= high) {
                    ++truth;
                }
            }

            const uint64_t error = estimate > truth ? estimate - truth : truth - estimate;
            EXPECT_LE(error, 2 * ((points_per_leaf + 1) / 2))
                    << "estimate " << estimate << " vs truth " << truth;
        }
    }
}

// A range that cannot match anything must estimate zero, or the bypass gate
// would decline to use an index that answers instantly.
TEST(BkdReaderTest, EstimateIsZeroWhenNothingCanMatch) {
    OpenedIndex opened;
    ASSERT_TRUE(open_index(lookup_points(), /*points_per_leaf=*/8, &opened).ok());

    const std::vector<uint8_t> far_low = sortable_bigint(-1000);
    const std::vector<uint8_t> far_high = sortable_bigint(-999);
    uint64_t estimate = 1;
    ASSERT_TRUE(
            opened.reader
                    ->estimate_cardinality(Slice(far_low), true, Slice(far_high), true, &estimate)
                    .ok());
    EXPECT_EQ(estimate, 0U);

    // An inverted interval is a legal query (a planner fusing `a > 30 AND a < 10`
    // produces one) and estimates zero too.
    const std::vector<uint8_t> high = sortable_bigint(30);
    const std::vector<uint8_t> low = sortable_bigint(10);
    estimate = 1;
    ASSERT_TRUE(
            opened.reader->estimate_cardinality(Slice(high), false, Slice(low), false, &estimate)
                    .ok());
    EXPECT_EQ(estimate, 0U);

    OpenedIndex empty;
    ASSERT_TRUE(open_index({}, /*points_per_leaf=*/8, &empty).ok());
    estimate = 1;
    ASSERT_TRUE(empty.reader->estimate_cardinality(Slice(), true, Slice(), true, &estimate).ok());
    EXPECT_EQ(estimate, 0U);
}

// An unbounded interval covers everything, and there the estimate is not an
// estimate at all: no leaf is a boundary leaf, so every count is exact.
TEST(BkdReaderTest, EstimateIsExactWhenNoLeafIsPartial) {
    const std::vector<Point> points = lookup_points();
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, /*points_per_leaf=*/8, &opened).ok());

    uint64_t estimate = 0;
    ASSERT_TRUE(opened.reader->estimate_cardinality(Slice(), true, Slice(), true, &estimate).ok());
    EXPECT_EQ(estimate, points.size());
}

// lookup_many must not depend on the order it is handed.
//
// This was a caller invariant enforced by a bare glog DCHECK, which compiles out
// under NDEBUG -- so a release build silently LOST rows for unsorted input,
// because the watermark merge skips every leaf below the high-water mark. The
// caller it exists to serve (InListPredicateBase over a hash-backed HybridSet)
// cannot supply ascending order at all.
TEST(BkdReaderTest, LookupManyDoesNotDependOnProbeOrder) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 8; ++i) {
        points.push_back(Point {static_cast<int64_t>((i + 1) * 10), i});
    }
    OpenedIndex opened;
    ASSERT_TRUE(open_index(points, 2, &opened).ok());
    ASSERT_EQ(opened.reader->leaf_count(), 4U);

    // Descending: the exact shape the watermark used to swallow. Ascending here
    // yields {0, 7}; before the fix, descending yielded {7}.
    const auto probe = [&](const std::vector<int64_t>& raw) {
        std::vector<std::vector<uint8_t>> encoded;
        for (const int64_t v : raw) {
            encoded.push_back(sortable_bigint(v));
        }
        std::vector<Slice> slices;
        for (const auto& e : encoded) {
            slices.emplace_back(e);
        }
        roaring::Roaring hits;
        EXPECT_TRUE(opened.reader->lookup_many(slices, &hits).ok());
        return hits;
    };

    roaring::Roaring both;
    both.add(0);
    both.add(7);
    EXPECT_TRUE(probe({10, 80}) == both);
    EXPECT_TRUE(probe({80, 10}) == both) << "descending probes lost a row";

    roaring::Roaring three;
    three.add(1);
    three.add(4);
    three.add(6);
    EXPECT_TRUE(probe({20, 50, 70}) == three);
    EXPECT_TRUE(probe({50, 20, 70}) == three) << "unsorted probes lost a row";

    // Duplicates are absorbed, not double-counted or rejected.
    roaring::Roaring dup;
    dup.add(2);
    dup.add(5);
    EXPECT_TRUE(probe({30, 30, 60}) == dup);
    EXPECT_TRUE(probe({60, 30, 60, 30}) == dup);
}

} // namespace
} // namespace doris::snii::bkd
