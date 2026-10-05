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

#include "storage/index/snii/bkd/bkd_builder.h"

#include <dirent.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/bkd/bkd_index_block.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/bkd/leaf_codec.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/temp_dir.h"
#include "storage/olap_common.h"

namespace doris::snii::bkd {
namespace {

constexpr uint32_t kBytesPerDim = sizeof(int64_t);
constexpr uint32_t kRecordSize = kBytesPerDim + kPointDocIdBytes;
constexpr FieldType kFieldType = FieldType::OLAP_FIELD_TYPE_BIGINT;

// Unsigned big-endian sortable bytes for a BIGINT -- what
// KeyCoder::full_encode_ascending emits (sign bit flipped, then byte-swapped).
// The whole index compares values with memcmp from offset 0 (INV-1), so feeding
// little-endian bytes would build a self-consistent but semantically wrong index.
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

    bool operator==(const Point& other) const {
        return value == other.value && doc_id == other.doc_id;
    }
};

// The order the builder must produce: (value, doc_id), where the value order is the
// unsigned byte-wise one, which for BIGINT sortable bytes is the signed numeric
// order.
std::vector<Point> sorted_points(std::vector<Point> points) {
    std::ranges::sort(points, [](const Point& a, const Point& b) {
        if (a.value != b.value) return a.value < b.value;
        return a.doc_id < b.doc_id;
    });
    return points;
}

// Collects the bkd_data bytes. bytes_written() is the offset truth the builder
// bases its leaf directory on, exactly as io::FileWriter documents.
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

BkdBuilderOptions default_options(uint32_t points_per_leaf = kDefaultPointsPerLeaf) {
    BkdBuilderOptions options;
    options.bytes_per_dim = kBytesPerDim;
    options.field_type = kFieldType;
    options.points_per_leaf = points_per_leaf;
    return options;
}

// One completed build: the two sub-files plus what finish() reported.
struct BuiltIndex {
    std::vector<uint8_t> index_bytes;
    std::vector<uint8_t> data_bytes;
    BkdStats stats;
};

Status build(const std::vector<Point>& points, const BkdBuilderOptions& options, BuiltIndex* out) {
    std::unique_ptr<BkdBuilder> builder;
    RETURN_IF_ERROR(BkdBuilder::create(options, &builder));
    for (const Point& point : points) {
        RETURN_IF_ERROR(builder->add(point.doc_id, Slice(sortable_bigint(point.value))));
    }
    MemoryFileWriter data;
    ByteSink index;
    RETURN_IF_ERROR(builder->finish(&data, &index, &out->stats));
    out->index_bytes = index.take();
    out->data_bytes = data.bytes();
    return Status::OK();
}

// Everything the index says it holds, read back the way a query would: open
// bkd_index, then walk every leaf of bkd_data through decode_leaf_block and rebuild
// each point's full value from the leaf's common prefix plus its run's suffix.
::testing::AssertionResult read_back(const BuiltIndex& built, BkdIndexBlockReader* reader,
                                     std::vector<Point>* points) {
    const Status opened =
            BkdIndexBlockReader::open(Slice(built.index_bytes), built.data_bytes.size(), reader);
    if (!opened.ok()) {
        return ::testing::AssertionFailure() << "open failed: " << opened;
    }
    points->clear();
    for (uint32_t i = 0; i < reader->leaf_count(); ++i) {
        const LeafRef leaf = reader->leaf(i);
        const uint64_t end = (i + 1 < reader->leaf_count()) ? reader->leaf(i + 1).offset
                                                            : built.data_bytes.size();
        if (end <= leaf.offset || end > built.data_bytes.size()) {
            return ::testing::AssertionFailure()
                   << "leaf " << i << " spans [" << leaf.offset << ", " << end << ") of "
                   << built.data_bytes.size();
        }
        const Slice block(built.data_bytes.data() + leaf.offset,
                          static_cast<size_t>(end - leaf.offset));
        DecodedLeafBlock decoded;
        const Status status =
                decode_leaf_block(block, reader->header().bytes_per_dim, leaf.count, &decoded);
        if (!status.ok()) {
            return ::testing::AssertionFailure() << "leaf " << i << " decode failed: " << status;
        }
        for (const LeafValueRun& run : decoded.runs) {
            std::vector<uint8_t> value(decoded.common_prefix.data(),
                                       decoded.common_prefix.data() + decoded.common_prefix.size());
            value.insert(value.end(), run.suffix.data(), run.suffix.data() + run.suffix.size());
            if (value.size() != kBytesPerDim) {
                return ::testing::AssertionFailure()
                       << "leaf " << i << " rebuilt a " << value.size() << "-byte value";
            }
            // Undo the sortable encoding so failures print readable numbers.
            uint64_t u = 0;
            for (uint32_t b = 0; b < kBytesPerDim; ++b) {
                u = (u << 8) | value[b];
            }
            const int64_t decoded_value = static_cast<int64_t>(u ^ (uint64_t {1} << 63));
            for (uint32_t k = 0; k < run.count; ++k) {
                points->push_back(Point {decoded_value, decoded.doc_ids[run.first_point + k]});
            }
        }
    }
    return ::testing::AssertionSuccess();
}

// The split array is what a query binary-searches, so its contract -- split i is the
// FIRST value of leaf i + 1 (design 6.4) -- is checked against the leaves themselves.
::testing::AssertionResult splits_match_leaf_heads(const BkdIndexBlockReader& reader,
                                                   const std::vector<Point>& ordered) {
    uint32_t first_point = 0;
    for (uint32_t i = 0; i < reader.leaf_count(); ++i) {
        if (i > 0) {
            if (first_point >= ordered.size()) {
                return ::testing::AssertionFailure()
                       << "leaf " << i << " starts past the end of the point list";
            }
            const std::vector<uint8_t> expected = sortable_bigint(ordered[first_point].value);
            const Slice split = reader.split_value(i - 1);
            if (split.size() != expected.size() ||
                std::memcmp(split.data(), expected.data(), expected.size()) != 0) {
                return ::testing::AssertionFailure()
                       << "split " << (i - 1) << " is not the first value of leaf " << i;
            }
        }
        first_point += reader.leaf(i).count;
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult bounds_match(const BkdIndexBlockReader& reader,
                                        const std::vector<Point>& ordered) {
    // An empty index has no bounds to ask for at all (design 5.3), so report that as
    // a failed expectation rather than tripping the reader's DORIS_CHECK.
    if (reader.empty()) {
        return ::testing::AssertionFailure() << "index is empty, it has no bounds";
    }
    const std::vector<uint8_t> min = sortable_bigint(ordered.front().value);
    const std::vector<uint8_t> max = sortable_bigint(ordered.back().value);
    if (std::memcmp(reader.min_value().data(), min.data(), min.size()) != 0) {
        return ::testing::AssertionFailure() << "min_value is not the smallest point value";
    }
    if (std::memcmp(reader.max_value().data(), max.data(), max.size()) != 0) {
        return ::testing::AssertionFailure() << "max_value is not the largest point value";
    }
    return ::testing::AssertionSuccess();
}

// ---------------------------------------------------------------------------
// Round trip
// ---------------------------------------------------------------------------

TEST(BkdBuilderTest, RoundTripRebuildsEveryPointInOrder) {
    std::vector<Point> points;
    uint64_t state = 0x9E3779B97F4A7C15ULL;
    for (uint32_t i = 0; i < 5000; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        // Straddles zero so the sign-bit flip of the sortable encoding is exercised.
        points.push_back(Point {static_cast<int64_t>(state >> 40) - 4000000, i});
    }

    BuiltIndex built;
    ASSERT_TRUE(build(points, default_options(), &built).ok());

    EXPECT_EQ(built.stats.point_count, points.size());
    EXPECT_EQ(built.stats.doc_count, points.size());
    // Derived, not hardcoded: 5000 points cut at whatever the default leaf
    // capacity is. Pinning the literal made this test fail purely because the
    // default moved, which says nothing about the round trip it exists to check.
    EXPECT_EQ(built.stats.leaf_count,
              (points.size() + kDefaultPointsPerLeaf - 1) / kDefaultPointsPerLeaf);
    EXPECT_FALSE(built.stats.built_with_spill);
    EXPECT_EQ(built.stats.index_bytes, built.index_bytes.size());
    EXPECT_EQ(built.stats.data_bytes, built.data_bytes.size());

    BkdIndexBlockReader reader;
    std::vector<Point> decoded;
    ASSERT_TRUE(read_back(built, &reader, &decoded));

    EXPECT_EQ(reader.header().format_version, kFormatVersion);
    EXPECT_EQ(reader.header().flags, 0U);
    EXPECT_EQ(reader.header().bytes_per_dim, kBytesPerDim);
    EXPECT_EQ(reader.header().field_type, kFieldType);
    EXPECT_EQ(reader.header().point_count, points.size());
    EXPECT_EQ(reader.header().doc_count, points.size());
    EXPECT_EQ(reader.header().points_per_leaf, kDefaultPointsPerLeaf);

    const std::vector<Point> expected = sorted_points(points);
    EXPECT_EQ(decoded, expected);
    EXPECT_TRUE(bounds_match(reader, expected));
    EXPECT_TRUE(splits_match_leaf_heads(reader, expected));
}

// A caller that fills only the two REQUIRED options (design doc 6.1) must get
// the documented leaf capacity. Nothing else covers this: default_options()
// always assigns points_per_leaf explicitly, so it would keep passing even if
// the default member initializer were deleted -- at which point create()'s
// DORIS_CHECK_GT would turn every such caller into a crash. Asserting the
// struct's default in isolation cannot catch that either; only building through
// it can.
TEST(BkdBuilderTest, UntouchedOptionsUseTheDocumentedLeafCapacity) {
    BkdBuilderOptions options; // points_per_leaf / build_buffer_bytes untouched
    options.bytes_per_dim = kBytesPerDim;
    options.field_type = kFieldType;

    // Two full leaves plus a one-point remainder, so the observed leaf count
    // pins the capacity from both sides: 1023 or 1025 would give a different
    // answer.
    std::vector<Point> points;
    for (uint32_t i = 0; i < 2 * kDefaultPointsPerLeaf + 1; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }

    BuiltIndex built;
    ASSERT_TRUE(build(points, options, &built).ok());
    EXPECT_EQ(built.stats.leaf_count, 3U);

    BkdIndexBlockReader reader;
    std::vector<Point> decoded;
    ASSERT_TRUE(read_back(built, &reader, &decoded));
    EXPECT_EQ(reader.header().points_per_leaf, kDefaultPointsPerLeaf);
    ASSERT_EQ(reader.leaf_count(), 3U);
    EXPECT_EQ(reader.leaf(0).count, kDefaultPointsPerLeaf);
    EXPECT_EQ(reader.leaf(1).count, kDefaultPointsPerLeaf);
    EXPECT_EQ(reader.leaf(2).count, 1U);
}

// ---------------------------------------------------------------------------
// Bounded build (design 6.2 / 12.5)
// ---------------------------------------------------------------------------

// The property that makes spilling safe to turn on: a build that had to spill
// must produce the SAME BYTES as one that never did. Anything else -- including
// recording the build mode in the header -- would make two logically identical
// indexes compare unequal and would rob the golden digest of its meaning.
//
// built_with_spill is therefore reported through BkdStats only. It describes how
// the file was produced, not what it contains.
TEST(BkdBuilderTest, SpillingProducesByteIdenticalOutput) {
    std::vector<Point> points;
    uint64_t state = 0xA4093822299F31D0ULL;
    for (uint32_t i = 0; i < 5000; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        points.push_back(Point {static_cast<int64_t>(state >> 40) - 4000000, i});
    }

    BuiltIndex unbounded;
    ASSERT_TRUE(build(points, default_options(), &unbounded).ok());
    EXPECT_FALSE(unbounded.stats.built_with_spill);

    // Every one of these forces a different number of runs, and 12 bytes is one
    // record: a single-record buffer is the degenerate merge of 5000 runs.
    const size_t record_size = kBytesPerDim + kPointDocIdBytes;
    for (const uint32_t resident_points : {1U, 2U, 63U, 512U, 4999U}) {
        SCOPED_TRACE("resident_points " + std::to_string(resident_points));
        BkdBuilderOptions bounded = default_options();
        bounded.build_buffer_bytes = static_cast<uint64_t>(resident_points) * record_size;

        BuiltIndex spilled;
        ASSERT_TRUE(build(points, bounded, &spilled).ok());
        EXPECT_TRUE(spilled.stats.built_with_spill);

        EXPECT_EQ(spilled.index_bytes, unbounded.index_bytes);
        EXPECT_EQ(spilled.data_bytes, unbounded.data_bytes);
        EXPECT_EQ(spilled.stats.point_count, unbounded.stats.point_count);
        EXPECT_EQ(spilled.stats.doc_count, unbounded.stats.doc_count);
        EXPECT_EQ(spilled.stats.leaf_count, unbounded.stats.leaf_count);
    }
}

// Byte-identity alone does not prove the build is BOUNDED: a spill that wrote
// the run but forgot to drop the resident buffer would still produce the right
// bytes. This test samples the CHARGE rather than trusting the implementation to
// say so.
//
// What catches what, verified by mutation rather than assumed: commenting out
// the buffer release in spill_current_run() trips reserve_points()'s
// DORIS_CHECK(target >= needed) on the very next add, so THAT regression dies at
// the invariant, not here. This assertion's own job is the charge itself -- a
// reservation that is never lowered, or a growth policy that ratchets -- which
// no invariant covers.
//
// SCOPE: MemoryReporter tracks the resident RECORD BUFFER, which is what the
// ceiling governs. The merge's own windows are sized from max_points_ and are
// not reporter-charged; their bound is discussed with the merge path in
// bkd_builder.cpp, and it degrades when the run count grows large.
TEST(BkdBuilderTest, ResidentChargeStaysUnderTheCeilingAcrossManySpills) {
    constexpr uint32_t kResidentPoints = 64;
    writer::MemoryReporter reporter;
    BkdBuilderOptions options = default_options(16);
    options.reporter = &reporter;
    options.build_buffer_bytes = static_cast<uint64_t>(kResidentPoints) * kRecordSize;

    std::unique_ptr<BkdBuilder> builder;
    ASSERT_TRUE(BkdBuilder::create(options, &builder).ok());

    int64_t peak = 0;
    for (uint32_t i = 0; i < 5000; ++i) { // ~78 spills through a 64-point window
        ASSERT_TRUE(builder->add(i, Slice(sortable_bigint(i))).ok()) << "point " << i;
        peak = std::max(peak, reporter.current_bytes());
    }

    // The growth policy may overshoot the exact ceiling while doubling, so the
    // bound is a small multiple rather than an equality. What matters is that it
    // is a CONSTANT: 5000 points through a 64-point window must not charge
    // anything like 5000 records.
    const int64_t ceiling = static_cast<int64_t>(kResidentPoints) * kRecordSize;
    EXPECT_LE(peak, 4 * ceiling) << "peak " << peak << " vs ceiling " << ceiling;
    EXPECT_GT(peak, 0);

    MemoryFileWriter data;
    ByteSink index;
    BkdStats stats;
    ASSERT_TRUE(builder->finish(&data, &index, &stats).ok());
    EXPECT_TRUE(stats.built_with_spill);
    EXPECT_EQ(stats.point_count, 5000U);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

// A builder that is dropped mid-stream must not leave its runs behind. There is
// no handle to the paths from outside, so the check is on the temp dir itself.
TEST(BkdBuilderTest, AbandonedBuildRemovesItsRuns) {
    const std::string dir = writer::resolve_temp_dir();
    const auto count_runs = [&dir]() {
        size_t found = 0;
        DIR* handle = ::opendir(dir.c_str());
        if (handle == nullptr) {
            return found;
        }
        while (const dirent* entry = ::readdir(handle)) {
            const std::string name(entry->d_name);
            if (name.rfind("snii_bkd_", 0) == 0 && name.size() > 4 &&
                name.compare(name.size() - 4, 4, ".run") == 0) {
                ++found;
            }
        }
        ::closedir(handle);
        return found;
    };

    const size_t before = count_runs();
    {
        BkdBuilderOptions options = default_options(4);
        options.build_buffer_bytes = 4 * kRecordSize;
        std::unique_ptr<BkdBuilder> builder;
        ASSERT_TRUE(BkdBuilder::create(options, &builder).ok());
        for (uint32_t i = 0; i < 40; ++i) { // forces ~10 runs
            ASSERT_TRUE(builder->add(i, Slice(sortable_bigint(i))).ok());
        }
        // A builder that never spilled would make the assertion below vacuous:
        // "no runs left behind" is trivially true when no run was ever created.
        ASSERT_GT(count_runs(), before) << "the buffer never spilled; nothing to clean up";
        // builder goes out of scope WITHOUT finish()
    }
    EXPECT_EQ(count_runs(), before);
}

// A buffer that comfortably holds everything must not spill at all: the fast
// path is what the vast majority of segments take, and paying for temp files
// there would be a silent regression.
TEST(BkdBuilderTest, ABufferThatFitsNeverSpills) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 500; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    BkdBuilderOptions options = default_options();
    options.build_buffer_bytes = 500ULL * (kBytesPerDim + kPointDocIdBytes);

    BuiltIndex built;
    ASSERT_TRUE(build(points, options, &built).ok());
    EXPECT_FALSE(built.stats.built_with_spill);
}

// ---------------------------------------------------------------------------
// Degenerate shapes
// ---------------------------------------------------------------------------

TEST(BkdBuilderTest, EmptyIndexIsHeaderOnlyAndDataIsZeroLength) {
    BuiltIndex built;
    ASSERT_TRUE(build({}, default_options(), &built).ok());

    // Design 5.3: emptiness is STATED (leaf_count == 0), not implied by a sentinel
    // offset, and a zero-length bkd_data is legal rather than corruption.
    EXPECT_EQ(built.stats.point_count, 0U);
    EXPECT_EQ(built.stats.doc_count, 0U);
    EXPECT_EQ(built.stats.leaf_count, 0U);
    EXPECT_EQ(built.stats.data_bytes, 0U);
    EXPECT_TRUE(built.data_bytes.empty());
    EXPECT_GT(built.index_bytes.size(), 0U);

    BkdIndexBlockReader reader;
    std::vector<Point> decoded;
    ASSERT_TRUE(read_back(built, &reader, &decoded));
    EXPECT_TRUE(reader.empty());
    EXPECT_EQ(reader.leaf_count(), 0U);
    EXPECT_TRUE(decoded.empty());
}

TEST(BkdBuilderTest, SinglePointIndexHasOneLeafAndEqualBounds) {
    const std::vector<Point> points {{-7, 3}};
    BuiltIndex built;
    ASSERT_TRUE(build(points, default_options(), &built).ok());

    EXPECT_EQ(built.stats.point_count, 1U);
    EXPECT_EQ(built.stats.doc_count, 1U);
    EXPECT_EQ(built.stats.leaf_count, 1U);

    BkdIndexBlockReader reader;
    std::vector<Point> decoded;
    ASSERT_TRUE(read_back(built, &reader, &decoded));
    ASSERT_EQ(reader.leaf_count(), 1U);
    EXPECT_EQ(decoded, points);
    EXPECT_EQ(reader.leaf(0).offset, 0U);
    EXPECT_EQ(reader.leaf(0).count, 1U);
    // leaf_count == 1 means there is no boundary to record.
    EXPECT_TRUE(reader.split_values().empty());
    EXPECT_TRUE(bounds_match(reader, points));
}

TEST(BkdBuilderTest, ExactlyOneFullLeaf) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 4; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    BuiltIndex built;
    ASSERT_TRUE(build(points, default_options(4), &built).ok());

    BkdIndexBlockReader reader;
    std::vector<Point> decoded;
    ASSERT_TRUE(read_back(built, &reader, &decoded));
    ASSERT_EQ(reader.leaf_count(), 1U);
    EXPECT_EQ(reader.leaf(0).count, 4U);
    EXPECT_TRUE(reader.split_values().empty());
    EXPECT_EQ(decoded, points);
}

TEST(BkdBuilderTest, ExactlyTwoFullLeaves) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 8; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    BuiltIndex built;
    ASSERT_TRUE(build(points, default_options(4), &built).ok());

    BkdIndexBlockReader reader;
    std::vector<Point> decoded;
    ASSERT_TRUE(read_back(built, &reader, &decoded));
    ASSERT_EQ(reader.leaf_count(), 2U);
    EXPECT_EQ(reader.leaf(0).count, 4U);
    EXPECT_EQ(reader.leaf(1).count, 4U);
    EXPECT_EQ(reader.leaf(0).offset, 0U);
    EXPECT_GT(reader.leaf(1).offset, 0U);
    EXPECT_EQ(reader.split_values().size(), kBytesPerDim);
    EXPECT_EQ(decoded, points);
    EXPECT_TRUE(splits_match_leaf_heads(reader, points));
}

TEST(BkdBuilderTest, LastLeafIsShortAndLeafCountIsNotRoundedToAPowerOfTwo) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 9; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    BuiltIndex built;
    ASSERT_TRUE(build(points, default_options(4), &built).ok());

    BkdIndexBlockReader reader;
    std::vector<Point> decoded;
    ASSERT_TRUE(read_back(built, &reader, &decoded));
    // Design 6.4: an ordered split array has no complete-binary-tree requirement, so
    // 9 points at 4 per leaf are 4 + 4 + 1 and NOT four leaves of ~3 diluted by
    // rounding the leaf count up to a power of two.
    ASSERT_EQ(reader.leaf_count(), 3U);
    EXPECT_EQ(reader.leaf(0).count, 4U);
    EXPECT_EQ(reader.leaf(1).count, 4U);
    EXPECT_EQ(reader.leaf(2).count, 1U);
    EXPECT_EQ(built.stats.leaf_count, 3U);
    EXPECT_EQ(decoded, points);
    EXPECT_TRUE(splits_match_leaf_heads(reader, points));
}

TEST(BkdBuilderTest, AllEqualValuesAcrossManyLeaves) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 10; ++i) {
        points.push_back(Point {42, i});
    }
    BuiltIndex built;
    ASSERT_TRUE(build(points, default_options(4), &built).ok());

    BkdIndexBlockReader reader;
    std::vector<Point> decoded;
    ASSERT_TRUE(read_back(built, &reader, &decoded));
    ASSERT_EQ(reader.leaf_count(), 3U);
    EXPECT_EQ(decoded, points);
    // Every split is the repeated value: the split array is non-decreasing, not
    // strictly increasing, precisely because one value may span several leaves.
    EXPECT_TRUE(splits_match_leaf_heads(reader, points));
    EXPECT_TRUE(bounds_match(reader, points));
    EXPECT_EQ(std::memcmp(reader.min_value().data(), reader.max_value().data(), kBytesPerDim), 0);
}

TEST(BkdBuilderTest, HonoursTheFieldTypeWidthInsteadOfAssumingEightBytes) {
    // INV-2 ties bytes_per_dim to the indexed type, and every array in both
    // sub-files is strided by it. A width baked in anywhere would still round-trip
    // for BIGINT and silently mis-stride everything else, so one narrower type is
    // built end to end.
    constexpr uint32_t kIntBytes = sizeof(int32_t);
    const auto sortable_int = [](int32_t v) {
        const uint32_t u = static_cast<uint32_t>(v) ^ (uint32_t {1} << 31);
        std::vector<uint8_t> out(kIntBytes);
        for (uint32_t i = 0; i < kIntBytes; ++i) {
            out[kIntBytes - 1 - i] = static_cast<uint8_t>(u >> (8 * i));
        }
        return out;
    };

    BkdBuilderOptions options;
    options.bytes_per_dim = kIntBytes;
    options.field_type = FieldType::OLAP_FIELD_TYPE_INT;
    options.points_per_leaf = 4;

    const std::vector<int32_t> values {5, -3, 9, 0, -100, 7};
    std::unique_ptr<BkdBuilder> builder;
    ASSERT_TRUE(BkdBuilder::create(options, &builder).ok());
    for (uint32_t i = 0; i < values.size(); ++i) {
        ASSERT_TRUE(builder->add(i, Slice(sortable_int(values[i]))).ok());
    }
    MemoryFileWriter data;
    ByteSink index;
    BkdStats stats;
    ASSERT_TRUE(builder->finish(&data, &index, &stats).ok());

    BkdIndexBlockReader reader;
    ASSERT_TRUE(
            BkdIndexBlockReader::open(Slice(index.buffer()), data.bytes().size(), &reader).ok());
    EXPECT_EQ(reader.header().bytes_per_dim, kIntBytes);
    EXPECT_EQ(reader.header().field_type, FieldType::OLAP_FIELD_TYPE_INT);
    ASSERT_EQ(reader.leaf_count(), 2U);

    std::vector<int32_t> ordered = values;
    std::ranges::sort(ordered);
    const std::vector<uint8_t> min = sortable_int(ordered.front());
    const std::vector<uint8_t> max = sortable_int(ordered.back());
    EXPECT_EQ(reader.min_value().size(), kIntBytes);
    EXPECT_EQ(std::memcmp(reader.min_value().data(), min.data(), kIntBytes), 0);
    EXPECT_EQ(std::memcmp(reader.max_value().data(), max.data(), kIntBytes), 0);
    // The single split is the first value of leaf 1, i.e. the fifth smallest.
    ASSERT_EQ(reader.split_values().size(), kIntBytes);
    const std::vector<uint8_t> split = sortable_int(ordered[4]);
    EXPECT_EQ(std::memcmp(reader.split_value(0).data(), split.data(), kIntBytes), 0);

    std::vector<int32_t> decoded_values;
    for (uint32_t i = 0; i < reader.leaf_count(); ++i) {
        const LeafRef leaf = reader.leaf(i);
        const uint64_t end =
                (i + 1 < reader.leaf_count()) ? reader.leaf(i + 1).offset : data.bytes().size();
        DecodedLeafBlock decoded;
        ASSERT_TRUE(decode_leaf_block(Slice(data.bytes().data() + leaf.offset,
                                            static_cast<size_t>(end - leaf.offset)),
                                      kIntBytes, leaf.count, &decoded)
                            .ok());
        for (const LeafValueRun& run : decoded.runs) {
            std::vector<uint8_t> value(decoded.common_prefix.data(),
                                       decoded.common_prefix.data() + decoded.common_prefix.size());
            value.insert(value.end(), run.suffix.data(), run.suffix.data() + run.suffix.size());
            ASSERT_EQ(value.size(), kIntBytes);
            uint32_t u = 0;
            for (uint32_t b = 0; b < kIntBytes; ++b) {
                u = (u << 8) | value[b];
            }
            decoded_values.insert(decoded_values.end(), run.count,
                                  static_cast<int32_t>(u ^ (uint32_t {1} << 31)));
        }
    }
    EXPECT_EQ(decoded_values, ordered);
}

// ---------------------------------------------------------------------------
// doc_count
// ---------------------------------------------------------------------------

TEST(BkdBuilderTest, ArrayColumnRepeatsOneDocIdAndDocCountStaysDistinct) {
    // Three rows contributing 2, 3 and 1 points -- the array-column shape the old
    // writer's singleValuePerDoc flag falsely promised away.
    const std::vector<Point> points {{5, 0}, {9, 0}, {-1, 1}, {-1, 1}, {7, 1}, {3, 2}};
    BuiltIndex built;
    ASSERT_TRUE(build(points, default_options(4), &built).ok());

    EXPECT_EQ(built.stats.point_count, 6U);
    // Counted by the builder itself; nothing was pushed in from outside.
    EXPECT_EQ(built.stats.doc_count, 3U);

    BkdIndexBlockReader reader;
    std::vector<Point> decoded;
    ASSERT_TRUE(read_back(built, &reader, &decoded));
    EXPECT_EQ(reader.header().doc_count, 3U);
    EXPECT_EQ(decoded, sorted_points(points));
}

// ---------------------------------------------------------------------------
// Bounded memory
// ---------------------------------------------------------------------------

// The ceiling is a SPILL TRIGGER, not a refusal (design 6.2). Phase 1 returned
// MEM_LIMIT_EXCEEDED here as a deliberate placeholder -- being told beat the old
// implementation, which had no offline sort at all and just kept allocating until
// the process died -- and Phase 2 replaces the refusal with a run.
//
// What must NOT come back is the unbounded growth: crossing the ceiling many
// times over still has to succeed and still has to count every point.
TEST(BkdBuilderTest, CrossingTheResidentCeilingSpillsInsteadOfRefusing) {
    BkdBuilderOptions options = default_options(4);
    options.build_buffer_bytes = 10 * kRecordSize; // ten points resident

    std::unique_ptr<BkdBuilder> builder;
    ASSERT_TRUE(BkdBuilder::create(options, &builder).ok());
    // 105 points through a ten-point window: ten full runs plus a residual.
    for (uint32_t i = 0; i < 105; ++i) {
        ASSERT_TRUE(builder->add(i, Slice(sortable_bigint(i))).ok()) << "point " << i;
    }

    MemoryFileWriter data;
    ByteSink index;
    BkdStats stats;
    ASSERT_TRUE(builder->finish(&data, &index, &stats).ok());
    EXPECT_EQ(stats.point_count, 105U);
    // doc_count must survive the spills: every spill empties the resident buffer,
    // and counting "first point" by that buffer being empty would recount a
    // document at each boundary.
    EXPECT_EQ(stats.doc_count, 105U);
    EXPECT_EQ(stats.leaf_count, 27U); // ceil(105 / 4)
    EXPECT_TRUE(stats.built_with_spill);
}

TEST(BkdBuilderTest, MemoryReporterChargesTheRecordBufferAndReleasesItAtFinish) {
    writer::MemoryReporter reporter;
    BkdBuilderOptions options = default_options(4);
    options.reporter = &reporter;

    std::unique_ptr<BkdBuilder> builder;
    ASSERT_TRUE(BkdBuilder::create(options, &builder).ok());
    for (uint32_t i = 0; i < 100; ++i) {
        ASSERT_TRUE(builder->add(i, Slice(sortable_bigint(i))).ok());
    }
    EXPECT_GE(reporter.current_bytes(), static_cast<int64_t>(100 * kRecordSize));

    MemoryFileWriter data;
    ByteSink index;
    BkdStats stats;
    ASSERT_TRUE(builder->finish(&data, &index, &stats).ok());
    // finish() consumes the points, so a builder waiting to be destroyed holds no
    // build RAM at all.
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(BkdBuilderTest, HardLimitedReporterRefusesTheRecordBuffer) {
    // A cap below one record: no growth policy can satisfy it, so the very first
    // add() must report it rather than allocate anyway.
    writer::MemoryReporter reporter(nullptr, 8, writer::MemoryReporter::CapPolicy::kHardLimit);
    BkdBuilderOptions options = default_options(4);
    options.reporter = &reporter;

    std::unique_ptr<BkdBuilder> builder;
    ASSERT_TRUE(BkdBuilder::create(options, &builder).ok());
    const Status refused = builder->add(0, Slice(sortable_bigint(1)));
    EXPECT_TRUE(refused.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << refused;
    EXPECT_EQ(reporter.current_bytes(), 0);
}

// ---------------------------------------------------------------------------
// Output placement
// ---------------------------------------------------------------------------

TEST(BkdBuilderTest, AppendsToOutputsThatAlreadyCarryBytes) {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 9; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }

    MemoryFileWriter data;
    ByteSink index;
    // Stand-ins for other sub-files / sections of the same container.
    const std::vector<uint8_t> data_prefix(7, 0xAB);
    ASSERT_TRUE(data.append(Slice(data_prefix)).ok());
    index.put_fixed32(0xDEADBEEF);
    const size_t index_prefix_size = index.size();

    std::unique_ptr<BkdBuilder> builder;
    ASSERT_TRUE(BkdBuilder::create(default_options(4), &builder).ok());
    for (const Point& point : points) {
        ASSERT_TRUE(builder->add(point.doc_id, Slice(sortable_bigint(point.value))).ok());
    }
    BkdStats stats;
    ASSERT_TRUE(builder->finish(&data, &index, &stats).ok());

    // The prefixes survive, and the reported sizes cover only what this build added.
    EXPECT_EQ(stats.data_bytes, data.bytes().size() - data_prefix.size());
    EXPECT_EQ(stats.index_bytes, index.size() - index_prefix_size);
    EXPECT_EQ(std::memcmp(data.bytes().data(), data_prefix.data(), data_prefix.size()), 0);

    BuiltIndex built;
    built.stats = stats;
    built.index_bytes.assign(index.buffer().begin() + index_prefix_size, index.buffer().end());
    built.data_bytes.assign(data.bytes().begin() + data_prefix.size(), data.bytes().end());

    BkdIndexBlockReader reader;
    std::vector<Point> decoded;
    // Leaf offsets are relative to the START of this build's bkd_data, which is what
    // makes them bounded by the sub-file length the container records.
    ASSERT_TRUE(read_back(built, &reader, &decoded));
    ASSERT_EQ(reader.leaf_count(), 3U);
    EXPECT_EQ(reader.leaf(0).offset, 0U);
    EXPECT_EQ(decoded, points);
}

// A merge holds one read window per run. With a single k-way merge that
// footprint is run_count x per_run records, and per_run is the resident
// allowance split across the cursors -- so once run_count exceeds the allowance
// (in records), the floor of one record per cursor makes the merge hold
// run_count records, which is total_points / max_points and therefore GROWS
// with the input. The ceiling stops bounding anything.
//
// This is only reachable with a pathologically small build_buffer_bytes (a real
// 256 MiB buffer of 12-byte records is ~22M points per run), but "unreachable in
// practice" is not the same as "bounded", and the ceiling is the whole reason
// the spill path exists.
TEST(BkdBuilderTest, MergeStaysBoundedWhenRunCountExceedsTheResidentAllowance) {
    constexpr uint32_t kRecordSize = kBytesPerDim + kPointDocIdBytes;
    // Four points resident: 6000 points therefore spill ~1500 runs, far more
    // than the four-record allowance a single merge could window.
    constexpr uint32_t kResidentPoints = 4;
    constexpr uint32_t kPoints = 6000;

    // Descending input, so every run is a separate sorted fragment and the merge
    // genuinely has to interleave all of them.
    std::vector<Point> points;
    for (uint32_t i = 0; i < kPoints; ++i) {
        points.push_back(Point {static_cast<int64_t>(kPoints - i), i});
    }

    BkdBuilderOptions options = default_options(16);
    options.build_buffer_bytes = static_cast<uint64_t>(kResidentPoints) * kRecordSize;

    BuiltIndex built;
    ASSERT_TRUE(build(points, options, &built).ok());

    EXPECT_TRUE(built.stats.built_with_spill);
    EXPECT_EQ(built.stats.point_count, kPoints);

    // The bound the ceiling is supposed to give: cursor windows never exceed the
    // configured resident allowance. A leaf block sits on top of this and is
    // accounted separately -- a leaf must be materialized contiguously however
    // the points arrived.
    EXPECT_LE(built.stats.peak_merge_buffer_bytes, options.build_buffer_bytes)
            << "merge held " << built.stats.peak_merge_buffer_bytes
            << " bytes of cursor windows against a " << options.build_buffer_bytes
            << "-byte ceiling";

    // Reaching that bound with this many runs is only possible by folding them
    // in groups; a single pass over ~1500 cursors cannot fit in four records.
    EXPECT_GT(built.stats.merge_passes, 1U)
            << "expected a multi-pass fold, got a single merge over every run";
}

// The fold must not change the answer: whatever number of passes it takes, the
// emitted index is the one a fully resident build would have produced.
TEST(BkdBuilderTest, MultiPassMergeProducesTheSameIndexAsAResidentBuild) {
    constexpr uint32_t kRecordSize = kBytesPerDim + kPointDocIdBytes;
    constexpr uint32_t kPoints = 4000;

    std::vector<Point> points;
    uint64_t state = 0x243F6A8885A308D3ULL;
    for (uint32_t i = 0; i < kPoints; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        points.push_back(Point {static_cast<int64_t>(state % 2000) - 1000, i});
    }

    BkdBuilderOptions resident_options = default_options(32);
    resident_options.build_buffer_bytes = static_cast<uint64_t>(kPoints) * kRecordSize * 2;
    BuiltIndex resident;
    ASSERT_TRUE(build(points, resident_options, &resident).ok());
    ASSERT_FALSE(resident.stats.built_with_spill);

    // Three points resident -> ~1300 runs -> the fold path.
    BkdBuilderOptions folded_options = default_options(32);
    folded_options.build_buffer_bytes = 3ULL * kRecordSize;
    BuiltIndex folded;
    ASSERT_TRUE(build(points, folded_options, &folded).ok());
    ASSERT_TRUE(folded.stats.built_with_spill);
    ASSERT_GT(folded.stats.merge_passes, 1U);

    // Byte identity, not just equal answers: the two build modes are required to
    // emit the same index (design 12.5, D0), and a fold that reordered equal
    // keys would still answer every query correctly while breaking that.
    EXPECT_EQ(folded.stats.point_count, resident.stats.point_count);
    EXPECT_EQ(folded.stats.doc_count, resident.stats.doc_count);
    EXPECT_EQ(folded.stats.leaf_count, resident.stats.leaf_count);
    EXPECT_TRUE(folded.index_bytes == resident.index_bytes)
            << "a multi-pass fold changed the emitted bkd_index bytes";
    EXPECT_TRUE(folded.data_bytes == resident.data_bytes)
            << "a multi-pass fold changed the emitted bkd_data bytes";
}

} // namespace
} // namespace doris::snii::bkd
