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

// P1-6, design 12.2: property tests for the SNII-native BKD index.
//
// The differential test (design 12.1) can only prove "the rewrite answers what
// the CLucene BKD answers". An error the two SHARE passes it unnoticed, and the
// baseline has catalogued defects (design 14), so agreement alone is not
// correctness. These tests never consult either implementation: they state LAWS
// a correct one-dimensional range index obeys, and check them against
//
//   * a brute-force scan of the same point list -- the independent oracle
//     (design 12.2 / R1), and
//   * the index's own answers to RELATED queries, which is what catches an
//     oracle and an index that are wrong the same way.
//
// The laws are the part a brute-force comparison cannot express on its own: an
// answer that does not depend on points_per_leaf, a pivot that partitions the
// doc set, a widening interval that only ever adds docs, and a build whose bytes
// do not depend on the order equal-doc points were appended in.
//
// Every RNG seed is a literal in this file and every dataset is derived from one,
// so a failure reproduces from the test name alone -- no state carries between
// runs and no clock or address feeds the generator.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/bkd/bkd_builder.h"
#include "storage/index/snii/bkd/bkd_reader.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/types.h"

namespace doris::snii::bkd {
namespace {

// Point values travel as std::string because that is what
// KeyCoder::full_encode_ascending appends to. Every value of one index has the
// same width, so a memcmp over that width IS the total order the index
// implements (INV-1).
using Bytes = std::string;

struct EncodedPoint {
    uint32_t doc_id = 0;
    Bytes value;
};

// One side of an interval. `present == false` is the unbounded side, which is
// how <, <=, > and >= all reach BkdReader::range as a single call.
struct Bound {
    bool present = false;
    Bytes value;
    bool inclusive = true;
};

struct RangeQuery {
    Bound lower;
    Bound upper;
};

// ---------------------------------------------------------------------------
// Deterministic generation
// ---------------------------------------------------------------------------

// A 64-bit LCG. Nothing here may consult the clock, the address space or the
// environment: a property test is only worth its reproducibility.
class Rng {
public:
    explicit Rng(uint64_t seed) : state_(seed) {}
    uint64_t next() {
        state_ = state_ * 6364136223846793005ULL + 1442695040888963407ULL;
        return state_ >> 11;
    }
    uint64_t next_below(uint64_t bound) { return next() % bound; }
    int64_t next_in(int64_t low, int64_t high) {
        return low + static_cast<int64_t>(next_below(static_cast<uint64_t>(high - low + 1)));
    }
    bool next_bool() { return (next() & 1U) != 0; }

private:
    uint64_t state_;
};

// The seeds the whole file runs on. Several of them, because one seed is one
// point set and a law that only holds for one shape is not a law.
constexpr uint64_t kSeeds[] = {0x243F6A8885A308D3ULL, 0x13198A2E03707344ULL, 0xA4093822299F31D0ULL};

// Leaf capacities every answer must be invariant to. 1 and 2 make almost every
// leaf a boundary leaf; 1024 makes the whole point set one leaf and the split
// array empty. A range answer that differs between them is a leaf-boundary bug
// no single-capacity test would show.
constexpr uint32_t kLeafCapacities[] = {1, 2, 5, 16, 1024};

// The single encoder for both points and bounds. Its width is bytes_per_dim by
// construction, and that is asserted rather than assumed: a KeyCoder emitting
// something other than sizeof(CppType) would silently break INV-2.
template <FieldType FT>
Bytes encode(const typename CppTypeTraits<FT>::CppType& value) {
    Bytes buf;
    get_key_coder(FT)->full_encode_ascending(&value, &buf);
    EXPECT_EQ(buf.size(), field_type_size(FT));
    return buf;
}

// Rank -> value. Narrow types wrap, which is a legitimate duplicate-heavy shape
// rather than a defect of the generator: the index has to be right when a value
// repeats across thousands of points and dozens of leaves.
template <FieldType FT>
Bytes encode_rank(int64_t rank) {
    using CppType = typename CppTypeTraits<FT>::CppType;
    return encode<FT>(static_cast<CppType>(rank));
}

Slice to_slice(const Bytes& bytes) {
    return Slice(std::string_view(bytes));
}

// ---------------------------------------------------------------------------
// The oracle: a linear scan, independent of every line of the index
// ---------------------------------------------------------------------------

bool lower_ok(const Bound& bound, const Bytes& value) {
    if (!bound.present) {
        return true;
    }
    const int order = std::memcmp(value.data(), bound.value.data(), value.size());
    return bound.inclusive ? order >= 0 : order > 0;
}

bool upper_ok(const Bound& bound, const Bytes& value) {
    if (!bound.present) {
        return true;
    }
    const int order = std::memcmp(value.data(), bound.value.data(), value.size());
    return bound.inclusive ? order <= 0 : order < 0;
}

bool matches(const RangeQuery& query, const Bytes& value) {
    return lower_ok(query.lower, value) && upper_ok(query.upper, value);
}

roaring::Roaring brute_force(const std::vector<EncodedPoint>& points, const RangeQuery& query) {
    roaring::Roaring hits;
    for (const EncodedPoint& point : points) {
        if (matches(query, point.value)) {
            hits.add(point.doc_id);
        }
    }
    return hits;
}

std::string to_hex(const Bytes& bytes) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string text;
    for (const char byte : bytes) {
        const auto value = static_cast<uint8_t>(byte);
        text.push_back(kDigits[value >> 4]);
        text.push_back(kDigits[value & 0x0F]);
    }
    return text;
}

std::string describe(const RangeQuery& query) {
    std::string text = query.lower.present
                               ? (query.lower.inclusive ? "[" : "(") + to_hex(query.lower.value)
                               : std::string("(-inf");
    text += ", ";
    text += query.upper.present ? to_hex(query.upper.value) + (query.upper.inclusive ? "]" : ")")
                                : std::string("+inf)");
    return text;
}

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

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

class MemoryFileReader final : public io::FileReader {
public:
    explicit MemoryFileReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        out->resize(len);
        return read_into(offset, out->data(), len);
    }

    Status read_into(uint64_t offset, uint8_t* out, size_t out_len) override {
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

private:
    std::vector<uint8_t> bytes_;
};

// One built index: the two sub-files laid out inside a container image at
// non-zero offsets -- the shape a real SNII container hands the reader -- plus
// the reader opened over it. The raw sub-file bytes are kept so a test can
// compare two builds byte for byte.
class Index {
public:
    Status build(const std::vector<EncodedPoint>& points, FieldType field_type,
                 uint32_t points_per_leaf) {
        BkdBuilderOptions options;
        options.bytes_per_dim = static_cast<uint32_t>(field_type_size(field_type));
        options.field_type = field_type;
        options.points_per_leaf = points_per_leaf;

        std::unique_ptr<BkdBuilder> builder;
        RETURN_IF_ERROR(BkdBuilder::create(options, &builder));
        for (const EncodedPoint& point : points) {
            RETURN_IF_ERROR(builder->add(point.doc_id, to_slice(point.value)));
        }
        MemoryFileWriter data;
        ByteSink index;
        RETURN_IF_ERROR(builder->finish(&data, &index, &stats_));

        index_bytes_ = index.take();
        data_bytes_ = data.bytes();

        std::vector<uint8_t> image(kLeadingPad, 0xA5);
        BkdSections sections;
        sections.data_offset = image.size();
        sections.data_length = data_bytes_.size();
        image.insert(image.end(), data_bytes_.begin(), data_bytes_.end());
        image.insert(image.end(), kMiddlePad, 0x5A);
        sections.index_offset = image.size();
        sections.index_length = index_bytes_.size();
        image.insert(image.end(), index_bytes_.begin(), index_bytes_.end());
        image.insert(image.end(), kTrailingPad, 0xC3);

        file_ = std::make_unique<MemoryFileReader>(std::move(image));
        return BkdReader::open(file_.get(), sections, &reader_);
    }

    Status range(const RangeQuery& query, roaring::Roaring* hits) const {
        return reader_->range(query.lower.present ? to_slice(query.lower.value) : Slice(),
                              query.lower.inclusive,
                              query.upper.present ? to_slice(query.upper.value) : Slice(),
                              query.upper.inclusive, hits, &scratch_);
    }

    const BkdReader& reader() const { return *reader_; }
    const BkdStats& stats() const { return stats_; }
    const std::vector<uint8_t>& index_bytes() const { return index_bytes_; }
    const std::vector<uint8_t>& data_bytes() const { return data_bytes_; }

private:
    static constexpr size_t kLeadingPad = 13;
    static constexpr size_t kMiddlePad = 5;
    static constexpr size_t kTrailingPad = 3;

    std::unique_ptr<MemoryFileReader> file_;
    std::unique_ptr<BkdReader> reader_;
    std::vector<uint8_t> index_bytes_;
    std::vector<uint8_t> data_bytes_;
    BkdStats stats_;
    mutable BkdQueryScratch scratch_;
};

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------

// A point set plus the probe values its query intervals are drawn from.
struct Dataset {
    std::string name;
    std::vector<EncodedPoint> points;
    std::vector<Bytes> probes;
};

// Random ranks over [-span, span], doc ids ascending as add() requires
// (design 6.1). `points_per_doc_max > 1` is the array-column shape: one row
// contributing several points, which is a first-class case rather than
// something a "single value per doc" flag promises away (design 14 #8).
template <FieldType FT>
std::vector<EncodedPoint> random_points(Rng* rng, uint32_t doc_count, int64_t span,
                                        uint32_t points_per_doc_max) {
    std::vector<EncodedPoint> points;
    for (uint32_t doc = 0; doc < doc_count; ++doc) {
        // A doc with zero points is the NULL row: it never calls add() at all,
        // and must therefore never appear in any answer.
        const auto count = static_cast<uint32_t>(rng->next_below(points_per_doc_max + 1));
        for (uint32_t i = 0; i < count; ++i) {
            points.push_back(EncodedPoint {doc, encode_rank<FT>(rng->next_in(-span, span))});
        }
    }
    return points;
}

// Probes: values that are present, values that are not, and the two ends of the
// sortable byte space. The interesting bounds of a range index are exactly the
// ones that coincide with a stored value or with a leaf boundary, so present
// values are drawn from the point set itself rather than resampled.
template <FieldType FT>
std::vector<Bytes> probes_for(Rng* rng, const std::vector<EncodedPoint>& points, int64_t span) {
    std::vector<Bytes> probes;
    for (int i = 0; i < 12 && !points.empty(); ++i) {
        probes.push_back(points[rng->next_below(points.size())].value);
    }
    for (int i = 0; i < 8; ++i) {
        probes.push_back(encode_rank<FT>(rng->next_in(-span - 2, span + 2)));
    }
    probes.push_back(encode_rank<FT>(-span - 1));
    probes.push_back(encode_rank<FT>(span + 1));
    probes.push_back(Bytes(field_type_size(FT), '\x00'));
    probes.push_back(Bytes(field_type_size(FT), '\xFF'));
    return probes;
}

// The shapes every law is checked on. `span` is what decides the shape: a span
// far below the doc count makes long runs of equal values that straddle leaves,
// a span far above it makes nearly every value distinct.
template <FieldType FT>
std::vector<Dataset> make_datasets(Rng* rng) {
    struct Shape {
        const char* name;
        uint32_t doc_count;
        int64_t span;
        uint32_t points_per_doc_max;
    };
    constexpr Shape kShapes[] = {
            {"dense_duplicates", 400, 5, 1},
            {"moderate", 400, 90, 1},
            {"sparse", 400, 1000000, 1},
            {"array_column", 300, 40, 3},
    };
    std::vector<Dataset> datasets;
    for (const Shape& shape : kShapes) {
        Dataset dataset;
        dataset.name = shape.name;
        dataset.points =
                random_points<FT>(rng, shape.doc_count, shape.span, shape.points_per_doc_max);
        dataset.probes = probes_for<FT>(rng, dataset.points, shape.span);
        datasets.push_back(std::move(dataset));
    }
    return datasets;
}

std::vector<RangeQuery> random_queries(Rng* rng, const std::vector<Bytes>& probes, size_t count) {
    std::vector<RangeQuery> queries;
    for (size_t i = 0; i < count; ++i) {
        RangeQuery query;
        // One side unbounded once in eight, so <, <=, > and >= are covered as
        // often as the two-sided shapes are.
        query.lower.present = rng->next_below(8) != 0;
        query.upper.present = rng->next_below(8) != 0;
        query.lower.value = probes[rng->next_below(probes.size())];
        query.upper.value = probes[rng->next_below(probes.size())];
        query.lower.inclusive = rng->next_bool();
        query.upper.inclusive = rng->next_bool();
        queries.push_back(std::move(query));
    }
    return queries;
}

RangeQuery exactly(const Bytes& value) {
    return RangeQuery {Bound {true, value, true}, Bound {true, value, true}};
}

// ---------------------------------------------------------------------------
// The field types the laws are checked over: one byte, four, eight and sixteen.
// Width is what the split array, the leaf common prefix and every memcmp are
// parameterized on, so the extremes of the width range are the interesting ones
// (the exhaustive type sweep is the differential test's job).
// ---------------------------------------------------------------------------

template <FieldType FT>
struct FieldTag {
    static constexpr FieldType kType = FT;
};

template <typename Tag>
class BkdPropertyTest : public ::testing::Test {};

using WidthSpread = ::testing::Types<
        FieldTag<FieldType::OLAP_FIELD_TYPE_TINYINT>, FieldTag<FieldType::OLAP_FIELD_TYPE_INT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_BIGINT>, FieldTag<FieldType::OLAP_FIELD_TYPE_LARGEINT>>;
TYPED_TEST_SUITE(BkdPropertyTest, WidthSpread);

// ---------------------------------------------------------------------------
// Law 1: the answer is exactly what a linear scan says (design 12.2)
// ---------------------------------------------------------------------------

TYPED_TEST(BkdPropertyTest, RandomIntervalsMatchBruteForce) {
    constexpr FieldType kFieldType = TypeParam::kType;
    for (const uint64_t seed : kSeeds) {
        Rng rng(seed ^ static_cast<uint64_t>(kFieldType));
        for (const Dataset& dataset : make_datasets<kFieldType>(&rng)) {
            const uint32_t points_per_leaf = kLeafCapacities[rng.next_below(
                    sizeof(kLeafCapacities) / sizeof(kLeafCapacities[0]))];
            SCOPED_TRACE("seed=" + std::to_string(seed) + " dataset=" + dataset.name +
                         " points=" + std::to_string(dataset.points.size()) +
                         " points_per_leaf=" + std::to_string(points_per_leaf));
            Index index;
            ASSERT_TRUE(index.build(dataset.points, kFieldType, points_per_leaf).ok());

            roaring::Roaring hits;
            for (const RangeQuery& query : random_queries(&rng, dataset.probes, 150)) {
                ASSERT_TRUE(index.range(query, &hits).ok()) << describe(query);
                EXPECT_EQ(hits, brute_force(dataset.points, query)) << describe(query);
            }
            // Every value that is actually stored, as a point lookup: the bound
            // then coincides with a stored value, with a run boundary and --
            // for one point per leaf -- with a split value.
            for (const EncodedPoint& point : dataset.points) {
                const RangeQuery query = exactly(point.value);
                ASSERT_TRUE(index.range(query, &hits).ok()) << describe(query);
                ASSERT_EQ(hits, brute_force(dataset.points, query)) << describe(query);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Law 2: leaf capacity is a storage decision, never a semantic one
// ---------------------------------------------------------------------------

TYPED_TEST(BkdPropertyTest, AnswersDoNotDependOnLeafCapacity) {
    constexpr FieldType kFieldType = TypeParam::kType;
    for (const uint64_t seed : kSeeds) {
        Rng rng(seed ^ (static_cast<uint64_t>(kFieldType) << 8));
        for (const Dataset& dataset : make_datasets<kFieldType>(&rng)) {
            const std::vector<RangeQuery> queries = random_queries(&rng, dataset.probes, 100);

            // points_per_leaf decides how many leaves there are, which bound each
            // boundary leaf tests, and how many whole-leaf hits sit between them
            // -- three different code paths reaching the same answer.
            std::vector<roaring::Roaring> reference;
            for (const uint32_t points_per_leaf : kLeafCapacities) {
                SCOPED_TRACE("seed=" + std::to_string(seed) + " dataset=" + dataset.name +
                             " points_per_leaf=" + std::to_string(points_per_leaf));
                Index index;
                ASSERT_TRUE(index.build(dataset.points, kFieldType, points_per_leaf).ok());
                // Same points, same doc set, whatever the capacity.
                EXPECT_EQ(index.reader().point_count(), dataset.points.size());

                roaring::Roaring hits;
                for (size_t i = 0; i < queries.size(); ++i) {
                    ASSERT_TRUE(index.range(queries[i], &hits).ok()) << describe(queries[i]);
                    if (reference.size() <= i) {
                        reference.push_back(hits);
                        // Anchor the first capacity to the independent oracle, or
                        // the whole law could hold over a shared wrong answer.
                        ASSERT_EQ(hits, brute_force(dataset.points, queries[i]))
                                << describe(queries[i]);
                    } else {
                        EXPECT_EQ(hits, reference[i]) << describe(queries[i]);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Law 3: every pivot partitions the doc set
// ---------------------------------------------------------------------------

TYPED_TEST(BkdPropertyTest, EveryPivotPartitionsTheAnswer) {
    constexpr FieldType kFieldType = TypeParam::kType;
    for (const uint64_t seed : kSeeds) {
        Rng rng(seed ^ (static_cast<uint64_t>(kFieldType) << 16));
        // One point per doc: the partition is then a partition of DOCS, so the
        // two halves must be disjoint and not merely cover the whole.
        const std::vector<EncodedPoint> points = random_points<kFieldType>(&rng, 500, 300, 1);
        const std::vector<Bytes> probes = probes_for<kFieldType>(&rng, points, 300);

        Index index;
        ASSERT_TRUE(index.build(points, kFieldType, 8).ok());

        roaring::Roaring everything;
        ASSERT_TRUE(index.range(RangeQuery {}, &everything).ok());
        ASSERT_EQ(everything, brute_force(points, RangeQuery {}));

        for (const Bytes& pivot : probes) {
            SCOPED_TRACE("seed=" + std::to_string(seed) + " pivot=" + to_hex(pivot));
            roaring::Roaring below;
            roaring::Roaring at_or_above;
            roaring::Roaring at;
            roaring::Roaring above;
            roaring::Roaring at_or_below;
            ASSERT_TRUE(
                    index.range(RangeQuery {Bound {}, Bound {true, pivot, false}}, &below).ok());
            ASSERT_TRUE(index.range(RangeQuery {Bound {true, pivot, true}, Bound {}}, &at_or_above)
                                .ok());
            ASSERT_TRUE(index.range(exactly(pivot), &at).ok());
            ASSERT_TRUE(
                    index.range(RangeQuery {Bound {true, pivot, false}, Bound {}}, &above).ok());
            ASSERT_TRUE(index.range(RangeQuery {Bound {}, Bound {true, pivot, true}}, &at_or_below)
                                .ok());

            // < pivot and >= pivot cover everything and overlap in nothing.
            EXPECT_EQ(below | at_or_above, everything);
            EXPECT_TRUE((below & at_or_above).isEmpty());
            // The half-open ends differ by exactly the pivot's own docs.
            EXPECT_EQ(at_or_above - above, at);
            EXPECT_EQ(at_or_below - below, at);
            EXPECT_EQ(above | at, at_or_above);
        }
    }
}

// ---------------------------------------------------------------------------
// Law 4: widening an interval only ever adds docs
// ---------------------------------------------------------------------------

TYPED_TEST(BkdPropertyTest, WideningAnIntervalOnlyAddsDocs) {
    constexpr FieldType kFieldType = TypeParam::kType;
    for (const uint64_t seed : kSeeds) {
        Rng rng(seed ^ (static_cast<uint64_t>(kFieldType) << 24));
        const std::vector<EncodedPoint> points = random_points<kFieldType>(&rng, 400, 200, 2);
        std::vector<Bytes> sorted_probes = probes_for<kFieldType>(&rng, points, 200);
        std::ranges::sort(sorted_probes);

        Index index;
        ASSERT_TRUE(index.build(points, kFieldType, 6).ok());

        // Anchor the relational laws below to the independent oracle. Every
        // assertion in the loop relates the index's own outputs to each other, so
        // an index that answered EMPTY to everything would satisfy all of them:
        // empty - empty is empty, and empty | empty | empty == empty. One
        // brute-force comparison is what makes the rest mean anything.
        {
            roaring::Roaring everything;
            ASSERT_TRUE(index.range(RangeQuery {}, &everything).ok());
            ASSERT_EQ(everything, brute_force(points, RangeQuery {}));
            ASSERT_FALSE(everything.isEmpty());
        }

        for (size_t low = 0; low < sorted_probes.size(); ++low) {
            for (size_t high = low; high < sorted_probes.size(); ++high) {
                SCOPED_TRACE("seed=" + std::to_string(seed) + " [" + to_hex(sorted_probes[low]) +
                             ", " + to_hex(sorted_probes[high]) + "]");
                const RangeQuery inner {Bound {true, sorted_probes[low], false},
                                        Bound {true, sorted_probes[high], false}};
                const RangeQuery outer {Bound {true, sorted_probes[low], true},
                                        Bound {true, sorted_probes[high], true}};
                roaring::Roaring inner_hits;
                roaring::Roaring outer_hits;
                roaring::Roaring unbounded_hits;
                ASSERT_TRUE(index.range(inner, &inner_hits).ok());
                ASSERT_TRUE(index.range(outer, &outer_hits).ok());
                ASSERT_TRUE(index.range(RangeQuery {Bound {}, outer.upper}, &unbounded_hits).ok());
                // Relaxing a bound cannot lose a doc, and dropping one entirely
                // cannot either.
                EXPECT_TRUE((inner_hits - outer_hits).isEmpty());
                EXPECT_TRUE((outer_hits - unbounded_hits).isEmpty());
                // ... and the exclusive form is the inclusive one minus exactly
                // the two endpoints' docs, which is the law an off-by-one in the
                // boundary-leaf filter breaks.
                roaring::Roaring low_hits;
                roaring::Roaring high_hits;
                ASSERT_TRUE(index.range(exactly(sorted_probes[low]), &low_hits).ok());
                ASSERT_TRUE(index.range(exactly(sorted_probes[high]), &high_hits).ok());
                EXPECT_EQ(inner_hits | low_hits | high_hits, outer_hits);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Law 5: the header's own counts describe the point set
// ---------------------------------------------------------------------------

TYPED_TEST(BkdPropertyTest, HeaderCountsAndBoundsDescribeThePointSet) {
    constexpr FieldType kFieldType = TypeParam::kType;
    const auto width = static_cast<uint32_t>(field_type_size(kFieldType));
    for (const uint64_t seed : kSeeds) {
        Rng rng(seed ^ (static_cast<uint64_t>(kFieldType) << 32));
        for (const Dataset& dataset : make_datasets<kFieldType>(&rng)) {
            SCOPED_TRACE("seed=" + std::to_string(seed) + " dataset=" + dataset.name);
            Index index;
            ASSERT_TRUE(index.build(dataset.points, kFieldType, 9).ok());
            const BkdReader& reader = index.reader();

            std::set<uint32_t> docs;
            for (const EncodedPoint& point : dataset.points) {
                docs.insert(point.doc_id);
            }
            EXPECT_EQ(reader.point_count(), dataset.points.size());
            // doc_count is counted by the builder from consecutive doc ids
            // (design 6.1), so an array column repeating one doc must not
            // inflate it.
            EXPECT_EQ(reader.doc_count(), docs.size());
            EXPECT_EQ(reader.header().bytes_per_dim, width);
            EXPECT_EQ(reader.header().field_type, kFieldType);
            EXPECT_EQ(reader.empty(), dataset.points.empty());
            EXPECT_EQ(index.stats().point_count, dataset.points.size());
            EXPECT_EQ(index.stats().leaf_count, reader.leaf_count());
            EXPECT_FALSE(index.stats().built_with_spill);

            // The unbounded query returns every doc that owns a point -- no more
            // (a NULL row never called add()) and no fewer.
            roaring::Roaring everything;
            ASSERT_TRUE(index.range(RangeQuery {}, &everything).ok());
            EXPECT_EQ(everything.cardinality(), docs.size());
            EXPECT_EQ(everything, brute_force(dataset.points, RangeQuery {}));

            ASSERT_FALSE(dataset.points.empty());
            const auto [min_point, max_point] = std::ranges::minmax_element(
                    dataset.points,
                    [](const EncodedPoint& a, const EncodedPoint& b) { return a.value < b.value; });
            ASSERT_EQ(reader.min_value().size(), width);
            EXPECT_EQ(std::memcmp(reader.min_value().data(), min_point->value.data(), width), 0);
            EXPECT_EQ(std::memcmp(reader.max_value().data(), max_point->value.data(), width), 0);
        }
    }
}

// ---------------------------------------------------------------------------
// Law 6: the emitted bytes depend on the point MULTISET, not on append order
// ---------------------------------------------------------------------------

TYPED_TEST(BkdPropertyTest, BuildIsDeterministicUnderPerDocValueOrder) {
    constexpr FieldType kFieldType = TypeParam::kType;
    for (const uint64_t seed : kSeeds) {
        Rng rng(seed ^ (static_cast<uint64_t>(kFieldType) << 40));
        // add() requires non-decreasing doc ids, so the freedom the caller
        // really has is the order of one doc's own points -- exactly what an
        // array column's element order is.
        const std::vector<EncodedPoint> points = random_points<kFieldType>(&rng, 300, 25, 4);

        std::vector<EncodedPoint> shuffled = points;
        for (size_t i = 0; i + 1 < shuffled.size(); ++i) {
            if (shuffled[i].doc_id == shuffled[i + 1].doc_id && rng.next_bool()) {
                std::swap(shuffled[i], shuffled[i + 1]);
            }
        }

        Index original;
        Index reordered;
        ASSERT_TRUE(original.build(points, kFieldType, 7).ok());
        ASSERT_TRUE(reordered.build(shuffled, kFieldType, 7).ok());
        // The sort key is (value, doc_id) and it is TOTAL over the record bytes,
        // so two builds of the same multiset are byte-identical -- which is also
        // what makes a golden digest (design 12.3) meaningful at all.
        EXPECT_EQ(original.index_bytes(), reordered.index_bytes()) << "seed=" << seed;
        EXPECT_EQ(original.data_bytes(), reordered.data_bytes()) << "seed=" << seed;
    }
}

// ---------------------------------------------------------------------------
// INV-1: the encoding must preserve the NATIVE order
// ---------------------------------------------------------------------------
//
// Every law above draws its oracle from the ENCODED bytes, so the oracle and
// the index share one notion of order: if the encoder stopped being
// order-preserving they would be wrong together and stay green. That is exactly
// the failure design 3 calls out as silent -- a self-consistent tree that
// answers ranges with garbage and reports no error.
//
// The laws below take their oracle from the native C++ values instead. `values`
// is sorted and deduplicated by native `<`, so a position in it IS the native
// rank, and neither law mentions an encoded byte except to hand it to the index.
//
// The type axis matters as much as the oracle: the width spread above is four
// SIGNED integers, which cannot distinguish "flips the sign bit" from "flips it
// only when it should". The spread here crosses signed / unsigned / floating at
// 1, 2, 4, 8 and 16 bytes, which is where a KeyCoder mistake would actually hide.

template <FieldType FT>
std::vector<typename CppTypeTraits<FT>::CppType> native_spread() {
    using CppType = typename CppTypeTraits<FT>::CppType;
    std::vector<CppType> values;
    if constexpr (std::is_floating_point_v<CppType>) {
        // Negative zero is deliberate: it must not sort strictly below +0.0 nor
        // produce a second distinct encoding of the same value.
        for (const double v :
             {-3.5e30, -1.5, -1.0, -0.5, -1e-30, -0.0, 0.0, 1e-30, 0.5, 1.0, 1.5, 3.5e30}) {
            values.push_back(static_cast<CppType>(v));
        }
        values.push_back(std::numeric_limits<CppType>::lowest());
        values.push_back(std::numeric_limits<CppType>::max());
    } else {
        using UnsignedType = typename CppTypeTraits<FT>::UnsignedCppType;
        constexpr size_t kBits = sizeof(CppType) * 8;
        constexpr bool kIsSigned = std::is_signed_v<CppType>;
        const CppType lo = kIsSigned ? static_cast<CppType>(UnsignedType(1) << (kBits - 1))
                                     : static_cast<CppType>(0);
        const CppType hi = kIsSigned ? static_cast<CppType>(~(UnsignedType(1) << (kBits - 1)))
                                     : static_cast<CppType>(~static_cast<UnsignedType>(0));
        values.push_back(lo);
        values.push_back(static_cast<CppType>(lo + 1));
        values.push_back(hi);
        values.push_back(static_cast<CppType>(hi - 1));
        // Straddle zero: for a signed type this is where the sign-bit flip earns
        // its keep, and for an unsigned one it is where flipping would break it.
        for (const int64_t rank : {-3, -2, -1, 0, 1, 2, 3}) {
            if (!kIsSigned && rank < 0) {
                continue;
            }
            values.push_back(static_cast<CppType>(rank));
        }
        // Byte-order stress. Small values near zero are not enough: on a little
        // endian host their low byte happens to order the same way the value
        // does, so an encoder that forgot to_endian<big> would still look
        // correct. 2^(8k) and its predecessor differ in a HIGH byte while their
        // low bytes run the other way, which is precisely the pair a raw
        // little endian copy inverts.
        for (size_t k = 1; k < sizeof(CppType); ++k) {
            const UnsignedType step = static_cast<UnsignedType>(UnsignedType(1) << (8 * k));
            values.push_back(static_cast<CppType>(step));
            values.push_back(static_cast<CppType>(step - 1));
        }
    }
    std::sort(values.begin(), values.end());
    values.erase(std::unique(values.begin(), values.end()), values.end());
    return values;
}

int sign_of(int value) {
    return (value > 0) - (value < 0);
}

template <typename Tag>
class BkdNativeOrderTest : public ::testing::Test {};

// Signed / unsigned / floating at 1, 2, 4, 8 and 16 bytes. Types whose CppType
// is a composite (DECIMAL's decimal12_t, DATE's uint24_t, DECIMAL256's Int256)
// are covered by the differential and reader suites but are not constructible
// from an integer rank here, so they stay out of this spread.
using NativeOrderSpread = ::testing::Types<
        FieldTag<FieldType::OLAP_FIELD_TYPE_BOOL>, FieldTag<FieldType::OLAP_FIELD_TYPE_TINYINT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_SMALLINT>, FieldTag<FieldType::OLAP_FIELD_TYPE_INT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_BIGINT>, FieldTag<FieldType::OLAP_FIELD_TYPE_LARGEINT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_FLOAT>, FieldTag<FieldType::OLAP_FIELD_TYPE_DOUBLE>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL32>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL64>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DATEV2>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>, FieldTag<FieldType::OLAP_FIELD_TYPE_IPV4>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_IPV6>>;
TYPED_TEST_SUITE(BkdNativeOrderTest, NativeOrderSpread);

// INV-1 stated directly, with no index in the way: unsigned MSB-first byte order
// over the encoding must reproduce the native total order, for every ordered
// pair. This is the precondition BKD never checks and cannot detect -- every
// comparison it makes is a memcmp over these bytes.
TYPED_TEST(BkdNativeOrderTest, EncodingPreservesNativeOrder) {
    constexpr FieldType kFieldType = TypeParam::kType;
    const auto values = native_spread<kFieldType>();
    ASSERT_GE(values.size(), 4U);

    for (size_t i = 0; i < values.size(); ++i) {
        const Bytes a = encode<kFieldType>(values[i]);
        for (size_t j = 0; j < values.size(); ++j) {
            const Bytes b = encode<kFieldType>(values[j]);
            ASSERT_EQ(a.size(), b.size());
            // `values` is sorted and deduplicated by native `<`, so the
            // positions are the native ranks and no 128-bit or floating
            // comparison has to be reproduced in the assertion itself.
            const int expected = (i < j) ? -1 : ((i > j) ? 1 : 0);
            EXPECT_EQ(sign_of(std::memcmp(a.data(), b.data(), a.size())), expected)
                    << "field_type=" << static_cast<int>(kFieldType) << " rank " << i << " vs " << j
                    << " (" << to_hex(a) << " vs " << to_hex(b) << ")";
        }
    }
}

// The same oracle carried through the whole index. A range over native ranks
// [lo, hi] must come back as exactly the docs at those ranks -- the expected set
// is built from positions, so it stays correct even if the encoding does not.
TYPED_TEST(BkdNativeOrderTest, RangeAnswersFollowNativeOrder) {
    constexpr FieldType kFieldType = TypeParam::kType;
    const auto values = native_spread<kFieldType>();
    ASSERT_GE(values.size(), 4U);

    std::vector<EncodedPoint> points;
    for (size_t i = 0; i < values.size(); ++i) {
        points.push_back(EncodedPoint {static_cast<uint32_t>(i), encode<kFieldType>(values[i])});
    }

    for (const uint32_t points_per_leaf : {1U, 2U, 5U, 1024U}) {
        Index index;
        ASSERT_TRUE(index.build(points, kFieldType, points_per_leaf).ok());
        ASSERT_FALSE(::testing::Test::HasFatalFailure());

        for (size_t lo = 0; lo < values.size(); ++lo) {
            for (size_t hi = lo; hi < values.size(); ++hi) {
                RangeQuery query;
                query.lower = Bound {true, points[lo].value, true};
                query.upper = Bound {true, points[hi].value, true};

                roaring::Roaring expected;
                for (size_t k = lo; k <= hi; ++k) {
                    expected.add(static_cast<uint32_t>(k));
                }
                roaring::Roaring hits;
                ASSERT_TRUE(index.range(query, &hits).ok());
                EXPECT_TRUE(hits == expected)
                        << "field_type=" << static_cast<int>(kFieldType)
                        << " leaf=" << points_per_leaf << " ranks [" << lo << ", " << hi << "]";
            }
        }
    }
}

// Proof that the two laws above are not vacuous. A raw memcpy of the native
// value is what an encoder looks like when the sign flip and the byte swap are
// both missing; it must break EncodingPreservesNativeOrder on every type where
// either of those steps matters. If this test ever stops finding an inversion,
// the laws above have lost their teeth and the suite is lying.
TEST(BkdNativeOrderControl, ARawMemcpyEncoderIsDetectedAsOrderBreaking) {
    const auto inversions = [](auto tag) {
        constexpr FieldType kFieldType = decltype(tag)::kType;
        const auto values = native_spread<kFieldType>();
        size_t found = 0;
        for (size_t i = 0; i < values.size(); ++i) {
            for (size_t j = i + 1; j < values.size(); ++j) {
                Bytes a(sizeof(values[i]), '\0');
                Bytes b(sizeof(values[j]), '\0');
                std::memcpy(a.data(), &values[i], a.size());
                std::memcpy(b.data(), &values[j], b.size());
                if (std::memcmp(a.data(), b.data(), a.size()) >= 0) {
                    ++found; // i < j natively, yet the bytes do not say so
                }
            }
        }
        return found;
    };

    // Signed: the sign bit alone inverts negatives against positives.
    EXPECT_GT(inversions(FieldTag<FieldType::OLAP_FIELD_TYPE_INT> {}), 0U);
    EXPECT_GT(inversions(FieldTag<FieldType::OLAP_FIELD_TYPE_LARGEINT> {}), 0U);
    // Floating: sign bit plus the magnitude ordering of the bit pattern.
    EXPECT_GT(inversions(FieldTag<FieldType::OLAP_FIELD_TYPE_FLOAT> {}), 0U);
    EXPECT_GT(inversions(FieldTag<FieldType::OLAP_FIELD_TYPE_DOUBLE> {}), 0U);
    // Unsigned and multi-byte: no sign bit to flip, but the host is little
    // endian, so the byte swap alone is load-bearing.
    EXPECT_GT(inversions(FieldTag<FieldType::OLAP_FIELD_TYPE_DATETIMEV2> {}), 0U);
    EXPECT_GT(inversions(FieldTag<FieldType::OLAP_FIELD_TYPE_IPV6> {}), 0U);
}

} // namespace
} // namespace doris::snii::bkd
