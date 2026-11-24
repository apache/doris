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

#include "storage/sampling/key_range_block_sampler.h"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

#include "common/consts.h"
#include "common/status.h"
#include "storage/index/short_key_index.h"
#include "storage/sampling/tablet_sample_plan.h"
#include "storage/segment/segment_key_seeker.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
namespace {
using TestKey = std::pair<std::string, std::string>;
std::vector<std::string> fields(const TestKey& key) {
    return {std::string(1, KeyConsts::KEY_NORMAL_MARKER) + key.first,
            std::string(1, KeyConsts::KEY_NORMAL_MARKER) + key.second};
}

// Real block index, independent sorted row vector in place of the two VARCHAR column readers.
class TestSegmentKeys final : public SegmentKeySeeker {
public:
    TestSegmentKeys(std::vector<TestKey> rows, uint32_t stride) : _rows(std::move(rows)) {
        ShortKeyIndexBuilder builder(0, stride);
        for (size_t i = 0; i < _rows.size(); i += stride) {
            EXPECT_TRUE(builder.add_item(_rows[i].first.substr(0, 20)).ok());
        }
        std::vector<Slice> slices;
        segment_v2::PageFooterPB footer;
        EXPECT_TRUE(builder.finalize(_rows.size(), &slices, &footer).ok());
        for (const auto& slice : slices) {
            _body.append(slice.data, slice.size);
        }
        EXPECT_TRUE(_index.parse(_body, footer.short_key_page_footer()).ok());
    }
    const ShortKeyIndexDecoder& index() const override { return _index; }
    Status read_prefix(uint32_t pos, std::string* out) override {
        ++_prefix_reads;
        *out = _rows[pos].first.substr(0, 20);
        return Status::OK();
    }
    Status read_tuple(uint32_t pos, std::vector<std::string>* out, size_t* bytes) override {
        ++_tuple_reads;
        *out = fields(_rows[pos]);
        *bytes = 2 * sizeof(std::string) + (*out)[0].size() + (*out)[1].size();
        return Status::OK();
    }
    size_t total_reads() const { return _prefix_reads + _tuple_reads; }
    void reset_reads() {
        _prefix_reads = 0;
        _tuple_reads = 0;
    }

private:
    std::vector<TestKey> _rows;
    std::string _body;
    ShortKeyIndexDecoder _index;
    size_t _prefix_reads = 0;
    size_t _tuple_reads = 0;
};

bool selected(const segment_v2::RowRanges& ranges, uint32_t pos) {
    for (size_t i = 0; i < ranges.range_size(); ++i) {
        if (ranges.get_range_from(i) <= pos && pos < ranges.get_range_to(i)) {
            return true;
        }
    }
    return false;
}

TEST(SegmentKeySeekerTest, TailAndCompositeTupleOrdering) {
    std::vector<TestKey> rows(150, {"a", "z"});
    rows.resize(200, {"a\x01", "a"});
    TestSegmentKeys segment(rows, 1024);
    uint32_t pos;
    ASSERT_TRUE(segment.lower_bound("a\x01", {}, 200, &pos).ok());
    EXPECT_EQ(150, pos);
    ASSERT_TRUE(segment.lower_bound("a\x01", fields({"a\x01", "a"}), 200, &pos).ok());
    EXPECT_EQ(150, pos);
    ASSERT_TRUE(segment.lower_bound("b", {}, 200, &pos).ok());
    EXPECT_EQ(200, pos);
}

TEST(KeyRangeBlockSamplerTest, SparseIdsHotVersionsAndEqualKeyMarginals) {
    const std::string prefix(20, 'x');
    TestSegmentKeys hot(std::vector<TestKey>(32, {prefix + "a", "z"}), 4);
    std::vector<TestKey> delta(4, {prefix + "a", "z"});
    delta.resize(8, {prefix + "a\x01", "a"});
    delta.resize(12, {prefix + "b", ""});
    delta.resize(16, {prefix + "c", ""});
    TestSegmentKeys later(delta, 4);
    RowsetId first, second;
    first.init(10);
    second.init(11);
    std::vector<SampleSegment> segments {
            {first, 0, 0}, {first, 17, 32, &hot}, {second, 85, 16, &later}};
    std::array<int, 4> hits {};
    for (int seed = 0; seed < 2048; ++seed) {
        TabletSamplePlan plan;
        ASSERT_TRUE(KeyRangeBlockSampler(0.375, seed, 4, 4).build(71, segments, &plan).ok());
        ASSERT_EQ(4, plan.population_units());
        ASSERT_TRUE(plan.segments()[0].is_empty());
        for (uint32_t k = 0; k < 4; ++k) {
            bool chosen = selected(plan.segments()[2], k * 4);
            for (uint32_t r = k * 4; r < (k + 1) * 4; ++r) {
                ASSERT_EQ(chosen, selected(plan.segments()[2], r));
            }
            if (k == 0) {
                ASSERT_EQ(chosen ? 32 : 0, plan.segments()[1].count());
            }
            hits[k] += chosen;
        }
    }
    for (int count : hits) {
        EXPECT_NEAR(0.375, count / 2048.0, 0.04);
    }
    TabletSamplePlan plan;
    auto st = KeyRangeBlockSampler(1, 9, 4, 1).build(71, segments, &plan);
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_NE(std::string::npos,
              st.to_string().find("population_units=4 selected_units=4 exceed cap=1"));
}

TEST(KeyRangeBlockSamplerTest, EmptyBaseAndDisjointMonotonicDeltas) {
    RowsetId id;
    id.init(17);
    TestSegmentKeys first({{"a", ""}, {"b", ""}, {"c", ""}}, 2);
    TestSegmentKeys later({{"d", ""}, {"e", ""}, {"f", ""}}, 2);
    std::vector<SampleSegment> segments {{id, 0, 0}, {id, 4, 3, &first}, {id, 11, 3, &later}};
    TabletSamplePlan plan;
    ASSERT_TRUE(KeyRangeBlockSampler(1, 1, 2, 3).build(7, segments, &plan).ok());
    EXPECT_EQ(6, plan.selected_rows());
    EXPECT_TRUE(plan.segments()[0].is_empty());
    EXPECT_EQ(3, plan.segments()[1].count());
    EXPECT_EQ(3, plan.segments()[2].count());
}

TEST(KeyRangeBlockSamplerTest, SubdividesCoarseShortKeyBlocksWithInterleavedDeltas) {
    auto key_str = [](int k) {
        char buf[16];
        snprintf(buf, sizeof(buf), "%08d", k);
        return std::string(buf);
    };
    std::vector<TestKey> base_rows;
    std::vector<TestKey> delta_rows;
    for (int i = 0; i < 2048; ++i) {
        base_rows.emplace_back(key_str(i), "");
    }
    for (int i = 500; i < 1524; ++i) {
        delta_rows.emplace_back(key_str(i), "");
    }
    TestSegmentKeys base(base_rows, 1024);
    TestSegmentKeys delta(delta_rows, 1024);
    RowsetId r1, r2;
    r1.init(21);
    r2.init(22);
    std::vector<SampleSegment> segments {{r1, 0, 2048, &base}, {r2, 0, 1024, &delta}};
    TabletSamplePlan plan;
    ASSERT_TRUE(KeyRangeBlockSampler(0.0625, 42, 8, 512).build(99, segments, &plan).ok());
    EXPECT_GT(plan.population_units(), 200U);
    EXPECT_GT(plan.selected_units(), 10U);
    for (int k = 500; k < 1524; ++k) {
        ASSERT_EQ(selected(plan.segments()[0], k), selected(plan.segments()[1], k - 500));
    }
}

TEST(SegmentKeySeekerTest, SchemaEncodingCompatibility) {
    auto schema = [](int length, int width, int uid) {
        auto s = std::make_shared<TabletSchema>();
        s->_keys_type = AGG_KEYS;
        TabletColumn c(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                       FieldType::OLAP_FIELD_TYPE_VARCHAR, true);
        c.set_unique_id(uid);
        c.set_is_key(true);
        c.set_length(length);
        c.set_index_length(width);
        s->append_column(c);
        s->_num_short_key_columns = 1;
        return s;
    };
    auto current = schema(100, 20, 1);
    EXPECT_TRUE(SegmentKeySeeker::validate_schema(*current, *schema(30, 20, 1)).ok());
    EXPECT_FALSE(SegmentKeySeeker::validate_schema(*current, *schema(100, 10, 1)).ok());
    EXPECT_FALSE(SegmentKeySeeker::validate_schema(*current, *schema(100, 20, 3)).ok());
    auto old = schema(30, 20, 1);
    old->_sort_type = SortType::ZORDER;
    EXPECT_FALSE(SegmentKeySeeker::validate_schema(*current, *old).ok());
    old->_sort_type = SortType::LEXICAL;
    old->_cluster_key_uids = {1};
    EXPECT_FALSE(SegmentKeySeeker::validate_schema(*current, *old).ok());
}

std::string format_key(const char* fmt, int k) {
    char buf[16];
    snprintf(buf, sizeof(buf), fmt, k);
    return {buf};
}

void append_uniform_segments(int count, int rows_per_seg, int base_key, int key_step,
                             int rowset_base, const std::string& val,
                             std::vector<std::unique_ptr<TestSegmentKeys>>* storage,
                             std::vector<SampleSegment>* segments) {
    for (int s = 0; s < count; ++s) {
        std::vector<TestKey> rows;
        rows.reserve(rows_per_seg);
        for (int i = 0; i < rows_per_seg; ++i) {
            rows.emplace_back(format_key("%08d", base_key + i * key_step), val);
        }
        storage->push_back(std::make_unique<TestSegmentKeys>(std::move(rows), 1024));
        RowsetId rid;
        rid.init(rowset_base + s);
        segments->push_back({.rowset_id = rid,
                             .physical_id = 0,
                             .rows = static_cast<uint32_t>(rows_per_seg),
                             .keys = storage->back().get()});
    }
}

void record_bucket_hits(const TabletSamplePlan& p0, const TabletSamplePlan& p1,
                        const TabletSamplePlan& p2, std::array<int, 64>* hits) {
    for (int k = 0; k < 64; ++k) {
        const bool in_b2 = selected(p2.segments()[0], k);
        if (k < 60 && k % 2 == 0) {
            EXPECT_EQ(in_b2, selected(p0.segments()[0], k / 2));
        }
        if (k < 60 && k % 3 == 0) {
            EXPECT_EQ(in_b2, selected(p1.segments()[0], k / 3));
        }
        (*hits)[k] += in_b2 ? 1 : 0;
    }
}

TEST(KeyRangeBlockSamplerTest, ThousandOverlappingSegmentsBoundedSeeks) {
    // 500 overlapping segments sharing the exact same key(0) = "00000000", plus 250 early
    // segments before [1000..9000] and 250 late segments after [900000..900500].
    std::vector<std::unique_ptr<TestSegmentKeys>> storage;
    std::vector<SampleSegment> segments;
    storage.reserve(1000);
    segments.reserve(1000);
    append_uniform_segments(500, 200, 0, 50, 1000, "v", &storage, &segments);
    append_uniform_segments(250, 64, 0, 1, 2000, "e", &storage, &segments);
    append_uniform_segments(250, 64, 900000, 1, 3000, "l", &storage, &segments);

    KeyRangeSamplePlanSpec spec;
    KeyRangeBlockSampler sampler(0.1, 77, 1024, 512);
    ASSERT_TRUE(sampler.sample_intervals(501, segments, false, &spec).ok());
    EXPECT_GT(spec.population_units, 80U);
    EXPECT_GT(spec.selected_units, 5U);

    // Subdivision probes at most kMaxSubdivisionHeads (16) per shared prefix instead of all 500.
    size_t unprobed_overlapping = 0;
    for (int s = 0; s < 500; ++s) {
        unprobed_overlapping += storage[s]->total_reads() == 0 ? 1U : 0U;
    }
    EXPECT_GE(unprobed_overlapping, 480U);
    for (auto& seg : storage) {
        seg->reset_reads();
    }

    TabletSamplePlan plan;
    ASSERT_TRUE(sampler.locate_intervals(501, segments, spec, &plan).ok());
    const bool all_above_early = !spec.intervals.empty() &&
                                 spec.intervals.front().lower.has_value() &&
                                 spec.intervals.front().lower->prefix > format_key("%08d", 64);
    for (int s = 500; all_above_early && s < 750; ++s) {
        EXPECT_LE(storage[s]->total_reads(), 8U);
    }
    const bool all_below_late = !spec.intervals.empty() &&
                                spec.intervals.back().upper.has_value() &&
                                spec.intervals.back().upper->prefix < format_key("%08d", 900000);
    for (int s = 750; all_below_late && s < 1000; ++s) {
        EXPECT_EQ(0U, storage[s]->total_reads());
    }
}

TEST(KeyRangeBlockSamplerTest, SharedIntervalsCoordinateRandomBuckets) {
    // 3 buckets in a RANDOM AGG partition holding scattered partial rows for keys 0..63,
    // including keys present only in bucket 2 (60..63) and absent from bucket 0 (reference).
    std::vector<TestKey> b0_rows, b1_rows, b2_rows;
    for (int k = 0; k < 60; ++k) {
        if (k % 2 == 0) {
            b0_rows.emplace_back(format_key("k_%04d", k), "a");
        }
        if (k % 3 == 0) {
            b1_rows.emplace_back(format_key("k_%04d", k), "a");
        }
    }
    for (int k = 0; k < 64; ++k) {
        b2_rows.emplace_back(format_key("k_%04d", k), "a");
    }
    TestSegmentKeys seg0(b0_rows, 8);
    TestSegmentKeys seg1(b1_rows, 8);
    TestSegmentKeys seg2(b2_rows, 8);
    RowsetId r0, r1, r2;
    r0.init(1);
    r1.init(2);
    r2.init(3);
    std::vector<SampleSegment> s0 {{.rowset_id = r0,
                                    .physical_id = 0,
                                    .rows = static_cast<uint32_t>(b0_rows.size()),
                                    .keys = &seg0}};
    std::vector<SampleSegment> s1 {{.rowset_id = r1,
                                    .physical_id = 0,
                                    .rows = static_cast<uint32_t>(b1_rows.size()),
                                    .keys = &seg1}};
    std::vector<SampleSegment> s2 {{.rowset_id = r2,
                                    .physical_id = 0,
                                    .rows = static_cast<uint32_t>(b2_rows.size()),
                                    .keys = &seg2}};

    std::array<int, 64> hits {};
    for (int seed = 0; seed < 1024; ++seed) {
        KeyRangeSamplePlanSpec shared;
        ASSERT_TRUE(KeyRangeBlockSampler(0.25, seed, 6, 32)
                            .sample_intervals(9001, s0, true, &shared)
                            .ok());
        KeyRangeBlockSampler locator(0.25, seed, 6, 32, shared);
        TabletSamplePlan p0, p1, p2;
        ASSERT_TRUE(locator.build(101, s0, &p0).ok());
        ASSERT_TRUE(locator.build(102, s1, &p1).ok());
        ASSERT_TRUE(locator.build(103, s2, &p2).ok());
        record_bucket_hits(p0, p1, p2, &hits);
    }
    for (int count : hits) {
        EXPECT_NEAR(0.25, count / 1024.0, 0.05);
    }
}

TEST(KeyRangeBlockSamplerTest, EmptyReferenceBucketDrawsUniversalInterval) {
    std::vector<TestKey> b2_rows;
    for (int k = 0; k < 32; ++k) {
        b2_rows.emplace_back(format_key("k_%04d", k), "a");
    }
    TestSegmentKeys seg2(b2_rows, 8);
    RowsetId r0, r2;
    r0.init(1);
    r2.init(3);
    std::vector<SampleSegment> empty_ref {
            {.rowset_id = r0, .physical_id = 0, .rows = 0, .keys = nullptr}};
    std::vector<SampleSegment> s2 {{.rowset_id = r2,
                                    .physical_id = 0,
                                    .rows = static_cast<uint32_t>(b2_rows.size()),
                                    .keys = &seg2}};
    int empty_ref_hits = 0;
    for (int seed = 0; seed < 1024; ++seed) {
        KeyRangeSamplePlanSpec shared;
        ASSERT_TRUE(KeyRangeBlockSampler(0.4, seed, 6, 32)
                            .sample_intervals(9002, empty_ref, true, &shared)
                            .ok());
        EXPECT_EQ(1U, shared.population_units);
        TabletSamplePlan p2;
        ASSERT_TRUE(KeyRangeBlockSampler(0.4, seed, 6, 32, shared).build(103, s2, &p2).ok());
        ASSERT_TRUE(p2.selected_rows() == 0 || p2.selected_rows() == b2_rows.size());
        empty_ref_hits += p2.selected_rows() == b2_rows.size() ? 1 : 0;
    }
    EXPECT_NEAR(0.4, empty_ref_hits / 1024.0, 0.05);
}

TEST(KeyRangeBlockSamplerTest, FiftySegmentsCoarseVarcharPrefixCoversAllRegions) {
    // Reproduces 50 uncompacted segments on composite key (region VARCHAR, k BIGINT) where only
    // `region` (4 values) is in the short-key index and each region spans 12 blocks per segment.
    constexpr std::array<const char*, 4> regions {"EAST", "NORTH", "SOUTH", "WEST"};
    std::vector<std::vector<TestKey>> raw(50);
    std::vector<std::unique_ptr<TestSegmentKeys>> storage;
    std::vector<SampleSegment> segments;
    for (int s = 0; s < 50; ++s) {
        for (const char* reg : regions) {
            for (int i = 0; i < 96; ++i) {
                raw[s].emplace_back(reg, format_key("%06d", s * 100 + i));
            }
        }
        storage.push_back(std::make_unique<TestSegmentKeys>(raw[s], 8));
        RowsetId rid;
        rid.init(400 + s);
        segments.push_back({.rowset_id = rid,
                            .physical_id = 0,
                            .rows = static_cast<uint32_t>(raw[s].size()),
                            .keys = storage.back().get()});
    }

    std::array<int, 4> region_hits {};
    for (int seed = 0; seed < 256; ++seed) {
        KeyRangeSamplePlanSpec spec;
        ASSERT_TRUE(KeyRangeBlockSampler(0.1, seed, 128, 256)
                            .sample_intervals(9003, segments, false, &spec)
                            .ok());
        TabletSamplePlan plan;
        ASSERT_TRUE(
                KeyRangeBlockSampler(0.1, seed, 128, 256, spec).build(9003, segments, &plan).ok());
        // Track whether region r's midpoint row in segment 0 was sampled.
        for (size_t r = 0; r < regions.size(); ++r) {
            const auto probe_row = static_cast<uint32_t>(r * 96 + 48);
            if (selected(plan.segments()[0], probe_row)) {
                ++region_hits[r];
            }
        }
    }
    for (int count : region_hits) {
        EXPECT_NEAR(0.1, count / 256.0, 0.05);
    }
}

TEST(KeyRangeBlockSamplerTest, CollectWeightedCutsCoversDisjointBuckets) {
    std::vector<TestKey> b0_rows, b1_rows;
    for (int k = 0; k < 200; ++k) {
        b0_rows.emplace_back(format_key("k_%04d", k), "a");
        b1_rows.emplace_back(format_key("k_%04d", 200 + k), "a");
    }
    TestSegmentKeys seg0(b0_rows, 16);
    TestSegmentKeys seg1(b1_rows, 16);
    RowsetId r0, r1;
    r0.init(10);
    r1.init(11);
    std::vector<SampleSegment> s0 {{.rowset_id = r0,
                                    .physical_id = 0,
                                    .rows = static_cast<uint32_t>(b0_rows.size()),
                                    .keys = &seg0}};
    std::vector<SampleSegment> s1 {{.rowset_id = r1,
                                    .physical_id = 0,
                                    .rows = static_cast<uint32_t>(b1_rows.size()),
                                    .keys = &seg1}};
    std::vector<WeightedKeyRangeCut> cuts0, cuts1;
    uint64_t rows0 = 0, rows1 = 0;
    ASSERT_TRUE(KeyRangeBlockSampler::collect_weighted_cuts(s0, 25, 64, &cuts0, &rows0).ok());
    ASSERT_TRUE(KeyRangeBlockSampler::collect_weighted_cuts(s1, 25, 64, &cuts1, &rows1).ok());
    EXPECT_EQ(200U, rows0);
    EXPECT_EQ(200U, rows1);
    EXPECT_GE(cuts0.size(), 6U);
    EXPECT_GE(cuts1.size(), 6U);
    EXPECT_LT(cuts0.back().bound.prefix, cuts1.front().bound.prefix);
}

} // namespace
} // namespace doris
