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

#include "common/status.h"
#include "core/field.h"
#include "storage/index/short_key_index.h"
#include "storage/sampling/tablet_sample_plan.h"
#include "storage/segment/segment_key_seeker.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
namespace {
using TestKey = std::pair<std::string, std::string>;
std::vector<Field> fields(const TestKey& key) {
    return {Field::create_field<TYPE_STRING>(key.first),
            Field::create_field<TYPE_STRING>(key.second)};
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
        *out = _rows[pos].first.substr(0, 20);
        return Status::OK();
    }
    Status read_tuple(uint32_t pos, std::vector<Field>* out, size_t* bytes) override {
        *out = fields(_rows[pos]);
        *bytes = 2 * sizeof(Field) + _rows[pos].first.size() + _rows[pos].second.size();
        return Status::OK();
    }

private:
    std::vector<TestKey> _rows;
    std::string _body;
    ShortKeyIndexDecoder _index;
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

} // namespace
} // namespace doris
