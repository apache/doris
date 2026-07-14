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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "cloud/cloud_storage_engine.h"
#include "gtest/gtest_pred_impl.h"
#include "json2pb/json_to_pb.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

static constexpr int64_t kMiB = 1024L * 1024;
static constexpr int64_t kGiB = 1024L * kMiB;

class TestCloudSizeBasedCumulativeCompactionPolicy : public testing::Test {
public:
    TestCloudSizeBasedCumulativeCompactionPolicy() : _engine(CloudStorageEngine({})) {}

    void SetUp() {
        config::compaction_promotion_size_mbytes = 1024;
        config::compaction_promotion_ratio = 0.05;
        config::compaction_promotion_min_size_mbytes = 64;
        config::compaction_min_size_mbytes = 64;

        _tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                          UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                          TCompressionType::LZ4F));

        _json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 15673,
            "txn_id": 4042,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 2,
            "end_version": 2,
            "num_rows": 3929,
            "total_disk_size": 41,
            "data_disk_size": 41,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670,
            "num_segments": 3
        })";
    }
    void TearDown() {}

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end) {
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(_json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
        pb1->set_total_disk_size(41);
        pb1->set_tablet_schema(_tablet_meta->tablet_schema());
    }

    void init_rs_meta_small_base(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 0);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 1, 1);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 2, 2);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 3, 3);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 4, 4);
        rs_metas->push_back(ptr5);
    }

protected:
    std::string _json_rowset_meta;
    TabletMetaSharedPtr _tablet_meta;

private:
    CloudStorageEngine _engine;
};

static RowsetSharedPtr create_rowset(Version version, int num_segments, bool overlapping,
                                     int64_t data_size) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_rowset_type(BETA_ROWSET); // important
    rs_meta->_rowset_meta_pb.set_start_version(version.first);
    rs_meta->_rowset_meta_pb.set_end_version(version.second);
    rs_meta->set_num_segments(num_segments);
    rs_meta->set_segments_overlap(overlapping ? OVERLAPPING : NONOVERLAPPING);
    rs_meta->set_total_disk_size(data_size);
    RowsetSharedPtr rowset;
    Status st = RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset);
    if (!st.ok()) {
        return nullptr;
    }
    return rowset;
}

static int64_t total_disk_size(const std::vector<RowsetSharedPtr>& rowsets) {
    int64_t total_size = 0;
    for (const auto& rowset : rowsets) {
        total_size += rowset->total_disk_size();
    }
    return total_size;
}

TEST_F(TestCloudSizeBasedCumulativeCompactionPolicy, new_cumulative_point) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    CloudTablet _tablet(_engine, _tablet_meta);
    for (auto& rs_meta : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rs_meta));
    }
    _tablet._tablet_meta->_enable_unique_key_merge_on_write = true;
    _tablet._base_size = 100;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    RowsetSharedPtr output_rowset = create_rowset(Version(3, 5), 5, false, 100 * 1024 * 1024);
    Version version(1, 1);
    EXPECT_EQ(policy.new_cumulative_point(&_tablet, output_rowset, version, 2), 6);
}
TEST_F(TestCloudSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_large_head_not_repeated_when_output_below_promotion) {
    CloudTablet _tablet(_engine, _tablet_meta);
    _tablet._base_size = 20L * kGiB;

    std::vector<RowsetSharedPtr> candidate_rowsets;
    auto large_head = create_rowset(Version(2, 2), 1, false, 1023L * kMiB);
    candidate_rowsets.push_back(large_head);
    for (int i = 0; i < 20; i++) {
        candidate_rowsets.push_back(create_rowset(Version(i + 3, i + 3), 1, true, kMiB));
    }
    ASSERT_GT(total_disk_size(candidate_rowsets), kGiB);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    policy.pick_input_rowsets(&_tablet, candidate_rowsets, 100, 5, &input_rowsets,
                              &last_delete_version, &compaction_score, true);

    EXPECT_EQ(20, input_rowsets.size());
    EXPECT_EQ(20, compaction_score);
    EXPECT_EQ(3, input_rowsets.front()->start_version());
    EXPECT_EQ(22, input_rowsets.back()->end_version());
    EXPECT_LT(total_disk_size(input_rowsets), kGiB);

    auto output_rowset = create_rowset(Version(3, 22), 1, false, 20L * kMiB);
    EXPECT_EQ(2, policy.new_cumulative_point(&_tablet, output_rowset, last_delete_version, 2));

    std::vector<RowsetSharedPtr> next_candidate_rowsets {large_head, output_rowset};
    input_rowsets.clear();
    compaction_score = 0;
    policy.pick_input_rowsets(&_tablet, next_candidate_rowsets, 100, 5, &input_rowsets,
                              &last_delete_version, &compaction_score, true);

    EXPECT_TRUE(input_rowsets.empty());
    EXPECT_EQ(0, compaction_score);
}

TEST_F(TestCloudSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_large_head_single_overlapping_tail_selected) {
    CloudTablet _tablet(_engine, _tablet_meta);
    _tablet._base_size = 20L * kGiB;

    std::vector<RowsetSharedPtr> candidate_rowsets {
            create_rowset(Version(2, 2), 1, false, 900L * kMiB),
            create_rowset(Version(3, 3), 5, true, 128L * kMiB)};
    ASSERT_GT(total_disk_size(candidate_rowsets), kGiB);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    policy.pick_input_rowsets(&_tablet, candidate_rowsets, 100, 5, &input_rowsets,
                              &last_delete_version, &compaction_score, true);

    ASSERT_EQ(1, input_rowsets.size());
    EXPECT_EQ(5, compaction_score);
    EXPECT_EQ(3, input_rowsets.front()->start_version());
    EXPECT_EQ(128L * kMiB, input_rowsets.front()->total_disk_size());
}

TEST_F(TestCloudSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_single_overlapping_rowset_not_trimmed_empty) {
    CloudTablet _tablet(_engine, _tablet_meta);
    _tablet._base_size = 20L * kGiB;

    std::vector<RowsetSharedPtr> candidate_rowsets {
            create_rowset(Version(2, 2), 3, true, 2L * kGiB)};

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    policy.pick_input_rowsets(&_tablet, candidate_rowsets, 100, 5, &input_rowsets,
                              &last_delete_version, &compaction_score, true);

    EXPECT_EQ(1, input_rowsets.size());
    EXPECT_EQ(3, compaction_score);
    EXPECT_EQ(2, input_rowsets.front()->start_version());
}

TEST_F(TestCloudSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_single_non_overlapping_rowset_still_skipped) {
    CloudTablet _tablet(_engine, _tablet_meta);
    _tablet._base_size = 20L * kGiB;

    std::vector<RowsetSharedPtr> candidate_rowsets {
            create_rowset(Version(2, 2), 1, false, 2L * kGiB)};

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    policy.pick_input_rowsets(&_tablet, candidate_rowsets, 100, 5, &input_rowsets,
                              &last_delete_version, &compaction_score, true);

    EXPECT_TRUE(input_rowsets.empty());
    EXPECT_EQ(0, compaction_score);
}

} // namespace doris
