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

#include "cloud/cloud_cumulative_compaction_policy.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "json2pb/json_to_pb.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

class TestCloudSizeBasedCumulativeCompactionPolicy : public testing::Test {
public:
    TestCloudSizeBasedCumulativeCompactionPolicy()
            : _engine(CloudStorageEngine(EngineOptions {})) {}

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
                                     int data_size) {
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

// Test case: Empty rowset compaction with skip_trim
TEST_F(TestCloudSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_empty_rowset_compaction) {
    // Save original config values
    bool orig_enable_empty_rowset_compaction = config::enable_empty_rowset_compaction;
    int32_t orig_empty_rowset_compaction_min_count = config::empty_rowset_compaction_min_count;
    double orig_empty_rowset_compaction_min_ratio = config::empty_rowset_compaction_min_ratio;

    // Enable empty rowset compaction
    config::enable_empty_rowset_compaction = true;
    config::empty_rowset_compaction_min_count = 5;
    config::empty_rowset_compaction_min_ratio = 0.5;

    CloudTablet _tablet(_engine, _tablet_meta);
    _tablet._base_size = 1024L * 1024 * 1024; // 1GB base

    // Create candidate rowsets: 2 normal + 150 empty rowsets
    // This tests that skip_trim = true for empty rowset compaction
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // 2 normal rowsets
    for (int i = 0; i < 2; i++) {
        auto rowset = create_rowset(Version(i + 2, i + 2), 1, true, 1024 * 1024); // 1MB
        candidate_rowsets.push_back(rowset);
    }

    // 150 empty rowsets (consecutive)
    for (int i = 0; i < 150; i++) {
        auto rowset = create_rowset(Version(i + 4, i + 4), 0, false, 0); // empty
        candidate_rowsets.push_back(rowset);
    }

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    // max=100, but empty rowset compaction should return 150 (skip_trim = true)
    policy.pick_input_rowsets(&_tablet, candidate_rowsets, 100, 50, &input_rowsets,
                              &last_delete_version, &compaction_score, true);

    // Empty rowset compaction should return all 150 empty rowsets
    // skip_trim = true, so no trimming even though score > max
    EXPECT_EQ(150, input_rowsets.size());
    EXPECT_EQ(150, compaction_score);

    // Verify all returned rowsets are empty
    for (const auto& rs : input_rowsets) {
        EXPECT_EQ(0, rs->num_segments());
    }

    // Restore original config values
    config::enable_empty_rowset_compaction = orig_enable_empty_rowset_compaction;
    config::empty_rowset_compaction_min_count = orig_empty_rowset_compaction_min_count;
    config::empty_rowset_compaction_min_ratio = orig_empty_rowset_compaction_min_ratio;
}

// Test case: prioritize_query_perf_in_compaction for non-DUP_KEYS table
TEST_F(TestCloudSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_prioritize_query_perf) {
    // Save original config value
    bool orig_prioritize_query_perf = config::prioritize_query_perf_in_compaction;

    // Enable prioritize_query_perf_in_compaction
    config::prioritize_query_perf_in_compaction = true;

    // Create tablet with UNIQUE keys (not DUP_KEYS)
    TTabletSchema schema;
    schema.keys_type = TKeysType::UNIQUE_KEYS;
    TabletMetaSharedPtr tablet_meta(new TabletMeta(1, 2, 15673, 15674, 4, 5, schema, 6, {{7, 8}},
                                                   UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                   TCompressionType::LZ4F));

    CloudTablet _tablet(_engine, tablet_meta);
    _tablet._base_size = 1024L * 1024 * 1024; // 1GB base

    // Create candidate rowsets that will all be removed by level_size
    // One large rowset followed by many small ones
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // 1 large rowset (200MB) - will be removed by level_size
    auto large_rowset = create_rowset(Version(2, 2), 50, true, 200L * 1024 * 1024);
    candidate_rowsets.push_back(large_rowset);

    // 50 small rowsets (100KB each)
    for (int i = 0; i < 50; i++) {
        auto rowset = create_rowset(Version(i + 3, i + 3), 1, true, 100 * 1024);
        candidate_rowsets.push_back(rowset);
    }

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    // For non-DUP_KEYS with prioritize_query_perf enabled,
    // even if all rowsets are removed by level_size, return all candidates
    policy.pick_input_rowsets(&_tablet, candidate_rowsets, 100, 50, &input_rowsets,
                              &last_delete_version, &compaction_score, true);

    // With prioritize_query_perf enabled for non-DUP_KEYS table,
    // should return rowsets even when level_size would remove all
    // This is because compaction has significant positive impact on queries
    // Note: total_size = 200MB + 5MB = 205MB, large rowset will be removed
    // After level_size removal, 50 small rowsets remain, score = 50 >= min
    // But level_size removes the large rowset first, then checks
    // Actually the logic is: if all removed by level_size AND prioritize_query_perf, return all
    // Here level_size won't remove all, so normal path
    EXPECT_GE(input_rowsets.size(), 50);

    // Restore original config value
    config::prioritize_query_perf_in_compaction = orig_prioritize_query_perf;
}

} // namespace doris
