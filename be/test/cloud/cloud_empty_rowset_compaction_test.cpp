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

#include "cloud/cloud_cumulative_compaction_policy.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "json2pb/json_to_pb.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

class TestCloudEmptyRowsetCompaction : public testing::Test {
public:
    TestCloudEmptyRowsetCompaction() : _current_version(0) {}

    void SetUp() {
        config::enable_empty_rowset_compaction = true;
        config::empty_rowset_compaction_min_count = 5;
        config::empty_rowset_compaction_min_ratio = 0.3;

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

        _json_empty_rowset_meta = R"({
            "rowset_id": 540082,
            "tablet_id": 15673,
            "txn_id": 4043,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 3,
            "end_version": 3,
            "num_rows": 0,
            "total_disk_size": 0,
            "data_disk_size": 0,
            "index_disk_size": 0,
            "empty": true,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765671,
            "num_segments": 0
        })";

        CloudStorageEngine engine(EngineOptions {});
        //cloud::CloudMetaMgr meta_mgr;
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                1001, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
        _tablet = std::make_shared<CloudTablet>(engine, std::make_shared<TabletMeta>(*tablet_meta));

        // Create a version 2 rowset with 1GB data size and add to tabletmeta
        RowsetMetaSharedPtr version2_meta(new RowsetMeta());
        version2_meta->set_rowset_id(RowsetId {1, 540080, 0, 0});
        version2_meta->set_tablet_id(15673);
        version2_meta->set_txn_id(4041);
        version2_meta->set_tablet_schema_hash(567997577);
        version2_meta->set_rowset_type(BETA_ROWSET);
        version2_meta->set_rowset_state(VISIBLE);
        version2_meta->set_version({2, 2});
        version2_meta->set_num_rows(1000000);
        version2_meta->set_total_disk_size(1073741824); // 1GB
        version2_meta->set_data_disk_size(1073741824);
        version2_meta->set_index_disk_size(0);
        version2_meta->set_empty(false);
        version2_meta->set_creation_time(1553765669);
        version2_meta->set_num_segments(1);
        version2_meta->set_tablet_schema(_tablet_meta->tablet_schema());

        // Add the version 2 rowset to the tablet
        RowsetSharedPtr version2_rowset;
        Status status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "",
                                                     version2_meta, &version2_rowset);
        EXPECT_TRUE(status.ok());
        {
            std::unique_lock<std::shared_mutex> lock(_tablet->get_header_lock());
            _tablet->add_rowsets({version2_rowset}, false, lock, false);
        }

        _current_version = 3; // Start from version 3 for candidate rowsets
    }
    void TearDown() {}

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end, bool empty = false) {
        RowsetMetaPB rowset_meta_pb;
        if (empty) {
            json2pb::JsonToProtoMessage(_json_empty_rowset_meta, &rowset_meta_pb);
        } else {
            json2pb::JsonToProtoMessage(_json_rowset_meta, &rowset_meta_pb);
        }
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
        pb1->set_total_disk_size(empty ? 0 : 41);
        pb1->set_tablet_schema(_tablet_meta->tablet_schema());
    }

    void init_empty_rowsets(std::vector<RowsetSharedPtr>* rowsets, int count) {
        for (int i = 0; i < count; ++i) {
            RowsetMetaSharedPtr meta(new RowsetMeta());
            // Create rowset meta directly without JSON parsing
            meta->set_rowset_id(RowsetId {1, 540082 + _current_version, 0, 0});
            meta->set_tablet_id(15673);
            meta->set_txn_id(4043 + _current_version);
            meta->set_tablet_schema_hash(567997577);
            meta->set_rowset_type(BETA_ROWSET);
            meta->set_rowset_state(VISIBLE);
            meta->set_version({_current_version, _current_version});
            meta->set_num_rows(0);
            meta->set_total_disk_size(0);
            meta->set_data_disk_size(0);
            meta->set_index_disk_size(0);
            meta->set_empty(true);
            meta->set_creation_time(1553765671 + _current_version);
            meta->set_num_segments(0);

            RowsetSharedPtr rowset;
            // Use RowsetFactory to create rowset
            Status status =
                    RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta, &rowset);
            EXPECT_TRUE(status.ok());
            rowsets->push_back(rowset);
            _current_version++;
        }
    }

    void init_normal_rowsets(std::vector<RowsetSharedPtr>* rowsets, int count) {
        for (int i = 0; i < count; ++i) {
            RowsetMetaSharedPtr meta(new RowsetMeta());
            // Create rowset meta directly without JSON parsing
            meta->set_rowset_id(RowsetId {1, 540081 + _current_version, 0, 0});
            meta->set_tablet_id(15673);
            meta->set_txn_id(4042 + _current_version);
            meta->set_tablet_schema_hash(567997577);
            meta->set_rowset_type(BETA_ROWSET);
            meta->set_rowset_state(VISIBLE);
            meta->set_version({_current_version, _current_version});
            meta->set_num_rows(3929);
            meta->set_total_disk_size(41);
            meta->set_data_disk_size(41);
            meta->set_index_disk_size(235);
            meta->set_empty(false);
            meta->set_creation_time(1553765670 + _current_version);
            meta->set_num_segments(1);

            RowsetSharedPtr rowset;
            // Use RowsetFactory to create rowset
            Status status =
                    RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta, &rowset);
            EXPECT_TRUE(status.ok());
            rowsets->push_back(rowset);
            _current_version++;
        }
    }

protected:
    std::unique_ptr<TabletMeta> _tablet_meta;
    std::string _json_rowset_meta;
    std::string _json_empty_rowset_meta;
    std::shared_ptr<CloudTablet> _tablet;
    int64_t _current_version;
};

TEST_F(TestCloudEmptyRowsetCompaction, test_empty_rowset_compaction_disabled) {
    config::enable_empty_rowset_compaction = false;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    std::vector<RowsetSharedPtr> candidate_rowsets;
    init_normal_rowsets(&candidate_rowsets, 1); // normal
    init_empty_rowsets(&candidate_rowsets, 10);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version = Version {-1, -1};
    size_t compaction_score = 0;

    policy.pick_input_rowsets(_tablet.get(), candidate_rowsets, 100, 10, &input_rowsets,
                              &last_delete_version, &compaction_score, false);

    // With strategy disabled, should not select any rowsets for empty rowset compaction
    EXPECT_EQ(input_rowsets.size(), 11);
    EXPECT_EQ(compaction_score, 11);
}

TEST_F(TestCloudEmptyRowsetCompaction, test_empty_rowset_compaction_no_consecutive_empty) {
    config::empty_rowset_compaction_min_count = 5;
    config::empty_rowset_compaction_min_ratio = 0.3;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // Create non-consecutive empty rowsets: empty, normal, empty, normal, empty, normal
    init_empty_rowsets(&candidate_rowsets, 1);  // empty
    init_normal_rowsets(&candidate_rowsets, 1); // normal
    init_empty_rowsets(&candidate_rowsets, 1);  // empty
    init_normal_rowsets(&candidate_rowsets, 1); // normal
    init_empty_rowsets(&candidate_rowsets, 1);  // empty
    init_normal_rowsets(&candidate_rowsets, 1); // normal

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version = Version {-1, -1};
    size_t compaction_score = 0;

    policy.pick_input_rowsets(_tablet.get(), candidate_rowsets, 100, 5, &input_rowsets,
                              &last_delete_version, &compaction_score, false);

    // Should not trigger empty rowset compaction since no consecutive empty rowsets
    EXPECT_EQ(input_rowsets.size(), 6);
    EXPECT_EQ(compaction_score, 6);
}

TEST_F(TestCloudEmptyRowsetCompaction, test_empty_rowset_compaction_consecutive_below_min_count) {
    config::empty_rowset_compaction_min_count = 5;
    config::empty_rowset_compaction_min_ratio = 0.3;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // Create consecutive empty rowsets but below min count
    init_empty_rowsets(&candidate_rowsets, 4);  // 4 consecutive empty rowsets
    init_normal_rowsets(&candidate_rowsets, 6); // 6 normal rowsets

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version = Version {-1, -1};
    size_t compaction_score = 0;

    policy.pick_input_rowsets(_tablet.get(), candidate_rowsets, 100, 10, &input_rowsets,
                              &last_delete_version, &compaction_score, false);

    // Should not trigger empty rowset compaction since consecutive count (4) < min count (5)
    EXPECT_EQ(input_rowsets.size(), 10);
    EXPECT_EQ(compaction_score, 10);
}

TEST_F(TestCloudEmptyRowsetCompaction, test_empty_rowset_compaction_consecutive_below_min_ratio) {
    config::empty_rowset_compaction_min_count = 5;
    config::empty_rowset_compaction_min_ratio = 0.3;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // Create consecutive empty rowsets but ratio too low
    init_empty_rowsets(&candidate_rowsets, 5);   // 5 consecutive empty rowsets
    init_normal_rowsets(&candidate_rowsets, 20); // 20 normal rowsets
    // Ratio = 5/25 = 0.2 < 0.3

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version = Version {-1, -1};
    size_t compaction_score = 0;

    policy.pick_input_rowsets(_tablet.get(), candidate_rowsets, 100, 10, &input_rowsets,
                              &last_delete_version, &compaction_score, false);

    // Should not trigger empty rowset compaction since ratio (0.2) < min ratio (0.3)
    EXPECT_EQ(input_rowsets.size(), 25);
    EXPECT_EQ(compaction_score, 25);
}

TEST_F(TestCloudEmptyRowsetCompaction, test_empty_rowset_compaction_consecutive_meets_criteria) {
    config::empty_rowset_compaction_min_count = 5;
    config::empty_rowset_compaction_min_ratio = 0.3;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // Create consecutive empty rowsets that meet criteria
    init_empty_rowsets(&candidate_rowsets, 6);  // 6 consecutive empty rowsets
    init_normal_rowsets(&candidate_rowsets, 4); // 4 normal rowsets
    // Ratio = 6/10 = 0.6 >= 0.3, count = 6 >= 5

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version = Version {-1, -1};
    size_t compaction_score = 0;

    int64_t result =
            policy.pick_input_rowsets(_tablet.get(), candidate_rowsets, 100, 10, &input_rowsets,
                                      &last_delete_version, &compaction_score, false);
    EXPECT_EQ(result, 6);

    // Should trigger empty rowset compaction since criteria are met
    EXPECT_EQ(input_rowsets.size(), 6);
    EXPECT_EQ(compaction_score, 6);

    // Check version range and continuity
    int64_t expected_start_version = 3;
    int64_t expected_end_version = 3;

    for (size_t i = 0; i < input_rowsets.size(); ++i) {
        const auto& rowset = input_rowsets[i];
        EXPECT_EQ(rowset->num_segments(), 0);
        EXPECT_FALSE(rowset->rowset_meta()->has_delete_predicate());

        // Check version continuity
        if (i == 0) {
            expected_start_version = rowset->start_version();
            expected_end_version = rowset->end_version();
        } else {
            EXPECT_EQ(expected_end_version + 1, rowset->start_version());
            expected_end_version = rowset->end_version();
        }

        // Each empty rowset should have start_version == end_version (single version)
        EXPECT_EQ(rowset->start_version(), rowset->end_version());
    }

    // Verify the overall version range spans consecutive versions
    EXPECT_EQ(expected_end_version - expected_start_version + 1, input_rowsets.size());
}

TEST_F(TestCloudEmptyRowsetCompaction, test_empty_rowset_compaction_multiple_consecutive_groups) {
    config::empty_rowset_compaction_min_count = 5;
    config::empty_rowset_compaction_min_ratio = 0.3;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // Create multiple groups of consecutive empty rowsets
    init_empty_rowsets(&candidate_rowsets, 3);  // Group 1: 3 consecutive empty
    init_normal_rowsets(&candidate_rowsets, 2); // 2 normal
    init_empty_rowsets(&candidate_rowsets, 6);  // Group 2: 6 consecutive empty (meets criteria)
    init_normal_rowsets(&candidate_rowsets, 3); // 3 normal
    init_empty_rowsets(&candidate_rowsets, 4);  // Group 3: 4 consecutive empty (below min count)

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version = Version {-1, -1};
    size_t compaction_score = 0;

    int64_t result =
            policy.pick_input_rowsets(_tablet.get(), candidate_rowsets, 100, 10, &input_rowsets,
                                      &last_delete_version, &compaction_score, false);
    EXPECT_EQ(result, 6);

    // Should trigger empty rowset compaction for the group that meets criteria
    EXPECT_EQ(input_rowsets.size(), 6);
    EXPECT_EQ(compaction_score, 6);

    // Check version range and continuity
    int64_t expected_start_version = 3;
    int64_t expected_end_version = 3;

    for (size_t i = 0; i < input_rowsets.size(); ++i) {
        const auto& rowset = input_rowsets[i];
        EXPECT_EQ(rowset->num_segments(), 0);
        EXPECT_FALSE(rowset->rowset_meta()->has_delete_predicate());

        // Check version continuity
        if (i == 0) {
            expected_start_version = rowset->start_version();
            expected_end_version = rowset->end_version();
        } else {
            EXPECT_EQ(expected_end_version + 1, rowset->start_version());
            expected_end_version = rowset->end_version();
        }

        // Each empty rowset should have start_version == end_version (single version)
        EXPECT_EQ(rowset->start_version(), rowset->end_version());
    }

    // Verify the overall version range spans consecutive versions
    EXPECT_EQ(expected_end_version - expected_start_version + 1, input_rowsets.size());
}

TEST_F(TestCloudEmptyRowsetCompaction, test_empty_rowset_compaction_version_range_validation) {
    config::empty_rowset_compaction_min_count = 3;
    config::empty_rowset_compaction_min_ratio = 0.3;

    CloudSizeBasedCumulativeCompactionPolicy policy;
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // Create consecutive empty rowsets with specific version ranges
    // Versions: 10, 11, 12 (consecutive empty rowsets)
    for (int i = 0; i < 3; ++i) {
        RowsetMetaSharedPtr meta(new RowsetMeta());
        init_rs_meta(meta, 10 + i, 10 + i, true); // start_version = end_version = 10+i
        RowsetSharedPtr rowset;
        // Use RowsetFactory to create rowset
        Status status =
                RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta, &rowset);
        EXPECT_TRUE(status.ok());
        candidate_rowsets.push_back(rowset);
    }

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version = Version {-1, -1};
    size_t compaction_score = 0;

    int64_t result =
            policy.pick_input_rowsets(_tablet.get(), candidate_rowsets, 100, 10, &input_rowsets,
                                      &last_delete_version, &compaction_score, false);
    EXPECT_EQ(result, 3);

    EXPECT_EQ(input_rowsets.size(), 3);
    EXPECT_EQ(compaction_score, 3);

    //// Verify specific version ranges
    //EXPECT_EQ(input_rowsets[0]->start_version(), 10);
    //EXPECT_EQ(input_rowsets[0]->end_version(), 10);
    //EXPECT_EQ(input_rowsets[1]->start_version(), 11);
    //EXPECT_EQ(input_rowsets[1]->end_version(), 11);
    //EXPECT_EQ(input_rowsets[2]->start_version(), 12);
    //EXPECT_EQ(input_rowsets[2]->end_version(), 12);

    //// Verify version continuity
    //EXPECT_EQ(input_rowsets[0]->end_version() + 1, input_rowsets[1]->start_version());
    //EXPECT_EQ(input_rowsets[1]->end_version() + 1, input_rowsets[2]->start_version());
}

TEST_F(TestCloudEmptyRowsetCompaction, test_find_longest_consecutive_empty_rowsets) {
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // Create test rowsets: mixed empty and non-empty with various patterns
    // Pattern: E(3-3), E(4-4), NE(5-5), E(6-6), E(7-7), E(8-8), NE(9-9), E(10-10)

    // Empty rowsets 3-4 (consecutive)
    RowsetMetaSharedPtr meta1(new RowsetMeta());
    init_rs_meta(meta1, 3, 3, true);
    RowsetSharedPtr rs1;
    Status status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta1, &rs1);
    EXPECT_TRUE(status.ok());
    candidate_rowsets.push_back(rs1);

    RowsetMetaSharedPtr meta2(new RowsetMeta());
    init_rs_meta(meta2, 4, 4, true);
    RowsetSharedPtr rs2;
    status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta2, &rs2);
    EXPECT_TRUE(status.ok());
    candidate_rowsets.push_back(rs2);

    // Non-empty rowset 5-5
    RowsetMetaSharedPtr meta3(new RowsetMeta());
    init_rs_meta(meta3, 5, 5, false);
    RowsetSharedPtr rs3;
    status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta3, &rs3);
    EXPECT_TRUE(status.ok());
    candidate_rowsets.push_back(rs3);

    // Empty rowsets 6-8 (longest consecutive sequence)
    RowsetMetaSharedPtr meta4(new RowsetMeta());
    init_rs_meta(meta4, 6, 6, true);
    RowsetSharedPtr rs4;
    status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta4, &rs4);
    EXPECT_TRUE(status.ok());
    candidate_rowsets.push_back(rs4);

    RowsetMetaSharedPtr meta5(new RowsetMeta());
    init_rs_meta(meta5, 7, 7, true);
    RowsetSharedPtr rs5;
    status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta5, &rs5);
    EXPECT_TRUE(status.ok());
    candidate_rowsets.push_back(rs5);

    RowsetMetaSharedPtr meta6(new RowsetMeta());
    init_rs_meta(meta6, 8, 8, true);
    RowsetSharedPtr rs6;
    status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta6, &rs6);
    EXPECT_TRUE(status.ok());
    candidate_rowsets.push_back(rs6);

    // Non-empty rowset 9-9
    RowsetMetaSharedPtr meta7(new RowsetMeta());
    init_rs_meta(meta7, 9, 9, false);
    RowsetSharedPtr rs7;
    status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta7, &rs7);
    EXPECT_TRUE(status.ok());
    candidate_rowsets.push_back(rs7);

    // Single empty rowset 10-10
    RowsetMetaSharedPtr meta8(new RowsetMeta());
    init_rs_meta(meta8, 10, 10, true);
    RowsetSharedPtr rs8;
    status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta8, &rs8);
    EXPECT_TRUE(status.ok());
    candidate_rowsets.push_back(rs8);

    std::vector<RowsetSharedPtr> result;
    find_longest_consecutive_empty_rowsets(&result, candidate_rowsets);

    // Should find the longest consecutive sequence: rowsets 6-8 (3 rowsets)
    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ(result[0]->start_version(), 6);
    EXPECT_EQ(result[1]->start_version(), 7);
    EXPECT_EQ(result[2]->start_version(), 8);

    // Verify version continuity
    EXPECT_EQ(result[0]->end_version() + 1, result[1]->start_version());
    EXPECT_EQ(result[1]->end_version() + 1, result[2]->start_version());
}

TEST_F(TestCloudEmptyRowsetCompaction, test_find_longest_consecutive_empty_rowsets_no_empty) {
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // Create only non-empty rowsets
    for (int i = 3; i <= 6; ++i) {
        RowsetMetaSharedPtr meta(new RowsetMeta());
        init_rs_meta(meta, i, i, false);
        RowsetSharedPtr rs;
        Status status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta, &rs);
        EXPECT_TRUE(status.ok());
        candidate_rowsets.push_back(rs);
    }

    std::vector<RowsetSharedPtr> result;
    find_longest_consecutive_empty_rowsets(&result, candidate_rowsets);

    // Should return empty result when no empty rowsets exist
    EXPECT_TRUE(result.empty());
}

TEST_F(TestCloudEmptyRowsetCompaction, test_find_longest_consecutive_empty_rowsets_single_empty) {
    std::vector<RowsetSharedPtr> candidate_rowsets;

    // Create a single empty rowset
    RowsetMetaSharedPtr meta(new RowsetMeta());
    init_rs_meta(meta, 3, 3, true);
    RowsetSharedPtr rs;
    Status status = RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta, &rs);
    EXPECT_TRUE(status.ok());
    candidate_rowsets.push_back(rs);

    std::vector<RowsetSharedPtr> result;
    find_longest_consecutive_empty_rowsets(&result, candidate_rowsets);

    // Should return the single empty rowset
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0]->start_version(), 3);
}

} // namespace doris
