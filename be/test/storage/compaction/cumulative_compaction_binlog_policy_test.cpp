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

#include "storage/compaction/cumulative_compaction_binlog_policy.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "json2pb/json_to_pb.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

class TestBinlogCumulativeCompactionPolicy : public testing::Test {
public:
    TestBinlogCumulativeCompactionPolicy() : _engine(StorageEngine({})) {}

    void SetUp() {
        config::binlog_compaction_goal_size_mbytes = 128;
        config::binlog_compaction_file_count_threshold = 100;
        config::binlog_level_compaction_max_deltas = 2000;
        config::binlog_compaction_time_threshold_seconds = 3600;
        config::total_permits_for_compaction_score = 1000000;

        _tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                          UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                          TCompressionType::LZ4F));
        _tablet_meta->set_is_row_binlog_tablet(true);
        _tablet_meta->set_compaction_policy(std::string(CUMULATIVE_BINLOG_POLICY));

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
            "num_segments": 1
        })";
    }
    void TearDown() {}

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end,
                      int8_t compaction_level) {
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(_json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
        pb1->set_total_disk_size(41);
        pb1->set_data_disk_size(41);
        pb1->set_segments_overlap(NONOVERLAPPING);
        pb1->mark_row_binlog();
        pb1->set_compaction_level(compaction_level);
        pb1->set_tablet_schema(_tablet_meta->tablet_schema());
    }

    // LMax rowsets layout:
    //
    //   version:  0      1        2     | 3      4
    //   size:     -    goal     goal    | -    goal
    //   overlap:  -    yes(5)     -     | -      -
    //   enough:   Y      Y        Y     | N      Y
    //                                   ^
    //                          cumulative point = 3
    //
    // [0-0],[1-1],[2-2] are compact-enough -> cumulative point stops at 3.
    // [1-1] before the point is overlapping (raw score 5) but is counted as 1, so the score test
    // can tell apart "before the point counts as 1" from "after uses the raw compaction score".
    // After the point [3-3],[4-4] are non-overlapping (score 1 each), so their physical rewrite
    // score (2) is smaller than the quick merge score (3) and the quick merge path is chosen.
    void init_rs_meta_lmax(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        int64_t goal_size = config::binlog_compaction_goal_size_mbytes * 1024 * 1024;
        int8_t max_level = BinlogCumulativeCompactionPolicy::kBinlogCompactionMaxLevel - 1;

        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 0, max_level);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 1, 1, max_level);
        ptr2->set_total_disk_size(goal_size);
        ptr2->set_data_disk_size(goal_size);
        ptr2->set_num_segments(5);
        ptr2->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 2, 2, max_level);
        ptr3->set_total_disk_size(goal_size);
        ptr3->set_data_disk_size(goal_size);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 3, 3, max_level);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 4, 4, max_level);
        ptr5->set_total_disk_size(goal_size);
        ptr5->set_data_disk_size(goal_size);
        ptr5->set_num_segments(5);
        rs_metas->push_back(ptr5);
    }

    // L0 rowsets: two overlapping rowsets, score = 4 + 1.
    void init_rs_meta_l0(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 0, 0);
        ptr1->set_num_segments(4);
        ptr1->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 1, 1, 0);
        rs_metas->push_back(ptr2);
    }

protected:
    std::string _json_rowset_meta;
    StorageEngine _engine;
    TabletMetaSharedPtr _tablet_meta;
};

// cumulative point advances over the contiguous compact-enough LMax prefix and stops at
// the first rowset that is not compact-enough.
TEST_F(TestBinlogCumulativeCompactionPolicy, calculate_cumulative_point) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_lmax(&rs_metas);

    for (auto& rowset : rs_metas) {
        ASSERT_TRUE(_tablet_meta->add_rs_meta(rowset).ok());
    }

    TabletSharedPtr _tablet(new Tablet(_engine, _tablet_meta, nullptr));
    ASSERT_TRUE(_tablet->init().ok());
    _tablet->calculate_cumulative_point();

    EXPECT_EQ(3, _tablet->cumulative_layer_point());
}

// LMax score: each rowset before cumulative point counts as 1, the others use compaction score.
TEST_F(TestBinlogCumulativeCompactionPolicy, calc_lmax_score) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_lmax(&rs_metas);

    for (auto& rowset : rs_metas) {
        ASSERT_TRUE(_tablet_meta->add_rs_meta(rowset).ok());
    }

    TabletSharedPtr _tablet(new Tablet(_engine, _tablet_meta, nullptr));
    ASSERT_TRUE(_tablet->init().ok());
    _tablet->calculate_cumulative_point();
    EXPECT_EQ(3, _tablet->cumulative_layer_point());

    int8_t max_level = BinlogCumulativeCompactionPolicy::kBinlogCompactionMaxLevel - 1;
    // before cumulative point: [0-0] -> 1, [1-1] -> 1 (raw score 5 but counted as 1), [2-2] -> 1;
    // after cumulative point: [3-3] -> 1, [4-4] -> 1 (raw score). total = 5.
    EXPECT_EQ(5,
              dynamic_cast<BinlogCumulativeCompactionPolicy*>(
                      _tablet->cumulative_compaction_policy())
                      ->calc_binlog_compaction_level_score(_tablet.get(), max_level));
}

// L0 score always uses the raw compaction score and is independent of cumulative point.
TEST_F(TestBinlogCumulativeCompactionPolicy, calc_l0_score) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_l0(&rs_metas);

    for (auto& rowset : rs_metas) {
        ASSERT_TRUE(_tablet_meta->add_rs_meta(rowset).ok());
    }

    TabletSharedPtr _tablet(new Tablet(_engine, _tablet_meta, nullptr));
    ASSERT_TRUE(_tablet->init().ok());
    _tablet->calculate_cumulative_point();

    int8_t prefer_level = -1;
    uint32_t score =
            dynamic_cast<BinlogCumulativeCompactionPolicy*>(
                    _tablet->cumulative_compaction_policy())
                    ->calc_binlog_compaction_score(_tablet.get(), &prefer_level);
    // L0 rowsets: [0-0] -> 4 (overlapping), [1-1] -> 1. total = 5.
    EXPECT_EQ(5, score);
    EXPECT_EQ(0, prefer_level);
}

// Pick level from candidate rowsets. Versions are ordered old -> new as higher -> lower level:
// L2, L2, L1, L1, L0, L0. Even if the whole tablet has a higher L0 score, this round's
// candidate set only contains L1 rowsets, so the policy should choose L1.
TEST_F(TestBinlogCumulativeCompactionPolicy, pick_level_from_candidate_rowsets) {
    config::binlog_compaction_file_count_threshold = 2;

    for (int64_t version = 0; version < 2; ++version) {
        RowsetMetaSharedPtr rowset(new RowsetMeta());
        init_rs_meta(rowset, version, version, 2);
        ASSERT_TRUE(_tablet_meta->add_rs_meta(rowset).ok());
    }

    for (int64_t version = 2; version < 4; ++version) {
        RowsetMetaSharedPtr rowset(new RowsetMeta());
        init_rs_meta(rowset, version, version, 1);
        ASSERT_TRUE(_tablet_meta->add_rs_meta(rowset).ok());
    }

    for (int64_t version = 4; version < 6; ++version) {
        RowsetMetaSharedPtr rowset(new RowsetMeta());
        init_rs_meta(rowset, version, version, 0);
        rowset->set_num_segments(10);
        rowset->set_segments_overlap(OVERLAPPING);
        ASSERT_TRUE(_tablet_meta->add_rs_meta(rowset).ok());
    }

    TabletSharedPtr _tablet(new Tablet(_engine, _tablet_meta, nullptr));
    ASSERT_TRUE(_tablet->init().ok());

    std::vector<RowsetSharedPtr> candidate_rowsets;
    for (const auto& rs : _tablet->pick_candidate_rowsets_to_binlog_compaction()) {
        if (rs->rowset_meta()->compaction_level() == 1) {
            candidate_rowsets.push_back(rs);
        }
    }

    std::vector<RowsetSharedPtr> input_rowsets;
    size_t compaction_score = 0;
    int picked =
            dynamic_cast<BinlogCumulativeCompactionPolicy*>(
                    _tablet->cumulative_compaction_policy())
                    ->pick_input_rowsets(_tablet.get(), candidate_rowsets,
                                         config::binlog_level_compaction_max_deltas,
                                         config::cumulative_compaction_min_deltas, &input_rowsets,
                                         nullptr, &compaction_score);

    EXPECT_EQ(2, picked);
    EXPECT_EQ(2, compaction_score);
    EXPECT_EQ(2, input_rowsets.size());
    EXPECT_EQ(2, input_rowsets.front()->start_version());
    EXPECT_EQ(3, input_rowsets.back()->end_version());
}

// LMax quick merge: the physical rewrite over the remaining rowsets is triggered, but the quick
// merge over the compact-enough prefix has a higher score, so the quick merge path is chosen.
TEST_F(TestBinlogCumulativeCompactionPolicy, pick_input_rowsets_lmax_quick_merge) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_lmax(&rs_metas);

    for (auto& rowset : rs_metas) {
        ASSERT_TRUE(_tablet_meta->add_rs_meta(rowset).ok());
    }

    TabletSharedPtr _tablet(new Tablet(_engine, _tablet_meta, nullptr));
    ASSERT_TRUE(_tablet->init().ok());
    _tablet->calculate_cumulative_point();
    EXPECT_EQ(3, _tablet->cumulative_layer_point());

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_binlog_compaction();

    int8_t max_level = BinlogCumulativeCompactionPolicy::kBinlogCompactionMaxLevel - 1;
    std::vector<RowsetSharedPtr> input_rowsets;
    int picked =
            dynamic_cast<BinlogCumulativeCompactionPolicy*>(_tablet->cumulative_compaction_policy())
                    ->pick_input_rowsets(_tablet.get(), candidate_rowsets, max_level,
                                         config::binlog_level_compaction_max_deltas,
                                         &input_rowsets);

    // quick merge score (3) > physical rewrite score (2): [0-0],[1-1],[2-2] are quick merged.
    EXPECT_EQ(3, picked);
    EXPECT_EQ(3, input_rowsets.size());
    EXPECT_EQ(0, input_rowsets.front()->start_version());
    EXPECT_EQ(2, input_rowsets.back()->end_version());
}

// L0 physical rewrite: rowsets are merged when the file count trigger is met.
TEST_F(TestBinlogCumulativeCompactionPolicy, pick_input_rowsets_l0_physical_rewrite) {
    config::binlog_compaction_file_count_threshold = 3;
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_l0(&rs_metas);

    for (auto& rowset : rs_metas) {
        ASSERT_TRUE(_tablet_meta->add_rs_meta(rowset).ok());
    }

    TabletSharedPtr _tablet(new Tablet(_engine, _tablet_meta, nullptr));
    ASSERT_TRUE(_tablet->init().ok());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_binlog_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    int picked =
            dynamic_cast<BinlogCumulativeCompactionPolicy*>(_tablet->cumulative_compaction_policy())
                    ->pick_input_rowsets(_tablet.get(), candidate_rowsets, 0,
                                         config::binlog_level_compaction_max_deltas,
                                         &input_rowsets);

    // L0 score 4 + 1 = 5 >= file count threshold 3, so both rowsets are physically rewritten.
    EXPECT_EQ(2, picked);
    EXPECT_EQ(2, input_rowsets.size());
}

// L0 not triggered: when size / score / time thresholds are all unmet, nothing is picked.
TEST_F(TestBinlogCumulativeCompactionPolicy, pick_input_rowsets_l0_not_triggered) {
    config::binlog_compaction_file_count_threshold = 100;
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_l0(&rs_metas);

    for (auto& rowset : rs_metas) {
        ASSERT_TRUE(_tablet_meta->add_rs_meta(rowset).ok());
    }

    TabletSharedPtr _tablet(new Tablet(_engine, _tablet_meta, nullptr));
    ASSERT_TRUE(_tablet->init().ok());
    _tablet->set_last_cumu_compaction_success_time(UnixMillis());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_binlog_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    int picked =
            dynamic_cast<BinlogCumulativeCompactionPolicy*>(_tablet->cumulative_compaction_policy())
                    ->pick_input_rowsets(_tablet.get(), candidate_rowsets, 0,
                                         config::binlog_level_compaction_max_deltas,
                                         &input_rowsets);

    // L0 score 5 < file count threshold 100, size below goal, time threshold not reached.
    EXPECT_EQ(0, picked);
    EXPECT_EQ(0, input_rowsets.size());
}

} // namespace doris
