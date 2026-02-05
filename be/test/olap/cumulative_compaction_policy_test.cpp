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

#include "olap/cumulative_compaction_policy.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "json2pb/json_to_pb.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

class TestSizeBasedCumulativeCompactionPolicy : public testing::Test {
public:
    TestSizeBasedCumulativeCompactionPolicy() : _engine(StorageEngine({})) {}

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

    void init_rs_meta_big_base(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_total_disk_size(1024 * 1024 * 1024);
        ptr1->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_total_disk_size(65 * 1024 * 1024);
        ptr2->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 5);
        ptr3->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 6, 6);
        ptr4->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 7, 7);
        rs_metas->push_back(ptr5);
    }

    void init_rs_meta_pick_promotion(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_total_disk_size(1024 * 1024 * 1024);
        ptr1->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_total_disk_size(65 * 1024 * 1024);
        ptr2->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 5);
        ptr3->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 6, 6);
        ptr4->set_total_disk_size(65 * 1024 * 1024);
        ptr4->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr4);
    }

    void init_rs_meta_pick_not_same_level(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_total_disk_size(21474836480L); // 20G
        ptr1->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_total_disk_size(129 * 1024 * 1024);
        ptr2->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 5);
        ptr3->set_total_disk_size(12 * 1024 * 1024);
        ptr3->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 6, 6);
        ptr4->set_segments_overlap(OVERLAPPING);
        ptr4->set_total_disk_size(12 * 1024 * 1024);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 7, 7);
        rs_metas->push_back(ptr5);

        RowsetMetaSharedPtr ptr6(new RowsetMeta());
        init_rs_meta(ptr6, 8, 8);
        rs_metas->push_back(ptr6);
    }

    void init_rs_meta_pick_empty(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_total_disk_size(21474836480L); // 20G
        ptr1->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_total_disk_size(257 * 1024 * 1024);
        ptr2->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 5);
        ptr3->set_total_disk_size(129 * 1024 * 1024);
        ptr3->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        ptr4->set_total_disk_size(65 * 1024 * 1024);
        init_rs_meta(ptr4, 6, 6);
        ptr4->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr4);
    }

    void init_rs_meta_pick_empty_not_reach_min_limit(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_total_disk_size(21474836480L); // 20G
        ptr1->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_total_disk_size(257 * 1024 * 1024);
        ptr2->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 5);
        ptr3->set_total_disk_size(1 * 1024 * 1024);
        ptr3->set_num_segments(1);
        ptr3->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 6, 6);
        ptr4->set_total_disk_size(1 * 1024 * 1024);
        ptr4->set_num_segments(1);
        ptr4->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 7, 7);
        ptr5->set_total_disk_size(1 * 1024 * 1024);
        ptr5->set_num_segments(1);
        ptr5->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr5);
    }

    void init_all_rs_meta_cal_point(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 4);
        ptr3->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 5, 5);
        ptr4->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr4);
    }

    void init_all_rs_meta_delete(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_segments_overlap(NONOVERLAPPING);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 4);
        ptr3->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 5, 5);
        DeletePredicatePB del;
        del.add_sub_predicates("a = 1");
        del.set_version(5);
        ptr4->set_delete_predicate(del);
        ptr4->set_segments_overlap(OVERLAP_UNKNOWN);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 6);
        ptr5->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr5);
    }

    void init_rs_meta_missing_version(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 0);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 1, 1);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 2, 2);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 4, 4);
        rs_metas->push_back(ptr5);
    }

protected:
    std::string _json_rowset_meta;
    TabletMetaSharedPtr _tablet_meta;

private:
    StorageEngine _engine;
};

TEST_F(TestSizeBasedCumulativeCompactionPolicy, calc_cumulative_compaction_score) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_SIZE_BASED_POLICY);
    const uint32_t score = _tablet->calc_compaction_score();

    EXPECT_EQ(15, score);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, calc_cumulative_compaction_score_big_base) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();
    std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_SIZE_BASED_POLICY);
    const uint32_t score = _tablet->calc_compaction_score();

    EXPECT_EQ(9, score);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, calculate_cumulative_point_big_base) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    EXPECT_EQ(4, _tablet->cumulative_layer_point());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, calculate_cumulative_point_overlap) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_cal_point(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    EXPECT_EQ(2, _tablet->cumulative_layer_point());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_candidate_rowsets) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_cal_point(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();
    EXPECT_EQ(3, candidate_rowsets.size());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_candidate_rowsets_big_base) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();
    EXPECT_EQ(3, candidate_rowsets.size());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_normal) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // After fix: max=10, total score=12, trim from back until score <= max
    // 4 rowsets with score=3 each = 12, trim 1 -> 3 rowsets with score=9
    EXPECT_EQ(3, input_rowsets.size());
    EXPECT_EQ(9, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_big_base) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(3, input_rowsets.size());
    EXPECT_EQ(7, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_promotion) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_pick_promotion(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(2, input_rowsets.size());
    EXPECT_EQ(4, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_not_same_level) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_pick_not_same_level(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(4, input_rowsets.size());
    EXPECT_EQ(10, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_empty) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_pick_empty(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(0, input_rowsets.size());
    EXPECT_EQ(0, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_not_reach_min_limit) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_pick_empty_not_reach_min_limit(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(0, input_rowsets.size());
    EXPECT_EQ(0, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_delete_in_cumu_compaction) {
    config::enable_delete_when_cumu_compaction = true;
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_delete(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // now cumulative compaction support delete
    EXPECT_EQ(4, input_rowsets.size());
    EXPECT_EQ(10, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_delete) {
    config::enable_delete_when_cumu_compaction = false;
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_delete(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(2, input_rowsets.size());
    EXPECT_EQ(4, compaction_score);
    EXPECT_EQ(5, last_delete_version.first);
    EXPECT_EQ(5, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, _calc_promotion_size_big) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_pick_not_same_level(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    EXPECT_EQ(1073741824, _tablet->cumulative_promotion_size());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, _calc_promotion_size_small) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;
    _tablet->calculate_cumulative_point();

    EXPECT_EQ(67108864, _tablet->cumulative_promotion_size());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, _level_size) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }
    config::compaction_promotion_size_mbytes = 1024;
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    ;

    SizeBasedCumulativeCompactionPolicy* policy =
            dynamic_cast<SizeBasedCumulativeCompactionPolicy*>(
                    _tablet->_cumulative_compaction_policy.get());

    EXPECT_EQ(1 << 29, policy->_level_size(1 << 30));
    EXPECT_EQ(0, policy->_level_size(1000));
    EXPECT_EQ(1 << 20, policy->_level_size((1 << 20) + 100));
    EXPECT_EQ(1 << 19, policy->_level_size((1 << 20) - 100));
}

// Test case: Large head rowsets with high score removed by level_size,
// but now we collect all rowsets and trim from back, so small rowsets can pass min check
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_large_head_with_high_score) {
    // Scenario from the bug:
    // - Head rowsets have high score but get removed by level_size
    // - Previously, max_compaction_score check in collection phase limited collected rowsets
    // - After fix, we collect all and trim from back
    //
    // Key: total_size must be < promotion_size (64MB) to enter level_size logic
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset: version 0-1, 1GB, NONOVERLAPPING
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024); // 1GB
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Large rowset with high score: version 2-2, 35MB, 80 segments (score=80)
    // level(35MB) = 32MB, will be removed by level_size (35 > 22 remain)
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(35L * 1024 * 1024); // 35MB
    ptr2->set_num_segments(80);
    ptr2->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr2);

    // Another rowset: version 3-3, 15MB, 1 segment (score=1)
    // level(15MB) = 8MB, will also be removed by level_size (15 > 7 remain)
    RowsetMetaSharedPtr ptr3(new RowsetMeta());
    init_rs_meta(ptr3, 3, 3);
    ptr3->set_total_disk_size(15L * 1024 * 1024); // 15MB
    ptr3->set_num_segments(1);
    ptr3->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr3);

    // 70 small rowsets: version 4-73, each 100KB, score=1
    // total small = 7MB, total_size = 35+15+7 = 57MB < promotion_size(64MB)
    // After level_size removes head rowsets, these should pass min check (score=70 >= 50)
    for (int i = 4; i <= 73; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(100 * 1024); // 100KB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    // cumulative_point should be 2 (after base rowset)
    EXPECT_EQ(2, _tablet->cumulative_layer_point());

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();
    // Should have 72 candidate rowsets (version 2-73)
    EXPECT_EQ(72, candidate_rowsets.size());

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // max=100, min=50
    // total_size = 57MB < promotion_size(64MB), enters level_size logic
    // level_size removes rowsets 2,3 (35MB, 15MB), remaining 70 small rowsets
    // score after level_size = 70 >= min(50), trim to score <= 100
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 50, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should select rowsets and trim to max score
    EXPECT_GT(input_rowsets.size(), 0);
    EXPECT_LE(compaction_score, 100);
    // The first rowset in result should not be the large head rowsets (they are removed by level_size)
    if (!input_rowsets.empty()) {
        EXPECT_GE(input_rowsets.front()->start_version(), 4);
    }
}

// Test case: Verify trimming from back when score exceeds max
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_trim_from_back) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 150 small rowsets, each with score=1
    for (int i = 2; i <= 151; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();
    EXPECT_EQ(150, candidate_rowsets.size());

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // max=100, min=5
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should trim to max=100
    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
    // Verify we kept the earlier versions (trimmed from back)
    EXPECT_EQ(2, input_rowsets.front()->start_version());
    EXPECT_EQ(101, input_rowsets.back()->end_version());
}

// Test case: Score exactly at max, should not trim
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_score_exactly_max) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Exactly 100 small rowsets, each with score=1
    for (int i = 2; i <= 101; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // max=100, collected score=100, should not trim
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
}

// Test case: Single rowset with score > max should be kept (not trimmed to empty)
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_single_rowset_high_score) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Single rowset with very high score (150 segments)
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(100L * 1024 * 1024); // 100MB, passes min_size check
    ptr2->set_num_segments(150);
    ptr2->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr2);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // max=100, but single rowset has score=150
    // Should keep the single rowset (size > 1 protection)
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should keep the single rowset even though score > max
    EXPECT_EQ(1, input_rowsets.size());
    EXPECT_EQ(150, compaction_score);
}

// Test case: Fallback branch - all rowsets removed by level_size but score >= max
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_fallback_all_removed) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset: 1GB (promotion_size = 64MB with this base)
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024); // 1GB
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Large rowset with high score: 40MB, 50 segments
    // level(40MB) = 32MB, will be removed (32MB > remain_level after removal)
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(40L * 1024 * 1024);
    ptr2->set_num_segments(50);
    ptr2->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr2);

    // Another rowset: 15MB, 30 segments
    // level(15MB) = 8MB, will be removed
    RowsetMetaSharedPtr ptr3(new RowsetMeta());
    init_rs_meta(ptr3, 3, 3);
    ptr3->set_total_disk_size(15L * 1024 * 1024);
    ptr3->set_num_segments(30);
    ptr3->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr3);

    // Another rowset: 5MB, 25 segments
    // level(5MB) = 4MB, will be removed
    RowsetMetaSharedPtr ptr4(new RowsetMeta());
    init_rs_meta(ptr4, 4, 4);
    ptr4->set_total_disk_size(5L * 1024 * 1024);
    ptr4->set_num_segments(25);
    ptr4->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr4);

    // total_size = 60MB < promotion_size(64MB), enters level_size logic
    // level_size calculation:
    // - total=60MB, rowset2: level(40)=32, remain=20, level(20)=16, 32>16, remove
    // - total=20MB, rowset3: level(15)=8, remain=5, level(5)=4, 8>4, remove
    // - total=5MB, rowset4: level(5)=4, remain=0, level(0)=0, 4>0, remove
    // All removed! score=105 >= max(100), triggers fallback

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // All rowsets removed by level_size, score (105) >= max (100)
    // Fallback should select the rowset with max score (50 segments)
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Fallback selects single rowset with max score
    EXPECT_EQ(1, input_rowsets.size());
    EXPECT_EQ(50, compaction_score);
    EXPECT_EQ(2, input_rowsets.front()->start_version());
}

// Test case: Trim after promotion_size early return
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_trim_after_promotion_size) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset: 1GB
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 200 rowsets, each 1MB with score=1
    // Total size = 200MB, which is >= promotion_size (64MB default)
    // Total score = 200, which is > max (100)
    for (int i = 2; i <= 201; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    // Set promotion_min_size to 64MB so 200MB > promotion_size
    config::compaction_promotion_min_size_mbytes = 64;

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should trigger promotion_size early return but still trim to max
    EXPECT_LE(compaction_score, 100);
    EXPECT_EQ(100, input_rowsets.size());
}

// Test case: Trim with varying scores (high score rowsets at tail)
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_trim_high_score_tail) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 95 small rowsets with score=1 each
    for (int i = 2; i <= 96; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    // 1 rowset at tail with score=10
    RowsetMetaSharedPtr ptr_high(new RowsetMeta());
    init_rs_meta(ptr_high, 97, 97);
    ptr_high->set_total_disk_size(10 * 1024 * 1024); // 10MB
    ptr_high->set_num_segments(10);
    ptr_high->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr_high);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // Total score = 95 + 10 = 105, max = 100
    // Should trim the tail rowset (score=10), leaving 95 rowsets with score=95
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // After trimming high-score tail, we get score <= 100
    EXPECT_LE(compaction_score, 100);
    // The high score rowset at version 97 should be trimmed
    if (!input_rowsets.empty()) {
        EXPECT_LT(input_rowsets.back()->end_version(), 97);
    }
}

// Test case: Trim after delete version early return
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_trim_after_delete_version) {
    config::enable_delete_when_cumu_compaction = false;
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 150 small rowsets before delete version, each with score=1
    for (int i = 2; i <= 151; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    // Delete version at 152
    RowsetMetaSharedPtr ptr_del(new RowsetMeta());
    init_rs_meta(ptr_del, 152, 152);
    DeletePredicatePB del;
    del.add_sub_predicates("a = 1");
    del.set_version(152);
    ptr_del->set_delete_predicate(del);
    ptr_del->set_segments_overlap(OVERLAP_UNKNOWN);
    rs_metas.push_back(ptr_del);

    // More rowsets after delete version
    for (int i = 153; i <= 160; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024);
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // max=100, rowsets before delete have score=150
    // Should stop at delete version and trim to max
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should trim to max=100
    EXPECT_LE(compaction_score, 100);
    EXPECT_EQ(152, last_delete_version.first);
    // All selected rowsets should be before delete version
    for (auto& rs : input_rowsets) {
        EXPECT_LT(rs->end_version(), 152);
    }
}

// Test case: Empty rowsets mixed with normal rowsets
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_with_empty_rowsets) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset: 1GB
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Large rowset with high score: 35MB, 80 segments (score=80)
    // Will be removed by level_size
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(35L * 1024 * 1024); // 35MB
    ptr2->set_num_segments(80);
    ptr2->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr2);

    // 50 empty rowsets (score=1 each because NONOVERLAPPING)
    for (int i = 3; i <= 52; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(0);
        ptr->set_num_segments(0);
        ptr->set_segments_overlap(NONOVERLAPPING); // score=1 for NONOVERLAPPING
        rs_metas.push_back(ptr);
    }

    // 100 small rowsets (score=1 each)
    for (int i = 53; i <= 152; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(200 * 1024); // 200KB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    // total_size = 30MB + 0 + 20MB = 50MB < promotion_size(64MB)

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();
    // Total: 1 large + 50 empty + 100 small = 151 rowsets

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // max=100, min=50
    // Total score = 80 + 50 + 100 = 230 (empty rowsets have score=1 because NONOVERLAPPING)
    // After level_size removes large rowset, remaining = 50 empty + 100 small, score = 150
    // Trim to score <= 100
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 50, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should select rowsets after level_size removal and trim
    EXPECT_GT(input_rowsets.size(), 0);
    EXPECT_LE(compaction_score, 100);
    // First rowset should not be the large one (removed by level_size)
    if (!input_rowsets.empty()) {
        EXPECT_GE(input_rowsets.front()->start_version(), 3);
    }
}

// Test case: Score below min after level_size removal, should return empty
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_below_min_after_level_size) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset: 20GB
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(20L * 1024 * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Large rowset: 200MB, 80 segments (score=80)
    // Will be removed by level_size
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(200L * 1024 * 1024);
    ptr2->set_num_segments(80);
    ptr2->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr2);

    // 10 small rowsets (score=1 each, total=10)
    // After level_size removes large, remaining score=10 < min=50
    for (int i = 3; i <= 12; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(100 * 1024); // 100KB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // max=100, min=50
    // Total score = 90, but after level_size removes large rowset, remaining = 10 < 50
    // Also size < min_size (64MB), so should return empty
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 50, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should return empty since score < min and size < min_size
    EXPECT_EQ(0, input_rowsets.size());
    EXPECT_EQ(0, compaction_score);
}

// Test case: Min equals max configuration
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_min_equals_max) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 150 small rowsets, each with score=1
    for (int i = 2; i <= 151; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // min=max=100, should still work correctly
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 100, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should select exactly 100 rowsets (trim to max)
    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
}

// Test case: Very small gap between min and max
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_small_gap_min_max) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 150 small rowsets, each with score=1
    for (int i = 2; i <= 151; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // min=95, max=100, very small gap
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 95, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should select exactly 100 rowsets (trim to max)
    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
}

// Test case: Bug scenario with empty rowsets (full version)
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_large_head_with_empty_rowsets) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset: 1GB
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Large rowset with high score: 35MB, 79 segments (score=79)
    // Will be removed by level_size
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(35L * 1024 * 1024); // 35MB
    ptr2->set_num_segments(79);
    ptr2->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr2);

    // Another large rowset: 15MB, 1 segment (score=1)
    // Will also be removed by level_size
    RowsetMetaSharedPtr ptr3(new RowsetMeta());
    init_rs_meta(ptr3, 3, 3);
    ptr3->set_total_disk_size(15L * 1024 * 1024); // 15MB
    ptr3->set_num_segments(1);
    ptr3->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr3);

    // 50 empty rowsets (score=1 each because NONOVERLAPPING)
    for (int i = 4; i <= 53; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(0);
        ptr->set_num_segments(0);
        ptr->set_segments_overlap(NONOVERLAPPING); // score=1
        rs_metas.push_back(ptr);
    }

    // 70 small rowsets (score=1 each)
    for (int i = 54; i <= 123; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(100 * 1024); // 100KB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    // total_size = 30MB + 15MB + 0 + 7MB = 52MB < promotion_size(64MB)

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // max=100, min=50
    // Total: 2 large (score=80) + 50 empty (score=50) + 70 small (score=70) = 200
    // After level_size removes 2 large: 50 empty + 70 small, score=120 >= 50
    // Trim to score <= 100
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 50, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should select rowsets and trim to max
    EXPECT_GT(input_rowsets.size(), 0);
    EXPECT_LE(compaction_score, 100);
    // First rowset should be empty rowset (large ones removed by level_size)
    if (!input_rowsets.empty()) {
        EXPECT_GE(input_rowsets.front()->start_version(), 4);
    }
}

// Test case: Score exactly at min
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_score_exactly_min) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 50 small rowsets, each with score=1, total score=50=min
    for (int i = 2; i <= 51; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // min=50, score=50, should pass min check
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 50, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(50, input_rowsets.size());
    EXPECT_EQ(50, compaction_score);
}

// Test case: Score one above max
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_score_one_above_max) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 101 small rowsets, each with score=1, total score=101
    for (int i = 2; i <= 102; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // max=100, score=101, should trim 1 rowset from back
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
    EXPECT_EQ(101, input_rowsets.back()->end_version());
}

// Test case: Only empty rowsets
// Note: Empty rowsets with NONOVERLAPPING have score=1 (not 0!)
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_only_empty_rowsets) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 100 empty rowsets (score=1 each because NONOVERLAPPING)
    for (int i = 2; i <= 101; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(0);
        ptr->set_num_segments(0);
        ptr->set_segments_overlap(NONOVERLAPPING); // score=1
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // 100 empty rowsets, score=100 >= min=5
    // size=0 < min_size, but score >= min so pass min check
    // Trim to max=100
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // All 100 empty rowsets selected, score=100
    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
}

// Test case: Large count of empty rowsets mixed with normal
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_large_count_empty_rowsets) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 500 empty rowsets (score=0)
    for (int i = 2; i <= 501; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(0);
        ptr->set_num_segments(0);
        ptr->set_segments_overlap(NONOVERLAPPING);
        rs_metas.push_back(ptr);
    }

    // 100 normal rowsets (score=1 each)
    for (int i = 502; i <= 601; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();
    // Should have 600 candidate rowsets
    EXPECT_EQ(600, candidate_rowsets.size());

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // Total score = 500 (empty rowsets score=1) + 100 = 600
    // Increase max to 600 to allow collecting all
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 600, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Should select all 600 rowsets
    EXPECT_EQ(600, input_rowsets.size());
    EXPECT_EQ(600, compaction_score);
}

// Test case: Single non-overlapping rowset
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_single_non_overlapping) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Single non-overlapping rowset with size >= min_size
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(100L * 1024 * 1024); // 100MB >= 64MB min_size
    ptr2->set_num_segments(1);
    ptr2->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr2);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Single non-overlapping rowset doesn't need compaction
    EXPECT_EQ(0, input_rowsets.size());
    EXPECT_EQ(0, compaction_score);
}

// Test case: Fallback when all removed by level_size but score < max
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_fallback_score_below_max) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset: 20GB
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(20L * 1024 * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Large rowset: 200MB, 30 segments (score=30)
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(200L * 1024 * 1024);
    ptr2->set_num_segments(30);
    ptr2->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr2);

    // Another large rowset: 100MB, 20 segments (score=20)
    RowsetMetaSharedPtr ptr3(new RowsetMeta());
    init_rs_meta(ptr3, 3, 3);
    ptr3->set_total_disk_size(100L * 1024 * 1024);
    ptr3->set_num_segments(20);
    ptr3->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr3);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // All rowsets removed by level_size, score=50 < max=100
    // Does not trigger fallback, goes to normal flow with empty result
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // After level_size removes all, result is empty (no fallback since score < max)
    EXPECT_EQ(0, input_rowsets.size());
    EXPECT_EQ(0, compaction_score);
}

// Test case: level_size removes large head, then trim after
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_trim_after_level_size) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Large rowset: 35MB, 50 segments (will be removed by level_size)
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(35L * 1024 * 1024);
    ptr2->set_num_segments(50);
    ptr2->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr2);

    // 150 small rowsets (score=1 each)
    for (int i = 3; i <= 152; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(100 * 1024); // 100KB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // Total score = 50 + 150 = 200
    // After level_size removes large: score = 150 > max = 100
    // Should trim to 100
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
    // First rowset should be version 3 (large one removed)
    EXPECT_EQ(3, input_rowsets.front()->start_version());
}

// Test case: Size exactly at min_size
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_size_exactly_min_size) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Rowsets with total size = 64MB (exactly min_size)
    // 64 rowsets, each 1MB
    for (int i = 2; i <= 65; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1 * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // size = 64MB = min_size, score = 64 > min = 50
    // Should pass min check
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 50, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(64, input_rowsets.size());
    EXPECT_EQ(64, compaction_score);
}

// Test case: min=0 configuration
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_zero_min_score) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 3 small rowsets
    for (int i = 2; i <= 4; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(100 * 1024); // 100KB, size < min_size
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // min=0, score=3 >= 0, but size < min_size
    // When min=0, always pass score check, but still need size check
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 0, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // score >= min (0), pass min check even if size < min_size
    EXPECT_EQ(3, input_rowsets.size());
    EXPECT_EQ(3, compaction_score);
}

// Test case: All large rowsets with similar size (should keep all)
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_keep_all_large) {
    config::compaction_promotion_ratio = 1.0;
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 3 large rowsets with similar size (100MB each)
    for (int i = 2; i <= 4; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(100L * 1024 * 1024); // 100MB each
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 50, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // All 3 large rowsets should be kept (similar level_size)
    // total_size = 300MB, level(300MB) = 256MB
    // level(100MB) = 64MB, level(200MB) = 128MB, 64MB <= 128MB, so keep all
    EXPECT_EQ(3, input_rowsets.size());
    EXPECT_EQ(3, compaction_score);
}

// Test case: Size exactly one below min_size
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_size_one_below_min) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Small rowsets with total size = min_size - 1 byte
    // min_size = 64MB = 67108864 bytes
    // Create 10 rowsets, each about 6.7MB, total = 67108863 bytes (1 byte less than min_size)
    int64_t target_total = 64L * 1024 * 1024 - 1; // 64MB - 1 byte
    int64_t per_rowset = target_total / 10;
    int64_t remainder = target_total % 10;

    for (int i = 2; i <= 11; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        int64_t size = per_rowset;
        if (i == 11) size += remainder; // Add remainder to last rowset
        ptr->set_total_disk_size(size);
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // score=10 < min=50, size < min_size, should be cleared
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 50, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(0, input_rowsets.size());
    EXPECT_EQ(0, compaction_score);
}

// Test case: min greater than max (abnormal configuration)
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_min_greater_than_max) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 150 small rowsets, score=150
    for (int i = 2; i <= 151; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1L * 1024 * 1024); // 1MB each
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // Abnormal config: min=200, max=100
    // After trim, score=100, but min check happens before trim
    // So if score >= min before trim, it will pass
    // Here score=150 < 200, will be cleared by min check
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 200, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // score=150 < min=200, and size=150MB >= min_size=64MB
    // Size check passes, so min score check is bypassed
    // Result: 100 rowsets after trim
    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
}

// Test case: Multiple delete versions
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_multiple_delete_versions) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 5 normal rowsets
    for (int i = 2; i <= 6; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(20L * 1024 * 1024); // 20MB each
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    // First delete version at version 7
    RowsetMetaSharedPtr ptr_del1(new RowsetMeta());
    init_rs_meta(ptr_del1, 7, 7);
    ptr_del1->set_total_disk_size(1024);
    ptr_del1->set_num_segments(0);
    ptr_del1->set_segments_overlap(NONOVERLAPPING);
    DeletePredicatePB del_pred1;
    del_pred1.set_version(7);
    ptr_del1->set_delete_predicate(del_pred1);
    rs_metas.push_back(ptr_del1);

    // 3 normal rowsets
    for (int i = 8; i <= 10; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(20L * 1024 * 1024);
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    // Second delete version at version 11
    RowsetMetaSharedPtr ptr_del2(new RowsetMeta());
    init_rs_meta(ptr_del2, 11, 11);
    ptr_del2->set_total_disk_size(1024);
    ptr_del2->set_num_segments(0);
    ptr_del2->set_segments_overlap(NONOVERLAPPING);
    DeletePredicatePB del_pred2;
    del_pred2.set_version(11);
    ptr_del2->set_delete_predicate(del_pred2);
    rs_metas.push_back(ptr_del2);

    // 3 more normal rowsets
    for (int i = 12; i <= 14; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(20L * 1024 * 1024);
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // Should only select rowsets before first delete version
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 3, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Only first 5 rowsets before delete version 7
    EXPECT_EQ(5, input_rowsets.size());
    EXPECT_EQ(5, compaction_score);
    EXPECT_EQ(7, last_delete_version.first);
}

// Test case: Level size exactly half
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_level_size_exactly_half) {
    config::compaction_promotion_ratio = 1.0;
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Two rowsets with exactly same size (64MB each)
    // total = 128MB, level(128MB) = 128MB
    // level(64MB) = 64MB, level(64MB) = 64MB
    // 64MB <= 64MB, so keep both
    for (int i = 2; i <= 3; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(64L * 1024 * 1024); // 64MB each
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 2, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Both rowsets should be kept (same level)
    EXPECT_EQ(2, input_rowsets.size());
    EXPECT_EQ(2, compaction_score);
}

// Test case: Tiny rowsets (< 1KB)
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_tiny_rowsets) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 100 tiny rowsets (500 bytes each)
    for (int i = 2; i <= 101; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(500); // 500 bytes
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 50, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // All tiny rowsets have level_size = 0, so all should be kept
    // But trim from back due to max_compaction_score
    // total_size = 50KB < min_size, score = 100 >= min, size < min_size -> cleared
    // Wait, score >= min OR size >= min_size to pass
    // Here score = 100 >= 50, so should pass min check
    // Then trim to 100
    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
}

// Test case: Trim result below min (min check happens before trim)
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_trim_result_below_min) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 120 rowsets, each 1MB, score=1
    // total score = 120, after trim to max=100, score = 100
    // min = 90, after trim score = 100 >= 90
    // But test the case where trim could bring it below min conceptually
    for (int i = 2; i <= 121; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(1L * 1024 * 1024); // 1MB
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // min=90, max=100, initial score=120
    // After trim, score=100 >= 90, still passes
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 90, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
}

// Test case: Single rowset with score exceeding max (fallback allows it)
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_single_rowset_exceeds_max) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // Single large rowset with very high score (150 segments)
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(200L * 1024 * 1024); // 200MB
    ptr2->set_num_segments(150);                   // score = 150
    ptr2->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr2);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    // max=100, but single rowset score=150
    // Due to size() > 1 protection in DEFER, won't be trimmed
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 50, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    // Single rowset kept, score = 150 (exceeds max, but allowed due to size protection)
    EXPECT_EQ(1, input_rowsets.size());
    EXPECT_EQ(150, compaction_score);
}

// Large rowsets removed by level_size, small ones remain and get trimmed
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_fallback_no_overlapping) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    // Base rowset: 1GB
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 3 large rowsets (40MB, 15MB, 5MB) removed by level_size + 100 small ones remain
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(40L * 1024 * 1024);
    ptr2->set_num_segments(1);
    ptr2->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr2);

    RowsetMetaSharedPtr ptr3(new RowsetMeta());
    init_rs_meta(ptr3, 3, 3);
    ptr3->set_total_disk_size(15L * 1024 * 1024);
    ptr3->set_num_segments(1);
    ptr3->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr3);

    RowsetMetaSharedPtr ptr4(new RowsetMeta());
    init_rs_meta(ptr4, 4, 4);
    ptr4->set_total_disk_size(5L * 1024 * 1024);
    ptr4->set_num_segments(1);
    ptr4->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr4);

    for (int i = 5; i <= 104; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i, i);
        ptr->set_total_disk_size(10 * 1024);
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(100, input_rowsets.size());
    EXPECT_EQ(100, compaction_score);
    EXPECT_EQ(5, input_rowsets.front()->start_version());
}

// Fallback: all rowsets removed by level_size, score >= max, no high-score rowset found
// -> returns all, DEFER trims to max
TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_fallback_no_high_score_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 1);
    ptr1->set_total_disk_size(1024L * 1024 * 1024);
    ptr1->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr1);

    // 4 NONOVERLAPPING rowsets (score=1 each), all removed by level_size
    // max=3, score=4 >= max triggers fallback, no score > 1 found
    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 2, 2);
    ptr2->set_total_disk_size(40L * 1024 * 1024);
    ptr2->set_num_segments(1);
    ptr2->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr2);

    RowsetMetaSharedPtr ptr3(new RowsetMeta());
    init_rs_meta(ptr3, 3, 3);
    ptr3->set_total_disk_size(15L * 1024 * 1024);
    ptr3->set_num_segments(1);
    ptr3->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr3);

    RowsetMetaSharedPtr ptr4(new RowsetMeta());
    init_rs_meta(ptr4, 4, 4);
    ptr4->set_total_disk_size(5L * 1024 * 1024);
    ptr4->set_num_segments(1);
    ptr4->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr4);

    RowsetMetaSharedPtr ptr5(new RowsetMeta());
    init_rs_meta(ptr5, 5, 5);
    ptr5->set_total_disk_size(1L * 1024 * 1024);
    ptr5->set_num_segments(1);
    ptr5->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr5);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 3, 1, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(3, input_rowsets.size());
    EXPECT_EQ(3, compaction_score);
}

} // namespace doris

// @brief Test Stub
