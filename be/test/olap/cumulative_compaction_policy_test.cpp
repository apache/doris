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

#include <gtest/gtest.h>

#include <sstream>

#include "olap/cumulative_compaction.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_meta.h"

namespace doris {

class TestSizeBasedCumulativeCompactionPolicy : public testing::Test {
public:
    TestSizeBasedCumulativeCompactionPolicy() {}
    void SetUp() {
        config::cumulative_size_based_promotion_size_mbytes = 1024;
        config::cumulative_size_based_promotion_ratio = 0.05;
        config::cumulative_size_based_promotion_min_size_mbytes = 64;
        config::cumulative_size_based_compaction_lower_size_mbytes = 64;

        _tablet_meta = static_cast<TabletMetaSharedPtr>(new TabletMeta(
                1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));

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
        pb1->init_from_json(_json_rowset_meta);
        pb1->set_start_version(start);
        pb1->set_end_version(end);
        pb1->set_total_disk_size(41);
        pb1->set_creation_time(10000);
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
};

TEST_F(TestSizeBasedCumulativeCompactionPolicy, calc_cumulative_compaction_score) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy();
    const uint32_t score = _tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION,
                                                          cumulative_compaction_policy);

    EXPECT_EQ(15, score);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, calc_cumulative_compaction_score_big_base) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();
    std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy();
    const uint32_t score = _tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION,
                                                          cumulative_compaction_policy);

    EXPECT_EQ(7, score);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, calculate_cumulative_point_big_base) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    EXPECT_EQ(4, _tablet->cumulative_layer_point());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, calculate_cumulative_point_overlap) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_cal_point(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    EXPECT_EQ(2, _tablet->cumulative_layer_point());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_candidate_rowsets) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_cal_point(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    std::vector<RowsetSharedPtr> candidate_rowsets;
    std::shared_lock rdlock(_tablet->get_header_lock());
    _tablet->pick_candidate_rowsets_to_cumulative_compaction(&candidate_rowsets, rdlock);

    EXPECT_EQ(3, candidate_rowsets.size());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_candidate_rowsets_big_base) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    std::vector<RowsetSharedPtr> candidate_rowsets;
    std::shared_lock rdlock(_tablet->get_header_lock());
    _tablet->pick_candidate_rowsets_to_cumulative_compaction(&candidate_rowsets, rdlock);

    EXPECT_EQ(3, candidate_rowsets.size());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_normal) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    std::vector<RowsetSharedPtr> candidate_rowsets;

    std::shared_lock rdlock(_tablet->get_header_lock());
    _tablet->pick_candidate_rowsets_to_cumulative_compaction(&candidate_rowsets, rdlock);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score);

    EXPECT_EQ(4, input_rowsets.size());
    EXPECT_EQ(12, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_big_base) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    std::vector<RowsetSharedPtr> candidate_rowsets;

    std::shared_lock rdlock(_tablet->get_header_lock());
    _tablet->pick_candidate_rowsets_to_cumulative_compaction(&candidate_rowsets, rdlock);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score);

    EXPECT_EQ(3, input_rowsets.size());
    EXPECT_EQ(7, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_promotion) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_pick_promotion(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    std::vector<RowsetSharedPtr> candidate_rowsets;

    std::shared_lock rdlock(_tablet->get_header_lock());
    _tablet->pick_candidate_rowsets_to_cumulative_compaction(&candidate_rowsets, rdlock);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score);

    EXPECT_EQ(2, input_rowsets.size());
    EXPECT_EQ(4, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_not_same_level) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_pick_not_same_level(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    std::vector<RowsetSharedPtr> candidate_rowsets;

    std::shared_lock rdlock(_tablet->get_header_lock());
    _tablet->pick_candidate_rowsets_to_cumulative_compaction(&candidate_rowsets, rdlock);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score);

    EXPECT_EQ(4, input_rowsets.size());
    EXPECT_EQ(10, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_empty) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_pick_empty(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    std::vector<RowsetSharedPtr> candidate_rowsets;

    std::shared_lock rdlock(_tablet->get_header_lock());
    _tablet->pick_candidate_rowsets_to_cumulative_compaction(&candidate_rowsets, rdlock);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score);

    EXPECT_EQ(0, input_rowsets.size());
    EXPECT_EQ(0, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_not_reach_min_limit) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_pick_empty_not_reach_min_limit(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    std::vector<RowsetSharedPtr> candidate_rowsets;

    std::shared_lock rdlock(_tablet->get_header_lock());
    _tablet->pick_candidate_rowsets_to_cumulative_compaction(&candidate_rowsets, rdlock);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score);

    EXPECT_EQ(0, input_rowsets.size());
    EXPECT_EQ(0, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets_delete) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_delete(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    std::vector<RowsetSharedPtr> candidate_rowsets;

    std::shared_lock rdlock(_tablet->get_header_lock());
    _tablet->pick_candidate_rowsets_to_cumulative_compaction(&candidate_rowsets, rdlock);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score);

    EXPECT_EQ(2, input_rowsets.size());
    EXPECT_EQ(4, compaction_score);
    EXPECT_EQ(5, last_delete_version.first);
    EXPECT_EQ(5, last_delete_version.second);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, _calc_promotion_size_big) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_pick_not_same_level(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    SizeBasedCumulativeCompactionPolicy* policy =
            dynamic_cast<SizeBasedCumulativeCompactionPolicy*>(
                    _tablet->_cumulative_compaction_policy.get());

    EXPECT_EQ(1073741824, policy->_tablet_size_based_promotion_size);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, _calc_promotion_size_small) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();
    _tablet->calculate_cumulative_point();

    SizeBasedCumulativeCompactionPolicy* policy =
            dynamic_cast<SizeBasedCumulativeCompactionPolicy*>(
                    _tablet->_cumulative_compaction_policy.get());
    EXPECT_EQ(67108864, policy->_tablet_size_based_promotion_size);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, _level_size) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();

    SizeBasedCumulativeCompactionPolicy* policy =
            dynamic_cast<SizeBasedCumulativeCompactionPolicy*>(
                    _tablet->_cumulative_compaction_policy.get());

    EXPECT_EQ(536870912, policy->_levels[0]);
    EXPECT_EQ(268435456, policy->_levels[1]);
    EXPECT_EQ(134217728, policy->_levels[2]);
    EXPECT_EQ(67108864, policy->_levels[3]);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy, _pick_missing_version_cumulative_compaction) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_missing_version(&rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    _tablet->init();

    // has miss version
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.push_back(_tablet->get_rowset_by_version({0, 0}));
    rowsets.push_back(_tablet->get_rowset_by_version({1, 1}));
    rowsets.push_back(_tablet->get_rowset_by_version({2, 2}));
    rowsets.push_back(_tablet->get_rowset_by_version({4, 4}));
    CumulativeCompaction compaction(_tablet);
    compaction.find_longest_consecutive_version(&rowsets, nullptr);
    EXPECT_EQ(3, rowsets.size());
    EXPECT_EQ(2, rowsets[2]->end_version());

    // no miss version
    std::vector<RowsetSharedPtr> rowsets2;
    rowsets2.push_back(_tablet->get_rowset_by_version({0, 0}));
    compaction.find_longest_consecutive_version(&rowsets2, nullptr);
    EXPECT_EQ(1, rowsets2.size());
    EXPECT_EQ(0, rowsets[0]->end_version());

    // no version
    std::vector<RowsetSharedPtr> rowsets3;
    compaction.find_longest_consecutive_version(&rowsets3, nullptr);
    EXPECT_EQ(0, rowsets3.size());
}
} // namespace doris

// @brief Test Stub
