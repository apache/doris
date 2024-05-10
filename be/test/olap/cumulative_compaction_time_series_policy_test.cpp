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

#include "olap/cumulative_compaction_time_series_policy.h"

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
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {

class TestTimeSeriesCumulativeCompactionPolicy : public testing::Test {
public:
    TestTimeSeriesCumulativeCompactionPolicy() : _engine(StorageEngine({})) {}
    void SetUp() {
        _tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                          UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                          TCompressionType::LZ4F));

        _tablet_meta->set_compaction_policy(std::string(CUMULATIVE_TIME_SERIES_POLICY));
        _tablet_meta->set_time_series_compaction_goal_size_mbytes(100);
        _tablet_meta->set_time_series_compaction_file_count_threshold(10);
        _tablet_meta->set_time_series_compaction_time_threshold_seconds(3600);

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
        pb1->set_segments_overlap(NONOVERLAPPING);
        pb1->set_tablet_schema(_tablet_meta->tablet_schema());
    }

    void init_rs_meta_cal_point(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
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

    void init_rs_meta_big_rowset(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_total_disk_size(100 * 1024 * 1024);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_total_disk_size(100 * 1024 * 1024);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 4);
        ptr3->set_total_disk_size(51 * 1024 * 1024);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 5, 5);
        ptr4->set_segments_overlap(OVERLAPPING);
        ptr4->set_total_disk_size(51 * 1024 * 1024);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 6);
        ptr5->set_total_disk_size(51 * 1024 * 1024);
        rs_metas->push_back(ptr5);
    }

    void init_rs_meta_normal(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 0);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 1, 1);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 2, 2);
        ptr3->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 3, 3);
        ptr4->set_segments_overlap(OVERLAPPING);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 4, 4);
        rs_metas->push_back(ptr5);
    }

    void init_all_rs_meta_normal_size(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_total_disk_size(1 * 1024 * 1024);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_total_disk_size(2 * 1024 * 1024);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 4);
        ptr3->set_segments_overlap(OVERLAPPING);
        ptr3->set_total_disk_size(3 * 1024 * 1024);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 5, 5);
        ptr4->set_segments_overlap(OVERLAPPING);
        ptr4->set_total_disk_size(4 * 1024 * 1024);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 6);
        ptr5->set_segments_overlap(OVERLAPPING);
        ptr5->set_total_disk_size(5 * 1024 * 1024);
        rs_metas->push_back(ptr5);
    }

    void init_all_rs_meta_normal_size_nonoverlapping(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_total_disk_size(1 * 1024);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_total_disk_size(2 * 1024);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 4);
        ptr3->set_total_disk_size(3 * 1024);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 5, 5);
        ptr4->set_total_disk_size(4 * 1024);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 6);
        ptr5->set_total_disk_size(5 * 1024);
        rs_metas->push_back(ptr5);
    }

    void init_all_rs_meta_empty_nonoverlapping(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_total_disk_size(1 * 1024);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_total_disk_size(2 * 1024);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 4);
        ptr3->set_num_segments(0);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 5, 5);
        ptr4->set_num_segments(0);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 6);
        ptr5->set_num_segments(0);
        rs_metas->push_back(ptr5);

        RowsetMetaSharedPtr ptr6(new RowsetMeta());
        init_rs_meta(ptr6, 7, 7);
        ptr6->set_num_segments(0);
        rs_metas->push_back(ptr6);

        RowsetMetaSharedPtr ptr7(new RowsetMeta());
        init_rs_meta(ptr7, 8, 8);
        ptr7->set_num_segments(0);
        rs_metas->push_back(ptr7);

        RowsetMetaSharedPtr ptr8(new RowsetMeta());
        init_rs_meta(ptr8, 9, 9);
        ptr8->set_num_segments(0);
        rs_metas->push_back(ptr8);

        RowsetMetaSharedPtr ptr9(new RowsetMeta());
        init_rs_meta(ptr9, 10, 10);
        ptr9->set_num_segments(0);
        rs_metas->push_back(ptr9);

        RowsetMetaSharedPtr ptr10(new RowsetMeta());
        init_rs_meta(ptr10, 11, 11);
        ptr10->set_total_disk_size(2 * 1024);
        rs_metas->push_back(ptr10);
    }

    void init_rs_meta_pick_empty(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        ptr1->set_total_disk_size(1 * 1024);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
        ptr2->set_total_disk_size(2 * 1024);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 4, 4);
        ptr3->set_total_disk_size(3 * 1024);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 5, 5);
        ptr4->set_segments_overlap(OVERLAPPING);
        ptr4->set_total_disk_size(4 * 1024);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 6);
        ptr5->set_segments_overlap(OVERLAPPING);
        ptr5->set_total_disk_size(5 * 1024);
        rs_metas->push_back(ptr5);
    }

    void init_all_rs_meta_delete(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 1);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 2, 3);
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

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, calculate_cumulative_point_overlap) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_cal_point(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    StorageEngine engine({});
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    EXPECT_EQ(4, _tablet->cumulative_layer_point());
}

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, calculate_cumulative_point_big_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_rowset(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    StorageEngine engine({});
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    EXPECT_EQ(4, _tablet->cumulative_layer_point());
}

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, calc_cumulative_compaction_score) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_normal(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    StorageEngine engine({});
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_TIME_SERIES_POLICY);
    const uint32_t score = _tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION,
                                                          cumulative_compaction_policy);

    EXPECT_EQ(0, score);
}

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, calc_cumulative_compaction_score_big_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_rowset(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    StorageEngine engine({});
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();
    std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_TIME_SERIES_POLICY);
    const uint32_t score = _tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION,
                                                          cumulative_compaction_policy);

    EXPECT_EQ(5, score);
}

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, pick_candidate_rowsets) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_normal(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    StorageEngine engine({});
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();
    EXPECT_EQ(5, candidate_rowsets.size());
}

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, pick_candidate_rowsets_big_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_rowset(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    StorageEngine engine({});
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();
    EXPECT_EQ(3, candidate_rowsets.size());
}

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, pick_input_rowsets_goal_size) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_big_rowset(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    StorageEngine engine({});
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
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

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, pick_input_rowsets_file_count) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_normal_size(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    StorageEngine engine({});
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
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

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, pick_input_rowsets_time_interval) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_normal_size_nonoverlapping(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    StorageEngine engine({});
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();
    int64_t now = UnixMillis();
    _tablet->set_last_cumu_compaction_success_time(now - 3700 * 1000);

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(3, input_rowsets.size());
    EXPECT_EQ(3, compaction_score);
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, pick_input_rowsets_empty) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_normal_size_nonoverlapping(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    StorageEngine engine({});
    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();
    int64_t now = UnixMillis();
    _tablet->set_last_cumu_compaction_success_time(now);

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

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, pick_input_rowsets_delete) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_delete(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(1, input_rowsets.size());
    EXPECT_EQ(3, compaction_score);
    EXPECT_EQ(5, last_delete_version.first);
    EXPECT_EQ(5, last_delete_version.second);
}

TEST_F(TestTimeSeriesCumulativeCompactionPolicy, pick_empty_rowsets) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_all_rs_meta_empty_nonoverlapping(&rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_TIME_SERIES_POLICY));
    static_cast<void>(_tablet->init());
    _tablet->calculate_cumulative_point();

    auto candidate_rowsets = _tablet->pick_candidate_rowsets_to_cumulative_compaction();

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;

    _tablet->_cumulative_compaction_policy->pick_input_rowsets(
            _tablet.get(), candidate_rowsets, 10, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(7, input_rowsets.size());
    EXPECT_EQ(-1, last_delete_version.first);
    EXPECT_EQ(-1, last_delete_version.second);
}

} // namespace doris

// @brief Test Stub
