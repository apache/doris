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
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

static constexpr int64_t kMiB = 1024L * 1024;
static constexpr int64_t kGiB = 1024L * kMiB;

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

    std::vector<RowsetMetaSharedPtr> create_rs_meta_max_score_trim(bool include_stranded_head) {
        std::vector<RowsetMetaSharedPtr> rs_metas;
        auto add_rowset = [&](int64_t start_version, int64_t end_version, int num_segments,
                              bool overlapping, int64_t total_disk_size) {
            RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
            init_rs_meta(rowset_meta, start_version, end_version);
            rowset_meta->set_num_segments(num_segments);
            rowset_meta->set_segments_overlap(overlapping ? OVERLAPPING : NONOVERLAPPING);
            rowset_meta->set_total_disk_size(total_disk_size);
            rs_metas.push_back(rowset_meta);
        };

        add_rowset(0, include_stranded_head ? 12 : 56, 1, false, kGiB);
        if (include_stranded_head) {
            add_rowset(13, 56, 0, false, 0);
        }
        add_rowset(57, 57, 192, true, 256L * kMiB);
        add_rowset(58, 58, 0, false, 0);
        add_rowset(59, 59, 0, false, 0);
        add_rowset(60, 60, 150, true, 256L * kMiB);
        for (int64_t version = 61; version <= 67; ++version) {
            add_rowset(version, version, 1, false, kMiB);
        }
        return rs_metas;
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
    const uint32_t score = _tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION,
                                                          cumulative_compaction_policy);

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
    const uint32_t score = _tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION,
                                                          cumulative_compaction_policy);

    EXPECT_EQ(7, score);
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

    EXPECT_EQ(4, input_rowsets.size());
    EXPECT_EQ(12, compaction_score);
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

TEST_F(TestSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_large_head_not_repeated_when_output_below_promotion) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    RowsetMetaSharedPtr base_rs(new RowsetMeta());
    init_rs_meta(base_rs, 0, 1);
    base_rs->set_total_disk_size(20L * kGiB);
    base_rs->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(base_rs);

    RowsetMetaSharedPtr large_head(new RowsetMeta());
    init_rs_meta(large_head, 2, 2);
    large_head->set_total_disk_size(1023L * kMiB);
    large_head->set_num_segments(1);
    large_head->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(large_head);

    for (int i = 0; i < 20; i++) {
        RowsetMetaSharedPtr ptr(new RowsetMeta());
        init_rs_meta(ptr, i + 3, i + 3);
        ptr->set_total_disk_size(kMiB);
        ptr->set_num_segments(1);
        ptr->set_segments_overlap(OVERLAPPING);
        rs_metas.push_back(ptr);
    }

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(tablet->init());
    tablet->calculate_cumulative_point();
    ASSERT_EQ(2, tablet->cumulative_layer_point());
    ASSERT_EQ(kGiB, tablet->cumulative_promotion_size());

    auto candidate_rowsets = tablet->pick_candidate_rowsets_to_cumulative_compaction();
    ASSERT_EQ(21, candidate_rowsets.size());

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    tablet->_cumulative_compaction_policy->pick_input_rowsets(
            tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(20, input_rowsets.size());
    EXPECT_EQ(20, compaction_score);
    EXPECT_EQ(3, input_rowsets.front()->start_version());
    EXPECT_EQ(22, input_rowsets.back()->end_version());

    RowsetMetaSharedPtr output_meta(new RowsetMeta());
    init_rs_meta(output_meta, 3, 22);
    output_meta->set_total_disk_size(20L * kMiB);
    output_meta->set_num_segments(1);
    output_meta->set_segments_overlap(NONOVERLAPPING);
    RowsetSharedPtr output_rowset;
    ASSERT_TRUE(RowsetFactory::create_rowset(nullptr, "", output_meta, &output_rowset).ok());

    tablet->_cumulative_compaction_policy->update_cumulative_point(
            tablet.get(), input_rowsets, output_rowset, last_delete_version);
    EXPECT_EQ(2, tablet->cumulative_layer_point());

    std::vector<RowsetSharedPtr> next_candidate_rowsets {candidate_rowsets.front(), output_rowset};
    input_rowsets.clear();
    compaction_score = 0;
    tablet->_cumulative_compaction_policy->pick_input_rowsets(
            tablet.get(), next_candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_TRUE(input_rowsets.empty());
    EXPECT_EQ(0, compaction_score);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_large_head_single_overlapping_tail_selected) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    RowsetMetaSharedPtr base_rs(new RowsetMeta());
    init_rs_meta(base_rs, 0, 1);
    base_rs->set_total_disk_size(20L * kGiB);
    base_rs->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(base_rs);

    RowsetMetaSharedPtr large_head(new RowsetMeta());
    init_rs_meta(large_head, 2, 2);
    large_head->set_total_disk_size(900L * kMiB);
    large_head->set_num_segments(1);
    large_head->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(large_head);

    RowsetMetaSharedPtr tail(new RowsetMeta());
    init_rs_meta(tail, 3, 3);
    tail->set_total_disk_size(128L * kMiB);
    tail->set_num_segments(5);
    tail->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(tail);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(tablet->init());
    tablet->calculate_cumulative_point();
    ASSERT_EQ(2, tablet->cumulative_layer_point());
    ASSERT_EQ(kGiB, tablet->cumulative_promotion_size());

    auto candidate_rowsets = tablet->pick_candidate_rowsets_to_cumulative_compaction();
    ASSERT_EQ(2, candidate_rowsets.size());

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    tablet->_cumulative_compaction_policy->pick_input_rowsets(
            tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    ASSERT_EQ(1, input_rowsets.size());
    EXPECT_EQ(5, compaction_score);
    EXPECT_EQ(3, input_rowsets.front()->start_version());
    EXPECT_EQ(128L * kMiB, input_rowsets.front()->total_disk_size());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_single_overlapping_rowset_not_trimmed_empty) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    RowsetMetaSharedPtr base_rs(new RowsetMeta());
    init_rs_meta(base_rs, 0, 1);
    base_rs->set_total_disk_size(20L * kGiB);
    base_rs->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(base_rs);

    RowsetMetaSharedPtr ptr(new RowsetMeta());
    init_rs_meta(ptr, 2, 2);
    ptr->set_total_disk_size(2L * kGiB);
    ptr->set_num_segments(3);
    ptr->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(ptr);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(tablet->init());
    tablet->calculate_cumulative_point();

    auto candidate_rowsets = tablet->pick_candidate_rowsets_to_cumulative_compaction();
    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    tablet->_cumulative_compaction_policy->pick_input_rowsets(
            tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_EQ(1, input_rowsets.size());
    EXPECT_EQ(3, compaction_score);
    EXPECT_EQ(2, input_rowsets.front()->start_version());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_single_non_overlapping_rowset_still_skipped) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    RowsetMetaSharedPtr base_rs(new RowsetMeta());
    init_rs_meta(base_rs, 0, 1);
    base_rs->set_total_disk_size(20L * kGiB);
    base_rs->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(base_rs);

    RowsetMetaSharedPtr ptr(new RowsetMeta());
    init_rs_meta(ptr, 2, 2);
    ptr->set_total_disk_size(2L * kGiB);
    ptr->set_num_segments(1);
    ptr->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(ptr);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(tablet->init());
    tablet->calculate_cumulative_point();

    std::vector<RowsetSharedPtr> candidate_rowsets;
    RowsetSharedPtr rowset;
    ASSERT_TRUE(RowsetFactory::create_rowset(nullptr, "", ptr, &rowset).ok());
    candidate_rowsets.push_back(rowset);

    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    tablet->_cumulative_compaction_policy->pick_input_rowsets(
            tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    EXPECT_TRUE(input_rowsets.empty());
    EXPECT_EQ(0, compaction_score);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_keep_final_overlapping_below_max) {
    std::vector<RowsetMetaSharedPtr> rs_metas;

    RowsetMetaSharedPtr base_rs(new RowsetMeta());
    init_rs_meta(base_rs, 0, 1);
    base_rs->set_total_disk_size(20L * kGiB);
    base_rs->set_segments_overlap(NONOVERLAPPING);
    rs_metas.push_back(base_rs);

    RowsetMetaSharedPtr large_head(new RowsetMeta());
    init_rs_meta(large_head, 2, 2);
    large_head->set_total_disk_size(200L * kMiB);
    large_head->set_num_segments(30);
    large_head->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(large_head);

    RowsetMetaSharedPtr final_suffix(new RowsetMeta());
    init_rs_meta(final_suffix, 3, 3);
    final_suffix->set_total_disk_size(100L * kMiB);
    final_suffix->set_num_segments(20);
    final_suffix->set_segments_overlap(OVERLAPPING);
    rs_metas.push_back(final_suffix);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(tablet->init());
    tablet->calculate_cumulative_point();

    auto candidate_rowsets = tablet->pick_candidate_rowsets_to_cumulative_compaction();
    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    tablet->_cumulative_compaction_policy->pick_input_rowsets(
            tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    ASSERT_EQ(1, input_rowsets.size());
    EXPECT_EQ(20, compaction_score);
    EXPECT_EQ(3, input_rowsets.front()->start_version());
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_preserves_successor_for_non_overlapping_singleton) {
    auto rs_metas = create_rs_meta_max_score_trim(true);
    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(tablet->init());
    tablet->calculate_cumulative_point();
    EXPECT_EQ(13, tablet->cumulative_layer_point());

    auto candidate_rowsets = tablet->pick_candidate_rowsets_to_cumulative_compaction();
    ASSERT_EQ(12, candidate_rowsets.size());
    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    tablet->_cumulative_compaction_policy->pick_input_rowsets(
            tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    ASSERT_EQ(2, input_rowsets.size());
    EXPECT_EQ(Version(13, 56), input_rowsets[0]->version());
    EXPECT_EQ(Version(57, 57), input_rowsets[1]->version());
    EXPECT_GE(compaction_score, 192);
}

TEST_F(TestSizeBasedCumulativeCompactionPolicy,
       pick_input_rowsets_keeps_single_overlapping_rowset_at_max_score) {
    auto rs_metas = create_rs_meta_max_score_trim(false);
    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr tablet(
            new Tablet(_engine, _tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    static_cast<void>(tablet->init());
    tablet->calculate_cumulative_point();
    EXPECT_EQ(57, tablet->cumulative_layer_point());

    auto candidate_rowsets = tablet->pick_candidate_rowsets_to_cumulative_compaction();
    ASSERT_EQ(11, candidate_rowsets.size());
    std::vector<RowsetSharedPtr> input_rowsets;
    Version last_delete_version {-1, -1};
    size_t compaction_score = 0;
    tablet->_cumulative_compaction_policy->pick_input_rowsets(
            tablet.get(), candidate_rowsets, 100, 5, &input_rowsets, &last_delete_version,
            &compaction_score, config::enable_delete_when_cumu_compaction);

    ASSERT_EQ(1, input_rowsets.size());
    EXPECT_EQ(Version(57, 57), input_rowsets.front()->version());
    EXPECT_EQ(192, compaction_score);
}

} // namespace doris

// @brief Test Stub
