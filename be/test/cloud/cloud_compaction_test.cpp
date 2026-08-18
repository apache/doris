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

#include <memory>
#include <mutex>
#include <unordered_map>

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "cpp/sync_point.h"
#include "json2pb/json_to_pb.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_policy.h"
#include "storage/tablet/tablet_meta.h"
#include "util/defer_op.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
class TabletMap;

class CloudCompactionTest : public testing::Test {
    CloudCompactionTest() : _engine(CloudStorageEngine(EngineOptions {})) {}
    void SetUp() override {
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
        _cluster_info = std::make_shared<CloudClusterInfo>();
        _cluster_info->_is_in_standby = false;
        ExecEnv::GetInstance()->_cluster_info = _cluster_info.get();
    }
    void TearDown() override {}

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

public:
    CloudStorageEngine _engine;
    std::shared_ptr<CloudClusterInfo> _cluster_info;
};

TEST_F(CloudCompactionTest, failure_base_compaction_tablet_sleep_test) {
    auto filter_out = [](CloudTablet* t) { return false; };
    CloudTabletMgr mgr(_engine);

    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    CloudTabletSPtr tablet1 = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    for (auto& rs_meta : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rs_meta));
    }
    tablet1->tablet_meta()->_tablet_id = 10000;
    tablet1->set_last_base_compaction_failure_time(
            duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count() -
            100000);
    tablet1->set_last_base_compaction_failure_time(0);
    tablet1->tablet_meta()->tablet_schema()->set_disable_auto_compaction(false);
    tablet1->_approximate_num_rowsets = 10;
    mgr.put_tablet_for_UT(tablet1);

    int64_t max_score;
    std::vector<std::shared_ptr<CloudTablet>> tablets {};
    Status st = mgr.get_topn_tablets_to_compact(1, CompactionType::BASE_COMPACTION, filter_out,
                                                &tablets, &max_score);
    ASSERT_EQ(st, Status::OK());
    ASSERT_EQ(tablets.size(), 1);

    tablet1->set_last_base_compaction_failure_time(
            duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
    st = mgr.get_topn_tablets_to_compact(1, CompactionType::BASE_COMPACTION, filter_out, &tablets,
                                         &max_score);
    ASSERT_EQ(st, Status::OK());
    ASSERT_EQ(tablets.size(), 0);
}

TEST_F(CloudCompactionTest, failure_cumu_compaction_tablet_sleep_test) {
    auto filter_out = [](CloudTablet* t) { return false; };
    CloudTabletMgr mgr(_engine);

    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    CloudTabletSPtr tablet1 = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    for (auto& rs_meta : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rs_meta));
    }
    tablet1->tablet_meta()->_tablet_id = 10000;
    tablet1->set_last_cumu_compaction_failure_time(
            duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count() -
            100000);
    tablet1->set_last_cumu_compaction_failure_time(0);
    tablet1->tablet_meta()->tablet_schema()->set_disable_auto_compaction(false);
    tablet1->_approximate_cumu_num_deltas = 10;
    mgr.put_tablet_for_UT(tablet1);

    int64_t max_score;
    std::vector<std::shared_ptr<CloudTablet>> tablets {};
    Status st = mgr.get_topn_tablets_to_compact(1, CompactionType::CUMULATIVE_COMPACTION,
                                                filter_out, &tablets, &max_score);
    ASSERT_EQ(st, Status::OK());
    ASSERT_EQ(tablets.size(), 1);

    tablet1->set_last_cumu_compaction_failure_time(
            duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
    st = mgr.get_topn_tablets_to_compact(1, CompactionType::BASE_COMPACTION, filter_out, &tablets,
                                         &max_score);
    ASSERT_EQ(st, Status::OK());
    ASSERT_EQ(tablets.size(), 0);
}

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

static RowsetSharedPtr create_delete_rowset(Version version) {
    auto rowset = create_rowset(version, 0, false, 0);
    DORIS_CHECK(rowset != nullptr);
    DeletePredicatePB delete_predicate;
    delete_predicate.set_version(version.second);
    rowset->rowset_meta()->set_delete_predicate(std::move(delete_predicate));
    return rowset;
}

class TestableCloudCompaction : public CloudCompactionMixin {
public:
    TestableCloudCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet)
            : CloudCompactionMixin(engine, tablet, "test_compaction") {}

    // Set input rowsets for testing
    void set_input_rowsets(const std::vector<RowsetSharedPtr>& rowsets) {
        _input_rowsets = rowsets;
    }

    // Get input rowsets for verification
    const std::vector<RowsetSharedPtr>& get_input_rowsets() const { return _input_rowsets; }

    // Expose the protected method for testing
    size_t test_apply_txn_size_truncation_and_log(const std::string& compaction_name) {
        return apply_txn_size_truncation_and_log(compaction_name);
    }

    bool test_should_apply_cumulative_compaction_result(
            int64_t response_cumulative_compaction_cnt) {
        return should_apply_cumulative_compaction_result(response_cumulative_compaction_cnt);
    }

    Status prepare_compact() override { return Status::OK(); }

    ReaderType compaction_type() const override { return ReaderType::READER_CUMULATIVE_COMPACTION; }

    std::string_view compaction_name() const override { return "test_compaction"; }
};

TEST_F(CloudCompactionTest, cumulative_result_requires_next_counter) {
    auto tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    tablet->set_cumulative_compaction_cnt(1);
    tablet->last_sync_time_s = 1;
    TestableCloudCompaction compaction(_engine, tablet);

    std::unique_lock lock(tablet->get_header_lock());
    EXPECT_FALSE(compaction.test_should_apply_cumulative_compaction_result(1));
    EXPECT_EQ(tablet->last_sync_time_s, 1);
    EXPECT_TRUE(compaction.test_should_apply_cumulative_compaction_result(2));
    EXPECT_EQ(tablet->last_sync_time_s, 1);
    EXPECT_FALSE(compaction.test_should_apply_cumulative_compaction_result(3));
    EXPECT_EQ(tablet->last_sync_time_s, 0);
}

class TestableCloudCumulativeCompaction : public CloudCumulativeCompaction {
public:
    TestableCloudCumulativeCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet)
            : CloudCumulativeCompaction(engine, tablet) {}

    void set_input_rowsets(const std::vector<RowsetSharedPtr>& rowsets) {
        _input_rowsets = rowsets;
    }

    const std::vector<RowsetSharedPtr>& input_rowsets() const { return _input_rowsets; }

    void set_output_rowset(RowsetSharedPtr rowset) { _output_rowset = std::move(rowset); }

    Status test_modify_rowsets() { return modify_rowsets(); }
};

static TabletMetaSharedPtr create_cloud_compaction_test_tablet_meta(int64_t tablet_id) {
    return std::make_shared<TabletMeta>(1, 2, tablet_id, 15674, 4, 5, TTabletSchema(), 6,
                                        std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                        UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                        TCompressionType::LZ4F);
}

static CloudTabletSPtr create_cloud_tablet_with_rowsets(CloudStorageEngine& engine,
                                                        const TabletMetaSharedPtr& tablet_meta,
                                                        int64_t cumulative_point,
                                                        std::vector<RowsetSharedPtr> rowsets) {
    auto tablet = std::make_shared<CloudTablet>(engine, tablet_meta);
    auto num_rowsets = rowsets.size();
    {
        std::unique_lock wlock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, wlock, false);
    }
    tablet->set_cumulative_layer_point(cumulative_point);
    tablet->fetch_add_approximate_num_rowsets(static_cast<int64_t>(num_rowsets) -
                                              tablet->fetch_add_approximate_num_rowsets(0));
    tablet->last_sync_time_s = 1;
    return tablet;
}

static CloudTabletSPtr create_cloud_tablet_with_rowsets(CloudStorageEngine& engine,
                                                        const TabletMetaSharedPtr& tablet_meta,
                                                        int64_t cumulative_point,
                                                        const std::vector<int64_t>& versions,
                                                        int64_t data_size = 1024 * 1024) {
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(versions.size());
    for (int64_t version : versions) {
        auto rowset = create_rowset(Version(version, version), 1, false, data_size);
        DORIS_CHECK(rowset != nullptr);
        rowsets.push_back(rowset);
    }
    return create_cloud_tablet_with_rowsets(engine, tablet_meta, cumulative_point,
                                            std::move(rowsets));
}

static std::shared_ptr<TestableCloudCumulativeCompaction> create_inflight_cumu_compaction(
        CloudStorageEngine& engine, const CloudTabletSPtr& tablet, int64_t start, int64_t end) {
    std::vector<RowsetSharedPtr> input_rowsets;
    input_rowsets.reserve(end - start + 1);
    for (int64_t version = start; version <= end; ++version) {
        auto rowset = create_rowset(Version(version, version), 1, false, 1024 * 1024);
        DORIS_CHECK(rowset != nullptr);
        input_rowsets.push_back(rowset);
    }
    auto compaction = std::make_shared<TestableCloudCumulativeCompaction>(engine, tablet);
    compaction->set_input_rowsets(input_rowsets);
    return compaction;
}

TEST_F(CloudCompactionTest, base_result_with_newer_cumulative_point_forces_sync) {
    auto* sync_point = SyncPoint::get_instance();
    Defer clear_sync_points([&] {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    });
    int64_t response_cumulative_point = 6;
    sync_point->set_call_back("CloudMetaMgr::commit_tablet_job", [&](auto&& outcome) {
        auto* result = try_any_cast_ret<Status>(outcome);
        result->first = Status::OK();
        result->second = true;
        auto* response = try_any_cast<cloud::FinishTabletJobResponse*>(outcome[1]);
        response->mutable_status()->set_code(cloud::MetaServiceCode::OK);
        auto* stats = response->mutable_stats();
        stats->set_base_compaction_cnt(1);
        stats->set_cumulative_compaction_cnt(0);
        stats->set_cumulative_point(response_cumulative_point);
        stats->set_num_rowsets(1);
    });
    sync_point->enable_processing();

    auto run_case = [&](int64_t tablet_id, int64_t response_point, int64_t expected_sync_time) {
        auto input = create_rowset(Version(2, 7), 1, false, 1024);
        auto tablet = create_cloud_tablet_with_rowsets(
                _engine, create_cloud_compaction_test_tablet_meta(tablet_id), 6, {input});
        auto output = create_rowset(Version(2, 7), 1, false, 1024);
        CloudBaseCompaction compaction(_engine, tablet);
        compaction._input_rowsets = {input};
        compaction._output_rowset = output;
        response_cumulative_point = response_point;

        ASSERT_TRUE(compaction.modify_rowsets().ok());
        EXPECT_EQ(tablet->cumulative_layer_point(), 6);
        EXPECT_EQ(tablet->last_sync_time_s, expected_sync_time);
    };

    run_case(10008, 8, 0);
    run_case(10009, 6, 1);
}

TEST_F(CloudCompactionTest, cumulative_pick_uses_local_conflict_window) {
    auto old_min_deltas = config::cumulative_compaction_min_deltas;
    auto old_parallel_cumu_compaction = config::enable_parallel_cumu_compaction;
    Defer restore_config([&] {
        config::cumulative_compaction_min_deltas = old_min_deltas;
        config::enable_parallel_cumu_compaction = old_parallel_cumu_compaction;
    });
    config::cumulative_compaction_min_deltas = 2;
    config::enable_parallel_cumu_compaction = true;

    {
        auto tablet_meta = create_cloud_compaction_test_tablet_meta(10001);
        auto tablet =
                create_cloud_tablet_with_rowsets(_engine, tablet_meta, 114,
                                                 {114, 115, 116, 117, 118, 119, 120, 121, 122, 123,
                                                  124, 125, 126, 127, 128, 129, 130, 131, 132});
        _engine._submitted_cumu_compactions[tablet->tablet_id()] = {
                create_inflight_cumu_compaction(_engine, tablet, 117, 119),
                create_inflight_cumu_compaction(_engine, tablet, 126, 130)};

        TestableCloudCumulativeCompaction compaction(_engine, tablet);
        auto st = compaction.prepare_compact();
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(compaction.input_rowsets().size(), 3);
        EXPECT_EQ(compaction.input_rowsets().front()->start_version(), 114);
        EXPECT_EQ(compaction.input_rowsets().back()->end_version(), 116);
        _engine._submitted_cumu_compactions.clear();
    }

    {
        auto tablet_meta = create_cloud_compaction_test_tablet_meta(10002);
        auto tablet = create_cloud_tablet_with_rowsets(_engine, tablet_meta, 1, {1, 2, 3, 4});
        _engine._submitted_cumu_compactions[tablet->tablet_id()] = {
                create_inflight_cumu_compaction(_engine, tablet, 1, 2)};

        TestableCloudCumulativeCompaction compaction(_engine, tablet);
        auto st = compaction.prepare_compact();
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(compaction.input_rowsets().size(), 2);
        EXPECT_EQ(compaction.input_rowsets().front()->start_version(), 3);
        EXPECT_EQ(compaction.input_rowsets().back()->end_version(), 4);
        _engine._submitted_cumu_compactions.clear();
    }

    {
        auto tablet_meta = create_cloud_compaction_test_tablet_meta(10003);
        auto tablet = create_cloud_tablet_with_rowsets(_engine, tablet_meta, 114,
                                                       {114, 115, 116, 117, 118, 119});
        _engine._submitted_cumu_compactions[tablet->tablet_id()] = {
                create_inflight_cumu_compaction(_engine, tablet, 115, 116)};

        TestableCloudCumulativeCompaction compaction(_engine, tablet);
        auto st = compaction.prepare_compact();
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(compaction.input_rowsets().size(), 3);
        EXPECT_EQ(compaction.input_rowsets().front()->start_version(), 117);
        EXPECT_EQ(compaction.input_rowsets().back()->end_version(), 119);
        _engine._submitted_cumu_compactions.clear();
    }
}

TEST_F(CloudCompactionTest, serial_suffix_compaction_on_running_tablet_keeps_point) {
    auto old_parallel_cumu_compaction = config::enable_parallel_cumu_compaction;
    Defer restore_config(
            [&] { config::enable_parallel_cumu_compaction = old_parallel_cumu_compaction; });
    config::enable_parallel_cumu_compaction = false;

    auto* sync_point = SyncPoint::get_instance();
    Defer clear_sync_points([&] {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    });
    sync_point->set_call_back("CloudMetaMgr::commit_tablet_job", [&](auto&& outcome) {
        auto job = try_any_cast<cloud::TabletJobInfoPB>(outcome[0]);
        ASSERT_EQ(job.compaction_size(), 1);
        EXPECT_EQ(job.compaction(0).input_cumulative_point(), 2);
        EXPECT_EQ(job.compaction(0).output_cumulative_point(), 2);

        auto* result = try_any_cast_ret<Status>(outcome);
        result->first = Status::InternalError("stop after checking cumulative point");
        result->second = true;
    });
    sync_point->enable_processing();

    auto tablet_meta = create_cloud_compaction_test_tablet_meta(10007);
    auto tablet = create_cloud_tablet_with_rowsets(_engine, tablet_meta, 2, {2});
    TestableCloudCumulativeCompaction compaction(_engine, tablet);
    compaction.set_input_rowsets({create_rowset(Version(3, 22), 1, true, 20 * 1024 * 1024)});
    compaction.set_output_rowset(create_rowset(Version(3, 22), 1, false, 20 * 1024 * 1024));

    EXPECT_FALSE(compaction.test_modify_rowsets().ok());
}

TEST_F(CloudCompactionTest, parallel_time_series_pick_preserves_raw_singletons) {
    auto old_parallel_cumu_compaction = config::enable_parallel_cumu_compaction;
    Defer restore_config(
            [&] { config::enable_parallel_cumu_compaction = old_parallel_cumu_compaction; });
    config::enable_parallel_cumu_compaction = true;

    auto* sync_point = SyncPoint::get_instance();
    Defer clear_sync_points([&] {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    });
    bool point_update_called = false;
    sync_point->set_call_back("CloudMetaMgr::prepare_tablet_job", [&](auto&& outcome) {
        point_update_called = true;
        auto* result = try_any_cast_ret<Status>(outcome);
        result->first = Status::InternalError("unexpected cumulative point update");
        result->second = true;
    });
    sync_point->enable_processing();

    auto tablet_meta = create_cloud_compaction_test_tablet_meta(10008);
    tablet_meta->set_compaction_policy(std::string(CUMULATIVE_TIME_SERIES_POLICY));
    tablet_meta->set_time_series_compaction_level_threshold(1);
    std::vector<RowsetSharedPtr> rowsets {
            create_rowset(Version(2, 2), 1, false, 1024 * 1024),
            create_rowset(Version(3, 3), 1, false, 1024 * 1024),
    };
    for (const auto& rowset : rowsets) {
        rowset->rowset_meta()->set_creation_time(UnixSeconds());
    }
    auto tablet = create_cloud_tablet_with_rowsets(_engine, tablet_meta, 2, std::move(rowsets));
    TestableCloudCumulativeCompaction compaction(_engine, tablet);

    auto st = compaction.prepare_compact();
    EXPECT_TRUE(st.is<ErrorCode::CUMULATIVE_NO_SUITABLE_VERSION>()) << st;
    EXPECT_FALSE(point_update_called);
    EXPECT_EQ(tablet->cumulative_layer_point(), 2);
}

TEST_F(CloudCompactionTest, parallel_pick_keeps_mode_after_dynamic_config_change) {
    auto old_parallel_cumu_compaction = config::enable_parallel_cumu_compaction;
    Defer restore_config(
            [&] { config::enable_parallel_cumu_compaction = old_parallel_cumu_compaction; });
    config::enable_parallel_cumu_compaction = true;

    auto* sync_point = SyncPoint::get_instance();
    Defer clear_sync_points([&] {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    });

    bool prepare_called = false;
    sync_point->set_call_back("CloudMetaMgr::prepare_tablet_job", [&](auto&& outcome) {
        prepare_called = true;
        auto job = try_any_cast<cloud::TabletJobInfoPB>(outcome[0]);
        ASSERT_EQ(job.compaction_size(), 1);
        const auto& compaction = job.compaction(0);
        EXPECT_EQ(compaction.type(), cloud::TabletCompactionJobPB::EMPTY_CUMULATIVE);
        ASSERT_EQ(compaction.input_versions_size(), 2);
        EXPECT_EQ(compaction.input_versions(0), 2);
        EXPECT_EQ(compaction.input_versions(1), 4);
        EXPECT_TRUE(compaction.check_input_versions_range());
        EXPECT_EQ(compaction.base_compaction_cnt(), 0);
        EXPECT_EQ(compaction.cumulative_compaction_cnt(), 0);

        auto* result = try_any_cast_ret<Status>(outcome);
        result->first = Status::OK();
        result->second = true;
        auto* response = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
        response->mutable_status()->set_code(cloud::MetaServiceCode::OK);
    });

    bool commit_called = false;
    sync_point->set_call_back("CloudMetaMgr::commit_tablet_job", [&](auto&& outcome) {
        commit_called = true;
        auto job = try_any_cast<cloud::TabletJobInfoPB>(outcome[0]);
        ASSERT_EQ(job.compaction_size(), 1);
        const auto& compaction = job.compaction(0);
        EXPECT_EQ(compaction.input_cumulative_point(), 2);
        EXPECT_EQ(compaction.output_cumulative_point(), 5);

        auto* result = try_any_cast_ret<Status>(outcome);
        result->first = Status::OK();
        result->second = true;
        auto* response = try_any_cast<cloud::FinishTabletJobResponse*>(outcome[1]);
        response->mutable_status()->set_code(cloud::MetaServiceCode::OK);
        auto* stats = response->mutable_stats();
        stats->set_base_compaction_cnt(0);
        stats->set_cumulative_compaction_cnt(2);
        stats->set_cumulative_point(5);
        stats->set_num_rowsets(3);
        stats->set_num_segments(3);
        stats->set_num_rows(0);
        stats->set_data_size(300 * 1024 * 1024);
    });
    sync_point->enable_processing();

    auto tablet_meta = create_cloud_compaction_test_tablet_meta(10004);
    auto tablet =
            create_cloud_tablet_with_rowsets(_engine, tablet_meta, 2, {2, 3, 4}, 100 * 1024 * 1024);
    TestableCloudCumulativeCompaction compaction(_engine, tablet);
    config::enable_parallel_cumu_compaction = false;
    auto st = compaction.prepare_compact();

    EXPECT_TRUE(st.is<ErrorCode::CUMULATIVE_NO_SUITABLE_VERSION>()) << st;
    EXPECT_TRUE(prepare_called);
    EXPECT_TRUE(commit_called);
    EXPECT_EQ(tablet->cumulative_compaction_cnt(), 0);
    EXPECT_EQ(tablet->cumulative_layer_point(), 2);
    EXPECT_EQ(tablet->last_sync_time_s, 0);
}

TEST_F(CloudCompactionTest, parallel_pick_advances_continuous_low_prefix_through_delete) {
    auto old_parallel_cumu_compaction = config::enable_parallel_cumu_compaction;
    Defer restore_config(
            [&] { config::enable_parallel_cumu_compaction = old_parallel_cumu_compaction; });
    config::enable_parallel_cumu_compaction = true;

    auto* sync_point = SyncPoint::get_instance();
    Defer clear_sync_points([&] {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    });

    bool prepare_called = false;
    sync_point->set_call_back("CloudMetaMgr::prepare_tablet_job", [&](auto&& outcome) {
        prepare_called = true;
        auto job = try_any_cast<cloud::TabletJobInfoPB>(outcome[0]);
        ASSERT_EQ(job.compaction_size(), 1);
        const auto& compaction = job.compaction(0);
        EXPECT_EQ(compaction.type(), cloud::TabletCompactionJobPB::EMPTY_CUMULATIVE);
        ASSERT_EQ(compaction.input_versions_size(), 2);
        EXPECT_EQ(compaction.input_versions(0), 160);
        EXPECT_EQ(compaction.input_versions(1), 162);
        EXPECT_TRUE(compaction.check_input_versions_range());

        auto* result = try_any_cast_ret<Status>(outcome);
        result->first = Status::OK();
        result->second = true;
        auto* response = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
        response->mutable_status()->set_code(cloud::MetaServiceCode::OK);
    });

    bool commit_called = false;
    sync_point->set_call_back("CloudMetaMgr::commit_tablet_job", [&](auto&& outcome) {
        commit_called = true;
        auto job = try_any_cast<cloud::TabletJobInfoPB>(outcome[0]);
        ASSERT_EQ(job.compaction_size(), 1);
        const auto& compaction = job.compaction(0);
        EXPECT_EQ(compaction.input_cumulative_point(), 160);
        EXPECT_EQ(compaction.output_cumulative_point(), 163);

        auto* result = try_any_cast_ret<Status>(outcome);
        result->first = Status::OK();
        result->second = true;
        auto* response = try_any_cast<cloud::FinishTabletJobResponse*>(outcome[1]);
        response->mutable_status()->set_code(cloud::MetaServiceCode::OK);
        auto* stats = response->mutable_stats();
        stats->set_base_compaction_cnt(0);
        stats->set_cumulative_compaction_cnt(1);
        stats->set_cumulative_point(163);
        stats->set_num_rowsets(2);
        stats->set_num_segments(1);
        stats->set_num_rows(0);
        stats->set_data_size(1024);
    });
    sync_point->enable_processing();

    auto tablet_meta = create_cloud_compaction_test_tablet_meta(10005);
    std::vector<RowsetSharedPtr> rowsets {
            create_rowset(Version(160, 161), 1, false, 1024),
            create_delete_rowset(Version(162, 162)),
    };
    auto tablet = create_cloud_tablet_with_rowsets(_engine, tablet_meta, 160, std::move(rowsets));
    TestableCloudCumulativeCompaction compaction(_engine, tablet);
    auto st = compaction.prepare_compact();

    EXPECT_FALSE(st.ok()) << st;
    EXPECT_TRUE(prepare_called);
    EXPECT_TRUE(commit_called);
    EXPECT_EQ(tablet->cumulative_layer_point(), 163);
}

TEST_F(CloudCompactionTest, parallel_pick_does_not_advance_from_high_range_delete) {
    auto old_parallel_cumu_compaction = config::enable_parallel_cumu_compaction;
    Defer restore_config(
            [&] { config::enable_parallel_cumu_compaction = old_parallel_cumu_compaction; });
    config::enable_parallel_cumu_compaction = true;

    auto* sync_point = SyncPoint::get_instance();
    Defer clear_sync_points([&] {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    });
    bool prepare_called = false;
    sync_point->set_call_back("CloudMetaMgr::prepare_tablet_job", [&](auto&& outcome) {
        prepare_called = true;
        auto* result = try_any_cast_ret<Status>(outcome);
        result->first = Status::InternalError("unexpected cumulative point update");
        result->second = true;
    });
    sync_point->enable_processing();

    auto tablet_meta = create_cloud_compaction_test_tablet_meta(10006);
    std::vector<RowsetSharedPtr> rowsets {
            create_rowset(Version(100, 199), 1, false, 1024),
            create_rowset(Version(200, 300), 1, false, 1024),
            create_rowset(Version(301, 301), 1, false, 1024),
            create_delete_rowset(Version(302, 302)),
    };
    auto tablet = create_cloud_tablet_with_rowsets(_engine, tablet_meta, 100, std::move(rowsets));
    _engine._submitted_cumu_compactions[tablet->tablet_id()] = {
            create_inflight_cumu_compaction(_engine, tablet, 200, 300)};
    Defer clear_compactions([&] { _engine._submitted_cumu_compactions.clear(); });

    TestableCloudCumulativeCompaction compaction(_engine, tablet);
    auto st = compaction.prepare_compact();

    EXPECT_TRUE(st.is<ErrorCode::CUMULATIVE_NO_SUITABLE_VERSION>()) << st;
    EXPECT_FALSE(prepare_called);
    EXPECT_EQ(tablet->cumulative_layer_point(), 100);
}

TEST_F(CloudCompactionTest, test_set_storage_resource_from_input_rowsets) {
    S3Conf s3_conf {.bucket = "bucket",
                    .prefix = "prefix",
                    .client_conf = {
                            .endpoint = "endpoint",
                            .region = "region",
                            .ak = "ak",
                            .sk = "sk",
                            .token = "",
                            .bucket = "",
                            .role_arn = "",
                            .external_id = "",
                    }};
    std::string resource_id = "10000";
    auto res = io::S3FileSystem::create(std::move(s3_conf), resource_id);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto fs = res.value();
    StorageResource storage_resource(fs);

    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    TestableCloudCompaction compaction(_engine, tablet);

    // Test case 1: All rowsets are empty (num_segments = 0) - should succeed
    {
        std::vector<RowsetSharedPtr> rowsets;

        RowsetSharedPtr rowset1 = create_rowset(Version(2, 2), 0, false, 41);
        ASSERT_TRUE(rowset1 != nullptr);
        rowset1->set_hole_rowset(true); // Mark as hole rowset since num_segments=0
        rowsets.push_back(rowset1);

        RowsetSharedPtr rowset2 = create_rowset(Version(3, 3), 0, false, 41);
        ASSERT_TRUE(rowset2 != nullptr);
        rowset2->set_hole_rowset(true); // Mark as hole rowset since num_segments=0
        rowsets.push_back(rowset2);

        compaction.set_input_rowsets(rowsets);

        RowsetWriterContext ctx;
        Status st = compaction.set_storage_resource_from_input_rowsets(ctx);
        ASSERT_TRUE(st.ok()) << st.to_string();
        // No storage resource should be set since no rowset has resource_id
        ASSERT_FALSE(ctx.storage_resource.has_value());
    }

    // Test case 2: Backward iteration - last rowset has resource_id
    {
        std::vector<RowsetSharedPtr> rowsets;

        // First rowset: empty, no resource_id
        RowsetSharedPtr rowset1 = create_rowset(Version(2, 2), 0, false, 41);
        ASSERT_TRUE(rowset1 != nullptr);
        rowset1->set_hole_rowset(true);
        rowsets.push_back(rowset1);

        // Second rowset: empty, no resource_id
        RowsetSharedPtr rowset2 = create_rowset(Version(3, 3), 0, false, 41);
        ASSERT_TRUE(rowset2 != nullptr);
        rowset2->set_hole_rowset(true);
        rowsets.push_back(rowset2);

        // Third rowset: has resource_id (should be found during backward iteration)
        RowsetSharedPtr rowset3 = create_rowset(Version(4, 4), 1, false, 41);
        ASSERT_TRUE(rowset3 != nullptr);
        rowset3->rowset_meta()->set_remote_storage_resource(storage_resource);
        rowsets.push_back(rowset3);

        compaction.set_input_rowsets(rowsets);

        RowsetWriterContext ctx;
        Status st = compaction.set_storage_resource_from_input_rowsets(ctx);
        ASSERT_TRUE(st.ok()) << st.to_string();
        // Storage resource should be set from rowset3
        ASSERT_TRUE(ctx.storage_resource.has_value());
    }

    // Test case 3: Multiple rowsets with resource_id - should use the last one (backward iteration)
    {
        std::vector<RowsetSharedPtr> rowsets;

        // First rowset: has resource_id
        RowsetSharedPtr rowset1 = create_rowset(Version(2, 2), 1, false, 41);
        ASSERT_TRUE(rowset1 != nullptr);
        StorageResource first_resource(fs);
        rowset1->rowset_meta()->set_remote_storage_resource(first_resource);
        rowsets.push_back(rowset1);

        // Second rowset: empty, no resource_id
        RowsetSharedPtr rowset2 = create_rowset(Version(3, 3), 0, false, 41);
        ASSERT_TRUE(rowset2 != nullptr);
        rowset2->set_hole_rowset(true);
        rowsets.push_back(rowset2);

        // Third rowset: has different resource_id (should be used due to backward iteration)
        RowsetSharedPtr rowset3 = create_rowset(Version(4, 4), 1, false, 41);
        ASSERT_TRUE(rowset3 != nullptr);
        rowset3->rowset_meta()->set_remote_storage_resource(storage_resource);
        rowsets.push_back(rowset3);

        compaction.set_input_rowsets(rowsets);

        RowsetWriterContext ctx;
        Status st = compaction.set_storage_resource_from_input_rowsets(ctx);
        ASSERT_TRUE(st.ok()) << st.to_string();
        // Storage resource should be set from rowset3 (last one with resource_id)
        ASSERT_TRUE(ctx.storage_resource.has_value());
    }

    // Test case 4: Non-empty rowset in the middle without resource_id - should fail
    {
        std::vector<RowsetSharedPtr> rowsets;

        // First rowset: has resource_id
        RowsetSharedPtr rowset1 = create_rowset(Version(2, 2), 1, false, 41);
        ASSERT_TRUE(rowset1 != nullptr);
        rowset1->rowset_meta()->set_remote_storage_resource(storage_resource);
        rowsets.push_back(rowset1);

        // Second rowset: non-empty but no resource_id (invalid)
        RowsetSharedPtr rowset2 = create_rowset(Version(3, 3), 2, false, 41);
        ASSERT_TRUE(rowset2 != nullptr);
        // Intentionally don't set resource_id
        rowsets.push_back(rowset2);

        // Third rowset: empty, no resource_id
        RowsetSharedPtr rowset3 = create_rowset(Version(4, 4), 0, false, 41);
        ASSERT_TRUE(rowset3 != nullptr);
        rowset3->set_hole_rowset(true); // Mark as hole rowset since num_segments=0
        rowsets.push_back(rowset3);

        compaction.set_input_rowsets(rowsets);

        RowsetWriterContext ctx;
        Status st = compaction.set_storage_resource_from_input_rowsets(ctx);
        ASSERT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>());
        ASSERT_TRUE(st.to_string().find("Non-empty rowset must have valid resource_id") !=
                    std::string::npos)
                << st.to_string();
    }

    // Test case 5: Empty input rowsets - should succeed
    {
        std::vector<RowsetSharedPtr> rowsets; // Empty vector

        compaction.set_input_rowsets(rowsets);

        RowsetWriterContext ctx;
        Status st = compaction.set_storage_resource_from_input_rowsets(ctx);
        ASSERT_TRUE(st.ok()) << st.to_string();
        // No storage resource should be set
        ASSERT_FALSE(ctx.storage_resource.has_value());
    }
}
TEST_F(CloudCompactionTest, should_cache_compaction_output) {
    auto old_write_index_file_only = config::enable_file_cache_write_index_file_only;
    auto old_keep_base_compaction_output = config::enable_file_cache_keep_base_compaction_output;
    Defer restore_config {[&] {
        config::enable_file_cache_write_index_file_only = old_write_index_file_only;
        config::enable_file_cache_keep_base_compaction_output = old_keep_base_compaction_output;
    }};
    config::enable_file_cache_write_index_file_only = false;
    config::enable_file_cache_keep_base_compaction_output = false;

    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, std::make_shared<TabletMeta>());
    CloudBaseCompaction cloud_base_compaction(_engine, tablet);
    cloud_base_compaction._input_rowsets_total_size = 0;
    cloud_base_compaction._input_rowsets_cached_data_size = 0;
    cloud_base_compaction._input_rowsets_cached_index_size = 0;
    ASSERT_EQ(cloud_base_compaction.should_cache_compaction_output(), false);

    cloud_base_compaction._input_rowsets_total_size = 100;
    cloud_base_compaction._input_rowsets_cached_data_size = 0;
    cloud_base_compaction._input_rowsets_cached_index_size = 0;
    ASSERT_EQ(cloud_base_compaction.should_cache_compaction_output(), false);

    cloud_base_compaction._input_rowsets_total_size = 100;
    cloud_base_compaction._input_rowsets_cached_data_size = 70;
    cloud_base_compaction._input_rowsets_cached_index_size = 0;
    ASSERT_EQ(cloud_base_compaction.should_cache_compaction_output(), false);

    cloud_base_compaction._input_rowsets_total_size = 100;
    cloud_base_compaction._input_rowsets_cached_data_size = 0;
    cloud_base_compaction._input_rowsets_cached_index_size = 70;
    ASSERT_EQ(cloud_base_compaction.should_cache_compaction_output(), false);

    cloud_base_compaction._input_rowsets_total_size = 100;
    cloud_base_compaction._input_rowsets_cached_data_size = 0;
    cloud_base_compaction._input_rowsets_cached_index_size = 70;
    ASSERT_EQ(cloud_base_compaction.should_cache_compaction_output(), false);

    cloud_base_compaction._input_rowsets_total_size = 100;
    cloud_base_compaction._input_rowsets_cached_data_size = 80;
    cloud_base_compaction._input_rowsets_cached_index_size = 0;
    ASSERT_EQ(cloud_base_compaction.should_cache_compaction_output(), true);

    cloud_base_compaction._input_rowsets_total_size = 100;
    cloud_base_compaction._input_rowsets_cached_data_size = 0;
    cloud_base_compaction._input_rowsets_cached_index_size = 80;
    ASSERT_EQ(cloud_base_compaction.should_cache_compaction_output(), true);

    cloud_base_compaction._input_rowsets_total_size = 100;
    cloud_base_compaction._input_rowsets_cached_data_size = 50;
    cloud_base_compaction._input_rowsets_cached_index_size = 50;
    ASSERT_EQ(cloud_base_compaction.should_cache_compaction_output(), true);

    config::enable_file_cache_keep_base_compaction_output = true;
    ASSERT_EQ(cloud_base_compaction.should_cache_compaction_output(), true);

    config::enable_file_cache_write_index_file_only = true;
    ASSERT_EQ(cloud_base_compaction.should_cache_compaction_output(), false);
    LOG(INFO) << "should_cache_compaction_output done";
}

TEST_F(CloudCompactionTest, test_truncate_rowsets_by_txn_size_empty_input) {
    std::vector<RowsetSharedPtr> rowsets;
    int64_t kept_size = 100;
    int64_t truncated_size = 50;

    size_t truncated = cloud::truncate_rowsets_by_txn_size(rowsets, kept_size, truncated_size);

    ASSERT_EQ(truncated, 0);
    ASSERT_EQ(kept_size, 0);
    ASSERT_EQ(truncated_size, 0);
    ASSERT_EQ(rowsets.size(), 0);
}

TEST_F(CloudCompactionTest, test_truncate_rowsets_by_txn_size_single_rowset_under_limit) {
    // Create a single rowset
    std::vector<RowsetSharedPtr> rowsets;
    RowsetSharedPtr rowset1 = create_rowset(Version(2, 2), 1, false, 1024);
    ASSERT_TRUE(rowset1 != nullptr);
    rowsets.push_back(rowset1);

    // Set a large max size
    config::compaction_txn_max_size_bytes = 1024 * 1024 * 1024; // 1GB

    int64_t kept_size = 0;
    int64_t truncated_size = 0;

    size_t truncated = cloud::truncate_rowsets_by_txn_size(rowsets, kept_size, truncated_size);

    ASSERT_EQ(truncated, 0);
    ASSERT_EQ(rowsets.size(), 1);
    ASSERT_GT(kept_size, 0);
    ASSERT_EQ(truncated_size, 0);
}

TEST_F(CloudCompactionTest, test_truncate_rowsets_by_txn_size_multiple_rowsets_all_fit) {
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 0; i < 5; i++) {
        RowsetSharedPtr rowset = create_rowset(Version(i, i), 1, false, 1024);
        ASSERT_TRUE(rowset != nullptr);
        rowsets.push_back(rowset);
    }

    config::compaction_txn_max_size_bytes = 1024 * 1024 * 1024; // 1GB

    int64_t kept_size = 0;
    int64_t truncated_size = 0;

    size_t truncated = cloud::truncate_rowsets_by_txn_size(rowsets, kept_size, truncated_size);

    ASSERT_EQ(truncated, 0);
    ASSERT_EQ(rowsets.size(), 5);
    ASSERT_GT(kept_size, 0);
    ASSERT_EQ(truncated_size, 0);
}

TEST_F(CloudCompactionTest, test_truncate_rowsets_by_txn_size_exceeds_limit) {
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 0; i < 10; i++) {
        RowsetSharedPtr rowset = create_rowset(Version(i, i), 1, false, 1024);
        ASSERT_TRUE(rowset != nullptr);
        rowsets.push_back(rowset);
    }

    // Set a very small max size to force truncation
    config::compaction_txn_max_size_bytes = 50; // 50 bytes, should keep only a few rowsets

    int64_t kept_size = 0;
    int64_t truncated_size = 0;

    size_t truncated = cloud::truncate_rowsets_by_txn_size(rowsets, kept_size, truncated_size);

    // Should truncate some rowsets
    ASSERT_GT(truncated, 0);
    ASSERT_LT(rowsets.size(), 10);
    ASSERT_GT(rowsets.size(), 0); // At least 1 rowset kept
    ASSERT_GT(truncated_size, 0);
}

TEST_F(CloudCompactionTest, test_truncate_rowsets_by_txn_size_first_rowset_exceeds_limit) {
    std::vector<RowsetSharedPtr> rowsets;
    RowsetSharedPtr rowset1 = create_rowset(Version(0, 0), 1, false, 1024);
    ASSERT_TRUE(rowset1 != nullptr);
    rowsets.push_back(rowset1);

    // Set max size smaller than the first rowset's metadata size
    config::compaction_txn_max_size_bytes = 1; // 1 byte

    int64_t kept_size = 0;
    int64_t truncated_size = 0;

    size_t truncated = cloud::truncate_rowsets_by_txn_size(rowsets, kept_size, truncated_size);

    // Should keep at least 1 rowset even if it exceeds the limit
    ASSERT_EQ(truncated, 0);
    ASSERT_EQ(rowsets.size(), 1);
    ASSERT_GT(kept_size, config::compaction_txn_max_size_bytes);
    ASSERT_EQ(truncated_size, 0);
}

TEST_F(CloudCompactionTest, test_truncate_rowsets_by_txn_size_exact_boundary) {
    std::vector<RowsetSharedPtr> rowsets;
    RowsetSharedPtr rowset1 = create_rowset(Version(0, 0), 1, false, 1024);
    ASSERT_TRUE(rowset1 != nullptr);
    rowsets.push_back(rowset1);

    // Get the actual size of the first rowset
    int64_t first_kept_size = 0;
    int64_t first_truncated_size = 0;

    std::vector<RowsetSharedPtr> temp_rowsets = {rowset1};
    cloud::truncate_rowsets_by_txn_size(temp_rowsets, first_kept_size, first_truncated_size);

    // Add more rowsets
    for (int i = 1; i < 5; i++) {
        RowsetSharedPtr rowset = create_rowset(Version(i, i), 1, false, 1024);
        ASSERT_TRUE(rowset != nullptr);
        rowsets.push_back(rowset);
    }

    // Set max size to exactly the size of first rowset
    config::compaction_txn_max_size_bytes = first_kept_size;

    int64_t kept_size = 0;
    int64_t truncated_size = 0;

    size_t truncated = cloud::truncate_rowsets_by_txn_size(rowsets, kept_size, truncated_size);

    // Should keep only 1 rowset at the boundary
    ASSERT_EQ(rowsets.size(), 1);
    ASSERT_EQ(truncated, 4);
    ASSERT_EQ(kept_size, first_kept_size);
    ASSERT_GT(truncated_size, 0);
}

TEST_F(CloudCompactionTest, test_truncate_rowsets_by_txn_size_output_parameters) {
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 0; i < 3; i++) {
        RowsetSharedPtr rowset = create_rowset(Version(i, i), 1, false, 1024);
        ASSERT_TRUE(rowset != nullptr);
        rowsets.push_back(rowset);
    }

    config::compaction_txn_max_size_bytes = 1024 * 1024;

    int64_t kept_size = 0;
    int64_t truncated_size = 0;

    size_t truncated = cloud::truncate_rowsets_by_txn_size(rowsets, kept_size, truncated_size);

    // Verify output parameters are set correctly
    ASSERT_EQ(truncated, 0); // All rowsets fit
    ASSERT_EQ(rowsets.size(), 3);
    ASSERT_GT(kept_size, 0);
    ASSERT_EQ(truncated_size, 0);
}

TEST_F(CloudCompactionTest, test_apply_txn_size_truncation_and_log_empty_input) {
    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    TestableCloudCompaction compaction(_engine, tablet);

    // Test with empty input rowsets
    std::vector<RowsetSharedPtr> empty_rowsets;
    compaction.set_input_rowsets(empty_rowsets);

    size_t truncated = compaction.test_apply_txn_size_truncation_and_log("test_compaction");

    ASSERT_EQ(truncated, 0);
    ASSERT_EQ(compaction.get_input_rowsets().size(), 0);
}

TEST_F(CloudCompactionTest, test_apply_txn_size_truncation_and_log_no_truncation) {
    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    TestableCloudCompaction compaction(_engine, tablet);

    // Create rowsets that fit within the limit
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 0; i < 3; i++) {
        RowsetSharedPtr rowset = create_rowset(Version(i, i), 1, false, 1024);
        ASSERT_TRUE(rowset != nullptr);
        rowsets.push_back(rowset);
    }

    compaction.set_input_rowsets(rowsets);

    // Set a large max size
    config::compaction_txn_max_size_bytes = 1024 * 1024 * 1024; // 1GB

    size_t truncated = compaction.test_apply_txn_size_truncation_and_log("test_compaction");

    ASSERT_EQ(truncated, 0);
    ASSERT_EQ(compaction.get_input_rowsets().size(), 3);
}

TEST_F(CloudCompactionTest, test_apply_txn_size_truncation_and_log_with_truncation) {
    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    TestableCloudCompaction compaction(_engine, tablet);

    // Create multiple rowsets
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 0; i < 10; i++) {
        RowsetSharedPtr rowset = create_rowset(Version(i, i), 1, false, 1024);
        ASSERT_TRUE(rowset != nullptr);
        rowsets.push_back(rowset);
    }

    compaction.set_input_rowsets(rowsets);

    // Set a small max size to force truncation
    config::compaction_txn_max_size_bytes = 100; // Very small to force truncation

    size_t truncated = compaction.test_apply_txn_size_truncation_and_log("test_compaction");

    // Should have truncated some rowsets
    ASSERT_GT(truncated, 0);
    ASSERT_LT(compaction.get_input_rowsets().size(), 10);
    ASSERT_GT(compaction.get_input_rowsets().size(), 0);
}

TEST_F(CloudCompactionTest, test_apply_txn_size_truncation_and_log_version_range) {
    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    TestableCloudCompaction compaction(_engine, tablet);

    // Create rowsets with consecutive versions
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 10; i < 20; i++) {
        RowsetSharedPtr rowset = create_rowset(Version(i, i), 1, false, 1024);
        ASSERT_TRUE(rowset != nullptr);
        rowsets.push_back(rowset);
    }

    compaction.set_input_rowsets(rowsets);

    // Set a size that will keep first 5 rowsets
    config::compaction_txn_max_size_bytes = 100; // Small enough to truncate

    size_t truncated = compaction.test_apply_txn_size_truncation_and_log("base_compaction");

    if (truncated > 0) {
        // Verify that the version range is adjusted correctly
        ASSERT_GT(compaction.get_input_rowsets().size(), 0);
        // First rowset should still start at version 10
        ASSERT_EQ(compaction.get_input_rowsets().front()->start_version(), 10);
        // Last rowset version should be less than 19
        ASSERT_LT(compaction.get_input_rowsets().back()->end_version(), 20);
    }
}

TEST_F(CloudCompactionTest, test_apply_txn_size_truncation_and_log_single_large_rowset) {
    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    TestableCloudCompaction compaction(_engine, tablet);

    // Create a single large rowset
    std::vector<RowsetSharedPtr> rowsets;
    RowsetSharedPtr rowset = create_rowset(Version(0, 0), 1, false, 1024 * 1024);
    ASSERT_TRUE(rowset != nullptr);
    rowsets.push_back(rowset);

    compaction.set_input_rowsets(rowsets);

    // Set max size smaller than the rowset's metadata size
    config::compaction_txn_max_size_bytes = 1;

    size_t truncated = compaction.test_apply_txn_size_truncation_and_log("cumu_compaction");

    // Should keep at least 1 rowset even if it exceeds the limit
    ASSERT_EQ(truncated, 0);
    ASSERT_EQ(compaction.get_input_rowsets().size(), 1);
}
} // namespace doris
