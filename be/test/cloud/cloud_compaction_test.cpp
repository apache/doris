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

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "json2pb/json_to_pb.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/storage_policy.h"
#include "olap/tablet_meta.h"
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

class TestableCloudCompaction : public CloudCompactionMixin {
public:
    TestableCloudCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet)
            : CloudCompactionMixin(engine, tablet, "test_compaction") {}

    // Set input rowsets for testing
    void set_input_rowsets(const std::vector<RowsetSharedPtr>& rowsets) {
        _input_rowsets = rowsets;
    }

    Status prepare_compact() override { return Status::OK(); }

    ReaderType compaction_type() const override { return ReaderType::READER_CUMULATIVE_COMPACTION; }

    std::string_view compaction_name() const override { return "test_compaction"; }
};

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
    LOG(INFO) << "should_cache_compaction_output done";
}
} // namespace doris
