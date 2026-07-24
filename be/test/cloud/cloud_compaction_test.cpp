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
#include <string>

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "json2pb/json_to_pb.h"
#include "storage/compaction/cumulative_compaction_time_series_policy.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_policy.h"
#include "storage/tablet/tablet_meta.h"
#include "util/defer_op.h"
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
                                     int data_size, int num_key_columns = 1) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_rowset_type(BETA_ROWSET); // important
    rs_meta->_rowset_meta_pb.set_start_version(version.first);
    rs_meta->_rowset_meta_pb.set_end_version(version.second);
    rs_meta->set_num_segments(num_segments);
    rs_meta->set_segments_overlap(overlapping ? OVERLAPPING : NONOVERLAPPING);
    rs_meta->set_total_disk_size(data_size);
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_keys_type(DUP_KEYS);
    for (int i = 0; i < num_key_columns + 1; ++i) {
        ColumnPB* column = tablet_schema_pb.add_column();
        column->set_unique_id(i);
        column->set_name("c" + std::to_string(i));
        column->set_type("INT");
        column->set_is_key(i < num_key_columns);
        column->set_is_nullable(false);
    }
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(tablet_schema_pb);
    rs_meta->set_tablet_schema(tablet_schema);
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

    // Get input rowsets for verification
    const std::vector<RowsetSharedPtr>& get_input_rowsets() const { return _input_rowsets; }

    // Expose the protected method for testing
    size_t test_apply_txn_size_truncation_and_log(const std::string& compaction_name) {
        return apply_txn_size_truncation_and_log(compaction_name);
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

TEST_F(CloudCompactionTest, single_rowset_grouped_compaction_execution_path_conditions) {
    auto old_enable = config::enable_cloud_single_rowset_compaction;
    auto old_min_segments = config::cloud_single_rowset_compaction_min_segments;
    auto old_group_size = config::cloud_single_rowset_compaction_segment_group_size;
    Defer restore_config {[&] {
        config::enable_cloud_single_rowset_compaction = old_enable;
        config::cloud_single_rowset_compaction_min_segments = old_min_segments;
        config::cloud_single_rowset_compaction_segment_group_size = old_group_size;
    }};
    config::enable_cloud_single_rowset_compaction = true;
    config::cloud_single_rowset_compaction_min_segments = 4;
    config::cloud_single_rowset_compaction_segment_group_size = 2;

    RowsetSharedPtr candidate = create_rowset(Version(2, 2), 4, true, 1024);
    ASSERT_TRUE(candidate != nullptr);
    const auto& tablet_schema = *candidate->tablet_schema();
    EXPECT_TRUE(cloud::is_single_rowset_compaction_candidate(candidate));
    EXPECT_TRUE(cloud::should_use_single_rowset_grouped_compaction({candidate}, tablet_schema,
                                                                   CUMULATIVE_SIZE_BASED_POLICY));
    EXPECT_FALSE(cloud::should_use_single_rowset_grouped_compaction({candidate}, tablet_schema,
                                                                    CUMULATIVE_TIME_SERIES_POLICY));

    TabletSchemaPB cluster_key_schema_pb;
    tablet_schema.to_schema_pb(&cluster_key_schema_pb);
    cluster_key_schema_pb.set_keys_type(UNIQUE_KEYS);
    cluster_key_schema_pb.add_cluster_key_uids(1);
    TabletSchema cluster_key_schema;
    cluster_key_schema.init_from_pb(cluster_key_schema_pb);
    EXPECT_FALSE(cloud::should_use_single_rowset_grouped_compaction({candidate}, cluster_key_schema,
                                                                    CUMULATIVE_SIZE_BASED_POLICY));

    config::enable_cloud_single_rowset_compaction = false;
    EXPECT_TRUE(cloud::is_single_rowset_compaction_candidate(candidate));
    EXPECT_FALSE(cloud::should_use_single_rowset_grouped_compaction({candidate}, tablet_schema,
                                                                    CUMULATIVE_SIZE_BASED_POLICY));
    config::enable_cloud_single_rowset_compaction = true;

    RowsetSharedPtr non_overlapping = create_rowset(Version(3, 3), 4, false, 1024);
    ASSERT_TRUE(non_overlapping != nullptr);
    EXPECT_FALSE(cloud::is_single_rowset_compaction_candidate(non_overlapping));
    EXPECT_FALSE(cloud::should_use_single_rowset_grouped_compaction(
            {non_overlapping}, tablet_schema, CUMULATIVE_SIZE_BASED_POLICY));

    RowsetSharedPtr too_few_segments = create_rowset(Version(4, 4), 3, true, 1024);
    ASSERT_TRUE(too_few_segments != nullptr);
    EXPECT_FALSE(cloud::is_single_rowset_compaction_candidate(too_few_segments));
    EXPECT_FALSE(cloud::should_use_single_rowset_grouped_compaction(
            {too_few_segments}, tablet_schema, CUMULATIVE_SIZE_BASED_POLICY));

    RowsetSharedPtr no_key_columns = create_rowset(Version(5, 5), 4, true, 1024, 0);
    ASSERT_TRUE(no_key_columns != nullptr);
    EXPECT_TRUE(cloud::is_single_rowset_compaction_candidate(no_key_columns));
    EXPECT_FALSE(cloud::should_use_single_rowset_grouped_compaction(
            {no_key_columns}, *no_key_columns->tablet_schema(), CUMULATIVE_SIZE_BASED_POLICY));

    RowsetSharedPtr with_delete_predicate = create_rowset(Version(6, 6), 4, true, 1024);
    ASSERT_TRUE(with_delete_predicate != nullptr);
    DeletePredicatePB delete_predicate;
    auto* in_predicate = delete_predicate.add_in_predicates();
    in_predicate->set_column_name("c1");
    in_predicate->add_values("1");
    with_delete_predicate->rowset_meta()->set_delete_predicate(std::move(delete_predicate));
    EXPECT_FALSE(cloud::is_single_rowset_compaction_candidate(with_delete_predicate));
    EXPECT_FALSE(cloud::should_use_single_rowset_grouped_compaction(
            {with_delete_predicate}, tablet_schema, CUMULATIVE_SIZE_BASED_POLICY));

    RowsetSharedPtr another_candidate = create_rowset(Version(7, 7), 4, true, 1024);
    ASSERT_TRUE(another_candidate != nullptr);
    EXPECT_FALSE(cloud::should_use_single_rowset_grouped_compaction(
            {candidate, another_candidate}, tablet_schema, CUMULATIVE_SIZE_BASED_POLICY));
    EXPECT_FALSE(cloud::should_use_single_rowset_grouped_compaction({}, tablet_schema,
                                                                    CUMULATIVE_SIZE_BASED_POLICY));

    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    CloudCumulativeCompaction compaction(_engine, tablet);
    compaction._input_rowsets = {candidate};
    compaction._cur_tablet_schema = candidate->tablet_schema();
    compaction._single_rowset_compaction_segment_group_size =
            config::cloud_single_rowset_compaction_segment_group_size;
    Compaction::MergeInputRowsetsResult result;
    ASSERT_TRUE(compaction.prepare_merge_input_rowsets(&result).ok());
    EXPECT_TRUE(compaction._single_rowset_compaction_segment_group_size.has_value());
    EXPECT_TRUE(result.is_segment_grouped);
    EXPECT_EQ(result.segment_group_size, config::cloud_single_rowset_compaction_segment_group_size);

    _tablet_meta->set_compaction_policy(std::string(CUMULATIVE_TIME_SERIES_POLICY));
    CloudCumulativeCompaction time_series_compaction(_engine, tablet);
    time_series_compaction._input_rowsets = {candidate};
    time_series_compaction._cur_tablet_schema = candidate->tablet_schema();
    Compaction::MergeInputRowsetsResult time_series_result;
    ASSERT_TRUE(time_series_compaction.prepare_merge_input_rowsets(&time_series_result).ok());
    EXPECT_FALSE(time_series_compaction._single_rowset_compaction_segment_group_size.has_value());
    EXPECT_FALSE(time_series_result.is_segment_grouped);
}

TEST_F(CloudCompactionTest, single_rowset_grouped_compaction_rejects_invalid_group_size) {
    auto old_enable = config::enable_cloud_single_rowset_compaction;
    auto old_min_segments = config::cloud_single_rowset_compaction_min_segments;
    auto old_group_size = config::cloud_single_rowset_compaction_segment_group_size;
    Defer restore_config {[&] {
        config::enable_cloud_single_rowset_compaction = old_enable;
        config::cloud_single_rowset_compaction_min_segments = old_min_segments;
        config::cloud_single_rowset_compaction_segment_group_size = old_group_size;
    }};
    config::enable_cloud_single_rowset_compaction = true;
    config::cloud_single_rowset_compaction_min_segments = 4;
    config::cloud_single_rowset_compaction_segment_group_size = 0;

    RowsetSharedPtr candidate = create_rowset(Version(2, 2), 4, true, 1024);
    ASSERT_TRUE(candidate != nullptr);

    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    CloudCumulativeCompaction compaction(_engine, tablet);
    compaction._input_rowsets = {candidate};
    compaction._cur_tablet_schema = candidate->tablet_schema();
    compaction._single_rowset_compaction_segment_group_size =
            config::cloud_single_rowset_compaction_segment_group_size;

    Compaction::MergeInputRowsetsResult result;
    Status st = compaction.prepare_merge_input_rowsets(&result);
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st;
    EXPECT_TRUE(st.to_string().find(
                        "cloud_single_rowset_compaction_segment_group_size must be positive") !=
                std::string::npos)
            << st;
}

TEST_F(CloudCompactionTest, single_rowset_grouped_compaction_uses_selection_snapshot) {
    auto old_enable = config::enable_cloud_single_rowset_compaction;
    auto old_min_segments = config::cloud_single_rowset_compaction_min_segments;
    auto old_group_size = config::cloud_single_rowset_compaction_segment_group_size;
    Defer restore_config {[&] {
        config::enable_cloud_single_rowset_compaction = old_enable;
        config::cloud_single_rowset_compaction_min_segments = old_min_segments;
        config::cloud_single_rowset_compaction_segment_group_size = old_group_size;
    }};
    config::enable_cloud_single_rowset_compaction = true;
    config::cloud_single_rowset_compaction_min_segments = 4;
    config::cloud_single_rowset_compaction_segment_group_size = 2;

    std::vector<RowsetSharedPtr> rowsets;
    auto grouped_rowset = create_rowset(Version(2, 2), 4, true, 1024);
    ASSERT_TRUE(grouped_rowset != nullptr);
    rowsets.push_back(grouped_rowset);
    for (int64_t version = 3; version <= 13; ++version) {
        auto rowset = create_rowset(Version(version, version), 1, false, 1024);
        ASSERT_TRUE(rowset != nullptr);
        rowsets.push_back(std::move(rowset));
    }

    TabletSchemaPB tablet_schema_pb;
    grouped_rowset->tablet_schema()->to_schema_pb(&tablet_schema_pb);
    _tablet_meta->mutable_tablet_schema()->init_from_pb(tablet_schema_pb);
    _tablet_meta->set_compaction_policy(std::string(CUMULATIVE_SIZE_BASED_POLICY));
    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    {
        std::unique_lock wlock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, wlock, false);
    }

    CloudCumulativeCompaction compaction(_engine, tablet);
    ASSERT_TRUE(compaction.pick_rowsets_to_compact().ok());
    ASSERT_EQ(compaction._input_rowsets.size(), 1);
    EXPECT_EQ(compaction._input_rowsets.front(), grouped_rowset);
    ASSERT_TRUE(compaction._single_rowset_compaction_segment_group_size.has_value());
    EXPECT_EQ(*compaction._single_rowset_compaction_segment_group_size, 2);

    config::enable_cloud_single_rowset_compaction = false;
    config::cloud_single_rowset_compaction_min_segments = 5;
    config::cloud_single_rowset_compaction_segment_group_size = 3;

    Compaction::MergeInputRowsetsResult result;
    ASSERT_TRUE(compaction.prepare_merge_input_rowsets(&result).ok());
    EXPECT_TRUE(compaction._single_rowset_compaction_segment_group_size.has_value());
    EXPECT_TRUE(result.is_segment_grouped);
    EXPECT_EQ(result.segment_group_size, 2);
}

TEST_F(CloudCompactionTest, single_rowset_grouped_compaction_honors_notready_policy_filter) {
    auto old_enable = config::enable_cloud_single_rowset_compaction;
    auto old_min_segments = config::cloud_single_rowset_compaction_min_segments;
    auto old_enable_empty_rowset_compaction = config::enable_empty_rowset_compaction;
    Defer restore_config {[&] {
        config::enable_cloud_single_rowset_compaction = old_enable;
        config::cloud_single_rowset_compaction_min_segments = old_min_segments;
        config::enable_empty_rowset_compaction = old_enable_empty_rowset_compaction;
    }};
    config::enable_cloud_single_rowset_compaction = true;
    config::cloud_single_rowset_compaction_min_segments = 4;
    config::enable_empty_rowset_compaction = false;

    std::vector<RowsetSharedPtr> rowsets;
    auto filtered_grouped_rowset = create_rowset(Version(2, 2), 4, true, 1024);
    ASSERT_TRUE(filtered_grouped_rowset != nullptr);
    rowsets.push_back(filtered_grouped_rowset);
    for (int64_t version = 3; version <= 13; ++version) {
        auto rowset = create_rowset(Version(version, version), 1, false, 1024);
        ASSERT_TRUE(rowset != nullptr);
        rowsets.push_back(std::move(rowset));
    }

    TabletSchemaPB tablet_schema_pb;
    filtered_grouped_rowset->tablet_schema()->to_schema_pb(&tablet_schema_pb);
    _tablet_meta->mutable_tablet_schema()->init_from_pb(tablet_schema_pb);
    _tablet_meta->set_compaction_policy(std::string(CUMULATIVE_SIZE_BASED_POLICY));
    _tablet_meta->set_tablet_state(TABLET_NOTREADY);
    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    tablet->set_alter_version(1);
    {
        std::unique_lock wlock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, wlock, false);
    }

    CloudCumulativeCompaction compaction(_engine, tablet);
    ASSERT_TRUE(compaction.pick_rowsets_to_compact().ok());
    ASSERT_EQ(compaction._input_rowsets.size(), 11);
    EXPECT_EQ(compaction._input_rowsets.front()->version(), Version(3, 3));
    EXPECT_EQ(compaction._input_rowsets.back()->version(), Version(13, 13));
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
