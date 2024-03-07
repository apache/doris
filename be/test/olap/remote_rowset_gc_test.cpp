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
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <unistd.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gen_cpp/internal_service.pb.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "io/fs/remote_file_system.h"
#include "io/fs/s3_file_system.h"
#include "olap/data_dir.h"
#include "olap/delta_writer.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/txn_manager.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "util/doris_metrics.h"
#include "util/s3_util.h"

namespace doris {
class OlapMeta;

static std::unique_ptr<StorageEngine> k_engine;

static const std::string kTestDir = "./ut_dir/remote_rowset_gc_test";
static constexpr int64_t kResourceId = 10000;
static constexpr int64_t kStoragePolicyId = 10002;

class RemoteRowsetGcTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        S3Conf s3_conf;
        s3_conf.ak = config::test_s3_ak;
        s3_conf.sk = config::test_s3_sk;
        s3_conf.endpoint = config::test_s3_endpoint;
        s3_conf.region = config::test_s3_region;
        s3_conf.bucket = config::test_s3_bucket;
        s3_conf.prefix = "remote_rowset_gc_test";
        std::shared_ptr<io::S3FileSystem> s3_fs;
        ASSERT_TRUE(
                io::S3FileSystem::create(std::move(s3_conf), std::to_string(kResourceId), &s3_fs)
                        .ok());
        put_storage_resource(kResourceId, {s3_fs, 1});
        auto storage_policy = std::make_shared<StoragePolicy>();
        storage_policy->name = "TabletCooldownTest";
        storage_policy->version = 1;
        storage_policy->resource_id = kResourceId;
        put_storage_policy(kStoragePolicyId, storage_policy);

        constexpr uint32_t MAX_PATH_LEN = 1024;
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = std::string(buffer) + "/" + kTestDir;
        config::min_file_descriptor_number = 1000;

        auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(config::storage_root_path);
        ASSERT_TRUE(st.ok()) << st;

        std::vector<StorePath> paths {{config::storage_root_path, -1}};

        EngineOptions options;
        options.store_paths = paths;
        doris::StorageEngine::open(options, &k_engine);
    }

    static void TearDownTestSuite() { k_engine.reset(); }
};

static void create_tablet_request_with_sequence_col(int64_t tablet_id, int32_t schema_hash,
                                                    TCreateTabletReq* request) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->partition_id = 30003;
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::UNIQUE_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->tablet_schema.__set_sequence_col_idx(2);
    request->__set_storage_format(TStorageFormat::V2);

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn sequence_col;
    sequence_col.column_name = SEQUENCE_COL;
    sequence_col.__set_is_key(false);
    sequence_col.column_type.type = TPrimitiveType::INT;
    sequence_col.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(sequence_col);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    v1.column_type.type = TPrimitiveType::DATETIME;
    v1.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v1);
}

static TDescriptorTable create_descriptor_tablet_with_sequence_col() {
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("k1").column_pos(0).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("k2").column_pos(1).build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .column_name(SEQUENCE_COL)
                                   .column_pos(2)
                                   .build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATETIME).column_name("v1").column_pos(3).build());
    tuple_builder.build(&desc_tbl_builder);

    return desc_tbl_builder.desc_tbl();
}

TEST_F(RemoteRowsetGcTest, normal) {
    RuntimeProfile profile("CreateTablet");
    TCreateTabletReq request;
    create_tablet_request_with_sequence_col(10005, 270068377, &request);
    Status st = k_engine->create_tablet(request, &profile);
    ASSERT_EQ(Status::OK(), st);

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    auto param = std::make_shared<OlapTableSchemaParam>();

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req;
    write_req.tablet_id = 10005;
    write_req.schema_hash = 270068377;
    write_req.txn_id = 20003;
    write_req.partition_id = 30003;
    write_req.load_id = load_id;
    write_req.tuple_desc = tuple_desc;
    write_req.slots = &(tuple_desc->slots());
    write_req.is_high_priority = false;
    write_req.table_schema_param = param;
    std::unique_ptr<RuntimeProfile> profile;
    profile = std::make_unique<RuntimeProfile>("LoadChannels");
    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer, profile.get());
    ASSERT_NE(delta_writer, nullptr);

    st = delta_writer->close();
    ASSERT_EQ(Status::OK(), st);
    st = delta_writer->close_wait(PSlaveTabletNodes(), false);
    ASSERT_EQ(Status::OK(), st);

    // publish version success
    TabletSharedPtr tablet =
            k_engine->tablet_manager()->get_tablet(write_req.tablet_id, write_req.schema_hash);
    OlapMeta* meta = tablet->data_dir()->get_meta();
    Version version;
    version.first = tablet->get_rowset_with_max_version()->end_version() + 1;
    version.second = tablet->get_rowset_with_max_version()->end_version() + 1;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
            write_req.txn_id, write_req.partition_id, &tablet_related_rs);
    for (auto& tablet_rs : tablet_related_rs) {
        RowsetSharedPtr rowset = tablet_rs.second;
        TabletPublishStatistics stats;
        st = k_engine->txn_manager()->publish_txn(meta, write_req.partition_id, write_req.txn_id,
                                                  write_req.tablet_id, write_req.schema_hash,
                                                  tablet_rs.first.tablet_uid, version, &stats);
        ASSERT_EQ(Status::OK(), st);
        st = tablet->add_inc_rowset(rowset);
        ASSERT_EQ(Status::OK(), st);
    }
    EXPECT_EQ(0, tablet->num_rows());

    tablet->set_storage_policy_id(kStoragePolicyId);
    st = tablet->cooldown(); // rowset [0-1]
    ASSERT_EQ(Status::OK(), st);
    st = tablet->cooldown(); // rowset [2-2]
    ASSERT_EQ(Status::OK(), st);
    ASSERT_EQ(DorisMetrics::instance()->upload_rowset_count->value(), 1);

    delete delta_writer;

    auto fs = get_storage_resource(kResourceId).fs;
    auto rowset = tablet->get_rowset_by_version({2, 2});
    ASSERT_TRUE(rowset);
    auto seg_path = BetaRowset::remote_segment_path(10005, rowset->rowset_id(), 0);
    bool exists = false;
    st = fs->exists(seg_path, &exists);
    ASSERT_EQ(Status::OK(), st);
    ASSERT_TRUE(exists);

    st = k_engine->tablet_manager()->drop_tablet(10005, 0, true);
    ASSERT_EQ(Status::OK(), st);
    tablet->data_dir()->perform_remote_tablet_gc();
    st = fs->exists(seg_path, &exists);
    ASSERT_EQ(Status::OK(), st);
    ASSERT_FALSE(exists);
}

} // namespace doris
