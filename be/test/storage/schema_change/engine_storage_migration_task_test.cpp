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

#include "storage/task/engine_storage_migration_task.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "core/data_type/define_primitive_type.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "load/delta_writer/delta_writer.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset_builder.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet_info.h"
#include "storage/task/engine_publish_version_task.h"
#include "storage/txn/txn_manager.h"
#include "testutil/creators.h"

namespace doris {
class OlapMeta;

static const uint32_t MAX_PATH_LEN = 1024;

static StorageEngine* engine_ref = nullptr;
static std::string path1;
static std::string path2;

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    path1 = std::string(buffer) + "/data_test_1";
    path2 = std::string(buffer) + "/data_test_2";
    config::storage_root_path = path1 + ";" + path2;
    config::min_file_descriptor_number = 1000;
    auto st = io::global_local_filesystem()->delete_directory(path1);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(path1);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->delete_directory(path2);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(path2);
    ASSERT_TRUE(st.ok()) << st;
    std::vector<StorePath> paths;
    paths.emplace_back(path1, -1);
    paths.emplace_back(path2, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    auto engine = std::make_unique<StorageEngine>(options);
    engine_ref = engine.get();
    Status s = engine->open();
    EXPECT_TRUE(s.ok()) << s;
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    exec_env->set_storage_engine(std::move(engine));
}

static void tear_down() {
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(nullptr);
    engine_ref = nullptr;
    exec_env->set_storage_engine(nullptr);
    EXPECT_EQ(system("rm -rf ./data_test_1"), 0);
    EXPECT_EQ(system("rm -rf ./data_test_2"), 0);
    EXPECT_TRUE(io::global_local_filesystem()
                        ->delete_directory(std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX)
                        .ok());
}

static void create_tablet_request_with_sequence_col(int64_t tablet_id, int32_t schema_hash,
                                                    TCreateTabletReq* request) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->partition_id = 10001;
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::UNIQUE_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->tablet_schema.__set_sequence_col_idx(2);

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
    TDescriptorTableBuilder dtb;
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
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

class TestEngineStorageMigrationTask : public ::testing::Test {
public:
    TestEngineStorageMigrationTask() = default;
    ~TestEngineStorageMigrationTask() override = default;
    static void SetUpTestSuite() {
        config::min_file_descriptor_number = 100;
        set_up();
    }

    static void TearDownTestSuite() { tear_down(); }

protected:
    struct RowBinlogGroupLoadContext {
        TCreateTabletReq base_request;
        TCreateTabletReq row_binlog_request;
        TabletSharedPtr base_tablet;
        TabletSharedPtr row_binlog_tablet;
        std::shared_ptr<OlapTableSchemaParam> schema_param;
        WriteRequest data_request;
        WriteRequest row_binlog_request_for_write;
        WriteRequest group_request;
    };

    void create_row_binlog_group_load_context(int64_t base_tablet_id, int64_t row_binlog_tablet_id,
                                              int64_t transaction_id,
                                              RowBinlogGroupLoadContext* context) {
        RuntimeProfile create_tablet_profile("CreateRowBinlogTablets");
        context->base_request = testutil::create_tablet_request(
                base_tablet_id, base_tablet_id + 1000, base_tablet_id + 2000, 1,
                TKeysType::UNIQUE_KEYS,
                {{"k1", TPrimitiveType::INT, true}, {"v1", TPrimitiveType::INT, false}});
        context->base_request.__set_enable_unique_key_merge_on_write(true);
        testutil::enable_row_binlog(&context->base_request);
        Status status = engine_ref->create_tablet(context->base_request, &create_tablet_profile);
        ASSERT_TRUE(status.ok()) << status;

        context->base_tablet =
                engine_ref->tablet_manager()->get_tablet(context->base_request.tablet_id);
        ASSERT_NE(context->base_tablet, nullptr);

        context->row_binlog_request = context->base_request;
        context->row_binlog_request.tablet_id = row_binlog_tablet_id;
        context->row_binlog_request.tablet_schema = testutil::create_row_binlog_tablet_schema(
                context->base_request.tablet_schema,
                context->base_request.tablet_schema.schema_hash + 1);
        context->row_binlog_request.__set_base_tablet_id(base_tablet_id);
        context->row_binlog_request.__set_tablet_role(TTabletRole::TABLET_ROLE_ROW_BINLOG);
        status = engine_ref->create_tablet(context->row_binlog_request, &create_tablet_profile);
        ASSERT_TRUE(status.ok()) << status;

        context->row_binlog_tablet = engine_ref->tablet_manager()->get_tablet(row_binlog_tablet_id);
        ASSERT_NE(context->row_binlog_tablet, nullptr);
        ASSERT_TRUE(context->row_binlog_tablet->is_row_binlog_tablet());
        ASSERT_EQ(context->base_tablet->data_dir(), context->row_binlog_tablet->data_dir());

        const int64_t index_id = base_tablet_id + 3000;
        const int64_t row_binlog_index_id = base_tablet_id + 4000;
        TDescriptorTable descriptor_table = testutil::create_descriptor_table(
                {{TYPE_INT, "k1", false}, {TYPE_INT, "v1", false}});
        context->schema_param = testutil::create_table_schema_param(
                descriptor_table, index_id, context->base_request.tablet_schema.schema_hash,
                context->base_request.tablet_schema.columns, row_binlog_index_id,
                context->row_binlog_request.tablet_schema.schema_hash,
                &context->row_binlog_request.tablet_schema.columns);
        ASSERT_NE(context->schema_param, nullptr);

        context->data_request.tablet_id = base_tablet_id;
        context->data_request.schema_hash = context->base_request.tablet_schema.schema_hash;
        context->data_request.txn_id = transaction_id;
        context->data_request.partition_id = context->base_request.partition_id;
        context->data_request.index_id = index_id;
        context->data_request.binlog_tablet_id = row_binlog_tablet_id;
        context->data_request.load_id.set_hi(transaction_id);
        context->data_request.load_id.set_lo(transaction_id);
        context->data_request.table_schema_param = context->schema_param;
        context->data_request.write_req_type = WriteRequestType::DATA;

        context->row_binlog_request_for_write = context->data_request;
        context->row_binlog_request_for_write.tablet_id = row_binlog_tablet_id;
        context->row_binlog_request_for_write.schema_hash =
                context->row_binlog_request.tablet_schema.schema_hash;
        context->row_binlog_request_for_write.index_id = row_binlog_index_id;
        context->row_binlog_request_for_write.write_req_type = WriteRequestType::ROW_BINLOG;

        context->group_request = context->data_request;
        context->group_request.write_req_type = WriteRequestType::GROUP;
    }

    static DataDir* other_store(const TabletSharedPtr& tablet) {
        if (tablet->data_dir()->path() == path1) {
            return engine_ref->get_store(path2);
        }
        return engine_ref->get_store(path1);
    }

    static Status migrate(const TabletSharedPtr& tablet, DataDir* dest_store) {
        EngineStorageMigrationTask migration_task(*engine_ref, tablet, dest_store);
        return migration_task.execute();
    }

    static void assert_related_transaction(const RowBinlogGroupLoadContext& context,
                                           bool expected) {
        int64_t found_partition_id = -1;
        std::set<int64_t> transaction_ids;
        engine_ref->txn_manager()->get_tablet_related_txns(context.row_binlog_tablet->tablet_id(),
                                                           context.row_binlog_tablet->tablet_uid(),
                                                           &found_partition_id, &transaction_ids);
        if (expected) {
            ASSERT_EQ(found_partition_id, context.data_request.partition_id);
            ASSERT_EQ(transaction_ids, std::set<int64_t> {context.data_request.txn_id});
        } else {
            ASSERT_TRUE(transaction_ids.empty());
        }
    }

    static void publish_group_transaction(const RowBinlogGroupLoadContext& context,
                                          int64_t version) {
        std::map<TabletInfo, RowsetSharedPtr> tablet_related_rowsets;
        std::map<TabletInfo, std::shared_ptr<TabletTxnInfo>> tablet_related_txn_infos;
        engine_ref->txn_manager()->get_txn_related_tablets(
                context.data_request.txn_id, context.data_request.partition_id,
                &tablet_related_rowsets, &tablet_related_txn_infos);

        const TabletInfo base_tablet_info = context.base_tablet->get_tablet_info();
        auto rowset_it = tablet_related_rowsets.find(base_tablet_info);
        auto txn_info_it = tablet_related_txn_infos.find(base_tablet_info);
        ASSERT_NE(rowset_it, tablet_related_rowsets.end());
        ASSERT_NE(txn_info_it, tablet_related_txn_infos.end());
        ASSERT_EQ(txn_info_it->second->attach_row_binlog.tablet.get(),
                  context.row_binlog_tablet.get());
        ASSERT_NE(txn_info_it->second->attach_row_binlog.rowset, nullptr);

        TabletPublishTxnTask publish_task(
                *engine_ref, nullptr, context.base_tablet, rowset_it->second,
                txn_info_it->second->attach_row_binlog, context.data_request.partition_id,
                context.data_request.txn_id, Version(version, version), base_tablet_info, -1);
        publish_task.handle();
        ASSERT_TRUE(publish_task.result().ok()) << publish_task.result();
    }

    static void drop_row_binlog_group(const RowBinlogGroupLoadContext& context) {
        Status status = engine_ref->tablet_manager()->drop_tablet(
                context.row_binlog_request.tablet_id, context.row_binlog_request.replica_id, false);
        ASSERT_TRUE(status.ok()) << status;
        status = engine_ref->tablet_manager()->drop_tablet(context.base_request.tablet_id,
                                                           context.base_request.replica_id, false);
        ASSERT_TRUE(status.ok()) << status;
    }
};

// The two-way migration scenario intentionally keeps setup, publish, and both migrations together.
// NOLINTNEXTLINE(readability-function-size, readability-function-cognitive-complexity)
TEST_F(TestEngineStorageMigrationTask, write_and_migration) {
    std::unique_ptr<RuntimeProfile> profile;
    profile = std::make_unique<RuntimeProfile>("CreateTablet");
    TCreateTabletReq request;
    create_tablet_request_with_sequence_col(10005, 270068377, &request);
    Status res = engine_ref->create_tablet(request, profile.get());
    EXPECT_EQ(Status::OK(), res);

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    static_cast<void>(DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl));
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

    profile = std::make_unique<RuntimeProfile>("LoadChannels");
    auto delta_writer =
            std::make_unique<DeltaWriter>(*engine_ref, write_req, profile.get(), TUniqueId {});

    res = delta_writer->close();
    EXPECT_EQ(Status::OK(), res);
    res = delta_writer->build_rowset();
    EXPECT_EQ(Status::OK(), res);
    res = delta_writer->commit_txn();
    EXPECT_EQ(Status::OK(), res);

    // publish version success
    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(write_req.tablet_id);
    OlapMeta* meta = tablet->data_dir()->get_meta();
    Version version;
    version.first = tablet->get_rowset_with_max_version()->end_version() + 1;
    version.second = tablet->get_rowset_with_max_version()->end_version() + 1;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    engine_ref->txn_manager()->get_txn_related_tablets(write_req.txn_id, write_req.partition_id,
                                                       &tablet_related_rs);
    for (auto& tablet_rs : tablet_related_rs) {
        RowsetSharedPtr rowset = tablet_rs.second;
        TabletPublishStatistics stats;
        std::shared_ptr<TabletTxnInfo> extend_tablet_txn_info_lifetime = nullptr;
        res = engine_ref->txn_manager()->publish_txn(
                meta, write_req.partition_id, write_req.txn_id, tablet->tablet_id(),
                tablet->tablet_uid(), version, &stats, extend_tablet_txn_info_lifetime);
        EXPECT_EQ(Status::OK(), res);
        res = tablet->add_inc_rowset(rowset);
        EXPECT_EQ(Status::OK(), res);
    }
    EXPECT_EQ(0, tablet->num_rows());
    // we should sleep 1 second for the migrated tablet has different time with the current tablet
    sleep(1);

    // test case 1
    // prepare
    DataDir* dest_store = nullptr;
    if (tablet->data_dir()->path() == path1) {
        dest_store = engine_ref->get_store(path2);
    } else if (tablet->data_dir()->path() == path2) {
        dest_store = engine_ref->get_store(path1);
    }
    EXPECT_NE(dest_store, nullptr);
    // migrating
    EngineStorageMigrationTask engine_task(*engine_ref, tablet, dest_store);
    res = engine_task.execute();
    EXPECT_EQ(Status::OK(), res);
    // reget the tablet from manager after migration
    TabletSharedPtr tablet2 = engine_ref->tablet_manager()->get_tablet(request.tablet_id);
    // check path
    EXPECT_EQ(tablet2->data_dir()->path(), dest_store->path());
    // check rows
    EXPECT_EQ(0, tablet2->num_rows());
    // tablet2 should not equal to tablet
    EXPECT_NE(tablet2, tablet);

    // test case 2
    // migrate tablet2 back to the tablet's path
    // sleep 1 second for update time
    sleep(1);
    dest_store = engine_ref->get_store(tablet->data_dir()->path());
    EXPECT_NE(dest_store, nullptr);
    EXPECT_NE(dest_store->path(), tablet2->data_dir()->path());
    EngineStorageMigrationTask engine_task2(*engine_ref, tablet2, dest_store);
    res = engine_task2.execute();
    EXPECT_EQ(Status::OK(), res);
    TabletSharedPtr tablet3 = engine_ref->tablet_manager()->get_tablet(request.tablet_id);
    // check path
    EXPECT_EQ(tablet3->data_dir()->path(), tablet->data_dir()->path());
    // check rows
    EXPECT_EQ(0, tablet3->num_rows());
    // orgi_tablet should not equal to new_tablet and tablet
    EXPECT_NE(tablet3, tablet2);
    EXPECT_NE(tablet3, tablet);
    // test case 2 end

    res = engine_ref->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    EXPECT_EQ(Status::OK(), res);
}

TEST_F(TestEngineStorageMigrationTask, row_binlog_prepared_txn_blocks_migration) {
    RowBinlogGroupLoadContext context;
    create_row_binlog_group_load_context(11005, 11006, 21005, &context);
    DataDir* dest_store = other_store(context.row_binlog_tablet);
    ASSERT_NE(dest_store, nullptr);
    ASSERT_NE(dest_store, context.row_binlog_tablet->data_dir());

    {
        RuntimeProfile load_profile("PreparedRowBinlogGroupLoad");
        GroupRowsetBuilder builder(*engine_ref, context.group_request, context.data_request,
                                   context.row_binlog_request_for_write, &load_profile);
        Status status = builder.init();
        ASSERT_TRUE(status.ok()) << status;
        assert_related_transaction(context, true);

        status = engine_ref->txn_manager()->attach_row_binlog_tablet_to_txn(
                context.data_request.partition_id, context.data_request.txn_id,
                context.base_tablet->get_tablet_info(), context.row_binlog_tablet);
        ASSERT_TRUE(status.ok()) << status;

        status = engine_ref->txn_manager()->attach_row_binlog_tablet_to_txn(
                context.data_request.partition_id, context.data_request.txn_id,
                context.base_tablet->get_tablet_info(), context.base_tablet);
        ASSERT_TRUE(status.is<ErrorCode::PUSH_TRANSACTION_ALREADY_EXIST>()) << status;

        status = engine_ref->txn_manager()->attach_row_binlog_tablet_to_txn(
                context.data_request.partition_id, context.data_request.txn_id + 1,
                context.base_tablet->get_tablet_info(), context.row_binlog_tablet);
        ASSERT_TRUE(status.is<ErrorCode::TRANSACTION_NOT_EXIST>()) << status;

        const TabletUid old_uid = context.row_binlog_tablet->tablet_uid();
        const std::string old_path = context.row_binlog_tablet->tablet_path();
        status = migrate(context.row_binlog_tablet, dest_store);
        ASSERT_FALSE(status.ok());
        ASSERT_NE(status.to_string().find("unfinished txns"), std::string::npos) << status;

        TabletSharedPtr current_tablet = engine_ref->tablet_manager()->get_tablet(
                context.row_binlog_tablet->tablet_id(), old_uid);
        ASSERT_EQ(current_tablet.get(), context.row_binlog_tablet.get());
        ASSERT_EQ(current_tablet->tablet_uid(), old_uid);
        ASSERT_EQ(current_tablet->tablet_path(), old_path);
    }

    // Destroying an uncommitted group builder rolls back the base transaction and removes the
    // attached row-binlog relation.
    assert_related_transaction(context, false);
    drop_row_binlog_group(context);
}

TEST_F(TestEngineStorageMigrationTask, row_binlog_committed_txn_blocks_migration_until_publish) {
    RowBinlogGroupLoadContext context;
    create_row_binlog_group_load_context(12005, 12006, 22005, &context);
    DataDir* dest_store = other_store(context.row_binlog_tablet);
    ASSERT_NE(dest_store, nullptr);
    ASSERT_NE(dest_store, context.row_binlog_tablet->data_dir());

    RuntimeProfile load_profile("CommittedRowBinlogGroupLoad");
    GroupRowsetBuilder builder(*engine_ref, context.group_request, context.data_request,
                               context.row_binlog_request_for_write, &load_profile);
    Status status = builder.init();
    ASSERT_TRUE(status.ok()) << status;
    status = builder.rowset_writer()->flush();
    ASSERT_TRUE(status.ok()) << status;
    status = builder.build_rowset();
    ASSERT_TRUE(status.ok()) << status;
    status = builder.commit_txn();
    ASSERT_TRUE(status.ok()) << status;
    assert_related_transaction(context, true);

    const TabletUid old_uid = context.row_binlog_tablet->tablet_uid();
    const std::string old_path = context.row_binlog_tablet->tablet_path();
    status = migrate(context.row_binlog_tablet, dest_store);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(status.to_string().find("unfinished txns"), std::string::npos) << status;

    TabletSharedPtr current_tablet = engine_ref->tablet_manager()->get_tablet(
            context.row_binlog_tablet->tablet_id(), old_uid);
    ASSERT_EQ(current_tablet.get(), context.row_binlog_tablet.get());
    ASSERT_EQ(current_tablet->tablet_uid(), old_uid);
    ASSERT_EQ(current_tablet->tablet_path(), old_path);

    constexpr int64_t publish_version = 2;
    publish_group_transaction(context, publish_version);
    assert_related_transaction(context, false);
    ASSERT_NE(context.base_tablet->get_rowset_by_version(Version(publish_version, publish_version)),
              nullptr);
    ASSERT_NE(context.row_binlog_tablet->get_rowset_by_version(
                      Version(publish_version, publish_version)),
              nullptr);

    // Give the reloaded tablet a newer creation time than the source tablet.
    sleep(1);
    EngineStorageMigrationTask migration_task(*engine_ref, context.row_binlog_tablet, dest_store);
    status = migration_task.execute();
    ASSERT_TRUE(status.ok()) << status;

    TabletSharedPtr migrated_tablet =
            engine_ref->tablet_manager()->get_tablet(context.row_binlog_tablet->tablet_id());
    ASSERT_NE(migrated_tablet, nullptr);
    ASSERT_NE(migrated_tablet.get(), context.row_binlog_tablet.get());
    ASSERT_NE(migrated_tablet->tablet_uid(), old_uid);
    ASSERT_EQ(migrated_tablet->data_dir(), dest_store);
    ASSERT_NE(migrated_tablet->get_rowset_by_version(Version(publish_version, publish_version)),
              nullptr);

    drop_row_binlog_group(context);
}

} // namespace doris
