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
#include <gtest/gtest.h>
#include <stdlib.h>
#include <unistd.h>

#include <array>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/config.h"
#include "io/fs/local_file_system.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "storage/binlog.h"
#include "storage/data_dir.h"
#include "storage/rowset_builder.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "storage/tablet_info.h"
#include "storage/task/engine_publish_version_task.h"
#include "testutil/creators.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris {

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;

static void open_engine() {
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    EngineOptions options;
    options.store_paths = paths;
    auto engine = std::make_unique<StorageEngine>(options);
    engine_ref = engine.get();
    Status s = engine->open();
    ASSERT_TRUE(s.ok()) << s;
    s = ThreadPoolBuilder("TabletPublishTxnThreadPool")
                .set_min_threads(1)
                .set_max_threads(2)
                .build(&engine->_tablet_publish_txn_thread_pool);
    ASSERT_TRUE(s.ok()) << s;
    ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
}

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    config::storage_root_path = std::string(buffer) + "/data_test";
    auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    open_engine();
}

static void restart_engine() {
    engine_ref = nullptr;
    ExecEnv::GetInstance()->set_storage_engine(nullptr);
    open_engine();
}

static void tear_down() {
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(nullptr);
    engine_ref = nullptr;
    exec_env->set_storage_engine(nullptr);
    EXPECT_EQ(system("rm -rf ./data_test"), 0);
    static_cast<void>(io::global_local_filesystem()->delete_directory(
            std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX));
}

class GroupRowsetBuilderTest : public ::testing::Test {
public:
    void SetUp() override { set_up(); }
    void TearDown() override { tear_down(); }
};

TEST_F(GroupRowsetBuilderTest, buildWithRowBinlogMeta) {
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("CreateTablet");
    auto request = testutil::create_tablet_request(
            10010, 270068390, 10001, 1, TKeysType::UNIQUE_KEYS,
            {{"k1", TPrimitiveType::INT, true}, {"v1", TPrimitiveType::INT, false}});
    request.__set_enable_unique_key_merge_on_write(true);
    testutil::enable_row_binlog(&request);
    Status res = engine_ref->create_tablet(request, profile.get());
    ASSERT_TRUE(res.ok());

    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(request.tablet_id);
    ASSERT_TRUE(tablet != nullptr);

    auto row_binlog_request = request;
    row_binlog_request.tablet_id = 10011;
    auto row_binlog_tablet_schema = testutil::create_row_binlog_tablet_schema(
            request.tablet_schema, request.tablet_schema.schema_hash + 1);
    row_binlog_request.tablet_schema = row_binlog_tablet_schema;
    res = engine_ref->create_tablet(row_binlog_request, profile.get());
    ASSERT_TRUE(res.ok());
    TabletSharedPtr row_binlog_tablet =
            engine_ref->tablet_manager()->get_tablet(row_binlog_request.tablet_id);
    ASSERT_TRUE(row_binlog_tablet != nullptr);

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    const int64_t index_id = 10001;
    const int64_t row_binlog_index_id = 10002;

    TDescriptorTable tdesc_tbl =
            testutil::create_descriptor_table({{TYPE_INT, "k1", false}, {TYPE_INT, "v1", false}});
    auto param = testutil::create_table_schema_param(
            tdesc_tbl, index_id, request.tablet_schema.schema_hash, request.tablet_schema.columns,
            row_binlog_index_id, row_binlog_tablet_schema.schema_hash,
            &row_binlog_tablet_schema.columns);
    ASSERT_NE(param, nullptr);

    WriteRequest data_req;
    data_req.tablet_id = request.tablet_id;
    data_req.schema_hash = request.tablet_schema.schema_hash;
    data_req.txn_id = 20010;
    data_req.partition_id = request.partition_id;
    data_req.index_id = index_id;
    data_req.load_id = load_id;
    data_req.table_schema_param = param;
    data_req.write_req_type = WriteRequestType::DATA;

    WriteRequest row_binlog_req = data_req;
    row_binlog_req.tablet_id = row_binlog_request.tablet_id;
    row_binlog_req.index_id = row_binlog_index_id;
    row_binlog_req.schema_hash = row_binlog_tablet_schema.schema_hash;
    row_binlog_req.write_req_type = WriteRequestType::ROW_BINLOG;

    WriteRequest group_req = data_req;
    group_req.write_req_type = WriteRequestType::GROUP;

    GroupRowsetBuilder builder(*engine_ref, group_req, data_req, row_binlog_req, profile.get());
    ASSERT_TRUE(builder.init().ok());
    const auto& mappings = builder.row_binlog_builder()
                                   ->rowset_writer()
                                   ->context()
                                   .write_binlog_opt()
                                   .write_binlog_config()
                                   .column_mappings;
    ASSERT_EQ(mappings.size(), 2U);
    EXPECT_EQ(mappings[0], (segment_v2::RowBinlogColumnCidMapping {0, 0, std::nullopt}));
    EXPECT_EQ(mappings[1], (segment_v2::RowBinlogColumnCidMapping {1, 1, std::nullopt}));
    ASSERT_TRUE(builder.rowset_writer()->flush().ok());
    ASSERT_TRUE(builder.build_rowset().ok());

    auto row_binlog_meta = builder.row_binlog_builder()->rowset()->rowset_meta();
    auto data_meta = builder.txn_rowset_builder()->rowset()->rowset_meta();
    ASSERT_TRUE(row_binlog_meta->is_row_binlog());
    ASSERT_FALSE(data_meta->is_row_binlog());
    ASSERT_EQ(row_binlog_tablet_schema.schema_hash, row_binlog_meta->tablet_schema_hash());
    ASSERT_EQ(request.tablet_schema.schema_hash, data_meta->tablet_schema_hash());
    ASSERT_EQ(row_binlog_index_id, row_binlog_meta->index_id());
    ASSERT_EQ(index_id, data_meta->index_id());

    // Row-binlog schema must contain LSN column so that the derive stage can locate it.
    ASSERT_GE(row_binlog_meta->tablet_schema()->binlog_lsn_col_idx(), 0);

    res = engine_ref->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    ASSERT_TRUE(res.ok());
    res = engine_ref->tablet_manager()->drop_tablet(row_binlog_request.tablet_id,
                                                    row_binlog_request.replica_id, false);
    ASSERT_TRUE(res.ok());
}

// Keep the real-engine commit/restart/publish sequence together across the mapping variants.
// NOLINTNEXTLINE(readability-function-cognitive-complexity, readability-function-size)
static void recover_multiple_row_binlog_pairs(bool historical, bool key_only,
                                              std::optional<int32_t> max_gap = std::nullopt) {
    constexpr int64_t partition_id = 10100;
    constexpr int64_t txn_id = 20100;
    constexpr int64_t index_id = 30100;
    constexpr int64_t row_binlog_index_id = 30101;
    constexpr int32_t schema_hash = 40100;
    constexpr int32_t row_binlog_schema_hash = 40101;
    constexpr std::array<std::pair<int64_t, int64_t>, 2> tablet_pairs = {std::pair {10100, 10101},
                                                                         std::pair {10200, 10201}};

    auto base_request = testutil::create_tablet_request(
            0, schema_hash, partition_id, 1, TKeysType::UNIQUE_KEYS,
            {{"k1", TPrimitiveType::INT, true}, {"v1", TPrimitiveType::INT, false}});
    if (key_only) {
        base_request.tablet_schema.columns.resize(1);
    }
    base_request.__set_enable_unique_key_merge_on_write(true);
    testutil::enable_row_binlog(&base_request);
    auto row_binlog_schema = testutil::create_row_binlog_tablet_schema(base_request.tablet_schema,
                                                                       row_binlog_schema_hash);
    if (historical && !key_only) {
        auto before = row_binlog_schema.columns[1];
        before.column_name = binlog::build_before_column_name("v1");
        row_binlog_schema.columns.push_back(before);
    }

    RuntimeProfile profile("CreateTablet");
    for (const auto& [base_tablet_id, row_binlog_tablet_id] : tablet_pairs) {
        base_request.tablet_id = base_tablet_id;
        ASSERT_TRUE(engine_ref->create_tablet(base_request, &profile).ok());

        auto row_binlog_request = base_request;
        row_binlog_request.tablet_id = row_binlog_tablet_id;
        row_binlog_request.tablet_schema = row_binlog_schema;
        row_binlog_request.__set_base_tablet_id(base_tablet_id);
        row_binlog_request.__set_tablet_role(TTabletRole::TABLET_ROLE_ROW_BINLOG);
        ASSERT_TRUE(engine_ref->create_tablet(row_binlog_request, &profile).ok());

        auto base_tablet = engine_ref->tablet_manager()->get_tablet(base_tablet_id);
        ASSERT_NE(base_tablet, nullptr);
        TabletMetaPB in_memory_meta_pb;
        base_tablet->tablet_meta()->to_meta_pb(&in_memory_meta_pb, false);
        EXPECT_EQ(in_memory_meta_pb.binlog_tablet_id(), row_binlog_tablet_id);

        TabletMetaSharedPtr persisted_meta = std::make_shared<TabletMeta>();
        ASSERT_TRUE(TabletMetaManager::get_meta(base_tablet->data_dir(), base_tablet_id,
                                                schema_hash, persisted_meta)
                            .ok());
        TabletMetaPB persisted_meta_pb;
        persisted_meta->to_meta_pb(&persisted_meta_pb, false);
        EXPECT_EQ(persisted_meta_pb.binlog_tablet_id(), row_binlog_tablet_id);
    }

    TDescriptorTable tdesc_tbl =
            key_only ? testutil::create_descriptor_table({{TYPE_INT, "k1", false}})
                     : testutil::create_descriptor_table(
                               {{TYPE_INT, "k1", false}, {TYPE_INT, "v1", false}});
    auto schema_param = testutil::create_table_schema_param(
            tdesc_tbl, index_id, schema_hash, base_request.tablet_schema.columns,
            row_binlog_index_id, row_binlog_schema_hash, &row_binlog_schema.columns);
    ASSERT_NE(schema_param, nullptr);
    auto* source_index = schema_param->indexes()[0];
    source_index->row_binlog_need_historical_value = historical;
    if (historical && !key_only) {
        source_index->row_binlog_column_mappings[1].before_uid = 5;
    }

    TRowBinlogWriteColumnMappings thrift_snapshot;
    thrift_snapshot.__set_need_historical_value(historical);
    std::vector<TRowBinlogWriteColumnMapping> entries;
    for (const auto& mapping : source_index->row_binlog_column_mappings) {
        TRowBinlogWriteColumnMapping entry;
        entry.__set_source_column_unique_id(mapping.source_uid);
        entry.__set_current_column_unique_id(mapping.current_uid);
        if (mapping.before_uid.has_value()) {
            entry.__set_before_column_unique_id(*mapping.before_uid);
        }
        entries.push_back(entry);
    }
    thrift_snapshot.__set_entries(entries);

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(1);
    for (int64_t committed_txn = txn_id; committed_txn <= txn_id + max_gap.has_value();
         ++committed_txn) {
        for (const auto& [base_tablet_id, row_binlog_tablet_id] : tablet_pairs) {
            WriteRequest data_req;
            data_req.tablet_id = base_tablet_id;
            data_req.schema_hash = schema_hash;
            data_req.txn_id = committed_txn;
            data_req.partition_id = partition_id;
            data_req.index_id = index_id;
            data_req.load_id = load_id;
            data_req.table_schema_param = schema_param;
            data_req.write_req_type = WriteRequestType::DATA;

            WriteRequest row_binlog_req = data_req;
            row_binlog_req.tablet_id = row_binlog_tablet_id;
            row_binlog_req.index_id = row_binlog_index_id;
            row_binlog_req.schema_hash = row_binlog_schema_hash;
            row_binlog_req.write_req_type = WriteRequestType::ROW_BINLOG;

            WriteRequest group_req = data_req;
            group_req.write_req_type = WriteRequestType::GROUP;

            RuntimeProfile write_profile("GroupWrite");
            GroupRowsetBuilder builder(*engine_ref, group_req, data_req, row_binlog_req,
                                       &write_profile);
            ASSERT_TRUE(builder.init().ok());
            ASSERT_TRUE(builder.rowset_writer()->flush().ok());
            ASSERT_TRUE(builder.build_rowset().ok());
            ASSERT_TRUE(builder.commit_txn().ok());
        }
    }

    restart_engine();

    TPublishVersionRequest publish_request;
    publish_request.__set_transaction_id(txn_id);
    TPartitionVersionInfo version_info;
    version_info.__set_partition_id(partition_id);
    version_info.__set_version(2);
    version_info.__set_commit_tso(12345);
    publish_request.__set_partition_version_infos({version_info});
    std::set<TTabletId> errors;
    std::map<TTabletId, TVersion> successes;
    std::vector<DiscontinuousVersionTablet> discontinuous;
    std::map<TTableId, std::map<TTabletId, int64_t>> delta_rows;
    auto publish = [&] {
        errors.clear();
        discontinuous.clear();
        EnginePublishVersionTask task(*engine_ref, publish_request, &errors, &successes,
                                      &discontinuous, &delta_rows);
        return task.execute();
    };

    // Missing, wrong-index, malformed, and invalid-UID snapshots must fail before visibility.
    auto assert_committed = [&] {
        std::map<TabletInfo, RowsetSharedPtr> committed;
        std::map<TabletInfo, std::shared_ptr<TabletTxnInfo>> infos;
        engine_ref->txn_manager()->get_txn_related_tablets(txn_id, partition_id, &committed,
                                                           &infos);
        ASSERT_EQ(committed.size(), tablet_pairs.size());
        for (const auto& [tablet_info, info] : infos) {
            EXPECT_EQ(info->rowset->rowset_meta()->rowset_state(), RowsetStatePB::COMMITTED);
            EXPECT_EQ(info->attach_row_binlog.rowset->rowset_meta()->rowset_state(),
                      RowsetStatePB::COMMITTED);
        }
    };
    EXPECT_FALSE(publish().ok());
    assert_committed();
    publish_request.__set_row_binlog_column_mappings({{row_binlog_index_id, thrift_snapshot}});
    EXPECT_FALSE(publish().ok());
    assert_committed();
    auto malformed_snapshot = thrift_snapshot;
    malformed_snapshot.__isset.need_historical_value = false;
    publish_request.__set_row_binlog_column_mappings({{index_id, malformed_snapshot}});
    EXPECT_FALSE(publish().ok());
    assert_committed();
    auto invalid_snapshot = thrift_snapshot;
    invalid_snapshot.entries[0].source_column_unique_id = 9999;
    publish_request.__set_row_binlog_column_mappings({{index_id, invalid_snapshot}});
    EXPECT_FALSE(publish().ok());
    assert_committed();
    // Local tablet metadata does not carry an index ID. Select the committed writer's source
    // index, not the default tablet index or the attached binlog index.
    publish_request.__set_row_binlog_column_mappings(
            {{index_id, thrift_snapshot}, {0, invalid_snapshot}});

    if (max_gap.has_value()) {
        const auto old_gap = config::mow_publish_max_discontinuous_version_num;
        config::mow_publish_max_discontinuous_version_num = *max_gap;
        Defer restore_gap([&] { config::mow_publish_max_discontinuous_version_num = old_gap; });
        publish_request.partition_version_infos[0].version = 3;
        EXPECT_TRUE(publish().is<ErrorCode::PUBLISH_VERSION_NOT_CONTINUOUS>());
        if (*max_gap == 0) {
            EXPECT_TRUE(discontinuous.empty());
        } else {
            ASSERT_EQ(discontinuous.size(), tablet_pairs.size());
            for (const auto& item : discontinuous) {
                engine_ref->add_async_publish_task(
                        item.partition_id, item.tablet_id, item.publish_version, txn_id, false,
                        item.commit_tso, item.row_binlog_column_mappings);
            }
        }
        // Both discontinuity paths persist the snapshot; subsequent execution needs no FE map.
        restart_engine();
        for (const auto& [base_tablet_id, binlog_tablet_id] : tablet_pairs) {
            const auto& queued = engine_ref->_async_publish_tasks.at(base_tablet_id).at(3);
            ASSERT_NE(std::get<3>(queued), nullptr);
            EXPECT_EQ(std::get<3>(queued)->need_historical_value(), historical);
            EXPECT_EQ(std::get<3>(queued)->entries_size(), key_only ? 1 : 2);
            EXPECT_EQ(std::get<2>(queued), 12345);
        }
        publish_request.transaction_id = txn_id + 1;
        publish_request.partition_version_infos[0].version = 2;
        auto st = publish();
        ASSERT_TRUE(st.ok()) << st;
        publish_request.row_binlog_column_mappings.clear();
    }

    std::map<TabletInfo, RowsetSharedPtr> rowsets;
    std::map<TabletInfo, std::shared_ptr<TabletTxnInfo>> txn_infos;
    engine_ref->txn_manager()->get_txn_related_tablets(txn_id, partition_id, &rowsets, &txn_infos);
    ASSERT_EQ(txn_infos.size(), tablet_pairs.size());
    for (const auto& [base_tablet_id, row_binlog_tablet_id] : tablet_pairs) {
        auto base_tablet = engine_ref->tablet_manager()->get_tablet(base_tablet_id);
        ASSERT_NE(base_tablet, nullptr);
        auto txn_info = txn_infos.find(base_tablet->get_tablet_info());
        ASSERT_NE(txn_info, txn_infos.end());
        ASSERT_NE(txn_info->second->attach_row_binlog.tablet, nullptr);
        ASSERT_NE(txn_info->second->attach_row_binlog.rowset, nullptr);
        EXPECT_EQ(txn_info->second->attach_row_binlog.tablet->tablet_id(), row_binlog_tablet_id);
        EXPECT_EQ(txn_info->second->attach_row_binlog.rowset->rowset_meta()->tablet_id(),
                  row_binlog_tablet_id);

        // Committed-rowset recovery deliberately does not reconstruct a mapping snapshot.
        ASSERT_TRUE(txn_info->second->unique_key_merge_on_write);
        EXPECT_TRUE(txn_info->second->attach_row_binlog.column_mappings.empty());

        // Model ADD COLUMN after commit: publish must resolve the old snapshot against the
        // transaction rowsets, not this newer tablet schema requiring an additional mapping.
        TabletSchemaPB latest_schema_pb;
        base_tablet->tablet_schema()->to_schema_pb(&latest_schema_pb);
        latest_schema_pb.set_schema_version(latest_schema_pb.schema_version() + 1);
        auto* added_column = latest_schema_pb.add_column();
        added_column->CopyFrom(latest_schema_pb.column(0));
        added_column->set_name("added_after_commit");
        added_column->set_unique_id(1000);
        added_column->set_is_key(false);
        added_column->set_is_nullable(true);
        added_column->set_aggregation("REPLACE");
        auto latest_schema = std::make_shared<TabletSchema>();
        latest_schema->init_from_pb(latest_schema_pb);
        base_tablet->update_max_version_schema(latest_schema);
        ASSERT_GE(base_tablet->tablet_schema()->field_index(1000), 0);
        ASSERT_EQ(txn_info->second->rowset->tablet_schema()->field_index(1000), -1);
    }
    if (max_gap.has_value()) {
        engine_ref->_process_async_publish();
        engine_ref->_tablet_publish_txn_thread_pool->wait();
    } else {
        auto st = publish();
        ASSERT_TRUE(st.ok()) << st;
    }
    for (const auto& [base_tablet_id, row_binlog_tablet_id] : tablet_pairs) {
        auto base_tablet = engine_ref->tablet_manager()->get_tablet(base_tablet_id);
        auto binlog_tablet = engine_ref->tablet_manager()->get_tablet(row_binlog_tablet_id);
        EXPECT_EQ(base_tablet->max_version().second, max_gap.has_value() ? 3 : 2);
        EXPECT_EQ(binlog_tablet->max_version().second, max_gap.has_value() ? 3 : 2);
        const auto& info = txn_infos.at(base_tablet->get_tablet_info());
        EXPECT_EQ(info->rowset->rowset_meta()->rowset_state(), RowsetStatePB::VISIBLE);
        const auto& snapshot = info->attach_row_binlog;
        EXPECT_EQ(snapshot.need_historical_value, historical);
        ASSERT_EQ(snapshot.column_mappings.size(), key_only ? 1U : 2U);
        EXPECT_EQ(snapshot.column_mappings[0].source_uid, 0);
        EXPECT_EQ(snapshot.column_mappings[0].current_uid, 0);
        EXPECT_FALSE(snapshot.column_mappings[0].before_uid.has_value());
        if (!key_only) {
            EXPECT_EQ(snapshot.column_mappings[1].source_uid, 1);
            EXPECT_EQ(snapshot.column_mappings[1].current_uid, 1);
            EXPECT_EQ(snapshot.column_mappings[1].before_uid,
                      historical ? std::optional<int32_t>(5) : std::nullopt);
        }
    }
}

TEST_F(GroupRowsetBuilderTest, recoverMultipleRowBinlogPairsInOneTxn) {
    recover_multiple_row_binlog_pairs(false, false);
}

TEST_F(GroupRowsetBuilderTest, recoverHistoricalRowBinlogPublishSnapshot) {
    recover_multiple_row_binlog_pairs(true, false);
}

TEST_F(GroupRowsetBuilderTest, recoverKeyOnlyHistoricalRowBinlogPublishSnapshot) {
    recover_multiple_row_binlog_pairs(true, true);
}

TEST_F(GroupRowsetBuilderTest, recoverDirectPendingRowBinlogSnapshot) {
    recover_multiple_row_binlog_pairs(true, false, 0);
}

TEST_F(GroupRowsetBuilderTest, recoverRetryPendingRowBinlogSnapshot) {
    recover_multiple_row_binlog_pairs(true, true, 100);
}

} // namespace doris
