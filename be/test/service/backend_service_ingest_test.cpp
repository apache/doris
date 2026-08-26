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

#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "common/config.h"
#include "core/block/block.h"
#include "service/backend_service_ingest_helper.h"
#include "storage/data_dir.h"
#include "storage/olap_meta.h"
#include "storage/options.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/txn/txn_manager.h"
#include "util/uid_util.h"

namespace doris {

static const std::string kTestDir = "./be/test/service/backend_service_ingest_test_data";
static const int64_t kTabletId = 40001;
static const int64_t kPartitionId = 30001;
static const int64_t kTxnId = 50001;
static const int64_t kSchemaHash = 1111;

class BackendServiceIngestTest : public testing::Test {
public:
    void SetUp() override {
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;

        char buffer[1024];
        EXPECT_NE(getcwd(buffer, 1024), nullptr);
        config::storage_root_path = std::string(buffer) + "/backend_service_ingest_test_meta";

        std::filesystem::remove_all(config::storage_root_path);
        std::filesystem::remove_all(kTestDir);
        EXPECT_TRUE(std::filesystem::create_directory(config::storage_root_path));
        EXPECT_TRUE(std::filesystem::create_directory(kTestDir));

        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);
        EngineOptions options;
        options.store_paths = paths;
        options.backend_uid = UniqueId::gen_uid();

        auto engine = std::make_unique<StorageEngine>(options);
        Status st = engine->open();
        ASSERT_TRUE(st.ok()) << st.to_string();
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        _data_dir = std::make_unique<DataDir>(*_engine, kTestDir);
        st = _data_dir->init();
        ASSERT_TRUE(st.ok()) << st.to_string();
        static_cast<void>(_data_dir->update_capacity());

        _create_mow_tablet();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _engine = nullptr;
        _data_dir.reset();
        std::filesystem::remove_all(config::storage_root_path);
        std::filesystem::remove_all(kTestDir);
    }

protected:
    void _create_mow_tablet_schema(TabletSchemaSPtr tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(UNIQUE_KEYS);
        tablet_schema_pb.set_num_short_key_columns(1);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(3);
        tablet_schema_pb.set_sequence_col_idx(2);

        ColumnPB* column_1 = tablet_schema_pb.add_column();
        column_1->set_unique_id(1);
        column_1->set_name("k1");
        column_1->set_type("INT");
        column_1->set_is_key(true);
        column_1->set_length(4);
        column_1->set_index_length(4);
        column_1->set_is_nullable(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("v1");
        column_2->set_type("INT");
        column_2->set_is_key(false);
        column_2->set_length(4);
        column_2->set_is_nullable(false);
        column_2->set_aggregation("REPLACE");

        ColumnPB* column_3 = tablet_schema_pb.add_column();
        column_3->set_unique_id(3);
        column_3->set_name(SEQUENCE_COL);
        column_3->set_type("INT");
        column_3->set_is_key(false);
        column_3->set_length(4);
        column_3->set_is_nullable(false);
        column_3->set_aggregation("REPLACE");

        tablet_schema->init_from_pb(tablet_schema_pb);
    }

    void _create_mow_tablet() {
        _tablet_schema = std::make_shared<TabletSchema>();
        _create_mow_tablet_schema(_tablet_schema);

        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = kTabletId;
        tablet_meta->set_tablet_uid(_tablet_uid);
        static_cast<void>(tablet_meta->set_partition_id(kPartitionId));
        tablet_meta->_schema = _tablet_schema;
        tablet_meta->_enable_unique_key_merge_on_write = true;
        tablet_meta->_shard_id = 1;
        tablet_meta->_schema_hash = kSchemaHash;

        auto tablet =
                std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir.get(), "ingest_test");
        Status st = tablet->init();
        ASSERT_TRUE(st.ok()) << st.to_string();

        // Ensure the tablet directory exists so rowset writers can create segment files.
        std::filesystem::create_directories(tablet->tablet_path());

        auto& tablet_map = _engine->tablet_manager()->_get_tablet_map(kTabletId);
        tablet_map[kTabletId] = tablet;
        _tablet = tablet;
    }

    RowsetSharedPtr _write_rowset(int64_t rowset_id, const PUniqueId& load_id, int num_segments,
                                  int rows_per_segment) {
        RowsetWriterContext writer_context;
        RowsetId rs_id;
        rs_id.init(rowset_id);
        writer_context.rowset_id = rs_id;
        writer_context.tablet_id = kTabletId;
        writer_context.tablet_schema_hash = kSchemaHash;
        writer_context.partition_id = kPartitionId;
        writer_context.rowset_type = BETA_ROWSET;
        writer_context.tablet_path = _tablet->tablet_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = _tablet_schema;
        writer_context.version.first = 1;
        writer_context.version.second = 1;
        writer_context.txn_id = kTxnId;
        writer_context.load_id = load_id;
        writer_context.tablet = _tablet;
        writer_context.enable_unique_key_merge_on_write = true;

        // MOW context is required for merge-on-write rowsets so that segment
        // metadata (primary key index) is generated for delete bitmap calculation.
        auto rsids = std::make_shared<RowsetIdUnorderedSet>();
        std::vector<RowsetSharedPtr> rowset_ptrs;
        auto delete_bitmap = std::make_shared<DeleteBitmap>(kTabletId);
        writer_context.mow_context =
                std::make_shared<MowContext>(1, kTxnId, rsids, rowset_ptrs, delete_bitmap);

        auto res = RowsetFactory::create_rowset_writer(*_engine, writer_context, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();

        for (int seg = 0; seg < num_segments; ++seg) {
            Block block = _tablet_schema->create_block();
            auto columns = std::move(block).mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                // Use a small key space so the same keys appear in every segment.
                // calc_delete_bitmap_between_segments then generates a non-empty
                // delete bitmap keyed by the rowset id.
                int32_t k1 = rid % 5;
                int32_t v1 = k1 * 10;
                int32_t seq = 0;
                columns[0]->insert_data(reinterpret_cast<const char*>(&k1), sizeof(k1));
                columns[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(v1));
                columns[2]->insert_data(reinterpret_cast<const char*>(&seq), sizeof(seq));
            }
            block.set_columns(std::move(columns));
            Status st = rowset_writer->add_block(&block);
            EXPECT_TRUE(st.ok()) << st.to_string();
            st = rowset_writer->flush();
            EXPECT_TRUE(st.ok()) << st.to_string();
        }

        RowsetSharedPtr rowset;
        Status st = rowset_writer->build(rowset);
        EXPECT_TRUE(st.ok()) << st.to_string();
        return rowset;
    }

    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    TabletSharedPtr _tablet;
    TabletSchemaSPtr _tablet_schema;
    TabletUid _tablet_uid {10001, 10002};
};

// Verify that re-committing the same txn/load id with a different rowset id returns
// kAlreadyExist and does not overwrite the previously committed rowset or its delete bitmap.
TEST_F(BackendServiceIngestTest, CommitIngestedRowsetAlreadyExist) {
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    // First ingest commits R1.
    auto rowset_r1 = _write_rowset(60001, load_id, 2, 1);
    auto rowset_meta_r1 = rowset_r1->rowset_meta();
    auto guard_r1 = _engine->pending_local_rowsets().add(rowset_meta_r1->rowset_id());
    MonotonicStopWatch watch;
    std::unordered_map<std::string_view, uint64_t> elapsed_time_map;
    auto result = commit_ingested_rowset(*_engine, _tablet, kTxnId, kPartitionId, rowset_meta_r1,
                                         std::move(guard_r1), watch, elapsed_time_map);
    ASSERT_EQ(result, IngestCommitResult::kCommitted);

    RowsetMetaSharedPtr committed_meta_r1(new RowsetMeta());
    Status st = RowsetMetaManager::get_rowset_meta(_data_dir->get_meta(), _tablet_uid,
                                                   rowset_meta_r1->rowset_id(), committed_meta_r1);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Capture the txn delete bitmap and rowset ids after R1 commit.
    CommitTabletTxnInfoVec txn_info_vec_r1;
    _engine->txn_manager()->get_all_commit_tablet_txn_info_by_tablet(*_tablet, &txn_info_vec_r1);
    ASSERT_EQ(txn_info_vec_r1.size(), 1);
    const auto& txn_info_r1 = txn_info_vec_r1[0];
    ASSERT_EQ(txn_info_r1.transaction_id, kTxnId);
    ASSERT_EQ(txn_info_r1.partition_id, kPartitionId);
    ASSERT_TRUE(txn_info_r1.delete_bitmap != nullptr);
    const size_t r1_bitmap_count = txn_info_r1.delete_bitmap->get_delete_bitmap_count();
    const size_t r1_bitmap_cardinality = txn_info_r1.delete_bitmap->cardinality();
    const auto r1_rowset_ids = txn_info_r1.rowset_ids;

    // The same keys appear in both segments, so R1 must have generated a non-empty
    // delete bitmap keyed by its own rowset id. The sentinel mark proves the
    // multi-segment delete bitmap was calculated for R1.
    ASSERT_GT(r1_bitmap_count, 0);
    ASSERT_TRUE(txn_info_r1.delete_bitmap->contains(
            {rowset_meta_r1->rowset_id(), DeleteBitmap::INVALID_SEGMENT_ID,
             DeleteBitmap::TEMP_VERSION_COMMON},
            DeleteBitmap::ROWSET_SENTINEL_MARK));

    // Second ingest with the same txn/load id but a different rowset id must be idempotent.
    auto rowset_r2 = _write_rowset(60002, load_id, 2, 1);
    auto rowset_meta_r2 = rowset_r2->rowset_meta();
    auto guard_r2 = _engine->pending_local_rowsets().add(rowset_meta_r2->rowset_id());
    elapsed_time_map.clear();
    result = commit_ingested_rowset(*_engine, _tablet, kTxnId, kPartitionId, rowset_meta_r2,
                                    std::move(guard_r2), watch, elapsed_time_map);
    ASSERT_EQ(result, IngestCommitResult::kAlreadyExist);

    // R1 is still the committed rowset; R2 must not have replaced it.
    RowsetMetaSharedPtr committed_meta_r1_after(new RowsetMeta());
    st = RowsetMetaManager::get_rowset_meta(_data_dir->get_meta(), _tablet_uid,
                                            rowset_meta_r1->rowset_id(), committed_meta_r1_after);
    ASSERT_TRUE(st.ok()) << st.to_string();

    RowsetMetaSharedPtr committed_meta_r2(new RowsetMeta());
    st = RowsetMetaManager::get_rowset_meta(_data_dir->get_meta(), _tablet_uid,
                                            rowset_meta_r2->rowset_id(), committed_meta_r2);
    ASSERT_FALSE(st.ok());

    // The txn delete bitmap and rowset ids must be unchanged after kAlreadyExist.
    CommitTabletTxnInfoVec txn_info_vec_r2;
    _engine->txn_manager()->get_all_commit_tablet_txn_info_by_tablet(*_tablet, &txn_info_vec_r2);
    ASSERT_EQ(txn_info_vec_r2.size(), 1);
    const auto& txn_info_r2 = txn_info_vec_r2[0];
    ASSERT_EQ(txn_info_r2.transaction_id, kTxnId);
    ASSERT_EQ(txn_info_r2.partition_id, kPartitionId);
    ASSERT_TRUE(txn_info_r2.delete_bitmap != nullptr);
    ASSERT_EQ(txn_info_r2.delete_bitmap->get_delete_bitmap_count(), r1_bitmap_count);
    ASSERT_EQ(txn_info_r2.delete_bitmap->cardinality(), r1_bitmap_cardinality);
    ASSERT_EQ(txn_info_r2.rowset_ids, r1_rowset_ids);
    // R1's multi-segment sentinel mark must still be present; R2's must not appear.
    ASSERT_TRUE(txn_info_r2.delete_bitmap->contains(
            {rowset_meta_r1->rowset_id(), DeleteBitmap::INVALID_SEGMENT_ID,
             DeleteBitmap::TEMP_VERSION_COMMON},
            DeleteBitmap::ROWSET_SENTINEL_MARK));
    ASSERT_FALSE(txn_info_r2.delete_bitmap->contains(
            {rowset_meta_r2->rowset_id(), DeleteBitmap::INVALID_SEGMENT_ID,
             DeleteBitmap::TEMP_VERSION_COMMON},
            DeleteBitmap::ROWSET_SENTINEL_MARK));
}

TEST_F(BackendServiceIngestTest, DeleteDownloadedFiles) {
    auto tmp_dir = std::filesystem::path(kTestDir) / "delete_downloaded_files_test";
    std::filesystem::remove_all(tmp_dir);
    std::filesystem::create_directories(tmp_dir);

    auto f1 = tmp_dir / "file1";
    auto f2 = tmp_dir / "file2";
    {
        std::ofstream ofs(f1);
        ofs << "data1";
    }
    {
        std::ofstream ofs(f2);
        ofs << "data2";
    }
    ASSERT_TRUE(std::filesystem::exists(f1));
    ASSERT_TRUE(std::filesystem::exists(f2));

    std::vector<std::string> files = {f1.string(), f2.string()};
    _delete_downloaded_files(files, "test cleanup", kTxnId);
    ASSERT_FALSE(std::filesystem::exists(f1));
    ASSERT_FALSE(std::filesystem::exists(f2));

    // Empty list should be a no-op.
    _delete_downloaded_files({}, "empty cleanup", kTxnId);
}

// Verify that fetch_from_peer rejects a rowset which claims to have segments but
// provides no file list. This is the reverse test for the empty-rowset fast path.
TEST_F(BackendServiceIngestTest, IngestBinlogFromPeerRejectsEmptyFilesWithSegments) {
    PUniqueId pb_load_id;
    pb_load_id.set_hi(0);
    pb_load_id.set_lo(0);

    TUniqueId thrift_load_id;
    thrift_load_id.hi = 0;
    thrift_load_id.lo = 0;

    // Create a rowset with at least one segment.
    auto rowset = _write_rowset(70001, pb_load_id, 2, 1);
    auto rowset_meta = rowset->rowset_meta();
    std::string rowset_meta_str;
    ASSERT_TRUE(rowset_meta->serialize(&rowset_meta_str));

    TIngestBinlogRequest request;
    request.__set_txn_id(kTxnId);
    request.__set_partition_id(kPartitionId);
    request.__set_local_tablet_id(kTabletId);
    request.__set_load_id(thrift_load_id);
    request.__set_fetch_from_peer(true);
    request.__set_peer_host("127.0.0.1");
    request.__set_peer_http_port("8040");
    request.__set_peer_token("token");
    request.__set_rowset_meta(rowset_meta_str);
    // files intentionally left empty while rowset_meta->num_segments() > 0

    TStatus tstatus;
    _ingest_binlog_from_peer(*_engine, request, _tablet, kTxnId, kPartitionId, tstatus);
    ASSERT_EQ(tstatus.status_code, static_cast<TStatusCode::type>(TStatusCode::ANALYSIS_ERROR))
            << "expected ANALYSIS_ERROR when files is empty but num_segments > 0";
    ASSERT_FALSE(tstatus.error_msgs.empty());
}

} // namespace doris
