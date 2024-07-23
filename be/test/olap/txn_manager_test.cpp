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

#include "olap/txn_manager.h"

#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <new>
#include <string>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/olap_meta.h"
#include "olap/options.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_schema.h"
#include "olap/task/engine_publish_version_task.h"
#include "util/uid_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

static std::unique_ptr<StorageEngine> k_engine;

const std::string rowset_meta_path = "./be/test/olap/test_data/rowset_meta.json";
const std::string rowset_meta_path_2 = "./be/test/olap/test_data/rowset_meta2.json";
const std::string rowset_meta_path_3 = "./be/test/olap/test_data/rowset_meta3.json";

class TxnManagerTest : public testing::Test {
public:
    void init_tablet_schema() {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(DUP_KEYS);
        tablet_schema_pb.set_num_short_key_columns(3);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(4);

        ColumnPB* column_1 = tablet_schema_pb.add_column();
        column_1->set_unique_id(1);
        column_1->set_name("k1");
        column_1->set_type("INT");
        column_1->set_is_key(true);
        column_1->set_length(4);
        column_1->set_index_length(4);
        column_1->set_is_nullable(true);
        column_1->set_is_bf_column(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("k2");
        column_2->set_type("INT");
        column_2->set_length(4);
        column_2->set_index_length(4);
        column_2->set_is_nullable(true);
        column_2->set_is_key(true);
        column_2->set_is_nullable(true);
        column_2->set_is_bf_column(false);

        ColumnPB* column_3 = tablet_schema_pb.add_column();
        column_3->set_unique_id(3);
        column_3->set_name("v1");
        column_3->set_type("VARCHAR");
        column_3->set_length(10);
        column_3->set_index_length(10);
        column_3->set_is_key(true);
        column_3->set_is_nullable(false);
        column_3->set_is_bf_column(false);

        _schema.reset(new TabletSchema);
        _schema->init_from_pb(tablet_schema_pb);
    }

    void create_tablet() {
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->set_tablet_uid(_tablet_uid);
        auto tablet = std::make_shared<Tablet>(*k_engine, std::move(tablet_meta), nullptr);
        auto& tablet_map = k_engine->tablet_manager()->_get_tablet_map(tablet_id);
        tablet_map[tablet_id] = std::move(tablet);
    }

    virtual void SetUp() {
        config::max_runnings_transactions_per_txn_map = 500;

        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        // won't open engine, options.path is needless
        k_engine = std::make_unique<StorageEngine>(options);
        create_tablet();
        std::string meta_path = "./meta";
        std::filesystem::remove_all("./meta");
        EXPECT_TRUE(std::filesystem::create_directory(meta_path));
        _meta = std::make_unique<OlapMeta>(meta_path);
        Status st = _meta->init();
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(std::filesystem::exists("./meta"));
        load_id.set_hi(0);
        load_id.set_lo(0);

        init_tablet_schema();

        // init rowset meta 1
        std::ifstream infile(rowset_meta_path);
        char buffer[1024];
        while (!infile.eof()) {
            infile.getline(buffer, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer + "\n";
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        RowsetId rowset_id;
        rowset_id.init(10000);
        RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
        rowset_meta->init_from_json(_json_rowset_meta);
        EXPECT_EQ(rowset_meta->rowset_id(), rowset_id);
        EXPECT_EQ(Status::OK(),
                  RowsetFactory::create_rowset(_schema, rowset_meta_path, rowset_meta, &_rowset));
        EXPECT_EQ(Status::OK(), RowsetFactory::create_rowset(_schema, rowset_meta_path, rowset_meta,
                                                             &_rowset_same_id));

        // init rowset meta 2
        _json_rowset_meta = "";
        std::ifstream infile2(rowset_meta_path_2);
        char buffer2[1024];
        while (!infile2.eof()) {
            infile2.getline(buffer2, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer2 + "\n";
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        rowset_id.init(10001);
        RowsetMetaSharedPtr rowset_meta2(new RowsetMeta());
        rowset_meta2->init_from_json(_json_rowset_meta);
        EXPECT_EQ(rowset_meta2->rowset_id(), rowset_id);
        EXPECT_EQ(Status::OK(), RowsetFactory::create_rowset(_schema, rowset_meta_path_2,
                                                             rowset_meta2, &_rowset_diff_id));

        // init rowset meta 3
        _json_rowset_meta = "";
        std::ifstream infile3(rowset_meta_path_3);
        char buffer3[1024];
        while (!infile3.eof()) {
            infile3.getline(buffer3, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer3 + "\n";
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        rowset_id.init(10002);
        RowsetMetaSharedPtr rowset_meta3(new RowsetMeta());
        rowset_meta3->init_from_json(_json_rowset_meta);
        EXPECT_EQ(rowset_meta3->rowset_id(), rowset_id);
        EXPECT_EQ(Status::OK(), RowsetFactory::create_rowset(_schema, rowset_meta_path_3,
                                                             rowset_meta3, &_rowset_ingested));
        _tablet_uid = TabletUid(10, 10);
    }

    virtual void TearDown() {
        k_engine.reset();
        EXPECT_TRUE(std::filesystem::remove_all("./meta"));
    }

private:
    std::unique_ptr<OlapMeta> _meta;
    std::string _json_rowset_meta;
    TPartitionId partition_id = 1123;
    TTransactionId transaction_id = 111;
    TTabletId tablet_id = 222;
    TabletUid _tablet_uid {0, 0};
    PUniqueId load_id;
    TabletSchemaSPtr _schema;
    RowsetSharedPtr _rowset;
    RowsetSharedPtr _rowset_same_id;
    RowsetSharedPtr _rowset_diff_id;
    RowsetSharedPtr _rowset_ingested;
};

TEST_F(TxnManagerTest, PrepareNewTxn) {
    Status status = k_engine->txn_manager()->prepare_txn(partition_id, transaction_id, tablet_id,
                                                         _tablet_uid, load_id);
    EXPECT_TRUE(status == Status::OK());
}

// 1. prepare txn
// 2. commit txn
// 3. should be success
TEST_F(TxnManagerTest, CommitTxnWithPrepare) {
    auto st = k_engine->txn_manager()->prepare_txn(partition_id, transaction_id, tablet_id,
                                                   _tablet_uid, load_id);
    ASSERT_TRUE(st.ok()) << st;
    auto guard = k_engine->pending_local_rowsets().add(_rowset->rowset_id());
    st = k_engine->txn_manager()->commit_txn(_meta.get(), partition_id, transaction_id, tablet_id,
                                             _tablet_uid, load_id, _rowset, std::move(guard),
                                             false);
    ASSERT_TRUE(st.ok()) << st;
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    st = RowsetMetaManager::get_rowset_meta(_meta.get(), _tablet_uid, _rowset->rowset_id(),
                                            rowset_meta);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(rowset_meta->rowset_id(), _rowset->rowset_id());
    EXPECT_TRUE(k_engine->pending_local_rowsets().contains(_rowset->rowset_id()));
}

// 1. commit without prepare
// 2. should success
TEST_F(TxnManagerTest, CommitTxnWithNoPrepare) {
    auto guard = k_engine->pending_local_rowsets().add(_rowset->rowset_id());
    auto st = k_engine->txn_manager()->commit_txn(_meta.get(), partition_id, transaction_id,
                                                  tablet_id, _tablet_uid, load_id, _rowset,
                                                  std::move(guard), false);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(k_engine->pending_local_rowsets().contains(_rowset->rowset_id()));
}

// 1. commit twice with different rowset id
// 2. should failed
TEST_F(TxnManagerTest, CommitTxnTwiceWithDiffRowsetId) {
    auto guard1 = k_engine->pending_local_rowsets().add(_rowset->rowset_id());
    auto st = k_engine->txn_manager()->commit_txn(_meta.get(), partition_id, transaction_id,
                                                  tablet_id, _tablet_uid, load_id, _rowset,
                                                  std::move(guard1), false);
    ASSERT_TRUE(st.ok()) << st;
    auto guard2 = k_engine->pending_local_rowsets().add(_rowset_diff_id->rowset_id());
    st = k_engine->txn_manager()->commit_txn(_meta.get(), partition_id, transaction_id, tablet_id,
                                             _tablet_uid, load_id, _rowset_diff_id,
                                             std::move(guard2), false);
    ASSERT_FALSE(st.ok()) << st;
    EXPECT_TRUE(k_engine->pending_local_rowsets().contains(_rowset->rowset_id()));
    EXPECT_FALSE(k_engine->pending_local_rowsets().contains(_rowset_diff_id->rowset_id()));
}

// 1. commit twice with same rowset id
// 2. should success
TEST_F(TxnManagerTest, CommitTxnTwiceWithSameRowsetId) {
    auto guard1 = k_engine->pending_local_rowsets().add(_rowset->rowset_id());
    auto st = k_engine->txn_manager()->commit_txn(_meta.get(), partition_id, transaction_id,
                                                  tablet_id, _tablet_uid, load_id, _rowset,
                                                  std::move(guard1), false);
    ASSERT_TRUE(st.ok()) << st;
    auto guard2 = k_engine->pending_local_rowsets().add(_rowset->rowset_id());
    st = k_engine->txn_manager()->commit_txn(_meta.get(), partition_id, transaction_id, tablet_id,
                                             _tablet_uid, load_id, _rowset_same_id,
                                             std::move(guard2), false);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(k_engine->pending_local_rowsets().contains(_rowset->rowset_id()));
}

// 1. prepare twice should be success
TEST_F(TxnManagerTest, PrepareNewTxnTwice) {
    auto st = k_engine->txn_manager()->prepare_txn(partition_id, transaction_id, tablet_id,
                                                   _tablet_uid, load_id);
    ASSERT_TRUE(st.ok()) << st;
    st = k_engine->txn_manager()->prepare_txn(partition_id, transaction_id, tablet_id, _tablet_uid,
                                              load_id);
    ASSERT_TRUE(st.ok()) << st;
}

// 1. txn could be rollbacked if it is not committed
TEST_F(TxnManagerTest, RollbackNotCommittedTxn) {
    auto st = k_engine->txn_manager()->prepare_txn(partition_id, transaction_id, tablet_id,
                                                   _tablet_uid, load_id);
    ASSERT_TRUE(st.ok()) << st;
    st = k_engine->txn_manager()->rollback_txn(partition_id, transaction_id, tablet_id,
                                               _tablet_uid);
    ASSERT_TRUE(st.ok()) << st;
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    st = RowsetMetaManager::get_rowset_meta(_meta.get(), _tablet_uid, _rowset->rowset_id(),
                                            rowset_meta);
    ASSERT_FALSE(st.ok()) << st;
}

// 1. txn could not be rollbacked if it is committed
TEST_F(TxnManagerTest, RollbackCommittedTxn) {
    auto guard = k_engine->pending_local_rowsets().add(_rowset->rowset_id());
    auto st = k_engine->txn_manager()->commit_txn(_meta.get(), partition_id, transaction_id,
                                                  tablet_id, _tablet_uid, load_id, _rowset,
                                                  std::move(guard), false);
    ASSERT_TRUE(st.ok()) << st;
    st = k_engine->txn_manager()->rollback_txn(partition_id, transaction_id, tablet_id,
                                               _tablet_uid);
    ASSERT_FALSE(st.ok()) << st;
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    st = RowsetMetaManager::get_rowset_meta(_meta.get(), _tablet_uid, _rowset->rowset_id(),
                                            rowset_meta);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(rowset_meta->rowset_id(), _rowset->rowset_id());
    EXPECT_TRUE(k_engine->pending_local_rowsets().contains(_rowset->rowset_id()));
}

// 1. publish version success
TEST_F(TxnManagerTest, PublishVersionSuccessful) {
    auto guard = k_engine->pending_local_rowsets().add(_rowset->rowset_id());
    auto st = k_engine->txn_manager()->commit_txn(_meta.get(), partition_id, transaction_id,
                                                  tablet_id, _tablet_uid, load_id, _rowset,
                                                  std::move(guard), false);
    ASSERT_TRUE(st.ok()) << st;
    Version new_version(10, 11);
    TabletPublishStatistics stats;
    st = k_engine->txn_manager()->publish_txn(_meta.get(), partition_id, transaction_id, tablet_id,
                                              _tablet_uid, new_version, &stats);
    ASSERT_TRUE(st.ok()) << st;

    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    st = RowsetMetaManager::get_rowset_meta(_meta.get(), _tablet_uid, _rowset->rowset_id(),
                                            rowset_meta);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(rowset_meta->rowset_id(), _rowset->rowset_id());
    EXPECT_FALSE(k_engine->pending_local_rowsets().contains(_rowset->rowset_id()));
    EXPECT_EQ(rowset_meta->start_version(), 10);
    EXPECT_EQ(rowset_meta->end_version(), 11);
}

// 1. publish version failed if not found related txn and rowset
TEST_F(TxnManagerTest, PublishNotExistedTxn) {
    Version new_version(10, 11);
    auto not_exist_txn = transaction_id + 1000;
    TabletPublishStatistics stats;
    auto st = k_engine->txn_manager()->publish_txn(_meta.get(), partition_id, not_exist_txn,
                                                   tablet_id, _tablet_uid, new_version, &stats);
    ASSERT_FALSE(st.ok()) << st;
}

TEST_F(TxnManagerTest, DeletePreparedTxn) {
    auto st = k_engine->txn_manager()->prepare_txn(partition_id, transaction_id, tablet_id,
                                                   _tablet_uid, load_id);
    ASSERT_TRUE(st.ok()) << st;
    st = k_engine->txn_manager()->delete_txn(_meta.get(), partition_id, transaction_id, tablet_id,
                                             _tablet_uid);
    ASSERT_TRUE(st.ok()) << st;
}

TEST_F(TxnManagerTest, DeleteCommittedTxn) {
    auto guard = k_engine->pending_local_rowsets().add(_rowset->rowset_id());
    auto st = k_engine->txn_manager()->commit_txn(_meta.get(), partition_id, transaction_id,
                                                  tablet_id, _tablet_uid, load_id, _rowset,
                                                  std::move(guard), false);
    ASSERT_TRUE(st.ok()) << st;
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    st = RowsetMetaManager::get_rowset_meta(_meta.get(), _tablet_uid, _rowset->rowset_id(),
                                            rowset_meta);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(rowset_meta->rowset_id(), _rowset->rowset_id());
    st = k_engine->txn_manager()->delete_txn(_meta.get(), partition_id, transaction_id, tablet_id,
                                             _tablet_uid);
    ASSERT_TRUE(st.ok()) << st;
    RowsetMetaSharedPtr rowset_meta2(new RowsetMeta());
    st = RowsetMetaManager::get_rowset_meta(_meta.get(), _tablet_uid, _rowset->rowset_id(),
                                            rowset_meta2);
    ASSERT_FALSE(st.ok()) << st;
    EXPECT_FALSE(k_engine->pending_local_rowsets().contains(_rowset->rowset_id()));
}

TEST_F(TxnManagerTest, TabletVersionCache) {
    std::unique_ptr<TxnManager> txn_mgr = std::make_unique<TxnManager>(*k_engine, 64, 1024);
    txn_mgr->update_tablet_version_txn(123, 100, 456);
    txn_mgr->update_tablet_version_txn(124, 100, 567);
    int64_t tx1 = txn_mgr->get_txn_by_tablet_version(123, 100);
    EXPECT_EQ(tx1, 456);
    int64_t tx2 = txn_mgr->get_txn_by_tablet_version(124, 100);
    EXPECT_EQ(tx2, 567);
    int64_t tx3 = txn_mgr->get_txn_by_tablet_version(124, 101);
    EXPECT_EQ(tx3, -1);
    txn_mgr->update_tablet_version_txn(123, 101, 888);
    txn_mgr->update_tablet_version_txn(124, 101, 890);
    int64_t tx4 = txn_mgr->get_txn_by_tablet_version(123, 100);
    EXPECT_EQ(tx4, 456);
    int64_t tx5 = txn_mgr->get_txn_by_tablet_version(123, 101);
    EXPECT_EQ(tx5, 888);
    int64_t tx6 = txn_mgr->get_txn_by_tablet_version(124, 101);
    EXPECT_EQ(tx6, 890);
}

TEST_F(TxnManagerTest, DeleteCommittedTxnForIngestingBinlog) {
    auto guard = k_engine->pending_local_rowsets().add(_rowset_ingested->rowset_id());
    auto st = k_engine->txn_manager()->commit_txn(_meta.get(), partition_id, transaction_id,
                                                  tablet_id, _tablet_uid, load_id, _rowset_ingested,
                                                  std::move(guard), false);
    ASSERT_TRUE(st.ok()) << st;
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    st = RowsetMetaManager::get_rowset_meta(_meta.get(), _tablet_uid, _rowset_ingested->rowset_id(),
                                            rowset_meta);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(rowset_meta->rowset_id(), _rowset_ingested->rowset_id());
    st = k_engine->txn_manager()->delete_txn(_meta.get(), partition_id, transaction_id, tablet_id,
                                             _tablet_uid);
    ASSERT_TRUE(st.ok()) << st;
    RowsetMetaSharedPtr rowset_meta2(new RowsetMeta());
    st = RowsetMetaManager::get_rowset_meta(_meta.get(), _tablet_uid, _rowset_ingested->rowset_id(),
                                            rowset_meta2);
    ASSERT_FALSE(st.ok()) << st;
    EXPECT_FALSE(k_engine->pending_local_rowsets().contains(_rowset_ingested->rowset_id()));
}

} // namespace doris
