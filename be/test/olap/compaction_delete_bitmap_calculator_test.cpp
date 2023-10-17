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
#include "olap/olap_meta.h"
#include "olap/storage_engine.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/rowset_factory.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/txn_manager.h"
#include "util/uid_util.h"

namespace doris {


static StorageEngine* k_engine = nullptr;

const std::string rowset_meta_path = "./be/test/olap/test_data/rowset_meta.json";
const std::string rowset_meta_path_2 = "./be/test/olap/test_data/rowset_meta2.json";

class TestCompactionDeleteBitmapCalculator: public testing::Test {
public:
    TestCompactionDeleteBitmapCalculator() = default;
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

     void SetUp() override {
        config::max_runnings_transactions_per_txn_map = 500;
        _txn_mgr.reset(new TxnManager(64, 1024));

        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        EngineOptions options;
        // won't open engine, options.path is needless
        options.backend_uid = UniqueId::gen_uid();
        if (k_engine == nullptr) {
            k_engine = new StorageEngine(options);
        }
        ExecEnv::GetInstance()->set_storage_engine(k_engine);

        std::string meta_path = "./meta";
        std::filesystem::remove_all("./meta");
        EXPECT_TRUE(std::filesystem::create_directory(meta_path));
        _meta = new (std::nothrow) OlapMeta(meta_path);
        EXPECT_NE(nullptr, _meta);
        Status st = _meta->init();
        EXPECT_TRUE(st == Status::OK());
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
        _tablet_uid = TabletUid(10, 10);
    }

     void TearDown() override {
        delete _meta;
        EXPECT_TRUE(std::filesystem::remove_all("./meta"));
    }

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;
    std::unique_ptr<TxnManager> _txn_mgr;
    TPartitionId partition_id = 1123;
    TTransactionId transaction_id = 111;
    TTabletId tablet_id = 222;
    TabletUid _tablet_uid {0, 0};
    PUniqueId load_id;
    TabletSchemaSPtr _schema;
    RowsetSharedPtr _rowset0;
    RowsetSharedPtr _rowset1;
    RowsetSharedPtr _rowset2;
};

TEST_F(TestCompactionDeleteBitmapCalculator, a) {
    Status status =
            _txn_mgr->prepare_txn(partition_id, transaction_id, tablet_id, _tablet_uid, load_id);
    static_cast<void>(_txn_mgr->commit_txn(_meta, partition_id, transaction_id, tablet_id,
                                           _tablet_uid, load_id, _rowset, false));
    EXPECT_TRUE(status == Status::OK());
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    status = RowsetMetaManager::get_rowset_meta(_meta, _tablet_uid, _rowset->rowset_id(),
                                                rowset_meta);
    EXPECT_TRUE(status == Status::OK());
    EXPECT_TRUE(rowset_meta->rowset_id() == _rowset->rowset_id());

    static_cast<void>(_tablet->init());

    std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_SIZE_BASED_POLICY);
    const uint32_t score = _tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION,
                                                          cumulative_compaction_policy);

    CommitTabletTxnInfoVec commit_tablet_txn_info_vec {};
    StorageEngine::instance()->txn_manager()->get_all_commit_tablet_txn_info_by_tablet(
                    _tablet, &commit_tablet_txn_info_vec);
    _tablet->calc_compaction_output_rowset_delete_bitmap(
                            _input_rowsets, _rowid_conversion, 0, UINT64_MAX, &missed_rows,
                            &location_map, *it.delete_bitmap.get(), &txn_output_delete_bitmap);
}
} // namespace doris

// @brief Test Stub