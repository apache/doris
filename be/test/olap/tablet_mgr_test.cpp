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
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "runtime/exec_env.h"
#include "util/uid_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class TabletMgrTest : public testing::Test {
public:
    virtual void SetUp() {
        _engine_data_path = "./be/test/olap/test_data/converter_test_data/tmp";
        auto st = io::global_local_filesystem()->delete_directory(_engine_data_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_engine_data_path);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(
                io::global_local_filesystem()->create_directory(_engine_data_path + "/meta").ok());

        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        EngineOptions options;
        // won't open engine, options.path is needless
        options.backend_uid = UniqueId::gen_uid();
        k_engine = new StorageEngine(options);
        ExecEnv::GetInstance()->set_storage_engine(k_engine);
        _data_dir = new DataDir(_engine_data_path, 1000000000);
        static_cast<void>(_data_dir->init());
        _tablet_mgr = k_engine->tablet_manager();
    }

    virtual void TearDown() {
        SAFE_DELETE(_data_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_engine_data_path).ok());
        if (k_engine != nullptr) {
            k_engine->stop();
        }
        SAFE_DELETE(k_engine);
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _tablet_mgr = nullptr;
        config::compaction_num_per_round = 1;
    }
    StorageEngine* k_engine = nullptr;

private:
    DataDir* _data_dir = nullptr;
    std::string _engine_data_path;
    TabletManager* _tablet_mgr = nullptr;
};

TEST_F(TabletMgrTest, CreateTablet) {
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    RuntimeProfile profile("CreateTablet");
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist).ok());
    EXPECT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    Status check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    EXPECT_TRUE(check_meta_st == Status::OK());

    // retry create should be successfully
    create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());

    Status drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet.reset();
    Status trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
}

TEST_F(TabletMgrTest, CreateTabletWithSequence) {
    std::vector<TColumn> cols;
    TColumn col1;
    col1.column_type.type = TPrimitiveType::SMALLINT;
    col1.__set_column_name("col1");
    col1.__set_is_key(true);
    cols.push_back(col1);

    TColumn col2;
    col2.column_type.type = TPrimitiveType::INT;
    col2.__set_column_name(SEQUENCE_COL);
    col2.__set_is_key(false);
    col2.__set_aggregation_type(TAggregationType::REPLACE);
    cols.push_back(col2);

    TColumn col3;
    col3.column_type.type = TPrimitiveType::INT;
    col3.__set_column_name("v1");
    col3.__set_is_key(false);
    col3.__set_aggregation_type(TAggregationType::REPLACE);
    cols.push_back(col3);

    RuntimeProfile profile("CreateTablet");
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    tablet_schema.__set_sequence_col_idx(1);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());

    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist).ok());
    EXPECT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    Status check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    EXPECT_TRUE(check_meta_st == Status::OK());

    Status drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet.reset();
    Status trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
}

TEST_F(TabletMgrTest, DropTablet) {
    RuntimeProfile profile("CreateTablet");
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    // drop unexist tablet will be success
    Status drop_st = _tablet_mgr->drop_tablet(1121, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    // drop exist tablet will be success
    drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet == nullptr);
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet != nullptr);

    // check dir exist
    std::string tablet_path = tablet->tablet_path();
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_TRUE(dir_exist);

    // do trash sweep, tablet will not be garbage collected
    // because tablet ptr referenced it
    Status trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet != nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_TRUE(dir_exist);

    // reset tablet ptr
    tablet.reset();
    trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet == nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_FALSE(dir_exist);
}

TEST_F(TabletMgrTest, GetRowsetId) {
    // normal case
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);
    }
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781/";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);
    }
    // normal case
    {
        std::string path =
                _engine_data_path +
                "/data/0/15007/368169781/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);

        RowsetId id;
        EXPECT_TRUE(_tablet_mgr->get_rowset_id_from_path(path, &id));
        EXPECT_EQ(2UL << 56 | 1, id.hi);
        EXPECT_EQ(2, id.mi);
        EXPECT_EQ(3, id.lo);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(0, schema_hash);

        RowsetId id;
        EXPECT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007/";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(0, schema_hash);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007abc";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_FALSE(
                _tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
    }
    // not match pattern
    {
        std::string path =
                _engine_data_path +
                "/data/0/15007/123abc/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_FALSE(
                _tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));

        RowsetId id;
        EXPECT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
}

TEST_F(TabletMgrTest, FindTabletWithCompact) {
    auto create_tablet = [this](int64_t tablet_id, bool enable_single_compact, int rowset_size) {
        std::vector<TColumn> cols;
        TColumn col1;
        col1.column_type.type = TPrimitiveType::SMALLINT;
        col1.__set_column_name("col1");
        col1.__set_is_key(true);
        cols.push_back(col1);

        TColumn col2;
        col2.column_type.type = TPrimitiveType::INT;
        col2.__set_column_name(SEQUENCE_COL);
        col2.__set_is_key(false);
        col2.__set_aggregation_type(TAggregationType::REPLACE);
        cols.push_back(col2);

        TColumn col3;
        col3.column_type.type = TPrimitiveType::INT;
        col3.__set_column_name("v1");
        col3.__set_is_key(false);
        col3.__set_aggregation_type(TAggregationType::REPLACE);
        cols.push_back(col3);

        RuntimeProfile profile("CreateTablet");
        TTabletSchema tablet_schema;
        tablet_schema.__set_short_key_column_count(1);
        tablet_schema.__set_schema_hash(3333);
        tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
        tablet_schema.__set_storage_type(TStorageType::COLUMN);
        tablet_schema.__set_columns(cols);
        tablet_schema.__set_sequence_col_idx(1);
        tablet_schema.__set_enable_single_replica_compaction(enable_single_compact);
        TCreateTabletReq create_tablet_req;
        create_tablet_req.__set_tablet_schema(tablet_schema);
        create_tablet_req.__set_tablet_id(tablet_id);
        create_tablet_req.__set_version(1);
        create_tablet_req.__set_replica_id(tablet_id * 10);
        std::vector<DataDir*> data_dirs;
        data_dirs.push_back(_data_dir);
        Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
        ASSERT_TRUE(create_st.ok()) << create_st;

        TabletSharedPtr tablet = _tablet_mgr->get_tablet(tablet_id);
        ASSERT_TRUE(tablet);
        // check dir exist
        bool dir_exist = false;
        Status exist_st = io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist);
        ASSERT_TRUE(exist_st.ok()) << exist_st;
        ASSERT_TRUE(dir_exist);
        // check meta has this tablet
        TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
        Status check_meta_st =
                TabletMetaManager::get_meta(_data_dir, tablet_id, 3333, new_tablet_meta);
        ASSERT_TRUE(check_meta_st.ok()) << check_meta_st;
        // insert into rowset
        auto create_rowset = [=, this](int64_t start, int64 end) {
            auto rowset_meta = std::make_shared<RowsetMeta>();
            Version version(start, end);
            rowset_meta->set_version(version);
            rowset_meta->set_tablet_id(tablet->tablet_id());
            rowset_meta->set_tablet_uid(tablet->tablet_uid());
            rowset_meta->set_rowset_id(k_engine->next_rowset_id());
            return std::make_shared<BetaRowset>(tablet->tablet_schema(), tablet->tablet_path(),
                                                std::move(rowset_meta));
        };
        auto st = tablet->init();
        ASSERT_TRUE(st.ok()) << st;
        for (int i = 2; i <= rowset_size; ++i) {
            auto rs = create_rowset(i, i);
            auto st = tablet->add_inc_rowset(rs);
            ASSERT_TRUE(st.ok()) << st;
        }
    };

    int rowset_size = 5;

    // create 10 tablets
    for (int64_t id = 1; id <= 10; ++id) {
        create_tablet(id, false, rowset_size++);
    }

    std::unordered_set<TabletSharedPtr> cumu_set;
    std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>
            cumulative_compaction_policies;
    cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_SIZE_BASED_POLICY);
    cumulative_compaction_policies[CUMULATIVE_TIME_SERIES_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_TIME_SERIES_POLICY);
    uint32_t score = 0;
    auto compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0]->tablet_id(), 10);
    ASSERT_EQ(score, 13);

    // create 10 tablets enable single compact
    // 5 tablets do cumu compaction, 5 tablets do single compaction
    // if BE_TEST is defined, tablet_id % 2 == 0 means that tablet needs to do single compact
    for (int64_t id = 11; id <= 20; ++id) {
        create_tablet(id, true, rowset_size++);
    }

    compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0]->tablet_id(), 20);
    ASSERT_EQ(score, 23);

    create_tablet(21, false, rowset_size++);

    compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0]->tablet_id(), 21);
    ASSERT_EQ(score, 24);

    // drop all tablets
    for (int64_t id = 1; id <= 21; ++id) {
        Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
    }

    {
        config::compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 100; ++i) {
            create_tablet(10000 + i, false, i);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 10);
        int index = 0;
        for (auto t : compact_tablets) {
            ASSERT_EQ(t->tablet_id(), 10100 - index);
            ASSERT_EQ(t->calc_compaction_score(
                              CompactionType::CUMULATIVE_COMPACTION,
                              cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY]),
                      99 - index);
            index++;
        }
        config::compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 10001; id <= 10100; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }
    }

    {
        config::compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 100; ++i) {
            create_tablet(20000 + i, false, i);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 10);
        for (int i = 0; i < 10; ++i) {
            ASSERT_EQ(compact_tablets[i]->tablet_id(), 20100 - i);
            ASSERT_EQ(compact_tablets[i]->calc_compaction_score(
                              CompactionType::CUMULATIVE_COMPACTION,
                              cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY]),
                      99 - i);
        }

        config::compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 20001; id <= 20100; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }

        Status drop_st = _tablet_mgr->drop_tablet(20102, 20102 * 10, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
    }

    {
        config::compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 5; ++i) {
            create_tablet(30000 + i, false, i + 5);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 5);
        for (int i = 0; i < 5; ++i) {
            ASSERT_EQ(compact_tablets[i]->tablet_id(), 30000 + 5 - i);
            ASSERT_EQ(compact_tablets[i]->calc_compaction_score(
                              CompactionType::CUMULATIVE_COMPACTION,
                              cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY]),
                      9 - i);
        }

        config::compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 30001; id <= 30005; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }
    }

    Status trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st.ok()) << trash_st;
}

} // namespace doris
