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
#include <string_view>
#include <vector>

#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/compaction/cumulative_compaction_policy.h"
#include "storage/compaction/cumulative_compaction_time_series_policy.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class TabletMgrTest : public testing::Test {
public:
    virtual void SetUp() {
        _engine_data_path = "./be/test/storage/test_data/converter_test_data/tmp";
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
        auto engine = std::make_unique<StorageEngine>(options);
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        k_engine = &ExecEnv::GetInstance()->storage_engine().to_local();
        _data_dir = new DataDir(*k_engine, _engine_data_path, 1000000000);
        static_cast<void>(_data_dir->init());
        _tablet_mgr = k_engine->tablet_manager();
    }

    virtual void TearDown() {
        SAFE_DELETE(_data_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_engine_data_path).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _tablet_mgr = nullptr;
        config::compaction_num_per_round = 1;
    }

    TabletSharedPtr create_compaction_tablet(
            int64_t tablet_id, int rowset_size,
            std::string_view compaction_policy = CUMULATIVE_SIZE_BASED_POLICY,
            DataDir* data_dir = nullptr) {
        data_dir = data_dir == nullptr ? _data_dir : data_dir;
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
        create_tablet_req.__set_tablet_id(tablet_id);
        create_tablet_req.__set_version(1);
        create_tablet_req.__set_replica_id(tablet_id * 10);
        create_tablet_req.__set_compaction_policy(std::string(compaction_policy));
        if (compaction_policy == CUMULATIVE_TIME_SERIES_POLICY) {
            create_tablet_req.__set_time_series_compaction_file_count_threshold(1);
        }
        std::vector<DataDir*> data_dirs;
        data_dirs.push_back(data_dir);
        Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
        if (!create_st.ok()) {
            ADD_FAILURE() << create_st;
            return nullptr;
        }

        TabletSharedPtr tablet = _tablet_mgr->get_tablet(tablet_id);
        if (tablet == nullptr) {
            ADD_FAILURE() << "failed to get tablet " << tablet_id;
            return nullptr;
        }

        auto create_rowset = [=, this](int64_t start, int64_t end) {
            auto rowset_meta = std::make_shared<RowsetMeta>();
            Version version(start, end);
            rowset_meta->set_version(version);
            rowset_meta->set_tablet_id(tablet->tablet_id());
            rowset_meta->set_tablet_uid(tablet->tablet_uid());
            rowset_meta->set_rowset_id(k_engine->next_rowset_id());
            return std::make_shared<BetaRowset>(tablet->tablet_schema(), std::move(rowset_meta),
                                                tablet->tablet_path());
        };
        auto st = tablet->init();
        if (!st.ok()) {
            ADD_FAILURE() << st;
            return nullptr;
        }
        for (int i = 2; i <= rowset_size; ++i) {
            auto rs = create_rowset(i, i);
            st = tablet->add_inc_rowset(rs);
            if (!st.ok()) {
                ADD_FAILURE() << st;
                return nullptr;
            }
        }
        return tablet;
    }

    StorageEngine* k_engine;

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
    auto create_tablet = [this](int64_t tablet_id, int rowset_size,
                                std::string_view compaction_policy = CUMULATIVE_SIZE_BASED_POLICY) {
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
        create_tablet_req.__set_tablet_id(tablet_id);
        create_tablet_req.__set_version(1);
        create_tablet_req.__set_replica_id(tablet_id * 10);
        create_tablet_req.__set_compaction_policy(std::string(compaction_policy));
        if (compaction_policy == CUMULATIVE_TIME_SERIES_POLICY) {
            create_tablet_req.__set_time_series_compaction_file_count_threshold(1);
        }
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
        auto create_rowset = [=, this](int64_t start, int64_t end) {
            auto rowset_meta = std::make_shared<RowsetMeta>();
            Version version(start, end);
            rowset_meta->set_version(version);
            rowset_meta->set_tablet_id(tablet->tablet_id());
            rowset_meta->set_tablet_uid(tablet->tablet_uid());
            rowset_meta->set_rowset_id(k_engine->next_rowset_id());
            return std::make_shared<BetaRowset>(tablet->tablet_schema(), std::move(rowset_meta),
                                                tablet->tablet_path());
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
        create_tablet(id, rowset_size++);
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
    CompactionScoreStats score_stats;
    auto compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0].tablet->tablet_id(), 10);
    ASSERT_TRUE(score_stats.scanned);
    ASSERT_EQ(score_stats.max_score, 14);
    ASSERT_EQ(score_stats.size_based_max_score, 14);
    ASSERT_EQ(score_stats.time_series_max_score, 0);

    // create 10 more tablets with higher compaction scores
    for (int64_t id = 11; id <= 20; ++id) {
        create_tablet(id, rowset_size++);
    }

    compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0].tablet->tablet_id(), 20);
    ASSERT_EQ(score_stats.max_score, 24);
    ASSERT_EQ(score_stats.size_based_max_score, 24);
    ASSERT_EQ(score_stats.time_series_max_score, 0);

    create_tablet(21, rowset_size++);

    compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0].tablet->tablet_id(), 21);
    ASSERT_EQ(score_stats.max_score, 25);
    ASSERT_EQ(score_stats.size_based_max_score, 25);
    ASSERT_EQ(score_stats.time_series_max_score, 0);

    // drop all tablets
    for (int64_t id = 1; id <= 21; ++id) {
        Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
    }

    {
        create_tablet(40001, 8, CUMULATIVE_SIZE_BASED_POLICY);
        create_tablet(40002, 12, CUMULATIVE_TIME_SERIES_POLICY);

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
                cumulative_compaction_policies);
        ASSERT_TRUE(score_stats.scanned);
        ASSERT_EQ(score_stats.max_score, 12);
        ASSERT_EQ(score_stats.size_based_max_score, 8);
        ASSERT_EQ(score_stats.time_series_max_score, 12);
        ASSERT_EQ(compact_tablets.size(), 1);
        ASSERT_EQ(compact_tablets[0].tablet->tablet_id(), 40002);

        Status drop_st = _tablet_mgr->drop_tablet(40001, 400010, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
        drop_st = _tablet_mgr->drop_tablet(40002, 400020, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
    }

    {
        k_engine->_compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 100; ++i) {
            create_tablet(10000 + i, i);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 10);
        int index = 0;
        for (auto& t : compact_tablets) {
            ASSERT_EQ(t.tablet->tablet_id(), 10100 - index);
            ASSERT_EQ(t.tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION),
                      100 - index);
            index++;
        }
        k_engine->_compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 10001; id <= 10100; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }
    }

    {
        k_engine->_compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 5; ++i) {
            create_tablet(30000 + i, i + 5);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 5);
        for (int i = 0; i < 5; ++i) {
            ASSERT_EQ(compact_tablets[i].tablet->tablet_id(), 30000 + 5 - i);
            ASSERT_EQ(compact_tablets[i].tablet->calc_compaction_score(
                              CompactionType::CUMULATIVE_COMPACTION),
                      10 - i);
        }

        k_engine->_compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 30001; id <= 30005; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }
    }

    Status trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st.ok()) << trash_st;
}

TEST_F(TabletMgrTest, FindBestTabletsIgnoresUnsuitablePolicyScore) {
    auto tablet = create_compaction_tablet(50001, 12, CUMULATIVE_TIME_SERIES_POLICY);
    ASSERT_TRUE(tablet != nullptr);
    ASSERT_GT(tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION), 5);

    bool old_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    Defer restore_debug_points([&] { config::enable_debug_points = old_enable_debug_points; });
    DebugPoints::instance()->add("Tablet._calc_cumulative_compaction_score.return");
    Defer clear_debug_point([] { DebugPoints::instance()->clear(); });

    std::unordered_set<TabletSharedPtr> cumu_set;
    std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>
            cumulative_compaction_policies;
    cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_SIZE_BASED_POLICY);
    cumulative_compaction_policies[CUMULATIVE_TIME_SERIES_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_TIME_SERIES_POLICY);

    CompactionScoreStats score_stats;
    auto compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
            cumulative_compaction_policies);
    ASSERT_TRUE(score_stats.scanned);
    ASSERT_EQ(score_stats.max_score, 0);
    ASSERT_EQ(score_stats.size_based_max_score, 0);
    ASSERT_EQ(score_stats.time_series_max_score, 0);
    ASSERT_TRUE(compact_tablets.empty());
}

TEST_F(TabletMgrTest, GenerateCompactionTasksClearsMissingPolicyScoreOnCheck) {
    auto tablet = create_compaction_tablet(51001, 8, CUMULATIVE_SIZE_BASED_POLICY);
    ASSERT_TRUE(tablet != nullptr);
    auto* metrics = DorisMetrics::instance();
    metrics->tablet_cumulative_max_compaction_score->set_value(101);
    metrics->tablet_time_series_max_compaction_score->set_value(200);

    std::vector<DataDir*> data_dirs {_data_dir};
    auto tasks = k_engine->generate_compaction_tasks_for_test(CompactionType::CUMULATIVE_COMPACTION,
                                                              data_dirs, true);

    ASSERT_EQ(tasks.size(), 1);
    ASSERT_EQ(tasks[0]->tablet_id(), 51001);
    ASSERT_EQ(metrics->tablet_cumulative_max_compaction_score->value(), 8);
    ASSERT_EQ(metrics->tablet_time_series_max_compaction_score->value(), 0);
}

TEST_F(TabletMgrTest, GenerateCompactionTasksKeepsMissingPolicyScoreWithoutCheck) {
    auto tablet = create_compaction_tablet(52001, 8, CUMULATIVE_SIZE_BASED_POLICY);
    ASSERT_TRUE(tablet != nullptr);
    auto* metrics = DorisMetrics::instance();
    metrics->tablet_cumulative_max_compaction_score->set_value(101);
    metrics->tablet_time_series_max_compaction_score->set_value(200);

    std::vector<DataDir*> data_dirs {_data_dir};
    auto tasks = k_engine->generate_compaction_tasks_for_test(CompactionType::CUMULATIVE_COMPACTION,
                                                              data_dirs, false);

    ASSERT_EQ(tasks.size(), 1);
    ASSERT_EQ(tasks[0]->tablet_id(), 52001);
    ASSERT_EQ(metrics->tablet_cumulative_max_compaction_score->value(), 8);
    ASSERT_EQ(metrics->tablet_time_series_max_compaction_score->value(), 200);
}

TEST_F(TabletMgrTest, GenerateCompactionTasksDoesNotUpdateMetricWhenNoDirScanned) {
    auto* metrics = DorisMetrics::instance();
    metrics->tablet_cumulative_max_compaction_score->set_value(101);
    metrics->tablet_time_series_max_compaction_score->set_value(200);

    std::vector<DataDir*> data_dirs;
    auto tasks = k_engine->generate_compaction_tasks_for_test(CompactionType::CUMULATIVE_COMPACTION,
                                                              data_dirs, true);

    ASSERT_TRUE(tasks.empty());
    ASSERT_EQ(metrics->tablet_cumulative_max_compaction_score->value(), 101);
    ASSERT_EQ(metrics->tablet_time_series_max_compaction_score->value(), 200);
}

TEST_F(TabletMgrTest, GenerateCompactionTasksAggregatesScoreWhenNoSlot) {
    auto dummy = create_compaction_tablet(53000, 5, CUMULATIVE_SIZE_BASED_POLICY);
    auto size_based = create_compaction_tablet(53001, 8, CUMULATIVE_SIZE_BASED_POLICY);
    auto time_series = create_compaction_tablet(53002, 12, CUMULATIVE_TIME_SERIES_POLICY);
    ASSERT_TRUE(dummy != nullptr);
    ASSERT_TRUE(size_based != nullptr);
    ASSERT_TRUE(time_series != nullptr);

    std::vector<DataDir*> data_dirs {_data_dir};
    auto& registry = k_engine->compaction_submit_registry_for_test();
    registry.reset(data_dirs);
    Defer reset_registry([&] { registry.reset(data_dirs); });
    dummy->compaction_stage = CompactionStage::EXECUTING;
    ASSERT_FALSE(registry.insert(dummy, CompactionType::CUMULATIVE_COMPACTION));

    int32_t old_compaction_task_num_per_disk = config::compaction_task_num_per_disk;
    config::compaction_task_num_per_disk = 1;
    Defer restore_config(
            [&] { config::compaction_task_num_per_disk = old_compaction_task_num_per_disk; });
    bool old_enable_compaction_priority_scheduling = config::enable_compaction_priority_scheduling;
    config::enable_compaction_priority_scheduling = false;
    Defer restore_priority_scheduling([&] {
        config::enable_compaction_priority_scheduling = old_enable_compaction_priority_scheduling;
    });

    auto* metrics = DorisMetrics::instance();
    metrics->tablet_cumulative_max_compaction_score->set_value(0);
    metrics->tablet_time_series_max_compaction_score->set_value(0);

    auto tasks = k_engine->generate_compaction_tasks_for_test(CompactionType::CUMULATIVE_COMPACTION,
                                                              data_dirs, true);

    ASSERT_TRUE(tasks.empty());
    ASSERT_EQ(metrics->tablet_cumulative_max_compaction_score->value(), 8);
    ASSERT_EQ(metrics->tablet_time_series_max_compaction_score->value(), 12);
}

TEST_F(TabletMgrTest, GenerateCompactionTasksDoesNotLowerPolicyScoreWhenDirFull) {
    std::string full_dir_path = "./be/test/storage/test_data/converter_test_data/tmp_full";
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(full_dir_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(full_dir_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(full_dir_path + "/meta").ok());
    Defer cleanup_full_dir([&] {
        static_cast<void>(io::global_local_filesystem()->delete_directory(full_dir_path));
    });

    auto full_data_dir = std::make_unique<DataDir>(*k_engine, full_dir_path, 1000000000);
    ASSERT_TRUE(full_data_dir->init().ok());
    auto full_time_series =
            create_compaction_tablet(54001, 12, CUMULATIVE_TIME_SERIES_POLICY, full_data_dir.get());
    auto size_based = create_compaction_tablet(54002, 8, CUMULATIVE_SIZE_BASED_POLICY);
    ASSERT_TRUE(full_time_series != nullptr);
    ASSERT_TRUE(size_based != nullptr);
    Defer drop_full_tablet(
            [&] { static_cast<void>(_tablet_mgr->drop_tablet(54001, 540010, false)); });
    full_data_dir->set_capacity_for_test(100, 0);

    auto* metrics = DorisMetrics::instance();
    metrics->tablet_cumulative_max_compaction_score->set_value(0);
    metrics->tablet_time_series_max_compaction_score->set_value(200);

    std::vector<DataDir*> data_dirs {_data_dir, full_data_dir.get()};
    auto tasks = k_engine->generate_compaction_tasks_for_test(CompactionType::CUMULATIVE_COMPACTION,
                                                              data_dirs, true);

    ASSERT_EQ(tasks.size(), 1);
    ASSERT_EQ(tasks[0]->tablet_id(), 54002);
    ASSERT_EQ(metrics->tablet_cumulative_max_compaction_score->value(), 8);
    ASSERT_EQ(metrics->tablet_time_series_max_compaction_score->value(), 200);
}

TEST_F(TabletMgrTest, LoadTabletFromMeta) {
    TTabletId tablet_id = 111;
    TSchemaHash schema_hash = 3333;
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
    Status create_st =
            k_engine->tablet_manager()->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = k_engine->tablet_manager()->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    std::string serialized_tablet_meta;
    tablet->tablet_meta()->serialize(&serialized_tablet_meta);
    bool update_meta = true;
    bool force = true;
    bool restore = false;
    bool check_path = true;
    Status st = _tablet_mgr->load_tablet_from_meta(_data_dir, tablet_id, schema_hash,
                                                   serialized_tablet_meta, update_meta, force,
                                                   restore, check_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // After reload, the original tablet should not be allowed to save meta.
    ASSERT_FALSE(tablet->do_tablet_meta_checkpoint());
}

} // namespace doris
