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
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/options.h"
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
        EXPECT_TRUE(
                io::global_local_filesystem()->delete_and_create_directory(_engine_data_path).ok());
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
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        SAFE_DELETE(k_engine);
        _tablet_mgr = nullptr;
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

} // namespace doris
