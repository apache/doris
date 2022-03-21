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

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "json2pb/json_to_pb.h"
#include "olap/olap_meta.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta_manager.h"
#include "olap/txn_manager.h"
#include "util/file_utils.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

static StorageEngine* k_engine = nullptr;

class TabletMgrTest : public testing::Test {
public:
    virtual void SetUp() {
        _engine_data_path = "./be/test/olap/test_data/converter_test_data/tmp";
        std::filesystem::remove_all(_engine_data_path);
        FileUtils::create_dir(_engine_data_path);
        FileUtils::create_dir(_engine_data_path + "/meta");

        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        EngineOptions options;
        // won't open engine, options.path is needless
        options.backend_uid = UniqueId::gen_uid();
        if (k_engine == nullptr) {
            k_engine = new StorageEngine(options);
        }

        _data_dir = new DataDir(_engine_data_path, 1000000000);
        _data_dir->init();
        _tablet_mgr = k_engine->tablet_manager();
    }

    virtual void TearDown() {
        delete _data_dir;
        if (std::filesystem::exists(_engine_data_path)) {
            ASSERT_TRUE(std::filesystem::remove_all(_engine_data_path));
        }
    }

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
    OLAPStatus create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st == OLAP_SUCCESS);
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111, 3333);
    ASSERT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = FileUtils::check_exist(tablet->tablet_path_desc().filepath);
    ASSERT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    OLAPStatus check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    ASSERT_TRUE(check_meta_st == OLAP_SUCCESS);

    // retry create should be successfully
    create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st == OLAP_SUCCESS);

    OLAPStatus drop_st = _tablet_mgr->drop_tablet(111, 3333, false);
    ASSERT_TRUE(drop_st == OLAP_SUCCESS);
    tablet.reset();
    OLAPStatus trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st == OLAP_SUCCESS);
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
    OLAPStatus create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st == OLAP_SUCCESS);

    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111, 3333);
    ASSERT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = FileUtils::check_exist(tablet->tablet_path_desc().filepath);
    ASSERT_TRUE(dir_exist) << tablet->tablet_path_desc().filepath;
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    OLAPStatus check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    ASSERT_TRUE(check_meta_st == OLAP_SUCCESS);

    OLAPStatus drop_st = _tablet_mgr->drop_tablet(111, 3333, false);
    ASSERT_TRUE(drop_st == OLAP_SUCCESS);
    tablet.reset();
    OLAPStatus trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st == OLAP_SUCCESS);
}

TEST_F(TabletMgrTest, DropTablet) {
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
    OLAPStatus create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st == OLAP_SUCCESS);
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111, 3333);
    ASSERT_TRUE(tablet != nullptr);

    // drop unexist tablet will be success
    OLAPStatus drop_st = _tablet_mgr->drop_tablet(1121, 4444, false);
    ASSERT_TRUE(drop_st == OLAP_SUCCESS);
    tablet = _tablet_mgr->get_tablet(111, 3333);
    ASSERT_TRUE(tablet != nullptr);

    // drop exist tablet will be success
    drop_st = _tablet_mgr->drop_tablet(111, 3333, false);
    ASSERT_TRUE(drop_st == OLAP_SUCCESS);
    tablet = _tablet_mgr->get_tablet(111, 3333);
    ASSERT_TRUE(tablet == nullptr);
    tablet = _tablet_mgr->get_tablet(111, 3333, true);
    ASSERT_TRUE(tablet != nullptr);

    // check dir exist
    std::string tablet_path = tablet->tablet_path_desc().filepath;
    bool dir_exist = FileUtils::check_exist(tablet_path);
    ASSERT_TRUE(dir_exist);

    // do trash sweep, tablet will not be garbage collected
    // because tablet ptr referenced it
    OLAPStatus trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st == OLAP_SUCCESS);
    tablet = _tablet_mgr->get_tablet(111, 3333, true);
    ASSERT_TRUE(tablet != nullptr);
    dir_exist = FileUtils::check_exist(tablet_path);
    ASSERT_TRUE(dir_exist);

    // reset tablet ptr
    tablet.reset();
    trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st == OLAP_SUCCESS);
    tablet = _tablet_mgr->get_tablet(111, 3333, true);
    ASSERT_TRUE(tablet == nullptr);
    dir_exist = FileUtils::check_exist(tablet_path);
    ASSERT_TRUE(!dir_exist);
}

TEST_F(TabletMgrTest, GetRowsetId) {
    // normal case
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(368169781, schema_hash);
    }
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781/";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(368169781, schema_hash);
    }
    // normal case
    {
        std::string path =
                _engine_data_path +
                "/data/0/15007/368169781/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(368169781, schema_hash);

        RowsetId id;
        ASSERT_TRUE(_tablet_mgr->get_rowset_id_from_path(path, &id));
        EXPECT_EQ(2UL << 56 | 1, id.hi);
        ASSERT_EQ(2, id.mi);
        ASSERT_EQ(3, id.lo);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(0, schema_hash);

        RowsetId id;
        ASSERT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007/";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        ASSERT_EQ(15007, tid);
        ASSERT_EQ(0, schema_hash);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007abc";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_FALSE(
                _tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
    }
    // not match pattern
    {
        std::string path =
                _engine_data_path +
                "/data/0/15007/123abc/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        ASSERT_FALSE(
                _tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));

        RowsetId id;
        ASSERT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
