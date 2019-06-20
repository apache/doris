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

#include <string>
#include <sstream>
#include <fstream>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "olap/olap_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/txn_manager.h"
#include "olap/new_status.h"
#include "boost/filesystem.hpp"
#include "json2pb/json_to_pb.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class TabletMgrTest : public testing::Test {
public:
    virtual void SetUp() {
        auto cache = new_lru_cache(config::file_descriptor_cache_capacity);
        FileHandler::set_fd_cache(cache);
        string test_engine_data_path = "./be/test/olap/test_data/converter_test_data/data";
        _engine_data_path = "./be/test/olap/test_data/converter_test_data/tmp";
        boost::filesystem::remove_all(_engine_data_path);
        create_dirs(_engine_data_path);
        _data_dir = new DataDir(_engine_data_path, 1000000000);
        _data_dir->init();
        _meta_path = "./meta";
        string tmp_data_path = _engine_data_path + "/data"; 
        if (boost::filesystem::exists(tmp_data_path)) {
            boost::filesystem::remove_all(tmp_data_path);
        }
        copy_dir(test_engine_data_path, tmp_data_path);
        _tablet_id = 15007;
        _schema_hash = 368169781;
        _tablet_data_path = tmp_data_path 
                + "/" + std::to_string(0)
                + "/" + std::to_string(_tablet_id)
                + "/" + std::to_string(_schema_hash);
        if (boost::filesystem::exists(_meta_path)) {
            boost::filesystem::remove_all(_meta_path);
        }
        ASSERT_TRUE(boost::filesystem::create_directory(_meta_path));
        ASSERT_TRUE(boost::filesystem::exists(_meta_path));
        _meta = new(std::nothrow) OlapMeta(_meta_path);
        ASSERT_NE(nullptr, _meta);
        OLAPStatus st = _meta->init();
        ASSERT_TRUE(st == OLAP_SUCCESS);
    }

    virtual void TearDown() {
        delete _meta;
        delete _data_dir;
        if (boost::filesystem::exists(_meta_path)) {
            ASSERT_TRUE(boost::filesystem::remove_all(_meta_path));
        }
        if (boost::filesystem::exists(_engine_data_path)) {
            ASSERT_TRUE(boost::filesystem::remove_all(_engine_data_path));
        }
        _tablet_mgr.clear();
    }

private:
    DataDir* _data_dir;
    OlapMeta* _meta;
    std::string _json_rowset_meta;
    TxnManager _txn_mgr;
    std::string _engine_data_path;
    std::string _meta_path;
    int64_t _tablet_id;
    int32_t _schema_hash;
    string _tablet_data_path;
    TabletManager _tablet_mgr;
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
    create_tablet_req.__set_version_hash(3333);
    vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    OLAPStatus create_st = _tablet_mgr.create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st == OLAP_SUCCESS);
    TabletSharedPtr tablet = _tablet_mgr.get_tablet(111, 3333);
    ASSERT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = check_dir_existed(tablet->tablet_path());
    ASSERT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    OLAPStatus check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    ASSERT_TRUE(check_meta_st == OLAP_SUCCESS);

    // retry create should be successfully
    create_st = _tablet_mgr.create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st == OLAP_SUCCESS);

    // create tablet with different schema hash should be error
    tablet_schema.__set_schema_hash(4444);
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_st = _tablet_mgr.create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st == OLAP_ERR_CE_TABLET_ID_EXIST);
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
    create_tablet_req.__set_version_hash(3333);
    vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    OLAPStatus create_st = _tablet_mgr.create_tablet(create_tablet_req, data_dirs);
    ASSERT_TRUE(create_st == OLAP_SUCCESS);
    TabletSharedPtr tablet = _tablet_mgr.get_tablet(111, 3333);
    ASSERT_TRUE(tablet != nullptr);

    // drop unexist tablet will be success
    OLAPStatus drop_st = _tablet_mgr.drop_tablet(111, 4444, false);
    ASSERT_TRUE(drop_st == OLAP_SUCCESS);
    tablet = _tablet_mgr.get_tablet(111, 3333);
    ASSERT_TRUE(tablet != nullptr);

    // drop exist tablet will be success
    drop_st = _tablet_mgr.drop_tablet(111, 3333, false);
    ASSERT_TRUE(drop_st == OLAP_SUCCESS);
    tablet = _tablet_mgr.get_tablet(111, 3333);
    ASSERT_TRUE(tablet == nullptr);
    tablet = _tablet_mgr.get_tablet(111, 3333, true);
    ASSERT_TRUE(tablet != nullptr);

    // check dir exist
    std::string tablet_path = tablet->tablet_path();
    bool dir_exist = check_dir_existed(tablet_path);
    ASSERT_TRUE(dir_exist);

    // do trash sweep, tablet will not be garbage collected
    // because tablet ptr referenced it
    OLAPStatus trash_st = _tablet_mgr.start_trash_sweep();
    ASSERT_TRUE(trash_st == OLAP_SUCCESS);
    tablet = _tablet_mgr.get_tablet(111, 3333, true);
    ASSERT_TRUE(tablet != nullptr);
    dir_exist = check_dir_existed(tablet_path);
    ASSERT_TRUE(dir_exist);

    // reset tablet ptr
    tablet.reset();
    trash_st = _tablet_mgr.start_trash_sweep();
    ASSERT_TRUE(trash_st == OLAP_SUCCESS);
    tablet = _tablet_mgr.get_tablet(111, 3333, true);
    ASSERT_TRUE(tablet == nullptr);
    dir_exist = check_dir_existed(tablet_path);
    ASSERT_TRUE(!dir_exist);
}

}  // namespace doris

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
