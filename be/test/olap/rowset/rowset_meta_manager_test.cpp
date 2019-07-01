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

const std::string rowset_meta_path = "./be/test/olap/test_data/rowset_meta.json";

class RowsetMetaManagerTest : public testing::Test {
public:
    virtual void SetUp() {
        std::string meta_path = "./meta";
        ASSERT_TRUE(boost::filesystem::create_directory(meta_path));
        _meta = new(std::nothrow) OlapMeta(meta_path);
        ASSERT_NE(nullptr, _meta);
        OLAPStatus st = _meta->init();
        ASSERT_TRUE(st == OLAP_SUCCESS);
        ASSERT_TRUE(boost::filesystem::exists("./meta"));

        std::ifstream infile(rowset_meta_path);
        char buffer[1024];
        while (!infile.eof()) {
            infile.getline(buffer, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer + "\n";
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        _tablet_uid = TabletUid(10, 10);
    }

    virtual void TearDown() {
        delete _meta;
        ASSERT_TRUE(boost::filesystem::remove_all("./meta"));
    }

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;
    TabletUid _tablet_uid;
};

TEST_F(RowsetMetaManagerTest, TestSaveAndGetAndRemove) {
    uint64_t rowset_id = 10000;
    RowsetMeta rowset_meta;
    rowset_meta.init_from_json(_json_rowset_meta);
    ASSERT_EQ(rowset_meta.rowset_id(), rowset_id);
    OLAPStatus status = RowsetMetaManager::save(_meta, _tablet_uid, rowset_id, &rowset_meta);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    ASSERT_TRUE(RowsetMetaManager::check_rowset_meta(_meta, _tablet_uid, rowset_id));
    std::string json_rowset_meta_read;
    status = RowsetMetaManager::get_json_rowset_meta(_meta, _tablet_uid, rowset_id, &json_rowset_meta_read);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    ASSERT_EQ(_json_rowset_meta, json_rowset_meta_read);
    status = RowsetMetaManager::remove(_meta, _tablet_uid, rowset_id);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    ASSERT_FALSE(RowsetMetaManager::check_rowset_meta(_meta, _tablet_uid, rowset_id));
    RowsetMetaSharedPtr rowset_meta_read(new RowsetMeta());
    status = RowsetMetaManager::get_rowset_meta(_meta, _tablet_uid, rowset_id, rowset_meta_read);
    ASSERT_TRUE(status != OLAP_SUCCESS);
}

TEST_F(RowsetMetaManagerTest, TestLoad) {
    uint64_t rowset_id = 10000;
    OLAPStatus status = RowsetMetaManager::load_json_rowset_meta(_meta, rowset_meta_path);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    ASSERT_TRUE(RowsetMetaManager::check_rowset_meta(_meta, _tablet_uid, rowset_id));
    std::string json_rowset_meta_read;
    status = RowsetMetaManager::get_json_rowset_meta(_meta, _tablet_uid, rowset_id, &json_rowset_meta_read);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    ASSERT_EQ(_json_rowset_meta, json_rowset_meta_read);
}

}  // namespace doris

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
