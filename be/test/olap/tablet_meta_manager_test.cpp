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

#include "olap/tablet_meta_manager.h"

#include <gtest/gtest.h>
#include <json2pb/json_to_pb.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "olap/olap_define.h"
#include "util/file_utils.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using std::string;

namespace doris {

const std::string meta_path = "./be/test/olap/test_data/header_without_inc_rs.txt";

class TabletMetaManagerTest : public testing::Test {
public:
    virtual void SetUp() {
        std::string root_path = "./store";
        ASSERT_TRUE(std::filesystem::create_directory(root_path));
        _data_dir = new (std::nothrow) DataDir(root_path);
        ASSERT_NE(nullptr, _data_dir);
        Status st = _data_dir->init();
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(std::filesystem::exists(root_path + "/meta"));

        std::ifstream infile(meta_path);
        char buffer[1024];
        while (!infile.eof()) {
            infile.getline(buffer, 1024);
            _json_header = _json_header + buffer + "\n";
        }
        _json_header = _json_header.substr(0, _json_header.size() - 1);
        _json_header = _json_header.substr(0, _json_header.size() - 1);
    }

    virtual void TearDown() {
        delete _data_dir;
        ASSERT_TRUE(std::filesystem::remove_all("./store"));
    }

private:
    DataDir* _data_dir;
    std::string _json_header;
};

TEST_F(TabletMetaManagerTest, TestSaveAndGetAndRemove) {
    const TTabletId tablet_id = 15672;
    const TSchemaHash schema_hash = 567997577;
    TabletMetaPB tablet_meta_pb;
    bool ret = json2pb::JsonToProtoMessage(_json_header, &tablet_meta_pb);
    ASSERT_TRUE(ret);

    std::string meta_binary;
    tablet_meta_pb.SerializeToString(&meta_binary);
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    OLAPStatus s = tablet_meta->deserialize(meta_binary);
    ASSERT_EQ(OLAP_SUCCESS, s);

    s = TabletMetaManager::save(_data_dir, tablet_id, schema_hash, tablet_meta);
    ASSERT_EQ(OLAP_SUCCESS, s);
    std::string json_meta_read;
    s = TabletMetaManager::get_json_meta(_data_dir, tablet_id, schema_hash, &json_meta_read);
    ASSERT_EQ(OLAP_SUCCESS, s);
    ASSERT_EQ(_json_header, json_meta_read);
    s = TabletMetaManager::remove(_data_dir, tablet_id, schema_hash);
    ASSERT_EQ(OLAP_SUCCESS, s);
    TabletMetaSharedPtr meta_read(new TabletMeta());
    s = TabletMetaManager::get_meta(_data_dir, tablet_id, schema_hash, meta_read);
    ASSERT_EQ(OLAP_ERR_META_KEY_NOT_FOUND, s);
}

TEST_F(TabletMetaManagerTest, TestLoad) {
    const TTabletId tablet_id = 15672;
    const TSchemaHash schema_hash = 567997577;
    OLAPStatus s = TabletMetaManager::load_json_meta(_data_dir, meta_path);
    ASSERT_EQ(OLAP_SUCCESS, s);
    std::string json_meta_read;
    s = TabletMetaManager::get_json_meta(_data_dir, tablet_id, schema_hash, &json_meta_read);
    ASSERT_EQ(OLAP_SUCCESS, s);
    ASSERT_EQ(_json_header, json_meta_read);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
