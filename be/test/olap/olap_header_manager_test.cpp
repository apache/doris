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

#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <json2pb/json_to_pb.h>

#include "olap/store.h"
#include "olap/olap_header_manager.h"
#include "olap/olap_define.h"
#include "util/file_utils.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using std::string;

namespace doris {

const std::string header_path = "./be/test/olap/test_data/header.txt";

class OlapHeaderManagerTest : public testing::Test {
public:
    virtual void SetUp() {
        _root_path = "./ut_dir/olap_header_mgr_test";
        FileUtils::remove_all(_root_path);
        FileUtils::create_dir(_root_path);
        _store = new(std::nothrow) OlapStore(_root_path);
        ASSERT_NE(nullptr, _store);
        Status st = _store->load();
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(boost::filesystem::exists(_root_path + "/meta"));

        std::ifstream infile(header_path);
        char buffer[1024];
        while (!infile.eof()) {
            infile.getline(buffer, 1024);
            _json_header = _json_header + buffer + "\n";
        }
        _json_header = _json_header.substr(0, _json_header.size() - 1);
        _json_header = _json_header.substr(0, _json_header.size() - 1);
        std::cout << "set up finish" << std::endl;
    }

    virtual void TearDown() {
        delete _store;
        ASSERT_TRUE(boost::filesystem::remove_all(_root_path));
    }

private:
    std::string _root_path;
    OlapStore* _store;
    std::string _json_header;
};

TEST_F(OlapHeaderManagerTest, TestConvertedFlag) {
    bool converted_flag;
    OLAPStatus s = OlapHeaderManager::get_header_converted(_store, converted_flag);
    ASSERT_EQ(false, converted_flag);
    s = OlapHeaderManager::set_converted_flag(_store);
    ASSERT_EQ(OLAP_SUCCESS, s);
    s = OlapHeaderManager::get_header_converted(_store, converted_flag);
    ASSERT_EQ(true, converted_flag);
}

TEST_F(OlapHeaderManagerTest, TestSaveAndGetAndRemove) {
    const TTabletId tablet_id = 20487;
    const TSchemaHash schema_hash = 1520686811;
    OLAPHeader header;
    bool ret = json2pb::JsonToProtoMessage(_json_header, &header);
    ASSERT_TRUE(ret);
    OLAPStatus s = OlapHeaderManager::save(_store, tablet_id, schema_hash, &header);
    ASSERT_EQ(OLAP_SUCCESS, s);
    std::string json_header_read;
    s = OlapHeaderManager::get_json_header(_store, tablet_id, schema_hash, &json_header_read);
    ASSERT_EQ(OLAP_SUCCESS, s);
    ASSERT_EQ(_json_header, json_header_read);
    s = OlapHeaderManager::remove(_store, tablet_id, schema_hash);
    ASSERT_EQ(OLAP_SUCCESS, s);
    OLAPHeader header_read;
    s = OlapHeaderManager::get_header(_store, tablet_id, schema_hash, &header_read);
    ASSERT_EQ(OLAP_ERR_META_KEY_NOT_FOUND, s);
}

TEST_F(OlapHeaderManagerTest, TestLoad) {
    const TTabletId tablet_id = 20487;
    const TSchemaHash schema_hash = 1520686811;
    OLAPStatus s = OlapHeaderManager::load_json_header(_store, header_path);
    ASSERT_EQ(OLAP_SUCCESS, s);
    std::string json_header_read;
    s = OlapHeaderManager::get_json_header(_store, tablet_id, schema_hash, &json_header_read);
    ASSERT_EQ(OLAP_SUCCESS, s);
    ASSERT_EQ(_json_header, json_header_read);
}

}  // namespace doris

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
