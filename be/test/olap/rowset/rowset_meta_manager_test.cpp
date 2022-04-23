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

#include "olap/rowset/rowset_meta_manager.h"

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "json2pb/json_to_pb.h"
#include "olap/olap_meta.h"
#include "olap/storage_engine.h"

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
        LOG(INFO) << "SetUp";
        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        EngineOptions options;
        // won't open engine, options.path is needless
        options.backend_uid = UniqueId::gen_uid();
        if (k_engine == nullptr) {
            k_engine = new StorageEngine(options);
        }

        std::string meta_path = "./meta";
        EXPECT_TRUE(std::filesystem::create_directory(meta_path));
        _meta = new (std::nothrow) OlapMeta(meta_path);
        EXPECT_NE(nullptr, _meta);
        Status st = _meta->init();
        EXPECT_TRUE(st == Status::OK());
        EXPECT_TRUE(std::filesystem::exists("./meta"));

        std::ifstream infile(rowset_meta_path);
        char buffer[1024];
        while (!infile.eof()) {
            infile.getline(buffer, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer + "\n";
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        boost::replace_all(_json_rowset_meta, "\r", "");
        _tablet_uid = TabletUid(10, 10);
    }

    virtual void TearDown() {
        SAFE_DELETE(_meta);
        SAFE_DELETE(k_engine);
        EXPECT_TRUE(std::filesystem::remove_all("./meta"));
        LOG(INFO) << "TearDown";
    }
    StorageEngine* k_engine = nullptr;

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;
    TabletUid _tablet_uid {0, 0};
};

TEST_F(RowsetMetaManagerTest, TestSaveAndGetAndRemove) {
    RowsetId rowset_id;
    rowset_id.init(10000);
    RowsetMeta rowset_meta;
    rowset_meta.init_from_json(_json_rowset_meta);
    EXPECT_EQ(rowset_meta.rowset_id(), rowset_id);
    RowsetMetaPB rowset_meta_pb;
    rowset_meta.to_rowset_pb(&rowset_meta_pb);
    Status status = RowsetMetaManager::save(_meta, _tablet_uid, rowset_id, rowset_meta_pb);
    EXPECT_TRUE(status == Status::OK());
    EXPECT_TRUE(RowsetMetaManager::check_rowset_meta(_meta, _tablet_uid, rowset_id));
    std::string json_rowset_meta_read;
    status = RowsetMetaManager::get_json_rowset_meta(_meta, _tablet_uid, rowset_id,
                                                     &json_rowset_meta_read);
    EXPECT_TRUE(status == Status::OK());
    EXPECT_EQ(_json_rowset_meta, json_rowset_meta_read);
    status = RowsetMetaManager::remove(_meta, _tablet_uid, rowset_id);
    EXPECT_TRUE(status == Status::OK());
    EXPECT_FALSE(RowsetMetaManager::check_rowset_meta(_meta, _tablet_uid, rowset_id));
    RowsetMetaSharedPtr rowset_meta_read(new RowsetMeta());
    status = RowsetMetaManager::get_rowset_meta(_meta, _tablet_uid, rowset_id, rowset_meta_read);
    EXPECT_TRUE(status != Status::OK());
}

TEST_F(RowsetMetaManagerTest, TestLoad) {
    RowsetId rowset_id;
    rowset_id.init(10000);
    Status status = RowsetMetaManager::load_json_rowset_meta(_meta, rowset_meta_path);
    EXPECT_TRUE(status == Status::OK());
    EXPECT_TRUE(RowsetMetaManager::check_rowset_meta(_meta, _tablet_uid, rowset_id));
    std::string json_rowset_meta_read;
    status = RowsetMetaManager::get_json_rowset_meta(_meta, _tablet_uid, rowset_id,
                                                     &json_rowset_meta_read);
    EXPECT_TRUE(status == Status::OK());
    EXPECT_EQ(_json_rowset_meta, json_rowset_meta_read);
}

} // namespace doris
