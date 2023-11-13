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

#include "olap/rowset/rowset_meta.h"

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <filesystem>
#include <fstream>
#include <new>
#include <string>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/olap_common.h"
#include "olap/olap_meta.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

const std::string rowset_meta_path = "./be/test/olap/test_data/rowset.json";

class RowsetMetaTest : public testing::Test {
public:
    virtual void SetUp() {
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
    }

    virtual void TearDown() {
        delete _meta;
        EXPECT_TRUE(std::filesystem::remove_all("./meta"));
    }

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;
};

void do_check(RowsetMeta rowset_meta) {
    RowsetId rowset_id;
    rowset_id.init(540081);
    EXPECT_EQ(rowset_id, rowset_meta.rowset_id());
    EXPECT_EQ(15673, rowset_meta.tablet_id());
    EXPECT_EQ(4042, rowset_meta.txn_id());
    EXPECT_EQ(567997577, rowset_meta.tablet_schema_hash());
    EXPECT_EQ(BETA_ROWSET, rowset_meta.rowset_type());
    EXPECT_EQ(VISIBLE, rowset_meta.rowset_state());
    EXPECT_EQ(2, rowset_meta.start_version());
    EXPECT_EQ(2, rowset_meta.end_version());
    EXPECT_EQ(3929, rowset_meta.num_rows());
    EXPECT_EQ(84699, rowset_meta.total_disk_size());
    EXPECT_EQ(84464, rowset_meta.data_disk_size());
    EXPECT_EQ(235, rowset_meta.index_disk_size());
    EXPECT_EQ(false, rowset_meta.empty());
    EXPECT_EQ(1553765670, rowset_meta.creation_time());
}

TEST_F(RowsetMetaTest, TestInit) {
    RowsetMeta rowset_meta;
    EXPECT_TRUE(rowset_meta.init_from_json(_json_rowset_meta));
    do_check(rowset_meta);
    RowsetMetaPB rowset_meta_pb;
    rowset_meta.to_rowset_pb(&rowset_meta_pb);
    RowsetMeta rowset_meta_2;
    rowset_meta_2.init_from_pb(rowset_meta_pb);
    do_check(rowset_meta_2);
    std::string value = "";
    rowset_meta_pb.SerializeToString(&value);
    RowsetMeta rowset_meta_3;
    rowset_meta_3.init(value);
    do_check(rowset_meta_3);
}

TEST_F(RowsetMetaTest, TestInitWithInvalidData) {
    RowsetMeta rowset_meta;
    EXPECT_FALSE(rowset_meta.init_from_json("invalid json meta data"));
    EXPECT_FALSE(rowset_meta.init("invalid pb meta data"));
}

} // namespace doris
