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

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <boost/algorithm/string/replace.hpp>
#include <filesystem>
#include <fstream>
#include <new>
#include <string>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/uid_util.h"

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
        EXPECT_TRUE(std::filesystem::remove_all("./meta"));
        LOG(INFO) << "TearDown";
    }

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;
    TabletUid _tablet_uid {0, 0};
};

} // namespace doris
