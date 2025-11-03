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

#include "olap/olap_meta.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include <filesystem>
#include <memory>
#include <sstream>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/olap_define.h"

using std::string;

namespace doris {
using namespace ErrorCode;

class OlapMetaTest : public testing::Test {
public:
    virtual void SetUp() {
        _root_path = "./ut_dir/olap_meta_test";
        auto st = io::global_local_filesystem()->delete_directory(_root_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_root_path);
        ASSERT_TRUE(st.ok()) << st;
        _meta = new OlapMeta(_root_path);
        Status s = _meta->init();
        EXPECT_EQ(Status::OK(), s);
        EXPECT_TRUE(std::filesystem::exists(_root_path + "/meta"));
    }

    virtual void TearDown() {
        delete _meta;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_root_path).ok());
    }

private:
    std::string _root_path;
    OlapMeta* _meta;
};

TEST_F(OlapMetaTest, TestGetRootPath) {
    std::string root_path = _meta->get_root_path();
    EXPECT_EQ("./ut_dir/olap_meta_test", root_path);
}

TEST_F(OlapMetaTest, TestPutAndGet) {
    // normal cases
    std::string key = "key";
    std::string value = "value";
    Status s = _meta->put(META_COLUMN_FAMILY_INDEX, key, value);
    EXPECT_EQ(Status::OK(), s);
    std::string value_get;
    s = _meta->get(META_COLUMN_FAMILY_INDEX, key, &value_get);
    EXPECT_EQ(Status::OK(), s);
    EXPECT_EQ(value, value_get);

    // abnormal cases
    s = _meta->get(META_COLUMN_FAMILY_INDEX, "key_not_exist", &value_get);
    EXPECT_EQ(Status::Error<META_KEY_NOT_FOUND>(""), s);
}

TEST_F(OlapMetaTest, TestRemove) {
    // normal cases
    std::string key = "key";
    std::string value = "value";
    Status s = _meta->put(META_COLUMN_FAMILY_INDEX, key, value);
    EXPECT_EQ(Status::OK(), s);
    std::string value_get;
    s = _meta->get(META_COLUMN_FAMILY_INDEX, key, &value_get);
    EXPECT_EQ(Status::OK(), s);
    EXPECT_EQ(value, value_get);
    s = _meta->remove(META_COLUMN_FAMILY_INDEX, key);
    EXPECT_EQ(Status::OK(), s);
    s = _meta->remove(META_COLUMN_FAMILY_INDEX, "key_not_exist");
    EXPECT_EQ(Status::OK(), s);
}

TEST_F(OlapMetaTest, TestIterate) {
    // normal cases
    std::string key = "hdr_key";
    std::string value = "value";
    Status s = Status::OK();
    for (int i = 0; i < 10; i++) {
        std::stringstream ss;
        ss << key << "_" << i;
        s = _meta->put(META_COLUMN_FAMILY_INDEX, ss.str(), value);
        EXPECT_EQ(Status::OK(), s);
    }
    bool error_flag = false;
    s = _meta->iterate(META_COLUMN_FAMILY_INDEX, "hdr_",
                       [&error_flag](std::string_view key, std::string_view value) -> bool {
                           size_t pos = key.find_first_of("hdr_");
                           if (pos != 0) {
                               error_flag = true;
                           }
                           return true;
                       });
    EXPECT_EQ(false, error_flag);
    EXPECT_EQ(Status::OK(), s);
}

} // namespace doris
