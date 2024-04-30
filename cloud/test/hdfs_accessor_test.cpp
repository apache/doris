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

#include "recycler/hdfs_accessor.h"

#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <iostream>

#include "common/config.h"
#include "common/logging.h"

using namespace doris;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!cloud::init_glog("hdfs_accessor_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

#define HdfsAccessorTest DISABLED_HdfsAccessorTest
TEST(HdfsAccessorTest, normal) {
    HdfsVaultInfo info;
    *info.mutable_prefix() = config::test_hdfs_prefix;
    auto* conf = info.mutable_build_conf();
    *conf->mutable_fs_name() = config::test_hdfs_fs_name;

    HdfsAccessor accessor(info);
    int ret = accessor.init();
    ASSERT_EQ(ret, 0);

    ret = accessor.exist("file_1");
    ASSERT_EQ(ret, 1);

    ret = accessor.delete_object("file_1");
    // Delete not exist path should return success
    ASSERT_EQ(ret, 0);

    ret = accessor.delete_objects_by_prefix("dir_1/");
    // Delete not exist path should return success
    ASSERT_EQ(ret, 0);

    ret = accessor.put_object("file_2", "abc");
    ASSERT_EQ(ret, 0);

    ret = accessor.exist("file_2");
    ASSERT_EQ(ret, 0);

    ret = accessor.put_object("file_3", "abc");
    ASSERT_EQ(ret, 0);

    ret = accessor.exist("file_3");
    ASSERT_EQ(ret, 0);

    // HDFS will create parent directories if not exist
    ret = accessor.put_object("dir_1/file_4", "abc");
    ASSERT_EQ(ret, 0);

    ret = accessor.exist("dir_1/file_4");
    ASSERT_EQ(ret, 0);

    std::vector<ObjectMeta> files;
    ret = accessor.list("", &files);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(files.size(), 3);
    std::sort(files.begin(), files.end(), [](auto&& f1, auto&& f2) { return f1.path < f2.path; });
    EXPECT_EQ(files[0].path, "dir_1");
    EXPECT_EQ(files[1].path, "file_2");
    EXPECT_EQ(files[2].path, "file_3");
    for (auto& file : files) {
        std::cout << "file: " << file.path << ' ' << file.size << std::endl;
    }

    files.clear();
    ret = accessor.list("dir_1", &files);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(files.size(), 1);
    EXPECT_EQ(files[0].path, "dir_1/file_4");

    ret = accessor.delete_object("file_2");
    ASSERT_EQ(ret, 0);

    ret = accessor.exist("file_2");
    ASSERT_EQ(ret, 1);

    files.clear();
    ret = accessor.list("", &files);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(files.size(), 2);
    EXPECT_EQ(files[0].path, "dir_1");
    EXPECT_EQ(files[1].path, "file_3");
    for (auto& file : files) {
        std::cout << "file: " << file.path << ' ' << file.size << std::endl;
    }

    ret = accessor.delete_objects_by_prefix("dir_1/");
    ASSERT_EQ(ret, 0);
    ret = accessor.exist("dir_1");
    ASSERT_EQ(ret, 1);
    ret = accessor.exist("dir_1/file_4");
    ASSERT_EQ(ret, 1);

    // Check existence of the root dir of the accessor
    ret = accessor.exist("");
    ASSERT_EQ(ret, 0);

    // Delete the root dir of the accessor
    ret = accessor.delete_objects_by_prefix("");
    ASSERT_EQ(ret, 0);
    ret = accessor.list("", &files);
    ASSERT_NE(ret, 0);
    ret = accessor.exist("");
    ASSERT_EQ(ret, 1);
}

} // namespace doris::cloud
