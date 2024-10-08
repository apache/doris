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

#include <butil/guid.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <iostream>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"

using namespace doris;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (cloud::config::test_hdfs_fs_name.empty()) {
        std::cout << "empty test_hdfs_fs_name, skip HdfsAccessorTest" << std::endl;
        return 0;
    }

    if (!cloud::init_glog("hdfs_accessor_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

TEST(HdfsAccessorTest, normal) {
    HdfsVaultInfo info;
    info.set_prefix(config::test_hdfs_prefix + "/HdfsAccessorTest/" + butil::GenerateGUID());
    auto* conf = info.mutable_build_conf();
    conf->set_fs_name(config::test_hdfs_fs_name);

    HdfsAccessor accessor(info);
    int ret = accessor.init();
    ASSERT_EQ(ret, 0);

    std::string file1 = "data/10000/1_0.dat";

    ret = accessor.delete_directory("");
    ASSERT_NE(ret, 0);
    ret = accessor.delete_all();
    ASSERT_EQ(ret, 0);

    ret = accessor.put_file(file1, "");
    ASSERT_EQ(ret, 0);

    ret = accessor.exists(file1);
    ASSERT_EQ(ret, 0);

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();

    size_t alloc_entries = 0;
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "DirEntries",
            [&alloc_entries](auto&& args) { alloc_entries += *try_any_cast<size_t*>(args[0]); },
            &guards.emplace_back());
    sp->set_call_back(
            "~DirEntries",
            [&alloc_entries](auto&& args) { alloc_entries -= *try_any_cast<size_t*>(args[0]); },
            &guards.emplace_back());

    std::unique_ptr<ListIterator> iter;
    ret = accessor.list_directory("data", &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(iter);
    ASSERT_TRUE(iter->is_valid());
    ASSERT_TRUE(iter->has_next());
    ASSERT_EQ(iter->next()->path, file1);
    ASSERT_FALSE(iter->has_next());
    iter.reset();
    ASSERT_EQ(alloc_entries, 0);

    ret = accessor.list_directory("data/", &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(iter->is_valid());
    ASSERT_TRUE(iter->has_next());
    ASSERT_EQ(iter->next()->path, file1);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());
    iter.reset();
    ASSERT_EQ(alloc_entries, 0);

    ret = accessor.list_directory("data/100", &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());
    iter.reset();
    ASSERT_EQ(alloc_entries, 0);

    ret = accessor.delete_file(file1);
    ASSERT_EQ(ret, 0);
    ret = accessor.exists(file1);
    ASSERT_EQ(ret, 1);
    ret = accessor.list_directory("", &iter);
    ASSERT_NE(ret, 0);
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());
    iter.reset();
    ASSERT_EQ(alloc_entries, 0);
    ret = accessor.delete_file(file1);
    ASSERT_EQ(ret, 0);

    std::vector<std::string> files;
    for (int dir = 10000; dir < 10005; ++dir) {
        for (int suffix = 0; suffix < 5; ++suffix) {
            files.push_back(fmt::format("data/{}/1/{}.dat", dir, suffix));
        }
    }

    for (auto&& file : files) {
        ret = accessor.put_file(file, "");
        ASSERT_EQ(ret, 0);
    }

    std::unordered_set<std::string> list_files;
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    for (auto file = iter->next(); file.has_value(); file = iter->next()) {
        list_files.insert(std::move(file->path));
    }
    iter.reset();
    ASSERT_EQ(alloc_entries, 0);
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    std::vector<std::string> to_delete_files;
    to_delete_files.reserve(5);
    for (int i = 0; i < 5; ++i) {
        to_delete_files.push_back(std::move(files.back()));
        files.pop_back();
    }
    ret = accessor.delete_files(to_delete_files);
    ASSERT_EQ(ret, 0);

    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    list_files.clear();
    for (auto file = iter->next(); file.has_value(); file = iter->next()) {
        list_files.insert(std::move(file->path));
    }
    iter.reset();
    ASSERT_EQ(alloc_entries, 0);
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    std::string to_delete_dir = "data/10001";
    ret = accessor.delete_directory(to_delete_dir);
    ASSERT_EQ(ret, 0);
    ret = accessor.list_directory(to_delete_dir, &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());

    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](auto&& file) { return file.starts_with(to_delete_dir); }),
                files.end());
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    list_files.clear();
    for (auto file = iter->next(); file.has_value(); file = iter->next()) {
        list_files.insert(std::move(file->path));
    }
    iter.reset();
    ASSERT_EQ(alloc_entries, 0);
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    std::string to_delete_prefix = "data/10003/";
    ret = accessor.delete_directory(to_delete_prefix);
    ASSERT_EQ(ret, 0);
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](auto&& file) { return file.starts_with(to_delete_prefix); }),
                files.end());
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    list_files.clear();
    for (auto file = iter->next(); file.has_value(); file = iter->next()) {
        list_files.insert(std::move(file->path));
    }
    iter.reset();
    ASSERT_EQ(alloc_entries, 0);
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    ret = accessor.delete_all();
    ASSERT_EQ(ret, 0);
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());
}

} // namespace doris::cloud
