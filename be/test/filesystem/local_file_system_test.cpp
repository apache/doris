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

#include "filesystem/local_file_system.h"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <string_view>

namespace doris {

class LocalFileSystemTest : public testing::Test {
public:
    LocalFileSystemTest() = default;
    ~LocalFileSystemTest() override = default;
};

TEST_F(LocalFileSystemTest, normal) {
    namespace fs = std::filesystem;
    fs::create_directories("ut_dir/local_fs");
    fs::create_directories("ut_dir/local_fs/dir");
    system("touch ut_dir/local_fs/file");
    system("touch ut_dir/local_fs/dir/a");
    system("touch ut_dir/local_fs/dir/b");
    system("touch ut_dir/local_fs/dir/c");

    auto local_fs = LocalFileSystem("ut_dir/local_fs");
    bool res;

    local_fs.exists("dir", &res);
    ASSERT_TRUE(res);
    local_fs.exists("abc", &res);
    ASSERT_FALSE(res);

    local_fs.is_directory("dir", &res);
    ASSERT_TRUE(res);
    local_fs.is_directory("file", &res);
    ASSERT_FALSE(res);
    local_fs.is_directory("abc", &res);
    ASSERT_FALSE(res);

    local_fs.is_file("dir", &res);
    ASSERT_FALSE(res);
    local_fs.is_file("file", &res);
    ASSERT_TRUE(res);
    local_fs.is_file("abc", &res);
    ASSERT_FALSE(res);

    std::vector<FileStat> files;
    local_fs.list("dir", &files);
    ASSERT_EQ(files.size(), 3);
    auto s = local_fs.list("file", &files);
    ASSERT_TRUE(!s.ok());

    s = local_fs.delete_file("file");
    ASSERT_TRUE(s.ok());
    s = local_fs.delete_file("dir");
    ASSERT_TRUE(!s.ok());
    s = local_fs.delete_file("abc");
    ASSERT_TRUE(s.ok());

    s = local_fs.delete_directory("dir/a");
    ASSERT_TRUE(!s.ok());
    s = local_fs.delete_directory("dir");
    ASSERT_TRUE(s.ok());
    s = local_fs.delete_directory("abc");
    ASSERT_TRUE(s.ok());

    s = local_fs.create_directory("dir/dir");
    ASSERT_TRUE(s.ok());
    s = local_fs.create_directory("dir");
    ASSERT_TRUE(!s.ok());
}

} // namespace doris
