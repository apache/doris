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

#include "util/zip_util.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "env/env.h"
#include "gutil/strings/util.h"
#include "testutil/test_util.h"
#include "util/file_utils.h"
#include "util/logging.h"

namespace doris {

using namespace strings;

TEST(ZipUtilTest, basic) {
    std::string path = GetCurrentRunningDir();

    FileUtils::remove_all(path + "/util/test_data/target");

    ZipFile zf = ZipFile(path + "/util/test_data/zip_normal.zip");
    auto st = zf.extract(path + "/util/test_data", "target");
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(FileUtils::check_exist(path + "/util/test_data/target/zip_normal_data"));
    EXPECT_FALSE(FileUtils::is_dir(path + "/util/test_data/target/zip_normal_data"));

    std::unique_ptr<RandomAccessFile> file;
    Env::Default()->new_random_access_file(path + "/util/test_data/target/zip_normal_data", &file);

    char f[11];
    Slice slice(f, 11);
    file->read_at(0, &slice);

    EXPECT_EQ("hello world", slice.to_string());

    FileUtils::remove_all(path + "/util/test_data/target");
}

TEST(ZipUtilTest, dir) {
    std::string path = GetCurrentRunningDir();

    FileUtils::remove_all(path + "/util/test_data/target");

    ZipFile zipFile = ZipFile(path + "/util/test_data/zip_dir.zip");
    EXPECT_TRUE(zipFile.extract(path + "/util/test_data", "target").ok());

    EXPECT_TRUE(FileUtils::check_exist(path + "/util/test_data/target/zip_test/one"));
    EXPECT_TRUE(FileUtils::is_dir(path + "/util/test_data/target/zip_test/one"));

    EXPECT_TRUE(FileUtils::check_exist(path + "/util/test_data/target/zip_test/one/data"));
    EXPECT_FALSE(FileUtils::is_dir(path + "/util/test_data/target/zip_test/one/data"));

    EXPECT_TRUE(FileUtils::check_exist(path + "/util/test_data/target/zip_test/two"));
    EXPECT_TRUE(FileUtils::is_dir(path + "/util/test_data/target/zip_test/two"));

    std::unique_ptr<RandomAccessFile> file;
    Env::Default()->new_random_access_file(path + "/util/test_data/target/zip_test/one/data",
                                           &file);

    char f[4];
    Slice slice(f, 4);
    file->read_at(0, &slice);

    EXPECT_EQ("test", slice.to_string());

    FileUtils::remove_all(path + "/util/test_data/target");
}

TEST(ZipUtilTest, targetAlready) {
    std::string path = GetCurrentRunningDir();

    ZipFile f(path + "/util/test_data/zip_normal.zip");

    Status st = f.extract(path + "/util/test_data", "zip_test");
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(HasPrefixString(st.to_string(), "Already exist"));
}

TEST(ZipUtilTest, notzip) {
    std::string path = GetCurrentRunningDir();

    ZipFile f(path + "/util/test_data/zip_normal_data");
    Status st = f.extract("test", "test");
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(HasPrefixString(st.to_string(), "Invalid argument"));
}

} // namespace doris
