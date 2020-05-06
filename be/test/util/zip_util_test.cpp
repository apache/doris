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

#include <iostream>
#include <string>
#include <libgen.h>
#include <gtest/gtest.h>

#include "env/env.h"
#include "util/zip_util.h"
#include "util/file_utils.h"
#include "util/logging.h"
#include "gutil/strings/util.h"

namespace doris {

using namespace strings;

TEST(ZipUtilTest, basic) {
    char buf[1024];
    readlink("/proc/self/exe", buf, 1023);
    char* dir_path = dirname(buf);
    std::string path(dir_path);

    FileUtils::remove_all(path + "/test_data/target");

    ZipFile zf = ZipFile(path + "/test_data/zip_normal.zip");
    ASSERT_TRUE(zf.extract(path + "/test_data", "target").ok());
    ASSERT_TRUE(FileUtils::check_exist(path + "/test_data/target/zip_normal_data"));
    ASSERT_FALSE(FileUtils::is_dir(path + "/test_data/target/zip_normal_data"));

    std::unique_ptr<RandomAccessFile> file;
    Env::Default()->new_random_access_file(path + "/test_data/target/zip_normal_data", &file);
    
    char f[11];
    Slice slice(f, 11);
    file->read_at(0, slice);
    
    ASSERT_EQ("hello world", slice.to_string());
    
    FileUtils::remove_all(path + "/test_data/target");
}

TEST(ZipUtilTest, dir) {
    char buf[1024];
    readlink("/proc/self/exe", buf, 1023);
    char* dir_path = dirname(buf);
    std::string path(dir_path);

    FileUtils::remove_all(path + "/test_data/target");

    ZipFile zipFile = ZipFile(path + "/test_data/zip_dir.zip");
    ASSERT_TRUE(zipFile.extract( path + "/test_data", "target").ok());

    ASSERT_TRUE(FileUtils::check_exist(path + "/test_data/target/zip_test/one"));
    ASSERT_TRUE(FileUtils::is_dir(path + "/test_data/target/zip_test/one"));

    ASSERT_TRUE(FileUtils::check_exist(path + "/test_data/target/zip_test/one/data"));
    ASSERT_FALSE(FileUtils::is_dir(path + "/test_data/target/zip_test/one/data"));

    ASSERT_TRUE(FileUtils::check_exist(path + "/test_data/target/zip_test/two"));
    ASSERT_TRUE(FileUtils::is_dir(path + "/test_data/target/zip_test/two"));

    std::unique_ptr<RandomAccessFile> file;
    Env::Default()->new_random_access_file(path + "/test_data/target/zip_test/one/data", &file);

    char f[4];
    Slice slice(f, 4);
    file->read_at(0, slice);

    ASSERT_EQ("test", slice.to_string());

    FileUtils::remove_all(path + "/test_data/target");
}


TEST(ZipUtilTest, targetAlready) {
    char buf[1024];
    readlink("/proc/self/exe", buf, 1023);
    char* dir_path = dirname(buf);
    std::string path(dir_path);

    ZipFile f(path + "/test_data/zip_normal.zip");
    
    Status st = f.extract(path + "/test_data", "zip_test");
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(HasPrefixString(st.to_string(), "Already exist"));
}


TEST(ZipUtilTest, notzip) {
    char buf[1024];
    readlink("/proc/self/exe", buf, 1023);
    char* dir_path = dirname(buf);
    std::string path(dir_path);

    ZipFile f(path + "/test_data/zip_normal_data");
    Status st = f.extract("test", "test");
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(HasPrefixString(st.to_string(), "Invalid argument"));
}


}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
