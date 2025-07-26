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

#include "runtime/user_function_cache.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <filesystem>
#include <iostream>
#include <string>

#include "gtest/gtest.h"

namespace doris {

class UserFunctionCacheTest : public ::testing::Test {
protected:
    UserFunctionCache ufc;
};

TEST_F(UserFunctionCacheTest, SplitStringByChecksumTest) {
    // Test valid string format
    std::string valid_str =
            "7119053928154065546.20c8228267b6c9ce620fddb39467d3eb.postgresql-42.5.0.jar";
    auto result = ufc._split_string_by_checksum(valid_str);
    ASSERT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], "7119053928154065546");
    EXPECT_EQ(result[1], "20c8228267b6c9ce620fddb39467d3eb");
    EXPECT_EQ(result[2], "postgresql-42.5.0");
    EXPECT_EQ(result[3], "jar");
}

TEST_F(UserFunctionCacheTest, getFileNameFromUrl) {
    std::string url =
            "https://asdadadadas.oss-cn-hangzhou.aliyuncs.com/udf/"
            "java-udf-demo-jar-with-dependencies.jar?Expires=1751901956&OSSAccessKeyId=TMP."
            "3KnCifjy4MFB4df4AAAAAAAAAvqkTyZHvfGbxS4UYDJkfKn3wbtWgHnpqQGAKV64bY426DnB9jf6cEctwvShPa"
            "oyL4zD6v&Signature=AYs2HN4bQ7wG9onjEQ9nRcF6EGM%3D";

    auto result = ufc._get_file_name_from_url(url, doris::LibType::JAR);
    std::cout << result << std::endl;
    EXPECT_EQ(result, "java-udf-demo-jar-with-dependencies.jar");

    url = "https://asdadadadas.oss-cn-hangzhou.aliyuncs.com/udf/"
          "java-udf-demo-jar-with-dependencies.jar";
    result = ufc._get_file_name_from_url(url, doris::LibType::JAR);
    std::cout << result << std::endl;
    EXPECT_EQ(result, "java-udf-demo-jar-with-dependencies.jar");

    url = "file:///mnt/disk8/zhangsida/doris/samples/doris-demo/java-udf-demo/target/"
          "java-udf-demo-jar-with-dependencies.jar";
    result = ufc._get_file_name_from_url(url, doris::LibType::JAR);
    std::cout << result << std::endl;
    EXPECT_EQ(result, "java-udf-demo-jar-with-dependencies.jar");
}

TEST_F(UserFunctionCacheTest, makeLibFile) {
    ufc._lib_dir = config::user_function_dir;
    auto result = ufc._make_lib_file(123, "20c8228267b6c9ce620fddb39467d3eb", doris::LibType::JAR,
                                     "test.jar");
    std::cout << result << std::endl;
    EXPECT_EQ(result, ufc._lib_dir + "/123.20c8228267b6c9ce620fddb39467d3eb.test.jar");
}

TEST_F(UserFunctionCacheTest, downloadLib) {
    ufc._lib_dir = "test_data/user_function_cache/lib";
    std::string file_str(__FILE__);
    std::filesystem::path current_file(file_str);
    std::filesystem::path test_dir_base = current_file.parent_path();
    std::string test_dir_str = "/test_data/user_function_cache/test=.jar";
    std::string url = "file://" + test_dir_base.string() + test_dir_str;

    int64_t function_id = 123;
    std::string checksum = "65616717ee59c10da12f76823e837791";
    doris::LibType type = doris::LibType::JAR;
    std::string file_name = "test.jar";
    auto result_lib_file = ufc._make_lib_file(function_id, checksum, type, file_name);

    std::string result_lib_file2 = test_dir_base.string() + "/" + result_lib_file;
    auto entry =
            UserFunctionCacheEntry::create_shared(function_id, checksum, result_lib_file2, type);
    auto st = ufc._download_lib(url, entry);
    ASSERT_TRUE(st.ok()) << st;
    std::cout << "Library downloaded successfully." << std::endl;

    std::filesystem::path res_file(result_lib_file2);
    ASSERT_TRUE(std::filesystem::exists(res_file)) << "File does not exist: " << res_file.string();
}

} // namespace doris
