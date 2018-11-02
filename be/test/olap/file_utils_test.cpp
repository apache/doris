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

#include <algorithm>
#include <fstream>
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "agent/status.h"
#include "olap/olap_define.h"
#include "boost/filesystem.hpp"
#include "olap/file_helper.h"
#include "util/file_utils.h"
#include "util/logging.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class FileUtilsTest : public testing::Test {
public:
    // create a mock cgroup folder 
    virtual void SetUp() {
        ASSERT_FALSE(boost::filesystem::exists(_s_test_data_path));
        // create a mock cgroup path
        ASSERT_TRUE(boost::filesystem::create_directory(_s_test_data_path));
    }

    // delete the mock cgroup folder
    virtual void TearDown() {
        ASSERT_TRUE(boost::filesystem::remove_all(_s_test_data_path));
    }
    

    static std::string _s_test_data_path;
};

std::string FileUtilsTest::_s_test_data_path = "./file_utils_testxxxx123";

TEST_F(FileUtilsTest, TestCopyFile) {
    FileHandler src_file_handler;
    std::string src_file_name = _s_test_data_path + "/abcd12345.txt";
    // create a file using open
    ASSERT_FALSE(boost::filesystem::exists(src_file_name));
    OLAPStatus op_status = src_file_handler.open_with_mode(src_file_name, 
            O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
    ASSERT_EQ(OLAPStatus::OLAP_SUCCESS, op_status);
    ASSERT_TRUE(boost::filesystem::exists(src_file_name));
    
    char* large_bytes2[(1 << 12)];
    memset(large_bytes2, 0, sizeof(char)*((1 << 12)));
    int i = 0;
    while (i < 1 << 10) {
        src_file_handler.write(large_bytes2, ((1 << 12)));
        ++i;
    }
    src_file_handler.write(large_bytes2, 13);
    src_file_handler.close();
    
    std::string dst_file_name = _s_test_data_path + "/abcd123456.txt";
    FileUtils::copy_file(src_file_name, dst_file_name);
    FileHandler dst_file_handler;
    dst_file_handler.open(dst_file_name, O_RDONLY);
    int64_t dst_length = dst_file_handler.length();
    int64_t src_length = 4194317;
    ASSERT_EQ(src_length, dst_length);
}

}  // namespace doris

int main(int argc, char **argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
