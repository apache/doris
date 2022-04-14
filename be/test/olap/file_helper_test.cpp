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

#include "olap/file_helper.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

#include "common/configbase.h"
#include "common/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "olap/olap_define.h"
#include "testutil/test_util.h"
#include "util/logging.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class FileHandlerTest : public testing::Test {
public:
    // create a mock cgroup folder
    virtual void SetUp() {
        std::filesystem::remove_all(_s_test_data_path);
        EXPECT_FALSE(std::filesystem::exists(_s_test_data_path));
        // create a mock cgroup path
        EXPECT_TRUE(std::filesystem::create_directory(_s_test_data_path));
    }

    // delete the mock cgroup folder
    virtual void TearDown() { EXPECT_TRUE(std::filesystem::remove_all(_s_test_data_path)); }

    static std::string _s_test_data_path;
};

std::string FileHandlerTest::_s_test_data_path = "./log/file_handler_testxxxx123";

TEST_F(FileHandlerTest, TestWrite) {
    FileHandler file_handler;
    std::string file_name = _s_test_data_path + "/abcd123.txt";
    // create a file using open
    EXPECT_FALSE(std::filesystem::exists(file_name));
    Status op_status =
            file_handler.open_with_mode(file_name, O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
    EXPECT_EQ(Status::OK(), op_status);
    EXPECT_TRUE(std::filesystem::exists(file_name));

    // tell current offset
    off_t cur_offset = file_handler.tell();
    EXPECT_EQ(0, cur_offset);
    off_t length = file_handler.length();
    EXPECT_EQ(0, length);

    // seek to 10 and test offset
    off_t res = file_handler.seek(10, SEEK_SET);
    EXPECT_EQ(10, res);
    length = file_handler.length();
    EXPECT_EQ(0, length);

    cur_offset = file_handler.tell();
    EXPECT_EQ(10, cur_offset);

    // write 12 bytes to disk
    char ten_bytes[12];
    memset(&ten_bytes, 0, sizeof(ten_bytes));
    file_handler.write(&ten_bytes, sizeof(ten_bytes));
    cur_offset = file_handler.tell();
    EXPECT_EQ(22, cur_offset);
    length = file_handler.length();
    EXPECT_EQ(22, length);

    char large_bytes2[(1 << 10)];
    memset(&large_bytes2, 0, sizeof(large_bytes2));
    int i = 1;
    while (i < LOOP_LESS_OR_MORE(1 << 10, 1 << 17)) {
        file_handler.write(&large_bytes2, sizeof(large_bytes2));
        ++i;
    }
}

} // namespace doris
