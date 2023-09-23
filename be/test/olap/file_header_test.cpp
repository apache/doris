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

#include "olap/file_header.h"

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <filesystem>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class FileHeaderTest : public testing::Test {
public:
    virtual void SetUp() {
        std::filesystem::remove_all(_s_test_data_path);
        EXPECT_FALSE(std::filesystem::exists(_s_test_data_path));
        EXPECT_TRUE(std::filesystem::create_directory(_s_test_data_path));
    }

    virtual void TearDown() { EXPECT_TRUE(std::filesystem::remove_all(_s_test_data_path)); }

    static std::string _s_test_data_path;
};

std::string FileHeaderTest::_s_test_data_path = "./file_handler_testxxxx123";

TEST_F(FileHeaderTest, TestWrite) {
    std::shared_ptr<io::LocalFileSystem> fs = io::global_local_filesystem();
    std::string file_name = _s_test_data_path + "/abcd123.txt";
    bool exists = true;
    EXPECT_TRUE(fs->exists(file_name, &exists).ok());
    EXPECT_FALSE(exists);

    io::FileWriterPtr file_writer;
    EXPECT_TRUE(fs->create_file(file_name, &file_writer).ok());

    // write 12 bytes to disk
    char ten_bytes[12];
    memset(&ten_bytes, 0, sizeof(ten_bytes));
    EXPECT_TRUE(file_writer->append({ten_bytes, sizeof(ten_bytes)}).ok());

    char large_bytes2[(1 << 10)];
    memset(&large_bytes2, 0, sizeof(large_bytes2));
    int i = 1;
    while (i < LOOP_LESS_OR_MORE(1 << 10, 1 << 17)) {
        EXPECT_TRUE(file_writer->append({large_bytes2, sizeof(large_bytes2)}).ok());
        ++i;
    }
    EXPECT_TRUE(file_writer->close().ok());
}

} // namespace doris
