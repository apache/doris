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

#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>

#include "common/config.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"

namespace doris::segment_v2 {

class DorisFSDirectoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Enable debug points for testing
        _original_enable_debug_points = config::enable_debug_points;
        config::enable_debug_points = true;

        _tmp_dir = std::filesystem::temp_directory_path() / "doris_fs_directory_test";
        std::filesystem::remove_all(_tmp_dir);
        std::filesystem::create_directories(_tmp_dir);
        _fs = io::global_local_filesystem();
        _directory = std::make_unique<DorisFSDirectory>();
        _directory->init(_fs, _tmp_dir.string().c_str());
    }

    void TearDown() override {
        _directory.reset();
        std::filesystem::remove_all(_tmp_dir);
        config::enable_debug_points = _original_enable_debug_points;
    }

    std::filesystem::path _tmp_dir;
    io::FileSystemSPtr _fs;
    std::unique_ptr<DorisFSDirectory> _directory;
    bool _original_enable_debug_points;
};

TEST_F(DorisFSDirectoryTest, FSIndexInputReadInternalTimer) {
    std::string file_name = "test_timer_file";
    std::filesystem::path test_file = _tmp_dir / file_name;
    std::ofstream ofs(test_file);
    std::string content = "some test content for timer";
    ofs << content;
    ofs.close();

    lucene::store::IndexInput* input1 = nullptr;
    CLuceneError error;
    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), input1, error);
    EXPECT_TRUE(result);
    ASSERT_NE(input1, nullptr);

    auto* fs_input1 = dynamic_cast<DorisFSDirectory::FSIndexInput*>(input1);
    ASSERT_NE(fs_input1, nullptr);

    io::FileCacheStatistics stats;
    fs_input1->_io_ctx.file_cache_stats = &stats;

    auto* input2 = fs_input1->clone();
    auto* fs_input2 = dynamic_cast<DorisFSDirectory::FSIndexInput*>(input2);
    ASSERT_NE(fs_input2, nullptr);

    fs_input2->_io_ctx.file_cache_stats = &stats;

    uint8_t buffer1[10];
    input1->readBytes(buffer1, 10);
    EXPECT_GT(stats.inverted_index_io_timer, 0);
    int64_t old_time = stats.inverted_index_io_timer;

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    input2->seek(0);
    uint8_t buffer2[10];
    input2->readBytes(buffer2, 10);
    EXPECT_GT(stats.inverted_index_io_timer, old_time);

    _CLDELETE(input2);
    _CLDELETE(input1);
}

TEST_F(DorisFSDirectoryTest, PrivGetFN) {
    {
        std::string file_name = "my_file.txt";
        std::string result = _directory->priv_getFN(file_name);
        std::string expected_path = (_tmp_dir / file_name).string();
        EXPECT_EQ(result, expected_path);
    }
    {
        std::string file_name = "";
        std::string result = _directory->priv_getFN(file_name);
        std::string expected_path = (_tmp_dir / file_name).string();
        EXPECT_EQ(result, expected_path);
    }
    {
        std::string file_name = "subdir/another_file.log";
        std::string result = _directory->priv_getFN(file_name);
        std::string expected_path = (_tmp_dir / file_name).string();
        EXPECT_EQ(result, expected_path);
    }
}

} // namespace doris::segment_v2
