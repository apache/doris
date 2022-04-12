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

#include "olap/fs/file_block_manager.h"

#include <gtest/gtest.h>

#include <string>

#include "env/env.h"
#include "util/file_utils.h"
#include "util/slice.h"

using std::string;

namespace doris {

class FileBlockManagerTest : public testing::Test {
protected:
    const std::string kBlockManagerDir = "./ut_dir/file_block_manager";

    void SetUp() override {
        if (FileUtils::check_exist(kBlockManagerDir)) {
            EXPECT_TRUE(FileUtils::remove_all(kBlockManagerDir).ok());
        }
        EXPECT_TRUE(FileUtils::create_dir(kBlockManagerDir).ok());
    }

    void TearDown() override {
        if (FileUtils::check_exist(kBlockManagerDir)) {
            EXPECT_TRUE(FileUtils::remove_all(kBlockManagerDir).ok());
        }
    }
};

TEST_F(FileBlockManagerTest, NormalTest) {
    fs::BlockManagerOptions bm_opts;
    bm_opts.read_only = false;
    bm_opts.enable_metric = false;
    Env* env = Env::Default();
    std::unique_ptr<fs::FileBlockManager> fbm(new fs::FileBlockManager(env, std::move(bm_opts)));

    std::unique_ptr<fs::WritableBlock> wblock;
    std::string fname = kBlockManagerDir + "/test_file";
    fs::CreateBlockOptions wblock_opts(fname);
    Status st = fbm->create_block(wblock_opts, &wblock);
    EXPECT_TRUE(st.ok()) << st.get_error_msg();

    std::string data = "abcdefghijklmnopqrstuvwxyz";
    wblock->append(data);
    wblock->close();

    FilePathDesc path_desc;
    path_desc.filepath = fname;
    std::unique_ptr<fs::ReadableBlock> rblock;
    st = fbm->open_block(path_desc, &rblock);
    uint64_t file_size = 0;
    EXPECT_TRUE(rblock->size(&file_size).ok());
    EXPECT_EQ(data.size(), file_size);
    std::string read_buff(data.size(), 'a');
    Slice read_slice(read_buff);
    rblock->read(0, read_slice);
    EXPECT_EQ(data, read_buff);
    rblock->close();
}

} // namespace doris
