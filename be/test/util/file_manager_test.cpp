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

#include "util/file_manager.h"

#include <gtest/gtest.h>

#include "env/env.h"

namespace doris {

class FileManagerTest : public testing::Test {
public:
    FileManagerTest() { }

    void SetUp() override {
        _file_exist = "file_exist";
    }

    void TearDown() override {
        
    }

private:
    std::string _file_exist;
};

TEST_F(FileManagerTest, normal) {
    FileManager* file_manager = FileManager::instance();
    OpenedFileHandle<RandomAccessFile> rfile;
    auto st = file_manager->open_file(_file_exist, &rfile);
    ASSERT_FALSE(st.ok());

    std::unique_ptr<WritableFile> wfile;
    st = Env::Default()->new_writable_file(_file_exist, &wfile);
    ASSERT_TRUE(st.ok());
    st = wfile->close();
    ASSERT_TRUE(st.ok());

    st = file_manager->open_file(_file_exist, &rfile);
    ASSERT_TRUE(st.ok());
    ASSERT_NE(nullptr, rfile.file());

    st = Env::Default()->delete_file(_file_exist);
    ASSERT_TRUE(st.ok());
}

}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
