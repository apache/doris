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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/index_writer.h" // segment_v2::TmpFileDirs
#include "storage/options.h"            // StorePath

namespace doris {
namespace {

// Initializes ExecEnv's tmp_file_dirs ONCE for the whole doris_be_test binary so the
// SNII writer's resolve_temp_dir() (ExecEnv::get_tmp_file_dirs()->get_tmp_file_dir())
// works in unit tests, which otherwise leave tmp_file_dirs null. Mirrors the pattern
// used by ann_index_writer_test / segcompaction_test.
class SniiTmpDirEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        if (ExecEnv::GetInstance()->get_tmp_file_dirs() != nullptr) {
            return; // another test environment already configured it
        }
        const std::string tmp_dir = "./ut_dir/snii_tmp";
        static_cast<void>(io::global_local_filesystem()->delete_directory(tmp_dir));
        static_cast<void>(io::global_local_filesystem()->create_directory(tmp_dir));
        std::vector<StorePath> paths;
        paths.emplace_back(tmp_dir, -1);
        auto dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        static_cast<void>(dirs->init());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(dirs));
    }
};

[[maybe_unused]] ::testing::Environment* const g_snii_tmp_dir_env =
        ::testing::AddGlobalTestEnvironment(new SniiTmpDirEnvironment);

} // namespace
} // namespace doris
