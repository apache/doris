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

#include "testutil/tmp_file_dirs_util.h"

#include <memory>
#include <string>
#include <vector>

#include "common/check.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/index_writer.h"
#include "storage/options.h"

namespace doris::test {

void ensure_tmp_file_dirs() {
    if (ExecEnv::GetInstance()->get_tmp_file_dirs() != nullptr) {
        return;
    }
    const std::string directory = "./ut_dir/tmp_file_dirs";
    DORIS_CHECK(io::global_local_filesystem()->delete_directory(directory).ok());
    DORIS_CHECK(io::global_local_filesystem()->create_directory(directory).ok());
    std::vector<StorePath> paths;
    paths.emplace_back(directory, -1);
    auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
    const Status status = tmp_file_dirs->init();
    DORIS_CHECK(status.ok()) << status.to_string();
    ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
}

} // namespace doris::test
