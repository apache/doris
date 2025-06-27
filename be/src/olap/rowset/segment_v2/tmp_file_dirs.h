
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

#pragma once

#include <vector>

#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "olap/options.h"

namespace doris::segment_v2 {
class TmpFileDirs {
public:
    TmpFileDirs(const std::vector<doris::StorePath>& store_paths) {
        for (const auto& store_path : store_paths) {
            _tmp_file_dirs.emplace_back(store_path.path + "/" + config::tmp_file_dir);
        }
    };

    Status init() {
        for (auto& tmp_file_dir : _tmp_file_dirs) {
            // delete the tmp dir to avoid the tmp files left by last crash
            RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(tmp_file_dir));
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(tmp_file_dir));
        }
        return Status::OK();
    };

    io::Path get_tmp_file_dir() {
        std::cout << "TmpFileDirs size: " << _tmp_file_dirs.size() << std::endl;
        size_t cur_index = _next_index.fetch_add(1);
        return _tmp_file_dirs[cur_index % _tmp_file_dirs.size()];
    };

    ~TmpFileDirs() { std::cout << "TmpFileDirs destroyed!" << std::endl; }

private:
    std::vector<io::Path> _tmp_file_dirs;
    std::atomic_size_t _next_index {0}; // use for round-robin
};

} // namespace doris::segment_v2
