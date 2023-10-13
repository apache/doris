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

#include <string>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "olap/olap_common.h"

namespace doris {

class SpecialDir {
public:
    SpecialDir(const std::string& path);
    ~SpecialDir();

    void get_dir_info(SpecialDirInfo* special_dir_info) {
        special_dir_info->path = _path;
        special_dir_info->capacity = _capacity_bytes;
        special_dir_info->available = _available_bytes;
        special_dir_info->is_used = _is_used;
        return;
    }

    Status update_capacity();

    void health_check();

private:
    Status _read_and_write_test_file();

    std::string _path;
    io::FileSystemSPtr _fs;
    // the actual available capacity of the disk of this data dir
    size_t _available_bytes;
    // the actual capacity of the disk of this data dir
    size_t _capacity_bytes;
    bool _is_used;
};

} // namespace doris
