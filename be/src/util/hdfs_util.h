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

#include "io/fs/hdfs.h"
#include "io/fs/path.h"

namespace doris {
class HDFSCommonBuilder;

namespace io {

class HDFSHandle {
public:
    ~HDFSHandle() {}

    static HDFSHandle& instance();

    hdfsFS create_hdfs_fs(HDFSCommonBuilder& builder);

private:
    HDFSHandle() {}
};

// if the format of path is hdfs://ip:port/path, replace it to /path.
// path like hdfs://ip:port/path can't be used by libhdfs3.
Path convert_path(const Path& path, const std::string& namenode);

std::string get_fs_name(const std::string& path);

// return true if path_or_fs contains "hdfs://"
bool is_hdfs(const std::string& path_or_fs);

} // namespace io
} // namespace doris
