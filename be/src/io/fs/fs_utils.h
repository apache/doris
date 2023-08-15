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

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <string>

#include "common/status.h"
#include "io/fs/path.h"

namespace doris {
namespace io {

struct FileSystemProperties {
    TFileType::type system_type;
    std::map<std::string, std::string> properties;
    THdfsParams hdfs_params;
    std::vector<TNetworkAddress> broker_addresses;
};

struct FileDescription {
    std::string path;
    int64_t start_offset;
    // length of the file in bytes.
    // -1 means unset.
    // If the file length is not set, the file length will be fetched from the file system.
    int64_t file_size = -1;
    // modification time of this file.
    // 0 means unset.
    int64_t mtime = 0;
};

} // namespace io
} // namespace doris
