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

#include "storage/index/snii/writer/temp_dir.h"

#include <filesystem>
#include <string>
#include <system_error>

#include "runtime/exec_env.h"
#include "storage/index/index_writer.h" // segment_v2::TmpFileDirs (full definition)

namespace doris::snii::writer {

std::string resolve_temp_dir() {
    // Use Doris's configured spill/scratch dirs (the same source the inverted-index
    // writer uses; see index_file_writer.cpp). SNII spills/section temp files live in
    // a dedicated "snii" subdirectory so they do not crowd the tmp root alongside
    // every other component's files.
    auto dir = ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir();
    dir /= "snii";
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    return dir.native();
}

} // namespace doris::snii::writer
