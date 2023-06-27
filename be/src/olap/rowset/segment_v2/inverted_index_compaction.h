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

#include <cstdint>
#include <string>
#include <vector>

#include "io/fs/file_system.h"

namespace doris {

namespace segment_v2 {
Status compact_column(int32_t index_id, int src_segment_num, int dest_segment_num,
                      std::vector<std::string> src_index_files,
                      std::vector<std::string> dest_index_files, const io::FileSystemSPtr& fs,
                      std::string index_writer_path, std::string tablet_path,
                      std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec,
                      std::vector<uint32_t> dest_segment_num_rows);
} // namespace segment_v2
} // namespace doris
