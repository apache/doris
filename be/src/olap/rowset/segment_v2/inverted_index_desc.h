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

#include <stdint.h>

#include <string>

namespace doris {
struct RowsetId;

namespace segment_v2 {

class InvertedIndexDescriptor {
public:
    static std::string get_temporary_index_path(const std::string& segment_path, uint32_t uuid,
                                                const std::string& index_suffix_path);
    static std::string get_index_file_name(const std::string& path, uint32_t uuid,
                                           const std::string& index_suffix_path);
    static const std::string get_temporary_null_bitmap_file_name() { return "null_bitmap"; }
    static const std::string get_temporary_bkd_index_data_file_name() { return "bkd"; }
    static const std::string get_temporary_bkd_index_meta_file_name() { return "bkd_meta"; }
    static const std::string get_temporary_bkd_index_file_name() { return "bkd_index"; }
    static std::string inverted_index_file_path(const std::string& rowset_dir,
                                                const RowsetId& rowset_id, int segment_id,
                                                int64_t index_id,
                                                const std::string& index_suffix_path);

    static std::string local_inverted_index_path_segcompacted(const std::string& tablet_path,
                                                              const RowsetId& rowset_id,
                                                              int64_t begin, int64_t end,
                                                              int64_t index_id,
                                                              const std::string& index_suffix_path);
};

} // namespace segment_v2
} // namespace doris