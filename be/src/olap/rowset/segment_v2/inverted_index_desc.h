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
    static constexpr std::string_view segment_suffix = ".dat";
    static constexpr std::string_view index_suffix = ".idx";
    static std::string get_temporary_index_path(std::string_view tmp_dir_path,
                                                std::string_view rowset_id, int64_t seg_id,
                                                int64_t index_id,
                                                std::string_view index_path_suffix);
    // InvertedIndexStorageFormat V1
    static std::string get_index_file_path_v1(std::string_view index_path_prefix, int64_t index_id,
                                              std::string_view index_path_suffix);
    // InvertedIndexStorageFormat V2
    static std::string get_index_file_path_v2(std::string_view index_path_prefix);

    static std::string_view get_index_file_path_prefix(std::string_view segment_path);

    static std::string get_index_file_cache_key(std::string_view index_path_prefix,
                                                int64_t index_id,
                                                std::string_view index_path_suffix);

    static const char* get_temporary_null_bitmap_file_name() { return "null_bitmap"; }
    static const char* get_temporary_bkd_index_data_file_name() { return "bkd"; }
    static const char* get_temporary_bkd_index_meta_file_name() { return "bkd_meta"; }
    static const char* get_temporary_bkd_index_file_name() { return "bkd_index"; }
};

} // namespace segment_v2
} // namespace doris