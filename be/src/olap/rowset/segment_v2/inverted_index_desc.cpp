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

#include "olap/rowset/segment_v2/inverted_index_desc.h"

#include <fmt/format.h>

#include "common/status.h"
#include "gutil/strings/strip.h"
#include "olap/olap_common.h"

namespace doris::segment_v2 {

// {tmp_dir}/{rowset_id}_{seg_id}_{index_id}@{suffix}
std::string InvertedIndexDescriptor::get_temporary_index_path(std::string_view tmp_dir_path,
                                                              std::string_view rowset_id,
                                                              int64_t seg_id, int64_t index_id,
                                                              std::string_view index_path_suffix) {
    std::string suffix =
            index_path_suffix.empty() ? "" : std::string {"@"} + index_path_suffix.data();
    return fmt::format("{}/{}_{}_{}{}", tmp_dir_path, rowset_id, seg_id, index_id, suffix);
}

// InvertedIndexStorageFormat V1
// {prefix}_{index_id}@{suffix}.idx
std::string InvertedIndexDescriptor::get_index_file_path_v1(std::string_view index_path_prefix,
                                                            int64_t index_id,
                                                            std::string_view index_path_suffix) {
    std::string suffix =
            index_path_suffix.empty() ? "" : std::string {"@"} + index_path_suffix.data();
    return fmt::format("{}_{}{}{}", index_path_prefix, index_id, suffix, index_suffix);
}

// InvertedIndexStorageFormat V2
// {prefix}.idx
std::string InvertedIndexDescriptor::get_index_file_path_v2(std::string_view index_path_prefix) {
    return fmt::format("{}{}", index_path_prefix, index_suffix);
}

// local path prefix:
//   {storage_dir}/data/{shard_id}/{tablet_id}/{schema_hash}/{rowset_id}_{seg_id}
// remote path v0 prefix:
//   data/{tablet_id}/{rowset_id}_{seg_id}
std::string_view InvertedIndexDescriptor::get_index_file_path_prefix(
        std::string_view segment_path) {
    CHECK(segment_path.ends_with(segment_suffix));
    segment_path.remove_suffix(segment_suffix.size());
    return segment_path;
}

// {prefix}_{index_id}@{suffix} for inverted index cache
std::string InvertedIndexDescriptor::get_index_file_cache_key(std::string_view index_path_prefix,
                                                              int64_t index_id,
                                                              std::string_view index_path_suffix) {
    std::string suffix =
            index_path_suffix.empty() ? "" : std::string {"@"} + index_path_suffix.data();
    return fmt::format("{}_{}{}", index_path_prefix, index_id, suffix);
}

IndexFileNameFragment InvertedIndexDescriptor::decompose_local_index_file_name(
        const std::string& index_file_name) {
    DCHECK(index_file_name.ends_with(index_suffix));
    IndexFileNameFragment file_name_fragment;
    try {
        size_t underscore_pos = index_file_name.find('_');
        if (underscore_pos == std::string::npos) {
            return {};
        }

        // {rowset_id}_{seg_id}{index_suffix}
        file_name_fragment.rowset_id = std::string_view(index_file_name.data(), underscore_pos);
        file_name_fragment.seg_id = std::stoi(index_file_name.substr(underscore_pos + 1));

        size_t suffix_start_pos = index_file_name.find('_', underscore_pos + 1);
        if (suffix_start_pos == std::string::npos) {
            file_name_fragment.index_suffix = index_suffix;
        } else {
            file_name_fragment.index_suffix = std::string_view(
                    index_file_name.data() + suffix_start_pos); // "such as _30293@path.idx"
        }

        return file_name_fragment;
    } catch (...) {
        return {};
    }
}

std::string InvertedIndexDescriptor::snapshot_index_file_name(const std::string& index_file_name) {
    DCHECK(index_file_name.ends_with(index_suffix));
    return fmt::format("{}{}", index_file_name.substr(0, index_file_name.size() - 4),
                       ".binlog-index");
}

} // namespace doris::segment_v2