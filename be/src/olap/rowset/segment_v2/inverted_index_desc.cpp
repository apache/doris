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

#include "gutil/strings/strip.h"
#include "olap/olap_common.h"

namespace doris::segment_v2 {
const std::string segment_suffix = ".dat";
const std::string index_suffix = ".idx";
const std::string index_name_separator = "_";

std::string InvertedIndexDescriptor::get_temporary_index_path(
        const std::string& segment_path, uint32_t uuid, const std::string& index_suffix_path) {
    return StripSuffixString(segment_path, segment_suffix) + index_name_separator +
           std::to_string(uuid) + index_suffix_path;
}

std::string InvertedIndexDescriptor::get_index_file_name(const std::string& segment_path,
                                                         uint32_t uuid,
                                                         const std::string& index_suffix_path) {
    return StripSuffixString(segment_path, segment_suffix) + index_name_separator +
           std::to_string(uuid) + index_suffix_path + index_suffix;
}

std::string InvertedIndexDescriptor::inverted_index_file_path(
        const string& rowset_dir, const RowsetId& rowset_id, int segment_id, int64_t index_id,
        const std::string& index_suffix_path) {
    // {rowset_dir}/{schema_hash}/{rowset_id}_{seg_num}_{index_id}.idx
    return fmt::format("{}/{}_{}_{}{}.idx", rowset_dir, rowset_id.to_string(), segment_id, index_id,
                       index_suffix_path);
}

std::string InvertedIndexDescriptor::local_inverted_index_path_segcompacted(
        const string& tablet_path, const RowsetId& rowset_id, int64_t begin, int64_t end,
        int64_t index_id, const std::string& index_suffix_path) {
    // {root_path}/data/{shard_id}/{tablet_id}/{schema_hash}/{rowset_id}_{begin_seg}-{end_seg}_{index_id}.idx
    return fmt::format("{}/{}_{}-{}_{}{}.idx", tablet_path, rowset_id.to_string(), begin, end,
                       index_id, index_suffix_path);
}
} // namespace doris::segment_v2