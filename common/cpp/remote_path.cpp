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

#include "cpp/remote_path.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <charconv>
#include <vector>

#include "cpp/murmur_hash.h"

namespace doris {

namespace {

constexpr uint32_t MURMUR_SEED = 0xadc83b19;
constexpr std::string_view DATA_PREFIX = "data";
constexpr std::string_view DELETE_BITMAP_SUFFIX = "_delete_bitmap.db";

} // namespace

uint64_t remote_tablet_shard(int64_t tablet_id, int64_t shard_num) {
    return murmur_hash64A(&tablet_id, sizeof(tablet_id), MURMUR_SEED) % shard_num;
}

std::string remote_tablet_path(const RemotePathFormat& format, int64_t tablet_id) {
    // Callers reject unsupported formats when they create the storage resource; building a path
    // with a wrong layout would read missing objects or delete live ones
    CHECK(format.is_supported()) << "path_version=" << format.path_version
                                 << " shard_num=" << format.shard_num;
    if (format.path_version == 0) {
        return fmt::format("{}/{}", DATA_PREFIX, tablet_id);
    }
    return fmt::format("{}/{}/{}", DATA_PREFIX, remote_tablet_shard(tablet_id, format.shard_num),
                       tablet_id);
}

std::string remote_rowset_path_prefix(const RemotePathFormat& format, int64_t tablet_id,
                                      std::string_view rowset_id) {
    if (format.path_version == 0) {
        return fmt::format("{}/{}_", remote_tablet_path(format, tablet_id), rowset_id);
    }
    return fmt::format("{}/{}/", remote_tablet_path(format, tablet_id), rowset_id);
}

std::string remote_segment_path(const RemotePathFormat& format, int64_t tablet_id,
                                std::string_view rowset_id, int64_t segment_id) {
    return fmt::format("{}{}.dat", remote_rowset_path_prefix(format, tablet_id, rowset_id),
                       segment_id);
}

std::string remote_inverted_index_path_v1(const RemotePathFormat& format, int64_t tablet_id,
                                          std::string_view rowset_id, int64_t segment_id,
                                          int64_t index_id, std::string_view index_suffix) {
    return fmt::format("{}{}_{}{}{}.idx", remote_rowset_path_prefix(format, tablet_id, rowset_id),
                       segment_id, index_id, index_suffix.empty() ? "" : "@", index_suffix);
}

std::string remote_inverted_index_path_v2(const RemotePathFormat& format, int64_t tablet_id,
                                          std::string_view rowset_id, int64_t segment_id) {
    return fmt::format("{}{}.idx", remote_rowset_path_prefix(format, tablet_id, rowset_id),
                       segment_id);
}

std::string remote_delete_bitmap_path(const RemotePathFormat& format, int64_t tablet_id,
                                      std::string_view rowset_id) {
    return fmt::format("{}/{}{}", remote_tablet_path(format, tablet_id), rowset_id,
                       DELETE_BITMAP_SUFFIX);
}

std::string remote_rowset_id_from_path(std::string_view path, const RemotePathFormat& format) {
    auto file_name_pos = path.rfind('/');
    if (file_name_pos == std::string_view::npos) {
        return "";
    }
    std::string_view file_name = path.substr(file_name_pos + 1);
    if (format.path_version == 0 || file_name.ends_with(DELETE_BITMAP_SUFFIX)) {
        // {rowset_id}_{segment_id}.dat, {rowset_id}_delete_bitmap.db
        auto pos = file_name.find('_');
        return pos == std::string_view::npos ? "" : std::string(file_name.substr(0, pos));
    }
    // {rowset_id}/{segment_id}.dat
    std::string_view dir = path.substr(0, file_name_pos);
    auto rowset_id_pos = dir.rfind('/');
    return rowset_id_pos == std::string_view::npos ? ""
                                                   : std::string(dir.substr(rowset_id_pos + 1));
}

bool split_remote_tablet_file_key(std::string_view key, const RemotePathFormat& format,
                                  int64_t* tablet_id, std::string* file_name) {
    std::vector<std::string_view> parts;
    for (auto pos = key.find('/'); pos != std::string_view::npos; pos = key.find('/')) {
        if (pos > 0) {
            parts.push_back(key.substr(0, pos));
        }
        key.remove_prefix(pos + 1);
    }
    if (!key.empty()) {
        parts.push_back(key);
    }

    std::string_view tablet_part;
    if (format.path_version == 0) {
        // data/{tablet_id}/{file_name}
        if (parts.size() < 3) {
            return false;
        }
        tablet_part = parts[1];
        *file_name = std::string(parts.back());
    } else if (parts.size() == 4) {
        // data/{shard}/{tablet_id}/{rowset_id}_delete_bitmap.db
        tablet_part = parts[2];
        *file_name = std::string(parts[3]);
    } else if (parts.size() == 5) {
        // data/{shard}/{tablet_id}/{rowset_id}/{file_name}
        tablet_part = parts[2];
        *file_name = fmt::format("{}_{}", parts[3], parts[4]);
    } else {
        return false;
    }
    auto [ptr, ec] = std::from_chars(tablet_part.data(), tablet_part.data() + tablet_part.size(),
                                     *tablet_id);
    return ec == std::errc {} && *tablet_id > 0;
}

} // namespace doris
