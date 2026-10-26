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
#include <string_view>

namespace doris {

// Object key layout of rowset files (`StorageVaultPB.path_format`), shared by BE, the recycler and
// the checker. Storage without a path format (legacy obj infos) uses path_version 0.
//   path_version 0: data/{tablet_id}/{rowset_id}_{segment_id}.dat
//   path_version 1: data/{shard}/{tablet_id}/{rowset_id}/{segment_id}.dat,
//                   shard = murmur_hash64A(tablet_id) % shard_num
struct RemotePathFormat {
    int64_t path_version = 0;
    int64_t shard_num = 0;

    bool is_supported() const { return path_version == 0 || (path_version == 1 && shard_num > 0); }
};

// The shard of a tablet in path_version 1
uint64_t remote_tablet_shard(int64_t tablet_id, int64_t shard_num);

// Directory of all objects of a tablet, without trailing '/'. data/{tablet_id} or
// data/{shard}/{tablet_id}
std::string remote_tablet_path(const RemotePathFormat& format, int64_t tablet_id);

// Prefix of the segment and inverted index files of a rowset, the file name follows directly:
// data/{tablet_id}/{rowset_id}_ or data/{shard}/{tablet_id}/{rowset_id}/
// In path_version 1 the delete bitmap file of the rowset is NOT under this prefix.
std::string remote_rowset_path_prefix(const RemotePathFormat& format, int64_t tablet_id,
                                      std::string_view rowset_id);

std::string remote_segment_path(const RemotePathFormat& format, int64_t tablet_id,
                                std::string_view rowset_id, int64_t segment_id);

// Inverted index storage format v1, one file per index
std::string remote_inverted_index_path_v1(const RemotePathFormat& format, int64_t tablet_id,
                                          std::string_view rowset_id, int64_t segment_id,
                                          int64_t index_id, std::string_view index_suffix);

// Inverted index storage format v2, one file per segment
std::string remote_inverted_index_path_v2(const RemotePathFormat& format, int64_t tablet_id,
                                          std::string_view rowset_id, int64_t segment_id);

std::string remote_delete_bitmap_path(const RemotePathFormat& format, int64_t tablet_id,
                                      std::string_view rowset_id);

// Split an object key of a tablet file into tablet id and file name. The file name is normalized to
// the path_version 0 form, e.g. both data/10005/{rowset_id}_5.dat and
// data/611/10005/{rowset_id}/5.dat give tablet 10005 and "{rowset_id}_5.dat".
// Return false if the key does not match the layout of `format`.
bool split_remote_tablet_file_key(std::string_view key, const RemotePathFormat& format,
                                  int64_t* tablet_id, std::string* file_name);

} // namespace doris
