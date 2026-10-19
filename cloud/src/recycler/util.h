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

#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

#include "common/defer.h"

namespace doris::cloud {

// The time unit is the same with BE: us
#define SCOPED_BVAR_LATENCY(bvar_item) \
    StopWatch sw;                      \
    DORIS_CLOUD_DEFER {                \
        bvar_item << sw.elapsed_us();  \
    };

class TxnKv;

/**
 * Get all instances, include DELETED instance
 * @return 0 for success, otherwise error
 */
int get_all_instances(TxnKv* txn_kv, std::vector<InstanceInfoPB>& res);

/**
 *
 * @return 0 for success
 */
int prepare_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 int64_t interval_ms);

void finish_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 bool success, int64_t ctime_ms);

/**
 *
 * @return 0 for success, 1 if job should be aborted, negative for other errors
 */
int lease_instance_recycle_job(TxnKv* txn_kv, std::string_view key, const std::string& instance_id,
                               const std::string& ip_port);

// Object key layout of a storage vault, it MUST be consistent with BE `StorageResource`.
//  path_version 0: data/{tablet_id}/{rowset_id}_{segment_id}.dat
//  path_version 1: data/{shard}/{tablet_id}/{rowset_id}/{segment_id}.dat,
//                  shard = murmur_hash64A(tablet_id) % shard_num
// Legacy obj infos and vaults without path format use path_version 0.
struct VaultPathFormat {
    int64_t path_version = 0;
    int64_t shard_num = 0;

    static VaultPathFormat from_pb(const StorageVaultPB::PathFormat& path_format) {
        return {path_format.path_version(), path_format.shard_num()};
    }

    bool is_supported() const { return path_version == 0 || (path_version == 1 && shard_num > 0); }
};

// MurmurHash2 64 bit version, it MUST be identical to BE `HashUtil::murmur_hash64A`.
uint64_t murmur_hash64A(const void* key, int64_t len, uint32_t seed);

// It MUST be identical to the shard function of BE `StorageResource`.
inline uint64_t tablet_shard(int64_t tablet_id, int64_t shard_num) {
    constexpr uint32_t MURMUR_SEED = 0xadc83b19;
    return murmur_hash64A(&tablet_id, sizeof(tablet_id), MURMUR_SEED) % shard_num;
}

// Directory of all objects of a tablet, without trailing '/'
inline std::string tablet_dir(int64_t tablet_id, const VaultPathFormat& path_format) {
    CHECK(path_format.is_supported()) << "path_version=" << path_format.path_version
                                      << " shard_num=" << path_format.shard_num;
    if (path_format.path_version == 0) {
        return fmt::format("data/{}", tablet_id);
    }
    return fmt::format("data/{}/{}", tablet_shard(tablet_id, path_format.shard_num), tablet_id);
}

// Prefix of all segment and index files of a rowset, file name follows directly.
// Under path_version 1 the delete bitmap file is NOT covered by this prefix, see `delete_bitmap_path`.
inline std::string rowset_path_prefix(int64_t tablet_id, const std::string& rowset_id,
                                      const VaultPathFormat& path_format = {}) {
    if (path_format.path_version == 0) {
        return fmt::format("{}/{}_", tablet_dir(tablet_id, path_format), rowset_id);
    }
    return fmt::format("{}/{}/", tablet_dir(tablet_id, path_format), rowset_id);
}

inline std::string tablet_path_prefix(int64_t tablet_id, const VaultPathFormat& path_format = {}) {
    return tablet_dir(tablet_id, path_format) + '/';
}

inline std::string segment_path(int64_t tablet_id, const std::string& rowset_id, int64_t segment_id,
                                const VaultPathFormat& path_format = {}) {
    return fmt::format("{}{}.dat", rowset_path_prefix(tablet_id, rowset_id, path_format),
                       segment_id);
}

inline std::string delete_bitmap_path(int64_t tablet_id, const std::string& rowset_id,
                                      const VaultPathFormat& path_format = {}) {
    return fmt::format("{}/{}_delete_bitmap.db", tablet_dir(tablet_id, path_format), rowset_id);
}

inline std::string inverted_index_path_v2(int64_t tablet_id, const std::string& rowset_id,
                                          int64_t segment_id,
                                          const VaultPathFormat& path_format = {}) {
    return fmt::format("{}{}.idx", rowset_path_prefix(tablet_id, rowset_id, path_format),
                       segment_id);
}

inline std::string inverted_index_path_v1(int64_t tablet_id, const std::string& rowset_id,
                                          int64_t segment_id, int64_t index_id,
                                          std::string_view index_path_suffix,
                                          const VaultPathFormat& path_format = {}) {
    std::string suffix =
            index_path_suffix.empty() ? "" : std::string {"@"} + index_path_suffix.data();
    return fmt::format("{}{}_{}{}.idx", rowset_path_prefix(tablet_id, rowset_id, path_format),
                       segment_id, index_id, suffix);
}

// Parse the rowset id from a path built by the helpers above, return empty string if `path` is
// not a rowset file (e.g. a packed file).
inline std::string rowset_id_from_path(std::string_view path, const VaultPathFormat& path_format) {
    auto file_name_pos = path.rfind('/');
    if (file_name_pos == std::string_view::npos) {
        return "";
    }
    std::string_view file_name = path.substr(file_name_pos + 1);
    if (path_format.path_version == 0 || file_name.ends_with("_delete_bitmap.db")) {
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

// Split an object key of a tablet file into tablet id and file name. The file name is normalized to
// the path_version 0 form, e.g. both "data/{tablet_id}/{rowset_id}_{segment_id}.dat" and
// "data/{shard}/{tablet_id}/{rowset_id}/{segment_id}.dat" give "{rowset_id}_{segment_id}.dat".
// Return false if the key does not match the layout of `path_format`.
inline bool split_tablet_file_key(const std::string& obj_key, const VaultPathFormat& path_format,
                                  int64_t* tablet_id, std::string* file_name) {
    std::vector<std::string> str;
    std::string_view rest = obj_key;
    for (auto pos = rest.find('/'); pos != std::string_view::npos; pos = rest.find('/')) {
        if (pos > 0) {
            str.emplace_back(rest.substr(0, pos));
        }
        rest.remove_prefix(pos + 1);
    }
    if (!rest.empty()) {
        str.emplace_back(rest);
    }
    if (path_format.path_version == 0) {
        // data/{tablet_id}/{file_name}
        if (str.size() < 3) {
            return false;
        }
        *tablet_id = atol(str[1].c_str());
        *file_name = str.back();
    } else if (str.size() == 4) {
        // data/{shard}/{tablet_id}/{rowset_id}_delete_bitmap.db
        *tablet_id = atol(str[2].c_str());
        *file_name = str[3];
    } else if (str.size() == 5) {
        // data/{shard}/{tablet_id}/{rowset_id}/{file_name}
        *tablet_id = atol(str[2].c_str());
        *file_name = str[3] + '_' + str[4];
    } else {
        return false;
    }
    return *tablet_id > 0;
}

int get_tablet_idx(TxnKv* txn_kv, const std::string& instance_id, int64_t tablet_id,
                   TabletIndexPB& tablet_idx);

int get_tablet_meta(TxnKv* txn_kv, const std::string& instance_id, int64_t tablet_id,
                    TabletMetaCloudPB& tablet_meta);
} // namespace doris::cloud
