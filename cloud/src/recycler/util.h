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
#include <string>
#include <string_view>
#include <vector>

#include "common/defer.h"
#include "cpp/remote_path.h"

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

// The object key layout of rowset files is defined once in `cpp/remote_path.h` and shared with BE.
// The helpers below are shorthands for it; the default format is path_version 0, which legacy obj
// infos and vaults without path format use.
inline RemotePathFormat path_format_from_pb(const StorageVaultPB::PathFormat& path_format) {
    return {.path_version = path_format.path_version(), .shard_num = path_format.shard_num()};
}

inline std::string rowset_path_prefix(int64_t tablet_id, const std::string& rowset_id,
                                      const RemotePathFormat& path_format = {}) {
    return remote_rowset_path_prefix(path_format, tablet_id, rowset_id);
}

inline std::string tablet_path_prefix(int64_t tablet_id, const RemotePathFormat& path_format = {}) {
    return remote_tablet_path(path_format, tablet_id) + '/';
}

inline std::string segment_path(int64_t tablet_id, const std::string& rowset_id, int64_t segment_id,
                                const RemotePathFormat& path_format = {}) {
    return remote_segment_path(path_format, tablet_id, rowset_id, segment_id);
}

inline std::string delete_bitmap_path(int64_t tablet_id, const std::string& rowset_id,
                                      const RemotePathFormat& path_format = {}) {
    return remote_delete_bitmap_path(path_format, tablet_id, rowset_id);
}

inline std::string inverted_index_path_v2(int64_t tablet_id, const std::string& rowset_id,
                                          int64_t segment_id,
                                          const RemotePathFormat& path_format = {}) {
    return remote_inverted_index_path_v2(path_format, tablet_id, rowset_id, segment_id);
}

inline std::string inverted_index_path_v1(int64_t tablet_id, const std::string& rowset_id,
                                          int64_t segment_id, int64_t index_id,
                                          std::string_view index_path_suffix,
                                          const RemotePathFormat& path_format = {}) {
    return remote_inverted_index_path_v1(path_format, tablet_id, rowset_id, segment_id, index_id,
                                         index_path_suffix);
}

int get_tablet_idx(TxnKv* txn_kv, const std::string& instance_id, int64_t tablet_id,
                   TabletIndexPB& tablet_idx);

int get_tablet_meta(TxnKv* txn_kv, const std::string& instance_id, int64_t tablet_id,
                    TabletMetaCloudPB& tablet_meta);
} // namespace doris::cloud
