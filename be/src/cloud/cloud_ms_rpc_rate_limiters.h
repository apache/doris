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

#include <bvar/bvar.h>

#include <array>
#include <memory>
#include <string>
#include <string_view>

#include "common/atomic_shared_ptr.h"
#include "cpp/s3_rate_limiter.h"

namespace doris::cloud {

// Macro to define all Meta Service RPC types
// Usage: META_SERVICE_RPC_TYPES(X) where X(enum_name, config_suffix, display_name)
// - enum_name: the enum value name (e.g., GET_TABLET_META)
// - config_suffix: suffix for the config name (e.g., get_tablet_meta -> ms_rpc_qps_get_tablet_meta)
// - display_name: the human-readable name for bvar metrics (e.g., "get tablet meta")
#define META_SERVICE_RPC_TYPES(X)                                                     \
    X(GET_TABLET_META, get_tablet_meta, "get tablet meta")                            \
    X(GET_ROWSET, get_rowset, "get rowset")                                           \
    X(PREPARE_ROWSET, prepare_rowset, "prepare rowset")                               \
    X(COMMIT_ROWSET, commit_rowset, "commit rowset")                                  \
    X(UPDATE_TMP_ROWSET, update_tmp_rowset, "update tmp rowset")                      \
    X(COMMIT_TXN, commit_txn, "commit txn")                                           \
    X(ABORT_TXN, abort_txn, "abort txn")                                              \
    X(PRECOMMIT_TXN, precommit_txn, "precommit txn")                                  \
    X(GET_OBJ_STORE_INFO, get_obj_store_info, "get obj store info")                   \
    X(START_TABLET_JOB, start_tablet_job, "start tablet job")                         \
    X(FINISH_TABLET_JOB, finish_tablet_job, "finish tablet job")                      \
    X(GET_DELETE_BITMAP, get_delete_bitmap, "get delete bitmap")                      \
    X(UPDATE_DELETE_BITMAP, update_delete_bitmap, "update delete bitmap")             \
    X(GET_DELETE_BITMAP_UPDATE_LOCK, get_delete_bitmap_update_lock,                   \
      "get delete bitmap update lock")                                                \
    X(REMOVE_DELETE_BITMAP_UPDATE_LOCK, remove_delete_bitmap_update_lock,             \
      "remove delete bitmap update lock")                                             \
    X(GET_INSTANCE, get_instance, "get instance")                                     \
    X(PREPARE_RESTORE_JOB, prepare_restore_job, "prepare restore job")                \
    X(COMMIT_RESTORE_JOB, commit_restore_job, "commit restore job")                   \
    X(FINISH_RESTORE_JOB, finish_restore_job, "finish restore job")                   \
    X(LIST_SNAPSHOTS, list_snapshots, "list snapshots")                               \
    X(UPDATE_PACKED_FILE_INFO, update_packed_file_info, "update packed file info")

// Enum class for Meta Service RPC types
enum class MetaServiceRPC : size_t {
#define DEFINE_ENUM(enum_name, config_suffix, display_name) enum_name,
    META_SERVICE_RPC_TYPES(DEFINE_ENUM)
#undef DEFINE_ENUM
    COUNT  // Total number of RPC types
};

// Get the display name for a MetaServiceRPC enum value
std::string_view meta_service_rpc_display_name(MetaServiceRPC rpc);

// Rate limiter with associated metrics for a single RPC method
struct RpcRateLimiter {
    std::unique_ptr<S3RateLimiterHolder> limiter;
    std::unique_ptr<bvar::LatencyRecorder> latency_recorder;

    RpcRateLimiter(int qps, std::string_view op_name);

    // Reset the rate limiter with new QPS
    void reset(int qps);
};

// Host-level rate limiters for MS RPCs to prevent burst traffic
// Each RPC method has its own rate limiter and bvar metrics
// Uses enum class MetaServiceRPC as key for O(1) lookup
class HostLevelMSRpcRateLimiters {
public:
    // Constructor initializes rate limiters for all RPC types from config
    HostLevelMSRpcRateLimiters();

    // Constructor for testing: initializes all rate limiters with uniform QPS
    // This allows unit tests to be independent of config values
    explicit HostLevelMSRpcRateLimiters(int uniform_qps);

    ~HostLevelMSRpcRateLimiters() = default;

    // Rate limit the specified RPC method, returns actual sleep time in nanoseconds
    // Thread-safe: each limiter handles its own synchronization
    int64_t limit(MetaServiceRPC rpc);

    // Reset a specific rate limiter with new QPS
    void reset(MetaServiceRPC rpc, int qps);

    // Reset all rate limiters (re-reads QPS from config)
    void reset_all();

private:
    void init_from_config();
    void init_with_uniform_qps(int qps);

    // Use atomic_shared_ptr for thread-safe access during concurrent limit() and reset() calls
    std::array<doris::atomic_shared_ptr<RpcRateLimiter>,
               static_cast<size_t>(MetaServiceRPC::COUNT)>
            _limiters;

    // For testing: allow direct access to internal state
    friend class HostLevelMSRpcRateLimitersTest;
    friend class HostLevelMSRpcRateLimitersConfigTest;
};

} // namespace doris::cloud
