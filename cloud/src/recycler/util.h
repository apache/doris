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

#include <string>

namespace doris::cloud {

// The time unit is the same with BE: us
#define SCOPED_BVAR_LATENCY(bvar_item)                     \
    StopWatch sw;                                          \
    std::unique_ptr<int, std::function<void(int*)>> defer( \
            (int*)0x01, [&](int*) { bvar_item << sw.elapsed_us(); });

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

inline std::string segment_path(int64_t tablet_id, const std::string& rowset_id,
                                int64_t segment_id) {
    return fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, segment_id);
}

inline std::string inverted_index_path_v2(int64_t tablet_id, const std::string& rowset_id,
                                          int64_t segment_id) {
    return fmt::format("data/{}/{}_{}.idx", tablet_id, rowset_id, segment_id);
}

inline std::string inverted_index_path_v1(int64_t tablet_id, const std::string& rowset_id,
                                          int64_t segment_id, int64_t index_id,
                                          std::string_view index_path_suffix) {
    std::string suffix =
            index_path_suffix.empty() ? "" : std::string {"@"} + index_path_suffix.data();
    return fmt::format("data/{}/{}_{}_{}{}.idx", tablet_id, rowset_id, segment_id, index_id,
                       suffix);
}

inline std::string rowset_path_prefix(int64_t tablet_id, const std::string& rowset_id) {
    return fmt::format("data/{}/{}_", tablet_id, rowset_id);
}

inline std::string tablet_path_prefix(int64_t tablet_id) {
    return fmt::format("data/{}/", tablet_id);
}

} // namespace doris::cloud
