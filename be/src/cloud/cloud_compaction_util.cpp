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

#include "cloud/cloud_compaction_util.h"

#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "gen_cpp/cloud.pb.h"
#include "util/time.h"

namespace doris {
#include "common/compile_check_begin.h"

bool should_do_compaction_for_cluster(CloudTablet* tablet) {
    std::string last_active_cluster = tablet->last_active_cluster_id();
    std::string my_cluster = tablet->my_cluster_id();

    // Case 1: No active cluster record, any cluster can compact
    if (last_active_cluster.empty()) {
        LOG(DEBUG) << "[compaction_rw_separation] tablet " << tablet->tablet_id()
                   << " has no active cluster record, allow compaction";
        return true;
    }

    // Case 2: This is the active cluster, allow compaction
    if (last_active_cluster == my_cluster) {
        return true;
    }

    auto status = static_cast<cloud::ClusterStatus>(tablet->last_active_cluster_status());
    int64_t status_mtime = tablet->last_active_cluster_status_mtime_ms();
    int64_t now = UnixMillis();

    // Case 3: Original cluster is NORMAL (still active), cannot takeover
    if (status == cloud::ClusterStatus::NORMAL) {
        LOG(INFO) << "[compaction_rw_separation] skip compaction for tablet " << tablet->tablet_id()
                  << ": write cluster (" << last_active_cluster << ") is still active (status=NORMAL)"
                  << ", this read cluster (" << my_cluster << ") should not compact";
        return false;
    }

    // Case 4: Original cluster is unavailable (SUSPENDED/MANUAL_SHUTDOWN/deleted)
    int64_t elapsed = now - status_mtime;
    int64_t timeout = config::compaction_cluster_takeover_timeout_ms;

    if (elapsed > timeout) {
        // Takeover successful
        LOG(INFO) << "[compaction_rw_separation] takeover compaction for tablet " << tablet->tablet_id()
                  << ": write cluster (" << last_active_cluster << ") is unavailable (status=" << status << ")"
                  << ", unavailable since " << status_mtime << " (" << elapsed << "ms ago)"
                  << ", this cluster (" << my_cluster << ") takes over (timeout=" << timeout << "ms)";
        return true;
    } else {
        // Timeout not reached yet, waiting
        LOG(INFO) << "[compaction_rw_separation] skip compaction for tablet " << tablet->tablet_id()
                  << ": write cluster (" << last_active_cluster << ") is unavailable (status=" << status << ")"
                  << ", but timeout not reached yet (elapsed=" << elapsed << "ms, timeout=" << timeout << "ms)"
                  << ", waiting before takeover by this cluster (" << my_cluster << ")";
        return false;
    }
}

#include "common/compile_check_end.h"
} // namespace doris
