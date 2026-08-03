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
#include <unordered_map>

namespace doris {

class DataDir;

enum class TabletPathGcMode : uint8_t {
    MOVE_TO_TRASH,
    DELETE_DIRECTLY,
};

enum class TabletPathGcReason : uint8_t {
    NORMAL_RETENTION,
    TRASH_RETENTION_DISABLED,
    MANUAL_CLEAN_TRASH,
    HIGH_DISK_WATERMARK,
    UNUSED_DATA_DIR,
};

struct ShutdownTabletGcPolicy {
    // Ineligible shutdown tablets remain in the global queue and are not dispatched.
    bool eligible = true;
    TabletPathGcMode mode = TabletPathGcMode::MOVE_TO_TRASH;
    TabletPathGcReason reason = TabletPathGcReason::NORMAL_RETENTION;
};

struct DataDirSweepPolicy {
    bool is_used = false;
    int32_t effective_trash_expire_seconds = 0;
    ShutdownTabletGcPolicy shutdown_tablet_gc;
};

using DataDirSweepPolicies = std::unordered_map<DataDir*, DataDirSweepPolicy>;

inline DataDirSweepPolicy build_data_dir_sweep_policy(bool is_used, bool ignore_guard,
                                                      int32_t configured_trash_expire,
                                                      double current_usage, double guard_space) {
    DataDirSweepPolicy policy;
    policy.is_used = is_used;
    if (!is_used) {
        policy.effective_trash_expire_seconds =
                configured_trash_expire <= 0 ? 0 : configured_trash_expire;
        policy.shutdown_tablet_gc.eligible = false;
        policy.shutdown_tablet_gc.reason = TabletPathGcReason::UNUSED_DATA_DIR;
        return policy;
    }

    const bool force_delete =
            ignore_guard || configured_trash_expire <= 0 || current_usage > guard_space;
    policy.effective_trash_expire_seconds = force_delete ? 0 : configured_trash_expire;
    policy.shutdown_tablet_gc.mode =
            force_delete ? TabletPathGcMode::DELETE_DIRECTLY : TabletPathGcMode::MOVE_TO_TRASH;

    if (configured_trash_expire <= 0) {
        policy.shutdown_tablet_gc.reason = TabletPathGcReason::TRASH_RETENTION_DISABLED;
    } else if (ignore_guard) {
        policy.shutdown_tablet_gc.reason = TabletPathGcReason::MANUAL_CLEAN_TRASH;
    } else if (current_usage > guard_space) {
        policy.shutdown_tablet_gc.reason = TabletPathGcReason::HIGH_DISK_WATERMARK;
    }
    return policy;
}

const char* tablet_path_gc_mode_name(TabletPathGcMode mode);
const char* tablet_path_gc_reason_name(TabletPathGcReason reason);

} // namespace doris
