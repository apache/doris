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

#include <gen_cpp/DataSinks_types.h>

#include <chrono>
#include <string>

namespace doris {

struct OutfileMarkerState {
    std::chrono::steady_clock::time_point updated_at;
    std::string owned_path;
    bool tombstoned = false;
};

constexpr auto OUTFILE_MARKER_STATE_TTL = std::chrono::hours(1);

inline bool should_expire_outfile_marker_state(const OutfileMarkerState& state,
                                               std::chrono::steady_clock::time_point now) {
    if (now - state.updated_at < OUTFILE_MARKER_STATE_TTL) {
        return false;
    }
    // A failed delete is the only terminal state that still owns rollback work.
    return !state.tombstoned || state.owned_path.empty();
}

inline bool should_sync_outfile_marker(TStorageBackendType::type storage_type) {
    // Local OUTFILE historically made the completion marker durable before acknowledging it.
    return storage_type == TStorageBackendType::LOCAL;
}

inline bool should_check_outfile_marker_existence(const TResultFileSinkOptions& file_options,
                                                  TStorageBackendType::type storage_type) {
    // Legacy remote writers keep their historical overwrite/append semantics during rolling
    // upgrades; only atomic requests opt into the cross-storage ownership check.
    return storage_type == TStorageBackendType::LOCAL || file_options.enable_atomic_outfile;
}

} // namespace doris
