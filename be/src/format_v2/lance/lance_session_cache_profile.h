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

#include <functional>
#include <memory>

#include "format_v2/lance/lance_session_manager.h"
#include "runtime/runtime_profile.h"

namespace doris::format::lance {

// Observes one reader's dataset-open windows without accessing its native dataset or scanner.
class LanceSessionCacheProfile {
public:
    using Snapshot = std::function<Status(LanceSessionManager::CacheStatistics*)>;

    LanceSessionCacheProfile(
            RuntimeProfile* profile, RuntimeProfile::Counter* index_misses,
            RuntimeProfile::Counter* metadata_misses, Snapshot snapshot = [](auto* stats) {
                return LanceSessionManager::instance().get_cache_statistics(stats);
            });
    ~LanceSessionCacheProfile();

    LanceSessionCacheProfile(const LanceSessionCacheProfile&) = delete;
    LanceSessionCacheProfile& operator=(const LanceSessionCacheProfile&) = delete;

    // The reader serializes start/stop; profile serialization may sample concurrently.
    void start();
    void stop();

private:
    struct State;
    std::shared_ptr<State> _state;
    std::shared_ptr<RuntimeProfile::SnapshotCallback> _snapshot_callback;
};

} // namespace doris::format::lance
