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

#include "format_v2/lance/lance_session_cache_profile.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include <utility>

#include "common/logging.h"

namespace doris::format::lance {

struct LanceSessionCacheProfile::State {
    State(RuntimeProfile* profile, RuntimeProfile::Counter* index_misses,
          RuntimeProfile::Counter* metadata_misses, Snapshot snapshot)
            : profile(profile),
              index_misses(index_misses),
              metadata_misses(metadata_misses),
              snapshot(std::move(snapshot)) {}

    void collect() {
        std::lock_guard<std::mutex> guard(mutex);
        collect_locked();
    }

    void collect_locked() {
        if (!active || !valid) {
            return;
        }
        LanceSessionManager::CacheStatistics current;
        const auto status = snapshot(&current);
        if (!status.ok()) {
            complete = false;
            profile->add_info_string("LanceSessionCacheStatsAvailable", "false");
            LOG(WARNING) << status;
            return;
        }
        // Serialization and dataset close share a baseline under this lock. Neither can
        // count a delta twice, or extend a closed window with unrelated Session activity.
        const auto update = [](RuntimeProfile::Counter* counter, uint64_t before, uint64_t after) {
            if (after >= before) {
                COUNTER_UPDATE(counter,
                               static_cast<int64_t>(std::min<uint64_t>(
                                       after - before, std::numeric_limits<int64_t>::max())));
            }
        };
        update(index_misses, previous.index_cache_misses, current.index_cache_misses);
        update(metadata_misses, previous.metadata_cache_misses, current.metadata_cache_misses);
        previous = current;
        profile->add_info_string("LanceSessionCacheStatsAvailable", complete ? "true" : "false");
    }

    std::mutex mutex;
    RuntimeProfile* profile;
    RuntimeProfile::Counter* index_misses;
    RuntimeProfile::Counter* metadata_misses;
    Snapshot snapshot;
    LanceSessionManager::CacheStatistics previous;
    bool active = false;
    bool valid = false;
    bool complete = true;
};

LanceSessionCacheProfile::LanceSessionCacheProfile(RuntimeProfile* profile,
                                                   RuntimeProfile::Counter* index_misses,
                                                   RuntimeProfile::Counter* metadata_misses,
                                                   Snapshot snapshot)
        : _state(std::make_shared<State>(profile, index_misses, metadata_misses,
                                         std::move(snapshot))),
          _snapshot_callback(
                  profile->add_snapshot_callback([state = _state] { state->collect(); })) {}

LanceSessionCacheProfile::~LanceSessionCacheProfile() {
    stop();
}

void LanceSessionCacheProfile::start() {
    // First-use Session initialization may perform I/O. A profile snapshot must not wait
    // for it; the dataset-open window starts only after its baseline is ready.
    LanceSessionManager::CacheStatistics previous;
    const bool valid = _state->snapshot(&previous).ok();
    std::lock_guard<std::mutex> guard(_state->mutex);
    _state->active = true;
    _state->valid = valid;
    _state->previous = previous;
    if (!_state->valid) {
        _state->complete = false;
        _state->profile->add_info_string("LanceSessionCacheStatsAvailable", "false");
    }
}

void LanceSessionCacheProfile::stop() {
    std::lock_guard<std::mutex> guard(_state->mutex);
    _state->collect_locked();
    _state->active = false;
}

} // namespace doris::format::lance
