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

#include "olap/uncommitted_rowset_registry.h"

#include <algorithm>

#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/config.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/time.h"

namespace doris {

void UncommittedRowsetRegistry::register_rowset(std::shared_ptr<UncommittedRowsetEntry> entry) {
    entry->register_time_ms = MonotonicMillis();
    auto& shard = _get_shard(entry->tablet_id);
    std::lock_guard wlock(shard.lock);
    shard.entries[entry->tablet_id].push_back(std::move(entry));
}

void UncommittedRowsetRegistry::unregister_rowset(int64_t tablet_id, int64_t transaction_id) {
    auto& shard = _get_shard(tablet_id);
    std::lock_guard wlock(shard.lock);

    auto it = shard.entries.find(tablet_id);
    if (it == shard.entries.end()) {
        return;
    }

    auto& entries = it->second;
    entries.erase(std::remove_if(entries.begin(), entries.end(),
                                  [transaction_id](const auto& e) {
                                      return e->transaction_id == transaction_id;
                                  }),
                  entries.end());

    if (entries.empty()) {
        shard.entries.erase(it);
    }
}

void UncommittedRowsetRegistry::get_uncommitted_rowsets(
        int64_t tablet_id, std::vector<std::shared_ptr<UncommittedRowsetEntry>>* result) {
    auto& shard = _get_shard(tablet_id);
    std::shared_lock rlock(shard.lock);

    auto it = shard.entries.find(tablet_id);
    if (it == shard.entries.end()) {
        return;
    }

    for (const auto& entry : it->second) {
        result->push_back(entry);
    }
}

void UncommittedRowsetRegistry::remove_expired_entries() {
    int64_t expire_ms = config::uncommitted_rowset_expire_sec * 1000L;
    int64_t now = MonotonicMillis();

    for (int i = 0; i < SHARD_COUNT; i++) {
        auto& shard = _shards[i];
        std::lock_guard wlock(shard.lock);
        for (auto it = shard.entries.begin(); it != shard.entries.end();) {
            auto& entries = it->second;
            entries.erase(std::remove_if(entries.begin(), entries.end(),
                                          [now, expire_ms](const auto& e) {
                                              return now - e->register_time_ms > expire_ms;
                                          }),
                          entries.end());
            if (entries.empty()) {
                it = shard.entries.erase(it);
            } else {
                ++it;
            }
        }
    }
}

UncommittedRowsetRegistry* get_uncommitted_rowset_registry() {
    auto* env = ExecEnv::GetInstance();
    if (!env) {
        return nullptr;
    }
    if (config::is_cloud_mode()) {
        return env->storage_engine().to_cloud().uncommitted_rowset_registry();
    } else {
        return env->storage_engine().to_local().uncommitted_rowset_registry();
    }
}

} // namespace doris
