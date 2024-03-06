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

#include <string>

#include "runtime/exec_env.h"
#include "runtime/memory/cache_policy.h"
#include "util/runtime_profile.h"
#include "util/time.h"

namespace doris {

// Hold the list of all caches, for prune when memory not enough or timing.
class CacheManager {
public:
    static CacheManager* create_global_instance() {
        CacheManager* res = new CacheManager();
        return res;
    }
    static CacheManager* instance() { return ExecEnv::GetInstance()->get_cache_manager(); }

    std::list<CachePolicy*>::iterator register_cache(CachePolicy* cache) {
        std::lock_guard<std::mutex> l(_caches_lock);
        return _caches.insert(_caches.end(), cache);
    }

    void unregister_cache(std::list<CachePolicy*>::iterator it) {
        std::lock_guard<std::mutex> l(_caches_lock);
        if (it != _caches.end()) {
            _caches.erase(it);
            it = _caches.end();
        }
    }

    int64_t for_each_cache_prune_stale_wrap(std::function<void(CachePolicy* cache_policy)> func,
                                            RuntimeProfile* profile = nullptr);

    int64_t for_each_cache_prune_stale(RuntimeProfile* profile = nullptr);

    int64_t for_each_cache_prune_all(RuntimeProfile* profile = nullptr);

    void clear_once(CachePolicy::CacheType type);

    bool need_prune(int64_t* last_timestamp, const std::string& type) {
        int64_t now = UnixSeconds();
        std::lock_guard<std::mutex> l(_caches_lock);
        if (now - *last_timestamp > config::cache_prune_interval_sec) {
            *last_timestamp = now;
            return true;
        }
        LOG(INFO) << fmt::format(
                "[MemoryGC] cache no prune {}, last prune less than interval {}, now {}, last "
                "timestamp {}",
                type, config::cache_prune_interval_sec, now, *last_timestamp);
        return false;
    }

private:
    std::mutex _caches_lock;
    std::list<CachePolicy*> _caches;
    int64_t _last_prune_stale_timestamp = 0;
    int64_t _last_prune_all_timestamp = 0;
};

} // namespace doris
