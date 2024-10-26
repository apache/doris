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
#include <unordered_map>

#include "runtime/exec_env.h"
#include "runtime/memory/cache_policy.h"
#include "util/runtime_profile.h"
#include "util/time.h"

namespace doris {

// Hold the list of all caches, for prune when memory not enough or timing.
class CacheManager {
public:
    static CacheManager* create_global_instance() { return new CacheManager(); }
    static CacheManager* instance() { return ExecEnv::GetInstance()->get_cache_manager(); }

    void register_cache(CachePolicy* cache) {
        std::lock_guard<std::mutex> l(_caches_lock);
        auto it = _caches.find(cache->type());
        if (it != _caches.end()) {
#ifdef BE_TEST
            _caches.erase(it);
#else
            LOG(FATAL) << "Repeat register cache " << CachePolicy::type_string(cache->type());
#endif // BE_TEST
        }
        _caches.insert({cache->type(), cache});
        LOG(INFO) << "Register Cache " << CachePolicy::type_string(cache->type());
    }

    void unregister_cache(CachePolicy::CacheType type) {
#ifdef BE_TEST
        return;
#endif // BE_TEST
        std::lock_guard<std::mutex> l(_caches_lock);
        auto it = _caches.find(type);
        if (it != _caches.end()) {
            _caches.erase(it);
        }
        LOG(INFO) << "Unregister Cache " << CachePolicy::type_string(type);
    }

    int64_t for_each_cache_prune_stale_wrap(std::function<void(CachePolicy* cache_policy)> func,
                                            RuntimeProfile* profile = nullptr);

    int64_t for_each_cache_prune_stale(RuntimeProfile* profile = nullptr);

    // if force is true, regardless of the two prune interval and cache size, cache will be pruned this time.
    int64_t for_each_cache_prune_all(RuntimeProfile* profile = nullptr, bool force = false);
    int64_t cache_prune_all(CachePolicy::CacheType type, bool force = false);

    bool need_prune(int64_t* last_timestamp, const std::string& type) {
        int64_t now = UnixSeconds();
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

    int64_t for_each_cache_refresh_capacity(double adjust_weighted,
                                            RuntimeProfile* profile = nullptr);

private:
    std::mutex _caches_lock;
    std::unordered_map<CachePolicy::CacheType, CachePolicy*> _caches;
    int64_t _last_prune_stale_timestamp = 0;
    int64_t _last_prune_all_timestamp = 0;
};

} // namespace doris
