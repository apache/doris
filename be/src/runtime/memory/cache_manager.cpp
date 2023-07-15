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

#include "runtime/memory/cache_manager.h"

#include "runtime/memory/cache_policy.h"
#include "util/runtime_profile.h"

namespace doris {

int64_t CacheManager::for_each_cache_prune_stale_wrap(
        std::function<void(CachePolicy* cache_policy)> func, RuntimeProfile* profile) {
    int64_t freed_size = 0;
    std::lock_guard<std::mutex> l(_caches_lock);
    for (auto cache_policy : _caches) {
        func(cache_policy);
        freed_size += cache_policy->profile()->get_counter("FreedMemory")->value();
        if (cache_policy->profile()->get_counter("FreedMemory")->value() != 0 && profile) {
            profile->add_child(cache_policy->profile(), true, nullptr);
        }
    }
    return freed_size;
}

int64_t CacheManager::for_each_cache_prune_stale(RuntimeProfile* profile) {
    return for_each_cache_prune_stale_wrap(
            [](CachePolicy* cache_policy) { cache_policy->prune_stale(); }, profile);
}

int64_t CacheManager::for_each_cache_prune_all(RuntimeProfile* profile) {
    return for_each_cache_prune_stale_wrap(
            [](CachePolicy* cache_policy) { cache_policy->prune_all(); }, profile);
}

} // namespace doris
