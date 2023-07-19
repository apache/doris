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

#include "runtime/memory/cache_manager.h"
#include "util/runtime_profile.h"

namespace doris {

static constexpr int32_t CACHE_MIN_FREE_SIZE = 67108864; // 64M

// Base of all caches. register to CacheManager when cache is constructed.
class CachePolicy {
public:
    CachePolicy(const std::string& name, uint32_t stale_sweep_time_s)
            : _name(name), _stale_sweep_time_s(stale_sweep_time_s) {
        _it = CacheManager::instance()->register_cache(this);
        init_profile();
    }

    virtual ~CachePolicy() { CacheManager::instance()->unregister_cache(_it); };
    virtual void prune_stale() = 0;
    virtual void prune_all() = 0;

    RuntimeProfile* profile() { return _profile.get(); }

protected:
    void init_profile() {
        _profile = std::make_unique<RuntimeProfile>(fmt::format("Cache name={}", _name));
        _prune_stale_number_counter = ADD_COUNTER(_profile, "PruneStaleNumber", TUnit::UNIT);
        _prune_all_number_counter = ADD_COUNTER(_profile, "PruneAllNumber", TUnit::UNIT);
        _freed_memory_counter = ADD_COUNTER(_profile, "FreedMemory", TUnit::BYTES);
        _freed_entrys_counter = ADD_COUNTER(_profile, "FreedEntrys", TUnit::UNIT);
        _cost_timer = ADD_TIMER(_profile, "CostTime");
    }

    std::string _name;
    std::list<CachePolicy*>::iterator _it;

    std::unique_ptr<RuntimeProfile> _profile;
    RuntimeProfile::Counter* _prune_stale_number_counter = nullptr;
    RuntimeProfile::Counter* _prune_all_number_counter = nullptr;
    // Reset before each gc
    RuntimeProfile::Counter* _freed_memory_counter = nullptr;
    RuntimeProfile::Counter* _freed_entrys_counter = nullptr;
    RuntimeProfile::Counter* _cost_timer = nullptr;

    uint32_t _stale_sweep_time_s;
};

} // namespace doris
