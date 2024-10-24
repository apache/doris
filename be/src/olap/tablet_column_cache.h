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

#include "olap/tablet_fwd.h"
#include "runtime/exec_env.h"
#include "runtime/memory/lru_cache_policy.h"

namespace doris {

class TabletColumnCache : public LRUCachePolicy {
public:
    TabletColumnCache(size_t capacity)
            : LRUCachePolicy(CachePolicy::CacheType::TABLET_COLUMN_CACHE, capacity,
                             LRUCacheType::NUMBER, config::tablet_schema_cache_recycle_interval) {}

    static TabletColumnCache* create_global_column_cache(size_t capacity) {
        auto* res = new TabletColumnCache(capacity);
        return res;
    }

    static TabletColumnCache* instance() {
        return ExecEnv::GetInstance()->get_tablet_column_cache();
    }

    TabletColumnPtr insert(const std::string& key);

    void release(Cache::Handle*);

private:
    class CacheValue : public LRUCacheValueBase {
    public:
        ~CacheValue() override;

        TabletColumnPtr tablet_column;
    };
};

} // namespace doris
