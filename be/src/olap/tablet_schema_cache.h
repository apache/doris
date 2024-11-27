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

class TabletSchemaCache : public LRUCachePolicyTrackingManual {
public:
    using LRUCachePolicyTrackingManual::insert;

    TabletSchemaCache(size_t capacity)
            : LRUCachePolicyTrackingManual(CachePolicy::CacheType::TABLET_SCHEMA_CACHE, capacity,
                                           LRUCacheType::NUMBER,
                                           config::tablet_schema_cache_recycle_interval) {}

    static TabletSchemaCache* create_global_schema_cache(size_t capacity) {
        auto* res = new TabletSchemaCache(capacity);
        return res;
    }

    static TabletSchemaCache* instance() {
        return ExecEnv::GetInstance()->get_tablet_schema_cache();
    }

    std::pair<Cache::Handle*, TabletSchemaSPtr> insert(const std::string& key);

    void release(Cache::Handle*);

private:
    class CacheValue : public LRUCacheValueBase {
    public:
        ~CacheValue() override;

        TabletSchemaSPtr tablet_schema;
    };
};

} // namespace doris
