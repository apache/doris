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
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "runtime/memory/lru_cache_policy.h"

namespace doris {

// TabletColumnObjectPool is a cache for TabletColumn objects. It is used to reduce memory consumption
// when there are a large number of identical TabletColumns in the cluster, which usually occurs
// when VARIANT type columns are modified and added, each Rowset has an individual TabletSchema.
// Excessive TabletSchemas can lead to significant memory overhead. Reusing memory for identical
// TabletColumns would greatly reduce this memory consumption.

class TabletColumnObjectPool : public LRUCachePolicy {
public:
    TabletColumnObjectPool(size_t capacity)
            : LRUCachePolicy(CachePolicy::CacheType::TABLET_COLUMN_OBJECT_POOL, capacity,
                             LRUCacheType::NUMBER, config::tablet_schema_cache_recycle_interval) {}

    static TabletColumnObjectPool* create_global_column_cache(size_t capacity) {
        auto* res = new TabletColumnObjectPool(capacity);
        return res;
    }

    static TabletColumnObjectPool* instance() {
        return ExecEnv::GetInstance()->get_tablet_column_object_pool();
    }

    std::pair<Cache::Handle*, TabletColumnPtr> insert(const std::string& key);

private:
    class CacheValue : public LRUCacheValueBase {
    public:
        ~CacheValue() override;
        TabletColumnPtr tablet_column;
    };
};

} // namespace doris
