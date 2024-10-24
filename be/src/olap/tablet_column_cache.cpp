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

#include "olap/tablet_column_cache.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>

#include "olap/tablet_schema.h"

namespace doris {

TabletColumnPtr TabletColumnCache::insert(const std::string& key) {
    auto* lru_handle = lookup(key);
    TabletColumnPtr tablet_column_ptr;
    if (lru_handle) {
        auto* value = (CacheValue*)LRUCachePolicy::value(lru_handle);
        tablet_column_ptr = value->tablet_column;
        LOG(INFO) << "reuse column " << tablet_column_ptr->name();
    } else {
        auto* value = new CacheValue;
        tablet_column_ptr = std::make_shared<TabletColumn>();
        ColumnPB pb;
        pb.ParseFromString(key);
        tablet_column_ptr->init_from_pb(pb);
        value->tablet_column = tablet_column_ptr;
        lru_handle =
                LRUCachePolicy::insert(key, value, sizeof(TabletColumn), 0, CachePriority::NORMAL);
    }
    DCHECK(lru_handle != nullptr);
    release(lru_handle);
    return tablet_column_ptr;
}

void TabletColumnCache::release(Cache::Handle* lru_handle) {
    LRUCachePolicy::release(lru_handle);
}

TabletColumnCache::CacheValue::~CacheValue() = default;

} // namespace doris
