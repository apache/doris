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

#include "olap/tablet_column_object_pool.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>

#include "olap/tablet_schema.h"

namespace doris {

bvar::Adder<int64_t> g_tablet_column_cache_count("tablet_column_cache_count");
bvar::Adder<int64_t> g_tablet_column_cache_hit_count("tablet_column_cache_hit_count");
bvar::Adder<int64_t> g_tablet_index_cache_count("tablet_index_cache_count");
bvar::Adder<int64_t> g_tablet_index_cache_hit_count("tablet_index_cache_hit_count");

template <typename T, typename CacheValueType>
std::pair<Cache::Handle*, std::shared_ptr<T>> insert_impl(
        TabletColumnObjectPool* pool, const std::string& key, const char* type_name,
        bvar::Adder<int64_t>& cache_counter, bvar::Adder<int64_t>& hit_counter,
        std::function<void(std::shared_ptr<T>, const std::string&)> init_from_pb) {
    auto* lru_handle = pool->lookup(key);
    std::shared_ptr<T> obj_ptr;

    if (lru_handle) {
        auto* value = reinterpret_cast<CacheValueType*>(pool->value(lru_handle));
        obj_ptr = value->value;
        VLOG_DEBUG << "reuse " << type_name;
        hit_counter << 1;
    } else {
        auto* value = new CacheValueType;
        obj_ptr = std::make_shared<T>();
        init_from_pb(obj_ptr, key);
        VLOG_DEBUG << "create " << type_name;
        value->value = obj_ptr;
        lru_handle = pool->LRUCachePolicy::insert(key, value, 1, 0, CachePriority::NORMAL);
        cache_counter << 1;
    }

    DCHECK(lru_handle != nullptr);
    return {lru_handle, obj_ptr};
}

std::pair<Cache::Handle*, TabletColumnPtr> TabletColumnObjectPool::insert(const std::string& key) {
    return insert_impl<TabletColumn, CacheValue>(this, key, "column", g_tablet_column_cache_count,
                                                 g_tablet_column_cache_hit_count,
                                                 [](auto&& column, auto&& key) {
                                                     ColumnPB pb;
                                                     pb.ParseFromString(key);
                                                     column->init_from_pb(pb);
                                                 });
}

std::pair<Cache::Handle*, TabletIndexPtr> TabletColumnObjectPool::insert_index(
        const std::string& key) {
    return insert_impl<TabletIndex, IndexCacheValue>(this, key, "index", g_tablet_index_cache_count,
                                                     g_tablet_index_cache_hit_count,
                                                     [](auto&& index, auto&& key) {
                                                         TabletIndexPB index_pb;
                                                         index_pb.ParseFromString(key);
                                                         index->init_from_pb(index_pb);
                                                     });
}

template <typename T>
TabletColumnObjectPool::BaseCacheValue<T>::BaseCacheValue::~BaseCacheValue() {
    if constexpr (std::is_same_v<T, TabletColumn>) {
        g_tablet_column_cache_count << -1;
    } else {
        g_tablet_index_cache_count << -1;
    }
}

template struct TabletColumnObjectPool::BaseCacheValue<TabletColumn>;
template struct TabletColumnObjectPool::BaseCacheValue<TabletIndex>;

} // namespace doris
