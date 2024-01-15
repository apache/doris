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

#include <fmt/core.h>
#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <type_traits>
#include <vector>

#include "common/logging.h"
#include "olap/iterators.h"
#include "olap/olap_common.h"
#include "olap/schema.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "util/time.h"

namespace doris {

namespace segment_v2 {
class Segment;
class SegmentIterator;
using SegmentIteratorUPtr = std::unique_ptr<SegmentIterator>;
} // namespace segment_v2

// The SchemaCache is utilized to cache pre-allocated data structures,
// eliminating the need for frequent allocation and deallocation during usage.
// This caching mechanism proves immensely advantageous, particularly in scenarios
// with high concurrency, where queries are executed simultaneously.
class SchemaCache : public LRUCachePolicy {
public:
    enum class Type { TABLET_SCHEMA = 0, SCHEMA = 1 };

    static SchemaCache* instance();

    static void create_global_instance(size_t capacity);

    // get cache schema key, delimiter with SCHEMA_DELIMITER
    static std::string get_schema_key(int32_t tablet_id, const TabletSchemaSPtr& schema,
                                      const std::vector<uint32_t>& column_ids, int32_t version,
                                      Type type);
    static std::string get_schema_key(int32_t tablet_id, const std::vector<TColumn>& columns,
                                      int32_t version, Type type);

    // Get a shared cached schema from cache, schema_key is a subset of column unique ids
    template <typename SchemaType>
    SchemaType get_schema(const std::string& schema_key) {
        if (!instance() || schema_key.empty()) {
            return {};
        }
        auto lru_handle = cache()->lookup(schema_key);
        if (lru_handle) {
            Defer release([cache = cache(), lru_handle] { cache->release(lru_handle); });
            auto value = (CacheValue*)cache()->value(lru_handle);
            value->last_visit_time = UnixMillis();
            VLOG_DEBUG << "use cache schema";
            if constexpr (std::is_same_v<SchemaType, TabletSchemaSPtr>) {
                return value->tablet_schema;
            }
            if constexpr (std::is_same_v<SchemaType, SchemaSPtr>) {
                return value->schema;
            }
        }
        return {};
    }

    // Insert a shared Schema into cache, schema_key is full column unique ids
    template <typename SchemaType>
    void insert_schema(const std::string& key, SchemaType schema) {
        if (!instance() || key.empty()) {
            return;
        }
        CacheValue* value = new CacheValue;
        value->last_visit_time = UnixMillis();
        if constexpr (std::is_same_v<SchemaType, TabletSchemaSPtr>) {
            value->type = Type::TABLET_SCHEMA;
            value->tablet_schema = schema;
        } else if constexpr (std::is_same_v<SchemaType, SchemaSPtr>) {
            value->type = Type::SCHEMA;
            value->schema = schema;
        }
        auto deleter = [](const doris::CacheKey& key, void* value) {
            CacheValue* cache_value = (CacheValue*)value;
            delete cache_value;
        };
        auto lru_handle =
                cache()->insert(key, value, 1, deleter, CachePriority::NORMAL, schema->mem_size());
        cache()->release(lru_handle);
    }

    // Try to prune the cache if expired.
    Status prune();

    struct CacheValue : public LRUCacheValueBase {
        Type type;
        // either tablet_schema or schema
        TabletSchemaSPtr tablet_schema = nullptr;
        SchemaSPtr schema = nullptr;
    };

    SchemaCache(size_t capacity)
            : LRUCachePolicy(CachePolicy::CacheType::SCHEMA_CACHE, capacity, LRUCacheType::NUMBER,
                             config::schema_cache_sweep_time_sec) {}

private:
    static constexpr char SCHEMA_DELIMITER = '-';
};

} // namespace doris