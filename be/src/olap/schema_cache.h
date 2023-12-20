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
} // namespace segment_v2

// The SchemaCache is utilized to cache pre-allocated data structures,
// eliminating the need for frequent allocation and deallocation during usage.
// This caching mechanism proves immensely advantageous, particularly in scenarios
// with high concurrency, where queries are executed simultaneously.
class SchemaCache : public LRUCachePolicy {
public:
    enum class Type { TABLET_SCHEMA = 0, SCHEMA = 1, SCHEMA_PB = 2 };

    static SchemaCache* instance();

    // get cache schema key, delimiter with SCHEMA_DELIMITER
    static std::string get_schema_key(int32_t tablet_id, const TabletSchemaSPtr& schema,
                                      const std::vector<uint32_t>& column_ids, int32_t version,
                                      Type type);

    static int32_t get_unique_id(const TColumn& col) { return col.col_unique_id; }

    static int32_t get_unique_id(const TabletColumn& col) { return col.unique_id(); }

    // format: tabletId-unique_id1-uniqueid2...-version-type
    template <typename T>
    static std::string get_schema_key(int32_t tablet_id, const std::vector<T>& columns,
                                      int32_t version, Type type) {
        if (columns.empty() || get_unique_id(columns[0]) < 0) {
            return "";
        }
        std::string key = fmt::format("{}-", tablet_id);
        std::for_each(columns.begin(), columns.end(), [&](const T& col) {
            key.append(fmt::format("{}", get_unique_id(col)));
            key.append("-");
        });
        key.append(fmt::format("{}-{}", version, type));
        return key;
    }

    // Get a shared cached schema from cache, schema_key is a subset of column unique ids
    template <typename SchemaType>
    SchemaType get_schema(const std::string& schema_key) {
        if (!instance() || schema_key.empty()) {
            return {};
        }
        auto* lru_handle = _cache->lookup(schema_key);
        if (lru_handle) {
            Defer release([cache = _cache.get(), lru_handle] { cache->release(lru_handle); });
            auto* value = (CacheValue*)_cache->value(lru_handle);
            value->last_visit_time = UnixMillis();
            VLOG_DEBUG << "use cache schema";
            if constexpr (std::is_same_v<SchemaType, TabletSchemaSPtr>) {
                return value->tablet_schema;
            }
            if constexpr (std::is_same_v<SchemaType, SchemaSPtr>) {
                return value->schema;
            }
            if constexpr (std::is_same_v<SchemaType, std::shared_ptr<TabletSchemaPB>>) {
                VLOG_DEBUG << "get value->schema_pb, address:" << value->schema_pb;
                return value->schema_pb;
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
        auto* value = new CacheValue;
        value->last_visit_time = UnixMillis();
        if constexpr (std::is_same_v<SchemaType, TabletSchemaSPtr>) {
            value->type = Type::TABLET_SCHEMA;
            value->tablet_schema = schema;
        } else if constexpr (std::is_same_v<SchemaType, SchemaSPtr>) {
            value->type = Type::SCHEMA;
            value->schema = schema;
        } else if constexpr (std::is_same_v<SchemaType, std::shared_ptr<TabletSchemaPB>>) {
            value->type = Type::SCHEMA_PB;
            value->schema_pb = schema;
            VLOG_DEBUG << "set value->schema_pb, address:" << value->schema_pb;
        }
        auto deleter = [](const doris::CacheKey& key, void* value) {
            auto* cache_value = (CacheValue*)value;
            delete cache_value;
        };
        int64_t mem_size = 0;
        if constexpr (std::is_same_v<SchemaType, std::shared_ptr<TabletSchemaPB>>) {
            mem_size = schema->ByteSizeLong();
        } else {
            mem_size = schema->mem_size();
        }
        auto lru_handle = _cache->insert(key, value, 1, deleter, CachePriority::NORMAL, mem_size);
        _cache->release(lru_handle);
    }

    struct CacheValue : public LRUCacheValueBase {
        Type type;
        // either tablet_schema, schema or schema_pb
        TabletSchemaSPtr tablet_schema = nullptr;
        SchemaSPtr schema = nullptr;
        std::shared_ptr<TabletSchemaPB> schema_pb = nullptr;
    };

    SchemaCache(size_t capacity)
            : LRUCachePolicy(CachePolicy::CacheType::SCHEMA_CACHE, capacity, LRUCacheType::NUMBER,
                             config::schema_cache_sweep_time_sec) {}

private:
    static constexpr char SCHEMA_DELIMITER = '-';
};

} // namespace doris