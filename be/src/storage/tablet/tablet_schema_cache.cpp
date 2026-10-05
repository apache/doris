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

#include "storage/tablet/tablet_schema_cache.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <json2pb/pb_to_json.h>

#include <cstdint>
#include <type_traits>
#include <utility>

#include "bvar/bvar.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet_info.h"
#include "util/sha.h"

bvar::Adder<int64_t> g_tablet_schema_cache_count("tablet_schema_cache_count");
bvar::Adder<int64_t> g_tablet_schema_cache_columns_count("tablet_schema_cache_columns_count");
bvar::Adder<int64_t> g_tablet_schema_cache_hit_count("tablet_schema_cache_hit_count");

namespace doris {

// to reduce the memory consumption of the serialized TabletSchema as key.
// use sha256 to prevent from hash collision
static std::string get_key_signature(const std::string& origin) {
    SHA256Digest digest;
    digest.reset(origin.data(), origin.length());
    return std::string {digest.digest().data(), digest.digest().length()};
}

template <typename T>
static void append_cache_key_value(std::string* key, T value) {
    static_assert(std::is_integral_v<T>);
    key->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

static void append_cache_key_string(std::string* key, const std::string& value) {
    append_cache_key_value(key, static_cast<uint64_t>(value.size()));
    key->append(value);
}

std::pair<Cache::Handle*, TabletSchemaSPtr> TabletSchemaCache::insert(const std::string& key) {
    std::string key_signature = get_key_signature(key);
    auto* lru_handle = lookup(key_signature);
    TabletSchemaSPtr tablet_schema_ptr;
    if (lru_handle) {
        auto* value = (CacheValue*)LRUCachePolicy::value(lru_handle);
        tablet_schema_ptr = value->tablet_schema;
        g_tablet_schema_cache_hit_count << 1;
    } else {
        auto* value = new CacheValue;
        tablet_schema_ptr = std::make_shared<TabletSchema>();
        TabletSchemaPB pb;
        pb.ParseFromString(key);
        // We should reuse the memory of the same TabletColumn/TabletIndex object, set reuse_cached_column to true
        tablet_schema_ptr->init_from_pb(pb, false, true);
        value->tablet_schema = tablet_schema_ptr;
        lru_handle = LRUCachePolicy::insert(key_signature, value, tablet_schema_ptr->num_columns(),
                                            tablet_schema_ptr->mem_size(), CachePriority::NORMAL);
        g_tablet_schema_cache_count << 1;
        g_tablet_schema_cache_columns_count << tablet_schema_ptr->num_columns();
    }
    DCHECK(lru_handle != nullptr);
    return std::make_pair(lru_handle, tablet_schema_ptr);
}

std::pair<Cache::Handle*, TabletSchemaSPtr> TabletSchemaCache::insert(
        const std::string& key, TabletSchemaSPtr tablet_schema) {
    auto* lru_handle = lookup(key);
    TabletSchemaSPtr tablet_schema_ptr;
    if (lru_handle) {
        auto* value = (CacheValue*)LRUCachePolicy::value(lru_handle);
        tablet_schema_ptr = value->tablet_schema;
        g_tablet_schema_cache_hit_count << 1;
    } else {
        DCHECK(tablet_schema != nullptr);
        auto* value = new CacheValue;
        tablet_schema_ptr = std::move(tablet_schema);
        value->tablet_schema = tablet_schema_ptr;
        lru_handle = LRUCachePolicy::insert(key, value, tablet_schema_ptr->num_columns(),
                                            tablet_schema_ptr->mem_size(), CachePriority::NORMAL);
        g_tablet_schema_cache_count << 1;
        g_tablet_schema_cache_columns_count << tablet_schema_ptr->num_columns();
    }
    DCHECK(lru_handle != nullptr);
    return std::make_pair(lru_handle, tablet_schema_ptr);
}

std::pair<Cache::Handle*, TabletSchemaSPtr> TabletSchemaCache::lookup_schema(
        const std::string& key) {
    auto* lru_handle = lookup(key);
    if (lru_handle == nullptr) {
        return {nullptr, nullptr};
    }
    auto* value = (CacheValue*)LRUCachePolicy::value(lru_handle);
    g_tablet_schema_cache_hit_count << 1;
    return {lru_handle, value->tablet_schema};
}

std::string TabletSchemaCache::build_load_schema_cache_key(
        int64_t index_id, const OlapTableSchemaParam* table_schema_param,
        const TabletSchema& ori_tablet_schema, const OlapTableIndexSchema* index_schema) {
    DCHECK(table_schema_param != nullptr);
    std::string cache_key;
    cache_key.append("load_schema_v2");
    append_cache_key_value(&cache_key, index_id);
    append_cache_key_value(&cache_key, table_schema_param->table_id());
    append_cache_key_value(&cache_key, table_schema_param->db_id());
    append_cache_key_value(&cache_key, table_schema_param->version());

    TabletSchemaPB ori_schema_pb;
    ori_tablet_schema.to_schema_pb(&ori_schema_pb);
    append_cache_key_string(&cache_key,
                            TabletSchema::deterministic_string_serialize(ori_schema_pb));
    if (ori_tablet_schema.num_variant_columns() > 0) {
        // Variant schemas carry path set info outside TabletSchemaPB, so do not share them
        // across different source TabletSchema objects unless that metadata is serialized.
        append_cache_key_value(&cache_key, reinterpret_cast<uintptr_t>(&ori_tablet_schema));
    }

    std::string auto_increment_column;
    if (table_schema_param->is_partial_update()) {
        auto_increment_column = table_schema_param->auto_increment_coulumn();
    }
    append_cache_key_string(&cache_key, auto_increment_column);

    const bool has_current_schema = index_schema != nullptr && !index_schema->columns.empty() &&
                                    index_schema->columns[0]->unique_id() >= 0;
    append_cache_key_value(&cache_key, has_current_schema);
    if (index_schema != nullptr) {
        POlapTableIndexSchema index_schema_pb;
        index_schema->to_protobuf(&index_schema_pb);
        append_cache_key_string(&cache_key,
                                TabletSchema::deterministic_string_serialize(index_schema_pb));
    }
    return get_key_signature(cache_key);
}

void TabletSchemaCache::release(Cache::Handle* lru_handle) {
    LRUCachePolicy::release(lru_handle);
}

TabletSchemaCache::CacheValue::~CacheValue() {
    g_tablet_schema_cache_count << -1;
    g_tablet_schema_cache_columns_count << -tablet_schema->num_columns();
}

} // namespace doris
