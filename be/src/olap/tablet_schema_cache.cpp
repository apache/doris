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

#include "olap/tablet_schema_cache.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <json2pb/pb_to_json.h>

#include "bvar/bvar.h"
#include "olap/tablet_schema.h"
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
        // We should reuse the memory of the same TabletColumn object, set reuse_cached_column to true
        tablet_schema_ptr->init_from_pb(pb, false, true);
        value->tablet_schema = tablet_schema_ptr;
        lru_handle = LRUCachePolicy::insert(key_signature, value, tablet_schema_ptr->num_columns(),
                                            0, CachePriority::NORMAL);
        g_tablet_schema_cache_count << 1;
        g_tablet_schema_cache_columns_count << tablet_schema_ptr->num_columns();
    }
    DCHECK(lru_handle != nullptr);
    return std::make_pair(lru_handle, tablet_schema_ptr);
}

void TabletSchemaCache::release(Cache::Handle* lru_handle) {
    LRUCachePolicy::release(lru_handle);
}

TabletSchemaCache::CacheValue::~CacheValue() {
    g_tablet_schema_cache_count << -1;
    g_tablet_schema_cache_columns_count << -tablet_schema->num_columns();
}

} // namespace doris
