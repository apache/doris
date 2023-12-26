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

#include "bvar/bvar.h"

namespace doris {

bvar::Adder<int64_t> g_tablet_schema_cache_count("tablet_schema_cache_count");
bvar::Adder<int64_t> g_tablet_schema_cache_columns_count("tablet_schema_cache_columns_count");

TabletSchemaSPtr TabletSchemaCache::insert(const std::string& key) {
    std::lock_guard guard(_mtx);
    auto iter = _cache.find(key);
    if (iter == _cache.end()) {
        TabletSchemaSPtr tablet_schema_ptr = std::make_shared<TabletSchema>();
        TabletSchemaPB pb;
        pb.ParseFromString(key);
        tablet_schema_ptr->init_from_pb(pb);
        _cache[key] = tablet_schema_ptr;
        g_tablet_schema_cache_count << 1;
        g_tablet_schema_cache_columns_count << tablet_schema_ptr->num_columns();
        return tablet_schema_ptr;
    }
    return iter->second;
}

void TabletSchemaCache::start() {
    std::thread t(&TabletSchemaCache::_recycle, this);
    t.detach();
    LOG(INFO) << "TabletSchemaCache started";
}

void TabletSchemaCache::stop() {
    _should_stop = true;
    while (!_is_stopped) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    LOG(INFO) << "TabletSchemaCache stopped";
}

/**
 * @brief recycle when TabletSchemaSPtr use_count equals 1.
 */
void TabletSchemaCache::_recycle() {
    int64_t check_interval = 5;
    int64_t left_second = config::tablet_schema_cache_recycle_interval;
    while (!_should_stop) {
        if (left_second > 0) {
            std::this_thread::sleep_for(std::chrono::seconds(check_interval));
            left_second -= check_interval;
            continue;
        } else {
            left_second = config::tablet_schema_cache_recycle_interval;
        }

        std::lock_guard guard(_mtx);
        LOG(INFO) << "Tablet Schema Cache Capacity " << _cache.size();
        for (auto iter = _cache.begin(), last = _cache.end(); iter != last;) {
            if (iter->second.unique()) {
                g_tablet_schema_cache_count << -1;
                g_tablet_schema_cache_columns_count << -iter->second->num_columns();
                iter = _cache.erase(iter);
            } else {
                ++iter;
            }
        }
    }
    _is_stopped = true;
}

} // namespace doris
