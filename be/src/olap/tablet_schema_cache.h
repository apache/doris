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

#include <gen_cpp/olap_file.pb.h>

#include <memory>
#include <mutex>
#include <unordered_map>

#include "olap/tablet_schema.h"

namespace doris {

class TabletSchemaCache {
public:
    static void create_global_schema_cache() {
        DCHECK(_s_instance == nullptr);
        static TabletSchemaCache instance;
        _s_instance = &instance;
        std::thread t(&TabletSchemaCache::_recycle, _s_instance);
        t.detach();
    }

    static TabletSchemaCache* instance() { return _s_instance; }

    TabletSchemaSPtr insert(const std::string& key) {
        DCHECK(_s_instance != nullptr);
        std::lock_guard guard(_mtx);
        auto iter = _cache.find(key);
        if (iter == _cache.end()) {
            TabletSchemaSPtr tablet_schema_ptr = std::make_shared<TabletSchema>();
            TabletSchemaPB pb;
            pb.ParseFromString(key);
            tablet_schema_ptr->init_from_pb(pb);
            _cache[key] = tablet_schema_ptr;
            return tablet_schema_ptr;
        }
        return iter->second;
    }

private:
    /**
     * @brief recycle when TabletSchemaSPtr use_count equals 1.
     */
    void _recycle() {
        int64_t tablet_schema_cache_recycle_interval = 86400; // s, one day
        for (;;) {
            std::this_thread::sleep_for(std::chrono::seconds(tablet_schema_cache_recycle_interval));
            std::lock_guard guard(_mtx);
            for (auto iter = _cache.begin(), last = _cache.end(); iter != last;) {
                if (iter->second.unique()) {
                    iter = _cache.erase(iter);
                } else {
                    ++iter;
                }
            }
        }
    }

private:
    static inline TabletSchemaCache* _s_instance = nullptr;
    std::mutex _mtx;
    std::unordered_map<std::string, TabletSchemaSPtr> _cache;
};

} // namespace doris