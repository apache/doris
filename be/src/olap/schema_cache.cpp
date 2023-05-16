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

#include "olap/schema_cache.h"

#include <butil/logging.h>
#include <fmt/core.h>
#include <gen_cpp/Descriptors_types.h>
#include <glog/logging.h>

#include <memory>
#include <mutex>
#include <vector>

#include "common/config.h"
#include "olap/schema.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris {

SchemaCache* SchemaCache::_s_instance = nullptr;

std::string SchemaCache::get_schema_key(int32_t tablet_id, const TabletSchemaSPtr& schema,
                                        const std::vector<uint32_t>& column_ids, Type type) {
    std::string key = fmt::format("{}-", tablet_id);
    std::for_each(column_ids.begin(), column_ids.end(), [&](const ColumnId& cid) {
        uint32_t col_unique_id = schema->column(cid).unique_id();
        key.append(fmt::format("{}", col_unique_id));
        key.append("-");
    });
    key.append(fmt::format("{}", type));
    return key;
}

std::string SchemaCache::get_schema_key(int32_t tablet_id, const std::vector<TColumn>& columns,
                                        Type type) {
    std::string key = fmt::format("{}-", tablet_id);
    std::for_each(columns.begin(), columns.end(), [&](const TColumn& col) {
        key.append(fmt::format("{}", col.col_unique_id));
        key.append("-");
    });
    key.append(fmt::format("{}", type));
    return key;
}

void SchemaCache::create_global_instance(size_t capacity) {
    DCHECK(_s_instance == nullptr);
    static SchemaCache instance(capacity);
    _s_instance = &instance;
}

SchemaCache::SchemaCache(size_t capacity) {
    _schema_cache =
            std::unique_ptr<Cache>(new_lru_cache("SchemaCache", capacity, LRUCacheType::NUMBER));
}

Status SchemaCache::prune() {
    const int64_t curtime = UnixMillis();
    auto pred = [curtime](const void* value) -> bool {
        CacheValue* cache_value = (CacheValue*)value;
        return (cache_value->last_visit_time + config::schema_cache_sweep_time_sec * 1000) <
               curtime;
    };

    MonotonicStopWatch watch;
    watch.start();
    // Prune cache in lazy mode to save cpu and minimize the time holding write lock
    int64_t prune_num = _schema_cache->prune_if(pred, true);
    LOG(INFO) << "prune " << prune_num
              << " entries in SchemaCache cache. cost(ms): " << watch.elapsed_time() / 1000 / 1000;
    return Status::OK();
}

} // namespace doris