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

#include <memory>

#include "butil/containers/doubly_buffered_data.h"
#include "common/status.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/int128.h"
#include "olap/tablet.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {

// For caching point lookup pre allocted blocks and exprs
class Reusable {
public:
    ~Reusable();

    bool is_expired(int64_t ttl_ms) const {
        return butil::gettimeofday_ms() - _create_timestamp > ttl_ms;
    }

    Status init(const TDescriptorTable& t_desc_tbl, const std::vector<TExpr>& output_exprs,
                size_t block_size = 1);

    RuntimeState* runtime_state() { return _runtime_state.get(); }

    std::unique_ptr<vectorized::Block> get_block();

    // do not touch block after returned
    void return_block(std::unique_ptr<vectorized::Block>& block);

    TupleDescriptor* tuple_desc() { return _desc_tbl->get_tuple_descriptor(0); }

    const std::vector<vectorized::VExprContext*>& output_exprs() { return _output_exprs_ctxs; }

private:
    // caching TupleDescriptor, output_expr, etc...
    std::unique_ptr<RuntimeState> _runtime_state;
    DescriptorTbl* _desc_tbl;
    std::mutex _block_mutex;
    // prevent from allocte too many tmp blocks
    std::vector<std::unique_ptr<vectorized::Block>> _block_pool;
    std::vector<vectorized::VExprContext*> _output_exprs_ctxs;
    int64_t _create_timestamp = 0;
};

// A cache used for prepare stmt.
// One connection per stmt perf uuid
// Use DoublyBufferedData to wrap Cache for performance and thread safe,
// since it's not barely modified
class LookupCache {
public:
    // uuid to reusable
    using Cache = phmap::flat_hash_map<uint128, std::shared_ptr<Reusable>>;
    using CacheIter = Cache::iterator;

    LookupCache() = default;
    static LookupCache& instance() {
        static LookupCache ins;
        return ins;
    }

    void add(uint128 cache_id, std::shared_ptr<Reusable> item) {
        assert(item != nullptr);
        _double_buffer_cache.Modify(update_cache, std::make_pair(cache_id, item));
    }

    // find an item, return null if not exist
    std::shared_ptr<Reusable> get(uint128 cache_id) {
        butil::DoublyBufferedData<Cache>::ScopedPtr s;
        if (_double_buffer_cache.Read(&s) != 0) {
            LOG(WARNING) << "failed to get cache from double buffer data";
            return nullptr;
        }
        auto it = s->find(cache_id);
        if (it != s->end()) {
            return it->second;
        }
        return nullptr;
    }

private:
    butil::DoublyBufferedData<Cache> _double_buffer_cache;
    // 30 seconds for expiring an item
    int32_t _expir_seconds = config::tablet_lookup_cache_clean_interval;

    static size_t update_cache(Cache& old_cache,
                               const std::pair<uint128, std::shared_ptr<Reusable>>& p) {
        old_cache.emplace(p);
        return 1;
    }

    static size_t remove_items(Cache& old_cache, const std::vector<uint128>& keys) {
        for (size_t i = 0; i < keys.size(); ++i) {
            old_cache.erase(keys[i]);
        }
        return 1;
    }

    // Called from StorageEngine::_start_clean_lookup_cache
    friend class StorageEngine;
    void prune() {
        std::vector<uint128> expired_keys;
        {
            butil::DoublyBufferedData<Cache>::ScopedPtr s;
            if (_double_buffer_cache.Read(&s) != 0) {
                return;
            }
            for (auto it = s->begin(); it != s->end(); ++it) {
                if (it->second->is_expired(_expir_seconds * 1000)) {
                    expired_keys.push_back(it->first);
                }
            }
        }

        _double_buffer_cache.Modify(remove_items, expired_keys);
        LOG(INFO) << "prune lookup cache, total " << expired_keys.size() << " expired items";
    }
};

struct Metrics {
    Metrics()
            : init_ns(TUnit::TIME_NS),
              init_key_ns(TUnit::TIME_NS),
              lookup_key_ns(TUnit::TIME_NS),
              lookup_data_ns(TUnit::TIME_NS),
              output_data_ns(TUnit::TIME_NS) {}
    RuntimeProfile::Counter init_ns;
    RuntimeProfile::Counter init_key_ns;
    RuntimeProfile::Counter lookup_key_ns;
    RuntimeProfile::Counter lookup_data_ns;
    RuntimeProfile::Counter output_data_ns;
};

// An util to do tablet lookup
class TabletLookupMetric {
public:
    Status init(const PTabletKeyLookupRequest* request, PTabletKeyLookupResponse* response);

    Status lookup_up();

    std::string print_profile();

private:
    Status _init_keys(const PTabletKeyLookupRequest* request);

    Status _lookup_row_key();

    Status _lookup_row_data();

    Status _output_data();

    PTabletKeyLookupResponse* _response;
    TabletSharedPtr _tablet;
    std::vector<std::string> _primary_keys;
    std::vector<RowLocation> _row_locations;
    std::shared_ptr<Reusable> _reusable;
    std::unique_ptr<vectorized::Block> _result_block;
    Metrics _profile_metrics;
    bool _hit_lookup_cache = false;
    bool _binary_row_format = false;
};

} // namespace doris