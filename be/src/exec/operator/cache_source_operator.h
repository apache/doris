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

#include <stdint.h>

#include <memory>

#include "common/status.h"
#include "exec/operator/operator.h"
#include "runtime/query_cache/query_cache.h"

namespace doris {
class RuntimeState;

class Block;

class DataQueue;

class CacheSourceOperatorX;
class CacheSourceLocalState final : public PipelineXLocalState<DataQueueSharedState> {
public:
    ENABLE_FACTORY_CREATOR(CacheSourceLocalState);
    using Base = PipelineXLocalState<DataQueueSharedState>;
    using Parent = CacheSourceOperatorX;
    CacheSourceLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {};

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;

    [[nodiscard]] std::string debug_string(int indentation_level = 0) const override;

private:
    friend class CacheSourceOperatorX;
    friend class OperatorX<CacheSourceLocalState>;

    // Account `rows`/`bytes` against the entry limits. Once the limits are
    // exceeded, drop the pending write-back blocks and stop caching (the data
    // itself still passes through to the parent). Returns whether the entry is
    // still cacheable.
    bool _account_write_back(int64_t rows, int64_t bytes);
    // Snapshot `block` (already reordered to this query's slot orders) into
    // the write-back set as a zero-copy view sharing its COW columns. Used in
    // INCREMENTAL mode for the cached blocks, whose columns belong to the
    // entry the decision pins: the write-back set then costs no extra memory
    // until QueryCache::insert() performs the single cache-owned
    // materialization, so a replacement peaks at old entry + new entry
    // instead of three full payloads.
    Status _append_block_for_write_back(const Block& block);

    QueryCache* _global_cache = QueryCache::instance();

    std::string _cache_key {};
    int64_t _version = 0;
    std::vector<BlockUPtr> _local_cache_blocks;
    std::vector<int> _slot_orders;
    int64_t _current_query_cache_bytes = 0;
    int64_t _current_query_cache_rows = 0;
    bool _need_insert_cache = true;

    // The per-instance cache decision shared with the olap scan operator of the
    // same fragment (both observe the same object, so they can never disagree).
    // It also pins the reused cache entry for the lifetime of this query.
    std::shared_ptr<QueryCacheInstanceDecision> _cache_decision;
    // Shortcut for _cache_decision->mode == INCREMENTAL: after the cached
    // blocks are emitted, the delta partial result is drained from the data
    // queue, and both are written back as the merged entry.
    bool _is_incremental = false;
    // delta_count to write back: 0 for a full recompute, cached + 1 for an
    // incremental merge (drives periodic compaction, see QueryCacheRuntime).
    int64_t _insert_delta_count = 0;

    std::vector<BlockUPtr>* _hit_cache_results = nullptr;
    std::vector<int> _hit_cache_column_orders;
    int _hit_cache_pos = 0;
};

class CacheSourceOperatorX final : public OperatorX<CacheSourceLocalState> {
public:
    using Base = OperatorX<CacheSourceLocalState>;
    CacheSourceOperatorX(ObjectPool* pool, int plan_node_id, int operator_id,
                         const TQueryCacheParam& cache_param,
                         std::shared_ptr<QueryCacheRuntime> query_cache_runtime)
            : Base(pool, plan_node_id, operator_id),
              _cache_param(cache_param),
              _query_cache_runtime(std::move(query_cache_runtime)) {
        _op_name = "CACHE_SOURCE_OPERATOR";
    };

#ifdef BE_TEST
    CacheSourceOperatorX() = default;
#endif

    ~CacheSourceOperatorX() override = default;
    Status get_block_impl(RuntimeState* state, Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    const RowDescriptor& intermediate_row_desc() const override {
        return _child->intermediate_row_desc();
    }
    RowDescriptor& row_descriptor() override { return _child->row_descriptor(); }
    const RowDescriptor& row_desc() const override { return _child->row_desc(); }

private:
    TQueryCacheParam _cache_param;
    std::shared_ptr<QueryCacheRuntime> _query_cache_runtime;
    friend class CacheSourceLocalState;
};

} // namespace doris
