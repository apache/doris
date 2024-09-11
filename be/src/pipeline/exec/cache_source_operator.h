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
#include "operator.h"
#include "pipeline/query_cache/query_cache.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

namespace pipeline {
class DataQueue;

class CacheSourceOperatorX;
class CacheSourceLocalState final : public PipelineXLocalState<CacheSharedState> {
public:
    ENABLE_FACTORY_CREATOR(CacheSourceLocalState);
    using Base = PipelineXLocalState<CacheSharedState>;
    using Parent = CacheSourceOperatorX;
    CacheSourceLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {};

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;

    [[nodiscard]] std::string debug_string(int indentation_level = 0) const override;

private:
    friend class CacheSourceOperatorX;
    friend class OperatorX<CacheSourceLocalState>;

    QueryCache* _global_cache = QueryCache::instance();

    std::string _cache_key {};
    int64_t _version = 0;
    std::vector<vectorized::BlockUPtr> _local_cache_blocks;
    std::vector<int> _slot_orders;
    size_t _current_query_cache_bytes = 0;
    size_t _current_query_cache_rows = 0;
    bool _need_insert_cache = true;

    QueryCacheHandle _query_cache_handle;
    std::vector<vectorized::BlockUPtr>* _hit_cache_results = nullptr;
    std::vector<int> _hit_cache_column_orders;
    int _hit_cache_pos = 0;
};

class CacheSourceOperatorX final : public OperatorX<CacheSourceLocalState> {
public:
    using Base = OperatorX<CacheSourceLocalState>;
    CacheSourceOperatorX(ObjectPool* pool, int plan_node_id, int operator_id,
                         const TQueryCacheParam& cache_param)
            : Base(pool, plan_node_id, operator_id), _cache_param(cache_param) {
        _op_name = "CACHE_SOURCE_OPERATOR";
    };
    ~CacheSourceOperatorX() override = default;
    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    Status open(RuntimeState* state) override {
        static_cast<void>(Base::open(state));
        return Status::OK();
    }

    const RowDescriptor& intermediate_row_desc() const override {
        return _child->intermediate_row_desc();
    }
    RowDescriptor& row_descriptor() override { return _child->row_descriptor(); }
    const RowDescriptor& row_desc() const override { return _child->row_desc(); }

private:
    TQueryCacheParam _cache_param;
    bool _has_data(RuntimeState* state) const {
        auto& local_state = get_local_state(state);
        return local_state._shared_state->data_queue.remaining_has_data();
    }
    friend class CacheSourceLocalState;
};

} // namespace pipeline
} // namespace doris
