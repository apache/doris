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

#include "common/exception.h"
#include "common/status.h"
#include "exprs/runtime_filter.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h" // IWYU pragma: keep
#include "vec/runtime/shared_hash_table_controller.h"

namespace doris {
// this class used in hash join node
class VRuntimeFilterSlots {
public:
    VRuntimeFilterSlots(
            const std::vector<std::shared_ptr<vectorized::VExprContext>>& build_expr_ctxs,
            const std::vector<IRuntimeFilter*>& runtime_filters)
            : _build_expr_context(build_expr_ctxs), _runtime_filters(runtime_filters) {
        for (auto* runtime_filter : _runtime_filters) {
            _runtime_filters_map[runtime_filter->expr_order()].push_back(runtime_filter);
        }
    }

    Status send_filter_size(RuntimeState* state, uint64_t hash_table_size,
                            std::shared_ptr<pipeline::Dependency> dependency) {
        if (_runtime_filters.empty()) {
            return Status::OK();
        }
        for (auto* runtime_filter : _runtime_filters) {
            if (runtime_filter->need_sync_filter_size()) {
                runtime_filter->set_dependency(dependency);
            }
        }

        // send_filter_size may call dependency->sub(), so we call set_dependency firstly for all rf to avoid dependency set_ready repeatedly
        for (auto* runtime_filter : _runtime_filters) {
            if (runtime_filter->need_sync_filter_size()) {
                RETURN_IF_ERROR(runtime_filter->send_filter_size(state, hash_table_size));
            }
        }
        return Status::OK();
    }

    // use synced size when this rf has global merged
    static uint64_t get_real_size(IRuntimeFilter* runtime_filter, uint64_t hash_table_size) {
        return runtime_filter->isset_synced_size() ? runtime_filter->get_synced_size()
                                                   : hash_table_size;
    }

    Status ignore_filters(RuntimeState* state) {
        // process ignore duplicate IN_FILTER
        std::unordered_set<int> has_in_filter;
        for (auto* filter : _runtime_filters) {
            if (filter->get_ignored()) {
                continue;
            }
            if (filter->get_real_type() != RuntimeFilterType::IN_FILTER) {
                continue;
            }
            if (has_in_filter.contains(filter->expr_order())) {
                filter->set_ignored();
                continue;
            }
            has_in_filter.insert(filter->expr_order());
        }

        // process ignore filter when it has IN_FILTER on same expr, and init bloom filter size
        for (auto* filter : _runtime_filters) {
            if (filter->get_ignored()) {
                continue;
            }
            if (filter->get_real_type() == RuntimeFilterType::IN_FILTER ||
                !has_in_filter.contains(filter->expr_order())) {
                continue;
            }
            filter->set_ignored();
        }
        return Status::OK();
    }

    Status init_filters(RuntimeState* state, uint64_t local_hash_table_size) {
        // process IN_OR_BLOOM_FILTER's real type
        for (auto* filter : _runtime_filters) {
            if (filter->get_ignored()) {
                continue;
            }
            if (filter->type() == RuntimeFilterType::IN_OR_BLOOM_FILTER &&
                get_real_size(filter, local_hash_table_size) > state->runtime_filter_max_in_num()) {
                RETURN_IF_ERROR(filter->change_to_bloom_filter());
            }

            if (filter->get_real_type() == RuntimeFilterType::BLOOM_FILTER) {
                if (filter->need_sync_filter_size() != filter->isset_synced_size()) {
                    return Status::InternalError("sync filter size meet error, filter: {}",
                                                 filter->debug_string());
                }
                RETURN_IF_ERROR(
                        filter->init_bloom_filter(get_real_size(filter, local_hash_table_size)));
            }
        }
        return Status::OK();
    }

    void insert(const vectorized::Block* block) {
        for (int i = 0; i < _build_expr_context.size(); ++i) {
            auto iter = _runtime_filters_map.find(i);
            if (iter == _runtime_filters_map.end()) {
                continue;
            }

            int result_column_id = _build_expr_context[i]->get_last_result_column_id();
            const auto& column = block->get_by_position(result_column_id).column;
            for (auto* filter : iter->second) {
                if (filter->get_ignored()) {
                    continue;
                }
                filter->insert_batch(column, 1);
            }
        }
    }

    // publish runtime filter
    Status publish(bool publish_local = false) {
        for (auto& pair : _runtime_filters_map) {
            for (auto& filter : pair.second) {
                RETURN_IF_ERROR(filter->publish(publish_local));
            }
        }
        return Status::OK();
    }

    void copy_to_shared_context(vectorized::SharedHashTableContextPtr& context) {
        for (auto& it : _runtime_filters_map) {
            for (auto& filter : it.second) {
                context->runtime_filters[filter->filter_id()] = filter->get_shared_context_ref();
            }
        }
    }

    Status copy_from_shared_context(vectorized::SharedHashTableContextPtr& context) {
        for (auto& it : _runtime_filters_map) {
            for (auto& filter : it.second) {
                auto filter_id = filter->filter_id();
                auto ret = context->runtime_filters.find(filter_id);
                if (ret == context->runtime_filters.end()) {
                    return Status::Aborted("invalid runtime filter id: {}", filter_id);
                }
                filter->get_shared_context_ref() = ret->second;
            }
        }
        return Status::OK();
    }

    bool empty() { return _runtime_filters_map.empty(); }

private:
    const std::vector<std::shared_ptr<vectorized::VExprContext>>& _build_expr_context;
    std::vector<IRuntimeFilter*> _runtime_filters;
    // prob_contition index -> [IRuntimeFilter]
    std::map<int, std::list<IRuntimeFilter*>> _runtime_filters_map;
};

} // namespace doris
