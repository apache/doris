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

#include "pipeline/exec/cache_source_operator.h"

#include <functional>
#include <utility>

#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "vec/core/block.h"

namespace doris {
class RuntimeState;

namespace pipeline {

Status CacheSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    ((CacheSharedState*)_dependency->shared_state())
            ->data_queue.set_source_dependency(_shared_state->source_deps.front());
    const auto& scan_ranges = info.scan_ranges;
    bool hit_cache = false;
    if (scan_ranges.size() > 1) {
        return Status::InternalError("CacheSourceOperator only support one scan range, plan error");
    }

    const auto& cache_param = _parent->cast<CacheSourceOperatorX>()._cache_param;
    // 1. init the slot orders
    const auto& tuple_descs = _parent->cast<CacheSourceOperatorX>().row_desc().tuple_descriptors();
    for (auto tuple_desc : tuple_descs) {
        for (auto slot_desc : tuple_desc->slots()) {
            if (cache_param.output_slot_mapping.find(slot_desc->id()) !=
                cache_param.output_slot_mapping.end()) {
                _slot_orders.emplace_back(cache_param.output_slot_mapping.at(slot_desc->id()));
            } else {
                return Status::InternalError(
                        fmt::format("Cache can find the mapping slot id {}, node id {}",
                                    slot_desc->id(), cache_param.node_id));
            }
        }
    }

    // 2. build cache key by digest_tablet_id
    RETURN_IF_ERROR(QueryCache::build_cache_key(scan_ranges, cache_param, &_cache_key, &_version));
    _runtime_profile->add_info_string(
            "CacheTabletId", std::to_string(scan_ranges[0].scan_range.palo_scan_range.tablet_id));

    // 3. lookup the cache and find proper slot order
    hit_cache = QueryCache::instance()->lookup(_cache_key, _version, &_query_cache_handle);
    _runtime_profile->add_info_string("HitCache", hit_cache ? "1" : "0");
    if (hit_cache && !cache_param.force_refresh_query_cache) {
        _hit_cache_results = _query_cache_handle.get_cache_result();
        auto hit_cache_slot_orders = _query_cache_handle.get_cache_slot_orders();

        bool need_reorder = _slot_orders.size() != hit_cache_slot_orders->size();
        if (!need_reorder) {
            for (int i = 0; i < _slot_orders.size(); ++i) {
                need_reorder = _slot_orders[i] != (*hit_cache_slot_orders)[i];
            }
        }

        if (need_reorder) {
            for (auto slot_id : _slot_orders) {
                auto find_res = std::find(hit_cache_slot_orders->begin(),
                                          hit_cache_slot_orders->end(), slot_id);
                if (find_res != hit_cache_slot_orders->end()) {
                    _hit_cache_column_orders.emplace_back(find_res -
                                                          hit_cache_slot_orders->begin());
                } else {
                    return Status::InternalError(fmt::format(
                            "Cache can find the mapping slot id {}, node id {}, "
                            "hit_cache_column_orders [{}]",
                            slot_id, cache_param.node_id, fmt::join(*hit_cache_slot_orders, ",")));
                }
            }
        }
    }

    return Status::OK();
}

Status CacheSourceLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    return Status::OK();
}

std::string CacheSourceLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", Base::debug_string(indentation_level));
    if (_shared_state) {
        fmt::format_to(debug_string_buffer, ", data_queue: (is_all_finish = {}, has_data = {})",
                       _shared_state->data_queue.is_all_finish(),
                       _shared_state->data_queue.remaining_has_data());
    }
    return fmt::to_string(debug_string_buffer);
}

Status CacheSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());

    block->clear_column_data(_row_descriptor.num_materialized_slots());
    bool need_clone_empty = block->columns() == 0;

    if (local_state._hit_cache_results == nullptr) {
        Defer insert_cache([&] {
            if (*eos && local_state._need_insert_cache) {
                local_state._runtime_profile->add_info_string("InsertCache", "1");
                local_state._global_cache->insert(local_state._cache_key, local_state._version,
                                                  local_state._local_cache_blocks,
                                                  local_state._slot_orders,
                                                  local_state._current_query_cache_bytes);
                local_state._local_cache_blocks.clear();
            }
        });

        std::unique_ptr<vectorized::Block> output_block;
        int child_idx = 0;
        RETURN_IF_ERROR(local_state._shared_state->data_queue.get_block_from_queue(&output_block,
                                                                                   &child_idx));
        // Here, check the value of `_has_data(state)` again after `data_queue.is_all_finish()` is TRUE
        // as there may be one or more blocks when `data_queue.is_all_finish()` is TRUE.
        *eos = !_has_data(state) && local_state._shared_state->data_queue.is_all_finish();

        if (!output_block) {
            return Status::OK();
        }

        if (local_state._need_insert_cache) {
            if (need_clone_empty) {
                *block = output_block->clone_empty();
            }
            RETURN_IF_ERROR(
                    vectorized::MutableBlock::build_mutable_block(block).merge(*output_block));
            local_state._current_query_cache_rows += output_block->rows();
            auto mem_consume = output_block->allocated_bytes();
            local_state._current_query_cache_bytes += mem_consume;
            local_state._mem_tracker->consume(mem_consume);

            if (_cache_param.entry_max_bytes < local_state._current_query_cache_bytes ||
                _cache_param.entry_max_rows < local_state._current_query_cache_rows) {
                // over the max bytes, pass through the data, no need to do cache
                local_state._local_cache_blocks.clear();
                local_state._need_insert_cache = false;
                local_state._runtime_profile->add_info_string("InsertCache", "0");
            } else {
                local_state._local_cache_blocks.emplace_back(std::move(output_block));
            }
        } else {
            *block = std::move(*output_block);
        }
    } else {
        if (local_state._hit_cache_pos < local_state._hit_cache_results->size()) {
            const auto& hit_cache_block =
                    local_state._hit_cache_results->at(local_state._hit_cache_pos++);
            if (need_clone_empty) {
                *block = hit_cache_block->clone_empty();
            }
            RETURN_IF_ERROR(
                    vectorized::MutableBlock::build_mutable_block(block).merge(*hit_cache_block));
            if (!local_state._hit_cache_column_orders.empty()) {
                auto datas = block->get_columns_with_type_and_name();
                block->clear();
                for (auto loc : local_state._hit_cache_column_orders) {
                    block->insert(datas[loc]);
                }
            }
        } else {
            *eos = true;
        }
    }

    local_state.reached_limit(block, eos);
    return Status::OK();
}

} // namespace pipeline
} // namespace doris
