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

#include "exec/operator/cache_source_operator.h"

#include <functional>
#include <utility>

#include "cloud/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/operator/operator.h"
#include "exec/pipeline/dependency.h"
#include "runtime/runtime_state.h"

namespace doris {

Status CacheSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    ((DataQueueSharedState*)_dependency->shared_state())
            ->data_queue.set_source_dependency(_shared_state->source_deps.front());
    const auto& scan_ranges = info.scan_ranges;

    auto& parent = _parent->cast<CacheSourceOperatorX>();
    const auto& cache_param = parent._cache_param;
    // 1. init the slot orders
    const auto& tuple_descs = parent.row_desc().tuple_descriptors();
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

    std::vector<int64_t> cache_tablet_ids;
    cache_tablet_ids.reserve(scan_ranges.size());
    for (const auto& scan_range : scan_ranges) {
        cache_tablet_ids.push_back(scan_range.scan_range.palo_scan_range.tablet_id);
    }
    std::sort(cache_tablet_ids.begin(), cache_tablet_ids.end());
    std::string tablet_ids_str;
    for (size_t i = 0; i < cache_tablet_ids.size(); ++i) {
        tablet_ids_str += std::to_string(cache_tablet_ids[i]);
        if (i < cache_tablet_ids.size() - 1) {
            tablet_ids_str += ",";
        }
    }
    custom_profile()->add_info_string("CacheTabletId", tablet_ids_str);

    // 2. consume the per-instance cache decision. It is made exactly once for
    // this instance and shared with the olap scan operator, so both operators
    // always act consistently (e.g. the scan skips scanning if and only if this
    // operator emits the cached blocks) -- whatever their init order is, and
    // even if the entry gets evicted in between (the decision pins it).
    if (parent._query_cache_runtime == nullptr) {
        // The fragment context always creates the runtime together with this
        // operator. Degrading to a pass-through here would silently drop data
        // if the paired scan operator still made a HIT decision (it skips
        // scanning while nothing emits the entry), so a broken setup must
        // fail loudly, mirroring the scan side.
        return Status::InternalError(
                "query cache runtime is absent at the cache source, node_id={}",
                cache_param.node_id);
    }
    _global_cache = parent._query_cache_runtime->cache();
    _cache_decision = parent._query_cache_runtime->get_or_make_decision(scan_ranges);
    _cache_key = _cache_decision->cache_key;
    _version = _cache_decision->current_version;

    using Mode = QueryCacheInstanceDecision::Mode;
    const bool hit_cache = _cache_decision->mode == Mode::HIT;
    _is_incremental = _cache_decision->mode == Mode::INCREMENTAL;
    // HIT emits the entry unchanged, so there is nothing to write back; both
    // MISS and INCREMENTAL rebuild the entry (from scratch / by merge), unless
    // the decision already knows the merged entry could never fit the entry
    // limits (write_back_feasible == false).
    // A query reading through a warmed-up-layout knob (both cloud only) must
    // not write back either, because its scan is not guaranteed to represent
    // exactly the queried version the entry would be stamped with. Both
    // knobs walk the version graph by warmed-up preference and, unlike the
    // plain capture, never clip an edge whose end lies past the requested
    // window: the read may OVERSHOOT the queried version (a warmed
    // compaction rowset spanning it drags the path beyond), and under
    // freshness tolerance it may also STOP BELOW it (the walk halts at the
    // warmed boundary). Neither knob carries an affectQueryResult
    // annotation, so such an entry shares its cache key with plain runs of
    // the same statement, which would then serve (exact hit) or extend
    // (incremental merge) a base whose content does not match its version
    // stamp: missing rows where the read stopped short, double-counted rows
    // where it overshot. Suppressing the write-back keeps these reads pure
    // consumers: they may still serve an exact HIT filled by a plain run
    // (precise data, no worse than asked).
    //
    // Carve-out for the prefer knob on cloud merge-on-write UNIQUE tables:
    // CloudTablet::capture_consistent_versions_unlocked honors
    // enable_prefer_cached_rowset only for non-MOW tables (it guards on
    // !enable_unique_key_merge_on_write()), so a MOW query that set prefer but
    // not freshness still reads the exact queried version. That fill is
    // version-exact and cacheable; suppressing it would keep a cloud MOW table
    // with prefer set from ever populating the cache. Freshness tolerance has
    // no such MOW guard (it is honored for MOW too), so it still forces
    // suppression regardless of table type.
    const bool inexact_version_fill =
            config::is_cloud_mode() &&
            (state->enable_query_freshness_tolerance() ||
             (state->enable_prefer_cached_rowset() && !cache_param.is_merge_on_write));
    _need_insert_cache = _cache_decision->key_valid && !hit_cache &&
                         _cache_decision->write_back_feasible && !inexact_version_fill;
    _insert_delta_count = _is_incremental ? _cache_decision->cached_delta_count + 1 : 0;

    if (hit_cache || _is_incremental) {
        _hit_cache_results = _cache_decision->handle.get_cache_result();
        auto hit_cache_slot_orders = _cache_decision->handle.get_cache_slot_orders();

        if (_slot_orders != *hit_cache_slot_orders) {
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

    custom_profile()->add_info_string("HitCache", std::to_string(hit_cache));
    custom_profile()->add_info_string("HitCacheStale", std::to_string(_is_incremental));
    if (_is_incremental) {
        custom_profile()->add_info_string(
                "IncrementalDeltaVersions",
                fmt::format("({}, {}]", _cache_decision->cached_version, _version));
    }
    if (!_cache_decision->incremental_fallback_reason.empty()) {
        custom_profile()->add_info_string("IncrementalFallbackReason",
                                          _cache_decision->incremental_fallback_reason);
    }

    return Status::OK();
}

Status CacheSourceLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    return Status::OK();
}

bool CacheSourceLocalState::_account_write_back(int64_t rows, int64_t bytes) {
    _current_query_cache_rows += rows;
    _current_query_cache_bytes += bytes;
    const auto& cache_param = _parent->cast<CacheSourceOperatorX>()._cache_param;
    if (cache_param.entry_max_bytes < _current_query_cache_bytes ||
        cache_param.entry_max_rows < _current_query_cache_rows) {
        // over the max bytes/rows: pass the data through, no need to do cache
        _local_cache_blocks.clear();
        _need_insert_cache = false;
        return false;
    }
    return true;
}

Status CacheSourceLocalState::_append_block_for_write_back(const Block& block) {
    if (!_account_write_back(block.rows(), block.allocated_bytes())) {
        return Status::OK();
    }
    // Zero-copy snapshot: inserting shares the COW column pointers instead
    // of cloning the payload. The cached columns stay alive (and immutable)
    // through the decision's entry pin until QueryCache::insert() deep-copies
    // the accumulated set once.
    auto& kept = _local_cache_blocks.emplace_back(Block::create_unique());
    for (const auto& column : block.get_columns_with_type_and_name()) {
        kept->insert(column);
    }
    return Status::OK();
}

std::string CacheSourceLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", Base::debug_string(indentation_level));
    if (_shared_state) {
        fmt::format_to(debug_string_buffer, ", data_queue: {}",
                       _shared_state->data_queue.debug_string());
    }
    return fmt::to_string(debug_string_buffer);
}

Status CacheSourceOperatorX::get_block_impl(RuntimeState* state, Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());

    block->clear_column_data(_row_descriptor.num_materialized_slots());
    bool need_clone_empty = block->columns() == 0;

    const bool has_cached_block =
            local_state._hit_cache_results != nullptr &&
            local_state._hit_cache_pos < local_state._hit_cache_results->size();

    if (has_cached_block) {
        // Emit one cached block: the whole result on HIT, or the cached part
        // ahead of the delta on INCREMENTAL. Both the cached blocks and the
        // delta blocks are partial aggregation states, so the upstream merge
        // aggregation combines them into the final result.
        // Note: this operator is only scheduled once the data queue has data or
        // finished, so on INCREMENTAL the cached blocks are not emitted before
        // the first delta block arrives. Making the source dependency initially
        // ready for HIT/INCREMENTAL would overlap emitting the cached part with
        // the delta scan -- a possible future latency optimization.
        const auto& hit_cache_block =
                local_state._hit_cache_results->at(local_state._hit_cache_pos++);
        // Reorder the cached block to this query's slot order BEFORE merging
        // (a zero-copy view: inserting reuses the COW column pointers).
        // Merging first and permuting afterwards is an order trap on the
        // second cached block whenever the reused output block keeps its
        // schema between pulls: a cached-order block then merges positionally
        // into query-order columns -- a type error for heterogeneous slots,
        // silently misplaced data for same-typed ones. Today the trap stays
        // latent by accident: this operator is built without a plan node, so
        // its own _row_descriptor reports zero materialized slots and the
        // clear_column_data() above wipes the whole block every pull. The
        // pipeline driver's clears are schema-keeping though, so giving this
        // operator a real row descriptor (or dropping the redundant-looking
        // wipe above) would arm it; reordering the source keeps the merge
        // order-aligned under both block shapes.
        Block reordered_cache_block;
        const Block* cache_block_to_merge = hit_cache_block.get();
        if (!local_state._hit_cache_column_orders.empty()) {
            for (auto loc : local_state._hit_cache_column_orders) {
                reordered_cache_block.insert(hit_cache_block->get_by_position(loc));
            }
            cache_block_to_merge = &reordered_cache_block;
        }
        if (need_clone_empty) {
            *block = cache_block_to_merge->clone_empty();
        }
        {
            ScopedMutableBlock scoped_mutable_block(block);
            auto& mutable_block = scoped_mutable_block.mutable_block();
            RETURN_IF_ERROR(mutable_block.merge(*cache_block_to_merge));
            scoped_mutable_block.restore();
        }
        if (local_state._is_incremental && local_state._need_insert_cache) {
            // Snapshot the cached block (already in this query's slot order)
            // for the write-back, so the new entry holds "cached + delta"
            // under one consistent slot order. The snapshot shares the COW
            // columns of the pinned entry, so no payload is copied before
            // the insert-time materialization.
            RETURN_IF_ERROR(local_state._append_block_for_write_back(*cache_block_to_merge));
        }
    } else if (local_state._hit_cache_results != nullptr && !local_state._is_incremental) {
        // HIT: all cached blocks are emitted.
        *eos = true;
    } else {
        // MISS, or the delta phase of INCREMENTAL after the cached blocks.
        // The entry is committed only from the explicit success paths below,
        // never during error unwinding: on the final delta block *eos is set
        // BEFORE the block is merged, so a deferred/unconditional commit
        // would publish an entry that is missing the failed block under the
        // current version, and a later exact hit would silently serve it.
        auto commit_entry_if_finished = [&]() {
            if (*eos) {
                local_state.custom_profile()->add_info_string(
                        "InsertCache", std::to_string(local_state._need_insert_cache));
                if (local_state._need_insert_cache) {
                    local_state._global_cache->insert(local_state._cache_key, local_state._version,
                                                      local_state._local_cache_blocks,
                                                      local_state._slot_orders,
                                                      local_state._current_query_cache_bytes,
                                                      local_state._insert_delta_count);
                    local_state._local_cache_blocks.clear();
                    // Latch off so the commit is exactly-once by construction:
                    // a spurious re-poll after eos (no in-tree driver does
                    // this today) would otherwise re-publish the now-cleared,
                    // EMPTY block set over the entry just inserted, and later
                    // exact hits would silently serve an empty result. This
                    // is the only source operator whose eos path carries a
                    // global side effect, so it must not rely on the pull
                    // protocol never poking it again.
                    local_state._need_insert_cache = false;
                }
            }
        };

        auto queue_block = DORIS_TRY(local_state._shared_state->data_queue.get_block_from_queue());
        *eos = queue_block.eos;

        if (!queue_block.block) {
            commit_entry_if_finished();
            return Status::OK();
        }

        auto& output_block = queue_block.block;
        if (local_state._need_insert_cache) {
            if (need_clone_empty) {
                *block = output_block->clone_empty();
            }
            ScopedMutableBlock scoped_mutable_block(block);
            auto& mutable_block = scoped_mutable_block.mutable_block();
            RETURN_IF_ERROR(mutable_block.merge(*output_block));
            scoped_mutable_block.restore();
            if (local_state._account_write_back(output_block->rows(),
                                                output_block->allocated_bytes())) {
                local_state._local_cache_blocks.emplace_back(std::move(output_block));
            }
        } else {
            *block = std::move(*output_block);
        }
        commit_entry_if_finished();
    }

    local_state.reached_limit(block, eos);
    return Status::OK();
}

} // namespace doris
