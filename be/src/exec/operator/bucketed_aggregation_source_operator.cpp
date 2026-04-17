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

#include "exec/operator/bucketed_aggregation_source_operator.h"

#include <memory>
#include <string>

#include "common/exception.h"
#include "core/column/column_vector.h"
#include "exec/common/hash_table/hash.h"
#include "exec/common/util.hpp"
#include "exec/operator/operator.h"
#include "exprs/vectorized_agg_fn.h"
#include "runtime/runtime_profile.h"
#include "runtime/thread_context.h"

namespace doris {

// Helper to set/get null key data on hash tables that support it (DataWithNullKey).
// For hash tables without nullable key support (PHHashMap), these are no-ops.
// This is needed because in nested std::visit lambdas, the outer hash table type is already
// resolved and doesn't depend on the inner template parameter, so `if constexpr` inside the
// inner lambda cannot suppress compilation of code that accesses has_null_key_data() on the
// outer (non-dependent) type.
template <typename HashTable>
constexpr bool has_nullable_key_v =
        std::is_assignable_v<decltype(std::declval<HashTable&>().has_null_key_data()), bool>;

template <typename HashTable>
void set_null_key_flag(HashTable& ht, bool val) {
    if constexpr (has_nullable_key_v<HashTable>) {
        ht.has_null_key_data() = val;
    }
}

template <typename HashTable>
bool get_null_key_flag(const HashTable& ht) {
    if constexpr (has_nullable_key_v<HashTable>) {
        return ht.has_null_key_data();
    } else {
        return false;
    }
}

template <typename HashTable>
AggregateDataPtr get_null_key_agg_data(HashTable& ht) {
    if constexpr (has_nullable_key_v<HashTable>) {
        return ht.template get_null_key_data<AggregateDataPtr>();
    } else {
        return nullptr;
    }
}

template <typename HashTable>
void set_null_key_agg_data(HashTable& ht, AggregateDataPtr val) {
    if constexpr (has_nullable_key_v<HashTable>) {
        ht.template get_null_key_data<AggregateDataPtr>() = val;
    }
}

// Returns a REFERENCE to the null key's AggregateDataPtr slot.
// Critical for simple_count merge: writing through a copy would lose the update (Bug #30).
template <typename HashTable>
AggregateDataPtr& get_null_key_agg_data_ref(HashTable& ht) {
    static_assert(has_nullable_key_v<HashTable>,
                  "get_null_key_agg_data_ref requires a nullable hash table");
    return ht.template get_null_key_data<AggregateDataPtr>();
}

// Helper for emplace that works with PHHashMap (3-arg).
template <typename HashTable, typename Key>
auto hash_table_emplace(HashTable& ht, const Key& key, typename HashTable::LookupResult& it,
                        bool& inserted) -> decltype(ht.emplace(key, it, inserted), void()) {
    ht.emplace(key, it, inserted);
}

/// Merge src aggregate state into dst_ref (a reference to the mapped slot).
/// For simple_count, adds UInt64 counters directly via the reference.
/// For regular aggregates, calls merge() on each function then destroys src state.
/// After return, src is consumed and must not be used.
static void merge_agg_states(AggregateDataPtr& dst_ref, AggregateDataPtr src, bool use_simple_count,
                             const std::vector<AggFnEvaluator*>& evaluators, const Sizes& offsets,
                             Arena& arena) {
    if (use_simple_count) {
        // simple_count: mapped slots hold UInt64 counters. MUST use reference
        // to write back correctly.
        reinterpret_cast<UInt64&>(dst_ref) += reinterpret_cast<UInt64>(src);
    } else {
        const size_t num_fns = evaluators.size();
        for (size_t i = 0; i < num_fns; ++i) {
            evaluators[i]->function()->merge(dst_ref + offsets[i], src + offsets[i], arena);
        }
        for (size_t i = 0; i < num_fns; ++i) {
            evaluators[i]->function()->destroy(src + offsets[i]);
        }
    }
}

/// Merge a source null key into a destination null key slot. Handles three cases:
/// 1. Dst has no null key yet: move src's null key to dst (no merge needed).
/// 2. Dst already has a null key: merge src into dst using merge_agg_states.
/// 3. Src has no null key: no-op.
/// After merge, clears the src null key slot.
template <typename HashTable>
static void merge_null_key(HashTable& dst_data, HashTable& src_data, bool use_simple_count,
                           const std::vector<AggFnEvaluator*>& evaluators, const Sizes& offsets,
                           Arena& arena) {
    if constexpr (has_nullable_key_v<HashTable>) {
        if (!get_null_key_flag(src_data)) {
            return;
        }
        auto src_null = get_null_key_agg_data(src_data);
        if (!src_null) {
            return;
        }
        if (!get_null_key_flag(dst_data)) {
            // Dst has no null key yet — move src's null key to dst.
            set_null_key_flag(dst_data, true);
            set_null_key_agg_data(dst_data, src_null);
        } else {
            // Both have null keys — merge src into dst.
            auto& dst_null_ref = get_null_key_agg_data_ref(dst_data);
            merge_agg_states(dst_null_ref, src_null, use_simple_count, evaluators, offsets, arena);
        }
        set_null_key_agg_data(src_data, nullptr);
        set_null_key_flag(src_data, false);
    }
}

BucketedAggLocalState::BucketedAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status BucketedAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    _task_idx = info.task_idx;

    _get_results_timer = ADD_TIMER(custom_profile(), "GetResultsTime");
    _hash_table_iterate_timer = ADD_TIMER(custom_profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(custom_profile(), "InsertKeysToColumnTime");
    _insert_values_to_column_timer = ADD_TIMER(custom_profile(), "InsertValuesToColumnTime");
    _merge_timer = ADD_TIMER(custom_profile(), "MergeTime");

    return Status::OK();
}

Status BucketedAggLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }

    // Release any held per-bucket CAS lock. This can happen when the source
    // is closed prematurely (e.g., LIMIT reached via reached_limit() while
    // we were mid-output on a bucket). Without this, the other source instance
    // would spin forever trying to acquire this bucket's lock.
    if (_current_output_bucket >= 0) {
        auto& bs = _shared_state->bucket_states[_current_output_bucket];
        bs.output_done.store(true, std::memory_order_release);
        bs.merge_in_progress.store(false, std::memory_order_release);
        _current_output_bucket = -1;
        _shared_state->state_generation.fetch_add(1, std::memory_order_release);
        _wake_up_other_sources();
    }

    return Base::close(state);
}

void BucketedAggLocalState::_make_nullable_output_key(Block* block) {
    if (block->rows() != 0) {
        for (auto cid : _shared_state->make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

void BucketedAggLocalState::_wake_up_other_sources() {
    auto& shared_state = *_shared_state;
    for (int i = 0; i < static_cast<int>(shared_state.source_deps.size()); ++i) {
        shared_state.source_deps[i]->set_ready();
    }
}

int BucketedAggLocalState::_merge_bucket(int bucket, int merge_target) {
    SCOPED_TIMER(_merge_timer);
    auto& shared_state = *_shared_state;
    auto& bs = shared_state.bucket_states[bucket];

    // Merge target's bucket is the destination.
    auto& dst_agg_data = *shared_state.per_instance_data[merge_target].bucket_agg_data[bucket];
    int merged_count = 0;

    std::visit(
            Overload {
                    [&](std::monostate& arg) -> void {
                        // uninited — no data to merge
                    },
                    [&](auto& dst_method) -> void {
                        using AggMethodType = std::decay_t<decltype(dst_method)>;
                        auto& dst_data = *dst_method.hash_table;

                        // Merge all finished sink instances (except merge_target itself)
                        // into the merge target's bucket.
                        for (int inst_idx = 0; inst_idx < shared_state.num_sink_instances;
                             ++inst_idx) {
                            if (inst_idx == merge_target) {
                                continue;
                            }
                            // Skip instances already merged for this bucket.
                            if (bs.merged_instances[inst_idx]) {
                                continue;
                            }
                            // Only merge sinks that have finished.
                            if (!shared_state.sink_finished[inst_idx].load(
                                        std::memory_order_acquire)) {
                                continue;
                            }

                            auto& src_inst = shared_state.per_instance_data[inst_idx];
                            auto& src_agg_data = *src_inst.bucket_agg_data[bucket];

                            std::visit(
                                    Overload {
                                            [&](std::monostate& arg) -> void {
                                                // Mark as merged even if monostate (no data).
                                                bs.merged_instances[inst_idx] = true;
                                            },
                                            [&](auto& src_method) -> void {
                                                using SrcMethodType =
                                                        std::decay_t<decltype(src_method)>;
                                                if constexpr (std::is_same_v<SrcMethodType,
                                                                             AggMethodType>) {
                                                    auto& src_data = *src_method.hash_table;

                                                    ++merged_count;

                                                    // Direct merge: iterate source hash table
                                                    // entries, emplace into destination, and null
                                                    // out source entries in one pass. This avoids
                                                    // allocating intermediate vectors (keys,
                                                    // mappeds, hashes) and eliminates the separate
                                                    // null-out traversal.
                                                    const bool use_simple_count =
                                                            shared_state.use_simple_count;
                                                    src_data.for_each([&](const auto& key,
                                                                          auto& mapped) {
                                                        if (!mapped) {
                                                            return;
                                                        }
                                                        auto src_mapped = mapped;
                                                        mapped = nullptr;

                                                        typename std::remove_reference_t<
                                                                decltype(dst_data)>::LookupResult
                                                                dst_it;
                                                        bool inserted = false;
                                                        hash_table_emplace(dst_data, key, dst_it,
                                                                           inserted);

                                                        if (inserted) {
                                                            *::lookup_result_get_mapped(dst_it) =
                                                                    src_mapped;
                                                        } else {
                                                            auto& dst_mapped =
                                                                    *::lookup_result_get_mapped(
                                                                            dst_it);
                                                            merge_agg_states(
                                                                    dst_mapped, src_mapped,
                                                                    use_simple_count,
                                                                    shared_state
                                                                            .aggregate_evaluators,
                                                                    shared_state
                                                                            .offsets_of_aggregate_states,
                                                                    *src_inst.arena);
                                                        }
                                                    });

                                                    merge_null_key(
                                                            dst_data, src_data, use_simple_count,
                                                            shared_state.aggregate_evaluators,
                                                            shared_state
                                                                    .offsets_of_aggregate_states,
                                                            *src_inst.arena);

                                                    // Mark this instance as merged for this bucket.
                                                    bs.merged_instances[inst_idx] = true;
                                                }
                                            }},
                                    src_agg_data.method_variant);
                        }
                    }},
            dst_agg_data.method_variant);

    return merged_count;
}

void BucketedAggLocalState::_build_output_block(Block* block, MutableColumns& key_columns,
                                                const std::vector<AggregateDataPtr>& values,
                                                uint32_t num_rows, bool mem_reuse) {
    SCOPED_TIMER(_insert_values_to_column_timer);
    auto& shared_state = *_shared_state;
    auto& p = _parent->template cast<BucketedAggSourceOperatorX>();
    size_t key_size = shared_state.probe_expr_ctxs.size();
    size_t agg_size = shared_state.aggregate_evaluators.size();

    if (p._needs_finalize) {
        auto columns_with_schema =
                VectorizedUtils::create_columns_with_type_and_name(p.row_descriptor());
        MutableColumns value_columns;
        for (size_t i = key_size; i < columns_with_schema.size(); ++i) {
            if (mem_reuse) {
                value_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
            } else {
                value_columns.emplace_back(columns_with_schema[i].type->create_column());
            }
        }

        if (shared_state.use_simple_count) {
            // simple_count: mapped slots hold UInt64 counters directly.
            // Write them to ColumnInt64.
            DCHECK_EQ(agg_size, 1);
            auto* col = assert_cast<ColumnInt64*>(value_columns[0].get());
            for (uint32_t r = 0; r < num_rows; ++r) {
                col->insert_value(static_cast<Int64>(reinterpret_cast<UInt64>(values[r])));
            }
        } else {
            for (size_t i = 0; i < agg_size; ++i) {
                shared_state.aggregate_evaluators[i]->insert_result_info_vec(
                        values, shared_state.offsets_of_aggregate_states[i], value_columns[i].get(),
                        num_rows);
            }
        }

        if (!mem_reuse) {
            ColumnsWithTypeAndName result_columns;
            for (size_t i = 0; i < key_size; ++i) {
                result_columns.emplace_back(std::move(key_columns[i]),
                                            shared_state.probe_expr_ctxs[i]->root()->data_type(),
                                            shared_state.probe_expr_ctxs[i]->root()->expr_name());
            }
            for (size_t i = 0; i < agg_size; ++i) {
                result_columns.emplace_back(std::move(value_columns[i]),
                                            columns_with_schema[key_size + i].type, "");
            }
            *block = Block(result_columns);
        }
    } else {
        // Serialize path. simple_count should always finalize.
        DCHECK(!shared_state.use_simple_count);
        MutableColumns value_columns(agg_size);
        DataTypes value_data_types(agg_size);

        for (size_t i = 0; i < agg_size; ++i) {
            value_data_types[i] =
                    shared_state.aggregate_evaluators[i]->function()->get_serialized_type();
            if (mem_reuse) {
                value_columns[i] = std::move(*block->get_by_position(key_size + i).column).mutate();
            } else {
                value_columns[i] =
                        shared_state.aggregate_evaluators[i]->function()->create_serialize_column();
            }
            shared_state.aggregate_evaluators[i]->function()->serialize_to_column(
                    values, shared_state.offsets_of_aggregate_states[i], value_columns[i],
                    num_rows);
        }

        if (!mem_reuse) {
            ColumnsWithTypeAndName result_columns;
            for (size_t i = 0; i < key_size; ++i) {
                result_columns.emplace_back(std::move(key_columns[i]),
                                            shared_state.probe_expr_ctxs[i]->root()->data_type(),
                                            shared_state.probe_expr_ctxs[i]->root()->expr_name());
            }
            for (size_t i = 0; i < agg_size; ++i) {
                result_columns.emplace_back(std::move(value_columns[i]), value_data_types[i], "");
            }
            *block = Block(result_columns);
        }
    }
}

Status BucketedAggLocalState::_output_bucket(RuntimeState* state, Block* block, int bucket,
                                             int merge_target, uint32_t* rows_output) {
    auto& shared_state = *_shared_state;
    size_t key_size = shared_state.probe_expr_ctxs.size();

    auto& bucket_data = *shared_state.per_instance_data[merge_target].bucket_agg_data[bucket];

    return std::visit(
            Overload {[&](std::monostate& arg) -> Status {
                          *rows_output = 0;
                          return Status::OK();
                      },
                      [&](auto& agg_method) -> Status {
                          bool mem_reuse =
                                  shared_state.make_nullable_keys.empty() && block->mem_reuse();

                          // Initialize iterator (once per bucket — idempotent).
                          agg_method.init_iterator();
                          auto& it = agg_method.begin;
                          auto& it_end = agg_method.end;

                          uint32_t batch_size = state->batch_size();
                          auto table_size = agg_method.hash_table->size();
                          auto alloc_size = std::min(static_cast<size_t>(batch_size),
                                                     static_cast<size_t>(table_size));

                          // Reuse member buffers to avoid per-call heap allocation.
                          _output_values.resize(alloc_size);
                          agg_method.output_keys.resize(alloc_size);

                          uint32_t num_rows = 0;
                          {
                              SCOPED_TIMER(_hash_table_iterate_timer);
                              while (it != it_end && num_rows < batch_size) {
                                  auto key = it.get_first();
                                  auto mapped = it.get_second();
                                  ++it;
                                  if (mapped) {
                                      agg_method.output_keys[num_rows] = key;
                                      _output_values[num_rows] = mapped;
                                      ++num_rows;
                                  }
                              }
                          }

                          if (num_rows == 0) {
                              *rows_output = 0;
                              return Status::OK();
                          }

                          // Build key columns.
                          MutableColumns key_columns;
                          for (size_t i = 0; i < key_size; ++i) {
                              if (mem_reuse) {
                                  key_columns.emplace_back(
                                          std::move(*block->get_by_position(i).column).mutate());
                              } else {
                                  key_columns.emplace_back(shared_state.probe_expr_ctxs[i]
                                                                   ->root()
                                                                   ->data_type()
                                                                   ->create_column());
                              }
                          }

                          {
                              SCOPED_TIMER(_insert_keys_to_column_timer);
                              agg_method.insert_keys_into_columns(agg_method.output_keys,
                                                                  key_columns, num_rows);
                          }

                          _build_output_block(block, key_columns, _output_values, num_rows,
                                              mem_reuse);

                          *rows_output = num_rows;
                          return Status::OK();
                      }},
            bucket_data.method_variant);
}

Status BucketedAggLocalState::_merge_and_output_null_keys(RuntimeState* state, Block* block) {
    auto& shared_state = *_shared_state;
    size_t key_size = shared_state.probe_expr_ctxs.size();
    int merge_target = shared_state.merge_target_instance.load(std::memory_order_relaxed);

    // Merge null keys from all 256 buckets (in merge target) into bucket 0.
    // After per-bucket merge, each bucket in merge target may have its own null key data.
    // We merge them all into bucket 0's null key.
    auto& target_inst = shared_state.per_instance_data[merge_target];

    // We need to visit bucket 0's method variant to know the type.
    std::visit(Overload {[&](std::monostate& arg) -> void {
                             // No data
                         },
                         [&](auto& dst_method) -> void {
                             using AggMethodType = std::decay_t<decltype(dst_method)>;
                             auto& dst_data = *dst_method.hash_table;

                             if constexpr (has_nullable_key_v<
                                                   std::remove_reference_t<decltype(dst_data)>>) {
                                 const bool use_simple_count = shared_state.use_simple_count;
                                 // Merge null keys from buckets [1..255] into bucket 0.
                                 for (int b = 1; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
                                     auto& src_method = std::get<AggMethodType>(
                                             target_inst.bucket_agg_data[b]->method_variant);
                                     auto& src_data = *src_method.hash_table;

                                     merge_null_key(dst_data, src_data, use_simple_count,
                                                    shared_state.aggregate_evaluators,
                                                    shared_state.offsets_of_aggregate_states,
                                                    *target_inst.arena);
                                 }
                             }
                         }},
               target_inst.bucket_agg_data[0]->method_variant);

    // Now output the merged null key from bucket 0 (if any).
    return std::visit(
            Overload {
                    [&](std::monostate& arg) -> Status { return Status::OK(); },
                    [&](auto& agg_method) -> Status {
                        auto& data = *agg_method.hash_table;

                        if constexpr (has_nullable_key_v<std::remove_reference_t<decltype(data)>>) {
                            if (!get_null_key_flag(data)) {
                                return Status::OK();
                            }
                            auto null_data = get_null_key_agg_data(data);
                            if (!null_data) {
                                return Status::OK();
                            }

                            bool mem_reuse =
                                    shared_state.make_nullable_keys.empty() && block->mem_reuse();

                            MutableColumns key_columns;
                            for (size_t i = 0; i < key_size; ++i) {
                                if (mem_reuse) {
                                    key_columns.emplace_back(
                                            std::move(*block->get_by_position(i).column).mutate());
                                } else {
                                    key_columns.emplace_back(shared_state.probe_expr_ctxs[i]
                                                                     ->root()
                                                                     ->data_type()
                                                                     ->create_column());
                                }
                            }

                            DCHECK_EQ(key_columns.size(), 1);
                            DCHECK(key_columns[0]->is_nullable());
                            key_columns[0]->insert_data(nullptr, 0);

                            _output_values.resize(1);
                            _output_values[0] = null_data;
                            _build_output_block(block, key_columns, _output_values, 1, mem_reuse);
                        }
                        return Status::OK();
                    }},
            target_inst.bucket_agg_data[0]->method_variant);
}

Status BucketedAggLocalState::_get_results(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_get_results_timer);
    auto& shared_state = *_shared_state;

    // If merge_target is not yet set, no sink has finished yet — shouldn't happen
    // because source dependency is only set_ready after first sink finishes.
    int merge_target = shared_state.merge_target_instance.load(std::memory_order_acquire);
    DCHECK_GE(merge_target, 0);

    // If we have a bucket in progress (output didn't finish in previous get_block),
    // continue outputting from it. The per-bucket CAS lock (merge_in_progress) is
    // still held from the previous call.
    if (_current_output_bucket >= 0) {
        uint32_t rows_output = 0;
        RETURN_IF_ERROR(
                _output_bucket(state, block, _current_output_bucket, merge_target, &rows_output));
        if (rows_output > 0) {
            return Status::OK();
        }
        // This bucket is fully output. Release the CAS lock and wake other sources.
        auto& bs = shared_state.bucket_states[_current_output_bucket];
        bs.output_done.store(true, std::memory_order_release);
        bs.merge_in_progress.store(false, std::memory_order_release);
        _current_output_bucket = -1;
        shared_state.state_generation.fetch_add(1, std::memory_order_release);
        _wake_up_other_sources();
    }

    // Snapshot the state generation BEFORE scanning — used to detect races with block().
    // Any state change (bucket release, sink finish) bumps this counter. If it changes
    // between our snapshot and the re-check after block(), we know we might have missed
    // work and must unblock immediately. This eliminates the TOCTOU gap where the
    // per-bucket scan in the re-check could miss a bucket that was released and
    // immediately re-acquired by another source.
    uint64_t gen_before = shared_state.state_generation.load(std::memory_order_acquire);
    bool all_sinks_done = shared_state.num_sinks_finished.load(std::memory_order_acquire) ==
                          shared_state.num_sink_instances;
    bool did_work = false;

    for (int b = 0; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
        auto& bs = shared_state.bucket_states[b];

        // Skip buckets already fully output.
        if (bs.output_done.load(std::memory_order_acquire)) {
            continue;
        }

        // Try to acquire the per-bucket CAS lock.
        bool expected = false;
        if (!bs.merge_in_progress.compare_exchange_strong(expected, true,
                                                          std::memory_order_acquire)) {
            // Another source is working on this bucket — skip.
            continue;
        }

        // Under the lock: merge all finished sink instances into merge target's bucket.
        int merged_count = _merge_bucket(b, merge_target);

        if (all_sinks_done) {
            // All sinks are done — this bucket is fully merged. Output it.
            // Keep the lock while outputting to prevent other sources from
            // touching this bucket.
            uint32_t rows_output = 0;
            auto st = _output_bucket(state, block, b, merge_target, &rows_output);
            if (!st.ok()) {
                bs.merge_in_progress.store(false, std::memory_order_release);
                shared_state.state_generation.fetch_add(1, std::memory_order_release);
                _wake_up_other_sources();
                return st;
            }

            if (rows_output > 0) {
                // Bucket has more data than one batch — keep the CAS lock held
                // and remember where we left off. We'll resume on the next call.
                _current_output_bucket = b;
                return Status::OK();
            }

            // Bucket fully output in one batch. Release lock and wake others.
            bs.output_done.store(true, std::memory_order_release);
            bs.merge_in_progress.store(false, std::memory_order_release);
            shared_state.state_generation.fetch_add(1, std::memory_order_release);
            _wake_up_other_sources();
            did_work = true;
        } else {
            // Not all sinks done yet — release the lock. We'll come back later
            // when more sinks finish.
            bs.merge_in_progress.store(false, std::memory_order_release);
            if (merged_count > 0) {
                // We actually merged new data — bump generation and wake others
                // so they know state changed.
                shared_state.state_generation.fetch_add(1, std::memory_order_release);
                _wake_up_other_sources();
                did_work = true;
            }
        }
    }

    // Check if all 256 buckets have been output.
    {
        int actually_done = 0;
        for (int b = 0; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
            if (shared_state.bucket_states[b].output_done.load(std::memory_order_acquire)) {
                ++actually_done;
            }
        }
        if (actually_done == BUCKETED_AGG_NUM_BUCKETS) {
            // All buckets output. Handle null key.
            bool expected = false;
            if (shared_state.null_key_output_claimed.compare_exchange_strong(expected, true)) {
                RETURN_IF_ERROR(_merge_and_output_null_keys(state, block));
                if (block->rows() > 0) {
                    return Status::OK();
                }
            }
            // No more data.
            *eos = true;
            return Status::OK();
        }
    }

    // No work done in this call. Some buckets are still pending — either locked
    // by another source instance or waiting for more sinks to finish.
    // Block ourselves so we don't spin at 100% CPU. We will be woken up when:
    //   - A sink finishes (bumps state_generation and calls set_ready_to_read), OR
    //   - Another source releases a bucket lock (bumps state_generation and calls
    //     _wake_up_other_sources).
    if (!did_work) {
        _dependency->block();
        // Re-check for missed wakeups using the generation counter. If any state
        // change occurred since our scan started (gen_before), unblock immediately.
        // This is infallible: unlike scanning for unlocked buckets (which has a
        // TOCTOU gap where a bucket can be released and re-acquired between the
        // block() and the scan), the generation counter monotonically increases
        // and captures ALL state changes.
        uint64_t gen_after = shared_state.state_generation.load(std::memory_order_acquire);
        if (gen_after != gen_before) {
            _dependency->set_ready();
        }
    }

    // Return empty block — scheduler will re-run us when dependency becomes ready.
    return Status::OK();
}

// ============ BucketedAggSourceOperatorX ============

BucketedAggSourceOperatorX::BucketedAggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                       int operator_id, const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs),
          _needs_finalize(tnode.bucketed_agg_node.need_finalize) {}

Status BucketedAggSourceOperatorX::get_block(RuntimeState* state, Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());

    RETURN_IF_ERROR(local_state._get_results(state, block, eos));

    local_state._make_nullable_output_key(block);

    // Apply HAVING clause conjuncts (if any).
    RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block));

    // Apply LIMIT.
    if (!*eos) {
        local_state.reached_limit(block, eos);
    }

    return Status::OK();
}

} // namespace doris
