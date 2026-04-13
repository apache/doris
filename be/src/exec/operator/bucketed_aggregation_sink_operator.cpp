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

#include "exec/operator/bucketed_aggregation_sink_operator.h"

#include <memory>
#include <string>

#include "common/status.h"
#include "exec/common/hash_table/hash.h"
#include "exec/operator/operator.h"
#include "exprs/vectorized_agg_fn.h"
#include "runtime/runtime_profile.h"
#include "runtime/thread_context.h"

namespace doris {

BucketedAggSinkLocalState::BucketedAggSinkLocalState(DataSinkOperatorXBase* parent,
                                                     RuntimeState* state)
        : Base(parent, state) {}

Status BucketedAggSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_init_timer);

    _instance_idx = info.task_idx;

    // Sink dependencies start as ready=false by default. We must explicitly
    // set them to ready so the pipeline task can execute (call sink()).
    // This follows the same pattern as HashJoinBuildSinkLocalState::init().
    _dependency->set_ready();

    _hash_table_size_counter = ADD_COUNTER(custom_profile(), "HashTableSize", TUnit::UNIT);
    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "MemoryUsageHashTable", TUnit::BYTES, 1);

    _build_timer = ADD_TIMER(Base::custom_profile(), "BuildTime");
    _expr_timer = ADD_TIMER(Base::custom_profile(), "ExprTime");
    _hash_table_compute_timer = ADD_TIMER(Base::custom_profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::custom_profile(), "HashTableEmplaceTime");
    _hash_table_input_counter =
            ADD_COUNTER(Base::custom_profile(), "HashTableInputCount", TUnit::UNIT);
    _memory_usage_arena = ADD_COUNTER(custom_profile(), "MemoryUsageArena", TUnit::BYTES);

    return Status::OK();
}

Status BucketedAggSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    auto& p = Base::_parent->template cast<BucketedAggSinkOperatorX>();
    auto& shared_state = *Base::_shared_state;

    // Initialize per-instance data and shared metadata. Multiple sink instances call open()
    // concurrently, so all shared-state writes must be inside call_once to avoid data races.
    // init_instances stores the init status in shared state so all threads see failures.
    RETURN_IF_ERROR(shared_state.init_instances(state->task_num(), [&]() -> Status {
        // Copy metadata to shared state (once, from the first instance to reach here).
        shared_state.align_aggregate_states = p._align_aggregate_states;
        shared_state.total_size_of_aggregate_states = p._total_size_of_aggregate_states;
        shared_state.offsets_of_aggregate_states = p._offsets_of_aggregate_states;
        shared_state.make_nullable_keys = p._make_nullable_keys;

        shared_state.probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
        for (size_t i = 0; i < shared_state.probe_expr_ctxs.size(); i++) {
            RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, shared_state.probe_expr_ctxs[i]));
        }

        for (auto& evaluator : p._aggregate_evaluators) {
            shared_state.aggregate_evaluators.push_back(evaluator->clone(state, p._pool));
        }

        // Detect simple_count: exactly one COUNT(*) with no args, with GROUP BY present.
        // Bucketed agg always has GROUP BY (without-key not supported).
        if (p._aggregate_evaluators.size() == 1 &&
            p._aggregate_evaluators[0]->function()->is_simple_count()) {
            shared_state.use_simple_count = true;
        }
        return Status::OK();
    }));

    // Now safe to access per_instance_data since init_instances has been called.
    auto& inst = shared_state.per_instance_data[_instance_idx];
    _arena = inst.arena.get();

    // Clone probe expression contexts for this sink instance. Each instance needs
    // its own VExprContext because VExprContext::execute() mutates _last_result_column_id
    // and FunctionContext internal state, causing data races when multiple sink
    // instances call execute() concurrently on shared VExprContext objects.
    _probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, _probe_expr_ctxs[i]));
    }

    // Clone aggregate evaluators for this sink instance. Each instance needs
    // its own evaluators because AggFnEvaluator::_calc_argument_columns()
    // mutates internal state (_agg_columns), causing data races when multiple
    // sink instances call execute_batch_add() concurrently on shared evaluators.
    for (auto& evaluator : p._aggregate_evaluators) {
        _aggregate_evaluators.push_back(evaluator->clone(state, p._pool));
    }

    // Set up _bucket_agg_data as 256 pointers into this instance's bucket hash tables.
    _bucket_agg_data.resize(BUCKETED_AGG_NUM_BUCKETS);
    for (int b = 0; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
        _bucket_agg_data[b] = inst.bucket_agg_data[b].get();
    }

    // Initialize hash method for all 256 bucket hash tables. Each bucket uses
    // the same hash key type. We use per-instance _probe_expr_ctxs here (read-only
    // for data types), which is safe since they were cloned above.
    RETURN_IF_ERROR(_init_hash_method(_probe_expr_ctxs));

    return Status::OK();
}

Status BucketedAggSinkLocalState::_create_agg_status(AggregateDataPtr data) {
    auto& shared_state = *Base::_shared_state;
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        try {
            _aggregate_evaluators[i]->create(data + shared_state.offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                _aggregate_evaluators[j]->destroy(data +
                                                  shared_state.offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
    return Status::OK();
}

Status BucketedAggSinkLocalState::_destroy_agg_status(AggregateDataPtr data) {
    auto& shared_state = *Base::_shared_state;
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->destroy(data +
                                                      shared_state.offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status BucketedAggSinkLocalState::_execute_with_serialized_key(Block* block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());
    _memory_usage_last_executing = 0;
    SCOPED_PEAK_MEM(&_memory_usage_last_executing);

    auto& shared_state = *Base::_shared_state;
    auto& p = Base::_parent->template cast<BucketedAggSinkOperatorX>();
    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);

    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(block, &result_column_id));
            block->get_by_position(result_column_id).column =
                    block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            key_columns[i] = block->get_by_position(result_column_id).column.get();
            key_columns[i]->assume_mutable()->replace_float_special_values();
        }
    }

    auto rows = (uint32_t)block->rows();
    if (_places.size() < rows) {
        _places.resize(rows);
    }

    _emplace_into_hash_table(_places.data(), key_columns, rows);

    if (!shared_state.use_simple_count) {
        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add(
                    block, p._offsets_of_aggregate_states[i], _places.data(), *_arena));
        }
    }

    return Status::OK();
}

void BucketedAggSinkLocalState::_emplace_into_hash_table(AggregateDataPtr* places,
                                                         ColumnRawPtrs& key_columns,
                                                         uint32_t num_rows) {
    auto& p = Base::_parent->template cast<BucketedAggSinkOperatorX>();

    // Use bucket 0's method to compute keys and hash values for all rows.
    // All 256 buckets use the same hash key type, so the hash function is identical.
    std::visit(
            Overload {[&](std::monostate& arg) -> void {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                      },
                      [&](auto& agg_method) -> void {
                          using HashMethodType = std::decay_t<decltype(agg_method)>;
                          using AggState = typename HashMethodType::State;
                          AggState state(key_columns);

                          {
                              SCOPED_TIMER(_hash_table_compute_timer);
                              agg_method.init_serialized_keys(key_columns, num_rows);
                          }

                          const bool use_simple_count = Base::_shared_state->use_simple_count;

                          auto creator = [this, &p, use_simple_count](const auto& ctor, auto& key,
                                                                      auto& origin) {
                              HashMethodType::try_presis_key_and_origin(key, origin, *_arena);
                              if (use_simple_count) {
                                  AggregateDataPtr mapped = nullptr;
                                  ctor(key, mapped);
                              } else {
                                  auto mapped =
                                          _arena->aligned_alloc(p._total_size_of_aggregate_states,
                                                                p._align_aggregate_states);
                                  auto st = _create_agg_status(mapped);
                                  if (!st) {
                                      throw Exception(st.code(), st.to_string());
                                  }
                                  ctor(key, mapped);
                              }
                          };

                          auto creator_for_null_key = [this, &p, use_simple_count](auto& mapped) {
                              if (use_simple_count) {
                                  mapped = nullptr;
                              } else {
                                  mapped = _arena->aligned_alloc(p._total_size_of_aggregate_states,
                                                                 p._align_aggregate_states);
                                  auto st = _create_agg_status(mapped);
                                  if (!st) {
                                      throw Exception(st.code(), st.to_string());
                                  }
                              }
                          };

                          {
                              SCOPED_TIMER(_hash_table_emplace_timer);

                              // Phase 1: Pre-group rows by bucket index.
                              // Single pass over hash_values to partition row indices
                              // into 256 bucket groups. This converts random 256-way scatter
                              // into sequential per-bucket batches with good cache locality.
                              for (int b = 0; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
                                  _bucket_row_indices[b].clear();
                              }
                              for (uint32_t i = 0; i < num_rows; ++i) {
                                  int bucket = static_cast<int>((agg_method.hash_values[i] >> 24) &
                                                                0xFF);
                                  _bucket_row_indices[bucket].push_back(i);
                              }

                              // Phase 2: Process each bucket's rows as a batch.
                              // All rows in a bucket hit the same hash table, enabling
                              // effective prefetch and cache-friendly access patterns.
                              for (int b = 0; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
                                  auto& indices = _bucket_row_indices[b];
                                  if (indices.empty()) {
                                      continue;
                                  }

                                  auto& bucket_method = std::get<HashMethodType>(
                                          _bucket_agg_data[b]->method_variant);
                                  auto& bucket_ht = *bucket_method.hash_table;

                                  // Batch emplace with prefetch into this bucket's hash table.
                                  for (size_t j = 0; j < indices.size(); ++j) {
                                      // Prefetch ahead within this bucket's hash table.
                                      if (j + HASH_MAP_PREFETCH_DIST < indices.size()) {
                                          uint32_t prefetch_row =
                                                  indices[j + HASH_MAP_PREFETCH_DIST];
                                          bucket_ht.template prefetch<false>(
                                                  agg_method.keys[prefetch_row],
                                                  agg_method.hash_values[prefetch_row]);
                                      }
                                      uint32_t row = indices[j];
                                      auto* mapped = state.lazy_emplace_key(
                                              bucket_ht, row, agg_method.keys[row],
                                              agg_method.hash_values[row], creator,
                                              creator_for_null_key);
                                      if (use_simple_count) {
                                          ++reinterpret_cast<UInt64&>(*mapped);
                                      } else {
                                          places[row] = *mapped;
                                      }
                                  }
                              }
                          }

                          COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                      }},
            _bucket_agg_data[0]->method_variant);
}

size_t BucketedAggSinkLocalState::_get_hash_table_size() const {
    size_t total = 0;
    for (int b = 0; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
        total += std::visit(Overload {[&](std::monostate& arg) -> size_t { return 0; },
                                      [&](auto& agg_method) -> size_t {
                                          return agg_method.hash_table->size();
                                      }},
                            _bucket_agg_data[b]->method_variant);
    }
    return total;
}

Status BucketedAggSinkLocalState::_init_hash_method(const VExprContextSPtrs& probe_exprs) {
    // Initialize all 256 bucket hash tables with the same hash key type.
    auto data_types = get_data_types(probe_exprs);
    for (int b = 0; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
        RETURN_IF_ERROR(init_hash_method<BucketedAggDataVariants>(_bucket_agg_data[b], data_types,
                                                                  true /* is_first_phase */));
    }
    return Status::OK();
}

void BucketedAggSinkLocalState::_update_memusage() {
    int64_t hash_table_memory_usage = 0;
    for (int b = 0; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
        std::visit(Overload {[&](std::monostate& arg) -> void {},
                             [&](auto& agg_method) -> void {
                                 hash_table_memory_usage +=
                                         agg_method.hash_table->get_buffer_size_in_bytes();
                             }},
                   _bucket_agg_data[b]->method_variant);
    }
    int64_t arena_memory_usage = _arena->size();

    COUNTER_SET(_hash_table_memory_usage, hash_table_memory_usage);
    COUNTER_SET(_memory_usage_arena, arena_memory_usage);
    COUNTER_SET(_memory_used_counter, hash_table_memory_usage + arena_memory_usage);
}

size_t BucketedAggSinkLocalState::get_reserve_mem_size(RuntimeState* state, bool eos) const {
    // Estimate the memory needed for the next batch across all 256 bucket hash tables.
    // Each bucket's hash table may need to resize, so we take the max estimate across
    // all buckets and multiply conservatively. In practice, data is distributed across
    // buckets, so the per-bucket growth is batch_size/256 on average, but we use the
    // full batch_size estimate from the largest bucket for safety.
    size_t size_to_reserve = 0;
    for (int b = 0; b < BUCKETED_AGG_NUM_BUCKETS; ++b) {
        size_to_reserve += std::visit(
                Overload {[&](std::monostate& arg) -> size_t { return 0; },
                          [&](auto& agg_method) -> size_t {
                              // estimate_memory returns the delta needed if we insert
                              // num_elem more entries. Since rows are distributed across
                              // 256 buckets, each bucket sees ~batch_size/256 rows on average.
                              // However, skew is possible, so we use a conservative per-bucket
                              // estimate of batch_size/128 (2x average).
                              auto per_bucket_rows = std::max<size_t>(
                                      1, state->batch_size() / (BUCKETED_AGG_NUM_BUCKETS / 2));
                              return agg_method.hash_table->estimate_memory(per_bucket_rows);
                          }},
                _bucket_agg_data[b]->method_variant);
    }
    size_to_reserve += _memory_usage_last_executing;
    return size_to_reserve;
}

Status BucketedAggSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    if (Base::_closed) {
        return Status::OK();
    }
    PODArray<AggregateDataPtr> tmp_places;
    _places.swap(tmp_places);
    return Base::close(state, exec_status);
}

// ============ BucketedAggSinkOperatorX ============

BucketedAggSinkOperatorX::BucketedAggSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                                   const TPlanNode& tnode,
                                                   const DescriptorTbl& descs)
        : DataSinkOperatorX<BucketedAggSinkLocalState>(operator_id, tnode, dest_id),
          _tuple_id(tnode.bucketed_agg_node.tuple_id),
          _pool(pool) {}

Status BucketedAggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<BucketedAggSinkLocalState>::init(tnode, state));

    RETURN_IF_ERROR(
            VExpr::create_expr_trees(tnode.bucketed_agg_node.grouping_exprs, _probe_expr_ctxs));

    _aggregate_evaluators.reserve(tnode.bucketed_agg_node.aggregate_functions.size());
    TSortInfo dummy;
    for (int i = 0; i < tnode.bucketed_agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(
                _pool, tnode.bucketed_agg_node.aggregate_functions[i], dummy,
                tnode.bucketed_agg_node.grouping_exprs.empty(), false, &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    return Status::OK();
}

Status BucketedAggSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<BucketedAggSinkLocalState>::prepare(state));

    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    _output_tuple_desc = _intermediate_tuple_desc;

    RETURN_IF_ERROR(
            VExpr::prepare(_probe_expr_ctxs, state,
                           DataSinkOperatorX<BucketedAggSinkLocalState>::_child->row_desc()));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));

    size_t j = _probe_expr_ctxs.size();
    for (size_t i = 0; i < j; ++i) {
        auto nullable_output = _output_tuple_desc->slots()[i]->is_nullable();
        auto nullable_input = _probe_expr_ctxs[i]->root()->is_nullable();
        if (nullable_output != nullable_input) {
            DCHECK(nullable_output);
            _make_nullable_keys.emplace_back(i);
        }
    }

    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(
                state, DataSinkOperatorX<BucketedAggSinkLocalState>::_child->row_desc(),
                intermediate_slot_desc, output_slot_desc));
        _aggregate_evaluators[i]->set_version(state->be_exec_version());
    }

    for (auto& evaluator : _aggregate_evaluators) {
        RETURN_IF_ERROR(evaluator->open(state));
    }

    // Compute aggregate state layout.
    _offsets_of_aggregate_states.resize(_aggregate_evaluators.size());
    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
        _offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;
        const auto& agg_function = _aggregate_evaluators[i]->function();
        _align_aggregate_states = std::max(_align_aggregate_states, agg_function->align_of_data());
        _total_size_of_aggregate_states += agg_function->size_of_data();
        if (i + 1 < _aggregate_evaluators.size()) {
            size_t alignment_of_next_state =
                    _aggregate_evaluators[i + 1]->function()->align_of_data();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0) {
                return Status::RuntimeError("Logical error: align_of_data is not 2^N");
            }
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }

    return Status::OK();
}

Status BucketedAggSinkOperatorX::sink(RuntimeState* state, Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    local_state._shared_state->input_num_rows += in_block->rows();

    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(local_state._execute_with_serialized_key(in_block));
        local_state._update_memusage();
        COUNTER_SET(local_state._hash_table_size_counter,
                    (int64_t)local_state._get_hash_table_size());
    }

    if (eos) {
        // Mark this sink instance as finished and try to become the merge target.
        // The first sink to finish becomes the merge target — its bucket hash tables
        // are the destination for all other sinks' data during source-side merge.
        auto* ss = local_state._shared_state;
        int expected = -1;
        ss->merge_target_instance.compare_exchange_strong(expected, local_state._instance_idx);
        // Mark this instance's data as safe to read by source.
        ss->sink_finished[local_state._instance_idx].store(true, std::memory_order_release);
        // Increment finished count, bump state generation, and unblock all source instances.
        // Every sink completion triggers source wakeup so sources can merge
        // newly-available data immediately, rather than waiting for all sinks.
        ss->num_sinks_finished.fetch_add(1, std::memory_order_release);
        ss->state_generation.fetch_add(1, std::memory_order_release);
        for (int i = 0; i < static_cast<int>(ss->source_deps.size()); i++) {
            local_state._dependency->set_ready_to_read(i);
        }
    }
    return Status::OK();
}

size_t BucketedAggSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state.get_reserve_mem_size(state, eos);
}

} // namespace doris
