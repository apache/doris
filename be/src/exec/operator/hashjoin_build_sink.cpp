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

#include "exec/operator/hashjoin_build_sink.h"

#include <cstdlib>
#include <string>
#include <variant>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "exec/common/template_helpers.hpp"
#include "exec/operator/hashjoin_probe_operator.h"
#include "exec/operator/operator.h"
#include "exec/pipeline/pipeline_task.h"
#include "util/pretty_printer.h"
#include "util/uid_util.h"

namespace doris {
HashJoinBuildSinkLocalState::HashJoinBuildSinkLocalState(DataSinkOperatorXBase* parent,
                                                         RuntimeState* state)
        : JoinBuildSinkLocalState(parent, state) {
    _finish_dependency = std::make_shared<CountedFinishDependency>(
            parent->operator_id(), parent->node_id(), parent->get_name() + "_FINISH_DEPENDENCY");
}

Status HashJoinBuildSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(JoinBuildSinkLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _task_idx = info.task_idx;
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    _shared_state->join_op_variants = p._join_op_variants;

    _build_expr_ctxs.resize(p._build_expr_ctxs.size());
    for (size_t i = 0; i < _build_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._build_expr_ctxs[i]->clone(state, _build_expr_ctxs[i]));
    }
    _shared_state->build_exprs_size = _build_expr_ctxs.size();

    _should_build_hash_table = true;
    custom_profile()->add_info_string("BroadcastJoin", std::to_string(p._is_broadcast_join));
    if (p._use_shared_hash_table) {
        _should_build_hash_table = info.task_idx == 0;
    }
    custom_profile()->add_info_string("BuildShareHashTable",
                                      std::to_string(_should_build_hash_table));
    custom_profile()->add_info_string("ShareHashTableEnabled",
                                      std::to_string(p._use_shared_hash_table));
    if (!_should_build_hash_table) {
        _dependency->block();
        _finish_dependency->block();
        {
            std::lock_guard<std::mutex> guard(p._mutex);
            p._finish_dependencies.push_back(_finish_dependency);
        }
    } else {
        _dependency->set_ready();
    }

    _build_blocks_memory_usage =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageBuildBlocks", TUnit::BYTES, 1);
    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageHashTable", TUnit::BYTES, 1);
    _build_arena_memory_usage =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageBuildKeyArena", TUnit::BYTES, 1);

    // Build phase
    auto* record_profile = _should_build_hash_table ? custom_profile() : faker_runtime_profile();
    _build_table_timer = ADD_TIMER(custom_profile(), "BuildHashTableTime");
    _build_side_merge_block_timer = ADD_TIMER(custom_profile(), "MergeBuildBlockTime");
    _build_table_insert_timer = ADD_TIMER(record_profile, "BuildTableInsertTime");
    _build_expr_call_timer = ADD_TIMER(record_profile, "BuildExprCallTime");

    // ASOF index build counters (only for ASOF join types)
    if (is_asof_join(p._join_op) && state->enable_profile() && state->profile_level() >= 2) {
        static constexpr auto ASOF_INDEX_BUILD_TIMER = "AsofIndexBuildTime";
        _asof_index_total_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), ASOF_INDEX_BUILD_TIMER, 2);
        _asof_index_expr_timer = ADD_CHILD_TIMER_WITH_LEVEL(custom_profile(), "AsofIndexExprTime",
                                                            ASOF_INDEX_BUILD_TIMER, 2);
        _asof_index_sort_timer = ADD_CHILD_TIMER_WITH_LEVEL(custom_profile(), "AsofIndexSortTime",
                                                            ASOF_INDEX_BUILD_TIMER, 2);
        _asof_index_group_timer = ADD_CHILD_TIMER_WITH_LEVEL(custom_profile(), "AsofIndexGroupTime",
                                                             ASOF_INDEX_BUILD_TIMER, 2);
    }

    _runtime_filter_producer_helper = std::make_shared<RuntimeFilterProducerHelper>(
            _should_build_hash_table, p._is_broadcast_join);
    RETURN_IF_ERROR(_runtime_filter_producer_helper->init(state, _build_expr_ctxs,
                                                          p._runtime_filter_descs));
    return Status::OK();
}

Status HashJoinBuildSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(JoinBuildSinkLocalState::open(state));
    return Status::OK();
}

Status HashJoinBuildSinkLocalState::terminate(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    if (_terminated) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_runtime_filter_producer_helper->skip_process(state));
    return JoinBuildSinkLocalState::terminate(state);
}

size_t HashJoinBuildSinkLocalState::get_reserve_mem_size(RuntimeState* state, bool eos) {
    if (!_should_build_hash_table) {
        return 0;
    }

    if (_shared_state->build_block) {
        return 0;
    }

    size_t size_to_reserve = 0;

    const size_t build_block_rows = _build_side_mutable_block.rows();
    if (build_block_rows != 0) {
        const auto bytes = _build_side_mutable_block.bytes();
        const auto allocated_bytes = _build_side_mutable_block.allocated_bytes();
        const auto bytes_per_row = bytes / build_block_rows;
        const auto estimated_size_of_next_block = bytes_per_row * state->batch_size();
        // If the new size is greater than 85% of allocalted bytes, it maybe need to realloc.
        if (((estimated_size_of_next_block + bytes) * 100 / allocated_bytes) >= 85) {
            size_to_reserve += static_cast<size_t>(static_cast<double>(allocated_bytes) * 1.15);
        }
    }

    if (eos) {
        const size_t rows = build_block_rows + state->batch_size();
        const auto bucket_size = hash_join_table_calc_bucket_size(rows);

        size_to_reserve += bucket_size * sizeof(uint32_t); // JoinHashTable::first
        size_to_reserve += rows * sizeof(uint32_t);        // JoinHashTable::next

        auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
        if (p._join_op == TJoinOp::FULL_OUTER_JOIN || p._join_op == TJoinOp::RIGHT_OUTER_JOIN ||
            p._join_op == TJoinOp::RIGHT_ANTI_JOIN || p._join_op == TJoinOp::RIGHT_SEMI_JOIN) {
            size_to_reserve += rows * sizeof(uint8_t); // JoinHashTable::visited
        }
        size_to_reserve += _evaluate_mem_usage;

        ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());

        if (build_block_rows > 0) {
            auto block = _build_side_mutable_block.to_block();
            std::vector<uint16_t> converted_columns;
            Defer defer([&]() {
                for (auto i : converted_columns) {
                    auto& data = block.get_by_position(i);
                    data.column = remove_nullable(data.column);
                    data.type = remove_nullable(data.type);
                }
                _build_side_mutable_block = MutableBlock(std::move(block));
            });
            ColumnUInt8::MutablePtr null_map_val;
            if (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN ||
                p._join_op == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
                converted_columns = _convert_block_to_null(block);
                // first row is mocked
                for (int i = 0; i < block.columns(); i++) {
                    auto [column, is_const] = unpack_if_const(block.safe_get_by_position(i).column);
                    assert_cast<ColumnNullable*>(column->assume_mutable().get())
                            ->get_null_map_column()
                            .get_data()
                            .data()[0] = 1;
                }
            }

            null_map_val = ColumnUInt8::create();
            null_map_val->get_data().assign(build_block_rows, (uint8_t)0);

            // Get the key column that needs to be built
            Status st = _extract_join_column(block, null_map_val, raw_ptrs, _build_col_ids);
            if (!st.ok()) {
                throw Exception(st);
            }

            std::visit(Overload {[&](std::monostate& arg) {},
                                 [&](auto&& hash_map_context) {
                                     size_to_reserve += hash_map_context.estimated_size(
                                             raw_ptrs, (uint32_t)block.rows(), true, true,
                                             bucket_size);
                                 }},
                       _shared_state->hash_table_variant_vector.front()->method_variant);
        }
    }
    return size_to_reserve;
}

Status HashJoinBuildSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    Defer defer {[&]() {
        if (!_should_build_hash_table) {
            return;
        }
        // The build side hash key column maybe no need output, but we need to keep the column in block
        // because it is used to compare with probe side hash key column

        if (p._should_keep_hash_key_column && _build_col_ids.size() == 1) {
            // when key column from build side tuple, we should keep it
            // if key column belong to intermediate tuple, it means _build_col_ids[0] >= _should_keep_column_flags.size(),
            // key column still kept too.
            if (_build_col_ids[0] < p._should_keep_column_flags.size()) {
                p._should_keep_column_flags[_build_col_ids[0]] = true;
            }
        }

        if (_shared_state->build_block) {
            // release the memory of unused column in probe stage
            _shared_state->build_block->clear_column_mem_not_keep(p._should_keep_column_flags,
                                                                  p._use_shared_hash_table);
        }

        if (p._use_shared_hash_table) {
            std::unique_lock lock(p._mutex);
            // Only signal non-builder tasks when the builder actually built the hash table.
            // When the builder is terminated (woken up early because the probe side finished
            // first), it never called process_build_block() so the hash table variant is still
            // monostate. Setting _signaled=true in that case would cause non-builder tasks to
            // enter std::visit on monostate and crash with "Hash table type mismatch".
            //
            // _terminated is reliably true here when the task was woken up early, because
            // operator terminate() is called from the execute() Defer in PipelineTask
            // before close() is invoked.
            if (!_terminated) {
                p._signaled = true;
            }
            for (auto& dep : _shared_state->sink_deps) {
                dep->set_ready();
            }
            for (auto& dep : p._finish_dependencies) {
                dep->set_ready();
            }
        }
    }};

    try {
        if (!_terminated && _runtime_filter_producer_helper && !state->is_cancelled()) {
            RETURN_IF_ERROR(_runtime_filter_producer_helper->build(
                    state, _shared_state->build_block.get(), p._use_shared_hash_table,
                    p._runtime_filters));
            // only single join conjunct and left semi join can direct return
            if (p.allow_left_semi_direct_return(state)) {
                auto wrapper = _runtime_filter_producer_helper->detect_local_in_filter(state);
                if (wrapper) {
                    _shared_state->left_semi_direct_return = true;
                    wrapper->set_disable_always_true_logic();
                    custom_profile()->add_info_string(
                            "LeftSemiDirectReturn",
                            std::to_string(_shared_state->left_semi_direct_return));
                }
            }
            RETURN_IF_ERROR(_runtime_filter_producer_helper->publish(state));
        }
    } catch (Exception& e) {
        bool blocked_by_shared_hash_table_signal =
                !_should_build_hash_table && p._use_shared_hash_table && !p._signaled;

        return Status::InternalError(
                "rf process meet error: {}, _terminated: {}, should_build_hash_table: "
                "{}, _finish_dependency: {}, "
                "blocked_by_shared_hash_table_signal: "
                "{}",
                e.to_string(), _terminated, _should_build_hash_table,
                _finish_dependency ? _finish_dependency->debug_string() : "null",
                blocked_by_shared_hash_table_signal);
    }
    if (_runtime_filter_producer_helper) {
        _runtime_filter_producer_helper->collect_realtime_profile(custom_profile());
    }
    return Base::close(state, exec_status);
}

bool HashJoinBuildSinkLocalState::build_unique() const {
    return _parent->cast<HashJoinBuildSinkOperatorX>()._build_unique;
}

void HashJoinBuildSinkLocalState::init_short_circuit_for_probe() {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    bool empty_block =
            !_shared_state->build_block ||
            !(_shared_state->build_block->rows() > 1); // build size always mock a row into block
    _shared_state->short_circuit_for_probe =
            ((_shared_state->_has_null_in_build_side &&
              p._join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) ||
             (empty_block &&
              (p._join_op == TJoinOp::INNER_JOIN || p._join_op == TJoinOp::LEFT_SEMI_JOIN ||
               p._join_op == TJoinOp::RIGHT_OUTER_JOIN || p._join_op == TJoinOp::RIGHT_SEMI_JOIN ||
               p._join_op == TJoinOp::RIGHT_ANTI_JOIN ||
               p._join_op == TJoinOp::ASOF_LEFT_INNER_JOIN))) &&
            !p._is_mark_join;

    //when build table rows is 0 and not have other_join_conjunct and not _is_mark_join and join type is one of LEFT_OUTER_JOIN/FULL_OUTER_JOIN/LEFT_ANTI_JOIN
    //we could get the result is probe table + null-column(if need output)
    _shared_state->empty_right_table_need_probe_dispose =
            (empty_block && !p._have_other_join_conjunct && !p._is_mark_join) &&
            (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN ||
             p._join_op == TJoinOp::LEFT_ANTI_JOIN || p._join_op == TJoinOp::ASOF_LEFT_OUTER_JOIN);
}

Status HashJoinBuildSinkLocalState::build_asof_index(Block& block) {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();

    // Only for ASOF JOIN types
    if (!is_asof_join(p._join_op)) {
        return Status::OK();
    }

    SCOPED_TIMER(_asof_index_total_timer);

    DORIS_CHECK(block.rows() != 0);
    if (block.rows() == 1) {
        // Empty or only mock row
        return Status::OK();
    }

    // Get hash table's first and next arrays to traverse buckets
    uint32_t bucket_size = 0;
    const uint32_t* first_array = nullptr;
    const uint32_t* next_array = nullptr;
    size_t build_rows = 0;

    std::visit(Overload {[&](std::monostate&) {},
                         [&](auto&& hash_table_ctx) {
                             auto* hash_table = hash_table_ctx.hash_table.get();
                             DORIS_CHECK(hash_table);
                             bucket_size = hash_table->get_bucket_size();
                             first_array = hash_table->get_first().data();
                             next_array = hash_table->get_next().data();
                             build_rows = hash_table->size();
                         }},
               _shared_state->hash_table_variant_vector.front()->method_variant);

    if (bucket_size == 0) {
        return Status::OK();
    }
    DORIS_CHECK(first_array);
    DORIS_CHECK(next_array);

    // Set inequality direction from opcode (moved from probe open())
    _shared_state->asof_inequality_is_greater =
            (p._asof_opcode == TExprOpcode::GE || p._asof_opcode == TExprOpcode::GT);
    _shared_state->asof_inequality_is_strict =
            (p._asof_opcode == TExprOpcode::GT || p._asof_opcode == TExprOpcode::LT);

    // Compute build ASOF column by executing build-side expression directly on build block.
    // Expression is prepared against build child's row_desc, matching the build block layout.
    DORIS_CHECK(p._asof_build_side_expr);
    ColumnPtr asof_build_col;
    {
        SCOPED_TIMER(_asof_index_expr_timer);
        RETURN_IF_ERROR(p._asof_build_side_expr->execute(&block, asof_build_col));
    }
    asof_build_col = asof_build_col->convert_to_full_column_if_const();

    // Handle nullable: extract nested column for value access, keep nullable for null checks
    const ColumnNullable* nullable_col = nullptr;
    ColumnPtr build_col_nested = asof_build_col;
    if (asof_build_col->is_nullable()) {
        nullable_col = assert_cast<const ColumnNullable*>(asof_build_col.get());
        build_col_nested = nullable_col->get_nested_column_ptr();
    }

    // Initialize reverse mapping (all build rows, including NULL ASOF values)
    _shared_state->asof_build_row_to_bucket.resize(build_rows + 1, 0);

    // Dispatch on ASOF column type to create typed AsofIndexGroups with inline values.
    // Sub-group by actual key equality within each hash bucket (hash collisions),
    // extract integer representation of ASOF values, then sort by inline values.
    asof_column_dispatch(build_col_nested.get(), [&](const auto* typed_col) {
        using ColType = std::remove_const_t<std::remove_pointer_t<decltype(typed_col)>>;

        if constexpr (std::is_same_v<ColType, IColumn>) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Unsupported ASOF column type for inline optimization");
        } else {
            using IntType = typename ColType::value_type::underlying_value;
            const auto& col_data = typed_col->get_data();

            auto& groups = _shared_state->asof_index_groups
                                   .emplace<std::vector<AsofIndexGroup<IntType>>>();

            std::visit(
                    Overload {
                            [&](std::monostate&) {},
                            [&](auto&& hash_table_ctx) {
                                auto* hash_table = hash_table_ctx.hash_table.get();
                                DORIS_CHECK(hash_table);
                                const auto* build_keys = hash_table->get_build_keys();
                                using KeyType = std::remove_const_t<
                                        std::remove_pointer_t<decltype(build_keys)>>;
                                uint32_t next_group_id = 0;

                                // Group rows by equality key within each hash bucket,
                                // then sort each group by ASOF value only (pure integer compare).
                                // This avoids the previous approach of sorting by (build_key, asof_value)
                                // which required memcmp per comparison.
                                //
                                // For each bucket: walk the chain, find-or-create a group for
                                // each distinct build_key, insert rows into the group with their
                                // inline ASOF value. After all buckets are processed, sort each group.

                                // Map from build_key -> group_id, reused across buckets.
                                // Within a single hash bucket, the number of distinct keys is
                                // typically very small (hash collisions are rare), so a flat
                                // scan is efficient.
                                struct KeyGroupEntry {
                                    KeyType key;
                                    uint32_t group_id;
                                };
                                std::vector<KeyGroupEntry> bucket_key_groups;

                                {
                                    SCOPED_TIMER(_asof_index_group_timer);
                                    for (uint32_t bucket = 0; bucket <= bucket_size; ++bucket) {
                                        uint32_t row_idx = first_array[bucket];
                                        if (row_idx == 0) {
                                            continue;
                                        }

                                        // For each row in this bucket's chain, find-or-create its group
                                        bucket_key_groups.clear();
                                        while (row_idx != 0) {
                                            DCHECK(row_idx <= build_rows);
                                            const auto& key = build_keys[row_idx];

                                            // Linear scan to find existing group for this key.
                                            // Bucket chains are short (avg ~1-2 distinct keys per bucket),
                                            // so this is faster than a hash map.
                                            uint32_t group_id = UINT32_MAX;
                                            for (const auto& entry : bucket_key_groups) {
                                                if (entry.key == key) {
                                                    group_id = entry.group_id;
                                                    break;
                                                }
                                            }
                                            if (group_id == UINT32_MAX) {
                                                group_id = next_group_id++;
                                                DCHECK(group_id == groups.size());
                                                groups.emplace_back();
                                                bucket_key_groups.push_back({key, group_id});
                                            }

                                            _shared_state->asof_build_row_to_bucket[row_idx] =
                                                    group_id;
                                            if (!(nullable_col &&
                                                  nullable_col->is_null_at(row_idx))) {
                                                groups[group_id].add_row(
                                                        col_data[row_idx].to_date_int_val(),
                                                        row_idx);
                                            }

                                            row_idx = next_array[row_idx];
                                        }
                                    }
                                }

                                // Sort each group by ASOF value only (pure integer comparison).
                                {
                                    SCOPED_TIMER(_asof_index_sort_timer);
                                    for (auto& group : groups) {
                                        group.sort_and_finalize();
                                    }
                                }
                            }},
                    _shared_state->hash_table_variant_vector.front()->method_variant);
        }
    });

    return Status::OK();
}

Status HashJoinBuildSinkLocalState::_do_evaluate(Block& block, VExprContextSPtrs& exprs,
                                                 RuntimeProfile::Counter& expr_call_timer,
                                                 std::vector<int>& res_col_ids) {
    auto origin_size = block.allocated_bytes();
    for (size_t i = 0; i < exprs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        {
            SCOPED_TIMER(&expr_call_timer);
            RETURN_IF_ERROR(exprs[i]->execute(&block, &result_col_id));
        }

        // TODO: opt the column is const
        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        res_col_ids[i] = result_col_id;
    }

    _evaluate_mem_usage = block.allocated_bytes() - origin_size;
    return Status::OK();
}

std::vector<uint16_t> HashJoinBuildSinkLocalState::_convert_block_to_null(Block& block) {
    std::vector<uint16_t> results;
    for (int i = 0; i < block.columns(); ++i) {
        if (auto& column_type = block.safe_get_by_position(i); !column_type.type->is_nullable()) {
            DCHECK(!column_type.column->is_nullable());
            column_type.column = make_nullable(column_type.column);
            column_type.type = make_nullable(column_type.type);
            results.emplace_back(i);
        }
    }
    return results;
}

Status HashJoinBuildSinkLocalState::_extract_join_column(Block& block,
                                                         ColumnUInt8::MutablePtr& null_map,
                                                         ColumnRawPtrs& raw_ptrs,
                                                         const std::vector<int>& res_col_ids) {
    DCHECK(_should_build_hash_table);
    auto& shared_state = *_shared_state;
    for (size_t i = 0; i < shared_state.build_exprs_size; ++i) {
        const auto* column = block.get_by_position(res_col_ids[i]).column.get();
        if (!column->is_nullable() &&
            _parent->cast<HashJoinBuildSinkOperatorX>()._serialize_null_into_key[i]) {
            _key_columns_holder.emplace_back(
                    make_nullable(block.get_by_position(res_col_ids[i]).column));
            raw_ptrs[i] = _key_columns_holder.back().get();
        } else if (const auto* nullable = check_and_get_column<ColumnNullable>(*column);
                   !_parent->cast<HashJoinBuildSinkOperatorX>()._serialize_null_into_key[i] &&
                   nullable) {
            // update nulllmap and split nested out of ColumnNullable when serialize_null_into_key is false and column is nullable
            const auto& col_nested = nullable->get_nested_column();
            const auto& col_nullmap = nullable->get_null_map_data();
            DCHECK(null_map);
            VectorizedUtils::update_null_map(null_map->get_data(), col_nullmap);
            raw_ptrs[i] = &col_nested;
        } else {
            raw_ptrs[i] = column;
        }
    }
    return Status::OK();
}

Status HashJoinBuildSinkLocalState::process_build_block(RuntimeState* state, Block& block) {
    DCHECK(_should_build_hash_table);
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    SCOPED_TIMER(_build_table_timer);
    auto rows = (uint32_t)block.rows();
    // 1. Dispose the overflow of ColumnString
    // 2. Finalize the ColumnVariant to speed up
    for (auto& data : block) {
        data.column = std::move(*data.column).mutate()->convert_column_if_overflow();
        if (p._need_finalize_variant_column) {
            std::move(*data.column).mutate()->finalize();
        }
    }

    ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());
    ColumnUInt8::MutablePtr null_map_val;
    if (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN ||
        p._join_op == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
        _convert_block_to_null(block);
        // first row is mocked
        for (int i = 0; i < block.columns(); i++) {
            auto [column, is_const] = unpack_if_const(block.safe_get_by_position(i).column);
            assert_cast<ColumnNullable*>(column->assume_mutable().get())
                    ->get_null_map_column()
                    .get_data()
                    .data()[0] = 1;
        }
    }

    _set_build_side_has_external_nullmap(block, _build_col_ids);
    if (_build_side_has_external_nullmap) {
        null_map_val = ColumnUInt8::create();
        null_map_val->get_data().assign((size_t)rows, (uint8_t)0);
    }

    // Get the key column that needs to be built
    RETURN_IF_ERROR(_extract_join_column(block, null_map_val, raw_ptrs, _build_col_ids));

    RETURN_IF_ERROR(_hash_table_init(state, raw_ptrs));

    Status st = std::visit(
            Overload {[&](std::monostate& arg, auto join_op) -> Status {
                          throw Exception(Status::FatalError("FATAL: uninited hash table"));
                      },
                      [&](auto&& arg, auto&& join_op) -> Status {
                          using HashTableCtxType = std::decay_t<decltype(arg)>;
                          using JoinOpType = std::decay_t<decltype(join_op)>;
                          ProcessHashTableBuild<HashTableCtxType> hash_table_build_process(
                                  rows, raw_ptrs, this, state->batch_size(), state);
                          auto st = hash_table_build_process.template run<JoinOpType::value>(
                                  arg, null_map_val ? &null_map_val->get_data() : nullptr,
                                  &_shared_state->_has_null_in_build_side,
                                  p._short_circuit_for_null_in_build_side,
                                  p._have_other_join_conjunct);
                          COUNTER_SET(_memory_used_counter,
                                      _build_blocks_memory_usage->value() +
                                              (int64_t)(arg.hash_table->get_byte_size() +
                                                        arg.serialized_keys_size(true)));
                          return st;
                      }},
            _shared_state->hash_table_variant_vector.front()->method_variant,
            _shared_state->join_op_variants);
    return st;
}

void HashJoinBuildSinkLocalState::_set_build_side_has_external_nullmap(
        Block& block, const std::vector<int>& res_col_ids) {
    DCHECK(_should_build_hash_table);
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    if (p._short_circuit_for_null_in_build_side) {
        _build_side_has_external_nullmap = true;
        return;
    }
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        const auto* column = block.get_by_position(res_col_ids[i]).column.get();
        if (column->is_nullable() && !p._serialize_null_into_key[i]) {
            _build_side_has_external_nullmap = true;
            return;
        }
    }
}

Status HashJoinBuildSinkLocalState::_hash_table_init(RuntimeState* state,
                                                     const ColumnRawPtrs& raw_ptrs) {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    std::vector<DataTypePtr> data_types;
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        auto& ctx = _build_expr_ctxs[i];
        auto data_type = ctx->root()->data_type();

        /// For 'null safe equal' join,
        /// the build key column maybe be converted to nullable from non-nullable.
        if (p._serialize_null_into_key[i]) {
            data_types.emplace_back(make_nullable(data_type));
        } else {
            // in this case, we use nullmap to represent null value
            data_types.emplace_back(remove_nullable(data_type));
        }
    }
    if (_build_expr_ctxs.size() == 1) {
        p._should_keep_hash_key_column = true;
    }

    std::vector<std::shared_ptr<JoinDataVariants>> variant_ptrs;
    if (p._is_broadcast_join && p._use_shared_hash_table) {
        variant_ptrs = _shared_state->hash_table_variant_vector;
    } else {
        variant_ptrs.emplace_back(
                _shared_state->hash_table_variant_vector[p._use_shared_hash_table ? _task_idx : 0]);
    }

    for (auto& variant_ptr : variant_ptrs) {
        RETURN_IF_ERROR(init_hash_method<JoinDataVariants>(variant_ptr.get(), data_types, true));
    }
    std::visit([&](auto&& arg) { try_convert_to_direct_mapping(&arg, raw_ptrs, variant_ptrs); },
               variant_ptrs[0]->method_variant);
    return Status::OK();
}

HashJoinBuildSinkOperatorX::HashJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id,
                                                       int dest_id, const TPlanNode& tnode,
                                                       const DescriptorTbl& descs)
        : JoinBuildSinkOperatorX(pool, operator_id, dest_id, tnode, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _is_broadcast_join(tnode.hash_join_node.__isset.is_broadcast_join &&
                             tnode.hash_join_node.is_broadcast_join),
          _partition_exprs(tnode.__isset.distribute_expr_lists && !_is_broadcast_join
                                   ? tnode.distribute_expr_lists[1]
                                   : std::vector<TExpr> {}) {}

Status HashJoinBuildSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);

    if (tnode.hash_join_node.__isset.hash_output_slot_ids) {
        _hash_output_slot_ids = tnode.hash_join_node.hash_output_slot_ids;
    }

    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        VExprContextSPtr build_ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.right, build_ctx));
        {
            // for type check
            VExprContextSPtr probe_ctx;
            RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.left, probe_ctx));
            auto build_side_expr_type = build_ctx->root()->data_type();
            auto probe_side_expr_type = probe_ctx->root()->data_type();
            if (!make_nullable(build_side_expr_type)
                         ->equals(*make_nullable(probe_side_expr_type))) {
                return Status::InternalError(
                        "build side type {}, not match probe side type {} , node info "
                        "{}",
                        build_side_expr_type->get_name(), probe_side_expr_type->get_name(),
                        this->debug_string(0));
            }
        }
        _build_expr_ctxs.push_back(build_ctx);

        const auto vexpr = _build_expr_ctxs.back()->root();

        /// null safe equal means null = null is true, the operator in SQL should be: <=>.
        const bool is_null_safe_equal =
                eq_join_conjunct.__isset.opcode &&
                (eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL) &&
                // For a null safe equal join, FE may generate a plan that
                // both sides of the conjuct are not nullable, we just treat it
                // as a normal equal join conjunct.
                (eq_join_conjunct.right.nodes[0].is_nullable ||
                 eq_join_conjunct.left.nodes[0].is_nullable);

        _is_null_safe_eq_join.push_back(is_null_safe_equal);

        if (eq_join_conjuncts.size() == 1) {
            // single column key serialize method must use nullmap for represent null to instead serialize null into key
            _serialize_null_into_key.emplace_back(false);
        } else if (is_null_safe_equal) {
            // use serialize null into key to represent multi column null value
            _serialize_null_into_key.emplace_back(true);
        } else {
            // on normal conditions, because null!=null, it can be expressed directly with nullmap.
            _serialize_null_into_key.emplace_back(false);
        }
    }

    // For ASOF JOIN, extract the build-side expression from match_condition field.
    // match_condition is bound on input tuples (left child output + right child output),
    // so child(1) references build child's slots directly.
    if (is_asof_join(_join_op)) {
        DORIS_CHECK(tnode.hash_join_node.__isset.match_condition);
        DORIS_CHECK(!_asof_build_side_expr);
        VExprContextSPtr full_conjunct;
        RETURN_IF_ERROR(
                VExpr::create_expr_tree(tnode.hash_join_node.match_condition, full_conjunct));
        DORIS_CHECK(full_conjunct);
        DORIS_CHECK(full_conjunct->root());
        DORIS_CHECK(full_conjunct->root()->get_num_children() == 2);
        _asof_opcode = full_conjunct->root()->op();
        auto right_child_expr = full_conjunct->root()->get_child(1);
        _asof_build_side_expr = std::make_shared<VExprContext>(right_child_expr);
    }

    return Status::OK();
}

Status HashJoinBuildSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX<HashJoinBuildSinkLocalState>::prepare(state));
    _use_shared_hash_table =
            _is_broadcast_join && state->enable_share_hash_table_for_broadcast_join();
    auto init_keep_column_flags = [&](auto& tuple_descs, auto& output_slot_flags) {
        for (const auto& tuple_desc : tuple_descs) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                output_slot_flags.emplace_back(
                        std::find(_hash_output_slot_ids.begin(), _hash_output_slot_ids.end(),
                                  slot_desc->id()) != _hash_output_slot_ids.end());
                if (output_slot_flags.back() &&
                    slot_desc->type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
                    _need_finalize_variant_column = true;
                }
            }
        }
    };
    init_keep_column_flags(row_desc().tuple_descriptors(), _should_keep_column_flags);
    RETURN_IF_ERROR(VExpr::prepare(_build_expr_ctxs, state, _child->row_desc()));
    // Prepare ASOF build-side expression against build child's row_desc directly.
    // match_condition is bound on input tuples, so child(1) references build child's slots.
    if (is_asof_join(_join_op)) {
        DORIS_CHECK(_asof_build_side_expr);
        RETURN_IF_ERROR(_asof_build_side_expr->prepare(state, _child->row_desc()));
        RETURN_IF_ERROR(_asof_build_side_expr->open(state));
    }
    return VExpr::open(_build_expr_ctxs, state);
}

Status HashJoinBuildSinkOperatorX::sink(RuntimeState* state, Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    if (local_state._should_build_hash_table) {
        // If eos or have already met a null value using short-circuit strategy, we do not need to pull
        // data from probe side.

        if (local_state._build_side_mutable_block.empty()) {
            auto tmp_build_block =
                    VectorizedUtils::create_empty_columnswithtypename(_child->row_desc());
            tmp_build_block = *(tmp_build_block.create_same_struct_block(1, false));
            local_state._build_col_ids.resize(_build_expr_ctxs.size());
            RETURN_IF_ERROR(local_state._do_evaluate(tmp_build_block, local_state._build_expr_ctxs,
                                                     *local_state._build_expr_call_timer,
                                                     local_state._build_col_ids));
            local_state._build_side_mutable_block =
                    MutableBlock::build_mutable_block(&tmp_build_block);
        }

        if (!in_block->empty()) {
            std::vector<int> res_col_ids(_build_expr_ctxs.size());
            RETURN_IF_ERROR(local_state._do_evaluate(*in_block, local_state._build_expr_ctxs,
                                                     *local_state._build_expr_call_timer,
                                                     res_col_ids));
            local_state._build_side_rows += in_block->rows();
            if (local_state._build_side_rows > std::numeric_limits<uint32_t>::max()) {
                return Status::NotSupported(
                        "Hash join do not support build table rows over: {}, you should enable "
                        "join spill to avoid this issue",
                        std::to_string(std::numeric_limits<uint32_t>::max()));
            }

            SCOPED_TIMER(local_state._build_side_merge_block_timer);
            RETURN_IF_ERROR(local_state._build_side_mutable_block.merge_ignore_overflow(
                    std::move(*in_block)));
            int64_t blocks_mem_usage = local_state._build_side_mutable_block.allocated_bytes();
            COUNTER_SET(local_state._memory_used_counter, blocks_mem_usage);
            COUNTER_SET(local_state._build_blocks_memory_usage, blocks_mem_usage);
        }
    }

    if (local_state._should_build_hash_table && eos) {
        DCHECK(!local_state._build_side_mutable_block.empty());
        local_state._shared_state->build_block =
                std::make_shared<Block>(local_state._build_side_mutable_block.to_block());

        RETURN_IF_ERROR(local_state._runtime_filter_producer_helper->send_filter_size(
                state, local_state._shared_state->build_block->rows(),
                local_state._finish_dependency));

        RETURN_IF_ERROR(
                local_state.process_build_block(state, (*local_state._shared_state->build_block)));
        // For ASOF JOIN, build pre-sorted index for O(log K) lookup
        RETURN_IF_ERROR(local_state.build_asof_index(*local_state._shared_state->build_block));
        local_state.init_short_circuit_for_probe();
    } else if (!local_state._should_build_hash_table) {
        // The non-builder instance waits for the builder (task 0) to finish building the hash table.
        // If _signaled is false, either the builder hasn't finished yet, or the builder was
        // terminated (woken up early) without building the hash table — in both cases, return EOF.
        //
        // The close() Defer in the builder conditionally sets _signaled=true ONLY when the builder
        // was NOT terminated (i.e., the hash table was actually built). When the builder is
        // terminated, _signaled stays false, so non-builders always hit this guard and return EOF
        // safely — never reaching the std::visit on an uninitialized (monostate) hash table.
        //
        // At this point, termination is reflected solely through the value of _signaled: a
        // terminated builder never sets _signaled to true. Checking !_signaled is therefore
        // sufficient and serves as the real guard against racing with an uninitialized hash table.
        if (!_signaled) {
            return Status::Error<ErrorCode::END_OF_FILE>("source has closed");
        }

        DCHECK_LE(local_state._task_idx,
                  local_state._shared_state->hash_table_variant_vector.size());
        std::visit(
                [](auto&& dst, auto&& src) {
                    if constexpr (!std::is_same_v<std::monostate, std::decay_t<decltype(dst)>> &&
                                  std::is_same_v<std::decay_t<decltype(src)>,
                                                 std::decay_t<decltype(dst)>>) {
                        dst.hash_table = src.hash_table;
                    } else {
                        throw Exception(Status::InternalError(
                                "Hash table type mismatch when share hash table"));
                    }
                },
                local_state._shared_state->hash_table_variant_vector[local_state._task_idx]
                        ->method_variant,
                local_state._shared_state->hash_table_variant_vector.front()->method_variant);
    }

    if (eos) {
        // If a shared hash table is used, states are shared by all tasks.
        // Sink and source has n-n relationship If a shared hash table is used otherwise 1-1 relationship.
        // So we should notify the `_task_idx` source task if a shared hash table is used.
        local_state._dependency->set_ready_to_read(_use_shared_hash_table ? local_state._task_idx
                                                                          : 0);
    }

    return Status::OK();
}

size_t HashJoinBuildSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state.get_reserve_mem_size(state, eos);
}

size_t HashJoinBuildSinkOperatorX::get_memory_usage(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._memory_used_counter->value();
}

std::string HashJoinBuildSinkOperatorX::get_memory_usage_debug_str(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return fmt::format("build block: {}, hash table: {}, build key arena: {}",
                       PrettyPrinter::print_bytes(local_state._build_blocks_memory_usage->value()),
                       PrettyPrinter::print_bytes(local_state._hash_table_memory_usage->value()),
                       PrettyPrinter::print_bytes(local_state._build_arena_memory_usage->value()));
}

} // namespace doris
