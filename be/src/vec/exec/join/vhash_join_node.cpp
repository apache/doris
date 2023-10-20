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

#include "vec/exec/join/vhash_join_node.h"

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <opentelemetry/nostd/shared_ptr.h>

#include <algorithm>
#include <array>
#include <boost/iterator/iterator_facade.hpp>
#include <functional>
#include <map>
#include <memory>
#include <new>
#include <ostream>
#include <type_traits>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_slots.h"
#include "gutil/strings/substitute.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/telemetry/telemetry.h"
#include "util/uid_util.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/common/uint128.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exec/join/join_op.h"
#include "vec/exec/join/process_hash_table_probe.h"
#include "vec/exec/join/vjoin_node_base.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/shared_hash_table_controller.h"
#include "vec/utils/template_helpers.hpp"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

static constexpr int PREFETCH_STEP = HashJoinNode::PREFETCH_STEP;

template Status HashJoinNode::_extract_join_column<true>(
        Block&, COW<IColumn>::mutable_ptr<ColumnVector<unsigned char>>&,
        std::vector<IColumn const*, std::allocator<IColumn const*>>&,
        std::vector<int, std::allocator<int>> const&);

template Status HashJoinNode::_extract_join_column<false>(
        Block&, COW<IColumn>::mutable_ptr<ColumnVector<unsigned char>>&,
        std::vector<IColumn const*, std::allocator<IColumn const*>>&,
        std::vector<int, std::allocator<int>> const&);

using ProfileCounter = RuntimeProfile::Counter;

template <typename... Callables>
struct Overload : Callables... {
    using Callables::operator()...;
};

template <typename... Callables>
Overload(Callables&&... callables) -> Overload<Callables...>;

template <class HashTableContext>
struct ProcessHashTableBuild {
    ProcessHashTableBuild(int rows, Block& acquired_block, ColumnRawPtrs& build_raw_ptrs,
                          HashJoinNode* join_node, int batch_size, uint8_t offset,
                          RuntimeState* state)
            : _rows(rows),
              _skip_rows(0),
              _acquired_block(acquired_block),
              _build_raw_ptrs(build_raw_ptrs),
              _join_node(join_node),
              _batch_size(batch_size),
              _offset(offset),
              _state(state),
              _build_side_compute_hash_timer(join_node->_build_side_compute_hash_timer) {}

    template <bool ignore_null, bool short_circuit_for_null>
    Status run(HashTableContext& hash_table_ctx, ConstNullMapPtr null_map, bool* has_null_key) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        Defer defer {[&]() {
            int64_t bucket_size = hash_table_ctx.hash_table.get_buffer_size_in_cells();
            int64_t filled_bucket_size = hash_table_ctx.hash_table.size();
            int64_t bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();
            COUNTER_SET(_join_node->_hash_table_memory_usage, bucket_bytes);
            COUNTER_SET(_join_node->_build_buckets_counter, bucket_size);
            COUNTER_SET(_join_node->_build_collisions_counter,
                        hash_table_ctx.hash_table.get_collisions());
            COUNTER_SET(_join_node->_build_buckets_fill_counter, filled_bucket_size);

            auto hash_table_buckets = hash_table_ctx.hash_table.get_buffer_sizes_in_cells();
            std::string hash_table_buckets_info;
            for (auto bucket_count : hash_table_buckets) {
                hash_table_buckets_info += std::to_string(bucket_count) + ", ";
            }
            _join_node->add_hash_buckets_info(hash_table_buckets_info);

            auto hash_table_sizes = hash_table_ctx.hash_table.sizes();
            hash_table_buckets_info.clear();
            for (auto table_size : hash_table_sizes) {
                hash_table_buckets_info += std::to_string(table_size) + ", ";
            }
            _join_node->add_hash_buckets_filled_info(hash_table_buckets_info);
        }};

        KeyGetter key_getter(_build_raw_ptrs, _join_node->_build_key_sz, nullptr);

        SCOPED_TIMER(_join_node->_build_table_insert_timer);
        hash_table_ctx.hash_table.reset_resize_timer();

        // only not build_unique, we need expanse hash table before insert data
        // 1. There are fewer duplicate keys, reducing the number of resize hash tables
        // can improve performance to a certain extent, about 2%-5%
        // 2. There are many duplicate keys, and the hash table filled bucket is far less than
        // the hash table build bucket, which may waste a lot of memory.
        // TODO, use the NDV expansion of the key column in the optimizer statistics
        if (!_join_node->_build_unique) {
            RETURN_IF_CATCH_EXCEPTION(hash_table_ctx.hash_table.expanse_for_add_elem(
                    std::min<int>(_rows, config::hash_table_pre_expanse_max_rows)));
        }

        vector<int>& inserted_rows = _join_node->_inserted_rows[&_acquired_block];
        bool has_runtime_filter = !_join_node->_runtime_filter_descs.empty();
        if (has_runtime_filter) {
            inserted_rows.reserve(_batch_size);
        }

        _build_side_hash_values.resize(_rows);
        auto& arena = *(_join_node->_arena);
        auto old_build_arena_memory = arena.size();
        {
            SCOPED_TIMER(_build_side_compute_hash_timer);
            if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<KeyGetter>::value) {
                auto old_keys_memory = hash_table_ctx.keys_memory_usage;
                hash_table_ctx.serialize_keys(_build_raw_ptrs, _rows);
                key_getter.set_serialized_keys(hash_table_ctx.keys.data());
                _join_node->_build_arena_memory_usage->add(hash_table_ctx.keys_memory_usage -
                                                           old_keys_memory);
            }

            for (size_t k = 0; k < _rows; ++k) {
                if (k % 65536 == 0) {
                    RETURN_IF_CANCELLED(_state);
                }
                if constexpr (ignore_null) {
                    if ((*null_map)[k]) {
                        continue;
                    }
                }
                // If apply short circuit strategy for null value (e.g. join operator is
                // NULL_AWARE_LEFT_ANTI_JOIN), we build hash table until we meet a null value.
                if constexpr (short_circuit_for_null) {
                    if ((*null_map)[k]) {
                        DCHECK(has_null_key);
                        *has_null_key = true;
                        return Status::OK();
                    }
                }
                if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                                      KeyGetter>::value) {
                    _build_side_hash_values[k] =
                            hash_table_ctx.hash_table.hash(key_getter.get_key_holder(k, arena).key);
                } else {
                    _build_side_hash_values[k] =
                            hash_table_ctx.hash_table.hash(key_getter.get_key_holder(k, arena));
                }
            }
        }

        bool build_unique = _join_node->_build_unique;
#define EMPLACE_IMPL(stmt)                                                                  \
    for (size_t k = 0; k < _rows; ++k) {                                                    \
        if (k % 65536 == 0) {                                                               \
            RETURN_IF_CANCELLED(_state);                                                    \
        }                                                                                   \
        if constexpr (ignore_null) {                                                        \
            if ((*null_map)[k]) {                                                           \
                continue;                                                                   \
            }                                                                               \
        }                                                                                   \
        auto emplace_result = key_getter.emplace_key(hash_table_ctx.hash_table,             \
                                                     _build_side_hash_values[k], k, arena); \
        if (k + PREFETCH_STEP < _rows) {                                                    \
            key_getter.template prefetch_by_hash<false>(                                    \
                    hash_table_ctx.hash_table, _build_side_hash_values[k + PREFETCH_STEP]); \
        }                                                                                   \
        stmt;                                                                               \
    }

        if (has_runtime_filter && build_unique) {
            EMPLACE_IMPL(
                    if (emplace_result.is_inserted()) {
                        new (&emplace_result.get_mapped()) Mapped({k, _offset});
                        inserted_rows.push_back(k);
                        _join_node->_build_bf_cardinality++;
                    } else { _skip_rows++; });
        } else if (has_runtime_filter && !build_unique) {
            EMPLACE_IMPL(
                    if (emplace_result.is_inserted()) {
                        new (&emplace_result.get_mapped()) Mapped({k, _offset});
                        inserted_rows.push_back(k);
                        _join_node->_build_bf_cardinality++;
                    } else {
                        emplace_result.get_mapped().insert({k, _offset}, *(_join_node->_arena));
                        inserted_rows.push_back(k);
                    });
        } else if (!has_runtime_filter && build_unique) {
            EMPLACE_IMPL(
                    if (emplace_result.is_inserted()) {
                        new (&emplace_result.get_mapped()) Mapped({k, _offset});
                    } else { _skip_rows++; });
        } else {
            EMPLACE_IMPL(
                    if (emplace_result.is_inserted()) {
                        new (&emplace_result.get_mapped()) Mapped({k, _offset});
                    } else {
                        emplace_result.get_mapped().insert({k, _offset}, *(_join_node->_arena));
                    });
        }
#undef EMPLACE_IMPL

        _join_node->_build_arena_memory_usage->add(arena.size() - old_build_arena_memory);

        COUNTER_UPDATE(_join_node->_build_table_expanse_timer,
                       hash_table_ctx.hash_table.get_resize_timer_value());
        COUNTER_UPDATE(_join_node->_build_table_convert_timer,
                       hash_table_ctx.hash_table.get_convert_timer_value());

        return Status::OK();
    }

private:
    const int _rows;
    int _skip_rows;
    Block& _acquired_block;
    ColumnRawPtrs& _build_raw_ptrs;
    HashJoinNode* _join_node;
    int _batch_size;
    uint8_t _offset;
    RuntimeState* _state;

    ProfileCounter* _build_side_compute_hash_timer;
    std::vector<size_t> _build_side_hash_values;
};

template <class HashTableContext>
struct ProcessRuntimeFilterBuild {
    ProcessRuntimeFilterBuild(HashJoinNode* join_node) : _join_node(join_node) {}

    Status operator()(RuntimeState* state, HashTableContext& hash_table_ctx) {
        if (_join_node->_runtime_filter_descs.empty()) {
            return Status::OK();
        }
        _join_node->_runtime_filter_slots = std::make_shared<VRuntimeFilterSlots>(
                _join_node->_probe_expr_ctxs, _join_node->_build_expr_ctxs,
                _join_node->_runtime_filter_descs);

        RETURN_IF_ERROR(_join_node->_runtime_filter_slots->init(
                state, hash_table_ctx.hash_table.get_size(), _join_node->_build_bf_cardinality));

        if (!_join_node->_runtime_filter_slots->empty() && !_join_node->_inserted_rows.empty()) {
            {
                SCOPED_TIMER(_join_node->_push_compute_timer);
                _join_node->_runtime_filter_slots->insert(_join_node->_inserted_rows);
            }
        }
        {
            SCOPED_TIMER(_join_node->_push_down_timer);
            RETURN_IF_ERROR(_join_node->_runtime_filter_slots->publish());
        }

        return Status::OK();
    }

private:
    HashJoinNode* _join_node;
};

HashJoinNode::HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : VJoinNodeBase(pool, tnode, descs),
          _is_broadcast_join(tnode.hash_join_node.__isset.is_broadcast_join &&
                             tnode.hash_join_node.is_broadcast_join),
          _hash_output_slot_ids(tnode.hash_join_node.__isset.hash_output_slot_ids
                                        ? tnode.hash_join_node.hash_output_slot_ids
                                        : std::vector<SlotId> {}),
          _build_block_idx(0),
          _build_side_mem_used(0),
          _build_side_last_mem_used(0) {
    _runtime_filter_descs = tnode.runtime_filters;
    _arena = std::make_shared<Arena>();
    _hash_table_variants = std::make_shared<HashTableVariants>();
    _process_hashtable_ctx_variants = std::make_unique<HashTableCtxVariants>();
    _build_blocks.reset(new std::vector<Block>());

    // avoid vector expand change block address.
    // one block can store 4g data, _build_blocks can store 128*4g data.
    // if probe data bigger than 512g, runtime filter maybe will core dump when insert data.
    _build_blocks->reserve(_MAX_BUILD_BLOCK_COUNT);
}

Status HashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VJoinNodeBase::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);

    const bool build_stores_null = _join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                                   _join_op == TJoinOp::FULL_OUTER_JOIN ||
                                   _join_op == TJoinOp::RIGHT_ANTI_JOIN;
    const bool probe_dispose_null =
            _match_all_probe || _build_unique || _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
            _join_op == TJoinOp::LEFT_ANTI_JOIN || _join_op == TJoinOp::LEFT_SEMI_JOIN;

    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    std::vector<bool> probe_not_ignore_null(eq_join_conjuncts.size());
    size_t conjuncts_index = 0;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        VExprContextSPtr ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.left, ctx));
        _probe_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.right, ctx));
        _build_expr_ctxs.push_back(ctx);

        bool null_aware = eq_join_conjunct.__isset.opcode &&
                          eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL;
        _is_null_safe_eq_join.push_back(null_aware);

        // if is null aware, build join column and probe join column both need dispose null value
        _store_null_in_hash_table.emplace_back(
                null_aware ||
                (_build_expr_ctxs.back()->root()->is_nullable() && build_stores_null));
        probe_not_ignore_null[conjuncts_index] =
                null_aware ||
                (_probe_expr_ctxs.back()->root()->is_nullable() && probe_dispose_null);
        conjuncts_index++;
    }
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        _probe_ignore_null |= !probe_not_ignore_null[i];
    }

    _probe_column_disguise_null.reserve(eq_join_conjuncts.size());

    if (tnode.hash_join_node.__isset.other_join_conjuncts &&
        !tnode.hash_join_node.other_join_conjuncts.empty()) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.hash_join_node.other_join_conjuncts,
                                                 _other_join_conjuncts));

        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    } else if (tnode.hash_join_node.__isset.vother_join_conjunct) {
        _other_join_conjuncts.resize(1);
        RETURN_IF_ERROR(VExpr::create_expr_tree(tnode.hash_join_node.vother_join_conjunct,
                                                _other_join_conjuncts[0]));

        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    }

    _runtime_filters.resize(_runtime_filter_descs.size());
    for (size_t i = 0; i < _runtime_filter_descs.size(); i++) {
        RETURN_IF_ERROR(state->runtime_filter_mgr()->register_producer_filter(
                _runtime_filter_descs[i], state->query_options(), _probe_expr_ctxs.size() == 1));
        RETURN_IF_ERROR(state->runtime_filter_mgr()->get_producer_filter(
                _runtime_filter_descs[i].filter_id, &_runtime_filters[i]));
    }

    // init left/right output slots flags, only column of slot_id in _hash_output_slot_ids need
    // insert to output block of hash join.
    // _left_output_slots_flags : column of left table need to output set flag = true
    // _rgiht_output_slots_flags : column of right table need to output set flag = true
    // if _hash_output_slot_ids is empty, means all column of left/right table need to output.
    auto init_output_slots_flags = [this](auto& tuple_descs, auto& output_slot_flags) {
        for (const auto& tuple_desc : tuple_descs) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                output_slot_flags.emplace_back(
                        _hash_output_slot_ids.empty() ||
                        std::find(_hash_output_slot_ids.begin(), _hash_output_slot_ids.end(),
                                  slot_desc->id()) != _hash_output_slot_ids.end());
            }
        }
    };
    init_output_slots_flags(child(0)->row_desc().tuple_descriptors(), _left_output_slot_flags);
    init_output_slots_flags(child(1)->row_desc().tuple_descriptors(), _right_output_slot_flags);

    return Status::OK();
}

Status HashJoinNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VJoinNodeBase::prepare(state));
    _should_build_hash_table = true;
    if (_is_broadcast_join) {
        runtime_profile()->add_info_string("BroadcastJoin", "true");
        if (state->enable_share_hash_table_for_broadcast_join()) {
            runtime_profile()->add_info_string("ShareHashTableEnabled", "true");
            _shared_hashtable_controller =
                    state->get_query_ctx()->get_shared_hash_table_controller();
            _shared_hash_table_context = _shared_hashtable_controller->get_context(id());
            _should_build_hash_table = _shared_hashtable_controller->should_build_hash_table(
                    state->fragment_instance_id(), id());
        } else {
            runtime_profile()->add_info_string("ShareHashTableEnabled", "false");
        }
    }

    _memory_usage_counter = ADD_LABEL_COUNTER(runtime_profile(), "MemoryUsage");

    _build_blocks_memory_usage =
            ADD_CHILD_COUNTER(runtime_profile(), "BuildBlocks", TUnit::BYTES, "MemoryUsage");
    _hash_table_memory_usage =
            ADD_CHILD_COUNTER(runtime_profile(), "HashTable", TUnit::BYTES, "MemoryUsage");
    _build_arena_memory_usage = runtime_profile()->AddHighWaterMarkCounter(
            "BuildKeyArena", TUnit::BYTES, "MemoryUsage");
    _probe_arena_memory_usage = runtime_profile()->AddHighWaterMarkCounter(
            "ProbeKeyArena", TUnit::BYTES, "MemoryUsage");

    // Build phase
    auto record_profile = _should_build_hash_table ? _build_phase_profile : faker_runtime_profile();
    _build_table_timer = ADD_CHILD_TIMER(_build_phase_profile, "BuildTableTime", "BuildTime");
    _build_side_merge_block_timer =
            ADD_CHILD_TIMER(_build_phase_profile, "BuildSideMergeBlockTime", "BuildTime");
    _build_table_insert_timer = ADD_TIMER(record_profile, "BuildTableInsertTime");
    _build_expr_call_timer = ADD_TIMER(record_profile, "BuildExprCallTime");
    _build_table_expanse_timer = ADD_TIMER(record_profile, "BuildTableExpanseTime");
    _build_table_convert_timer = ADD_TIMER(record_profile, "BuildTableConvertToPartitionedTime");
    _build_side_compute_hash_timer = ADD_TIMER(record_profile, "BuildSideHashComputingTime");
    _build_runtime_filter_timer = ADD_TIMER(record_profile, "BuildRuntimeFilterTime");

    // Probe phase
    auto probe_phase_profile = _probe_phase_profile;
    _probe_next_timer = ADD_TIMER(probe_phase_profile, "ProbeFindNextTime");
    _probe_expr_call_timer = ADD_TIMER(probe_phase_profile, "ProbeExprCallTime");
    _search_hashtable_timer =
            ADD_CHILD_TIMER(probe_phase_profile, "ProbeWhenSearchHashTableTime", "ProbeTime");
    _build_side_output_timer =
            ADD_CHILD_TIMER(probe_phase_profile, "ProbeWhenBuildSideOutputTime", "ProbeTime");
    _probe_side_output_timer =
            ADD_CHILD_TIMER(probe_phase_profile, "ProbeWhenProbeSideOutputTime", "ProbeTime");
    _probe_process_hashtable_timer =
            ADD_CHILD_TIMER(probe_phase_profile, "ProbeWhenProcessHashTableTime", "ProbeTime");
    _open_timer = ADD_TIMER(runtime_profile(), "OpenTime");
    _allocate_resource_timer = ADD_TIMER(runtime_profile(), "AllocateResourceTime");
    _process_other_join_conjunct_timer = ADD_TIMER(runtime_profile(), "OtherJoinConjunctTime");

    _build_buckets_counter = ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);
    _build_buckets_fill_counter = ADD_COUNTER(runtime_profile(), "FilledBuckets", TUnit::UNIT);

    _build_collisions_counter = ADD_COUNTER(runtime_profile(), "BuildCollisions", TUnit::UNIT);

    RETURN_IF_ERROR(VExpr::prepare(_build_expr_ctxs, state, child(1)->row_desc()));
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc()));

    // _other_join_conjuncts are evaluated in the context of the rows produced by this node
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
    }
    RETURN_IF_ERROR(VExpr::prepare(_output_expr_ctxs, state, *_intermediate_row_desc));

    // right table data types
    _right_table_data_types = VectorizedUtils::get_data_types(child(1)->row_desc());
    _left_table_data_types = VectorizedUtils::get_data_types(child(0)->row_desc());
    _right_table_column_names = VectorizedUtils::get_column_names(child(1)->row_desc());
    // Hash Table Init
    _hash_table_init(state);
    _construct_mutable_join_block();

    return Status::OK();
}

void HashJoinNode::add_hash_buckets_info(const std::string& info) {
    runtime_profile()->add_info_string("HashTableBuckets", info);
}

void HashJoinNode::add_hash_buckets_filled_info(const std::string& info) {
    runtime_profile()->add_info_string("HashTableFilledBuckets", info);
}

Status HashJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return VJoinNodeBase::close(state);
}

bool HashJoinNode::need_more_input_data() const {
    return (_probe_block.rows() == 0 || _probe_index == _probe_block.rows()) && !_probe_eos &&
           !_short_circuit_for_probe;
}

void HashJoinNode::prepare_for_next() {
    _probe_index = 0;
    _prepare_probe_block();
}

Status HashJoinNode::pull(doris::RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    SCOPED_TIMER(_probe_timer);
    if (_short_circuit_for_probe) {
        // If we use a short-circuit strategy, should return empty block directly.
        *eos = true;
        return Status::OK();
    }

    if (_short_circuit_for_null_in_probe_side && _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN &&
        _is_mark_join) {
        /// If `_short_circuit_for_null_in_probe_side` is true, this indicates no rows
        /// match the join condition, and this is 'mark join', so we need to create a column as mark
        /// with all rows set to 0.
        auto block_rows = _probe_block.rows();
        if (block_rows == 0) {
            *eos = _probe_eos;
            return Status::OK();
        }

        Block temp_block;
        //get probe side output column
        for (int i = 0; i < _left_output_slot_flags.size(); ++i) {
            if (_left_output_slot_flags[i]) {
                temp_block.insert(_probe_block.get_by_position(i));
            }
        }
        auto mark_column = ColumnUInt8::create(block_rows, 0);
        temp_block.insert({std::move(mark_column), std::make_shared<DataTypeUInt8>(), ""});

        {
            SCOPED_TIMER(_join_filter_timer);
            RETURN_IF_ERROR(
                    VExprContext::filter_block(_conjuncts, &temp_block, temp_block.columns()));
        }

        RETURN_IF_ERROR(_build_output_block(&temp_block, output_block, false));
        temp_block.clear();
        release_block_memory(_probe_block);
        reached_limit(output_block, eos);
        return Status::OK();
    }

    //TODO: this short circuit maybe could refactor, no need to check at here.
    if (_empty_right_table_need_probe_dispose) {
        // when build table rows is 0 and not have other_join_conjunct and join type is one of LEFT_OUTER_JOIN/FULL_OUTER_JOIN/LEFT_ANTI_JOIN
        // we could get the result is probe table + null-column(if need output)
        // If we use a short-circuit strategy, should return block directly by add additional null data.
        auto block_rows = _probe_block.rows();
        if (_probe_eos && block_rows == 0) {
            *eos = _probe_eos;
            return Status::OK();
        }

        Block temp_block;
        //get probe side output column
        for (int i = 0; i < _left_output_slot_flags.size(); ++i) {
            temp_block.insert(_probe_block.get_by_position(i));
        }

        //create build side null column, if need output
        for (int i = 0;
             (_join_op != TJoinOp::LEFT_ANTI_JOIN) && i < _right_output_slot_flags.size(); ++i) {
            auto type = remove_nullable(_right_table_data_types[i]);
            auto column = type->create_column();
            column->resize(block_rows);
            auto null_map_column = ColumnVector<UInt8>::create(block_rows, 1);
            auto nullable_column =
                    ColumnNullable::create(std::move(column), std::move(null_map_column));
            temp_block.insert({std::move(nullable_column), make_nullable(type),
                               _right_table_column_names[i]});
        }
        if (_is_outer_join) {
            reinterpret_cast<ColumnUInt8*>(_tuple_is_null_left_flag_column.get())
                    ->get_data()
                    .resize_fill(block_rows, 0);
            reinterpret_cast<ColumnUInt8*>(_tuple_is_null_right_flag_column.get())
                    ->get_data()
                    .resize_fill(block_rows, 1);
        }

        /// No need to check the block size in `_filter_data_and_build_output` because here dose not
        /// increase the output rows count(just same as `_probe_block`'s rows count).
        RETURN_IF_ERROR(
                _filter_data_and_build_output(state, output_block, eos, &temp_block, false));
        temp_block.clear();
        release_block_memory(_probe_block);
        return Status::OK();
    }
    _join_block.clear_column_data();

    MutableBlock mutable_join_block(&_join_block);
    Block temp_block;

    Status st;
    if (_probe_index < _probe_block.rows()) {
        DCHECK(_has_set_need_null_map_for_probe);
        RETURN_IF_CATCH_EXCEPTION({
            std::visit(
                    [&](auto&& arg, auto&& process_hashtable_ctx, auto need_null_map_for_probe,
                        auto ignore_null) {
                        using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                        if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                            using HashTableCtxType = std::decay_t<decltype(arg)>;
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                if (_have_other_join_conjunct) {
                                    st = process_hashtable_ctx
                                                 .template do_process_with_other_join_conjuncts<
                                                         need_null_map_for_probe, ignore_null>(
                                                         arg,
                                                         need_null_map_for_probe
                                                                 ? &_null_map_column->get_data()
                                                                 : nullptr,
                                                         mutable_join_block, &temp_block,
                                                         _probe_block.rows(), _is_mark_join);
                                } else {
                                    st = process_hashtable_ctx.template do_process<
                                            need_null_map_for_probe, ignore_null>(
                                            arg,
                                            need_null_map_for_probe ? &_null_map_column->get_data()
                                                                    : nullptr,
                                            mutable_join_block, &temp_block, _probe_block.rows(),
                                            _is_mark_join);
                                }
                            } else {
                                LOG(FATAL) << "FATAL: uninited hash table";
                            }
                        } else {
                            LOG(FATAL) << "FATAL: uninited hash table probe";
                        }
                    },
                    *_hash_table_variants, *_process_hashtable_ctx_variants,
                    make_bool_variant(_need_null_map_for_probe),
                    make_bool_variant(_probe_ignore_null));
        });
    } else if (_probe_eos) {
        if (_is_right_semi_anti || (_is_outer_join && _join_op != TJoinOp::LEFT_OUTER_JOIN)) {
            std::visit(
                    [&](auto&& arg, auto&& process_hashtable_ctx) {
                        using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                        if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                            using HashTableCtxType = std::decay_t<decltype(arg)>;
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                st = process_hashtable_ctx.process_data_in_hashtable(
                                        arg, mutable_join_block, &temp_block, eos);
                            } else {
                                LOG(FATAL) << "FATAL: uninited hash table";
                            }
                        } else {
                            LOG(FATAL) << "FATAL: uninited hash table probe";
                        }
                    },
                    *_hash_table_variants, *_process_hashtable_ctx_variants);
        } else {
            *eos = true;
            return Status::OK();
        }
    } else {
        return Status::OK();
    }
    if (!st) {
        return st;
    }
    RETURN_IF_ERROR(_filter_data_and_build_output(state, output_block, eos, &temp_block));
    // Here make _join_block release the columns' ptr
    _join_block.set_columns(_join_block.clone_empty_columns());
    mutable_join_block.clear();
    return Status::OK();
}

Status HashJoinNode::_filter_data_and_build_output(RuntimeState* state,
                                                   vectorized::Block* output_block, bool* eos,
                                                   Block* temp_block, bool check_rows_count) {
    if (_is_outer_join) {
        _add_tuple_is_null_column(temp_block);
    }
    auto output_rows = temp_block->rows();
    if (check_rows_count) { // If the join node does not increase the number of output rows, no need to check.
        DCHECK(output_rows <= state->batch_size());
    }
    {
        SCOPED_TIMER(_join_filter_timer);
        RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, temp_block, temp_block->columns()));
    }

    RETURN_IF_ERROR(_build_output_block(temp_block, output_block, false));
    _reset_tuple_is_null_column();
    reached_limit(output_block, eos);
    return Status::OK();
}

Status HashJoinNode::push(RuntimeState* /*state*/, vectorized::Block* input_block, bool eos) {
    _probe_eos = eos;
    if (input_block->rows() > 0) {
        COUNTER_UPDATE(_probe_rows_counter, input_block->rows());
        int probe_expr_ctxs_sz = _probe_expr_ctxs.size();
        _probe_columns.resize(probe_expr_ctxs_sz);

        std::vector<int> res_col_ids(probe_expr_ctxs_sz);
        if (_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN) {
            _probe_column_convert_to_null = _convert_block_to_null(*input_block);
        }
        RETURN_IF_ERROR(
                _do_evaluate(*input_block, _probe_expr_ctxs, *_probe_expr_call_timer, res_col_ids));

        // TODO: Now we are not sure whether a column is nullable only by ExecNode's `row_desc`
        //  so we have to initialize this flag by the first probe block.
        if (!_has_set_need_null_map_for_probe) {
            _has_set_need_null_map_for_probe = true;
            _need_null_map_for_probe = _need_probe_null_map(*input_block, res_col_ids);
        }
        if (_need_null_map_for_probe) {
            if (_null_map_column == nullptr) {
                _null_map_column = ColumnUInt8::create();
            }
            _null_map_column->get_data().assign(input_block->rows(), (uint8_t)0);
        }

        RETURN_IF_ERROR(_extract_join_column<false>(*input_block, _null_map_column, _probe_columns,
                                                    res_col_ids));
        if (&_probe_block != input_block) {
            input_block->swap(_probe_block);
        }
    }
    return Status::OK();
}

Status HashJoinNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    // If we use a short-circuit strategy, should return empty block directly.
    if (_short_circuit_for_probe) {
        *eos = true;
        return Status::OK();
    }

    if (_join_op == TJoinOp::RIGHT_OUTER_JOIN) {
        const auto hash_table_empty = std::visit(
                Overload {[&](std::monostate&) -> bool {
                              LOG(FATAL) << "FATAL: uninited hash table";
                              __builtin_unreachable();
                          },
                          [&](auto&& arg) -> bool { return arg.hash_table.size() == 0; }},
                *_hash_table_variants);

        if (hash_table_empty) {
            *eos = true;
            return Status::OK();
        }
    }

    while (need_more_input_data()) {
        prepare_for_next();
        SCOPED_TIMER(_probe_next_timer);
        RETURN_IF_ERROR(child(0)->get_next_after_projects(
                state, &_probe_block, &_probe_eos,
                std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                  ExecNode::get_next,
                          _children[0], std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));

        RETURN_IF_ERROR(push(state, &_probe_block, _probe_eos));
    }

    return pull(state, output_block, eos);
}

void HashJoinNode::_add_tuple_is_null_column(Block* block) {
    DCHECK(_is_outer_join);
    auto p0 = _tuple_is_null_left_flag_column->assume_mutable();
    auto p1 = _tuple_is_null_right_flag_column->assume_mutable();
    auto& left_null_map = reinterpret_cast<ColumnUInt8&>(*p0);
    auto& right_null_map = reinterpret_cast<ColumnUInt8&>(*p1);
    auto left_size = left_null_map.size();
    auto right_size = right_null_map.size();

    if (left_size == 0) {
        DCHECK_EQ(right_size, block->rows());
        left_null_map.get_data().resize_fill(right_size, 0);
    }
    if (right_size == 0) {
        DCHECK_EQ(left_size, block->rows());
        right_null_map.get_data().resize_fill(left_size, 0);
    }

    block->insert(
            {std::move(p0), std::make_shared<vectorized::DataTypeUInt8>(), "left_tuples_is_null"});
    block->insert(
            {std::move(p1), std::make_shared<vectorized::DataTypeUInt8>(), "right_tuples_is_null"});
}

void HashJoinNode::_prepare_probe_block() {
    // clear_column_data of _probe_block
    if (!_probe_column_disguise_null.empty()) {
        for (int i = 0; i < _probe_column_disguise_null.size(); ++i) {
            auto column_to_erase = _probe_column_disguise_null[i];
            _probe_block.erase(column_to_erase - i);
        }
        _probe_column_disguise_null.clear();
    }

    // remove add nullmap of probe columns
    for (auto index : _probe_column_convert_to_null) {
        auto& column_type = _probe_block.safe_get_by_position(index);
        DCHECK(column_type.column->is_nullable() || is_column_const(*(column_type.column.get())));
        DCHECK(column_type.type->is_nullable());

        column_type.column = remove_nullable(column_type.column);
        column_type.type = remove_nullable(column_type.type);
    }
    release_block_memory(_probe_block);
}

Status HashJoinNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(VJoinNodeBase::open(state));
    RETURN_IF_CANCELLED(state);
    return Status::OK();
}

Status HashJoinNode::alloc_resource(doris::RuntimeState* state) {
    SCOPED_TIMER(_allocate_resource_timer);
    RETURN_IF_ERROR(VJoinNodeBase::alloc_resource(state));
    for (size_t i = 0; i < _runtime_filter_descs.size(); i++) {
        if (auto bf = _runtime_filters[i]->get_bloomfilter()) {
            RETURN_IF_ERROR(bf->init_with_fixed_length());
        }
    }
    RETURN_IF_ERROR(VExpr::open(_build_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }
    return Status::OK();
}

void HashJoinNode::release_resource(RuntimeState* state) {
    _release_mem();
    VJoinNodeBase::release_resource(state);
}

Status HashJoinNode::_materialize_build_side(RuntimeState* state) {
    {
        SCOPED_TIMER(_build_get_next_timer);
        RETURN_IF_ERROR(child(1)->open(state));
    }
    if (_should_build_hash_table) {
        bool eos = false;
        Block block;
        // If eos or have already met a null value using short-circuit strategy, we do not need to pull
        // data from data.
        while (!eos && !_short_circuit_for_null_in_probe_side) {
            block.clear_column_data();
            RETURN_IF_CANCELLED(state);
            {
                SCOPED_TIMER(_build_get_next_timer);
                RETURN_IF_ERROR(child(1)->get_next_after_projects(
                        state, &block, &eos,
                        std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                          ExecNode::get_next,
                                  _children[1], std::placeholders::_1, std::placeholders::_2,
                                  std::placeholders::_3)));
            }
            RETURN_IF_ERROR(sink(state, &block, eos));
        }
        RETURN_IF_ERROR(child(1)->close(state));
    } else {
        RETURN_IF_ERROR(child(1)->close(state));
        RETURN_IF_ERROR(sink(state, nullptr, true));
    }
    return Status::OK();
}

Status HashJoinNode::sink(doris::RuntimeState* state, vectorized::Block* in_block, bool eos) {
    SCOPED_TIMER(_build_timer);

    // make one block for each 4 gigabytes
    constexpr static auto BUILD_BLOCK_MAX_SIZE = 4 * 1024UL * 1024UL * 1024UL;

    if (_short_circuit_for_null_in_probe_side) {
        // TODO: if _short_circuit_for_null_in_probe_side is true we should finish current pipeline task.
        DCHECK(state->enable_pipeline_exec());
        return Status::OK();
    }
    if (_should_build_hash_table) {
        // If eos or have already met a null value using short-circuit strategy, we do not need to pull
        // data from probe side.
        _build_side_mem_used += in_block->allocated_bytes();

        if (in_block->rows() != 0) {
            SCOPED_TIMER(_build_side_merge_block_timer);
            RETURN_IF_ERROR(_build_side_mutable_block.merge(*in_block));
        }

        if (UNLIKELY(_build_side_mem_used - _build_side_last_mem_used > BUILD_BLOCK_MAX_SIZE)) {
            if (_build_blocks->size() == _MAX_BUILD_BLOCK_COUNT) {
                return Status::NotSupported(
                        strings::Substitute("data size of right table in hash join > $0",
                                            BUILD_BLOCK_MAX_SIZE * _MAX_BUILD_BLOCK_COUNT));
            }
            _build_blocks->emplace_back(_build_side_mutable_block.to_block());

            COUNTER_UPDATE(_build_blocks_memory_usage, (*_build_blocks)[_build_block_idx].bytes());

            // TODO:: Rethink may we should do the process after we receive all build blocks ?
            // which is better.
            RETURN_IF_ERROR(_process_build_block(state, (*_build_blocks)[_build_block_idx],
                                                 _build_block_idx));

            _build_side_mutable_block = MutableBlock();
            ++_build_block_idx;
            _build_side_last_mem_used = _build_side_mem_used;
        }
    }

    if (_should_build_hash_table && eos) {
        if (!_build_side_mutable_block.empty()) {
            if (_build_blocks->size() == _MAX_BUILD_BLOCK_COUNT) {
                return Status::NotSupported(
                        strings::Substitute("data size of right table in hash join > $0",
                                            BUILD_BLOCK_MAX_SIZE * _MAX_BUILD_BLOCK_COUNT));
            }
            _build_blocks->emplace_back(_build_side_mutable_block.to_block());
            COUNTER_UPDATE(_build_blocks_memory_usage, (*_build_blocks)[_build_block_idx].bytes());
            RETURN_IF_ERROR(_process_build_block(state, (*_build_blocks)[_build_block_idx],
                                                 _build_block_idx));
        }
        auto ret = std::visit(Overload {[&](std::monostate&) -> Status {
                                            LOG(FATAL) << "FATAL: uninited hash table";
                                            __builtin_unreachable();
                                        },
                                        [&](auto&& arg) -> Status {
                                            using HashTableCtxType = std::decay_t<decltype(arg)>;
                                            ProcessRuntimeFilterBuild<HashTableCtxType>
                                                    runtime_filter_build_process(this);
                                            return runtime_filter_build_process(state, arg);
                                        }},
                              *_hash_table_variants);
        if (!ret.ok()) {
            if (_shared_hashtable_controller) {
                _shared_hash_table_context->status = ret;
                _shared_hashtable_controller->signal(id());
            }
            return ret;
        }
        if (_shared_hashtable_controller) {
            _shared_hash_table_context->status = Status::OK();
            // arena will be shared with other instances.
            _shared_hash_table_context->arena = _arena;
            _shared_hash_table_context->blocks = _build_blocks;
            _shared_hash_table_context->hash_table_variants = _hash_table_variants;
            _shared_hash_table_context->short_circuit_for_null_in_probe_side =
                    _short_circuit_for_null_in_probe_side;
            if (_runtime_filter_slots) {
                _runtime_filter_slots->copy_to_shared_context(_shared_hash_table_context);
            }
            _shared_hashtable_controller->signal(id());
        }
    } else if (!_should_build_hash_table) {
        DCHECK(_shared_hashtable_controller != nullptr);
        DCHECK(_shared_hash_table_context != nullptr);
        auto wait_timer =
                ADD_CHILD_TIMER(_build_phase_profile, "WaitForSharedHashTableTime", "BuildTime");
        SCOPED_TIMER(wait_timer);
        RETURN_IF_ERROR(
                _shared_hashtable_controller->wait_for_signal(state, _shared_hash_table_context));

        _build_phase_profile->add_info_string(
                "SharedHashTableFrom",
                print_id(_shared_hashtable_controller->get_builder_fragment_instance_id(id())));
        _short_circuit_for_null_in_probe_side =
                _shared_hash_table_context->short_circuit_for_null_in_probe_side;
        _hash_table_variants = std::static_pointer_cast<HashTableVariants>(
                _shared_hash_table_context->hash_table_variants);
        _build_blocks = _shared_hash_table_context->blocks;

        if (!_shared_hash_table_context->runtime_filters.empty()) {
            auto ret = std::visit(
                    Overload {[&](std::monostate&) -> Status {
                                  LOG(FATAL) << "FATAL: uninited hash table";
                                  __builtin_unreachable();
                              },
                              [&](auto&& arg) -> Status {
                                  if (_runtime_filter_descs.empty()) {
                                      return Status::OK();
                                  }
                                  _runtime_filter_slots = std::make_shared<VRuntimeFilterSlots>(
                                          _probe_expr_ctxs, _build_expr_ctxs,
                                          _runtime_filter_descs);

                                  RETURN_IF_ERROR(_runtime_filter_slots->init(
                                          state, arg.hash_table.get_size(), 0));
                                  RETURN_IF_ERROR(_runtime_filter_slots->copy_from_shared_context(
                                          _shared_hash_table_context));
                                  RETURN_IF_ERROR(_runtime_filter_slots->publish());
                                  return Status::OK();
                              }},
                    *_hash_table_variants);
            RETURN_IF_ERROR(ret);
        }
    }

    if (eos) {
        _process_hashtable_ctx_variants_init(state);
    }

    // Since the comparison of null values is meaningless, null aware left anti join should not output null
    // when the build side is not empty.
    if (!_build_blocks->empty() && _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        _probe_ignore_null = true;
    }
    _init_short_circuit_for_probe();

    return Status::OK();
}

void HashJoinNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "HashJoin(need_more_input_data=" << (need_more_input_data() ? "true" : "false")
         << " _probe_block.rows()=" << _probe_block.rows() << " _probe_index=" << _probe_index
         << " _probe_eos=" << _probe_eos
         << " _short_circuit_for_probe_side=" << _short_circuit_for_probe;
    *out << ")\n children=(";
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

template <bool BuildSide>
Status HashJoinNode::_extract_join_column(Block& block, ColumnUInt8::MutablePtr& null_map,
                                          ColumnRawPtrs& raw_ptrs,
                                          const std::vector<int>& res_col_ids) {
    DCHECK_EQ(_build_expr_ctxs.size(), _probe_expr_ctxs.size());
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        if (_is_null_safe_eq_join[i]) {
            raw_ptrs[i] = block.get_by_position(res_col_ids[i]).column.get();
        } else {
            auto column = block.get_by_position(res_col_ids[i]).column.get();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
                auto& col_nested = nullable->get_nested_column();
                auto& col_nullmap = nullable->get_null_map_data();

                if constexpr (!BuildSide) {
                    DCHECK(null_map != nullptr);
                    VectorizedUtils::update_null_map(null_map->get_data(), col_nullmap);
                }
                if (_store_null_in_hash_table[i]) {
                    raw_ptrs[i] = nullable;
                } else {
                    if constexpr (BuildSide) {
                        DCHECK(null_map != nullptr);
                        VectorizedUtils::update_null_map(null_map->get_data(), col_nullmap);
                    }
                    raw_ptrs[i] = &col_nested;
                }
            } else {
                raw_ptrs[i] = column;
            }
        }
    }
    return Status::OK();
}

Status HashJoinNode::_do_evaluate(Block& block, VExprContextSPtrs& exprs,
                                  RuntimeProfile::Counter& expr_call_timer,
                                  std::vector<int>& res_col_ids) {
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
    return Status::OK();
}

bool HashJoinNode::_need_probe_null_map(Block& block, const std::vector<int>& res_col_ids) {
    DCHECK_EQ(_build_expr_ctxs.size(), _probe_expr_ctxs.size());
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        if (!_is_null_safe_eq_join[i]) {
            auto column = block.get_by_position(res_col_ids[i]).column.get();
            if (check_and_get_column<ColumnNullable>(*column)) {
                return true;
            }
        }
    }
    return false;
}

void HashJoinNode::_set_build_ignore_flag(Block& block, const std::vector<int>& res_col_ids) {
    DCHECK_EQ(_build_expr_ctxs.size(), _probe_expr_ctxs.size());
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        if (!_is_null_safe_eq_join[i]) {
            auto column = block.get_by_position(res_col_ids[i]).column.get();
            if (check_and_get_column<ColumnNullable>(*column)) {
                _build_side_ignore_null |= (_join_op != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN &&
                                            !_store_null_in_hash_table[i]);
            }
        }
    }
}

Status HashJoinNode::_process_build_block(RuntimeState* state, Block& block, uint8_t offset) {
    SCOPED_TIMER(_build_table_timer);
    size_t rows = block.rows();
    if (UNLIKELY(rows == 0)) {
        return Status::OK();
    }
    COUNTER_UPDATE(_build_rows_counter, rows);

    ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());

    ColumnUInt8::MutablePtr null_map_val;
    std::vector<int> res_col_ids(_build_expr_ctxs.size());
    RETURN_IF_ERROR(_do_evaluate(block, _build_expr_ctxs, *_build_expr_call_timer, res_col_ids));
    if (_join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN) {
        _convert_block_to_null(block);
    }
    // TODO: Now we are not sure whether a column is nullable only by ExecNode's `row_desc`
    //  so we have to initialize this flag by the first build block.
    if (!_has_set_need_null_map_for_build) {
        _has_set_need_null_map_for_build = true;
        _set_build_ignore_flag(block, res_col_ids);
    }
    if (_short_circuit_for_null_in_build_side || _build_side_ignore_null) {
        null_map_val = ColumnUInt8::create();
        null_map_val->get_data().assign(rows, (uint8_t)0);
    }

    // Get the key column that needs to be built
    Status st = _extract_join_column<true>(block, null_map_val, raw_ptrs, res_col_ids);

    st = std::visit(
            Overload {
                    [&](std::monostate& arg, auto has_null_value,
                        auto short_circuit_for_null_in_build_side) -> Status {
                        LOG(FATAL) << "FATAL: uninited hash table";
                        __builtin_unreachable();
                        return Status::OK();
                    },
                    [&](auto&& arg, auto has_null_value,
                        auto short_circuit_for_null_in_build_side) -> Status {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        ProcessHashTableBuild<HashTableCtxType> hash_table_build_process(
                                rows, block, raw_ptrs, this, state->batch_size(), offset, state);
                        return hash_table_build_process
                                .template run<has_null_value, short_circuit_for_null_in_build_side>(
                                        arg,
                                        has_null_value || short_circuit_for_null_in_build_side
                                                ? &null_map_val->get_data()
                                                : nullptr,
                                        &_short_circuit_for_null_in_probe_side);
                    }},
            *_hash_table_variants, make_bool_variant(_build_side_ignore_null),
            make_bool_variant(_short_circuit_for_null_in_build_side));

    return st;
}

void HashJoinNode::_hash_table_init(RuntimeState* state) {
    std::visit(
            [&](auto&& join_op_variants, auto have_other_join_conjunct) {
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                using RowRefListType = std::conditional_t<
                        have_other_join_conjunct, RowRefListWithFlags,
                        std::conditional_t<JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN ||
                                                   JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                                                   JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN ||
                                                   JoinOpType::value == TJoinOp::FULL_OUTER_JOIN,
                                           RowRefListWithFlag, RowRefList>>;
                _probe_row_match_iter.emplace<ForwardIterator<RowRefListType>>();
                _outer_join_pull_visited_iter.emplace<ForwardIterator<RowRefListType>>();

                if (_build_expr_ctxs.size() == 1 && !_store_null_in_hash_table[0]) {
                    // Single column optimization
                    switch (_build_expr_ctxs[0]->root()->result_type()) {
                    case TYPE_BOOLEAN:
                    case TYPE_TINYINT:
                        _hash_table_variants->emplace<I8HashTableContext<RowRefListType>>();
                        break;
                    case TYPE_SMALLINT:
                        _hash_table_variants->emplace<I16HashTableContext<RowRefListType>>();
                        break;
                    case TYPE_INT:
                    case TYPE_FLOAT:
                    case TYPE_DATEV2:
                        _hash_table_variants->emplace<I32HashTableContext<RowRefListType>>();
                        break;
                    case TYPE_BIGINT:
                    case TYPE_DOUBLE:
                    case TYPE_DATETIME:
                    case TYPE_DATE:
                    case TYPE_DATETIMEV2:
                        _hash_table_variants->emplace<I64HashTableContext<RowRefListType>>();
                        break;
                    case TYPE_LARGEINT:
                    case TYPE_DECIMALV2:
                    case TYPE_DECIMAL32:
                    case TYPE_DECIMAL64:
                    case TYPE_DECIMAL128I: {
                        DataTypePtr& type_ptr = _build_expr_ctxs[0]->root()->data_type();
                        TypeIndex idx = _build_expr_ctxs[0]->root()->is_nullable()
                                                ? assert_cast<const DataTypeNullable&>(*type_ptr)
                                                          .get_nested_type()
                                                          ->get_type_id()
                                                : type_ptr->get_type_id();
                        WhichDataType which(idx);
                        if (which.is_decimal32()) {
                            _hash_table_variants->emplace<I32HashTableContext<RowRefListType>>();
                        } else if (which.is_decimal64()) {
                            _hash_table_variants->emplace<I64HashTableContext<RowRefListType>>();
                        } else {
                            _hash_table_variants->emplace<I128HashTableContext<RowRefListType>>();
                        }
                        break;
                    }
                    default:
                        _hash_table_variants->emplace<SerializedHashTableContext<RowRefListType>>();
                    }
                    return;
                }

                bool use_fixed_key = true;
                bool has_null = false;
                int key_byte_size = 0;

                _probe_key_sz.resize(_probe_expr_ctxs.size());
                _build_key_sz.resize(_build_expr_ctxs.size());

                for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
                    const auto vexpr = _build_expr_ctxs[i]->root();
                    const auto& data_type = vexpr->data_type();

                    if (!data_type->have_maximum_size_of_value()) {
                        use_fixed_key = false;
                        break;
                    }

                    auto is_null = data_type->is_nullable();
                    has_null |= is_null;
                    _build_key_sz[i] =
                            data_type->get_maximum_size_of_value_in_memory() - (is_null ? 1 : 0);
                    _probe_key_sz[i] = _build_key_sz[i];
                    key_byte_size += _probe_key_sz[i];
                }

                if (std::tuple_size<KeysNullMap<UInt256>>::value + key_byte_size >
                    sizeof(UInt256)) {
                    use_fixed_key = false;
                }

                if (use_fixed_key) {
                    // TODO: may we should support uint256 in the future
                    if (has_null) {
                        if (std::tuple_size<KeysNullMap<UInt64>>::value + key_byte_size <=
                            sizeof(UInt64)) {
                            _hash_table_variants
                                    ->emplace<I64FixedKeyHashTableContext<true, RowRefListType>>();
                        } else if (std::tuple_size<KeysNullMap<UInt128>>::value + key_byte_size <=
                                   sizeof(UInt128)) {
                            _hash_table_variants
                                    ->emplace<I128FixedKeyHashTableContext<true, RowRefListType>>();
                        } else {
                            _hash_table_variants
                                    ->emplace<I256FixedKeyHashTableContext<true, RowRefListType>>();
                        }
                    } else {
                        if (key_byte_size <= sizeof(UInt64)) {
                            _hash_table_variants
                                    ->emplace<I64FixedKeyHashTableContext<false, RowRefListType>>();
                        } else if (key_byte_size <= sizeof(UInt128)) {
                            _hash_table_variants->emplace<
                                    I128FixedKeyHashTableContext<false, RowRefListType>>();
                        } else {
                            _hash_table_variants->emplace<
                                    I256FixedKeyHashTableContext<false, RowRefListType>>();
                        }
                    }
                } else {
                    _hash_table_variants->emplace<SerializedHashTableContext<RowRefListType>>();
                }
            },
            _join_op_variants, make_bool_variant(_have_other_join_conjunct));

    DCHECK(!std::holds_alternative<std::monostate>(*_hash_table_variants));

    std::visit(Overload {[&](std::monostate& arg) {
                             LOG(FATAL) << "FATAL: uninited hash table";
                             __builtin_unreachable();
                         },
                         [&](auto&& arg) {
                             arg.hash_table.set_partitioned_threshold(
                                     state->partitioned_hash_join_rows_threshold());
                         }},
               *_hash_table_variants);
}

void HashJoinNode::_process_hashtable_ctx_variants_init(RuntimeState* state) {
    std::visit(
            [&](auto&& join_op_variants) {
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                _process_hashtable_ctx_variants->emplace<ProcessHashTableProbe<JoinOpType::value>>(
                        this, state->batch_size());
            },
            _join_op_variants);
}

std::vector<uint16_t> HashJoinNode::_convert_block_to_null(Block& block) {
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

HashJoinNode::~HashJoinNode() {
    if (_shared_hashtable_controller && _should_build_hash_table) {
        // signal at here is abnormal
        _shared_hashtable_controller->signal(id(), Status::Cancelled("signaled in destructor"));
    }
    if (_runtime_filter_slots != nullptr) {
        _runtime_filter_slots->finish_publish();
    }
}

void HashJoinNode::_release_mem() {
    _arena = nullptr;
    _hash_table_variants = nullptr;
    _process_hashtable_ctx_variants = nullptr;
    _null_map_column = nullptr;
    _tuple_is_null_left_flag_column = nullptr;
    _tuple_is_null_right_flag_column = nullptr;
    _shared_hash_table_context = nullptr;
    _probe_block.clear();
}

} // namespace doris::vectorized
