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

#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/defer_op.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/template_helpers.hpp"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

// TODO: Best prefetch step is decided by machine. We should also provide a
//  SQL hint to allow users to tune by hand.
static constexpr int PREFETCH_STEP = 64;

template Status HashJoinNode::_extract_join_column<true>(
        Block&, COW<IColumn>::mutable_ptr<ColumnVector<unsigned char>>&,
        std::vector<IColumn const*, std::allocator<IColumn const*>>&,
        std::vector<int, std::allocator<int>> const&);

template Status HashJoinNode::_extract_join_column<false>(
        Block&, COW<IColumn>::mutable_ptr<ColumnVector<unsigned char>>&,
        std::vector<IColumn const*, std::allocator<IColumn const*>>&,
        std::vector<int, std::allocator<int>> const&);

HashJoinNode::HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _join_op(tnode.hash_join_node.join_op),
          _mem_used(0),
          _have_other_join_conjunct(tnode.hash_join_node.__isset.vother_join_conjunct),
          _match_all_probe(_join_op == TJoinOp::LEFT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _match_all_build(_join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _build_unique(!_have_other_join_conjunct &&
                        (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                         _join_op == TJoinOp::LEFT_ANTI_JOIN ||
                         _join_op == TJoinOp::LEFT_SEMI_JOIN)),
          _is_right_semi_anti(_join_op == TJoinOp::RIGHT_ANTI_JOIN ||
                              _join_op == TJoinOp::RIGHT_SEMI_JOIN),
          _is_outer_join(_match_all_build || _match_all_probe),
          _hash_output_slot_ids(tnode.hash_join_node.__isset.hash_output_slot_ids
                                        ? tnode.hash_join_node.hash_output_slot_ids
                                        : std::vector<SlotId> {}),
          _intermediate_row_desc(
                  descs, tnode.hash_join_node.vintermediate_tuple_id_list,
                  std::vector<bool>(tnode.hash_join_node.vintermediate_tuple_id_list.size())),
          _output_row_desc(descs, {tnode.hash_join_node.voutput_tuple_id}, {false}) {
    _runtime_filter_descs = tnode.runtime_filters;
    init_join_op();

    // avoid vector expand change block address.
    // one block can store 4g data, _build_blocks can store 128*4g data.
    // if probe data bigger than 512g, runtime filter maybe will core dump when insert data.
    _build_blocks.reserve(_MAX_BUILD_BLOCK_COUNT);
}

HashJoinNode::~HashJoinNode() = default;

void HashJoinNode::init_join_op() {
    switch (_join_op) {
#define M(NAME)                                                                            \
    case TJoinOp::NAME:                                                                    \
        _join_op_variants.emplace<std::integral_constant<TJoinOp::type, TJoinOp::NAME>>(); \
        break;
        APPLY_FOR_JOINOP_VARIANTS(M);
#undef M
    default:
        //do nothing
        break;
    }
}

Status HashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
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
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjunct.left, &ctx));
        _probe_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjunct.right, &ctx));
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
        _build_side_ignore_null |= (_join_op != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN &&
                                    !_store_null_in_hash_table.back());
        conjuncts_index++;
    }
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        _probe_ignore_null |= !probe_not_ignore_null[i];
    }
    _short_circuit_for_null_in_build_side = _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;

    _probe_column_disguise_null.reserve(eq_join_conjuncts.size());

    if (tnode.hash_join_node.__isset.vother_join_conjunct) {
        _vother_join_conjunct_ptr.reset(new VExprContext*);
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, tnode.hash_join_node.vother_join_conjunct,
                                                _vother_join_conjunct_ptr.get()));

        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    }

    const auto& output_exprs = tnode.hash_join_node.srcExprList;
    for (const auto& expr : output_exprs) {
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, expr, &ctx));
        _output_expr_ctxs.push_back(ctx);
    }

    _runtime_filters.resize(_runtime_filter_descs.size());
    for (size_t i = 0; i < _runtime_filter_descs.size(); i++) {
        RETURN_IF_ERROR(state->runtime_filter_mgr()->regist_filter(
                RuntimeFilterRole::PRODUCER, _runtime_filter_descs[i], state->query_options()));
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

    // only use in outer join as the bool column to mark for function of `tuple_is_null`
    if (_is_outer_join) {
        _tuple_is_null_left_flag_column = ColumnUInt8::create();
        _tuple_is_null_right_flag_column = ColumnUInt8::create();
    }
    return Status::OK();
}

Status HashJoinNode::prepare(RuntimeState* state) {
    DCHECK(_runtime_profile.get() != nullptr);
    _rows_returned_counter = ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);
    _rows_returned_rate = runtime_profile()->add_derived_counter(
            ROW_THROUGHPUT_COUNTER, TUnit::UNIT_PER_SECOND,
            std::bind<int64_t>(&RuntimeProfile::units_per_second, _rows_returned_counter,
                               runtime_profile()->total_time_counter()),
            "");
    _mem_tracker = std::make_unique<MemTracker>("ExecNode:" + _runtime_profile->name(),
                                                _runtime_profile.get());

    if (_vconjunct_ctx_ptr) {
        RETURN_IF_ERROR((*_vconjunct_ctx_ptr)->prepare(state, _intermediate_row_desc));
    }

    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state));
    }
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    // Build phase
    auto build_phase_profile = runtime_profile()->create_child("BuildPhase", true, true);
    runtime_profile()->add_child(build_phase_profile, false, nullptr);
    _build_timer = ADD_TIMER(build_phase_profile, "BuildTime");
    _build_table_timer = ADD_TIMER(build_phase_profile, "BuildTableTime");
    _build_side_merge_block_timer = ADD_TIMER(build_phase_profile, "BuildSideMergeBlockTime");
    _build_table_insert_timer = ADD_TIMER(build_phase_profile, "BuildTableInsertTime");
    _build_expr_call_timer = ADD_TIMER(build_phase_profile, "BuildExprCallTime");
    _build_table_expanse_timer = ADD_TIMER(build_phase_profile, "BuildTableExpanseTime");
    _build_rows_counter = ADD_COUNTER(build_phase_profile, "BuildRows", TUnit::UNIT);
    _build_side_compute_hash_timer = ADD_TIMER(build_phase_profile, "BuildSideHashComputingTime");

    // Probe phase
    auto probe_phase_profile = runtime_profile()->create_child("ProbePhase", true, true);
    _probe_timer = ADD_TIMER(probe_phase_profile, "ProbeTime");
    _probe_next_timer = ADD_TIMER(probe_phase_profile, "ProbeFindNextTime");
    _probe_expr_call_timer = ADD_TIMER(probe_phase_profile, "ProbeExprCallTime");
    _probe_rows_counter = ADD_COUNTER(probe_phase_profile, "ProbeRows", TUnit::UNIT);
    _search_hashtable_timer = ADD_TIMER(probe_phase_profile, "ProbeWhenSearchHashTableTime");
    _build_side_output_timer = ADD_TIMER(probe_phase_profile, "ProbeWhenBuildSideOutputTime");
    _probe_side_output_timer = ADD_TIMER(probe_phase_profile, "ProbeWhenProbeSideOutputTime");

    _join_filter_timer = ADD_TIMER(runtime_profile(), "JoinFilterTimer");

    _push_down_timer = ADD_TIMER(runtime_profile(), "PushDownTime");
    _push_compute_timer = ADD_TIMER(runtime_profile(), "PushDownComputeTime");
    _build_buckets_counter = ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);

    RETURN_IF_ERROR(VExpr::prepare(_build_expr_ctxs, state, child(1)->row_desc()));
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc()));

    // _vother_join_conjuncts are evaluated in the context of the rows produced by this node
    if (_vother_join_conjunct_ptr) {
        RETURN_IF_ERROR((*_vother_join_conjunct_ptr)->prepare(state, _intermediate_row_desc));
    }
    RETURN_IF_ERROR(VExpr::prepare(_output_expr_ctxs, state, _intermediate_row_desc));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_projections, state, _intermediate_row_desc));

    // right table data types
    _right_table_data_types = VectorizedUtils::get_data_types(child(1)->row_desc());
    _left_table_data_types = VectorizedUtils::get_data_types(child(0)->row_desc());

    // Hash Table Init
    _hash_table_init();
    _process_hashtable_ctx_variants_init(state);
    _construct_mutable_join_block();

    return Status::OK();
}

Status HashJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    START_AND_SCOPE_SPAN(state->get_tracer(), span, "ashJoinNode::close");
    VExpr::close(_build_expr_ctxs, state);
    VExpr::close(_probe_expr_ctxs, state);

    if (_vother_join_conjunct_ptr) (*_vother_join_conjunct_ptr)->close(state);
    VExpr::close(_output_expr_ctxs, state);

    return ExecNode::close(state);
}

Status HashJoinNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented HashJoin Node::get_next scalar");
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

void HashJoinNode::_construct_mutable_join_block() {
    const auto& mutable_block_desc = _intermediate_row_desc;
    for (const auto tuple_desc : mutable_block_desc.tuple_descriptors()) {
        for (const auto slot_desc : tuple_desc->slots()) {
            auto type_ptr = slot_desc->get_data_type_ptr();
            _join_block.insert({type_ptr->create_column(), type_ptr, slot_desc->col_name()});
        }
    }
}

Status HashJoinNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "HashJoinNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    for (size_t i = 0; i < _runtime_filter_descs.size(); i++) {
        if (auto bf = _runtime_filters[i]->get_bloomfilter()) {
            RETURN_IF_ERROR(bf->init_with_fixed_length());
        }
    }
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(VExpr::open(_build_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    if (_vother_join_conjunct_ptr) {
        RETURN_IF_ERROR((*_vother_join_conjunct_ptr)->open(state));
    }
    RETURN_IF_ERROR(VExpr::open(_output_expr_ctxs, state));

    std::promise<Status> thread_status;
    std::thread([this, state, thread_status_p = &thread_status,
                 parent_span = opentelemetry::trace::Tracer::GetCurrentSpan()] {
        OpentelemetryScope scope {parent_span};
        this->_probe_side_open_thread(state, thread_status_p);
    }).detach();

    // Open the probe-side child so that it may perform any initialisation in parallel.
    // Don't exit even if we see an error, we still need to wait for the build thread
    // to finish.
    // ISSUE-1247, check open_status after buildThread execute.
    // If this return first, build thread will use 'thread_status'
    // which is already destructor and then coredump.
    Status status = _hash_table_build(state);
    RETURN_IF_ERROR(thread_status.get_future().get());
    return status;
}

void HashJoinNode::_probe_side_open_thread(RuntimeState* state, std::promise<Status>* status) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "HashJoinNode::_hash_table_build_thread");
    SCOPED_ATTACH_TASK(state);
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_shared());
    status->set_value(child(0)->open(state));
}

Status HashJoinNode::_hash_table_build(RuntimeState* state) {
    RETURN_IF_ERROR(child(1)->open(state));
    SCOPED_TIMER(_build_timer);
    MutableBlock mutable_block(child(1)->row_desc().tuple_descriptors());

    uint8_t index = 0;
    int64_t last_mem_used = 0;
    bool eos = false;

    // make one block for each 4 gigabytes
    constexpr static auto BUILD_BLOCK_MAX_SIZE = 4 * 1024UL * 1024UL * 1024UL;

    Block block;
    // If eos or have already met a null value using short-circuit strategy, we do not need to pull
    // data from data.
    while (!eos && !_short_circuit_for_null_in_probe_side) {
        block.clear_column_data();
        RETURN_IF_CANCELLED(state);

        RETURN_IF_ERROR_AND_CHECK_SPAN(child(1)->get_next_after_projects(state, &block, &eos),
                                       child(1)->get_next_span(), eos);
        _mem_used += block.allocated_bytes();

        if (block.rows() != 0) {
            SCOPED_TIMER(_build_side_merge_block_timer);
            mutable_block.merge(block);
        }

        if (UNLIKELY(_mem_used - last_mem_used > BUILD_BLOCK_MAX_SIZE)) {
            if (_build_blocks.size() == _MAX_BUILD_BLOCK_COUNT) {
                return Status::NotSupported(
                        strings::Substitute("data size of right table in hash join > $0",
                                            BUILD_BLOCK_MAX_SIZE * _MAX_BUILD_BLOCK_COUNT));
            }
            _build_blocks.emplace_back(mutable_block.to_block());
            // TODO:: Rethink may we should do the proess after we recevie all build blocks ?
            // which is better.
            RETURN_IF_ERROR(_process_build_block(state, _build_blocks[index], index));

            mutable_block = MutableBlock();
            ++index;
            last_mem_used = _mem_used;
        }
    }

    if (!mutable_block.empty() && !_short_circuit_for_null_in_probe_side) {
        if (_build_blocks.size() == _MAX_BUILD_BLOCK_COUNT) {
            return Status::NotSupported(
                    strings::Substitute("data size of right table in hash join > $0",
                                        BUILD_BLOCK_MAX_SIZE * _MAX_BUILD_BLOCK_COUNT));
        }
        _build_blocks.emplace_back(mutable_block.to_block());
        RETURN_IF_ERROR(_process_build_block(state, _build_blocks[index], index));
    }
    return _runtime_filter_build_process_variants(state);
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

Status HashJoinNode::_do_evaluate(Block& block, std::vector<VExprContext*>& exprs,
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

void HashJoinNode::_hash_table_init() {
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
                if constexpr (std::is_same_v<RowRefListType, RowRefList>) {
                    _hash_table_create_row_ref_list();
                } else if constexpr (std::is_same_v<RowRefListType, RowRefListWithFlag>) {
                    _hash_table_create_row_ref_list_with_flag();
                } else if constexpr (std::is_same_v<RowRefListType, RowRefListWithFlags>) {
                    _hash_table_create_row_ref_list_with_flags();
                }
            },
            _join_op_variants, make_bool_variant(_have_other_join_conjunct));
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

Status HashJoinNode::_build_output_block(Block* origin_block, Block* output_block) {
    auto is_mem_reuse = output_block->mem_reuse();
    MutableBlock mutable_block =
            is_mem_reuse ? MutableBlock(output_block)
                         : MutableBlock(VectorizedUtils::create_empty_columnswithtypename(
                                   _output_row_desc));
    auto rows = origin_block->rows();
    // TODO: After FE plan support same nullable of output expr and origin block and mutable column
    // we should replace `insert_column_datas` by `insert_range_from`

    auto insert_column_datas = [](auto& to, const auto& from, size_t rows) {
        if (to->is_nullable() && !from.is_nullable()) {
            auto& null_column = reinterpret_cast<ColumnNullable&>(*to);
            null_column.get_nested_column().insert_range_from(from, 0, rows);
            null_column.get_null_map_column().get_data().resize_fill(rows, 0);
        } else {
            to->insert_range_from(from, 0, rows);
        }
    };
    if (rows != 0) {
        auto& mutable_columns = mutable_block.mutable_columns();
        if (_output_expr_ctxs.empty()) {
            DCHECK(mutable_columns.size() == _output_row_desc.num_materialized_slots());
            for (int i = 0; i < mutable_columns.size(); ++i) {
                insert_column_datas(mutable_columns[i], *origin_block->get_by_position(i).column,
                                    rows);
            }
        } else {
            DCHECK(mutable_columns.size() == _output_row_desc.num_materialized_slots());
            for (int i = 0; i < mutable_columns.size(); ++i) {
                auto result_column_id = -1;
                RETURN_IF_ERROR(_output_expr_ctxs[i]->execute(origin_block, &result_column_id));
                auto column_ptr = origin_block->get_by_position(result_column_id)
                                          .column->convert_to_full_column_if_const();
                insert_column_datas(mutable_columns[i], *column_ptr, rows);
            }
        }

        if (!is_mem_reuse) {
            output_block->swap(mutable_block.to_block());
        }
        DCHECK(output_block->rows() == rows);
    }

    return Status::OK();
}

void HashJoinNode::_add_tuple_is_null_column(Block* block) {
    if (_is_outer_join) {
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

        block->insert({std::move(p0), std::make_shared<vectorized::DataTypeUInt8>(),
                       "left_tuples_is_null"});
        block->insert({std::move(p1), std::make_shared<vectorized::DataTypeUInt8>(),
                       "right_tuples_is_null"});
    }
}

void HashJoinNode::_reset_tuple_is_null_column() {
    if (_is_outer_join) {
        reinterpret_cast<ColumnUInt8&>(*_tuple_is_null_left_flag_column).clear();
        reinterpret_cast<ColumnUInt8&>(*_tuple_is_null_right_flag_column).clear();
    }
}

} // namespace doris::vectorized
