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

#include "exec/operator/operator.h"

#include "common/status.h"
#include "exec/common/util.hpp"
#include "exec/exchange/local_exchange_sink_operator.h"
#include "exec/exchange/local_exchange_source_operator.h"
#include "exec/operator/aggregation_sink_operator.h"
#include "exec/operator/aggregation_source_operator.h"
#include "exec/operator/analytic_sink_operator.h"
#include "exec/operator/analytic_source_operator.h"
#include "exec/operator/assert_num_rows_operator.h"
#include "exec/operator/blackhole_sink_operator.h"
#include "exec/operator/cache_sink_operator.h"
#include "exec/operator/cache_source_operator.h"
#include "exec/operator/datagen_operator.h"
#include "exec/operator/dict_sink_operator.h"
#include "exec/operator/distinct_streaming_aggregation_operator.h"
#include "exec/operator/empty_set_operator.h"
#include "exec/operator/es_scan_operator.h"
#include "exec/operator/exchange_sink_operator.h"
#include "exec/operator/exchange_source_operator.h"
#include "exec/operator/file_scan_operator.h"
#include "exec/operator/group_commit_block_sink_operator.h"
#include "exec/operator/group_commit_scan_operator.h"
#include "exec/operator/hashjoin_build_sink.h"
#include "exec/operator/hashjoin_probe_operator.h"
#include "exec/operator/hive_table_sink_operator.h"
#include "exec/operator/iceberg_delete_sink_operator.h"
#include "exec/operator/iceberg_merge_sink_operator.h"
#include "exec/operator/iceberg_table_sink_operator.h"
#include "exec/operator/jdbc_scan_operator.h"
#include "exec/operator/jdbc_table_sink_operator.h"
#include "exec/operator/local_merge_sort_source_operator.h"
#include "exec/operator/materialization_opertor.h"
#include "exec/operator/maxcompute_table_sink_operator.h"
#include "exec/operator/memory_scratch_sink_operator.h"
#include "exec/operator/meta_scan_operator.h"
#include "exec/operator/mock_operator.h"
#include "exec/operator/mock_scan_operator.h"
#include "exec/operator/multi_cast_data_stream_sink.h"
#include "exec/operator/multi_cast_data_stream_source.h"
#include "exec/operator/nested_loop_join_build_operator.h"
#include "exec/operator/nested_loop_join_probe_operator.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/operator/olap_table_sink_operator.h"
#include "exec/operator/olap_table_sink_v2_operator.h"
#include "exec/operator/partition_sort_sink_operator.h"
#include "exec/operator/partition_sort_source_operator.h"
#include "exec/operator/partitioned_aggregation_sink_operator.h"
#include "exec/operator/partitioned_aggregation_source_operator.h"
#include "exec/operator/partitioned_hash_join_probe_operator.h"
#include "exec/operator/partitioned_hash_join_sink_operator.h"
#include "exec/operator/rec_cte_anchor_sink_operator.h"
#include "exec/operator/rec_cte_scan_operator.h"
#include "exec/operator/rec_cte_sink_operator.h"
#include "exec/operator/rec_cte_source_operator.h"
#include "exec/operator/repeat_operator.h"
#include "exec/operator/result_file_sink_operator.h"
#include "exec/operator/result_sink_operator.h"
#include "exec/operator/schema_scan_operator.h"
#include "exec/operator/select_operator.h"
#include "exec/operator/set_probe_sink_operator.h"
#include "exec/operator/set_sink_operator.h"
#include "exec/operator/set_source_operator.h"
#include "exec/operator/sort_sink_operator.h"
#include "exec/operator/sort_source_operator.h"
#include "exec/operator/spill_iceberg_table_sink_operator.h"
#include "exec/operator/spill_sort_sink_operator.h"
#include "exec/operator/spill_sort_source_operator.h"
#include "exec/operator/streaming_aggregation_operator.h"
#include "exec/operator/table_function_operator.h"
#include "exec/operator/tvf_table_sink_operator.h"
#include "exec/operator/union_sink_operator.h"
#include "exec/operator/union_source_operator.h"
#include "exec/pipeline/dependency.h"
#include "exec/pipeline/pipeline.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_profile_counter_names.h"
#include "util/debug_util.h"
#include "util/string_util.h"

namespace doris {
#include "common/compile_check_begin.h"
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris {

Status OperatorBase::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    _is_closed = true;
    return Status::OK();
}

template <typename SharedStateArg>
std::string PipelineXLocalState<SharedStateArg>::name_suffix() const {
    if (_parent->nereids_id() == -1) {
        return fmt::format("(id={})", _parent->node_id());
    } else {
        return fmt::format("(nereids_id={}, id={})", _parent->nereids_id(), _parent->node_id());
    }
}

template <typename SharedStateArg>
std::string PipelineXSinkLocalState<SharedStateArg>::name_suffix() {
    if (_parent->nereids_id() == -1) {
        return fmt::format("(id={})", _parent->node_id());
    } else {
        return fmt::format("(nereids_id={}, id={})", _parent->nereids_id(), _parent->node_id());
    }
}

template <typename SharedStateArg>
Status PipelineXSinkLocalState<SharedStateArg>::terminate(RuntimeState* state) {
    if (_terminated) {
        return Status::OK();
    }
    _terminated = true;
    return Status::OK();
}

DataDistribution OperatorBase::required_data_distribution(RuntimeState* /*state*/) const {
    return _child && _child->is_serial_operator() && !is_source()
                   ? DataDistribution(ExchangeType::PASSTHROUGH)
                   : DataDistribution(ExchangeType::NOOP);
}

const RowDescriptor& OperatorBase::row_desc() const {
    return _child->row_desc();
}

template <typename SharedStateArg>
std::string PipelineXLocalState<SharedStateArg>::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", _parent->debug_string(indentation_level));
    return fmt::to_string(debug_string_buffer);
}

template <typename SharedStateArg>
std::string PipelineXSinkLocalState<SharedStateArg>::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", _parent->debug_string(indentation_level));
    return fmt::to_string(debug_string_buffer);
}

std::string OperatorXBase::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}{}: id={}, parallel_tasks={}, _is_serial_operator={}",
                   std::string(indentation_level * 2, ' '), _op_name, node_id(), _parallel_tasks,
                   _is_serial_operator);
    return fmt::to_string(debug_string_buffer);
}

std::string OperatorXBase::debug_string(RuntimeState* state, int indentation_level) const {
    return state->get_local_state(operator_id())->debug_string(indentation_level);
}

Status OperatorXBase::init(const TPlanNode& tnode, RuntimeState* state) {
    std::string node_name = print_plan_node_type(tnode.node_type);
    _nereids_id = tnode.nereids_id;
    if (!tnode.intermediate_output_tuple_id_list.empty()) {
        if (!tnode.__isset.output_tuple_id) {
            return Status::InternalError("no final output tuple id");
        }
        if (tnode.intermediate_output_tuple_id_list.size() !=
            tnode.intermediate_projections_list.size()) {
            return Status::InternalError(
                    "intermediate_output_tuple_id_list size:{} not match "
                    "intermediate_projections_list size:{}",
                    tnode.intermediate_output_tuple_id_list.size(),
                    tnode.intermediate_projections_list.size());
        }
    }
    auto substr = node_name.substr(0, node_name.find("_NODE"));
    _op_name = substr + "_OPERATOR";

    if (tnode.__isset.vconjunct) {
        return Status::InternalError("vconjunct is not supported yet");
    } else if (tnode.__isset.conjuncts) {
        for (const auto& conjunct : tnode.conjuncts) {
            VExprContextSPtr context;
            RETURN_IF_ERROR(VExpr::create_expr_tree(conjunct, context));
            _conjuncts.emplace_back(context);
        }
    }

    // create the projections expr
    if (tnode.__isset.projections) {
        DCHECK(tnode.__isset.output_tuple_id);
        RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.projections, _projections));
    }
    if (!tnode.intermediate_projections_list.empty()) {
        DCHECK(tnode.__isset.projections) << "no final projections";
        _intermediate_projections.reserve(tnode.intermediate_projections_list.size());
        for (const auto& tnode_projections : tnode.intermediate_projections_list) {
            VExprContextSPtrs projections;
            RETURN_IF_ERROR(VExpr::create_expr_trees(tnode_projections, projections));
            _intermediate_projections.push_back(projections);
        }
    }
    return Status::OK();
}

Status OperatorXBase::prepare(RuntimeState* state) {
    for (auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, intermediate_row_desc()));
    }
    if (state->enable_adjust_conjunct_order_by_cost()) {
        std::ranges::sort(_conjuncts, [](const auto& a, const auto& b) {
            return a->execute_cost() < b->execute_cost();
        });
    };

    for (int i = 0; i < _intermediate_projections.size(); i++) {
        RETURN_IF_ERROR(
                VExpr::prepare(_intermediate_projections[i], state, intermediate_row_desc(i)));
    }
    RETURN_IF_ERROR(VExpr::prepare(_projections, state, projections_row_desc()));

    if (has_output_row_desc()) {
        RETURN_IF_ERROR(VExpr::check_expr_output_type(_projections, *_output_row_descriptor));
    }

    for (auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }
    RETURN_IF_ERROR(VExpr::open(_projections, state));
    for (auto& projections : _intermediate_projections) {
        RETURN_IF_ERROR(VExpr::open(projections, state));
    }
    if (_child && !is_source()) {
        RETURN_IF_ERROR(_child->prepare(state));
    }

    if (VExpr::contains_blockable_function(_conjuncts) ||
        VExpr::contains_blockable_function(_projections)) {
        _blockable = true;
    }

    return Status::OK();
}

Status OperatorXBase::terminate(RuntimeState* state) {
    if (_child && !is_source()) {
        RETURN_IF_ERROR(_child->terminate(state));
    }
    auto result = state->get_local_state_result(operator_id());
    if (!result) {
        return result.error();
    }
    return result.value()->terminate(state);
}

Status OperatorXBase::close(RuntimeState* state) {
    if (_child && !is_source()) {
        RETURN_IF_ERROR(_child->close(state));
    }
    auto result = state->get_local_state_result(operator_id());
    if (!result) {
        return result.error();
    }
    return result.value()->close(state);
}

void PipelineXLocalStateBase::clear_origin_block() {
    _origin_block.clear_column_data(_parent->intermediate_row_desc().num_materialized_slots());
}

Status PipelineXLocalStateBase::filter_block(const VExprContextSPtrs& expr_contexts, Block* block) {
    RETURN_IF_ERROR(VExprContext::filter_block(expr_contexts, block, block->columns()));

    _estimate_memory_usage += VExprContext::get_memory_usage(expr_contexts);
    return Status::OK();
}

bool PipelineXLocalStateBase::is_blockable() const {
    return std::any_of(_projections.begin(), _projections.end(),
                       [&](VExprContextSPtr expr) -> bool { return expr->is_blockable(); });
}

Status OperatorXBase::do_projections(RuntimeState* state, Block* origin_block,
                                     Block* output_block) const {
    auto* local_state = state->get_local_state(operator_id());
    SCOPED_TIMER(local_state->exec_time_counter());
    SCOPED_TIMER(local_state->_projection_timer);
    const size_t rows = origin_block->rows();
    if (rows == 0) {
        return Status::OK();
    }
    Block input_block = *origin_block;

    size_t bytes_usage = 0;
    ColumnsWithTypeAndName new_columns;
    for (const auto& projections : local_state->_intermediate_projections) {
        if (projections.empty()) {
            return Status::InternalError("meet empty intermediate projection, node id: {}",
                                         node_id());
        }
        new_columns.resize(projections.size());
        for (int i = 0; i < projections.size(); i++) {
            RETURN_IF_ERROR(projections[i]->execute(&input_block, new_columns[i]));
            if (new_columns[i].column->size() != rows) {
                return Status::InternalError(
                        "intermediate projection result column size {} not equal input rows {}, "
                        "expr: {}",
                        new_columns[i].column->size(), rows,
                        projections[i]->root()->debug_string());
            }
        }
        Block tmp_block {new_columns};
        bytes_usage += tmp_block.allocated_bytes();
        input_block.swap(tmp_block);
    }

    if (input_block.rows() != rows) {
        return Status::InternalError(
                "after intermediate projections input block rows {} not equal origin rows {}, "
                "input_block: {}",
                input_block.rows(), rows, input_block.dump_structure());
    }
    auto insert_column_datas = [&](auto& to, ColumnPtr& from, size_t rows) {
        if (to->is_nullable() && !from->is_nullable()) {
            if (_keep_origin || !from->is_exclusive()) {
                auto& null_column = reinterpret_cast<ColumnNullable&>(*to);
                null_column.get_nested_column().insert_range_from(*from, 0, rows);
                null_column.get_null_map_column().get_data().resize_fill(rows, 0);
                bytes_usage += null_column.allocated_bytes();
            } else {
                to = make_nullable(from, false)->assume_mutable();
            }
        } else {
            if (_keep_origin || !from->is_exclusive()) {
                to->insert_range_from(*from, 0, rows);
                bytes_usage += from->allocated_bytes();
            } else {
                to = from->assume_mutable();
            }
        }
    };

    MutableBlock mutable_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, *_output_row_descriptor);
    if (rows != 0) {
        auto& mutable_columns = mutable_block.mutable_columns();
        const size_t origin_columns_count = input_block.columns();
        DCHECK_EQ(mutable_columns.size(), local_state->_projections.size()) << debug_string();
        for (int i = 0; i < mutable_columns.size(); ++i) {
            auto result_column_id = -1;
            ColumnPtr column_ptr;
            RETURN_IF_ERROR(local_state->_projections[i]->execute(&input_block, column_ptr));
            if (column_ptr->size() != rows) {
                return Status::InternalError(
                        "projection result column size {} not equal input rows {}, expr: {}",
                        column_ptr->size(), rows,
                        local_state->_projections[i]->root()->debug_string());
            }
            column_ptr = column_ptr->convert_to_full_column_if_const();
            if (result_column_id >= origin_columns_count) {
                bytes_usage += column_ptr->allocated_bytes();
            }
            insert_column_datas(mutable_columns[i], column_ptr, rows);
        }
        DCHECK(mutable_block.rows() == rows);
        output_block->set_columns(std::move(mutable_columns));
    }

    local_state->_estimate_memory_usage += bytes_usage;

    return Status::OK();
}

Status OperatorXBase::get_block_after_projects(RuntimeState* state, Block* block, bool* eos) {
    DBUG_EXECUTE_IF("Pipeline::return_empty_block", {
        if (this->_op_name == "AGGREGATION_OPERATOR" || this->_op_name == "HASH_JOIN_OPERATOR" ||
            this->_op_name == "PARTITIONED_AGGREGATION_OPERATOR" ||
            this->_op_name == "PARTITIONED_HASH_JOIN_OPERATOR" ||
            this->_op_name == "CROSS_JOIN_OPERATOR" || this->_op_name == "SORT_OPERATOR") {
            if (_debug_point_count++ % 2 == 0) {
                return Status::OK();
            }
        }
    });

    Status status;
    auto* local_state = state->get_local_state(operator_id());
    Defer defer([&]() {
        if (status.ok()) {
            if (auto rows = block->rows()) {
                COUNTER_UPDATE(local_state->_rows_returned_counter, rows);
                COUNTER_UPDATE(local_state->_blocks_returned_counter, 1);
            }
        }
    });
    if (_output_row_descriptor) {
        local_state->clear_origin_block();
        status = get_block(state, &local_state->_origin_block, eos);
        if (UNLIKELY(!status.ok())) {
            return status;
        }
        status = do_projections(state, &local_state->_origin_block, block);
        return status;
    }
    status = get_block(state, block, eos);
    RETURN_IF_ERROR(block->check_type_and_column());
    return status;
}

void PipelineXLocalStateBase::reached_limit(Block* block, bool* eos) {
    if (_parent->_limit != -1 and _num_rows_returned + block->rows() >= _parent->_limit) {
        block->set_num_rows(_parent->_limit - _num_rows_returned);
        *eos = true;
    }

    DBUG_EXECUTE_IF("Pipeline::reached_limit_early", {
        auto op_name = to_lower(_parent->_op_name);
        auto arg_op_name = dp->param<std::string>("op_name");
        arg_op_name = to_lower(arg_op_name);

        if (op_name == arg_op_name) {
            *eos = true;
        }
    });

    if (auto rows = block->rows()) {
        _num_rows_returned += rows;
    }
}

Status DataSinkOperatorXBase::terminate(RuntimeState* state) {
    auto result = state->get_sink_local_state_result();
    if (!result) {
        return result.error();
    }
    return result.value()->terminate(state);
}

std::string DataSinkOperatorXBase::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;

    fmt::format_to(debug_string_buffer, "{}{}: id={}, _is_serial_operator={}",
                   std::string(indentation_level * 2, ' '), _name, node_id(), _is_serial_operator);
    return fmt::to_string(debug_string_buffer);
}

std::string DataSinkOperatorXBase::debug_string(RuntimeState* state, int indentation_level) const {
    return state->get_sink_local_state()->debug_string(indentation_level);
}

Status DataSinkOperatorXBase::init(const TDataSink& tsink) {
    std::string op_name = "UNKNOWN_SINK";
    auto it = _TDataSinkType_VALUES_TO_NAMES.find(tsink.type);

    if (it != _TDataSinkType_VALUES_TO_NAMES.end()) {
        op_name = it->second;
    }
    _name = op_name + "_OPERATOR";
    return Status::OK();
}

Status DataSinkOperatorXBase::init(const TPlanNode& tnode, RuntimeState* state) {
    std::string op_name = print_plan_node_type(tnode.node_type);
    _nereids_id = tnode.nereids_id;
    auto substr = op_name.substr(0, op_name.find("_NODE"));
    _name = substr + "_SINK_OPERATOR";
    return Status::OK();
}

template <typename LocalStateType>
Status DataSinkOperatorX<LocalStateType>::setup_local_state(RuntimeState* state,
                                                            LocalSinkStateInfo& info) {
    auto local_state = LocalStateType::create_unique(this, state);
    RETURN_IF_ERROR(local_state->init(state, info));
    state->emplace_sink_local_state(operator_id(), std::move(local_state));
    return Status::OK();
}

template <typename LocalStateType>
std::shared_ptr<BasicSharedState> DataSinkOperatorX<LocalStateType>::create_shared_state() const {
    if constexpr (std::is_same_v<typename LocalStateType::SharedStateType,
                                 LocalExchangeSharedState>) {
        return nullptr;
    } else if constexpr (std::is_same_v<typename LocalStateType::SharedStateType,
                                        MultiCastSharedState>) {
        throw Exception(Status::FatalError("should not reach here!"));
    } else {
        auto ss = LocalStateType::SharedStateType::create_shared();
        ss->id = operator_id();
        for (auto& dest : dests_id()) {
            ss->related_op_ids.insert(dest);
        }
        return ss;
    }
}

template <typename LocalStateType>
Status OperatorX<LocalStateType>::setup_local_state(RuntimeState* state, LocalStateInfo& info) {
    auto local_state = LocalStateType::create_unique(state, this);
    RETURN_IF_ERROR(local_state->init(state, info));
    state->emplace_local_state(operator_id(), std::move(local_state));
    return Status::OK();
}

PipelineXSinkLocalStateBase::PipelineXSinkLocalStateBase(DataSinkOperatorXBase* parent,
                                                         RuntimeState* state)
        : _parent(parent), _state(state) {}

PipelineXLocalStateBase::PipelineXLocalStateBase(RuntimeState* state, OperatorXBase* parent)
        : _num_rows_returned(0), _rows_returned_counter(nullptr), _parent(parent), _state(state) {}

template <typename SharedStateArg>
Status PipelineXLocalState<SharedStateArg>::init(RuntimeState* state, LocalStateInfo& info) {
    _operator_profile.reset(new RuntimeProfile(_parent->get_name() + name_suffix()));
    _common_profile.reset(new RuntimeProfile(profile::COMMON_COUNTERS));
    _custom_profile.reset(new RuntimeProfile(profile::CUSTOM_COUNTERS));
    _operator_profile->set_metadata(_parent->node_id());
    // indent is false so that source operator will have same
    // indentation_level with its parent operator.
    info.parent_profile->add_child(_operator_profile.get(), /*indent=*/false);
    _operator_profile->add_child(_common_profile.get(), true);
    _operator_profile->add_child(_custom_profile.get(), true);
    constexpr auto is_fake_shared = std::is_same_v<SharedStateArg, FakeSharedState>;
    if constexpr (!is_fake_shared) {
        if (info.shared_state_map.find(_parent->operator_id()) != info.shared_state_map.end()) {
            _shared_state = info.shared_state_map.at(_parent->operator_id())
                                    .first.get()
                                    ->template cast<SharedStateArg>();

            _dependency = _shared_state->get_dep_by_channel_id(info.task_idx).front().get();
            _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
                    _common_profile, "WaitForDependency[" + _dependency->name() + "]Time", 1);
        } else if (info.shared_state) {
            if constexpr (std::is_same_v<LocalExchangeSharedState, SharedStateArg>) {
                DCHECK(false);
            }
            // For UnionSourceOperator without children, there is no shared state.
            _shared_state = info.shared_state->template cast<SharedStateArg>();

            _dependency = _shared_state->create_source_dependency(
                    _parent->operator_id(), _parent->node_id(), _parent->get_name());
            _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
                    _common_profile, "WaitForDependency[" + _dependency->name() + "]Time", 1);
        } else {
            if constexpr (std::is_same_v<LocalExchangeSharedState, SharedStateArg>) {
                DCHECK(false);
            }
        }
    }

    if (must_set_shared_state() && _shared_state == nullptr) {
        return Status::InternalError("must set shared state, in {}", _parent->get_name());
    }

    _rows_returned_counter =
            ADD_COUNTER_WITH_LEVEL(_common_profile, profile::ROWS_PRODUCED, TUnit::UNIT, 1);
    _blocks_returned_counter =
            ADD_COUNTER_WITH_LEVEL(_common_profile, profile::BLOCKS_PRODUCED, TUnit::UNIT, 1);
    _projection_timer = ADD_TIMER_WITH_LEVEL(_common_profile, profile::PROJECTION_TIME, 2);
    _init_timer = ADD_TIMER_WITH_LEVEL(_common_profile, profile::INIT_TIME, 2);
    _open_timer = ADD_TIMER_WITH_LEVEL(_common_profile, profile::OPEN_TIME, 2);
    _close_timer = ADD_TIMER_WITH_LEVEL(_common_profile, profile::CLOSE_TIME, 2);
    _exec_timer = ADD_TIMER_WITH_LEVEL(_common_profile, profile::EXEC_TIME, 1);
    _memory_used_counter =
            _common_profile->AddHighWaterMarkCounter(profile::MEMORY_USAGE, TUnit::BYTES, "", 1);
    _common_profile->add_info_string("IsColocate",
                                     std::to_string(_parent->is_colocated_operator()));
    _common_profile->add_info_string("IsShuffled", std::to_string(_parent->is_shuffled_operator()));
    _common_profile->add_info_string("FollowedByShuffledOperator",
                                     std::to_string(_parent->followed_by_shuffled_operator()));
    return Status::OK();
}

template <typename SharedStateArg>
Status PipelineXLocalState<SharedStateArg>::open(RuntimeState* state) {
    _conjuncts.resize(_parent->_conjuncts.size());
    _projections.resize(_parent->_projections.size());
    for (size_t i = 0; i < _conjuncts.size(); i++) {
        RETURN_IF_ERROR(_parent->_conjuncts[i]->clone(state, _conjuncts[i]));
    }
    for (size_t i = 0; i < _projections.size(); i++) {
        RETURN_IF_ERROR(_parent->_projections[i]->clone(state, _projections[i]));
    }
    _intermediate_projections.resize(_parent->_intermediate_projections.size());
    for (int i = 0; i < _parent->_intermediate_projections.size(); i++) {
        _intermediate_projections[i].resize(_parent->_intermediate_projections[i].size());
        for (int j = 0; j < _parent->_intermediate_projections[i].size(); j++) {
            RETURN_IF_ERROR(_parent->_intermediate_projections[i][j]->clone(
                    state, _intermediate_projections[i][j]));
        }
    }
    return Status::OK();
}

template <typename SharedStateArg>
Status PipelineXLocalState<SharedStateArg>::terminate(RuntimeState* state) {
    if (_terminated) {
        return Status::OK();
    }
    _terminated = true;
    return Status::OK();
}

template <typename SharedStateArg>
Status PipelineXLocalState<SharedStateArg>::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    if constexpr (!std::is_same_v<SharedStateArg, FakeSharedState>) {
        COUNTER_SET(_wait_for_dependency_timer, _dependency->watcher_elapse_time());
    }
    _closed = true;
    return Status::OK();
}

template <typename SharedState>
Status PipelineXSinkLocalState<SharedState>::init(RuntimeState* state, LocalSinkStateInfo& info) {
    // create profile
    _operator_profile =
            state->obj_pool()->add(new RuntimeProfile(_parent->get_name() + name_suffix()));
    _common_profile = state->obj_pool()->add(new RuntimeProfile(profile::COMMON_COUNTERS));
    _custom_profile = state->obj_pool()->add(new RuntimeProfile(profile::CUSTOM_COUNTERS));

    // indentation is true
    // The parent profile of sink operator is usually a RuntimeProfile called PipelineTask.
    // So we should set the indentation to true.
    info.parent_profile->add_child(_operator_profile, /*indent=*/true);
    _operator_profile->add_child(_common_profile, true);
    _operator_profile->add_child(_custom_profile, true);

    _operator_profile->set_metadata(_parent->node_id());
    _wait_for_finish_dependency_timer =
            ADD_TIMER(_common_profile, profile::PENDING_FINISH_DEPENDENCY);
    constexpr auto is_fake_shared = std::is_same_v<SharedState, FakeSharedState>;
    if constexpr (!is_fake_shared) {
        if (info.shared_state_map.find(_parent->dests_id().front()) !=
            info.shared_state_map.end()) {
            if constexpr (std::is_same_v<LocalExchangeSharedState, SharedState>) {
                DCHECK(info.shared_state_map.at(_parent->dests_id().front()).second.size() == 1);
            }
            _dependency = info.shared_state_map.at(_parent->dests_id().front())
                                  .second[std::is_same_v<LocalExchangeSharedState, SharedState>
                                                  ? 0
                                                  : info.task_idx]
                                  .get();
            _shared_state = _dependency->shared_state()->template cast<SharedState>();
        } else {
            if constexpr (std::is_same_v<LocalExchangeSharedState, SharedState>) {
                DCHECK(false);
            }
            _shared_state = info.shared_state->template cast<SharedState>();
            _dependency = _shared_state->create_sink_dependency(
                    _parent->dests_id().front(), _parent->node_id(), _parent->get_name());
        }
        _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
                _common_profile, "WaitForDependency[" + _dependency->name() + "]Time", 1);
    }

    if (must_set_shared_state() && _shared_state == nullptr) {
        return Status::InternalError("must set shared state, in {}", _parent->get_name());
    }

    _rows_input_counter =
            ADD_COUNTER_WITH_LEVEL(_common_profile, profile::INPUT_ROWS, TUnit::UNIT, 1);
    _init_timer = ADD_TIMER_WITH_LEVEL(_common_profile, profile::INIT_TIME, 2);
    _open_timer = ADD_TIMER_WITH_LEVEL(_common_profile, profile::OPEN_TIME, 2);
    _close_timer = ADD_TIMER_WITH_LEVEL(_common_profile, profile::CLOSE_TIME, 2);
    _exec_timer = ADD_TIMER_WITH_LEVEL(_common_profile, profile::EXEC_TIME, 1);
    _memory_used_counter =
            _common_profile->AddHighWaterMarkCounter(profile::MEMORY_USAGE, TUnit::BYTES, "", 1);
    _common_profile->add_info_string("IsColocate",
                                     std::to_string(_parent->is_colocated_operator()));
    _common_profile->add_info_string("IsShuffled", std::to_string(_parent->is_shuffled_operator()));
    _common_profile->add_info_string("FollowedByShuffledOperator",
                                     std::to_string(_parent->followed_by_shuffled_operator()));
    return Status::OK();
}

template <typename SharedState>
Status PipelineXSinkLocalState<SharedState>::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    if constexpr (!std::is_same_v<SharedState, FakeSharedState>) {
        COUNTER_SET(_wait_for_dependency_timer, _dependency->watcher_elapse_time());
    }
    _closed = true;
    return Status::OK();
}

template <typename LocalStateType>
Status StreamingOperatorX<LocalStateType>::get_block(RuntimeState* state, Block* block, bool* eos) {
    RETURN_IF_ERROR(OperatorX<LocalStateType>::_child->get_block_after_projects(state, block, eos));
    return pull(state, block, eos);
}

template <typename LocalStateType>
Status StatefulOperatorX<LocalStateType>::get_block(RuntimeState* state, Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    if (need_more_input_data(state)) {
        local_state._child_block->clear_column_data(
                OperatorX<LocalStateType>::_child->row_desc().num_materialized_slots());
        RETURN_IF_ERROR(OperatorX<LocalStateType>::_child->get_block_after_projects(
                state, local_state._child_block.get(), &local_state._child_eos));
        *eos = local_state._child_eos;
        if (local_state._child_block->rows() == 0 && !local_state._child_eos) {
            return Status::OK();
        }
        {
            SCOPED_TIMER(local_state.exec_time_counter());
            RETURN_IF_ERROR(push(state, local_state._child_block.get(), local_state._child_eos));
        }
    }

    if (!need_more_input_data(state)) {
        SCOPED_TIMER(local_state.exec_time_counter());
        bool new_eos = false;
        RETURN_IF_ERROR(pull(state, block, &new_eos));
        if (new_eos) {
            *eos = true;
        } else if (!need_more_input_data(state)) {
            *eos = false;
        }
    }
    return Status::OK();
}

template <typename Writer, typename Parent>
    requires(std::is_base_of_v<AsyncResultWriter, Writer>)
Status AsyncWriterSink<Writer, Parent>::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    _async_writer_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                         "AsyncWriterDependency", true);
    _writer.reset(new Writer(info.tsink, _output_vexpr_ctxs, _async_writer_dependency,
                             _finish_dependency));

    _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
            common_profile(), "WaitForDependency[" + _async_writer_dependency->name() + "]Time", 1);
    return Status::OK();
}

template <typename Writer, typename Parent>
    requires(std::is_base_of_v<AsyncResultWriter, Writer>)
Status AsyncWriterSink<Writer, Parent>::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    _output_vexpr_ctxs.resize(_parent->cast<Parent>()._output_vexpr_ctxs.size());
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
        RETURN_IF_ERROR(
                _parent->cast<Parent>()._output_vexpr_ctxs[i]->clone(state, _output_vexpr_ctxs[i]));
    }
    RETURN_IF_ERROR(_writer->start_writer(state, operator_profile()));
    return Status::OK();
}

template <typename Writer, typename Parent>
    requires(std::is_base_of_v<AsyncResultWriter, Writer>)
Status AsyncWriterSink<Writer, Parent>::sink(RuntimeState* state, Block* block, bool eos) {
    return _writer->sink(block, eos);
}

template <typename Writer, typename Parent>
    requires(std::is_base_of_v<AsyncResultWriter, Writer>)
Status AsyncWriterSink<Writer, Parent>::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    COUNTER_SET(_wait_for_dependency_timer, _async_writer_dependency->watcher_elapse_time());
    COUNTER_SET(_wait_for_finish_dependency_timer, _finish_dependency->watcher_elapse_time());
    // if the init failed, the _writer may be nullptr. so here need check
    if (_writer) {
        Status st = _writer->get_writer_status();
        if (exec_status.ok()) {
            _writer->force_close(state->is_cancelled() ? state->cancel_reason()
                                                       : Status::Cancelled("force close"));
        } else {
            _writer->force_close(exec_status);
        }
        // If there is an error in process_block thread, then we should get the writer
        // status before call force_close. For example, the thread may failed in commit
        // transaction.
        RETURN_IF_ERROR(st);
    }
    return Base::close(state, exec_status);
}

#define DECLARE_OPERATOR(LOCAL_STATE) template class DataSinkOperatorX<LOCAL_STATE>;
DECLARE_OPERATOR(HashJoinBuildSinkLocalState)
DECLARE_OPERATOR(ResultSinkLocalState)
DECLARE_OPERATOR(JdbcTableSinkLocalState)
DECLARE_OPERATOR(MemoryScratchSinkLocalState)
DECLARE_OPERATOR(ResultFileSinkLocalState)
DECLARE_OPERATOR(OlapTableSinkLocalState)
DECLARE_OPERATOR(OlapTableSinkV2LocalState)
DECLARE_OPERATOR(HiveTableSinkLocalState)
DECLARE_OPERATOR(TVFTableSinkLocalState)
DECLARE_OPERATOR(IcebergTableSinkLocalState)
DECLARE_OPERATOR(SpillIcebergTableSinkLocalState)
DECLARE_OPERATOR(IcebergDeleteSinkLocalState)
DECLARE_OPERATOR(IcebergMergeSinkLocalState)
DECLARE_OPERATOR(MCTableSinkLocalState)
DECLARE_OPERATOR(AnalyticSinkLocalState)
DECLARE_OPERATOR(BlackholeSinkLocalState)
DECLARE_OPERATOR(SortSinkLocalState)
DECLARE_OPERATOR(SpillSortSinkLocalState)
DECLARE_OPERATOR(LocalExchangeSinkLocalState)
DECLARE_OPERATOR(AggSinkLocalState)
DECLARE_OPERATOR(PartitionedAggSinkLocalState)
DECLARE_OPERATOR(ExchangeSinkLocalState)
DECLARE_OPERATOR(NestedLoopJoinBuildSinkLocalState)
DECLARE_OPERATOR(UnionSinkLocalState)
DECLARE_OPERATOR(MultiCastDataStreamSinkLocalState)
DECLARE_OPERATOR(PartitionSortSinkLocalState)
DECLARE_OPERATOR(SetProbeSinkLocalState<true>)
DECLARE_OPERATOR(SetProbeSinkLocalState<false>)
DECLARE_OPERATOR(SetSinkLocalState<true>)
DECLARE_OPERATOR(SetSinkLocalState<false>)
DECLARE_OPERATOR(PartitionedHashJoinSinkLocalState)
DECLARE_OPERATOR(GroupCommitBlockSinkLocalState)
DECLARE_OPERATOR(CacheSinkLocalState)
DECLARE_OPERATOR(DictSinkLocalState)
DECLARE_OPERATOR(RecCTESinkLocalState)
DECLARE_OPERATOR(RecCTEAnchorSinkLocalState)

#undef DECLARE_OPERATOR

#define DECLARE_OPERATOR(LOCAL_STATE) template class OperatorX<LOCAL_STATE>;
DECLARE_OPERATOR(HashJoinProbeLocalState)
DECLARE_OPERATOR(OlapScanLocalState)
DECLARE_OPERATOR(GroupCommitLocalState)
DECLARE_OPERATOR(JDBCScanLocalState)
DECLARE_OPERATOR(FileScanLocalState)
DECLARE_OPERATOR(EsScanLocalState)
DECLARE_OPERATOR(AnalyticLocalState)
DECLARE_OPERATOR(SortLocalState)
DECLARE_OPERATOR(SpillSortLocalState)
DECLARE_OPERATOR(LocalMergeSortLocalState)
DECLARE_OPERATOR(AggLocalState)
DECLARE_OPERATOR(PartitionedAggLocalState)
DECLARE_OPERATOR(TableFunctionLocalState)
DECLARE_OPERATOR(ExchangeLocalState)
DECLARE_OPERATOR(RepeatLocalState)
DECLARE_OPERATOR(NestedLoopJoinProbeLocalState)
DECLARE_OPERATOR(AssertNumRowsLocalState)
DECLARE_OPERATOR(EmptySetLocalState)
DECLARE_OPERATOR(UnionSourceLocalState)
DECLARE_OPERATOR(MultiCastDataStreamSourceLocalState)
DECLARE_OPERATOR(PartitionSortSourceLocalState)
DECLARE_OPERATOR(SetSourceLocalState<true>)
DECLARE_OPERATOR(SetSourceLocalState<false>)
DECLARE_OPERATOR(DataGenLocalState)
DECLARE_OPERATOR(SchemaScanLocalState)
DECLARE_OPERATOR(MetaScanLocalState)
DECLARE_OPERATOR(LocalExchangeSourceLocalState)
DECLARE_OPERATOR(PartitionedHashJoinProbeLocalState)
DECLARE_OPERATOR(CacheSourceLocalState)
DECLARE_OPERATOR(RecCTESourceLocalState)
DECLARE_OPERATOR(RecCTEScanLocalState)

#ifdef BE_TEST
DECLARE_OPERATOR(MockLocalState)
DECLARE_OPERATOR(MockScanLocalState)
#endif
#undef DECLARE_OPERATOR

template class StreamingOperatorX<AssertNumRowsLocalState>;
template class StreamingOperatorX<SelectLocalState>;

template class StatefulOperatorX<HashJoinProbeLocalState>;
template class StatefulOperatorX<PartitionedHashJoinProbeLocalState>;
template class StatefulOperatorX<RepeatLocalState>;
template class StatefulOperatorX<MaterializationLocalState>;
template class StatefulOperatorX<StreamingAggLocalState>;
template class StatefulOperatorX<DistinctStreamingAggLocalState>;
template class StatefulOperatorX<NestedLoopJoinProbeLocalState>;
template class StatefulOperatorX<TableFunctionLocalState>;

template class PipelineXSinkLocalState<HashJoinSharedState>;
template class PipelineXSinkLocalState<PartitionedHashJoinSharedState>;
template class PipelineXSinkLocalState<SortSharedState>;
template class PipelineXSinkLocalState<SpillSortSharedState>;
template class PipelineXSinkLocalState<NestedLoopJoinSharedState>;
template class PipelineXSinkLocalState<AnalyticSharedState>;
template class PipelineXSinkLocalState<AggSharedState>;
template class PipelineXSinkLocalState<PartitionedAggSharedState>;
template class PipelineXSinkLocalState<FakeSharedState>;
template class PipelineXSinkLocalState<UnionSharedState>;
template class PipelineXSinkLocalState<PartitionSortNodeSharedState>;
template class PipelineXSinkLocalState<MultiCastSharedState>;
template class PipelineXSinkLocalState<SetSharedState>;
template class PipelineXSinkLocalState<LocalExchangeSharedState>;
template class PipelineXSinkLocalState<BasicSharedState>;
template class PipelineXSinkLocalState<DataQueueSharedState>;
template class PipelineXSinkLocalState<RecCTESharedState>;

template class PipelineXLocalState<HashJoinSharedState>;
template class PipelineXLocalState<PartitionedHashJoinSharedState>;
template class PipelineXLocalState<SortSharedState>;
template class PipelineXLocalState<SpillSortSharedState>;
template class PipelineXLocalState<NestedLoopJoinSharedState>;
template class PipelineXLocalState<AnalyticSharedState>;
template class PipelineXLocalState<AggSharedState>;
template class PipelineXLocalState<PartitionedAggSharedState>;
template class PipelineXLocalState<FakeSharedState>;
template class PipelineXLocalState<UnionSharedState>;
template class PipelineXLocalState<DataQueueSharedState>;
template class PipelineXLocalState<MultiCastSharedState>;
template class PipelineXLocalState<PartitionSortNodeSharedState>;
template class PipelineXLocalState<SetSharedState>;
template class PipelineXLocalState<LocalExchangeSharedState>;
template class PipelineXLocalState<BasicSharedState>;
template class PipelineXLocalState<RecCTESharedState>;

template class AsyncWriterSink<doris::VFileResultWriter, ResultFileSinkOperatorX>;
template class AsyncWriterSink<doris::VJdbcTableWriter, JdbcTableSinkOperatorX>;
template class AsyncWriterSink<doris::VTabletWriter, OlapTableSinkOperatorX>;
template class AsyncWriterSink<doris::VTabletWriterV2, OlapTableSinkV2OperatorX>;
template class AsyncWriterSink<doris::VHiveTableWriter, HiveTableSinkOperatorX>;
template class AsyncWriterSink<doris::VIcebergTableWriter, IcebergTableSinkOperatorX>;
template class AsyncWriterSink<doris::VIcebergTableWriter, SpillIcebergTableSinkOperatorX>;
template class AsyncWriterSink<doris::VIcebergDeleteSink, IcebergDeleteSinkOperatorX>;
template class AsyncWriterSink<doris::VIcebergMergeSink, IcebergMergeSinkOperatorX>;
template class AsyncWriterSink<doris::VMCTableWriter, MCTableSinkOperatorX>;
template class AsyncWriterSink<doris::VTVFTableWriter, TVFTableSinkOperatorX>;

#ifdef BE_TEST
template class OperatorX<DummyOperatorLocalState>;
template class DataSinkOperatorX<DummySinkLocalState>;
#endif

#include "common/compile_check_end.h"
} // namespace doris
