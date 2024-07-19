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

#include "operator.h"

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/analytic_sink_operator.h"
#include "pipeline/exec/analytic_source_operator.h"
#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/datagen_operator.h"
#include "pipeline/exec/distinct_streaming_aggregation_operator.h"
#include "pipeline/exec/empty_set_operator.h"
#include "pipeline/exec/es_scan_operator.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "pipeline/exec/file_scan_operator.h"
#include "pipeline/exec/group_commit_block_sink_operator.h"
#include "pipeline/exec/group_commit_scan_operator.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/hive_table_sink_operator.h"
#include "pipeline/exec/iceberg_table_sink_operator.h"
#include "pipeline/exec/jdbc_scan_operator.h"
#include "pipeline/exec/jdbc_table_sink_operator.h"
#include "pipeline/exec/memory_scratch_sink_operator.h"
#include "pipeline/exec/meta_scan_operator.h"
#include "pipeline/exec/multi_cast_data_stream_sink.h"
#include "pipeline/exec/multi_cast_data_stream_source.h"
#include "pipeline/exec/nested_loop_join_build_operator.h"
#include "pipeline/exec/nested_loop_join_probe_operator.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "pipeline/exec/olap_table_sink_operator.h"
#include "pipeline/exec/olap_table_sink_v2_operator.h"
#include "pipeline/exec/partition_sort_sink_operator.h"
#include "pipeline/exec/partition_sort_source_operator.h"
#include "pipeline/exec/partitioned_aggregation_sink_operator.h"
#include "pipeline/exec/partitioned_aggregation_source_operator.h"
#include "pipeline/exec/partitioned_hash_join_probe_operator.h"
#include "pipeline/exec/partitioned_hash_join_sink_operator.h"
#include "pipeline/exec/repeat_operator.h"
#include "pipeline/exec/result_file_sink_operator.h"
#include "pipeline/exec/result_sink_operator.h"
#include "pipeline/exec/schema_scan_operator.h"
#include "pipeline/exec/select_operator.h"
#include "pipeline/exec/set_probe_sink_operator.h"
#include "pipeline/exec/set_sink_operator.h"
#include "pipeline/exec/set_source_operator.h"
#include "pipeline/exec/sort_sink_operator.h"
#include "pipeline/exec/sort_source_operator.h"
#include "pipeline/exec/spill_sort_sink_operator.h"
#include "pipeline/exec/spill_sort_source_operator.h"
#include "pipeline/exec/streaming_aggregation_operator.h"
#include "pipeline/exec/table_function_operator.h"
#include "pipeline/exec/union_sink_operator.h"
#include "pipeline/exec/union_source_operator.h"
#include "pipeline/local_exchange/local_exchange_sink_operator.h"
#include "pipeline/local_exchange/local_exchange_source_operator.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"
#include "util/string_util.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

Status OperatorBase::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    _is_closed = true;
    return Status::OK();
}

template <typename SharedStateArg>
std::string PipelineXLocalState<SharedStateArg>::name_suffix() const {
    return " (id=" + std::to_string(_parent->node_id()) + [&]() -> std::string {
        if (_parent->nereids_id() == -1) {
            return "";
        }
        return " , nereids_id=" + std::to_string(_parent->nereids_id());
    }() + ")";
}

template <typename SharedStateArg>
std::string PipelineXSinkLocalState<SharedStateArg>::name_suffix() {
    return " (id=" + std::to_string(_parent->node_id()) + [&]() -> std::string {
        if (_parent->nereids_id() == -1) {
            return "";
        }
        return " , nereids_id=" + std::to_string(_parent->nereids_id());
    }() + ")";
}

DataDistribution DataSinkOperatorXBase::required_data_distribution() const {
    return _child_x && _child_x->ignore_data_distribution()
                   ? DataDistribution(ExchangeType::PASSTHROUGH)
                   : DataDistribution(ExchangeType::NOOP);
}
const RowDescriptor& OperatorBase::row_desc() const {
    return _child_x->row_desc();
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
    fmt::format_to(debug_string_buffer, "{}{}: id={}, parallel_tasks={}",
                   std::string(indentation_level * 2, ' '), _op_name, node_id(), _parallel_tasks);
    return fmt::to_string(debug_string_buffer);
}

std::string OperatorXBase::debug_string(RuntimeState* state, int indentation_level) const {
    return state->get_local_state(operator_id())->debug_string(indentation_level);
}

Status OperatorXBase::init(const TPlanNode& tnode, RuntimeState* /*state*/) {
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
        vectorized::VExprContextSPtr context;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(tnode.vconjunct, context));
        _conjuncts.emplace_back(context);
    } else if (tnode.__isset.conjuncts) {
        for (auto& conjunct : tnode.conjuncts) {
            vectorized::VExprContextSPtr context;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(conjunct, context));
            _conjuncts.emplace_back(context);
        }
    }

    // create the projections expr

    if (tnode.__isset.projections) {
        DCHECK(tnode.__isset.output_tuple_id);
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(tnode.projections, _projections));
    }
    if (!tnode.intermediate_projections_list.empty()) {
        DCHECK(tnode.__isset.projections) << "no final projections";
        _intermediate_projections.reserve(tnode.intermediate_projections_list.size());
        for (const auto& tnode_projections : tnode.intermediate_projections_list) {
            vectorized::VExprContextSPtrs projections;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(tnode_projections, projections));
            _intermediate_projections.push_back(projections);
        }
    }
    return Status::OK();
}

Status OperatorXBase::prepare(RuntimeState* state) {
    for (auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, intermediate_row_desc()));
    }
    for (int i = 0; i < _intermediate_projections.size(); i++) {
        RETURN_IF_ERROR(vectorized::VExpr::prepare(_intermediate_projections[i], state,
                                                   intermediate_row_desc(i)));
    }
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_projections, state, projections_row_desc()));

    if (has_output_row_desc()) {
        RETURN_IF_ERROR(
                vectorized::VExpr::check_expr_output_type(_projections, *_output_row_descriptor));
    }

    if (_child_x && !is_source()) {
        RETURN_IF_ERROR(_child_x->prepare(state));
    }

    return Status::OK();
}

Status OperatorXBase::open(RuntimeState* state) {
    for (auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }
    RETURN_IF_ERROR(vectorized::VExpr::open(_projections, state));
    for (auto& projections : _intermediate_projections) {
        RETURN_IF_ERROR(vectorized::VExpr::open(projections, state));
    }
    if (_child_x && !is_source()) {
        RETURN_IF_ERROR(_child_x->open(state));
    }
    return Status::OK();
}

Status OperatorXBase::close(RuntimeState* state) {
    if (_child_x && !is_source()) {
        RETURN_IF_ERROR(_child_x->close(state));
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

Status OperatorXBase::do_projections(RuntimeState* state, vectorized::Block* origin_block,
                                     vectorized::Block* output_block) const {
    auto* local_state = state->get_local_state(operator_id());
    SCOPED_TIMER(local_state->exec_time_counter());
    SCOPED_TIMER(local_state->_projection_timer);
    const size_t rows = origin_block->rows();
    if (rows == 0) {
        return Status::OK();
    }
    vectorized::Block input_block = *origin_block;

    std::vector<int> result_column_ids;
    for (const auto& projections : _intermediate_projections) {
        result_column_ids.resize(projections.size());
        for (int i = 0; i < projections.size(); i++) {
            RETURN_IF_ERROR(projections[i]->execute(&input_block, &result_column_ids[i]));
        }
        input_block.shuffle_columns(result_column_ids);
    }

    DCHECK_EQ(rows, input_block.rows());
    auto insert_column_datas = [&](auto& to, vectorized::ColumnPtr& from, size_t rows) {
        if (to->is_nullable() && !from->is_nullable()) {
            if (_keep_origin || !from->is_exclusive()) {
                auto& null_column = reinterpret_cast<vectorized::ColumnNullable&>(*to);
                null_column.get_nested_column().insert_range_from(*from, 0, rows);
                null_column.get_null_map_column().get_data().resize_fill(rows, 0);
            } else {
                to = make_nullable(from, false)->assume_mutable();
            }
        } else {
            if (_keep_origin || !from->is_exclusive()) {
                to->insert_range_from(*from, 0, rows);
            } else {
                to = from->assume_mutable();
            }
        }
    };

    using namespace vectorized;
    vectorized::MutableBlock mutable_block =
            vectorized::VectorizedUtils::build_mutable_mem_reuse_block(output_block,
                                                                       *_output_row_descriptor);
    if (rows != 0) {
        auto& mutable_columns = mutable_block.mutable_columns();
        DCHECK(mutable_columns.size() == local_state->_projections.size());
        for (int i = 0; i < mutable_columns.size(); ++i) {
            auto result_column_id = -1;
            RETURN_IF_ERROR(local_state->_projections[i]->execute(&input_block, &result_column_id));
            auto column_ptr = input_block.get_by_position(result_column_id)
                                      .column->convert_to_full_column_if_const();
            insert_column_datas(mutable_columns[i], column_ptr, rows);
        }
        DCHECK(mutable_block.rows() == rows);
        output_block->set_columns(std::move(mutable_columns));
    }

    return Status::OK();
}

Status OperatorXBase::get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                               bool* eos) {
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

    auto* local_state = state->get_local_state(operator_id());
    if (_output_row_descriptor) {
        local_state->clear_origin_block();
        auto status = get_block(state, &local_state->_origin_block, eos);
        if (UNLIKELY(!status.ok())) {
            return status;
        }
        return do_projections(state, &local_state->_origin_block, block);
    }
    local_state->_peak_memory_usage_counter->set(local_state->_mem_tracker->peak_consumption());
    return get_block(state, block, eos);
}

void PipelineXLocalStateBase::reached_limit(vectorized::Block* block, bool* eos) {
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
        COUNTER_UPDATE(_blocks_returned_counter, 1);
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
}

std::string DataSinkOperatorXBase::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;

    fmt::format_to(debug_string_buffer, "{}{}: id={}", std::string(indentation_level * 2, ' '),
                   _name, node_id());
    return fmt::to_string(debug_string_buffer);
}

std::string DataSinkOperatorXBase::debug_string(RuntimeState* state, int indentation_level) const {
    return state->get_sink_local_state()->debug_string(indentation_level);
}

Status DataSinkOperatorXBase::init(const TDataSink& tsink) {
    std::string op_name = "UNKNOWN_SINK";
    std::map<int, const char*>::const_iterator it = _TDataSinkType_VALUES_TO_NAMES.find(tsink.type);

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
        LOG(FATAL) << "should not reach here!";
        return nullptr;
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
        : _parent(parent), _state(state) {
    _query_statistics = std::make_shared<QueryStatistics>();
}

PipelineXLocalStateBase::PipelineXLocalStateBase(RuntimeState* state, OperatorXBase* parent)
        : _num_rows_returned(0),
          _rows_returned_counter(nullptr),
          _peak_memory_usage_counter(nullptr),
          _parent(parent),
          _state(state) {
    _query_statistics = std::make_shared<QueryStatistics>();
}

template <typename SharedStateArg>
Status PipelineXLocalState<SharedStateArg>::init(RuntimeState* state, LocalStateInfo& info) {
    _runtime_profile.reset(new RuntimeProfile(_parent->get_name() + name_suffix()));
    _runtime_profile->set_metadata(_parent->node_id());
    _runtime_profile->set_is_sink(false);
    info.parent_profile->add_child(_runtime_profile.get(), true, nullptr);
    constexpr auto is_fake_shared = std::is_same_v<SharedStateArg, FakeSharedState>;
    if constexpr (!is_fake_shared) {
        if constexpr (std::is_same_v<LocalExchangeSharedState, SharedStateArg>) {
            DCHECK(info.le_state_map.find(_parent->operator_id()) != info.le_state_map.end());
            _shared_state = info.le_state_map.at(_parent->operator_id()).first.get();

            _dependency = _shared_state->get_dep_by_channel_id(info.task_idx);
            _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
                    _runtime_profile, "WaitForDependency[" + _dependency->name() + "]Time", 1);
        } else if (info.shared_state) {
            // For UnionSourceOperator without children, there is no shared state.
            _shared_state = info.shared_state->template cast<SharedStateArg>();

            _dependency = _shared_state->create_source_dependency(
                    _parent->operator_id(), _parent->node_id(), _parent->get_name());
            _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
                    _runtime_profile, "WaitForDependency[" + _dependency->name() + "]Time", 1);
        }
    }

    _rows_returned_counter =
            ADD_COUNTER_WITH_LEVEL(_runtime_profile, "RowsProduced", TUnit::UNIT, 1);
    _blocks_returned_counter =
            ADD_COUNTER_WITH_LEVEL(_runtime_profile, "BlocksProduced", TUnit::UNIT, 1);
    _projection_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "ProjectionTime", 1);
    _init_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "InitTime", 1);
    _open_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "OpenTime", 1);
    _close_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "CloseTime", 1);
    _exec_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "ExecTime", 1);
    _mem_tracker = std::make_unique<MemTracker>("PipelineXLocalState:" + _runtime_profile->name());
    _memory_used_counter = ADD_LABEL_COUNTER_WITH_LEVEL(_runtime_profile, "MemoryUsage", 1);
    _peak_memory_usage_counter = _runtime_profile->AddHighWaterMarkCounter(
            "PeakMemoryUsage", TUnit::BYTES, "MemoryUsage", 1);
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
Status PipelineXLocalState<SharedStateArg>::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    if constexpr (!std::is_same_v<SharedStateArg, FakeSharedState>) {
        COUNTER_SET(_wait_for_dependency_timer, _dependency->watcher_elapse_time());
    }
    if (_rows_returned_counter != nullptr) {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
    if (_peak_memory_usage_counter) {
        _peak_memory_usage_counter->set(_mem_tracker->peak_consumption());
    }
    _closed = true;
    // Some kinds of source operators has a 1-1 relationship with a sink operator (such as AnalyticOperator).
    // We must ensure AnalyticSinkOperator will not be blocked if AnalyticSourceOperator already closed.
    if (_shared_state && _shared_state->sink_deps.size() == 1) {
        _shared_state->sink_deps.front()->set_always_ready();
    }
    return Status::OK();
}

template <typename SharedState>
Status PipelineXSinkLocalState<SharedState>::init(RuntimeState* state, LocalSinkStateInfo& info) {
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(_parent->get_name() + name_suffix()));
    _profile->set_metadata(_parent->node_id());
    _profile->set_is_sink(true);
    _wait_for_finish_dependency_timer = ADD_TIMER(_profile, "PendingFinishDependency");
    constexpr auto is_fake_shared = std::is_same_v<SharedState, FakeSharedState>;
    if constexpr (!is_fake_shared) {
        if constexpr (std::is_same_v<LocalExchangeSharedState, SharedState>) {
            DCHECK(info.le_state_map.find(_parent->dests_id().front()) != info.le_state_map.end());
            _dependency = info.le_state_map.at(_parent->dests_id().front()).second.get();
            _shared_state = (SharedState*)_dependency->shared_state();
        } else {
            _shared_state = info.shared_state->template cast<SharedState>();
            _dependency = _shared_state->create_sink_dependency(
                    _parent->dests_id().front(), _parent->node_id(), _parent->get_name());
        }
        _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
                _profile, "WaitForDependency[" + _dependency->name() + "]Time", 1);
    } else {
        _dependency = nullptr;
    }
    _rows_input_counter = ADD_COUNTER_WITH_LEVEL(_profile, "InputRows", TUnit::UNIT, 1);
    _init_timer = ADD_TIMER_WITH_LEVEL(_profile, "InitTime", 1);
    _open_timer = ADD_TIMER_WITH_LEVEL(_profile, "OpenTime", 1);
    _close_timer = ADD_TIMER_WITH_LEVEL(_profile, "CloseTime", 1);
    _exec_timer = ADD_TIMER_WITH_LEVEL(_profile, "ExecTime", 1);
    info.parent_profile->add_child(_profile, true, nullptr);
    _mem_tracker = std::make_unique<MemTracker>(_parent->get_name());
    _memory_used_counter = ADD_LABEL_COUNTER_WITH_LEVEL(_profile, "MemoryUsage", 1);
    _peak_memory_usage_counter =
            _profile->AddHighWaterMarkCounter("PeakMemoryUsage", TUnit::BYTES, "MemoryUsage", 1);
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
    if (_peak_memory_usage_counter) {
        _peak_memory_usage_counter->set(_mem_tracker->peak_consumption());
    }
    _closed = true;
    return Status::OK();
}

template <typename LocalStateType>
Status StreamingOperatorX<LocalStateType>::get_block(RuntimeState* state, vectorized::Block* block,
                                                     bool* eos) {
    RETURN_IF_ERROR(
            OperatorX<LocalStateType>::_child_x->get_block_after_projects(state, block, eos));
    return pull(state, block, eos);
}

template <typename LocalStateType>
Status StatefulOperatorX<LocalStateType>::get_block(RuntimeState* state, vectorized::Block* block,
                                                    bool* eos) {
    auto& local_state = get_local_state(state);
    if (need_more_input_data(state)) {
        local_state._child_block->clear_column_data(
                OperatorX<LocalStateType>::_child_x->row_desc().num_materialized_slots());
        RETURN_IF_ERROR(OperatorX<LocalStateType>::_child_x->get_block_after_projects(
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
    requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
Status AsyncWriterSink<Writer, Parent>::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    _writer.reset(new Writer(info.tsink, _output_vexpr_ctxs));
    _async_writer_dependency =
            AsyncWriterDependency::create_shared(_parent->operator_id(), _parent->node_id());
    _writer->set_dependency(_async_writer_dependency.get(), _finish_dependency.get());

    _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
            _profile, "WaitForDependency[" + _async_writer_dependency->name() + "]Time", 1);
    return Status::OK();
}

template <typename Writer, typename Parent>
    requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
Status AsyncWriterSink<Writer, Parent>::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    _output_vexpr_ctxs.resize(_parent->cast<Parent>()._output_vexpr_ctxs.size());
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
        RETURN_IF_ERROR(
                _parent->cast<Parent>()._output_vexpr_ctxs[i]->clone(state, _output_vexpr_ctxs[i]));
    }
    RETURN_IF_ERROR(_writer->start_writer(state, _profile));
    return Status::OK();
}

template <typename Writer, typename Parent>
    requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
Status AsyncWriterSink<Writer, Parent>::sink(RuntimeState* state, vectorized::Block* block,
                                             bool eos) {
    return _writer->sink(block, eos);
}

template <typename Writer, typename Parent>
    requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
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
            _writer->force_close(state->is_cancelled() ? Status::Cancelled("Cancelled")
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

#define DECLARE_OPERATOR_X(LOCAL_STATE) template class DataSinkOperatorX<LOCAL_STATE>;
DECLARE_OPERATOR_X(HashJoinBuildSinkLocalState)
DECLARE_OPERATOR_X(ResultSinkLocalState)
DECLARE_OPERATOR_X(JdbcTableSinkLocalState)
DECLARE_OPERATOR_X(MemoryScratchSinkLocalState)
DECLARE_OPERATOR_X(ResultFileSinkLocalState)
DECLARE_OPERATOR_X(OlapTableSinkLocalState)
DECLARE_OPERATOR_X(OlapTableSinkV2LocalState)
DECLARE_OPERATOR_X(HiveTableSinkLocalState)
DECLARE_OPERATOR_X(IcebergTableSinkLocalState)
DECLARE_OPERATOR_X(AnalyticSinkLocalState)
DECLARE_OPERATOR_X(SortSinkLocalState)
DECLARE_OPERATOR_X(SpillSortSinkLocalState)
DECLARE_OPERATOR_X(LocalExchangeSinkLocalState)
DECLARE_OPERATOR_X(AggSinkLocalState)
DECLARE_OPERATOR_X(PartitionedAggSinkLocalState)
DECLARE_OPERATOR_X(ExchangeSinkLocalState)
DECLARE_OPERATOR_X(NestedLoopJoinBuildSinkLocalState)
DECLARE_OPERATOR_X(UnionSinkLocalState)
DECLARE_OPERATOR_X(MultiCastDataStreamSinkLocalState)
DECLARE_OPERATOR_X(PartitionSortSinkLocalState)
DECLARE_OPERATOR_X(SetProbeSinkLocalState<true>)
DECLARE_OPERATOR_X(SetProbeSinkLocalState<false>)
DECLARE_OPERATOR_X(SetSinkLocalState<true>)
DECLARE_OPERATOR_X(SetSinkLocalState<false>)
DECLARE_OPERATOR_X(PartitionedHashJoinSinkLocalState)
DECLARE_OPERATOR_X(GroupCommitBlockSinkLocalState)

#undef DECLARE_OPERATOR_X

#define DECLARE_OPERATOR_X(LOCAL_STATE) template class OperatorX<LOCAL_STATE>;
DECLARE_OPERATOR_X(HashJoinProbeLocalState)
DECLARE_OPERATOR_X(OlapScanLocalState)
DECLARE_OPERATOR_X(GroupCommitLocalState)
DECLARE_OPERATOR_X(JDBCScanLocalState)
DECLARE_OPERATOR_X(FileScanLocalState)
DECLARE_OPERATOR_X(EsScanLocalState)
DECLARE_OPERATOR_X(AnalyticLocalState)
DECLARE_OPERATOR_X(SortLocalState)
DECLARE_OPERATOR_X(SpillSortLocalState)
DECLARE_OPERATOR_X(AggLocalState)
DECLARE_OPERATOR_X(PartitionedAggLocalState)
DECLARE_OPERATOR_X(TableFunctionLocalState)
DECLARE_OPERATOR_X(ExchangeLocalState)
DECLARE_OPERATOR_X(RepeatLocalState)
DECLARE_OPERATOR_X(NestedLoopJoinProbeLocalState)
DECLARE_OPERATOR_X(AssertNumRowsLocalState)
DECLARE_OPERATOR_X(EmptySetLocalState)
DECLARE_OPERATOR_X(UnionSourceLocalState)
DECLARE_OPERATOR_X(MultiCastDataStreamSourceLocalState)
DECLARE_OPERATOR_X(PartitionSortSourceLocalState)
DECLARE_OPERATOR_X(SetSourceLocalState<true>)
DECLARE_OPERATOR_X(SetSourceLocalState<false>)
DECLARE_OPERATOR_X(DataGenLocalState)
DECLARE_OPERATOR_X(SchemaScanLocalState)
DECLARE_OPERATOR_X(MetaScanLocalState)
DECLARE_OPERATOR_X(LocalExchangeSourceLocalState)
DECLARE_OPERATOR_X(PartitionedHashJoinProbeLocalState)

#undef DECLARE_OPERATOR_X

template class StreamingOperatorX<AssertNumRowsLocalState>;
template class StreamingOperatorX<SelectLocalState>;

template class StatefulOperatorX<HashJoinProbeLocalState>;
template class StatefulOperatorX<PartitionedHashJoinProbeLocalState>;
template class StatefulOperatorX<RepeatLocalState>;
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
template class PipelineXLocalState<MultiCastSharedState>;
template class PipelineXLocalState<PartitionSortNodeSharedState>;
template class PipelineXLocalState<SetSharedState>;
template class PipelineXLocalState<LocalExchangeSharedState>;
template class PipelineXLocalState<BasicSharedState>;

template class AsyncWriterSink<doris::vectorized::VFileResultWriter, ResultFileSinkOperatorX>;
template class AsyncWriterSink<doris::vectorized::VJdbcTableWriter, JdbcTableSinkOperatorX>;
template class AsyncWriterSink<doris::vectorized::VTabletWriter, OlapTableSinkOperatorX>;
template class AsyncWriterSink<doris::vectorized::VTabletWriterV2, OlapTableSinkV2OperatorX>;
template class AsyncWriterSink<doris::vectorized::VHiveTableWriter, HiveTableSinkOperatorX>;
template class AsyncWriterSink<doris::vectorized::VIcebergTableWriter, IcebergTableSinkOperatorX>;

} // namespace doris::pipeline
