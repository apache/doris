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

#include <string>

#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/analytic_sink_operator.h"
#include "pipeline/exec/analytic_source_operator.h"
#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/empty_set_operator.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/nested_loop_join_build_operator.h"
#include "pipeline/exec/nested_loop_join_probe_operator.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "pipeline/exec/repeat_operator.h"
#include "pipeline/exec/result_sink_operator.h"
#include "pipeline/exec/select_operator.h"
#include "pipeline/exec/sort_sink_operator.h"
#include "pipeline/exec/sort_source_operator.h"
#include "pipeline/exec/streaming_aggregation_sink_operator.h"
#include "pipeline/exec/streaming_aggregation_source_operator.h"
#include "pipeline/exec/union_sink_operator.h"
#include "pipeline/exec/union_source_operator.h"
#include "util/debug_util.h"

namespace doris::pipeline {

std::string PipelineXLocalStateBase::debug_string(int indentation_level) const {
    return _parent->debug_string(indentation_level);
}

template <typename DependencyType>
std::string PipelineXLocalState<DependencyType>::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}",
                   PipelineXLocalStateBase::debug_string(indentation_level));
    if (_dependency) {
        fmt::format_to(debug_string_buffer, "\nDependency: \n {}",
                       _dependency->debug_string(indentation_level + 1));
    }
    return fmt::to_string(debug_string_buffer);
}

template <typename DependencyType>
std::string PipelineXSinkLocalState<DependencyType>::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}",
                   PipelineXSinkLocalStateBase::debug_string(indentation_level));
    if (_dependency) {
        fmt::format_to(debug_string_buffer, "\n{}Dependency: \n {}",
                       std::string(indentation_level * 2, ' '),
                       _dependency->debug_string(indentation_level + 1));
    }
    return fmt::to_string(debug_string_buffer);
}

std::string OperatorXBase::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}{}: id={}", std::string(indentation_level * 2, ' '),
                   _op_name, _id);
    return fmt::to_string(debug_string_buffer);
}

std::string OperatorXBase::debug_string(RuntimeState* state, int indentation_level) const {
    return state->get_local_state(id())->debug_string(indentation_level);
}

Status OperatorXBase::init(const TPlanNode& tnode, RuntimeState* /*state*/) {
    std::string node_name = print_plan_node_type(tnode.node_type);
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
    return Status::OK();
}

Status OperatorXBase::prepare(RuntimeState* state) {
    for (auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, intermediate_row_desc()));
    }

    RETURN_IF_ERROR(vectorized::VExpr::prepare(_projections, state, intermediate_row_desc()));

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
    if (_child_x && !is_source()) {
        RETURN_IF_ERROR(_child_x->open(state));
    }
    return Status::OK();
}

Status OperatorXBase::close(RuntimeState* state) {
    if (_child_x && !is_source()) {
        RETURN_IF_ERROR(_child_x->close(state));
    }
    return state->get_local_state(id())->close(state);
}

void PipelineXLocalStateBase::clear_origin_block() {
    _origin_block.clear_column_data(_parent->_row_descriptor.num_materialized_slots());
}

Status OperatorXBase::do_projections(RuntimeState* state, vectorized::Block* origin_block,
                                     vectorized::Block* output_block) const {
    auto local_state = state->get_local_state(id());
    SCOPED_TIMER(local_state->_projection_timer);
    using namespace vectorized;
    vectorized::MutableBlock mutable_block =
            vectorized::VectorizedUtils::build_mutable_mem_reuse_block(output_block,
                                                                       *_output_row_descriptor);
    auto rows = origin_block->rows();

    if (rows != 0) {
        auto& mutable_columns = mutable_block.mutable_columns();
        DCHECK(mutable_columns.size() == _projections.size());
        for (int i = 0; i < mutable_columns.size(); ++i) {
            auto result_column_id = -1;
            RETURN_IF_ERROR(_projections[i]->execute(origin_block, &result_column_id));
            auto column_ptr = origin_block->get_by_position(result_column_id)
                                      .column->convert_to_full_column_if_const();
            //TODO: this is a quick fix, we need a new function like "change_to_nullable" to do it
            if (mutable_columns[i]->is_nullable() xor column_ptr->is_nullable()) {
                DCHECK(mutable_columns[i]->is_nullable() && !column_ptr->is_nullable());
                reinterpret_cast<ColumnNullable*>(mutable_columns[i].get())
                        ->insert_range_from_not_nullable(*column_ptr, 0, rows);
            } else {
                mutable_columns[i]->insert_range_from(*column_ptr, 0, rows);
            }
        }
        DCHECK(mutable_block.rows() == rows);
    }

    return Status::OK();
}

Status OperatorXBase::get_next_after_projects(RuntimeState* state, vectorized::Block* block,
                                              SourceState& source_state) {
    auto local_state = state->get_local_state(id());
    if (_output_row_descriptor) {
        local_state->clear_origin_block();
        auto status = get_block(state, &local_state->_origin_block, source_state);
        if (UNLIKELY(!status.ok())) return status;
        return do_projections(state, &local_state->_origin_block, block);
    }
    local_state->_peak_memory_usage_counter->set(local_state->_mem_tracker->peak_consumption());
    return get_block(state, block, source_state);
}

bool PipelineXLocalStateBase::reached_limit() const {
    return _parent->_limit != -1 && _num_rows_returned >= _parent->_limit;
}

void PipelineXLocalStateBase::reached_limit(vectorized::Block* block, SourceState& source_state) {
    if (_parent->_limit != -1 and _num_rows_returned + block->rows() >= _parent->_limit) {
        block->set_num_rows(_parent->_limit - _num_rows_returned);
        source_state = SourceState::FINISHED;
    }

    _num_rows_returned += block->rows();
    COUNTER_UPDATE(_blocks_returned_counter, 1);
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
}

std::string DataSinkOperatorXBase::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;

    fmt::format_to(debug_string_buffer, "{}{}: id={}", std::string(indentation_level * 2, ' '),
                   _name, _id);
    return fmt::to_string(debug_string_buffer);
}

std::string PipelineXSinkLocalStateBase::debug_string(int indentation_level) const {
    return _parent->debug_string(indentation_level);
}

std::string DataSinkOperatorXBase::debug_string(RuntimeState* state, int indentation_level) const {
    return state->get_sink_local_state(id())->debug_string(indentation_level);
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

    auto substr = op_name.substr(0, op_name.find("_NODE"));

    _name = substr + "_SINK_OPERATOR";
    return Status::OK();
}

template <typename LocalStateType>
Status DataSinkOperatorX<LocalStateType>::setup_local_state(RuntimeState* state,
                                                            LocalSinkStateInfo& info) {
    auto local_state = LocalStateType::create_shared(this, state);
    state->emplace_sink_local_state(id(), local_state);
    return local_state->init(state, info);
}

template <typename LocalStateType>
void DataSinkOperatorX<LocalStateType>::get_dependency(DependencySPtr& dependency) {
    if constexpr (!std::is_same_v<typename LocalStateType::Dependency, FakeDependency>) {
        dependency.reset(new typename LocalStateType::Dependency(dest_id()));
    } else {
        dependency.reset((typename LocalStateType::Dependency*)nullptr);
    }
}

template <typename LocalStateType>
Status OperatorX<LocalStateType>::setup_local_state(RuntimeState* state, LocalStateInfo& info) {
    auto local_state = LocalStateType::create_shared(state, this);
    state->emplace_local_state(id(), local_state);
    return local_state->init(state, info);
}

template <typename LocalStateType>
Status StreamingOperatorX<LocalStateType>::get_block(RuntimeState* state, vectorized::Block* block,
                                                     SourceState& source_state) {
    RETURN_IF_ERROR(OperatorX<LocalStateType>::_child_x->get_next_after_projects(state, block,
                                                                                 source_state));
    return pull(state, block, source_state);
}

template <typename LocalStateType>
Status StatefulOperatorX<LocalStateType>::get_block(RuntimeState* state, vectorized::Block* block,
                                                    SourceState& source_state) {
    auto& local_state = state->get_local_state(OperatorX<LocalStateType>::id())
                                ->template cast<LocalStateType>();
    if (need_more_input_data(state)) {
        local_state._child_block->clear_column_data();
        RETURN_IF_ERROR(OperatorX<LocalStateType>::_child_x->get_next_after_projects(
                state, local_state._child_block.get(), local_state._child_source_state));
        source_state = local_state._child_source_state;
        if (local_state._child_block->rows() == 0 &&
            local_state._child_source_state != SourceState::FINISHED) {
            return Status::OK();
        }
        RETURN_IF_ERROR(
                push(state, local_state._child_block.get(), local_state._child_source_state));
    }

    if (!need_more_input_data(state)) {
        SourceState new_state = SourceState::DEPEND_ON_SOURCE;
        RETURN_IF_ERROR(pull(state, block, new_state));
        if (new_state == SourceState::FINISHED) {
            source_state = SourceState::FINISHED;
        } else if (!need_more_input_data(state)) {
            source_state = SourceState::MORE_DATA;
        } else if (source_state == SourceState::MORE_DATA) {
            source_state = local_state._child_source_state;
        }
    }
    return Status::OK();
}

#define DECLARE_OPERATOR_X(LOCAL_STATE) template class DataSinkOperatorX<LOCAL_STATE>;
DECLARE_OPERATOR_X(HashJoinBuildSinkLocalState)
DECLARE_OPERATOR_X(ResultSinkLocalState)
DECLARE_OPERATOR_X(AnalyticSinkLocalState)
DECLARE_OPERATOR_X(SortSinkLocalState)
DECLARE_OPERATOR_X(BlockingAggSinkLocalState)
DECLARE_OPERATOR_X(StreamingAggSinkLocalState)
DECLARE_OPERATOR_X(ExchangeSinkLocalState)
DECLARE_OPERATOR_X(NestedLoopJoinBuildSinkLocalState)
DECLARE_OPERATOR_X(UnionSinkLocalState)

#undef DECLARE_OPERATOR_X

#define DECLARE_OPERATOR_X(LOCAL_STATE) template class OperatorX<LOCAL_STATE>;
DECLARE_OPERATOR_X(HashJoinProbeLocalState)
DECLARE_OPERATOR_X(OlapScanLocalState)
DECLARE_OPERATOR_X(AnalyticLocalState)
DECLARE_OPERATOR_X(SortLocalState)
DECLARE_OPERATOR_X(AggLocalState)
DECLARE_OPERATOR_X(ExchangeLocalState)
DECLARE_OPERATOR_X(RepeatLocalState)
DECLARE_OPERATOR_X(NestedLoopJoinProbeLocalState)
DECLARE_OPERATOR_X(AssertNumRowsLocalState)
DECLARE_OPERATOR_X(EmptySetLocalState)
DECLARE_OPERATOR_X(UnionSourceLocalState)

#undef DECLARE_OPERATOR_X

template class StreamingOperatorX<AssertNumRowsLocalState>;
template class StreamingOperatorX<SelectLocalState>;

template class StatefulOperatorX<HashJoinProbeLocalState>;
template class StatefulOperatorX<RepeatLocalState>;
template class StatefulOperatorX<NestedLoopJoinProbeLocalState>;

template class PipelineXSinkLocalState<HashJoinDependency>;
template class PipelineXSinkLocalState<SortDependency>;
template class PipelineXSinkLocalState<NestedLoopJoinDependency>;
template class PipelineXSinkLocalState<AnalyticDependency>;
template class PipelineXSinkLocalState<AggDependency>;
template class PipelineXSinkLocalState<FakeDependency>;
template class PipelineXSinkLocalState<UnionDependency>;

template class PipelineXLocalState<HashJoinDependency>;
template class PipelineXLocalState<SortDependency>;
template class PipelineXLocalState<NestedLoopJoinDependency>;
template class PipelineXLocalState<AnalyticDependency>;
template class PipelineXLocalState<AggDependency>;
template class PipelineXLocalState<FakeDependency>;
template class PipelineXLocalState<UnionDependency>;

} // namespace doris::pipeline
