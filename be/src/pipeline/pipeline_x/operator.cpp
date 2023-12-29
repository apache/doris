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

#include <glog/logging.h>

#include <memory>
#include <string>

#include "common/logging.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/analytic_sink_operator.h"
#include "pipeline/exec/analytic_source_operator.h"
#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/datagen_operator.h"
#include "pipeline/exec/distinct_streaming_aggregation_sink_operator.h"
#include "pipeline/exec/empty_set_operator.h"
#include "pipeline/exec/es_scan_operator.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "pipeline/exec/file_scan_operator.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/jdbc_scan_operator.h"
#include "pipeline/exec/jdbc_table_sink_operator.h"
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
#include "pipeline/exec/streaming_aggregation_sink_operator.h"
#include "pipeline/exec/streaming_aggregation_source_operator.h"
#include "pipeline/exec/table_function_operator.h"
#include "pipeline/exec/union_sink_operator.h"
#include "pipeline/exec/union_source_operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/local_exchange/local_exchange_sink_operator.h"
#include "pipeline/pipeline_x/local_exchange/local_exchange_source_operator.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace doris::pipeline {

template <typename DependencyType>
std::string PipelineXLocalState<DependencyType>::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", _parent->debug_string(indentation_level));
    return fmt::to_string(debug_string_buffer);
}

template <typename DependencyType>
std::string PipelineXSinkLocalState<DependencyType>::debug_string(int indentation_level) const {
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
    return state->get_local_state(operator_id())->close(state);
}

void PipelineXLocalStateBase::clear_origin_block() {
    _origin_block.clear_column_data(_parent->_row_descriptor.num_materialized_slots());
}

Status OperatorXBase::do_projections(RuntimeState* state, vectorized::Block* origin_block,
                                     vectorized::Block* output_block) const {
    auto local_state = state->get_local_state(operator_id());
    SCOPED_TIMER(local_state->exec_time_counter());
    SCOPED_TIMER(local_state->_projection_timer);
    using namespace vectorized;
    vectorized::MutableBlock mutable_block =
            vectorized::VectorizedUtils::build_mutable_mem_reuse_block(output_block,
                                                                       *_output_row_descriptor);
    auto rows = origin_block->rows();

    if (rows != 0) {
        auto& mutable_columns = mutable_block.mutable_columns();
        DCHECK(mutable_columns.size() == local_state->_projections.size());
        for (int i = 0; i < mutable_columns.size(); ++i) {
            auto result_column_id = -1;
            RETURN_IF_ERROR(local_state->_projections[i]->execute(origin_block, &result_column_id));
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
        output_block->set_columns(std::move(mutable_columns));
    }

    return Status::OK();
}

Status OperatorXBase::get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                               SourceState& source_state) {
    auto local_state = state->get_local_state(operator_id());
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
    return state->get_sink_local_state(operator_id())->debug_string(indentation_level);
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
    auto local_state = LocalStateType::create_unique(this, state);
    RETURN_IF_ERROR(local_state->init(state, info));
    state->emplace_sink_local_state(operator_id(), std::move(local_state));
    return Status::OK();
}

template <typename SharedStateType>
constexpr bool NeedToCreate = true;
template <>
inline constexpr bool NeedToCreate<MultiCastSharedState> = false;
template <>
inline constexpr bool NeedToCreate<SetSharedState> = false;
template <>
inline constexpr bool NeedToCreate<UnionSharedState> = false;
template <>
inline constexpr bool NeedToCreate<LocalExchangeSharedState> = false;

template <typename LocalStateType>
void DataSinkOperatorX<LocalStateType>::get_dependency(vector<DependencySPtr>& dependency,
                                                       QueryContext* ctx) {
    std::shared_ptr<typename LocalStateType::DependencyType::SharedState> ss = nullptr;
    if constexpr (NeedToCreate<typename LocalStateType::DependencyType::SharedState>) {
        ss.reset(new typename LocalStateType::DependencyType::SharedState());
    }
    if constexpr (!std::is_same_v<typename LocalStateType::DependencyType, FakeDependency>) {
        auto& dests = dests_id();
        for (auto& dest_id : dests) {
            dependency.push_back(std::make_shared<typename LocalStateType::DependencyType>(
                    dest_id, _node_id, ctx));
            dependency.back()->set_shared_state(ss);
        }
    } else {
        dependency.push_back(nullptr);
    }
}

template <typename LocalStateType>
DependencySPtr OperatorX<LocalStateType>::get_dependency(QueryContext* ctx) {
    return std::make_shared<typename LocalStateType::DependencyType>(_operator_id, _node_id, ctx);
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
        : _num_rows_returned(0),
          _rows_returned_counter(nullptr),
          _peak_memory_usage_counter(nullptr),
          _parent(parent),
          _state(state) {}

template <typename DependencyType>
Status PipelineXLocalState<DependencyType>::init(RuntimeState* state, LocalStateInfo& info) {
    _runtime_profile.reset(new RuntimeProfile(_parent->get_name() + name_suffix()));
    _runtime_profile->set_metadata(_parent->node_id());
    _runtime_profile->set_is_sink(false);
    info.parent_profile->add_child(_runtime_profile.get(), true, nullptr);
    constexpr auto is_fake_shared =
            std::is_same_v<typename DependencyType::SharedState, FakeSharedState>;
    _dependency = (DependencyType*)info.dependency.get();
    if constexpr (!std::is_same_v<FakeDependency, DependencyType>) {
        _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
                _runtime_profile, "WaitForDependency[" + _dependency->name() + "]Time", 1);
        auto& deps = info.upstream_dependencies;
        if constexpr (std::is_same_v<LocalExchangeSourceDependency, DependencyType>) {
            _dependency->set_shared_state(info.le_state_map[_parent->operator_id()].first);
            _shared_state =
                    (typename DependencyType::SharedState*)_dependency->shared_state().get();

            _shared_state->source_dep = info.dependency;
            _shared_state->sink_dep = deps.front();
        } else if constexpr (!is_fake_shared) {
            _dependency->set_shared_state(deps.front()->shared_state());
            _shared_state =
                    (typename DependencyType::SharedState*)_dependency->shared_state().get();

            _shared_state->source_dep = info.dependency;
            _shared_state->sink_dep = deps.front();
        }
    }

    _conjuncts.resize(_parent->_conjuncts.size());
    _projections.resize(_parent->_projections.size());
    for (size_t i = 0; i < _conjuncts.size(); i++) {
        RETURN_IF_ERROR(_parent->_conjuncts[i]->clone(state, _conjuncts[i]));
    }
    for (size_t i = 0; i < _projections.size(); i++) {
        RETURN_IF_ERROR(_parent->_projections[i]->clone(state, _projections[i]));
    }
    _rows_returned_counter =
            ADD_COUNTER_WITH_LEVEL(_runtime_profile, "RowsProduced", TUnit::UNIT, 1);
    _blocks_returned_counter =
            ADD_COUNTER_WITH_LEVEL(_runtime_profile, "BlocksProduced", TUnit::UNIT, 1);
    _projection_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "ProjectionTime", 1);
    _open_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "OpenTime", 1);
    _close_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "CloseTime", 1);
    _exec_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "ExecTime", 1);
    _mem_tracker = std::make_unique<MemTracker>("PipelineXLocalState:" + _runtime_profile->name());
    _memory_used_counter = ADD_LABEL_COUNTER(_runtime_profile, "MemoryUsage");
    _peak_memory_usage_counter = _runtime_profile->AddHighWaterMarkCounter(
            "PeakMemoryUsage", TUnit::BYTES, "MemoryUsage");
    return Status::OK();
}

template <typename DependencyType>
Status PipelineXLocalState<DependencyType>::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    if (_shared_state) {
        RETURN_IF_ERROR(_shared_state->close(state));
    }
    if constexpr (!std::is_same_v<DependencyType, FakeDependency>) {
        COUNTER_SET(_wait_for_dependency_timer, _dependency->watcher_elapse_time());
    }
    if (_rows_returned_counter != nullptr) {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
    if (_peak_memory_usage_counter) {
        _peak_memory_usage_counter->set(_mem_tracker->peak_consumption());
    }
    _closed = true;
    return Status::OK();
}

template <typename DependencyType>
Status PipelineXSinkLocalState<DependencyType>::init(RuntimeState* state,
                                                     LocalSinkStateInfo& info) {
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(_parent->get_name() + name_suffix()));
    _profile->set_metadata(_parent->node_id());
    _profile->set_is_sink(true);
    _wait_for_finish_dependency_timer = ADD_TIMER(_profile, "PendingFinishDependency");
    constexpr auto is_fake_shared =
            std::is_same_v<typename DependencyType::SharedState, FakeSharedState>;
    if constexpr (!std::is_same_v<FakeDependency, DependencyType>) {
        auto& deps = info.dependencys;
        _dependency = (DependencyType*)deps.front().get();
        if constexpr (std::is_same_v<LocalExchangeSinkDependency, DependencyType>) {
            _dependency = info.le_state_map[_parent->dests_id().front()].second.get();
        }
        if (_dependency) {
            if constexpr (!is_fake_shared) {
                _shared_state =
                        (typename DependencyType::SharedState*)_dependency->shared_state().get();
            }

            _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
                    _profile, "WaitForDependency[" + _dependency->name() + "]Time", 1);
        }
    } else {
        auto& deps = info.dependencys;
        deps.front() = std::make_shared<FakeDependency>(0, 0, state->get_query_ctx());
        _dependency = (DependencyType*)deps.front().get();
    }
    _rows_input_counter = ADD_COUNTER_WITH_LEVEL(_profile, "InputRows", TUnit::UNIT, 1);
    _open_timer = ADD_TIMER_WITH_LEVEL(_profile, "OpenTime", 1);
    _close_timer = ADD_TIMER_WITH_LEVEL(_profile, "CloseTime", 1);
    _exec_timer = ADD_TIMER_WITH_LEVEL(_profile, "ExecTime", 1);
    info.parent_profile->add_child(_profile, true, nullptr);
    _mem_tracker = std::make_unique<MemTracker>(_parent->get_name());
    _memory_used_counter = ADD_LABEL_COUNTER(_profile, "MemoryUsage");
    _peak_memory_usage_counter =
            _profile->AddHighWaterMarkCounter("PeakMemoryUsage", TUnit::BYTES, "MemoryUsage");
    return Status::OK();
}

template <typename DependencyType>
Status PipelineXSinkLocalState<DependencyType>::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    if (_shared_state) {
        RETURN_IF_ERROR(_shared_state->close(state));
    }
    if constexpr (!std::is_same_v<DependencyType, FakeDependency>) {
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
                                                     SourceState& source_state) {
    RETURN_IF_ERROR(OperatorX<LocalStateType>::_child_x->get_block_after_projects(state, block,
                                                                                  source_state));
    return pull(state, block, source_state);
}

template <typename LocalStateType>
Status StatefulOperatorX<LocalStateType>::get_block(RuntimeState* state, vectorized::Block* block,
                                                    SourceState& source_state) {
    auto& local_state = state->get_local_state(OperatorX<LocalStateType>::operator_id())
                                ->template cast<LocalStateType>();
    if (need_more_input_data(state)) {
        local_state._child_block->clear_column_data();
        RETURN_IF_ERROR(OperatorX<LocalStateType>::_child_x->get_block_after_projects(
                state, local_state._child_block.get(), local_state._child_source_state));
        source_state = local_state._child_source_state;
        if (local_state._child_block->rows() == 0 &&
            local_state._child_source_state != SourceState::FINISHED) {
            return Status::OK();
        }
        {
            SCOPED_TIMER(local_state.exec_time_counter());
            RETURN_IF_ERROR(
                    push(state, local_state._child_block.get(), local_state._child_source_state));
        }
    }

    if (!need_more_input_data(state)) {
        SCOPED_TIMER(local_state.exec_time_counter());
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

template <typename Writer, typename Parent>
Status AsyncWriterSink<Writer, Parent>::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    _output_vexpr_ctxs.resize(_parent->cast<Parent>()._output_vexpr_ctxs.size());
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
        RETURN_IF_ERROR(
                _parent->cast<Parent>()._output_vexpr_ctxs[i]->clone(state, _output_vexpr_ctxs[i]));
    }
    _writer.reset(new Writer(info.tsink, _output_vexpr_ctxs));
    _async_writer_dependency = AsyncWriterDependency::create_shared(
            _parent->operator_id(), _parent->node_id(), state->get_query_ctx());
    _writer->set_dependency(_async_writer_dependency.get(), _finish_dependency.get());

    _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
            _profile, "WaitForDependency[" + _async_writer_dependency->name() + "]Time", 1);
    _finish_dependency->block();
    return Status::OK();
}

template <typename Writer, typename Parent>
Status AsyncWriterSink<Writer, Parent>::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    _writer->start_writer(state, _profile);
    return Status::OK();
}

template <typename Writer, typename Parent>
Status AsyncWriterSink<Writer, Parent>::sink(RuntimeState* state, vectorized::Block* block,
                                             SourceState source_state) {
    return _writer->sink(block, source_state == SourceState::FINISHED);
}

template <typename Writer, typename Parent>
Status AsyncWriterSink<Writer, Parent>::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    COUNTER_SET(_wait_for_dependency_timer, _async_writer_dependency->watcher_elapse_time());
    COUNTER_SET(_wait_for_finish_dependency_timer, _finish_dependency->watcher_elapse_time());
    // if the init failed, the _writer may be nullptr. so here need check
    if (_writer) {
        if (_writer->need_normal_close()) {
            if (exec_status.ok() && !state->is_cancelled()) {
                RETURN_IF_ERROR(_writer->commit_trans());
            }
            RETURN_IF_ERROR(_writer->close(exec_status));
        } else {
            RETURN_IF_ERROR(_writer->get_writer_status());
        }
    }
    return Base::close(state, exec_status);
}

template <typename Writer, typename Parent>
Status AsyncWriterSink<Writer, Parent>::try_close(RuntimeState* state, Status exec_status) {
    if (state->is_cancelled() || !exec_status.ok()) {
        _writer->force_close(!exec_status.ok() ? exec_status : Status::Cancelled("Cancelled"));
    }
    return Status::OK();
}

#define DECLARE_OPERATOR_X(LOCAL_STATE) template class DataSinkOperatorX<LOCAL_STATE>;
DECLARE_OPERATOR_X(HashJoinBuildSinkLocalState)
DECLARE_OPERATOR_X(ResultSinkLocalState)
DECLARE_OPERATOR_X(JdbcTableSinkLocalState)
DECLARE_OPERATOR_X(ResultFileSinkLocalState)
DECLARE_OPERATOR_X(OlapTableSinkLocalState)
DECLARE_OPERATOR_X(OlapTableSinkV2LocalState)
DECLARE_OPERATOR_X(AnalyticSinkLocalState)
DECLARE_OPERATOR_X(SortSinkLocalState)
DECLARE_OPERATOR_X(LocalExchangeSinkLocalState)
DECLARE_OPERATOR_X(BlockingAggSinkLocalState)
DECLARE_OPERATOR_X(StreamingAggSinkLocalState)
DECLARE_OPERATOR_X(DistinctStreamingAggSinkLocalState)
DECLARE_OPERATOR_X(ExchangeSinkLocalState)
DECLARE_OPERATOR_X(NestedLoopJoinBuildSinkLocalState)
DECLARE_OPERATOR_X(UnionSinkLocalState)
DECLARE_OPERATOR_X(MultiCastDataStreamSinkLocalState)
DECLARE_OPERATOR_X(PartitionSortSinkLocalState)
DECLARE_OPERATOR_X(SetProbeSinkLocalState<true>)
DECLARE_OPERATOR_X(SetProbeSinkLocalState<false>)
DECLARE_OPERATOR_X(SetSinkLocalState<true>)
DECLARE_OPERATOR_X(SetSinkLocalState<false>)

#undef DECLARE_OPERATOR_X

#define DECLARE_OPERATOR_X(LOCAL_STATE) template class OperatorX<LOCAL_STATE>;
DECLARE_OPERATOR_X(HashJoinProbeLocalState)
DECLARE_OPERATOR_X(OlapScanLocalState)
DECLARE_OPERATOR_X(JDBCScanLocalState)
DECLARE_OPERATOR_X(FileScanLocalState)
DECLARE_OPERATOR_X(EsScanLocalState)
DECLARE_OPERATOR_X(AnalyticLocalState)
DECLARE_OPERATOR_X(SortLocalState)
DECLARE_OPERATOR_X(AggLocalState)
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

#undef DECLARE_OPERATOR_X

template class StreamingOperatorX<AssertNumRowsLocalState>;
template class StreamingOperatorX<SelectLocalState>;

template class StatefulOperatorX<HashJoinProbeLocalState>;
template class StatefulOperatorX<RepeatLocalState>;
template class StatefulOperatorX<NestedLoopJoinProbeLocalState>;
template class StatefulOperatorX<TableFunctionLocalState>;

template class PipelineXSinkLocalState<SharedHashTableDependency>;
template class PipelineXSinkLocalState<SortSinkDependency>;
template class PipelineXSinkLocalState<NestedLoopJoinBuildSinkDependency>;
template class PipelineXSinkLocalState<AnalyticSinkDependency>;
template class PipelineXSinkLocalState<AggSinkDependency>;
template class PipelineXSinkLocalState<FakeDependency>;
template class PipelineXSinkLocalState<UnionSinkDependency>;
template class PipelineXSinkLocalState<PartitionSortSinkDependency>;
template class PipelineXSinkLocalState<MultiCastSinkDependency>;
template class PipelineXSinkLocalState<SetSinkDependency>;
template class PipelineXSinkLocalState<SetProbeSinkDependency>;
template class PipelineXSinkLocalState<LocalExchangeSinkDependency>;
template class PipelineXSinkLocalState<AndDependency>;
template class PipelineXSinkLocalState<ResultSinkDependency>;

template class PipelineXLocalState<HashJoinProbeDependency>;
template class PipelineXLocalState<SortSourceDependency>;
template class PipelineXLocalState<NestedLoopJoinProbeDependency>;
template class PipelineXLocalState<AnalyticSourceDependency>;
template class PipelineXLocalState<AggSourceDependency>;
template class PipelineXLocalState<FakeDependency>;
template class PipelineXLocalState<UnionSourceDependency>;
template class PipelineXLocalState<MultiCastSourceDependency>;
template class PipelineXLocalState<PartitionSortSourceDependency>;
template class PipelineXLocalState<SetSourceDependency>;
template class PipelineXLocalState<LocalExchangeSourceDependency>;
template class PipelineXLocalState<AndDependency>;
template class PipelineXLocalState<ScanDependency>;

template class AsyncWriterSink<doris::vectorized::VFileResultWriter, ResultFileSinkOperatorX>;
template class AsyncWriterSink<doris::vectorized::VJdbcTableWriter, JdbcTableSinkOperatorX>;
template class AsyncWriterSink<doris::vectorized::VTabletWriter, OlapTableSinkOperatorX>;
template class AsyncWriterSink<doris::vectorized::VTabletWriterV2, OlapTableSinkV2OperatorX>;

} // namespace doris::pipeline
