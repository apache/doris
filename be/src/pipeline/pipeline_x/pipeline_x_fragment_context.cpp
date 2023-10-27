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

#include "pipeline_x_fragment_context.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Planner_types.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_context.h>
#include <opentelemetry/trace/tracer.h>
#include <pthread.h>
#include <runtime/result_buffer_mgr.h>
#include <stdlib.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <map>
#include <ostream>
#include <typeinfo>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "exec/data_sink.h"
#include "exec/exec_node.h"
#include "exec/scan_node.h"
#include "io/fs/stream_load_pipe.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/analytic_sink_operator.h"
#include "pipeline/exec/analytic_source_operator.h"
#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/datagen_operator.h"
#include "pipeline/exec/distinct_streaming_aggregation_sink_operator.h"
#include "pipeline/exec/distinct_streaming_aggregation_source_operator.h"
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
#include "pipeline/exec/partition_sort_sink_operator.h"
#include "pipeline/exec/partition_sort_source_operator.h"
#include "pipeline/exec/repeat_operator.h"
#include "pipeline/exec/result_file_sink_operator.h"
#include "pipeline/exec/result_sink_operator.h"
#include "pipeline/exec/scan_operator.h"
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
#include "pipeline/pipeline_x/local_exchange/local_exchange_sink_operator.h"
#include "pipeline/pipeline_x/local_exchange/local_exchange_source_operator.h"
#include "pipeline/task_scheduler.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/container_util.hpp"
#include "util/debug_util.h"
#include "util/telemetry/telemetry.h"
#include "util/uid_util.h"
#include "vec/common/assert_cast.h"
#include "vec/runtime/vdata_stream_mgr.h"

namespace doris::pipeline {

#define FOR_EACH_RUNTIME_STATE(stmt)              \
    for (auto& runtime_state : _runtime_states) { \
        stmt                                      \
    }

PipelineXFragmentContext::PipelineXFragmentContext(
        const TUniqueId& query_id, const int fragment_id, std::shared_ptr<QueryContext> query_ctx,
        ExecEnv* exec_env, const std::function<void(RuntimeState*, Status*)>& call_back,
        const report_status_callback& report_status_cb, bool group_commit)
        : PipelineFragmentContext(query_id, TUniqueId(), fragment_id, -1, query_ctx, exec_env,
                                  call_back, report_status_cb, group_commit) {}

PipelineXFragmentContext::~PipelineXFragmentContext() {
    auto st = _query_ctx->exec_status();
    if (!_runtime_states.empty()) {
        // The memory released by the query end is recorded in the query mem tracker, main memory in _runtime_state.
        SCOPED_ATTACH_TASK(_runtime_state.get());
        FOR_EACH_RUNTIME_STATE(_call_back(runtime_state.get(), &st); runtime_state.reset();)
    } else {
        _call_back(nullptr, &st);
    }
    _runtime_state.reset();
}

void PipelineXFragmentContext::cancel(const PPlanFragmentCancelReason& reason,
                                      const std::string& msg) {
    LOG_INFO("PipelineXFragmentContext::cancel")
            .tag("query_id", print_id(_query_id))
            .tag("fragment_id", _fragment_id)
            .tag("reason", reason)
            .tag("error message", msg);
    if (_query_ctx->cancel(true, msg, Status::Cancelled(msg))) {
        if (reason != PPlanFragmentCancelReason::LIMIT_REACH) {
            FOR_EACH_RUNTIME_STATE(LOG(WARNING) << "PipelineXFragmentContext cancel instance: "
                                                << print_id(runtime_state->fragment_instance_id());)
        } else {
            _set_is_report_on_cancel(false); // TODO bug llj
        }
        // Get pipe from new load stream manager and send cancel to it or the fragment may hang to wait read from pipe
        // For stream load the fragment's query_id == load id, it is set in FE.
        auto stream_load_ctx = _exec_env->new_load_stream_mgr()->get(_query_id);
        if (stream_load_ctx != nullptr) {
            stream_load_ctx->pipe->cancel(msg);
        }

        // Cancel the result queue manager used by spark doris connector
        // TODO pipeline incomp
        // _exec_env->result_queue_mgr()->update_queue_status(id, Status::Aborted(msg));
    }
}

Status PipelineXFragmentContext::prepare(const doris::TPipelineFragmentParams& request) {
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    _runtime_profile.reset(new RuntimeProfile("PipelineContext"));
    _start_timer = ADD_TIMER(_runtime_profile, "StartTime");
    COUNTER_UPDATE(_start_timer, _fragment_watcher.elapsed_time());
    _prepare_timer = ADD_TIMER(_runtime_profile, "PrepareTime");
    SCOPED_TIMER(_prepare_timer);

    auto* fragment_context = this;
    OpentelemetryTracer tracer = telemetry::get_noop_tracer();
    if (opentelemetry::trace::Tracer::GetCurrentSpan()->GetContext().IsValid()) {
        tracer = telemetry::get_tracer(print_id(_query_id));
    }

    LOG_INFO("PipelineXFragmentContext::prepare")
            .tag("query_id", print_id(_query_id))
            .tag("fragment_id", _fragment_id)
            .tag("pthread_id", (uintptr_t)pthread_self());

    if (request.query_options.__isset.is_report_success) {
        fragment_context->set_is_report_success(request.query_options.is_report_success);
    }

    // 1. Set up the global runtime state.
    _runtime_state = RuntimeState::create_unique(request.query_id, request.fragment_id,
                                                 request.query_options, _query_ctx->query_globals,
                                                 _exec_env);
    _runtime_state->set_query_ctx(_query_ctx.get());
    _runtime_state->set_query_mem_tracker(_query_ctx->query_mem_tracker);
    _runtime_state->set_tracer(std::move(tracer));

    SCOPED_ATTACH_TASK(_runtime_state.get());
    if (request.__isset.backend_id) {
        _runtime_state->set_backend_id(request.backend_id);
    }
    if (request.__isset.import_label) {
        _runtime_state->set_import_label(request.import_label);
    }
    if (request.__isset.db_name) {
        _runtime_state->set_db_name(request.db_name);
    }
    if (request.__isset.load_job_id) {
        _runtime_state->set_load_job_id(request.load_job_id);
    }

    if (request.is_simplified_param) {
        _desc_tbl = _query_ctx->desc_tbl;
    } else {
        DCHECK(request.__isset.desc_tbl);
        RETURN_IF_ERROR(
                DescriptorTbl::create(_runtime_state->obj_pool(), request.desc_tbl, &_desc_tbl));
    }
    _runtime_state->set_desc_tbl(_desc_tbl);
    _runtime_state->set_num_per_fragment_instances(request.num_senders);

    // 2. Build pipelines with operators in this fragment.
    auto root_pipeline = add_pipeline();
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_build_pipelines(
            _runtime_state->obj_pool(), request, *_query_ctx->desc_tbl, &_root_op, root_pipeline));

    // 3. Create sink operator
    if (!request.fragment.__isset.output_sink) {
        return Status::InternalError("No output sink in this fragment!");
    }
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_create_data_sink(
            _runtime_state->obj_pool(), request.fragment.output_sink, request.fragment.output_exprs,
            request, root_pipeline->output_row_desc(), _runtime_state.get(), *_desc_tbl,
            root_pipeline->id()));
    RETURN_IF_ERROR(_sink->init(request.fragment.output_sink));
    static_cast<void>(root_pipeline->set_sink(_sink));

    // 4. Initialize global states in pipelines.
    for (PipelinePtr& pipeline : _pipelines) {
        //TODO: can we do this in set_sink?
        static_cast<void>(pipeline->sink_x()->set_child(pipeline->operator_xs().back()));
        RETURN_IF_ERROR(pipeline->prepare(_runtime_state.get()));
    }

    // 5. Build pipeline tasks and initialize local state.
    RETURN_IF_ERROR(_build_pipeline_tasks(request));

    _init_next_report_time();

    _prepared = true;
    return Status::OK();
}

Status PipelineXFragmentContext::_create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                                                   const std::vector<TExpr>& output_exprs,
                                                   const TPipelineFragmentParams& params,
                                                   const RowDescriptor& row_desc,
                                                   RuntimeState* state, DescriptorTbl& desc_tbl,
                                                   PipelineId cur_pipeline_id) {
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        if (!thrift_sink.__isset.stream_sink) {
            return Status::InternalError("Missing data stream sink.");
        }
        bool send_query_statistics_with_every_batch =
                params.__isset.send_query_statistics_with_every_batch
                        ? params.send_query_statistics_with_every_batch
                        : false;
        _sink.reset(new ExchangeSinkOperatorX(state, row_desc, next_operator_id(),
                                              thrift_sink.stream_sink, params.destinations,
                                              send_query_statistics_with_every_batch));
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        if (!thrift_sink.__isset.result_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        // TODO: figure out good buffer size based on size of output row
        _sink.reset(new ResultSinkOperatorX(next_operator_id(), row_desc, output_exprs,
                                            thrift_sink.result_sink));
        break;
    }
    case TDataSinkType::OLAP_TABLE_SINK: {
        if (state->query_options().enable_memtable_on_sink_node) {
            return Status::InternalError(
                    "Unsuported OLAP_TABLE_SINK with enable_memtable_on_sink_node ");
        } else {
            _sink.reset(new OlapTableSinkOperatorX(pool, next_operator_id(), row_desc, output_exprs,
                                                   false));
        }
        break;
    }
    case TDataSinkType::JDBC_TABLE_SINK: {
        if (!thrift_sink.__isset.jdbc_table_sink) {
            return Status::InternalError("Missing data jdbc sink.");
        }
        if (config::enable_java_support) {
            _sink.reset(new JdbcTableSinkOperatorX(row_desc, next_operator_id(), output_exprs));
        } else {
            return Status::InternalError(
                    "Jdbc table sink is not enabled, you can change be config "
                    "enable_java_support to true and restart be.");
        }
        break;
    }
    case TDataSinkType::RESULT_FILE_SINK: {
        if (!thrift_sink.__isset.result_file_sink) {
            return Status::InternalError("Missing result file sink.");
        }

        // TODO: figure out good buffer size based on size of output row
        bool send_query_statistics_with_every_batch =
                params.__isset.send_query_statistics_with_every_batch
                        ? params.send_query_statistics_with_every_batch
                        : false;
        // Result file sink is not the top sink
        if (params.__isset.destinations && params.destinations.size() > 0) {
            _sink.reset(new ResultFileSinkOperatorX(
                    next_operator_id(), row_desc, thrift_sink.result_file_sink, params.destinations,
                    send_query_statistics_with_every_batch, output_exprs, desc_tbl));
        } else {
            _sink.reset(new ResultFileSinkOperatorX(next_operator_id(), row_desc, output_exprs));
        }
        break;
    }
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK: {
        DCHECK(thrift_sink.__isset.multi_cast_stream_sink);
        DCHECK_GT(thrift_sink.multi_cast_stream_sink.sinks.size(), 0);
        // TODO: figure out good buffer size based on size of output row
        auto sink_id = next_operator_id();
        auto sender_size = thrift_sink.multi_cast_stream_sink.sinks.size();
        // one sink has multiple sources.
        std::vector<int> sources;
        for (int i = 0; i < sender_size; ++i) {
            auto source_id = next_operator_id();
            sources.push_back(source_id);
        }

        _sink.reset(new MultiCastDataStreamSinkOperatorX(
                sink_id, sources, thrift_sink.multi_cast_stream_sink.sinks.size(), pool,
                thrift_sink.multi_cast_stream_sink, row_desc));
        for (int i = 0; i < sender_size; ++i) {
            auto new_pipeline = add_pipeline();
            RowDescriptor* _row_desc = nullptr;
            {
                auto& tmp_row_desc =
                        !thrift_sink.multi_cast_stream_sink.sinks[i].output_exprs.empty()
                                ? RowDescriptor(state->desc_tbl(),
                                                {thrift_sink.multi_cast_stream_sink.sinks[i]
                                                         .output_tuple_id},
                                                {false})
                                : _sink->row_desc();
                _row_desc = pool->add(new RowDescriptor(tmp_row_desc));
            }
            auto source_id = sources[i];
            OperatorXPtr source_op;
            // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
            source_op.reset(new MultiCastDataStreamerSourceOperatorX(
                    i, pool, thrift_sink.multi_cast_stream_sink.sinks[i], row_desc, source_id));
            static_cast<void>(new_pipeline->add_operator(source_op));
            // 2. create and set sink operator of data stream sender for new pipeline

            DataSinkOperatorXPtr sink_op;
            sink_op.reset(new ExchangeSinkOperatorX(
                    state, *_row_desc, next_operator_id(),
                    thrift_sink.multi_cast_stream_sink.sinks[i],
                    thrift_sink.multi_cast_stream_sink.destinations[i], false));

            static_cast<void>(new_pipeline->set_sink(sink_op));
            {
                TDataSink* t = pool->add(new TDataSink());
                t->stream_sink = thrift_sink.multi_cast_stream_sink.sinks[i];
                RETURN_IF_ERROR(sink_op->init(*t));
            }

            // 3. set dependency dag
            _dag[new_pipeline->id()].push_back(cur_pipeline_id);
        }
        if (sources.empty()) {
            return Status::InternalError("size of sources must be greater than 0");
        }
        break;
    }
    default:
        return Status::InternalError("Unsuported sink type in pipeline: {}", thrift_sink.type);
    }
    return Status::OK();
}

Status PipelineXFragmentContext::_build_pipeline_tasks(
        const doris::TPipelineFragmentParams& request) {
    _total_tasks = 0;
    int target_size = request.local_params.size();
    _runtime_states.resize(target_size);
    _tasks.resize(target_size);
    for (size_t i = 0; i < target_size; i++) {
        const auto& local_params = request.local_params[i];

        _runtime_states[i] = RuntimeState::create_unique(local_params, request.query_id,
                                                         request.fragment_id, request.query_options,
                                                         _query_ctx->query_globals, _exec_env);
        _runtime_states[i]->set_query_ctx(_query_ctx.get());
        _runtime_states[i]->set_query_mem_tracker(_query_ctx->query_mem_tracker);
        _runtime_states[i]->set_tracer(_runtime_state->get_tracer());

        static_cast<void>(_runtime_states[i]->runtime_filter_mgr()->init());
        _runtime_states[i]->set_be_number(local_params.backend_num);

        if (request.__isset.backend_id) {
            _runtime_states[i]->set_backend_id(request.backend_id);
        }
        if (request.__isset.import_label) {
            _runtime_states[i]->set_import_label(request.import_label);
        }
        if (request.__isset.db_name) {
            _runtime_states[i]->set_db_name(request.db_name);
        }
        if (request.__isset.load_job_id) {
            _runtime_states[i]->set_load_job_id(request.load_job_id);
        }

        _runtime_states[i]->set_desc_tbl(_desc_tbl);
        _runtime_states[i]->set_per_fragment_instance_idx(local_params.sender_id);
        _runtime_states[i]->set_num_per_fragment_instances(request.num_senders);
        _runtime_states[i]->resize_op_id_to_local_state(max_operator_id());
        std::map<PipelineId, PipelineXTask*> pipeline_id_to_task;
        for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
            auto task = std::make_unique<PipelineXTask>(
                    _pipelines[pip_idx], _total_tasks++, _runtime_states[i].get(), this,
                    _runtime_states[i]->runtime_profile(),
                    _op_id_to_le_state.count(
                            _pipelines[pip_idx]->operator_xs().front()->operator_id()) > 0
                            ? _op_id_to_le_state
                                      [_pipelines[pip_idx]->operator_xs().front()->operator_id()]
                            : nullptr,
                    i);
            pipeline_id_to_task.insert({_pipelines[pip_idx]->id(), task.get()});
            _tasks[i].emplace_back(std::move(task));
        }

        /**
         * Build DAG for pipeline tasks.
         * For example, we have
         *
         *   ExchangeSink (Pipeline1)     JoinBuildSink (Pipeline2)
         *            \                      /
         *          JoinProbeOperator1 (Pipeline1)    JoinBuildSink (Pipeline3)
         *                 \                          /
         *               JoinProbeOperator2 (Pipeline1)
         *
         * In this fragment, we have three pipelines and pipeline 1 depends on pipeline 2 and pipeline 3.
         * To build this DAG, `_dag` manage dependencies between pipelines by pipeline ID and
         * `pipeline_id_to_task` is used to find the task by a unique pipeline ID.
         *
         * Finally, we have two upstream dependencies in Pipeline1 corresponding to JoinProbeOperator1
         * and JoinProbeOperator2.
         */

        for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
            auto task = pipeline_id_to_task[_pipelines[pip_idx]->id()];
            DCHECK(task != nullptr);

            // if this task has upstream dependency, then record them.
            if (_dag.find(_pipelines[pip_idx]->id()) != _dag.end()) {
                auto& deps = _dag[_pipelines[pip_idx]->id()];
                for (auto& dep : deps) {
                    task->add_upstream_dependency(
                            pipeline_id_to_task[dep]->get_downstream_dependency());
                }
            }
            RETURN_IF_ERROR(task->prepare(_runtime_states[i].get(), local_params,
                                          request.fragment.output_sink));
        }

        {
            std::lock_guard<std::mutex> l(_state_map_lock);
            _instance_id_to_runtime_state.insert(
                    {UniqueId(_runtime_states[i]->fragment_instance_id()),
                     _runtime_states[i].get()});
        }
    }
    _pipeline_parent_map.clear();
    _dag.clear();
    _op_id_to_le_state.clear();

    return Status::OK();
}

Status PipelineXFragmentContext::_build_pipelines(ObjectPool* pool,
                                                  const doris::TPipelineFragmentParams& request,
                                                  const DescriptorTbl& descs, OperatorXPtr* root,
                                                  PipelinePtr cur_pipe) {
    if (request.fragment.plan.nodes.size() == 0) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid plan which has no plan node!");
    }

    int node_idx = 0;
    RETURN_IF_ERROR(_create_tree_helper(pool, request.fragment.plan.nodes, request, descs, nullptr,
                                        &node_idx, root, cur_pipe, 0));

    if (node_idx + 1 != request.fragment.plan.nodes.size()) {
        // TODO: print thrift msg for diagnostic purposes.
        return Status::InternalError(
                "Plan tree only partially reconstructed. Not all thrift nodes were used.");
    }

    return Status::OK();
}

Status PipelineXFragmentContext::_create_tree_helper(ObjectPool* pool,
                                                     const std::vector<TPlanNode>& tnodes,
                                                     const doris::TPipelineFragmentParams& request,
                                                     const DescriptorTbl& descs,
                                                     OperatorXPtr parent, int* node_idx,
                                                     OperatorXPtr* root, PipelinePtr& cur_pipe,
                                                     int child_idx) {
    // propagate error case
    if (*node_idx >= tnodes.size()) {
        // TODO: print thrift msg
        return Status::InternalError(
                "Failed to reconstruct plan tree from thrift. Node id: {}, number of nodes: {}",
                *node_idx, tnodes.size());
    }
    const TPlanNode& tnode = tnodes[*node_idx];

    int num_children = tnodes[*node_idx].num_children;
    OperatorXPtr op = nullptr;
    RETURN_IF_ERROR(_create_operator(pool, tnodes[*node_idx], request, descs, op, cur_pipe,
                                     parent == nullptr ? -1 : parent->node_id(), child_idx));

    // assert(parent != nullptr || (node_idx == 0 && root_expr != nullptr));
    if (parent != nullptr) {
        // add to parent's child(s)
        RETURN_IF_ERROR(parent->set_child(op));
    } else {
        *root = op;
    }

    // rely on that tnodes is preorder of the plan
    for (int i = 0; i < num_children; i++) {
        ++*node_idx;
        RETURN_IF_ERROR(_create_tree_helper(pool, tnodes, request, descs, op, node_idx, nullptr,
                                            cur_pipe, i));

        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= tnodes.size()) {
            // TODO: print thrift msg
            return Status::InternalError(
                    "Failed to reconstruct plan tree from thrift. Node id: {}, number of nodes: {}",
                    *node_idx, tnodes.size());
        }
    }

    RETURN_IF_ERROR(op->init(tnode, _runtime_state.get()));

    return Status::OK();
}

Status PipelineXFragmentContext::_add_local_exchange(ObjectPool* pool, OperatorXPtr& op,
                                                     PipelinePtr& cur_pipe,
                                                     const std::vector<TExpr>& texprs) {
    if (!_runtime_state->enable_local_shuffle() ||
        _runtime_state->query_parallel_instance_num() == 1) {
        return Status::OK();
    }
    auto local_exchange_id = next_operator_id();
    op.reset(new LocalExchangeSourceOperatorX(pool, local_exchange_id));
    RETURN_IF_ERROR(cur_pipe->add_operator(op));

    const auto downstream_pipeline_id = cur_pipe->id();
    if (_dag.find(downstream_pipeline_id) == _dag.end()) {
        _dag.insert({downstream_pipeline_id, {}});
    }
    cur_pipe = add_pipeline();
    _dag[downstream_pipeline_id].push_back(cur_pipe->id());

    DataSinkOperatorXPtr sink;
    sink.reset(new LocalExchangeSinkOperatorX(
            local_exchange_id, _runtime_state->query_parallel_instance_num(), texprs));
    RETURN_IF_ERROR(cur_pipe->set_sink(sink));
    RETURN_IF_ERROR(cur_pipe->sink_x()->init());

    auto shared_state = LocalExchangeSharedState::create_shared();
    shared_state->data_queue.resize(_runtime_state->query_parallel_instance_num());
    _op_id_to_le_state.insert({local_exchange_id, shared_state});
    return Status::OK();
}

Status PipelineXFragmentContext::_create_operator(ObjectPool* pool, const TPlanNode& tnode,
                                                  const doris::TPipelineFragmentParams& request,
                                                  const DescriptorTbl& descs, OperatorXPtr& op,
                                                  PipelinePtr& cur_pipe, int parent_idx,
                                                  int child_idx) {
    _pipeline_parent_map.pop(cur_pipe, parent_idx, child_idx);
    std::stringstream error_msg;
    switch (tnode.node_type) {
    case TPlanNodeType::OLAP_SCAN_NODE: {
        op.reset(new OlapScanOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case doris::TPlanNodeType::JDBC_SCAN_NODE: {
        op.reset(new JDBCScanOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case doris::TPlanNodeType::FILE_SCAN_NODE: {
        op.reset(new FileScanOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case TPlanNodeType::ES_SCAN_NODE:
    case TPlanNodeType::ES_HTTP_SCAN_NODE: {
        op.reset(new EsScanOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case TPlanNodeType::EXCHANGE_NODE: {
        int num_senders = find_with_default(request.per_exch_num_senders, tnode.node_id, 0);
        DCHECK_GT(num_senders, 0);
        op.reset(new ExchangeSourceOperatorX(pool, tnode, next_operator_id(), descs, num_senders));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case TPlanNodeType::AGGREGATION_NODE: {
        if (tnode.agg_node.aggregate_functions.empty()) {
            op.reset(new DistinctStreamingAggSourceOperatorX(pool, tnode, next_operator_id(),
                                                             descs));
            RETURN_IF_ERROR(cur_pipe->add_operator(op));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (_dag.find(downstream_pipeline_id) == _dag.end()) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            cur_pipe = add_pipeline();
            _dag[downstream_pipeline_id].push_back(cur_pipe->id());
            DataSinkOperatorXPtr sink;
            sink.reset(
                    new DistinctStreamingAggSinkOperatorX(pool, next_operator_id(), tnode, descs));
            sink->set_dests_id({op->operator_id()});
            RETURN_IF_ERROR(cur_pipe->set_sink(sink));
            RETURN_IF_ERROR(cur_pipe->sink_x()->init(tnode, _runtime_state.get()));
        } else if (tnode.agg_node.__isset.use_streaming_preaggregation &&
                   tnode.agg_node.use_streaming_preaggregation) {
            op.reset(new StreamingAggSourceOperatorX(pool, tnode, next_operator_id(), descs));
            RETURN_IF_ERROR(cur_pipe->add_operator(op));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (_dag.find(downstream_pipeline_id) == _dag.end()) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            cur_pipe = add_pipeline();
            _dag[downstream_pipeline_id].push_back(cur_pipe->id());
            DataSinkOperatorXPtr sink;
            sink.reset(new StreamingAggSinkOperatorX(pool, next_operator_id(), tnode, descs));
            sink->set_dests_id({op->operator_id()});
            RETURN_IF_ERROR(cur_pipe->set_sink(sink));
            RETURN_IF_ERROR(cur_pipe->sink_x()->init(tnode, _runtime_state.get()));
        } else {
            op.reset(new AggSourceOperatorX(pool, tnode, next_operator_id(), descs));
            RETURN_IF_ERROR(cur_pipe->add_operator(op));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (_dag.find(downstream_pipeline_id) == _dag.end()) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            cur_pipe = add_pipeline();
            _dag[downstream_pipeline_id].push_back(cur_pipe->id());

            DataSinkOperatorXPtr sink;
            sink.reset(new AggSinkOperatorX<>(pool, next_operator_id(), tnode, descs));
            sink->set_dests_id({op->operator_id()});
            RETURN_IF_ERROR(cur_pipe->set_sink(sink));
            RETURN_IF_ERROR(cur_pipe->sink_x()->init(tnode, _runtime_state.get()));

            if (!tnode.agg_node.need_finalize) {
                RETURN_IF_ERROR(op->init(tnode, _runtime_state.get()));
                RETURN_IF_ERROR(
                        _add_local_exchange(pool, op, cur_pipe, tnode.agg_node.grouping_exprs));
            }
        }
        break;
    }
    case TPlanNodeType::HASH_JOIN_NODE: {
        op.reset(new HashJoinProbeOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        PipelinePtr build_side_pipe = add_pipeline();
        _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

        DataSinkOperatorXPtr sink;
        sink.reset(new HashJoinBuildSinkOperatorX(pool, next_operator_id(), tnode, descs));
        sink->set_dests_id({op->operator_id()});
        RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
        RETURN_IF_ERROR(build_side_pipe->sink_x()->init(tnode, _runtime_state.get()));
        _pipeline_parent_map.push(op->node_id(), cur_pipe);
        _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        break;
    }
    case TPlanNodeType::CROSS_JOIN_NODE: {
        op.reset(new NestedLoopJoinProbeOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        PipelinePtr build_side_pipe = add_pipeline();
        _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

        DataSinkOperatorXPtr sink;
        sink.reset(new NestedLoopJoinBuildSinkOperatorX(pool, next_operator_id(), tnode, descs));
        sink->set_dests_id({op->operator_id()});
        RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
        RETURN_IF_ERROR(build_side_pipe->sink_x()->init(tnode, _runtime_state.get()));
        _pipeline_parent_map.push(op->node_id(), cur_pipe);
        _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        break;
    }
    case TPlanNodeType::UNION_NODE: {
        int child_count = tnode.num_children;
        op.reset(new UnionSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        for (int i = 0; i < child_count; i++) {
            PipelinePtr build_side_pipe = add_pipeline();
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());
            DataSinkOperatorXPtr sink;
            sink.reset(new UnionSinkOperatorX(i, next_operator_id(), pool, tnode, descs));
            sink->set_dests_id({op->operator_id()});
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
            RETURN_IF_ERROR(build_side_pipe->sink_x()->init(tnode, _runtime_state.get()));
            // preset children pipelines. if any pipeline found this as its father, will use the prepared pipeline to build.
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        }
        break;
    }
    case TPlanNodeType::SORT_NODE: {
        op.reset(new SortSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline();
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorXPtr sink;
        sink.reset(new SortSinkOperatorX(pool, next_operator_id(), tnode, descs));
        sink->set_dests_id({op->operator_id()});
        RETURN_IF_ERROR(cur_pipe->set_sink(sink));
        RETURN_IF_ERROR(cur_pipe->sink_x()->init(tnode, _runtime_state.get()));
        break;
    }
    case doris::TPlanNodeType::PARTITION_SORT_NODE: {
        op.reset(new PartitionSortSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline();
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorXPtr sink;
        sink.reset(new PartitionSortSinkOperatorX(pool, next_operator_id(), tnode, descs));
        sink->set_dests_id({op->operator_id()});
        RETURN_IF_ERROR(cur_pipe->set_sink(sink));
        RETURN_IF_ERROR(cur_pipe->sink_x()->init(tnode, _runtime_state.get()));
        break;
    }
    case TPlanNodeType::ANALYTIC_EVAL_NODE: {
        op.reset(new AnalyticSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline();
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorXPtr sink;
        sink.reset(new AnalyticSinkOperatorX(pool, next_operator_id(), tnode, descs));
        sink->set_dests_id({op->operator_id()});
        RETURN_IF_ERROR(cur_pipe->set_sink(sink));
        RETURN_IF_ERROR(cur_pipe->sink_x()->init(tnode, _runtime_state.get()));
        break;
    }
    case TPlanNodeType::INTERSECT_NODE: {
        RETURN_IF_ERROR(_build_operators_for_set_operation_node<true>(
                pool, tnode, descs, op, cur_pipe, parent_idx, child_idx));
        break;
    }
    case TPlanNodeType::EXCEPT_NODE: {
        RETURN_IF_ERROR(_build_operators_for_set_operation_node<false>(
                pool, tnode, descs, op, cur_pipe, parent_idx, child_idx));
        break;
    }
    case TPlanNodeType::REPEAT_NODE: {
        op.reset(new RepeatOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case TPlanNodeType::TABLE_FUNCTION_NODE: {
        op.reset(new TableFunctionOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case TPlanNodeType::ASSERT_NUM_ROWS_NODE: {
        op.reset(new AssertNumRowsOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case TPlanNodeType::EMPTY_SET_NODE: {
        op.reset(new EmptySetSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case TPlanNodeType::DATA_GEN_SCAN_NODE: {
        op.reset(new DataGenSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case TPlanNodeType::SCHEMA_SCAN_NODE: {
        op.reset(new SchemaScanOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case TPlanNodeType::META_SCAN_NODE: {
        op.reset(new MetaScanOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    case TPlanNodeType::SELECT_NODE: {
        op.reset(new SelectOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        break;
    }
    default:
        return Status::InternalError("Unsupported exec type in pipelineX: {}",
                                     print_plan_node_type(tnode.node_type));
    }

    return Status::OK();
}
template <bool is_intersect>
Status PipelineXFragmentContext::_build_operators_for_set_operation_node(
        ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs, OperatorXPtr& op,
        PipelinePtr& cur_pipe, int parent_idx, int child_idx) {
    op.reset(new SetSourceOperatorX<is_intersect>(pool, tnode, next_operator_id(), descs));
    RETURN_IF_ERROR(cur_pipe->add_operator(op));

    const auto downstream_pipeline_id = cur_pipe->id();
    if (_dag.find(downstream_pipeline_id) == _dag.end()) {
        _dag.insert({downstream_pipeline_id, {}});
    }

    for (int child_id = 0; child_id < tnode.num_children; child_id++) {
        PipelinePtr probe_side_pipe = add_pipeline();
        _dag[downstream_pipeline_id].push_back(probe_side_pipe->id());

        DataSinkOperatorXPtr sink;
        if (child_id == 0) {
            sink.reset(new SetSinkOperatorX<is_intersect>(child_id, next_operator_id(), pool, tnode,
                                                          descs));
        } else {
            sink.reset(new SetProbeSinkOperatorX<is_intersect>(child_id, next_operator_id(), pool,
                                                               tnode, descs));
        }
        sink->set_dests_id({op->operator_id()});
        RETURN_IF_ERROR(probe_side_pipe->set_sink(sink));
        RETURN_IF_ERROR(probe_side_pipe->sink_x()->init(tnode, _runtime_state.get()));
        // prepare children pipelines. if any pipeline found this as its father, will use the prepared pipeline to build.
        _pipeline_parent_map.push(op->node_id(), probe_side_pipe);
    }

    return Status::OK();
}

Status PipelineXFragmentContext::submit() {
    if (_submitted) {
        return Status::InternalError("submitted");
    }
    _submitted = true;

    int submit_tasks = 0;
    Status st;
    auto* scheduler = _exec_env->pipeline_task_scheduler();
    if (_task_group_entity) {
        scheduler = _exec_env->pipeline_task_group_scheduler();
    }
    for (auto& task : _tasks) {
        for (auto& t : task) {
            st = scheduler->schedule_task(t.get());
            if (!st) {
                std::lock_guard<std::mutex> l(_status_lock);
                cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "submit context fail");
                _total_tasks = submit_tasks;
                break;
            }
            submit_tasks++;
        }
    }
    if (!st.ok()) {
        std::lock_guard<std::mutex> l(_task_mutex);
        if (_closed_tasks == _total_tasks) {
            std::call_once(_close_once_flag, [this] { _close_action(); });
        }
        return Status::InternalError("Submit pipeline failed. err = {}, BE: {}", st.to_string(),
                                     BackendOptions::get_localhost());
    } else {
        return st;
    }
}

void PipelineXFragmentContext::close_sink() {
    FOR_EACH_RUNTIME_STATE(static_cast<void>(_sink->close(
            runtime_state.get(),
            _prepared ? Status::RuntimeError("prepare failed") : Status::OK())););
}

void PipelineXFragmentContext::close_if_prepare_failed() {
    if (_tasks.empty()) {
        FOR_EACH_RUNTIME_STATE(
                static_cast<void>(_root_op->close(runtime_state.get())); static_cast<void>(
                        _sink->close(runtime_state.get(), Status::RuntimeError("prepare failed")));)
    }
    for (auto& task : _tasks) {
        for (auto& t : task) {
            DCHECK(!t->is_pending_finish());
            WARN_IF_ERROR(t->close(Status::OK()), "close_if_prepare_failed failed: ");
            close_a_pipeline();
        }
    }
}

void PipelineXFragmentContext::_close_action() {
    _runtime_profile->total_time_counter()->update(_fragment_watcher.elapsed_time());
    static_cast<void>(send_report(true));
    // all submitted tasks done
    _exec_env->fragment_mgr()->remove_pipeline_context(shared_from_this());
}

Status PipelineXFragmentContext::send_report(bool done) {
    Status exec_status = Status::OK();
    {
        std::lock_guard<std::mutex> l(_status_lock);
        exec_status = _query_ctx->exec_status();
    }

    // If plan is done successfully, but _is_report_success is false,
    // no need to send report.
    if (!_is_report_success && done && exec_status.ok()) {
        return Status::NeedSendAgain("");
    }

    // If both _is_report_success and _is_report_on_cancel are false,
    // which means no matter query is success or failed, no report is needed.
    // This may happen when the query limit reached and
    // a internal cancellation being processed
    if (!_is_report_success && !_is_report_on_cancel) {
        return Status::NeedSendAgain("");
    }

    std::vector<RuntimeState*> runtime_states(_runtime_states.size());
    for (size_t i = 0; i < _runtime_states.size(); i++) {
        runtime_states[i] = _runtime_states[i].get();
    }

    return _report_status_cb(
            {true, exec_status, runtime_states, nullptr, nullptr, done || !exec_status.ok(),
             _query_ctx->coord_addr, _query_id, _fragment_id, TUniqueId(), _backend_num,
             _runtime_state.get(),
             std::bind(&PipelineFragmentContext::update_status, this, std::placeholders::_1),
             std::bind(&PipelineFragmentContext::cancel, this, std::placeholders::_1,
                       std::placeholders::_2)},
            shared_from_this());
}
} // namespace doris::pipeline
