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
#include "pipeline/exec/olap_table_sink_v2_operator.h"
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
#include "util/uid_util.h"
#include "vec/common/assert_cast.h"
#include "vec/runtime/vdata_stream_mgr.h"

namespace doris::pipeline {

PipelineXFragmentContext::PipelineXFragmentContext(
        const TUniqueId& query_id, const int fragment_id, std::shared_ptr<QueryContext> query_ctx,
        ExecEnv* exec_env, const std::function<void(RuntimeState*, Status*)>& call_back,
        const report_status_callback& report_status_cb)
        : PipelineFragmentContext(query_id, TUniqueId(), fragment_id, -1, query_ctx, exec_env,
                                  call_back, report_status_cb) {}

PipelineXFragmentContext::~PipelineXFragmentContext() {
    auto st = _query_ctx->exec_status();
    if (!_task_runtime_states.empty()) {
        // The memory released by the query end is recorded in the query mem tracker, main memory in _runtime_state.
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_runtime_state->query_mem_tracker());
        for (auto& runtime_state : _task_runtime_states) {
            _call_back(runtime_state.get(), &st);
            runtime_state.reset();
        }
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
    if (reason == PPlanFragmentCancelReason::TIMEOUT) {
        LOG(WARNING) << "PipelineXFragmentContext is cancelled due to timeout : " << debug_string();
    }
    if (_query_ctx->cancel(true, msg, Status::Cancelled(msg), _fragment_id)) {
        if (reason != PPlanFragmentCancelReason::LIMIT_REACH) {
            for (auto& id : _fragment_instance_ids) {
                LOG(WARNING) << "PipelineXFragmentContext cancel instance: " << print_id(id);
            }
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
    for (auto& tasks : _tasks) {
        for (auto& task : tasks) {
            task->clear_blocking_state();
        }
    }
}

Status PipelineXFragmentContext::prepare(const doris::TPipelineFragmentParams& request) {
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    _num_instances = request.local_params.size();
    _runtime_profile.reset(new RuntimeProfile("PipelineContext"));
    _prepare_timer = ADD_TIMER(_runtime_profile, "PrepareTime");
    SCOPED_TIMER(_prepare_timer);

    auto* fragment_context = this;

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
    _runtime_state->set_load_stream_per_node(request.load_stream_per_node);
    _runtime_state->set_total_load_streams(request.total_load_streams);
    _runtime_state->set_num_local_sink(request.num_local_sink);

    _use_global_rf = request.__isset.parallel_instances && (request.__isset.per_node_shared_scans &&
                                                            !request.per_node_shared_scans.empty());
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

    for (PipelinePtr& pipeline : _pipelines) {
        DCHECK(pipeline->sink_x() != nullptr) << pipeline->operator_xs().size();
        static_cast<void>(pipeline->sink_x()->set_child(pipeline->operator_xs().back()));
    }
    if (_enable_local_shuffle()) {
        RETURN_IF_ERROR(
                _plan_local_exchange(request.num_buckets, request.bucket_seq_to_instance_idx));
    }
    // 4. Initialize global states in pipelines.
    for (PipelinePtr& pipeline : _pipelines) {
        pipeline->children().clear();
        RETURN_IF_ERROR(pipeline->prepare(_runtime_state.get()));
    }

    // 5. Build pipeline tasks and initialize local state.
    RETURN_IF_ERROR(_build_pipeline_tasks(request));

    _init_next_report_time();

    _prepared = true;
    return Status::OK();
}

Status PipelineXFragmentContext::_plan_local_exchange(
        int num_buckets, const std::map<int, int>& bucket_seq_to_instance_idx) {
    for (int pip_idx = _pipelines.size() - 1; pip_idx >= 0; pip_idx--) {
        _pipelines[pip_idx]->init_data_distribution();
        // Set property if child pipeline is not join operator's child.
        if (!_pipelines[pip_idx]->children().empty()) {
            for (auto& child : _pipelines[pip_idx]->children()) {
                if (child->sink_x()->node_id() ==
                    _pipelines[pip_idx]->operator_xs().front()->node_id()) {
                    RETURN_IF_ERROR(_pipelines[pip_idx]->operator_xs().front()->set_child(
                            child->operator_xs().back()));
                    _pipelines[pip_idx]->set_data_distribution(child->data_distribution());
                }
            }
        }

        RETURN_IF_ERROR(_plan_local_exchange(
                _pipelines[pip_idx]->operator_xs().front()->ignore_data_hash_distribution()
                        ? _num_instances
                        : num_buckets,
                pip_idx, _pipelines[pip_idx], bucket_seq_to_instance_idx,
                _pipelines[pip_idx]->operator_xs().front()->ignore_data_hash_distribution()));
    }
    return Status::OK();
}

Status PipelineXFragmentContext::_plan_local_exchange(
        int num_buckets, int pip_idx, PipelinePtr pip,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const bool ignore_data_hash_distribution) {
    int idx = 1;
    bool do_local_exchange = false;
    do {
        auto& ops = pip->operator_xs();
        do_local_exchange = false;
        // Plan local exchange for each operator.
        for (; idx < ops.size();) {
            if (ops[idx]->required_data_distribution().need_local_exchange()) {
                RETURN_IF_ERROR(_add_local_exchange(
                        pip_idx, idx, ops[idx]->node_id(), _runtime_state->obj_pool(), pip,
                        ops[idx]->required_data_distribution(), &do_local_exchange, num_buckets,
                        bucket_seq_to_instance_idx, ignore_data_hash_distribution));
            }
            if (do_local_exchange) {
                // If local exchange is needed for current operator, we will split this pipeline to
                // two pipelines by local exchange sink/source. And then we need to process remaining
                // operators in this pipeline so we set idx to 2 (0 is local exchange source and 1
                // is current operator was already processed) and continue to plan local exchange.
                idx = 2;
                break;
            }
            idx++;
        }
    } while (do_local_exchange);
    if (pip->sink_x()->required_data_distribution().need_local_exchange()) {
        RETURN_IF_ERROR(_add_local_exchange(
                pip_idx, idx, pip->sink_x()->node_id(), _runtime_state->obj_pool(), pip,
                pip->sink_x()->required_data_distribution(), &do_local_exchange, num_buckets,
                bucket_seq_to_instance_idx, ignore_data_hash_distribution));
    }
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
        _sink.reset(new ExchangeSinkOperatorX(state, row_desc, next_sink_operator_id(),
                                              thrift_sink.stream_sink, params.destinations,
                                              send_query_statistics_with_every_batch));
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        if (!thrift_sink.__isset.result_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        // TODO: figure out good buffer size based on size of output row
        _sink.reset(new ResultSinkOperatorX(next_sink_operator_id(), row_desc, output_exprs,
                                            thrift_sink.result_sink));
        break;
    }
    case TDataSinkType::OLAP_TABLE_SINK: {
        if (state->query_options().enable_memtable_on_sink_node &&
            !_has_inverted_index_or_partial_update(thrift_sink.olap_table_sink)) {
            _sink.reset(new OlapTableSinkV2OperatorX(pool, next_sink_operator_id(), row_desc,
                                                     output_exprs));
        } else {
            _sink.reset(new OlapTableSinkOperatorX(pool, next_sink_operator_id(), row_desc,
                                                   output_exprs));
        }
        break;
    }
    case TDataSinkType::JDBC_TABLE_SINK: {
        if (!thrift_sink.__isset.jdbc_table_sink) {
            return Status::InternalError("Missing data jdbc sink.");
        }
        if (config::enable_java_support) {
            _sink.reset(
                    new JdbcTableSinkOperatorX(row_desc, next_sink_operator_id(), output_exprs));
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
                    next_sink_operator_id(), row_desc, thrift_sink.result_file_sink,
                    params.destinations, send_query_statistics_with_every_batch, output_exprs,
                    desc_tbl));
        } else {
            _sink.reset(
                    new ResultFileSinkOperatorX(next_sink_operator_id(), row_desc, output_exprs));
        }
        break;
    }
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK: {
        DCHECK(thrift_sink.__isset.multi_cast_stream_sink);
        DCHECK_GT(thrift_sink.multi_cast_stream_sink.sinks.size(), 0);
        // TODO: figure out good buffer size based on size of output row
        auto sink_id = next_sink_operator_id();
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
                    state, *_row_desc, next_sink_operator_id(),
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
    _tasks.resize(target_size);
    auto& pipeline_id_to_profile = _runtime_state->pipeline_id_to_profile();
    DCHECK(pipeline_id_to_profile.empty());
    pipeline_id_to_profile.resize(_pipelines.size());
    {
        size_t pip_idx = 0;
        for (auto& pipeline_profile : pipeline_id_to_profile) {
            pipeline_profile =
                    std::make_unique<RuntimeProfile>("Pipeline : " + std::to_string(pip_idx));
            pip_idx++;
        }
    }

    for (size_t i = 0; i < target_size; i++) {
        const auto& local_params = request.local_params[i];
        auto fragment_instance_id = local_params.fragment_instance_id;
        _fragment_instance_ids.push_back(fragment_instance_id);
        std::unique_ptr<RuntimeFilterMgr> runtime_filter_mgr;
        auto init_runtime_state = [&](std::unique_ptr<RuntimeState>& runtime_state) {
            runtime_state->set_query_mem_tracker(_query_ctx->query_mem_tracker);

            runtime_state->set_query_ctx(_query_ctx.get());
            runtime_state->set_task_execution_context(shared_from_this());
            runtime_state->set_be_number(local_params.backend_num);

            if (request.__isset.backend_id) {
                runtime_state->set_backend_id(request.backend_id);
            }
            if (request.__isset.import_label) {
                runtime_state->set_import_label(request.import_label);
            }
            if (request.__isset.db_name) {
                runtime_state->set_db_name(request.db_name);
            }
            if (request.__isset.load_job_id) {
                runtime_state->set_load_job_id(request.load_job_id);
            }

            runtime_state->set_desc_tbl(_desc_tbl);
            runtime_state->set_per_fragment_instance_idx(local_params.sender_id);
            runtime_state->set_num_per_fragment_instances(request.num_senders);
            runtime_state->resize_op_id_to_local_state(max_operator_id(), max_sink_operator_id());
            runtime_state->set_load_stream_per_node(request.load_stream_per_node);
            runtime_state->set_total_load_streams(request.total_load_streams);
            runtime_state->set_num_local_sink(request.num_local_sink);
            DCHECK(runtime_filter_mgr);
            runtime_state->set_pipeline_x_runtime_filter_mgr(runtime_filter_mgr.get());
        };

        auto filterparams = std::make_unique<RuntimeFilterParamsContext>();

        {
            filterparams->runtime_filter_wait_infinitely =
                    _runtime_state->runtime_filter_wait_infinitely();
            filterparams->runtime_filter_wait_time_ms =
                    _runtime_state->runtime_filter_wait_time_ms();
            filterparams->enable_pipeline_exec = _runtime_state->enable_pipeline_exec();
            filterparams->execution_timeout = _runtime_state->execution_timeout();

            filterparams->exec_env = ExecEnv::GetInstance();
            filterparams->query_id.set_hi(_runtime_state->query_id().hi);
            filterparams->query_id.set_lo(_runtime_state->query_id().lo);

            filterparams->_fragment_instance_id.set_hi(fragment_instance_id.hi);
            filterparams->_fragment_instance_id.set_lo(fragment_instance_id.lo);
            filterparams->be_exec_version = _runtime_state->be_exec_version();
            filterparams->query_ctx = _query_ctx.get();
        }

        // build runtime_filter_mgr for each instance
        runtime_filter_mgr =
                std::make_unique<RuntimeFilterMgr>(request.query_id, filterparams.get());
        if (local_params.__isset.runtime_filter_params) {
            runtime_filter_mgr->set_runtime_filter_params(local_params.runtime_filter_params);
        }
        RETURN_IF_ERROR(runtime_filter_mgr->init());
        filterparams->runtime_filter_mgr = runtime_filter_mgr.get();

        _runtime_filter_states.push_back(std::move(filterparams));
        std::map<PipelineId, PipelineXTask*> pipeline_id_to_task;
        auto get_local_exchange_state = [&](PipelinePtr pipeline)
                -> std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>,
                                           std::shared_ptr<LocalExchangeSinkDependency>>> {
            std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>,
                                    std::shared_ptr<LocalExchangeSinkDependency>>>
                    le_state_map;
            auto source_id = pipeline->operator_xs().front()->operator_id();
            if (auto iter = _op_id_to_le_state.find(source_id); iter != _op_id_to_le_state.end()) {
                le_state_map.insert({source_id, iter->second});
            }
            for (auto sink_to_source_id : pipeline->sink_x()->dests_id()) {
                if (auto iter = _op_id_to_le_state.find(sink_to_source_id);
                    iter != _op_id_to_le_state.end()) {
                    le_state_map.insert({sink_to_source_id, iter->second});
                }
            }
            return le_state_map;
        };
        auto get_task_runtime_state = [&](int task_id) -> RuntimeState* {
            DCHECK(_task_runtime_states[task_id]);
            return _task_runtime_states[task_id].get();
        };
        for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
            auto& pipeline = _pipelines[pip_idx];
            if (pipeline->need_to_create_task()) {
                // build task runtime state
                _task_runtime_states.push_back(RuntimeState::create_unique(
                        this, local_params.fragment_instance_id, request.query_id,
                        request.fragment_id, request.query_options, _query_ctx->query_globals,
                        _exec_env));
                auto& task_runtime_state = _task_runtime_states.back();
                init_runtime_state(task_runtime_state);
                auto cur_task_id = _total_tasks++;
                auto task = std::make_unique<PipelineXTask>(
                        pipeline, cur_task_id, get_task_runtime_state(cur_task_id), this,
                        pipeline_id_to_profile[pip_idx].get(), get_local_exchange_state(pipeline),
                        i);
                pipeline_id_to_task.insert({pipeline->id(), task.get()});
                _tasks[i].emplace_back(std::move(task));
            }
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

        // First, set up the parent profile,task runtime state

        auto prepare_and_set_parent_profile = [&](PipelineXTask* task, size_t pip_idx) {
            DCHECK(pipeline_id_to_profile[pip_idx]);
            RETURN_IF_ERROR(task->prepare(local_params, request.fragment.output_sink));
            return Status::OK();
        };

        for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
            if (pipeline_id_to_task.contains(_pipelines[pip_idx]->id())) {
                auto* task = pipeline_id_to_task[_pipelines[pip_idx]->id()];
                DCHECK(task != nullptr);

                // if this task has upstream dependency, then record them.
                if (_dag.find(_pipelines[pip_idx]->id()) != _dag.end()) {
                    auto& deps = _dag[_pipelines[pip_idx]->id()];
                    for (auto& dep : deps) {
                        if (pipeline_id_to_task.contains(dep)) {
                            task->add_upstream_dependency(
                                    pipeline_id_to_task[dep]->get_downstream_dependency());
                        }
                    }
                }
                RETURN_IF_ERROR(prepare_and_set_parent_profile(task, pip_idx));
            }
        }
        {
            std::lock_guard<std::mutex> l(_state_map_lock);
            _runtime_filter_mgr_map[fragment_instance_id] = std::move(runtime_filter_mgr);
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

void PipelineXFragmentContext::_inherit_pipeline_properties(
        const DataDistribution& data_distribution, PipelinePtr pipe_with_source,
        PipelinePtr pipe_with_sink) {
    pipe_with_sink->set_num_tasks(pipe_with_source->num_tasks());
    pipe_with_source->set_num_tasks(_num_instances);
    pipe_with_source->set_data_distribution(data_distribution);
}

Status PipelineXFragmentContext::_add_local_exchange_impl(
        int idx, ObjectPool* pool, PipelinePtr cur_pipe, PipelinePtr new_pip,
        DataDistribution data_distribution, bool* do_local_exchange, int num_buckets,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const bool ignore_data_hash_distribution) {
    auto& operator_xs = cur_pipe->operator_xs();
    const auto downstream_pipeline_id = cur_pipe->id();
    auto local_exchange_id = next_operator_id();
    // 1. Create a new pipeline with local exchange sink.
    DataSinkOperatorXPtr sink;
    auto sink_id = next_sink_operator_id();
    sink.reset(new LocalExchangeSinkOperatorX(sink_id, local_exchange_id, _num_instances,
                                              data_distribution.partition_exprs,
                                              bucket_seq_to_instance_idx));
    RETURN_IF_ERROR(new_pip->set_sink(sink));
    RETURN_IF_ERROR(new_pip->sink_x()->init(data_distribution.distribution_type, num_buckets));

    // 2. Create and initialize LocalExchangeSharedState.
    auto shared_state = LocalExchangeSharedState::create_shared();
    switch (data_distribution.distribution_type) {
    case ExchangeType::HASH_SHUFFLE:
        shared_state->exchanger = ShuffleExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances);
        break;
    case ExchangeType::BUCKET_HASH_SHUFFLE:
        shared_state->exchanger = BucketShuffleExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances, num_buckets,
                ignore_data_hash_distribution);
        break;
    case ExchangeType::PASSTHROUGH:
        shared_state->exchanger =
                PassthroughExchanger::create_unique(cur_pipe->num_tasks(), _num_instances);
        break;
    case ExchangeType::BROADCAST:
        shared_state->exchanger =
                BroadcastExchanger::create_unique(cur_pipe->num_tasks(), _num_instances);
        break;
    case ExchangeType::PASS_TO_ONE:
        shared_state->exchanger =
                BroadcastExchanger::create_unique(cur_pipe->num_tasks(), _num_instances);
        break;
    case ExchangeType::ADAPTIVE_PASSTHROUGH:
        shared_state->exchanger =
                AdaptivePassthroughExchanger::create_unique(cur_pipe->num_tasks(), _num_instances);
        break;
    default:
        return Status::InternalError("Unsupported local exchange type : " +
                                     std::to_string((int)data_distribution.distribution_type));
    }
    shared_state->source_dependencies.resize(_num_instances, nullptr);
    shared_state->mem_trackers.resize(_num_instances, nullptr);
    auto sink_dep = std::make_shared<LocalExchangeSinkDependency>(sink_id, local_exchange_id,
                                                                  _runtime_state->get_query_ctx());
    sink_dep->set_shared_state(shared_state);
    shared_state->sink_dependency = sink_dep.get();
    _op_id_to_le_state.insert({local_exchange_id, {shared_state, sink_dep}});

    // 3. Set two pipelines' operator list. For example, split pipeline [Scan - AggSink] to
    // pipeline1 [Scan - LocalExchangeSink] and pipeline2 [LocalExchangeSource - AggSink].

    // 3.1 Initialize new pipeline's operator list.
    std::copy(operator_xs.begin(), operator_xs.begin() + idx,
              std::inserter(new_pip->operator_xs(), new_pip->operator_xs().end()));

    // 3.2 Erase unused operators in previous pipeline.
    operator_xs.erase(operator_xs.begin(), operator_xs.begin() + idx);

    // 4. Initialize LocalExchangeSource and insert it into this pipeline.
    OperatorXPtr source_op;
    source_op.reset(new LocalExchangeSourceOperatorX(pool, local_exchange_id));
    RETURN_IF_ERROR(source_op->set_child(new_pip->operator_xs().back()));
    RETURN_IF_ERROR(source_op->init(data_distribution.distribution_type));
    if (!operator_xs.empty()) {
        RETURN_IF_ERROR(operator_xs.front()->set_child(source_op));
    }
    operator_xs.insert(operator_xs.begin(), source_op);

    // 5. Set children for two pipelines separately.
    std::vector<std::shared_ptr<Pipeline>> new_children;
    std::vector<PipelineId> edges_with_source;
    for (auto child : cur_pipe->children()) {
        bool found = false;
        for (auto op : new_pip->operator_xs()) {
            if (child->sink_x()->node_id() == op->node_id()) {
                new_pip->set_children(child);
                found = true;
            };
        }
        if (!found) {
            new_children.push_back(child);
            edges_with_source.push_back(child->id());
        }
    }
    new_children.push_back(new_pip);
    edges_with_source.push_back(new_pip->id());

    // 6. Set DAG for new pipelines.
    if (!new_pip->children().empty()) {
        std::vector<PipelineId> edges_with_sink;
        for (auto child : new_pip->children()) {
            edges_with_sink.push_back(child->id());
        }
        _dag.insert({new_pip->id(), edges_with_sink});
    }
    cur_pipe->set_children(new_children);
    _dag[downstream_pipeline_id] = edges_with_source;
    RETURN_IF_ERROR(new_pip->sink_x()->set_child(new_pip->operator_xs().back()));
    RETURN_IF_ERROR(cur_pipe->sink_x()->set_child(cur_pipe->operator_xs().back()));

    // 7. Inherit properties from current pipeline.
    _inherit_pipeline_properties(data_distribution, cur_pipe, new_pip);
    return Status::OK();
}

Status PipelineXFragmentContext::_add_local_exchange(
        int pip_idx, int idx, int node_id, ObjectPool* pool, PipelinePtr cur_pipe,
        DataDistribution data_distribution, bool* do_local_exchange, int num_buckets,
        const std::map<int, int>& bucket_seq_to_instance_idx, const bool ignore_data_distribution) {
    DCHECK(_enable_local_shuffle());
    if (_num_instances <= 1) {
        return Status::OK();
    }

    if (!cur_pipe->need_to_local_exchange(data_distribution)) {
        return Status::OK();
    }
    *do_local_exchange = true;

    auto& operator_xs = cur_pipe->operator_xs();
    auto total_op_num = operator_xs.size();
    auto new_pip = add_pipeline(cur_pipe, pip_idx + 1);
    RETURN_IF_ERROR(_add_local_exchange_impl(idx, pool, cur_pipe, new_pip, data_distribution,
                                             do_local_exchange, num_buckets,
                                             bucket_seq_to_instance_idx, ignore_data_distribution));

    CHECK(total_op_num + 1 == cur_pipe->operator_xs().size() + new_pip->operator_xs().size())
            << "total_op_num: " << total_op_num
            << " cur_pipe->operator_xs().size(): " << cur_pipe->operator_xs().size()
            << " new_pip->operator_xs().size(): " << new_pip->operator_xs().size();

    // Add passthrough local exchanger if necessary
    if (cur_pipe->num_tasks() > 1 && new_pip->num_tasks() == 1 &&
        Pipeline::is_hash_exchange(data_distribution.distribution_type)) {
        RETURN_IF_ERROR(_add_local_exchange_impl(
                new_pip->operator_xs().size(), pool, new_pip, add_pipeline(new_pip, pip_idx + 2),
                DataDistribution(ExchangeType::PASSTHROUGH), do_local_exchange, num_buckets,
                bucket_seq_to_instance_idx, ignore_data_distribution));
    }
    return Status::OK();
}

Status PipelineXFragmentContext::_create_operator(ObjectPool* pool, const TPlanNode& tnode,
                                                  const doris::TPipelineFragmentParams& request,
                                                  const DescriptorTbl& descs, OperatorXPtr& op,
                                                  PipelinePtr& cur_pipe, int parent_idx,
                                                  int child_idx) {
    // We directly construct the operator from Thrift because the given array is in the order of preorder traversal.
    // Therefore, here we need to use a stack-like structure.
    _pipeline_parent_map.pop(cur_pipe, parent_idx, child_idx);
    std::stringstream error_msg;
    switch (tnode.node_type) {
    case TPlanNodeType::OLAP_SCAN_NODE: {
        op.reset(new OlapScanOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        if (find_with_default(request.per_node_shared_scans, op->node_id(), false)) {
            if (request.__isset.parallel_instances) {
                cur_pipe->set_num_tasks(request.parallel_instances);
            }
            op->set_ignore_data_distribution();
        }
        break;
    }
    case doris::TPlanNodeType::JDBC_SCAN_NODE: {
        if (config::enable_java_support) {
            op.reset(new JDBCScanOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
            RETURN_IF_ERROR(cur_pipe->add_operator(op));
        } else {
            return Status::InternalError(
                    "Jdbc scan node is disabled, you can change be config enable_java_support "
                    "to true and restart be.");
        }
        if (find_with_default(request.per_node_shared_scans, op->node_id(), false)) {
            if (request.__isset.parallel_instances) {
                cur_pipe->set_num_tasks(request.parallel_instances);
            }
            op->set_ignore_data_distribution();
        }
        break;
    }
    case doris::TPlanNodeType::FILE_SCAN_NODE: {
        op.reset(new FileScanOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        if (find_with_default(request.per_node_shared_scans, op->node_id(), false)) {
            if (request.__isset.parallel_instances) {
                cur_pipe->set_num_tasks(request.parallel_instances);
            }
            op->set_ignore_data_distribution();
        }
        break;
    }
    case TPlanNodeType::ES_SCAN_NODE:
    case TPlanNodeType::ES_HTTP_SCAN_NODE: {
        op.reset(new EsScanOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        if (find_with_default(request.per_node_shared_scans, op->node_id(), false)) {
            if (request.__isset.parallel_instances) {
                cur_pipe->set_num_tasks(request.parallel_instances);
            }
            op->set_ignore_data_distribution();
        }
        break;
    }
    case TPlanNodeType::EXCHANGE_NODE: {
        int num_senders = find_with_default(request.per_exch_num_senders, tnode.node_id, 0);
        DCHECK_GT(num_senders, 0);
        op.reset(new ExchangeSourceOperatorX(pool, tnode, next_operator_id(), descs, num_senders));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        if (request.__isset.parallel_instances) {
            op->set_ignore_data_distribution();
            cur_pipe->set_num_tasks(request.parallel_instances);
        }
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
            cur_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(cur_pipe->id());
            DataSinkOperatorXPtr sink;
            sink.reset(new DistinctStreamingAggSinkOperatorX(pool, next_sink_operator_id(), tnode,
                                                             descs));
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
            cur_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(cur_pipe->id());
            DataSinkOperatorXPtr sink;
            sink.reset(new StreamingAggSinkOperatorX(pool, next_sink_operator_id(), tnode, descs));
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
            cur_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(cur_pipe->id());

            DataSinkOperatorXPtr sink;
            sink.reset(new AggSinkOperatorX<>(pool, next_sink_operator_id(), tnode, descs));
            sink->set_dests_id({op->operator_id()});
            RETURN_IF_ERROR(cur_pipe->set_sink(sink));
            RETURN_IF_ERROR(cur_pipe->sink_x()->init(tnode, _runtime_state.get()));
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
        PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

        DataSinkOperatorXPtr sink;
        sink.reset(new HashJoinBuildSinkOperatorX(pool, next_sink_operator_id(), tnode, descs,
                                                  _use_global_rf));
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
        PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

        DataSinkOperatorXPtr sink;
        sink.reset(
                new NestedLoopJoinBuildSinkOperatorX(pool, next_sink_operator_id(), tnode, descs));
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
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());
            DataSinkOperatorXPtr sink;
            sink.reset(new UnionSinkOperatorX(i, next_sink_operator_id(), pool, tnode, descs));
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
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorXPtr sink;
        sink.reset(new SortSinkOperatorX(pool, next_sink_operator_id(), tnode, descs));
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
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorXPtr sink;
        sink.reset(new PartitionSortSinkOperatorX(pool, next_sink_operator_id(), tnode, descs));
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
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorXPtr sink;
        sink.reset(new AnalyticSinkOperatorX(pool, next_sink_operator_id(), tnode, descs));
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
        PipelinePtr probe_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(probe_side_pipe->id());

        DataSinkOperatorXPtr sink;
        if (child_id == 0) {
            sink.reset(new SetSinkOperatorX<is_intersect>(child_id, next_sink_operator_id(), pool,
                                                          tnode, descs));
        } else {
            sink.reset(new SetProbeSinkOperatorX<is_intersect>(child_id, next_sink_operator_id(),
                                                               pool, tnode, descs));
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
            _close_fragment_instance();
        }
        return Status::InternalError("Submit pipeline failed. err = {}, BE: {}", st.to_string(),
                                     BackendOptions::get_localhost());
    } else {
        return st;
    }
}

void PipelineXFragmentContext::close_sink() {
    for (auto& tasks : _tasks) {
        auto& root_task = *tasks.begin();
        auto st = root_task->close_sink(_prepared ? Status::RuntimeError("prepare failed")
                                                  : Status::OK());
        if (!st.ok()) {
            LOG_WARNING("PipelineXFragmentContext::close_sink() error").tag("msg", st.msg());
        }
    }
}

void PipelineXFragmentContext::close_if_prepare_failed() {
    for (auto& task : _tasks) {
        for (auto& t : task) {
            DCHECK(!t->is_pending_finish());
            WARN_IF_ERROR(t->close(Status::OK()), "close_if_prepare_failed failed: ");
            close_a_pipeline();
        }
    }
}

void PipelineXFragmentContext::_close_fragment_instance() {
    if (_is_fragment_instance_closed) {
        return;
    }
    Defer defer_op {[&]() { _is_fragment_instance_closed = true; }};
    _runtime_profile->total_time_counter()->update(_fragment_watcher.elapsed_time());
    static_cast<void>(send_report(true));
    // all submitted tasks done
    _exec_env->fragment_mgr()->remove_pipeline_context(
            std::dynamic_pointer_cast<PipelineXFragmentContext>(shared_from_this()));
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

    std::vector<RuntimeState*> runtime_states;

    for (auto& task_state : _task_runtime_states) {
        runtime_states.push_back(task_state.get());
    }
    return _report_status_cb(
            {true, exec_status, runtime_states, nullptr, _runtime_state->load_channel_profile(),
             done || !exec_status.ok(), _query_ctx->coord_addr, _query_id, _fragment_id,
             TUniqueId(), _backend_num, _runtime_state.get(),
             std::bind(&PipelineFragmentContext::update_status, this, std::placeholders::_1),
             std::bind(&PipelineFragmentContext::cancel, this, std::placeholders::_1,
                       std::placeholders::_2),
             nullptr},
            std::dynamic_pointer_cast<PipelineXFragmentContext>(shared_from_this()));
}

bool PipelineXFragmentContext::_has_inverted_index_or_partial_update(TOlapTableSink sink) {
    OlapTableSchemaParam schema;
    if (!schema.init(sink.schema).ok()) {
        return false;
    }
    if (schema.is_partial_update()) {
        return true;
    }
    for (const auto& index_schema : schema.indexes()) {
        for (const auto& index : index_schema->indexes) {
            if (index->index_type() == INVERTED) {
                return true;
            }
        }
    }
    return false;
}

std::string PipelineXFragmentContext::debug_string() {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "PipelineXFragmentContext Info:\n");
    for (size_t j = 0; j < _tasks.size(); j++) {
        fmt::format_to(debug_string_buffer, "Tasks in instance {}:\n", j);
        for (size_t i = 0; i < _tasks[j].size(); i++) {
            fmt::format_to(debug_string_buffer, "Task {}: {}\n", i, _tasks[j][i]->debug_string());
        }
    }

    return fmt::to_string(debug_string_buffer);
}
} // namespace doris::pipeline
