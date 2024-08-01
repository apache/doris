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

#include "pipeline_fragment_context.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <pthread.h>

#include <cstdlib>
// IWYU pragma: no_include <bits/chrono.h>
#include <fmt/format.h>

#include <chrono> // IWYU pragma: keep
#include <map>
#include <memory>
#include <ostream>
#include <utility>

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/stream_load_pipe.h"
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
#include "pipeline/local_exchange/local_exchanger.h"
#include "pipeline/task_scheduler.h"
#include "pipeline_task.h"
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
#include "vec/common/sort/heap_sorter.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/runtime/vdata_stream_mgr.h"

namespace doris::pipeline {
bvar::Adder<int64_t> g_pipeline_tasks_count("doris_pipeline_tasks_count");

PipelineFragmentContext::PipelineFragmentContext(
        const TUniqueId& query_id, const int fragment_id, std::shared_ptr<QueryContext> query_ctx,
        ExecEnv* exec_env, const std::function<void(RuntimeState*, Status*)>& call_back,
        const report_status_callback& report_status_cb)
        : _query_id(query_id),
          _fragment_id(fragment_id),
          _exec_env(exec_env),
          _query_ctx(std::move(query_ctx)),
          _call_back(call_back),
          _is_report_on_cancel(true),
          _report_status_cb(report_status_cb) {
    _fragment_watcher.start();
}

PipelineFragmentContext::~PipelineFragmentContext() {
    // The memory released by the query end is recorded in the query mem tracker.
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_ctx->query_mem_tracker);
    auto st = _query_ctx->exec_status();
    _query_ctx.reset();
    for (size_t i = 0; i < _tasks.size(); i++) {
        if (!_tasks[i].empty()) {
            _call_back(_tasks[i].front()->runtime_state(), &st);
        }
    }
    _tasks.clear();
    for (auto& runtime_state : _task_runtime_states) {
        runtime_state.reset();
    }
    _pipelines.clear();
    _sink.reset();
    _root_op.reset();
    _runtime_state.reset();
    _runtime_filter_states.clear();
    _runtime_filter_mgr_map.clear();
    _op_id_to_le_state.clear();
}

bool PipelineFragmentContext::is_timeout(timespec now) const {
    if (_timeout <= 0) {
        return false;
    }
    return _fragment_watcher.elapsed_time_seconds(now) > _timeout;
}

// Must not add lock in this method. Because it will call query ctx cancel. And
// QueryCtx cancel will call fragment ctx cancel. And Also Fragment ctx's running
// Method like exchange sink buffer will call query ctx cancel. If we add lock here
// There maybe dead lock.
void PipelineFragmentContext::cancel(const Status reason) {
    LOG_INFO("PipelineFragmentContext::cancel")
            .tag("query_id", print_id(_query_id))
            .tag("fragment_id", _fragment_id)
            .tag("reason", reason.to_string());
    {
        std::lock_guard<std::mutex> l(_task_mutex);
        if (_closed_tasks == _total_tasks) {
            // All tasks in this PipelineXFragmentContext already closed.
            return;
        }
    }
    // Timeout is a special error code, we need print current stack to debug timeout issue.
    if (reason.is<ErrorCode::TIMEOUT>()) {
        LOG(WARNING) << "PipelineFragmentContext is cancelled due to timeout : " << debug_string();
    }
    _query_ctx->cancel(reason, _fragment_id);
    if (reason.is<ErrorCode::LIMIT_REACH>()) {
        _is_report_on_cancel = false;
    } else {
        for (auto& id : _fragment_instance_ids) {
            LOG(WARNING) << "PipelineFragmentContext cancel instance: " << print_id(id);
        }
    }
    // Get pipe from new load stream manager and send cancel to it or the fragment may hang to wait read from pipe
    // For stream load the fragment's query_id == load id, it is set in FE.
    auto stream_load_ctx = _exec_env->new_load_stream_mgr()->get(_query_id);
    if (stream_load_ctx != nullptr) {
        stream_load_ctx->pipe->cancel(reason.to_string());
    }

    for (auto& tasks : _tasks) {
        for (auto& task : tasks) {
            task->clear_blocking_state();
        }
    }
}

PipelinePtr PipelineFragmentContext::add_pipeline() {
    // _prepared、_submitted, _canceled should do not add pipeline
    PipelineId id = _next_pipeline_id++;
    auto pipeline = std::make_shared<Pipeline>(
            id, _num_instances,
            std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()));
    _pipelines.emplace_back(pipeline);
    return pipeline;
}

PipelinePtr PipelineFragmentContext::add_pipeline(PipelinePtr parent, int idx) {
    // _prepared、_submitted, _canceled should do not add pipeline
    PipelineId id = _next_pipeline_id++;
    auto pipeline = std::make_shared<Pipeline>(
            id, _num_instances,
            std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()));
    if (idx >= 0) {
        _pipelines.insert(_pipelines.begin() + idx, pipeline);
    } else {
        _pipelines.emplace_back(pipeline);
    }
    parent->set_children(pipeline);
    return pipeline;
}

Status PipelineFragmentContext::prepare(const doris::TPipelineFragmentParams& request) {
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    if (request.__isset.query_options && request.query_options.__isset.execution_timeout) {
        _timeout = request.query_options.execution_timeout;
    }

    _runtime_profile = std::make_unique<RuntimeProfile>("PipelineContext");
    _prepare_timer = ADD_TIMER(_runtime_profile, "PrepareTime");
    SCOPED_TIMER(_prepare_timer);
    _build_pipelines_timer = ADD_TIMER(_runtime_profile, "BuildPipelinesTime");
    _init_context_timer = ADD_TIMER(_runtime_profile, "InitContextTime");
    _plan_local_shuffle_timer = ADD_TIMER(_runtime_profile, "PlanLocalShuffleTime");
    _build_tasks_timer = ADD_TIMER(_runtime_profile, "BuildTasksTime");
    _prepare_all_pipelines_timer = ADD_TIMER(_runtime_profile, "PrepareAllPipelinesTime");
    {
        SCOPED_TIMER(_init_context_timer);
        _num_instances = request.local_params.size();
        _total_instances =
                request.__isset.total_instances ? request.total_instances : _num_instances;

        auto* fragment_context = this;

        LOG_INFO("PipelineFragmentContext::prepare")
                .tag("query_id", print_id(_query_id))
                .tag("fragment_id", _fragment_id)
                .tag("pthread_id", (uintptr_t)pthread_self());

        if (request.query_options.__isset.is_report_success) {
            fragment_context->set_is_report_success(request.query_options.is_report_success);
        }

        // 1. Set up the global runtime state.
        _runtime_state = RuntimeState::create_unique(
                request.query_id, request.fragment_id, request.query_options,
                _query_ctx->query_globals, _exec_env, _query_ctx.get());

        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_runtime_state->query_mem_tracker());
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
            RETURN_IF_ERROR(DescriptorTbl::create(_runtime_state->obj_pool(), request.desc_tbl,
                                                  &_desc_tbl));
        }
        _runtime_state->set_desc_tbl(_desc_tbl);
        _runtime_state->set_num_per_fragment_instances(request.num_senders);
        _runtime_state->set_load_stream_per_node(request.load_stream_per_node);
        _runtime_state->set_total_load_streams(request.total_load_streams);
        _runtime_state->set_num_local_sink(request.num_local_sink);

        const auto& local_params = request.local_params[0];
        if (local_params.__isset.runtime_filter_params) {
            _query_ctx->runtime_filter_mgr()->set_runtime_filter_params(
                    local_params.runtime_filter_params);
        }
        if (local_params.__isset.topn_filter_descs) {
            _query_ctx->init_runtime_predicates(local_params.topn_filter_descs);
        }

        _need_local_merge = request.__isset.parallel_instances;
    }

    {
        SCOPED_TIMER(_build_pipelines_timer);
        // 2. Build pipelines with operators in this fragment.
        auto root_pipeline = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(_runtime_state->obj_pool(), request, *_query_ctx->desc_tbl,
                                         &_root_op, root_pipeline));

        // 3. Create sink operator
        if (!request.fragment.__isset.output_sink) {
            return Status::InternalError("No output sink in this fragment!");
        }
        RETURN_IF_ERROR(_create_data_sink(_runtime_state->obj_pool(), request.fragment.output_sink,
                                          request.fragment.output_exprs, request,
                                          root_pipeline->output_row_desc(), _runtime_state.get(),
                                          *_desc_tbl, root_pipeline->id()));
        RETURN_IF_ERROR(_sink->init(request.fragment.output_sink));
        RETURN_IF_ERROR(root_pipeline->set_sink(_sink));

        for (PipelinePtr& pipeline : _pipelines) {
            DCHECK(pipeline->sink_x() != nullptr) << pipeline->operator_xs().size();
            RETURN_IF_ERROR(pipeline->sink_x()->set_child(pipeline->operator_xs().back()));
        }
    }
    if (_enable_local_shuffle()) {
        SCOPED_TIMER(_plan_local_shuffle_timer);
        RETURN_IF_ERROR(_plan_local_exchange(request.num_buckets,
                                             request.bucket_seq_to_instance_idx,
                                             request.shuffle_idx_to_instance_idx));
    }

    // 4. Initialize global states in pipelines.
    for (PipelinePtr& pipeline : _pipelines) {
        SCOPED_TIMER(_prepare_all_pipelines_timer);
        pipeline->children().clear();
        RETURN_IF_ERROR(pipeline->prepare(_runtime_state.get()));
    }

    {
        SCOPED_TIMER(_build_tasks_timer);
        // 5. Build pipeline tasks and initialize local state.
        RETURN_IF_ERROR(_build_pipeline_tasks(request));
    }

    _init_next_report_time();

    _prepared = true;
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipeline_tasks(
        const doris::TPipelineFragmentParams& request) {
    _total_tasks = 0;
    int target_size = request.local_params.size();
    _tasks.resize(target_size);
    auto pipeline_id_to_profile = _runtime_state->build_pipeline_profile(_pipelines.size());

    for (size_t i = 0; i < target_size; i++) {
        const auto& local_params = request.local_params[i];
        auto fragment_instance_id = local_params.fragment_instance_id;
        _fragment_instance_ids.push_back(fragment_instance_id);
        std::unique_ptr<RuntimeFilterMgr> runtime_filter_mgr;
        auto init_runtime_state = [&](std::unique_ptr<RuntimeState>& runtime_state) {
            runtime_state->set_query_mem_tracker(_query_ctx->query_mem_tracker);

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
            if (request.__isset.wal_id) {
                runtime_state->set_wal_id(request.wal_id);
            }

            runtime_state->set_desc_tbl(_desc_tbl);
            runtime_state->set_per_fragment_instance_idx(local_params.sender_id);
            runtime_state->set_num_per_fragment_instances(request.num_senders);
            runtime_state->resize_op_id_to_local_state(max_operator_id());
            runtime_state->set_max_operator_id(max_operator_id());
            runtime_state->set_load_stream_per_node(request.load_stream_per_node);
            runtime_state->set_total_load_streams(request.total_load_streams);
            runtime_state->set_num_local_sink(request.num_local_sink);
            DCHECK(runtime_filter_mgr);
            runtime_state->set_runtime_filter_mgr(runtime_filter_mgr.get());
        };

        auto filterparams = std::make_unique<RuntimeFilterParamsContext>();

        {
            filterparams->runtime_filter_wait_infinitely =
                    _runtime_state->runtime_filter_wait_infinitely();
            filterparams->runtime_filter_wait_time_ms =
                    _runtime_state->runtime_filter_wait_time_ms();
            filterparams->execution_timeout = _runtime_state->execution_timeout();

            filterparams->exec_env = ExecEnv::GetInstance();
            filterparams->query_id.set_hi(_runtime_state->query_id().hi);
            filterparams->query_id.set_lo(_runtime_state->query_id().lo);

            filterparams->be_exec_version = _runtime_state->be_exec_version();
            filterparams->query_ctx = _query_ctx.get();
        }

        // build local_runtime_filter_mgr for each instance
        runtime_filter_mgr = std::make_unique<RuntimeFilterMgr>(
                request.query_id, filterparams.get(), _query_ctx->query_mem_tracker);

        filterparams->runtime_filter_mgr = runtime_filter_mgr.get();

        _runtime_filter_states.push_back(std::move(filterparams));
        std::map<PipelineId, PipelineTask*> pipeline_id_to_task;
        auto get_local_exchange_state = [&](PipelinePtr pipeline)
                -> std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>,
                                           std::shared_ptr<Dependency>>> {
            std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>,
                                    std::shared_ptr<Dependency>>>
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

        for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
            auto& pipeline = _pipelines[pip_idx];
            if (pipeline->need_to_create_task()) {
                // build task runtime state
                _task_runtime_states.push_back(RuntimeState::create_unique(
                        this, local_params.fragment_instance_id, request.query_id,
                        request.fragment_id, request.query_options, _query_ctx->query_globals,
                        _exec_env, _query_ctx.get()));
                auto& task_runtime_state = _task_runtime_states.back();
                init_runtime_state(task_runtime_state);
                auto cur_task_id = _total_tasks++;
                task_runtime_state->set_task_id(cur_task_id);
                task_runtime_state->set_task_num(pipeline->num_tasks());
                auto task = std::make_unique<PipelineTask>(pipeline, cur_task_id,
                                                           task_runtime_state.get(), this,
                                                           pipeline_id_to_profile[pip_idx].get(),
                                                           get_local_exchange_state(pipeline), i);
                task_runtime_state->set_task(task.get());
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

        auto prepare_and_set_parent_profile = [&](PipelineTask* task, size_t pip_idx) {
            DCHECK(pipeline_id_to_profile[pip_idx]);
            RETURN_IF_ERROR(
                    task->prepare(local_params, request.fragment.output_sink, _query_ctx.get()));
            return Status::OK();
        };

        for (auto& _pipeline : _pipelines) {
            if (pipeline_id_to_task.contains(_pipeline->id())) {
                auto* task = pipeline_id_to_task[_pipeline->id()];
                DCHECK(task != nullptr);

                // if this task has upstream dependency, then record them.
                if (_dag.find(_pipeline->id()) != _dag.end()) {
                    auto& deps = _dag[_pipeline->id()];
                    for (auto& dep : deps) {
                        if (pipeline_id_to_task.contains(dep)) {
                            auto ss = pipeline_id_to_task[dep]->get_sink_shared_state();
                            if (ss) {
                                task->inject_shared_state(ss);
                            } else {
                                pipeline_id_to_task[dep]->inject_shared_state(
                                        task->get_source_shared_state());
                            }
                        }
                    }
                }
            }
        }
        for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
            if (pipeline_id_to_task.contains(_pipelines[pip_idx]->id())) {
                auto* task = pipeline_id_to_task[_pipelines[pip_idx]->id()];
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

void PipelineFragmentContext::_init_next_report_time() {
    auto interval_s = config::pipeline_status_report_interval;
    if (_is_report_success && interval_s > 0 && _timeout > interval_s) {
        std::vector<string> ins_ids;
        instance_ids(ins_ids);
        VLOG_FILE << "enable period report: instance_id="
                  << fmt::format("{}", fmt::join(ins_ids, ", "));
        uint64_t report_fragment_offset = (uint64_t)(rand() % interval_s) * NANOS_PER_SEC;
        // We don't want to wait longer than it takes to run the entire fragment.
        _previous_report_time =
                MonotonicNanos() + report_fragment_offset - (uint64_t)(interval_s)*NANOS_PER_SEC;
        _disable_period_report = false;
    }
}

void PipelineFragmentContext::refresh_next_report_time() {
    auto disable = _disable_period_report.load(std::memory_order_acquire);
    DCHECK(disable == true);
    _previous_report_time.store(MonotonicNanos(), std::memory_order_release);
    _disable_period_report.compare_exchange_strong(disable, false);
}

void PipelineFragmentContext::trigger_report_if_necessary() {
    if (!_is_report_success) {
        return;
    }
    auto disable = _disable_period_report.load(std::memory_order_acquire);
    if (disable) {
        return;
    }
    int32_t interval_s = config::pipeline_status_report_interval;
    if (interval_s <= 0) {
        LOG(WARNING)
                << "config::status_report_interval is equal to or less than zero, do not trigger "
                   "report.";
    }
    uint64_t next_report_time = _previous_report_time.load(std::memory_order_acquire) +
                                (uint64_t)(interval_s)*NANOS_PER_SEC;
    if (MonotonicNanos() > next_report_time) {
        if (!_disable_period_report.compare_exchange_strong(disable, true,
                                                            std::memory_order_acq_rel)) {
            return;
        }
        if (VLOG_FILE_IS_ON) {
            std::vector<string> ins_ids;
            instance_ids(ins_ids);
            VLOG_FILE << "Reporting "
                      << "profile for query_id " << print_id(_query_id)
                      << ", instance ids: " << fmt::format("{}", fmt::join(ins_ids, ", "));

            std::stringstream ss;
            _runtime_state->runtime_profile()->compute_time_in_profile();
            _runtime_state->runtime_profile()->pretty_print(&ss);
            if (_runtime_state->load_channel_profile()) {
                _runtime_state->load_channel_profile()->pretty_print(&ss);
            }

            VLOG_FILE << "Query " << print_id(this->get_query_id()) << " fragment "
                      << this->get_fragment_id() << " profile:\n"
                      << ss.str();
        }
        auto st = send_report(false);
        if (!st.ok()) {
            disable = true;
            _disable_period_report.compare_exchange_strong(disable, false,
                                                           std::memory_order_acq_rel);
        }
    }
}

Status PipelineFragmentContext::_build_pipelines(ObjectPool* pool,
                                                 const doris::TPipelineFragmentParams& request,
                                                 const DescriptorTbl& descs, OperatorXPtr* root,
                                                 PipelinePtr cur_pipe) {
    if (request.fragment.plan.nodes.empty()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid plan which has no plan node!");
    }

    int node_idx = 0;

    RETURN_IF_ERROR(_create_tree_helper(pool, request.fragment.plan.nodes, request, descs, nullptr,
                                        &node_idx, root, cur_pipe, 0));

    if (node_idx + 1 != request.fragment.plan.nodes.size()) {
        return Status::InternalError(
                "Plan tree only partially reconstructed. Not all thrift nodes were used.");
    }
    return Status::OK();
}

Status PipelineFragmentContext::_create_tree_helper(ObjectPool* pool,
                                                    const std::vector<TPlanNode>& tnodes,
                                                    const doris::TPipelineFragmentParams& request,
                                                    const DescriptorTbl& descs, OperatorXPtr parent,
                                                    int* node_idx, OperatorXPtr* root,
                                                    PipelinePtr& cur_pipe, int child_idx) {
    // propagate error case
    if (*node_idx >= tnodes.size()) {
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
            return Status::InternalError(
                    "Failed to reconstruct plan tree from thrift. Node id: {}, number of nodes: {}",
                    *node_idx, tnodes.size());
        }
    }

    RETURN_IF_ERROR(op->init(tnode, _runtime_state.get()));

    return Status::OK();
}

void PipelineFragmentContext::_inherit_pipeline_properties(
        const DataDistribution& data_distribution, PipelinePtr pipe_with_source,
        PipelinePtr pipe_with_sink) {
    pipe_with_sink->set_num_tasks(pipe_with_source->num_tasks());
    pipe_with_source->set_num_tasks(_num_instances);
    pipe_with_source->set_data_distribution(data_distribution);
}

Status PipelineFragmentContext::_add_local_exchange_impl(
        int idx, ObjectPool* pool, PipelinePtr cur_pipe, PipelinePtr new_pip,
        DataDistribution data_distribution, bool* do_local_exchange, int num_buckets,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx,
        const bool ignore_data_hash_distribution) {
    auto& operator_xs = cur_pipe->operator_xs();
    const auto downstream_pipeline_id = cur_pipe->id();
    auto local_exchange_id = next_operator_id();
    // 1. Create a new pipeline with local exchange sink.
    DataSinkOperatorXPtr sink;
    auto sink_id = next_sink_operator_id();
    const bool is_shuffled_hash_join = operator_xs.size() > idx
                                               ? operator_xs[idx]->is_shuffled_hash_join()
                                               : cur_pipe->sink_x()->is_shuffled_hash_join();
    sink.reset(new LocalExchangeSinkOperatorX(
            sink_id, local_exchange_id, is_shuffled_hash_join ? _total_instances : _num_instances,
            data_distribution.partition_exprs, bucket_seq_to_instance_idx));
    RETURN_IF_ERROR(new_pip->set_sink(sink));
    RETURN_IF_ERROR(new_pip->sink_x()->init(data_distribution.distribution_type, num_buckets,
                                            is_shuffled_hash_join, shuffle_idx_to_instance_idx));

    // 2. Create and initialize LocalExchangeSharedState.
    auto shared_state = LocalExchangeSharedState::create_shared(_num_instances);
    switch (data_distribution.distribution_type) {
    case ExchangeType::HASH_SHUFFLE:
        shared_state->exchanger = ShuffleExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances),
                is_shuffled_hash_join ? _total_instances : _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? _runtime_state->query_options().local_exchange_free_blocks_limit
                        : 0);
        break;
    case ExchangeType::BUCKET_HASH_SHUFFLE:
        shared_state->exchanger = BucketShuffleExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances, num_buckets,
                ignore_data_hash_distribution,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? _runtime_state->query_options().local_exchange_free_blocks_limit
                        : 0);
        break;
    case ExchangeType::PASSTHROUGH:
        shared_state->exchanger = PassthroughExchanger::create_unique(
                cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? _runtime_state->query_options().local_exchange_free_blocks_limit
                        : 0);
        break;
    case ExchangeType::BROADCAST:
        shared_state->exchanger = BroadcastExchanger::create_unique(
                cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? _runtime_state->query_options().local_exchange_free_blocks_limit
                        : 0);
        break;
    case ExchangeType::PASS_TO_ONE:
        shared_state->exchanger = BroadcastExchanger::create_unique(
                cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? _runtime_state->query_options().local_exchange_free_blocks_limit
                        : 0);
        break;
    case ExchangeType::LOCAL_MERGE_SORT: {
        auto child_op = cur_pipe->sink_x()->child_x();
        auto sort_source = std::dynamic_pointer_cast<SortSourceOperatorX>(child_op);
        if (!sort_source) {
            return Status::InternalError(
                    "LOCAL_MERGE_SORT must use in SortSourceOperatorX , but now is {} ",
                    child_op->get_name());
        }
        shared_state->exchanger = LocalMergeSortExchanger::create_unique(
                sort_source, cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? _runtime_state->query_options().local_exchange_free_blocks_limit
                        : 0);
        break;
    }
    case ExchangeType::ADAPTIVE_PASSTHROUGH:
        shared_state->exchanger = AdaptivePassthroughExchanger::create_unique(
                cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? _runtime_state->query_options().local_exchange_free_blocks_limit
                        : 0);
        break;
    default:
        return Status::InternalError("Unsupported local exchange type : " +
                                     std::to_string((int)data_distribution.distribution_type));
    }
    auto sink_dep = std::make_shared<Dependency>(sink_id, local_exchange_id,
                                                 "LOCAL_EXCHANGE_SINK_DEPENDENCY", true);
    sink_dep->set_shared_state(shared_state.get());
    shared_state->sink_deps.push_back(sink_dep);
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

    shared_state->create_source_dependencies(source_op->operator_id(), source_op->node_id());

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

Status PipelineFragmentContext::_add_local_exchange(
        int pip_idx, int idx, int node_id, ObjectPool* pool, PipelinePtr cur_pipe,
        DataDistribution data_distribution, bool* do_local_exchange, int num_buckets,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx,
        const bool ignore_data_distribution) {
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
    RETURN_IF_ERROR(_add_local_exchange_impl(
            idx, pool, cur_pipe, new_pip, data_distribution, do_local_exchange, num_buckets,
            bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx, ignore_data_distribution));

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
                bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx, ignore_data_distribution));
    }
    return Status::OK();
}

Status PipelineFragmentContext::_plan_local_exchange(
        int num_buckets, const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
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

        // if 'num_buckets == 0' means the fragment is colocated by exchange node not the
        // scan node. so here use `_num_instance` to replace the `num_buckets` to prevent dividing 0
        // still keep colocate plan after local shuffle
        RETURN_IF_ERROR(_plan_local_exchange(
                _pipelines[pip_idx]->operator_xs().front()->ignore_data_hash_distribution() ||
                                num_buckets == 0
                        ? _num_instances
                        : num_buckets,
                pip_idx, _pipelines[pip_idx], bucket_seq_to_instance_idx,
                shuffle_idx_to_instance_idx,
                _pipelines[pip_idx]->operator_xs().front()->ignore_data_hash_distribution()));
    }
    return Status::OK();
}

Status PipelineFragmentContext::_plan_local_exchange(
        int num_buckets, int pip_idx, PipelinePtr pip,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx,
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
                        bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx,
                        ignore_data_hash_distribution));
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
                bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx,
                ignore_data_hash_distribution));
    }
    return Status::OK();
}

Status PipelineFragmentContext::_create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
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
        _sink.reset(new ExchangeSinkOperatorX(state, row_desc, next_sink_operator_id(),
                                              thrift_sink.stream_sink, params.destinations));
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        if (!thrift_sink.__isset.result_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        _sink.reset(new ResultSinkOperatorX(next_sink_operator_id(), row_desc, output_exprs,
                                            thrift_sink.result_sink));
        break;
    }
    case TDataSinkType::GROUP_COMMIT_OLAP_TABLE_SINK:
    case TDataSinkType::OLAP_TABLE_SINK: {
        if (state->query_options().enable_memtable_on_sink_node &&
            !_has_inverted_index_v1_or_partial_update(thrift_sink.olap_table_sink) &&
            !config::is_cloud_mode()) {
            _sink.reset(new OlapTableSinkV2OperatorX(pool, next_sink_operator_id(), row_desc,
                                                     output_exprs));
        } else {
            _sink.reset(new OlapTableSinkOperatorX(pool, next_sink_operator_id(), row_desc,
                                                   output_exprs));
        }
        break;
    }
    case TDataSinkType::GROUP_COMMIT_BLOCK_SINK: {
        DCHECK(thrift_sink.__isset.olap_table_sink);
        _sink.reset(
                new GroupCommitBlockSinkOperatorX(next_sink_operator_id(), row_desc, output_exprs));
        break;
    }
    case TDataSinkType::HIVE_TABLE_SINK: {
        if (!thrift_sink.__isset.hive_table_sink) {
            return Status::InternalError("Missing hive table sink.");
        }
        _sink.reset(
                new HiveTableSinkOperatorX(pool, next_sink_operator_id(), row_desc, output_exprs));
        break;
    }
    case TDataSinkType::ICEBERG_TABLE_SINK: {
        if (!thrift_sink.__isset.iceberg_table_sink) {
            return Status::InternalError("Missing hive table sink.");
        }
        _sink.reset(new IcebergTableSinkOperatorX(pool, next_sink_operator_id(), row_desc,
                                                  output_exprs));
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
    case TDataSinkType::MEMORY_SCRATCH_SINK: {
        if (!thrift_sink.__isset.memory_scratch_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        _sink.reset(
                new MemoryScratchSinkOperatorX(row_desc, next_sink_operator_id(), output_exprs));
        break;
    }
    case TDataSinkType::RESULT_FILE_SINK: {
        if (!thrift_sink.__isset.result_file_sink) {
            return Status::InternalError("Missing result file sink.");
        }

        // Result file sink is not the top sink
        if (params.__isset.destinations && !params.destinations.empty()) {
            _sink.reset(new ResultFileSinkOperatorX(next_sink_operator_id(), row_desc,
                                                    thrift_sink.result_file_sink,
                                                    params.destinations, output_exprs, desc_tbl));
        } else {
            _sink.reset(
                    new ResultFileSinkOperatorX(next_sink_operator_id(), row_desc, output_exprs));
        }
        break;
    }
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK: {
        DCHECK(thrift_sink.__isset.multi_cast_stream_sink);
        DCHECK_GT(thrift_sink.multi_cast_stream_sink.sinks.size(), 0);
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
                const auto& tmp_row_desc =
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
            RETURN_IF_ERROR(new_pipeline->add_operator(source_op));
            // 2. create and set sink operator of data stream sender for new pipeline

            DataSinkOperatorXPtr sink_op;
            sink_op.reset(
                    new ExchangeSinkOperatorX(state, *_row_desc, next_sink_operator_id(),
                                              thrift_sink.multi_cast_stream_sink.sinks[i],
                                              thrift_sink.multi_cast_stream_sink.destinations[i]));

            RETURN_IF_ERROR(new_pipeline->set_sink(sink_op));
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

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
Status PipelineFragmentContext::_create_operator(ObjectPool* pool, const TPlanNode& tnode,
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
        if (request.__isset.parallel_instances) {
            cur_pipe->set_num_tasks(request.parallel_instances);
            op->set_ignore_data_distribution();
        }
        break;
    }
    case TPlanNodeType::GROUP_COMMIT_SCAN_NODE: {
        op.reset(new GroupCommitOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        if (request.__isset.parallel_instances) {
            cur_pipe->set_num_tasks(request.parallel_instances);
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
        if (request.__isset.parallel_instances) {
            cur_pipe->set_num_tasks(request.parallel_instances);
            op->set_ignore_data_distribution();
        }
        break;
    }
    case doris::TPlanNodeType::FILE_SCAN_NODE: {
        op.reset(new FileScanOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        if (request.__isset.parallel_instances) {
            cur_pipe->set_num_tasks(request.parallel_instances);
            op->set_ignore_data_distribution();
        }
        break;
    }
    case TPlanNodeType::ES_SCAN_NODE:
    case TPlanNodeType::ES_HTTP_SCAN_NODE: {
        op.reset(new EsScanOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
        RETURN_IF_ERROR(cur_pipe->add_operator(op));
        if (request.__isset.parallel_instances) {
            cur_pipe->set_num_tasks(request.parallel_instances);
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
        if (tnode.agg_node.grouping_exprs.empty() &&
            descs.get_tuple_descriptor(tnode.agg_node.output_tuple_id)->slots().empty()) {
            return Status::InternalError("Illegal aggregate node " + std::to_string(tnode.node_id) +
                                         ": group by and output is empty");
        }

        const bool group_by_limit_opt =
                tnode.agg_node.__isset.agg_sort_info_by_group_key && tnode.limit > 0;

        /// PartitionedAggSourceOperatorX does not support "group by limit opt(#29641)" yet.
        /// If `group_by_limit_opt` is true, then it might not need to spill at all.
        const bool enable_spill = _runtime_state->enable_agg_spill() &&
                                  !tnode.agg_node.grouping_exprs.empty() && !group_by_limit_opt;

        if (tnode.agg_node.aggregate_functions.empty() && !enable_spill &&
            request.query_options.__isset.enable_distinct_streaming_aggregation &&
            request.query_options.enable_distinct_streaming_aggregation &&
            !tnode.agg_node.grouping_exprs.empty() && !group_by_limit_opt) {
            op.reset(new DistinctStreamingAggOperatorX(pool, next_operator_id(), tnode, descs,
                                                       _require_bucket_distribution));
            _require_bucket_distribution =
                    _require_bucket_distribution || op->require_data_distribution();
            RETURN_IF_ERROR(cur_pipe->add_operator(op));
        } else if (tnode.agg_node.__isset.use_streaming_preaggregation &&
                   tnode.agg_node.use_streaming_preaggregation &&
                   !tnode.agg_node.grouping_exprs.empty()) {
            op.reset(new StreamingAggOperatorX(pool, next_operator_id(), tnode, descs));
            RETURN_IF_ERROR(cur_pipe->add_operator(op));
        } else {
            if (enable_spill) {
                op.reset(new PartitionedAggSourceOperatorX(pool, tnode, next_operator_id(), descs));
            } else {
                op.reset(new AggSourceOperatorX(pool, tnode, next_operator_id(), descs));
            }
            RETURN_IF_ERROR(cur_pipe->add_operator(op));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (_dag.find(downstream_pipeline_id) == _dag.end()) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            cur_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(cur_pipe->id());

            DataSinkOperatorXPtr sink;
            if (enable_spill) {
                sink.reset(new PartitionedAggSinkOperatorX(pool, next_sink_operator_id(), tnode,
                                                           descs, _require_bucket_distribution));
            } else {
                sink.reset(new AggSinkOperatorX(pool, next_sink_operator_id(), tnode, descs,
                                                _require_bucket_distribution));
            }
            _require_bucket_distribution =
                    _require_bucket_distribution || sink->require_data_distribution();
            sink->set_dests_id({op->operator_id()});
            RETURN_IF_ERROR(cur_pipe->set_sink(sink));
            RETURN_IF_ERROR(cur_pipe->sink_x()->init(tnode, _runtime_state.get()));
        }
        break;
    }
    case TPlanNodeType::HASH_JOIN_NODE: {
        const auto is_broadcast_join = tnode.hash_join_node.__isset.is_broadcast_join &&
                                       tnode.hash_join_node.is_broadcast_join;
        const auto enable_join_spill = _runtime_state->enable_join_spill();
        if (enable_join_spill && !is_broadcast_join) {
            auto tnode_ = tnode;
            /// TODO: support rf in partitioned hash join
            tnode_.runtime_filters.clear();
            const uint32_t partition_count = 32;
            auto inner_probe_operator =
                    std::make_shared<HashJoinProbeOperatorX>(pool, tnode_, 0, descs);
            auto inner_sink_operator = std::make_shared<HashJoinBuildSinkOperatorX>(
                    pool, 0, tnode_, descs, _need_local_merge);

            RETURN_IF_ERROR(inner_probe_operator->init(tnode_, _runtime_state.get()));
            RETURN_IF_ERROR(inner_sink_operator->init(tnode_, _runtime_state.get()));

            auto probe_operator = std::make_shared<PartitionedHashJoinProbeOperatorX>(
                    pool, tnode_, next_operator_id(), descs, partition_count);
            probe_operator->set_inner_operators(inner_sink_operator, inner_probe_operator);
            op = std::move(probe_operator);
            RETURN_IF_ERROR(cur_pipe->add_operator(op));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (_dag.find(downstream_pipeline_id) == _dag.end()) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

            auto sink_operator = std::make_shared<PartitionedHashJoinSinkOperatorX>(
                    pool, next_sink_operator_id(), tnode_, descs, _need_local_merge,
                    partition_count);
            sink_operator->set_inner_operators(inner_sink_operator, inner_probe_operator);
            DataSinkOperatorXPtr sink = std::move(sink_operator);
            sink->set_dests_id({op->operator_id()});
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
            RETURN_IF_ERROR(build_side_pipe->sink_x()->init(tnode_, _runtime_state.get()));

            _pipeline_parent_map.push(op->node_id(), cur_pipe);
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        } else {
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
                                                      _need_local_merge));
            sink->set_dests_id({op->operator_id()});
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
            RETURN_IF_ERROR(build_side_pipe->sink_x()->init(tnode, _runtime_state.get()));

            _pipeline_parent_map.push(op->node_id(), cur_pipe);
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        }
        _require_bucket_distribution =
                _require_bucket_distribution || op->require_data_distribution();
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
        sink.reset(new NestedLoopJoinBuildSinkOperatorX(pool, next_sink_operator_id(), tnode, descs,
                                                        _need_local_merge));
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
        const auto should_spill = _runtime_state->enable_sort_spill() &&
                                  tnode.sort_node.algorithm == TSortAlgorithm::FULL_SORT;
        if (should_spill) {
            op.reset(new SpillSortSourceOperatorX(pool, tnode, next_operator_id(), descs));
        } else {
            op.reset(new SortSourceOperatorX(pool, tnode, next_operator_id(), descs));
        }
        RETURN_IF_ERROR(cur_pipe->add_operator(op));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorXPtr sink;
        if (should_spill) {
            sink.reset(new SpillSortSinkOperatorX(pool, next_sink_operator_id(), tnode, descs,
                                                  _require_bucket_distribution));
        } else {
            sink.reset(new SortSinkOperatorX(pool, next_sink_operator_id(), tnode, descs,
                                             _require_bucket_distribution));
        }
        _require_bucket_distribution =
                _require_bucket_distribution || sink->require_data_distribution();
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
        sink.reset(new AnalyticSinkOperatorX(pool, next_sink_operator_id(), tnode, descs,
                                             _require_bucket_distribution));
        _require_bucket_distribution =
                _require_bucket_distribution || sink->require_data_distribution();
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
        if (request.__isset.parallel_instances) {
            cur_pipe->set_num_tasks(request.parallel_instances);
            op->set_ignore_data_distribution();
        }
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
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

template <bool is_intersect>
Status PipelineFragmentContext::_build_operators_for_set_operation_node(
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

Status PipelineFragmentContext::submit() {
    if (_submitted) {
        return Status::InternalError("submitted");
    }
    _submitted = true;

    int submit_tasks = 0;
    Status st;
    auto* scheduler = _query_ctx->get_pipe_exec_scheduler();
    for (auto& task : _tasks) {
        for (auto& t : task) {
            st = scheduler->schedule_task(t.get());
            if (!st) {
                cancel(Status::InternalError("submit context to executor fail"));
                std::lock_guard<std::mutex> l(_task_mutex);
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

// If all pipeline tasks binded to the fragment instance are finished, then we could
// close the fragment instance.
void PipelineFragmentContext::_close_fragment_instance() {
    if (_is_fragment_instance_closed) {
        return;
    }
    Defer defer_op {[&]() { _is_fragment_instance_closed = true; }};
    _runtime_profile->total_time_counter()->update(_fragment_watcher.elapsed_time());
    static_cast<void>(send_report(true));
    // Print profile content in info log is a tempoeray solution for stream load.
    // Since stream load does not have someting like coordinator on FE, so
    // backend can not report profile to FE, ant its profile can not be shown
    // in the same way with other query. So we print the profile content to info log.
    // Print profile content in log is harmful for log readability, info log will be
    // full of profile content, and not just profile of stream load.
    // We know it, but currently we do not have a cheap and good solution for this.
    if (_runtime_state->enable_profile()) {
        std::stringstream ss;
        // Compute the _local_time_percent before pretty_print the runtime_profile
        // Before add this operation, the print out like that:
        // UNION_NODE (id=0):(Active: 56.720us, non-child: 00.00%)
        // After add the operation, the print out like that:
        // UNION_NODE (id=0):(Active: 56.720us, non-child: 82.53%)
        // We can easily know the exec node execute time without child time consumed.
        for (auto runtime_profile_ptr : _runtime_state->pipeline_id_to_profile()) {
            runtime_profile_ptr->pretty_print(&ss);
        }

        if (_runtime_state->load_channel_profile()) {
            _runtime_state->load_channel_profile()->pretty_print(&ss);
        }

        LOG_INFO("Query {} fragment {} profile:\n {}", print_id(this->_query_id),
                 this->_fragment_id, ss.str());
    }

    if (_query_ctx->enable_profile()) {
        _query_ctx->add_fragment_profile(_fragment_id, collect_realtime_profile_x(),
                                         collect_realtime_load_channel_profile_x());
    }

    // all submitted tasks done
    _exec_env->fragment_mgr()->remove_pipeline_context(
            std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()));
}

void PipelineFragmentContext::close_a_pipeline() {
    std::lock_guard<std::mutex> l(_task_mutex);
    g_pipeline_tasks_count << -1;
    ++_closed_tasks;
    if (_closed_tasks == _total_tasks) {
        _close_fragment_instance();
    }
}

Status PipelineFragmentContext::send_report(bool done) {
    Status exec_status = _query_ctx->exec_status();
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

    ReportStatusRequest req {exec_status,
                             runtime_states,
                             _runtime_profile.get(),
                             _runtime_state->load_channel_profile(),
                             done || !exec_status.ok(),
                             _query_ctx->coord_addr,
                             _query_id,
                             _fragment_id,
                             TUniqueId(),
                             -1,
                             _runtime_state.get(),
                             [this](const Status& reason) { cancel(reason); }};

    return _report_status_cb(
            req, std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()));
}

std::string PipelineFragmentContext::debug_string() {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "PipelineFragmentContext Info:\n");
    for (size_t j = 0; j < _tasks.size(); j++) {
        fmt::format_to(debug_string_buffer, "Tasks in instance {}:\n", j);
        for (size_t i = 0; i < _tasks[j].size(); i++) {
            fmt::format_to(debug_string_buffer, "Task {}: {}\n", i, _tasks[j][i]->debug_string());
        }
    }

    return fmt::to_string(debug_string_buffer);
}

std::vector<std::shared_ptr<TRuntimeProfileTree>>
PipelineFragmentContext::collect_realtime_profile_x() const {
    std::vector<std::shared_ptr<TRuntimeProfileTree>> res;

    // we do not have mutex to protect pipeline_id_to_profile
    // so we need to make sure this funciton is invoked after fragment context
    // has already been prepared.
    if (!this->_prepared) {
        std::string msg =
                "Query " + print_id(this->_query_id) + " collecting profile, but its not prepared";
        DCHECK(false) << msg;
        LOG_ERROR(msg);
        return res;
    }

    // pipeline_id_to_profile is initialized in prepare stage
    for (auto pipeline_profile : _runtime_state->pipeline_id_to_profile()) {
        auto profile_ptr = std::make_shared<TRuntimeProfileTree>();
        pipeline_profile->to_thrift(profile_ptr.get());
        res.push_back(profile_ptr);
    }

    return res;
}

std::shared_ptr<TRuntimeProfileTree>
PipelineFragmentContext::collect_realtime_load_channel_profile_x() const {
    // we do not have mutex to protect pipeline_id_to_profile
    // so we need to make sure this funciton is invoked after fragment context
    // has already been prepared.
    if (!this->_prepared) {
        std::string msg =
                "Query " + print_id(this->_query_id) + " collecting profile, but its not prepared";
        DCHECK(false) << msg;
        LOG_ERROR(msg);
        return nullptr;
    }

    for (auto& runtime_state : _task_runtime_states) {
        if (runtime_state->runtime_profile() == nullptr) {
            continue;
        }

        auto tmp_load_channel_profile = std::make_shared<TRuntimeProfileTree>();

        runtime_state->runtime_profile()->to_thrift(tmp_load_channel_profile.get());
        this->_runtime_state->load_channel_profile()->update(*tmp_load_channel_profile);
    }

    auto load_channel_profile = std::make_shared<TRuntimeProfileTree>();
    this->_runtime_state->load_channel_profile()->to_thrift(load_channel_profile.get());
    return load_channel_profile;
}

} // namespace doris::pipeline
