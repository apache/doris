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

#include <algorithm>
#include <cstdlib>
// IWYU pragma: no_include <bits/chrono.h>
#include <fmt/format.h>

#include <chrono> // IWYU pragma: keep
#include <map>
#include <memory>
#include <ostream>
#include <utility>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/stream_load_pipe.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/analytic_sink_operator.h"
#include "pipeline/exec/analytic_source_operator.h"
#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/cache_sink_operator.h"
#include "pipeline/exec/cache_source_operator.h"
#include "pipeline/exec/datagen_operator.h"
#include "pipeline/exec/dict_sink_operator.h"
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
#include "pipeline/exec/local_merge_sort_source_operator.h"
#include "pipeline/exec/materialization_sink_operator.h"
#include "pipeline/exec/materialization_source_operator.h"
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
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/thread_context.h"
#include "runtime_filter/runtime_filter_mgr.h"
#include "service/backend_options.h"
#include "util/container_util.hpp"
#include "util/debug_util.h"
#include "util/uid_util.h"
#include "vec/common/sort/heap_sorter.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/spill/spill_stream.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
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
    LOG_INFO("PipelineFragmentContext::~PipelineFragmentContext")
            .tag("query_id", print_id(_query_id))
            .tag("fragment_id", _fragment_id);
    // The memory released by the query end is recorded in the query mem tracker.
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_ctx->query_mem_tracker());
    auto st = _query_ctx->exec_status();
    for (size_t i = 0; i < _tasks.size(); i++) {
        if (!_tasks[i].empty()) {
            _call_back(_tasks[i].front()->runtime_state(), &st);
        }
    }
    for (auto& runtime_states : _task_runtime_states) {
        for (auto& runtime_state : runtime_states) {
            runtime_state.reset();
        }
    }
    _tasks.clear();
    _dag.clear();
    _pip_id_to_pipeline.clear();
    _pipelines.clear();
    _sink.reset();
    _root_op.reset();
    _runtime_state.reset();
    _runtime_filter_mgr_map.clear();
    _op_id_to_shared_state.clear();
    _query_ctx.reset();
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
        auto dbg_str = fmt::format("PipelineFragmentContext is cancelled due to timeout:\n{}",
                                   debug_string());
        LOG_LONG_STRING(WARNING, dbg_str);
    }

    // `ILLEGAL_STATE` means queries this fragment belongs to was not found in FE (maybe finished)
    if (reason.is<ErrorCode::ILLEGAL_STATE>()) {
        LOG_WARNING("PipelineFragmentContext is cancelled due to illegal state : {}",
                    debug_string());
    }

    if (reason.is<ErrorCode::MEM_LIMIT_EXCEEDED>() || reason.is<ErrorCode::MEM_ALLOC_FAILED>()) {
        print_profile("cancel pipeline, reason: " + reason.to_string());
    }

    if (auto error_url = get_load_error_url(); !error_url.empty()) {
        _query_ctx->set_load_error_url(error_url);
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
            task->terminate();
        }
    }
}

PipelinePtr PipelineFragmentContext::add_pipeline(PipelinePtr parent, int idx) {
    PipelineId id = _next_pipeline_id++;
    auto pipeline = std::make_shared<Pipeline>(
            id, parent ? std::min(parent->num_tasks(), _num_instances) : _num_instances,
            parent ? parent->num_tasks() : _num_instances);
    if (idx >= 0) {
        _pipelines.insert(_pipelines.begin() + idx, pipeline);
    } else {
        _pipelines.emplace_back(pipeline);
    }
    if (parent) {
        parent->set_children(pipeline);
    }
    return pipeline;
}

Status PipelineFragmentContext::prepare(const doris::TPipelineFragmentParams& request,
                                        ThreadPool* thread_pool) {
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    if (request.__isset.query_options && request.query_options.__isset.execution_timeout) {
        _timeout = request.query_options.execution_timeout;
    }

    _fragment_level_profile = std::make_unique<RuntimeProfile>("PipelineContext");
    _prepare_timer = ADD_TIMER(_fragment_level_profile, "PrepareTime");
    SCOPED_TIMER(_prepare_timer);
    _build_pipelines_timer = ADD_TIMER(_fragment_level_profile, "BuildPipelinesTime");
    _init_context_timer = ADD_TIMER(_fragment_level_profile, "InitContextTime");
    _plan_local_exchanger_timer = ADD_TIMER(_fragment_level_profile, "PlanLocalLocalExchangerTime");
    _build_tasks_timer = ADD_TIMER(_fragment_level_profile, "BuildTasksTime");
    _prepare_all_pipelines_timer = ADD_TIMER(_fragment_level_profile, "PrepareAllPipelinesTime");
    {
        SCOPED_TIMER(_init_context_timer);
        cast_set(_num_instances, request.local_params.size());
        _total_instances =
                request.__isset.total_instances ? request.total_instances : _num_instances;

        auto* fragment_context = this;

        if (request.query_options.__isset.is_report_success) {
            fragment_context->set_is_report_success(request.query_options.is_report_success);
        }

        // 1. Set up the global runtime state.
        _runtime_state = RuntimeState::create_unique(
                request.query_id, request.fragment_id, request.query_options,
                _query_ctx->query_globals, _exec_env, _query_ctx.get());
        _runtime_state->set_task_execution_context(shared_from_this());
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

        // init fragment_instance_ids
        const auto target_size = request.local_params.size();
        _fragment_instance_ids.resize(target_size);
        for (size_t i = 0; i < request.local_params.size(); i++) {
            auto fragment_instance_id = request.local_params[i].fragment_instance_id;
            _fragment_instance_ids[i] = fragment_instance_id;
        }
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
            DCHECK(pipeline->sink() != nullptr) << pipeline->operators().size();
            RETURN_IF_ERROR(pipeline->sink()->set_child(pipeline->operators().back()));
        }
    }
    // 4. Build local exchanger
    if (_runtime_state->enable_local_shuffle()) {
        SCOPED_TIMER(_plan_local_exchanger_timer);
        RETURN_IF_ERROR(_plan_local_exchange(request.num_buckets,
                                             request.bucket_seq_to_instance_idx,
                                             request.shuffle_idx_to_instance_idx));
    }

    // 5. Initialize global states in pipelines.
    for (PipelinePtr& pipeline : _pipelines) {
        SCOPED_TIMER(_prepare_all_pipelines_timer);
        pipeline->children().clear();
        RETURN_IF_ERROR(pipeline->prepare(_runtime_state.get()));
    }

    {
        SCOPED_TIMER(_build_tasks_timer);
        // 6. Build pipeline tasks and initialize local state.
        RETURN_IF_ERROR(_build_pipeline_tasks(request, thread_pool));
    }

    _init_next_report_time();

    _prepared = true;
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipeline_tasks(const doris::TPipelineFragmentParams& request,
                                                      ThreadPool* thread_pool) {
    _total_tasks = 0;
    const auto target_size = request.local_params.size();
    _tasks.resize(target_size);
    _runtime_filter_mgr_map.resize(target_size);
    _task_runtime_states.resize(_pipelines.size());
    for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
        _task_runtime_states[pip_idx].resize(_pipelines[pip_idx]->num_tasks());
        _pip_id_to_pipeline[_pipelines[pip_idx]->id()] = _pipelines[pip_idx].get();
    }
    auto pipeline_id_to_profile = _runtime_state->build_pipeline_profile(_pipelines.size());

    auto pre_and_submit = [&](int i, PipelineFragmentContext* ctx) {
        const auto& local_params = request.local_params[i];
        auto fragment_instance_id = local_params.fragment_instance_id;
        auto runtime_filter_mgr = std::make_unique<RuntimeFilterMgr>(false);
        std::map<PipelineId, PipelineTask*> pipeline_id_to_task;
        auto get_shared_state = [&](PipelinePtr pipeline)
                -> std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                           std::vector<std::shared_ptr<Dependency>>>> {
            std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                    std::vector<std::shared_ptr<Dependency>>>>
                    shared_state_map;
            for (auto& op : pipeline->operators()) {
                auto source_id = op->operator_id();
                if (auto iter = _op_id_to_shared_state.find(source_id);
                    iter != _op_id_to_shared_state.end()) {
                    shared_state_map.insert({source_id, iter->second});
                }
            }
            for (auto sink_to_source_id : pipeline->sink()->dests_id()) {
                if (auto iter = _op_id_to_shared_state.find(sink_to_source_id);
                    iter != _op_id_to_shared_state.end()) {
                    shared_state_map.insert({sink_to_source_id, iter->second});
                }
            }
            return shared_state_map;
        };

        for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
            auto& pipeline = _pipelines[pip_idx];
            if (pipeline->num_tasks() > 1 || i == 0) {
                DCHECK(_task_runtime_states[pip_idx][i] == nullptr)
                        << print_id(_task_runtime_states[pip_idx][i]->fragment_instance_id()) << " "
                        << pipeline->debug_string();
                _task_runtime_states[pip_idx][i] = RuntimeState::create_unique(
                        local_params.fragment_instance_id, request.query_id, request.fragment_id,
                        request.query_options, _query_ctx->query_globals, _exec_env,
                        _query_ctx.get());
                auto& task_runtime_state = _task_runtime_states[pip_idx][i];
                {
                    // Initialize runtime state for this task
                    task_runtime_state->set_query_mem_tracker(_query_ctx->query_mem_tracker());

                    task_runtime_state->set_task_execution_context(shared_from_this());
                    task_runtime_state->set_be_number(local_params.backend_num);

                    if (request.__isset.backend_id) {
                        task_runtime_state->set_backend_id(request.backend_id);
                    }
                    if (request.__isset.import_label) {
                        task_runtime_state->set_import_label(request.import_label);
                    }
                    if (request.__isset.db_name) {
                        task_runtime_state->set_db_name(request.db_name);
                    }
                    if (request.__isset.load_job_id) {
                        task_runtime_state->set_load_job_id(request.load_job_id);
                    }
                    if (request.__isset.wal_id) {
                        task_runtime_state->set_wal_id(request.wal_id);
                    }

                    task_runtime_state->set_desc_tbl(_desc_tbl);
                    task_runtime_state->set_per_fragment_instance_idx(local_params.sender_id);
                    task_runtime_state->set_num_per_fragment_instances(request.num_senders);
                    task_runtime_state->resize_op_id_to_local_state(max_operator_id());
                    task_runtime_state->set_max_operator_id(max_operator_id());
                    task_runtime_state->set_load_stream_per_node(request.load_stream_per_node);
                    task_runtime_state->set_total_load_streams(request.total_load_streams);
                    task_runtime_state->set_num_local_sink(request.num_local_sink);

                    task_runtime_state->set_runtime_filter_mgr(runtime_filter_mgr.get());
                }
                auto cur_task_id = _total_tasks++;
                task_runtime_state->set_task_id(cur_task_id);
                task_runtime_state->set_task_num(pipeline->num_tasks());
                auto task = std::make_shared<PipelineTask>(
                        pipeline, cur_task_id, task_runtime_state.get(),
                        std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()),
                        pipeline_id_to_profile[pip_idx].get(), get_shared_state(pipeline), i);
                pipeline->incr_created_tasks(i, task.get());
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
        for (auto& _pipeline : _pipelines) {
            if (pipeline_id_to_task.contains(_pipeline->id())) {
                auto* task = pipeline_id_to_task[_pipeline->id()];
                DCHECK(task != nullptr);

                // If this task has upstream dependency, then inject it into this task.
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
                DCHECK(pipeline_id_to_profile[pip_idx]);
                std::vector<TScanRangeParams> scan_ranges;
                scan_ranges = find_with_default(local_params.per_node_scan_ranges,
                                                _pipelines[pip_idx]->operators().front()->node_id(),
                                                scan_ranges);
                RETURN_IF_ERROR_OR_CATCH_EXCEPTION(task->prepare(
                        scan_ranges, local_params.sender_id, request.fragment.output_sink));
            }
        }
        {
            std::lock_guard<std::mutex> l(_state_map_lock);
            _runtime_filter_mgr_map[i] = std::move(runtime_filter_mgr);
        }
        return Status::OK();
    };
    if (target_size > 1 &&
        (_runtime_state->query_options().__isset.parallel_prepare_threshold &&
         target_size > _runtime_state->query_options().parallel_prepare_threshold)) {
        // If instances parallelism is big enough ( > parallel_prepare_threshold), we will prepare all tasks by multi-threads
        std::vector<Status> prepare_status(target_size);
        std::mutex m;
        std::condition_variable cv;
        int prepare_done = 0;
        for (int i = 0; i < target_size; i++) {
            RETURN_IF_ERROR(thread_pool->submit_func([&, i]() {
                SCOPED_ATTACH_TASK(_query_ctx.get());
                prepare_status[i] = pre_and_submit(i, this);
                std::unique_lock<std::mutex> lock(m);
                prepare_done++;
                if (prepare_done == target_size) {
                    cv.notify_one();
                }
            }));
        }
        std::unique_lock<std::mutex> lock(m);
        if (prepare_done != target_size) {
            cv.wait(lock);
            for (int i = 0; i < target_size; i++) {
                if (!prepare_status[i].ok()) {
                    return prepare_status[i];
                }
            }
        }
    } else {
        for (int i = 0; i < target_size; i++) {
            RETURN_IF_ERROR(pre_and_submit(i, this));
        }
    }
    _pipeline_parent_map.clear();
    _op_id_to_shared_state.clear();

    return Status::OK();
}

void PipelineFragmentContext::_init_next_report_time() {
    auto interval_s = config::pipeline_status_report_interval;
    if (_is_report_success && interval_s > 0 && _timeout > interval_s) {
        VLOG_FILE << "enable period report: fragment id=" << _fragment_id;
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
            VLOG_FILE << "Reporting "
                      << "profile for query_id " << print_id(_query_id)
                      << ", fragment id: " << _fragment_id;

            std::stringstream ss;
            _runtime_state->runtime_profile()->compute_time_in_profile();
            _runtime_state->runtime_profile()->pretty_print(&ss);
            if (_runtime_state->load_channel_profile()) {
                _runtime_state->load_channel_profile()->pretty_print(&ss);
            }

            VLOG_FILE << "Query " << print_id(get_query_id()) << " fragment " << get_fragment_id()
                      << " profile:\n"
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
                                                 const DescriptorTbl& descs, OperatorPtr* root,
                                                 PipelinePtr cur_pipe) {
    if (request.fragment.plan.nodes.empty()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid plan which has no plan node!");
    }

    int node_idx = 0;

    RETURN_IF_ERROR(_create_tree_helper(pool, request.fragment.plan.nodes, request, descs, nullptr,
                                        &node_idx, root, cur_pipe, 0, false));

    if (node_idx + 1 != request.fragment.plan.nodes.size()) {
        return Status::InternalError(
                "Plan tree only partially reconstructed. Not all thrift nodes were used.");
    }
    return Status::OK();
}

Status PipelineFragmentContext::_create_tree_helper(ObjectPool* pool,
                                                    const std::vector<TPlanNode>& tnodes,
                                                    const doris::TPipelineFragmentParams& request,
                                                    const DescriptorTbl& descs, OperatorPtr parent,
                                                    int* node_idx, OperatorPtr* root,
                                                    PipelinePtr& cur_pipe, int child_idx,
                                                    const bool followed_by_shuffled_operator) {
    // propagate error case
    if (*node_idx >= tnodes.size()) {
        return Status::InternalError(
                "Failed to reconstruct plan tree from thrift. Node id: {}, number of nodes: {}",
                *node_idx, tnodes.size());
    }
    const TPlanNode& tnode = tnodes[*node_idx];

    int num_children = tnodes[*node_idx].num_children;
    bool current_followed_by_shuffled_operator = followed_by_shuffled_operator;
    OperatorPtr op = nullptr;
    RETURN_IF_ERROR(_create_operator(pool, tnodes[*node_idx], request, descs, op, cur_pipe,
                                     parent == nullptr ? -1 : parent->node_id(), child_idx,
                                     followed_by_shuffled_operator));
    // Initialization must be done here. For example, group by expressions in agg will be used to
    // decide if a local shuffle should be planed, so it must be initialized here.
    RETURN_IF_ERROR(op->init(tnode, _runtime_state.get()));
    // assert(parent != nullptr || (node_idx == 0 && root_expr != nullptr));
    if (parent != nullptr) {
        // add to parent's child(s)
        RETURN_IF_ERROR(parent->set_child(op));
    } else {
        *root = op;
    }
    /**
     * `ExchangeType::HASH_SHUFFLE` should be used if an operator is followed by a shuffled operator (shuffled hash join, union operator followed by co-located operators).
     *
     * For plan:
     * LocalExchange(id=0) -> Aggregation(id=1) -> ShuffledHashJoin(id=2)
     *                           Exchange(id=3) -> ShuffledHashJoinBuild(id=2)
     * We must ensure data distribution of `LocalExchange(id=0)` is same as Exchange(id=3).
     *
     * If an operator's is followed by a local exchange without shuffle (e.g. passthrough), a
     * shuffled local exchanger will be used before join so it is not followed by shuffle join.
     */
    auto require_shuffled_data_distribution =
            cur_pipe->operators().empty() ? cur_pipe->sink()->require_shuffled_data_distribution()
                                          : op->require_shuffled_data_distribution();
    current_followed_by_shuffled_operator =
            (followed_by_shuffled_operator || op->is_shuffled_operator()) &&
            require_shuffled_data_distribution;

    if (num_children == 0) {
        _use_serial_source = op->is_serial_operator();
    }
    // rely on that tnodes is preorder of the plan
    for (int i = 0; i < num_children; i++) {
        ++*node_idx;
        RETURN_IF_ERROR(_create_tree_helper(pool, tnodes, request, descs, op, node_idx, nullptr,
                                            cur_pipe, i, current_followed_by_shuffled_operator));

        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= tnodes.size()) {
            return Status::InternalError(
                    "Failed to reconstruct plan tree from thrift. Node id: {}, number of nodes: {}",
                    *node_idx, tnodes.size());
        }
    }

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
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    auto& operators = cur_pipe->operators();
    const auto downstream_pipeline_id = cur_pipe->id();
    auto local_exchange_id = next_operator_id();
    // 1. Create a new pipeline with local exchange sink.
    DataSinkOperatorPtr sink;
    auto sink_id = next_sink_operator_id();

    /**
     * `bucket_seq_to_instance_idx` is empty if no scan operator is contained in this fragment.
     * So co-located operators(e.g. Agg, Analytic) should use `HASH_SHUFFLE` instead of `BUCKET_HASH_SHUFFLE`.
     */
    const bool followed_by_shuffled_operator =
            operators.size() > idx ? operators[idx]->followed_by_shuffled_operator()
                                   : cur_pipe->sink()->followed_by_shuffled_operator();
    const bool use_global_hash_shuffle =
            bucket_seq_to_instance_idx.empty() &&
            shuffle_idx_to_instance_idx.find(-1) == shuffle_idx_to_instance_idx.end() &&
            followed_by_shuffled_operator && !_use_serial_source;
    sink.reset(new LocalExchangeSinkOperatorX(
            sink_id, local_exchange_id, use_global_hash_shuffle ? _total_instances : _num_instances,
            data_distribution.partition_exprs, bucket_seq_to_instance_idx));
    if (bucket_seq_to_instance_idx.empty() &&
        data_distribution.distribution_type == ExchangeType::BUCKET_HASH_SHUFFLE) {
        data_distribution.distribution_type = ExchangeType::HASH_SHUFFLE;
    }
    RETURN_IF_ERROR(new_pip->set_sink(sink));
    RETURN_IF_ERROR(new_pip->sink()->init(data_distribution.distribution_type, num_buckets,
                                          use_global_hash_shuffle, shuffle_idx_to_instance_idx));

    // 2. Create and initialize LocalExchangeSharedState.
    std::shared_ptr<LocalExchangeSharedState> shared_state =
            LocalExchangeSharedState::create_shared(_num_instances);
    switch (data_distribution.distribution_type) {
    case ExchangeType::HASH_SHUFFLE:
        shared_state->exchanger = ShuffleExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances,
                use_global_hash_shuffle ? _total_instances : _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case ExchangeType::BUCKET_HASH_SHUFFLE:
        shared_state->exchanger = BucketShuffleExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances, num_buckets,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case ExchangeType::PASSTHROUGH:
        shared_state->exchanger = PassthroughExchanger::create_unique(
                cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case ExchangeType::BROADCAST:
        shared_state->exchanger = BroadcastExchanger::create_unique(
                cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case ExchangeType::PASS_TO_ONE:
        if (_runtime_state->enable_share_hash_table_for_broadcast_join()) {
            // If shared hash table is enabled for BJ, hash table will be built by only one task
            shared_state->exchanger = PassToOneExchanger::create_unique(
                    cur_pipe->num_tasks(), _num_instances,
                    _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                            ? cast_set<int>(_runtime_state->query_options()
                                                    .local_exchange_free_blocks_limit)
                            : 0);
        } else {
            shared_state->exchanger = BroadcastExchanger::create_unique(
                    cur_pipe->num_tasks(), _num_instances,
                    _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                            ? cast_set<int>(_runtime_state->query_options()
                                                    .local_exchange_free_blocks_limit)
                            : 0);
        }
        break;
    case ExchangeType::ADAPTIVE_PASSTHROUGH:
        shared_state->exchanger = AdaptivePassthroughExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    default:
        return Status::InternalError("Unsupported local exchange type : " +
                                     std::to_string((int)data_distribution.distribution_type));
    }
    shared_state->create_source_dependencies(_num_instances, local_exchange_id, local_exchange_id,
                                             "LOCAL_EXCHANGE_OPERATOR");
    shared_state->create_sink_dependency(sink_id, local_exchange_id, "LOCAL_EXCHANGE_SINK");
    _op_id_to_shared_state.insert({local_exchange_id, {shared_state, shared_state->sink_deps}});

    // 3. Set two pipelines' operator list. For example, split pipeline [Scan - AggSink] to
    // pipeline1 [Scan - LocalExchangeSink] and pipeline2 [LocalExchangeSource - AggSink].

    // 3.1 Initialize new pipeline's operator list.
    std::copy(operators.begin(), operators.begin() + idx,
              std::inserter(new_pip->operators(), new_pip->operators().end()));

    // 3.2 Erase unused operators in previous pipeline.
    operators.erase(operators.begin(), operators.begin() + idx);

    // 4. Initialize LocalExchangeSource and insert it into this pipeline.
    OperatorPtr source_op;
    source_op.reset(new LocalExchangeSourceOperatorX(pool, local_exchange_id));
    RETURN_IF_ERROR(source_op->set_child(new_pip->operators().back()));
    RETURN_IF_ERROR(source_op->init(data_distribution.distribution_type));
    if (!operators.empty()) {
        RETURN_IF_ERROR(operators.front()->set_child(nullptr));
        RETURN_IF_ERROR(operators.front()->set_child(source_op));
    }
    operators.insert(operators.begin(), source_op);

    // 5. Set children for two pipelines separately.
    std::vector<std::shared_ptr<Pipeline>> new_children;
    std::vector<PipelineId> edges_with_source;
    for (auto child : cur_pipe->children()) {
        bool found = false;
        for (auto op : new_pip->operators()) {
            if (child->sink()->node_id() == op->node_id()) {
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
    RETURN_IF_ERROR(new_pip->sink()->set_child(new_pip->operators().back()));
    RETURN_IF_ERROR(cur_pipe->sink()->set_child(nullptr));
    RETURN_IF_ERROR(cur_pipe->sink()->set_child(cur_pipe->operators().back()));

    // 7. Inherit properties from current pipeline.
    _inherit_pipeline_properties(data_distribution, cur_pipe, new_pip);
    return Status::OK();
}

Status PipelineFragmentContext::_add_local_exchange(
        int pip_idx, int idx, int node_id, ObjectPool* pool, PipelinePtr cur_pipe,
        DataDistribution data_distribution, bool* do_local_exchange, int num_buckets,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    if (_num_instances <= 1 || cur_pipe->num_tasks_of_parent() <= 1) {
        return Status::OK();
    }

    if (!cur_pipe->need_to_local_exchange(data_distribution, idx)) {
        return Status::OK();
    }
    *do_local_exchange = true;

    auto& operators = cur_pipe->operators();
    auto total_op_num = operators.size();
    auto new_pip = add_pipeline(cur_pipe, pip_idx + 1);
    RETURN_IF_ERROR(_add_local_exchange_impl(
            idx, pool, cur_pipe, new_pip, data_distribution, do_local_exchange, num_buckets,
            bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx));

    CHECK(total_op_num + 1 == cur_pipe->operators().size() + new_pip->operators().size())
            << "total_op_num: " << total_op_num
            << " cur_pipe->operators().size(): " << cur_pipe->operators().size()
            << " new_pip->operators().size(): " << new_pip->operators().size();

    // There are some local shuffles with relatively heavy operations on the sink.
    // If the local sink concurrency is 1 and the local source concurrency is n, the sink becomes a bottleneck.
    // Therefore, local passthrough is used to increase the concurrency of the sink.
    // op -> local sink(1) -> local source (n)
    // op -> local passthrough(1) -> local passthrough(n) ->  local sink(n) -> local source (n)
    if (cur_pipe->num_tasks() > 1 && new_pip->num_tasks() == 1 &&
        Pipeline::heavy_operations_on_the_sink(data_distribution.distribution_type)) {
        RETURN_IF_ERROR(_add_local_exchange_impl(
                cast_set<int>(new_pip->operators().size()), pool, new_pip,
                add_pipeline(new_pip, pip_idx + 2), DataDistribution(ExchangeType::PASSTHROUGH),
                do_local_exchange, num_buckets, bucket_seq_to_instance_idx,
                shuffle_idx_to_instance_idx));
    }
    return Status::OK();
}

Status PipelineFragmentContext::_plan_local_exchange(
        int num_buckets, const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    for (int pip_idx = cast_set<int>(_pipelines.size()) - 1; pip_idx >= 0; pip_idx--) {
        _pipelines[pip_idx]->init_data_distribution();
        // Set property if child pipeline is not join operator's child.
        if (!_pipelines[pip_idx]->children().empty()) {
            for (auto& child : _pipelines[pip_idx]->children()) {
                if (child->sink()->node_id() ==
                    _pipelines[pip_idx]->operators().front()->node_id()) {
                    _pipelines[pip_idx]->set_data_distribution(child->data_distribution());
                }
            }
        }

        // if 'num_buckets == 0' means the fragment is colocated by exchange node not the
        // scan node. so here use `_num_instance` to replace the `num_buckets` to prevent dividing 0
        // still keep colocate plan after local shuffle
        RETURN_IF_ERROR(_plan_local_exchange(num_buckets, pip_idx, _pipelines[pip_idx],
                                             bucket_seq_to_instance_idx,
                                             shuffle_idx_to_instance_idx));
    }
    return Status::OK();
}

Status PipelineFragmentContext::_plan_local_exchange(
        int num_buckets, int pip_idx, PipelinePtr pip,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    int idx = 1;
    bool do_local_exchange = false;
    do {
        auto& ops = pip->operators();
        do_local_exchange = false;
        // Plan local exchange for each operator.
        for (; idx < ops.size();) {
            if (ops[idx]->required_data_distribution().need_local_exchange()) {
                RETURN_IF_ERROR(_add_local_exchange(
                        pip_idx, idx, ops[idx]->node_id(), _runtime_state->obj_pool(), pip,
                        ops[idx]->required_data_distribution(), &do_local_exchange, num_buckets,
                        bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx));
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
    if (pip->sink()->required_data_distribution().need_local_exchange()) {
        RETURN_IF_ERROR(_add_local_exchange(
                pip_idx, idx, pip->sink()->node_id(), _runtime_state->obj_pool(), pip,
                pip->sink()->required_data_distribution(), &do_local_exchange, num_buckets,
                bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx));
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
                                              thrift_sink.stream_sink, params.destinations,
                                              _fragment_instance_ids));
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
    case TDataSinkType::DICTIONARY_SINK: {
        if (!thrift_sink.__isset.dictionary_sink) {
            return Status::InternalError("Missing dict sink.");
        }

        _sink.reset(new DictSinkOperatorX(next_sink_operator_id(), row_desc, output_exprs,
                                          thrift_sink.dictionary_sink));
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
#ifndef NDEBUG
        DCHECK(state->get_query_ctx() != nullptr);
        state->get_query_ctx()->query_mem_tracker()->is_group_commit_load = true;
#endif
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

        _sink.reset(new MultiCastDataStreamSinkOperatorX(sink_id, sources, pool,
                                                         thrift_sink.multi_cast_stream_sink));
        for (int i = 0; i < sender_size; ++i) {
            auto new_pipeline = add_pipeline();
            // use to exchange sink
            RowDescriptor* exchange_row_desc = nullptr;
            {
                const auto& tmp_row_desc =
                        !thrift_sink.multi_cast_stream_sink.sinks[i].output_exprs.empty()
                                ? RowDescriptor(state->desc_tbl(),
                                                {thrift_sink.multi_cast_stream_sink.sinks[i]
                                                         .output_tuple_id},
                                                {false})
                                : row_desc;
                exchange_row_desc = pool->add(new RowDescriptor(tmp_row_desc));
            }
            auto source_id = sources[i];
            OperatorPtr source_op;
            // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
            source_op.reset(new MultiCastDataStreamerSourceOperatorX(
                    i, pool, thrift_sink.multi_cast_stream_sink.sinks[i], row_desc, source_id));
            RETURN_IF_ERROR(new_pipeline->add_operator(
                    source_op, params.__isset.parallel_instances ? params.parallel_instances : 0));
            // 2. create and set sink operator of data stream sender for new pipeline

            DataSinkOperatorPtr sink_op;
            sink_op.reset(new ExchangeSinkOperatorX(
                    state, *exchange_row_desc, next_sink_operator_id(),
                    thrift_sink.multi_cast_stream_sink.sinks[i],
                    thrift_sink.multi_cast_stream_sink.destinations[i], _fragment_instance_ids));

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
                                                 const DescriptorTbl& descs, OperatorPtr& op,
                                                 PipelinePtr& cur_pipe, int parent_idx,
                                                 int child_idx,
                                                 const bool followed_by_shuffled_operator) {
    // We directly construct the operator from Thrift because the given array is in the order of preorder traversal.
    // Therefore, here we need to use a stack-like structure.
    _pipeline_parent_map.pop(cur_pipe, parent_idx, child_idx);
    std::stringstream error_msg;
    bool enable_query_cache = request.fragment.__isset.query_cache_param;

    bool fe_with_old_version = false;
    switch (tnode.node_type) {
    case TPlanNodeType::OLAP_SCAN_NODE: {
        op.reset(new OlapScanOperatorX(
                pool, tnode, next_operator_id(), descs, _num_instances,
                enable_query_cache ? request.fragment.query_cache_param : TQueryCacheParam {}));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::GROUP_COMMIT_SCAN_NODE: {
#ifndef NDEBUG
        DCHECK(_query_ctx != nullptr);
        _query_ctx->query_mem_tracker()->is_group_commit_load = true;
#endif
        op.reset(new GroupCommitOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case doris::TPlanNodeType::JDBC_SCAN_NODE: {
        if (config::enable_java_support) {
            op.reset(new JDBCScanOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
            RETURN_IF_ERROR(cur_pipe->add_operator(
                    op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        } else {
            return Status::InternalError(
                    "Jdbc scan node is disabled, you can change be config enable_java_support "
                    "to true and restart be.");
        }
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case doris::TPlanNodeType::FILE_SCAN_NODE: {
        op.reset(new FileScanOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::ES_SCAN_NODE:
    case TPlanNodeType::ES_HTTP_SCAN_NODE: {
        op.reset(new EsScanOperatorX(pool, tnode, next_operator_id(), descs, _num_instances));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::EXCHANGE_NODE: {
        int num_senders = find_with_default(request.per_exch_num_senders, tnode.node_id, 0);
        DCHECK_GT(num_senders, 0);
        op.reset(new ExchangeSourceOperatorX(pool, tnode, next_operator_id(), descs, num_senders));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::AGGREGATION_NODE: {
        if (tnode.agg_node.grouping_exprs.empty() &&
            descs.get_tuple_descriptor(tnode.agg_node.output_tuple_id)->slots().empty()) {
            return Status::InternalError("Illegal aggregate node " + std::to_string(tnode.node_id) +
                                         ": group by and output is empty");
        }
        bool need_create_cache_op =
                enable_query_cache && tnode.node_id == request.fragment.query_cache_param.node_id;
        auto create_query_cache_operator = [&](PipelinePtr& new_pipe) {
            auto cache_node_id = request.local_params[0].per_node_scan_ranges.begin()->first;
            auto cache_source_id = next_operator_id();
            op.reset(new CacheSourceOperatorX(pool, cache_node_id, cache_source_id,
                                              request.fragment.query_cache_param));
            RETURN_IF_ERROR(cur_pipe->add_operator(
                    op, request.__isset.parallel_instances ? request.parallel_instances : 0));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (_dag.find(downstream_pipeline_id) == _dag.end()) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            new_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(new_pipe->id());

            DataSinkOperatorPtr cache_sink(new CacheSinkOperatorX(
                    next_sink_operator_id(), cache_source_id, op->operator_id()));
            RETURN_IF_ERROR(new_pipe->set_sink(cache_sink));
            return Status::OK();
        };
        const bool group_by_limit_opt =
                tnode.agg_node.__isset.agg_sort_info_by_group_key && tnode.limit > 0;

        /// PartitionedAggSourceOperatorX does not support "group by limit opt(#29641)" yet.
        /// If `group_by_limit_opt` is true, then it might not need to spill at all.
        const bool enable_spill = _runtime_state->enable_spill() &&
                                  !tnode.agg_node.grouping_exprs.empty() && !group_by_limit_opt;
        const bool is_streaming_agg = tnode.agg_node.__isset.use_streaming_preaggregation &&
                                      tnode.agg_node.use_streaming_preaggregation &&
                                      !tnode.agg_node.grouping_exprs.empty();
        const bool can_use_distinct_streaming_agg =
                is_streaming_agg && tnode.agg_node.aggregate_functions.empty() &&
                request.query_options.__isset.enable_distinct_streaming_aggregation &&
                request.query_options.enable_distinct_streaming_aggregation;

        if (can_use_distinct_streaming_agg) {
            if (need_create_cache_op) {
                PipelinePtr new_pipe;
                RETURN_IF_ERROR(create_query_cache_operator(new_pipe));

                op.reset(new DistinctStreamingAggOperatorX(pool, next_operator_id(), tnode, descs,
                                                           _require_bucket_distribution));
                op->set_followed_by_shuffled_operator(false);
                _require_bucket_distribution = true;
                RETURN_IF_ERROR(new_pipe->add_operator(
                        op, request.__isset.parallel_instances ? request.parallel_instances : 0));
                RETURN_IF_ERROR(cur_pipe->operators().front()->set_child(op));
                cur_pipe = new_pipe;
            } else {
                op.reset(new DistinctStreamingAggOperatorX(pool, next_operator_id(), tnode, descs,
                                                           _require_bucket_distribution));
                op->set_followed_by_shuffled_operator(followed_by_shuffled_operator);
                _require_bucket_distribution =
                        _require_bucket_distribution || op->require_data_distribution();
                RETURN_IF_ERROR(cur_pipe->add_operator(
                        op, request.__isset.parallel_instances ? request.parallel_instances : 0));
            }
        } else if (is_streaming_agg) {
            if (need_create_cache_op) {
                PipelinePtr new_pipe;
                RETURN_IF_ERROR(create_query_cache_operator(new_pipe));

                op.reset(new StreamingAggOperatorX(pool, next_operator_id(), tnode, descs));
                RETURN_IF_ERROR(cur_pipe->operators().front()->set_child(op));
                RETURN_IF_ERROR(new_pipe->add_operator(
                        op, request.__isset.parallel_instances ? request.parallel_instances : 0));
                cur_pipe = new_pipe;
            } else {
                op.reset(new StreamingAggOperatorX(pool, next_operator_id(), tnode, descs));
                RETURN_IF_ERROR(cur_pipe->add_operator(
                        op, request.__isset.parallel_instances ? request.parallel_instances : 0));
            }
        } else {
            // create new pipeline to add query cache operator
            PipelinePtr new_pipe;
            if (need_create_cache_op) {
                RETURN_IF_ERROR(create_query_cache_operator(new_pipe));
            }

            if (enable_spill) {
                op.reset(new PartitionedAggSourceOperatorX(pool, tnode, next_operator_id(), descs));
            } else {
                op.reset(new AggSourceOperatorX(pool, tnode, next_operator_id(), descs));
            }
            if (need_create_cache_op) {
                RETURN_IF_ERROR(cur_pipe->operators().front()->set_child(op));
                RETURN_IF_ERROR(new_pipe->add_operator(
                        op, request.__isset.parallel_instances ? request.parallel_instances : 0));
                cur_pipe = new_pipe;
            } else {
                RETURN_IF_ERROR(cur_pipe->add_operator(
                        op, request.__isset.parallel_instances ? request.parallel_instances : 0));
            }

            const auto downstream_pipeline_id = cur_pipe->id();
            if (_dag.find(downstream_pipeline_id) == _dag.end()) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            cur_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(cur_pipe->id());

            DataSinkOperatorPtr sink;
            if (enable_spill) {
                sink.reset(new PartitionedAggSinkOperatorX(pool, next_sink_operator_id(),
                                                           op->operator_id(), tnode, descs,
                                                           _require_bucket_distribution));
            } else {
                sink.reset(new AggSinkOperatorX(pool, next_sink_operator_id(), op->operator_id(),
                                                tnode, descs, _require_bucket_distribution));
            }
            sink->set_followed_by_shuffled_operator(followed_by_shuffled_operator);
            _require_bucket_distribution =
                    _require_bucket_distribution || sink->require_data_distribution();
            RETURN_IF_ERROR(cur_pipe->set_sink(sink));
            RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        }
        break;
    }
    case TPlanNodeType::HASH_JOIN_NODE: {
        const auto is_broadcast_join = tnode.hash_join_node.__isset.is_broadcast_join &&
                                       tnode.hash_join_node.is_broadcast_join;
        const auto enable_spill = _runtime_state->enable_spill();
        if (enable_spill && !is_broadcast_join) {
            auto tnode_ = tnode;
            tnode_.runtime_filters.clear();
            uint32_t partition_count = _runtime_state->spill_hash_join_partition_count();
            auto inner_probe_operator =
                    std::make_shared<HashJoinProbeOperatorX>(pool, tnode_, 0, descs);

            // probe side inner sink operator is used to build hash table on probe side when data is spilled.
            // So here use `tnode_` which has no runtime filters.
            auto probe_side_inner_sink_operator =
                    std::make_shared<HashJoinBuildSinkOperatorX>(pool, 0, 0, tnode_, descs);

            RETURN_IF_ERROR(inner_probe_operator->init(tnode_, _runtime_state.get()));
            RETURN_IF_ERROR(probe_side_inner_sink_operator->init(tnode_, _runtime_state.get()));

            auto probe_operator = std::make_shared<PartitionedHashJoinProbeOperatorX>(
                    pool, tnode_, next_operator_id(), descs, partition_count);
            probe_operator->set_inner_operators(probe_side_inner_sink_operator,
                                                inner_probe_operator);
            op = std::move(probe_operator);
            RETURN_IF_ERROR(cur_pipe->add_operator(
                    op, request.__isset.parallel_instances ? request.parallel_instances : 0));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (_dag.find(downstream_pipeline_id) == _dag.end()) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

            auto inner_sink_operator =
                    std::make_shared<HashJoinBuildSinkOperatorX>(pool, 0, 0, tnode, descs);
            auto sink_operator = std::make_shared<PartitionedHashJoinSinkOperatorX>(
                    pool, next_sink_operator_id(), op->operator_id(), tnode_, descs,
                    partition_count);
            RETURN_IF_ERROR(inner_sink_operator->init(tnode, _runtime_state.get()));

            sink_operator->set_inner_operators(inner_sink_operator, inner_probe_operator);
            DataSinkOperatorPtr sink = std::move(sink_operator);
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
            RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode_, _runtime_state.get()));

            _pipeline_parent_map.push(op->node_id(), cur_pipe);
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
            sink->set_followed_by_shuffled_operator(sink->is_shuffled_operator());
            op->set_followed_by_shuffled_operator(op->is_shuffled_operator());
        } else {
            op.reset(new HashJoinProbeOperatorX(pool, tnode, next_operator_id(), descs));
            RETURN_IF_ERROR(cur_pipe->add_operator(
                    op, request.__isset.parallel_instances ? request.parallel_instances : 0));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (_dag.find(downstream_pipeline_id) == _dag.end()) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

            DataSinkOperatorPtr sink;
            sink.reset(new HashJoinBuildSinkOperatorX(pool, next_sink_operator_id(),
                                                      op->operator_id(), tnode, descs));
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
            RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode, _runtime_state.get()));

            _pipeline_parent_map.push(op->node_id(), cur_pipe);
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
            sink->set_followed_by_shuffled_operator(sink->is_shuffled_operator());
            op->set_followed_by_shuffled_operator(op->is_shuffled_operator());
        }
        if (is_broadcast_join && _runtime_state->enable_share_hash_table_for_broadcast_join()) {
            std::shared_ptr<HashJoinSharedState> shared_state =
                    HashJoinSharedState::create_shared(_num_instances);
            for (int i = 0; i < _num_instances; i++) {
                auto sink_dep = std::make_shared<Dependency>(op->operator_id(), op->node_id(),
                                                             "HASH_JOIN_BUILD_DEPENDENCY");
                sink_dep->set_shared_state(shared_state.get());
                shared_state->sink_deps.push_back(sink_dep);
            }
            shared_state->create_source_dependencies(_num_instances, op->operator_id(),
                                                     op->node_id(), "HASH_JOIN_PROBE");
            _op_id_to_shared_state.insert(
                    {op->operator_id(), {shared_state, shared_state->sink_deps}});
        }
        _require_bucket_distribution =
                _require_bucket_distribution || op->require_data_distribution();
        break;
    }
    case TPlanNodeType::CROSS_JOIN_NODE: {
        op.reset(new NestedLoopJoinProbeOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

        DataSinkOperatorPtr sink;
        sink.reset(new NestedLoopJoinBuildSinkOperatorX(pool, next_sink_operator_id(),
                                                        op->operator_id(), tnode, descs));
        RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
        RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode, _runtime_state.get()));
        _pipeline_parent_map.push(op->node_id(), cur_pipe);
        _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        break;
    }
    case TPlanNodeType::UNION_NODE: {
        int child_count = tnode.num_children;
        op.reset(new UnionSourceOperatorX(pool, tnode, next_operator_id(), descs));
        op->set_followed_by_shuffled_operator(_require_bucket_distribution);
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        for (int i = 0; i < child_count; i++) {
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());
            DataSinkOperatorPtr sink;
            sink.reset(new UnionSinkOperatorX(i, next_sink_operator_id(), op->operator_id(), pool,
                                              tnode, descs));
            sink->set_followed_by_shuffled_operator(_require_bucket_distribution);
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
            RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode, _runtime_state.get()));
            // preset children pipelines. if any pipeline found this as its father, will use the prepared pipeline to build.
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        }
        break;
    }
    case TPlanNodeType::SORT_NODE: {
        const auto should_spill = _runtime_state->enable_spill() &&
                                  tnode.sort_node.algorithm == TSortAlgorithm::FULL_SORT;
        const bool use_local_merge =
                tnode.sort_node.__isset.use_local_merge && tnode.sort_node.use_local_merge;
        if (should_spill) {
            op.reset(new SpillSortSourceOperatorX(pool, tnode, next_operator_id(), descs));
        } else if (use_local_merge) {
            op.reset(new LocalMergeSortSourceOperatorX(pool, tnode, next_operator_id(), descs));
        } else {
            op.reset(new SortSourceOperatorX(pool, tnode, next_operator_id(), descs));
        }
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorPtr sink;
        if (should_spill) {
            sink.reset(new SpillSortSinkOperatorX(pool, next_sink_operator_id(), op->operator_id(),
                                                  tnode, descs, _require_bucket_distribution));
        } else {
            sink.reset(new SortSinkOperatorX(pool, next_sink_operator_id(), op->operator_id(),
                                             tnode, descs, _require_bucket_distribution));
        }
        sink->set_followed_by_shuffled_operator(followed_by_shuffled_operator);
        _require_bucket_distribution =
                _require_bucket_distribution || sink->require_data_distribution();
        RETURN_IF_ERROR(cur_pipe->set_sink(sink));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        break;
    }
    case doris::TPlanNodeType::PARTITION_SORT_NODE: {
        op.reset(new PartitionSortSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorPtr sink;
        sink.reset(new PartitionSortSinkOperatorX(pool, next_sink_operator_id(), op->operator_id(),
                                                  tnode, descs));
        RETURN_IF_ERROR(cur_pipe->set_sink(sink));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        break;
    }
    case TPlanNodeType::ANALYTIC_EVAL_NODE: {
        op.reset(new AnalyticSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorPtr sink;
        sink.reset(new AnalyticSinkOperatorX(pool, next_sink_operator_id(), op->operator_id(),
                                             tnode, descs, _require_bucket_distribution));
        sink->set_followed_by_shuffled_operator(followed_by_shuffled_operator);
        _require_bucket_distribution =
                _require_bucket_distribution || sink->require_data_distribution();
        RETURN_IF_ERROR(cur_pipe->set_sink(sink));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        break;
    }
    case TPlanNodeType::MATERIALIZATION_NODE: {
        op.reset(new MaterializationSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (_dag.find(downstream_pipeline_id) == _dag.end()) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        auto new_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(new_pipe->id());

        DataSinkOperatorPtr sink(new MaterializationSinkOperatorX(
                op->operator_id(), next_sink_operator_id(), pool, tnode));
        RETURN_IF_ERROR(new_pipe->set_sink(sink));
        RETURN_IF_ERROR(new_pipe->sink()->init(tnode, _runtime_state.get()));
        cur_pipe = new_pipe;
        break;
    }
    case TPlanNodeType::INTERSECT_NODE: {
        RETURN_IF_ERROR(_build_operators_for_set_operation_node<true>(
                pool, tnode, descs, op, cur_pipe, parent_idx, child_idx, request));
        op->set_followed_by_shuffled_operator(_require_bucket_distribution);
        break;
    }
    case TPlanNodeType::EXCEPT_NODE: {
        RETURN_IF_ERROR(_build_operators_for_set_operation_node<false>(
                pool, tnode, descs, op, cur_pipe, parent_idx, child_idx, request));
        op->set_followed_by_shuffled_operator(_require_bucket_distribution);
        break;
    }
    case TPlanNodeType::REPEAT_NODE: {
        op.reset(new RepeatOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        break;
    }
    case TPlanNodeType::TABLE_FUNCTION_NODE: {
        op.reset(new TableFunctionOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        break;
    }
    case TPlanNodeType::ASSERT_NUM_ROWS_NODE: {
        op.reset(new AssertNumRowsOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        break;
    }
    case TPlanNodeType::EMPTY_SET_NODE: {
        op.reset(new EmptySetSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        break;
    }
    case TPlanNodeType::DATA_GEN_SCAN_NODE: {
        op.reset(new DataGenSourceOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::SCHEMA_SCAN_NODE: {
        op.reset(new SchemaScanOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        break;
    }
    case TPlanNodeType::META_SCAN_NODE: {
        op.reset(new MetaScanOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        break;
    }
    case TPlanNodeType::SELECT_NODE: {
        op.reset(new SelectOperatorX(pool, tnode, next_operator_id(), descs));
        RETURN_IF_ERROR(cur_pipe->add_operator(
                op, request.__isset.parallel_instances ? request.parallel_instances : 0));
        break;
    }
    default:
        return Status::InternalError("Unsupported exec type in pipeline: {}",
                                     print_plan_node_type(tnode.node_type));
    }
    if (request.__isset.parallel_instances && fe_with_old_version) {
        cur_pipe->set_num_tasks(request.parallel_instances);
        op->set_serial_operator();
    }

    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

template <bool is_intersect>
Status PipelineFragmentContext::_build_operators_for_set_operation_node(
        ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs, OperatorPtr& op,
        PipelinePtr& cur_pipe, int parent_idx, int child_idx,
        const doris::TPipelineFragmentParams& request) {
    op.reset(new SetSourceOperatorX<is_intersect>(pool, tnode, next_operator_id(), descs));
    RETURN_IF_ERROR(cur_pipe->add_operator(
            op, request.__isset.parallel_instances ? request.parallel_instances : 0));

    const auto downstream_pipeline_id = cur_pipe->id();
    if (_dag.find(downstream_pipeline_id) == _dag.end()) {
        _dag.insert({downstream_pipeline_id, {}});
    }

    for (int child_id = 0; child_id < tnode.num_children; child_id++) {
        PipelinePtr probe_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(probe_side_pipe->id());

        DataSinkOperatorPtr sink;
        if (child_id == 0) {
            sink.reset(new SetSinkOperatorX<is_intersect>(child_id, next_sink_operator_id(),
                                                          op->operator_id(), pool, tnode, descs));
        } else {
            sink.reset(new SetProbeSinkOperatorX<is_intersect>(
                    child_id, next_sink_operator_id(), op->operator_id(), pool, tnode, descs));
        }
        RETURN_IF_ERROR(probe_side_pipe->set_sink(sink));
        RETURN_IF_ERROR(probe_side_pipe->sink()->init(tnode, _runtime_state.get()));
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
            st = scheduler->schedule_task(t);
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

void PipelineFragmentContext::print_profile(const std::string& extra_info) {
    if (_runtime_state->enable_profile()) {
        std::stringstream ss;
        for (auto runtime_profile_ptr : _runtime_state->pipeline_id_to_profile()) {
            runtime_profile_ptr->pretty_print(&ss);
        }

        if (_runtime_state->load_channel_profile()) {
            _runtime_state->load_channel_profile()->pretty_print(&ss);
        }

        auto profile_str =
                fmt::format("Query {} fragment {} {}, profile, {}", print_id(this->_query_id),
                            this->_fragment_id, extra_info, ss.str());
        LOG_LONG_STRING(INFO, profile_str);
    }
}
// If all pipeline tasks binded to the fragment instance are finished, then we could
// close the fragment instance.
void PipelineFragmentContext::_close_fragment_instance() {
    if (_is_fragment_instance_closed) {
        return;
    }
    Defer defer_op {[&]() { _is_fragment_instance_closed = true; }};
    _fragment_level_profile->total_time_counter()->update(_fragment_watcher.elapsed_time());
    static_cast<void>(send_report(true));
    // Print profile content in info log is a tempoeray solution for stream load and external_connector.
    // Since stream load does not have someting like coordinator on FE, so
    // backend can not report profile to FE, ant its profile can not be shown
    // in the same way with other query. So we print the profile content to info log.

    if (_runtime_state->enable_profile() &&
        (_query_ctx->get_query_source() == QuerySource::STREAM_LOAD ||
         _query_ctx->get_query_source() == QuerySource::EXTERNAL_CONNECTOR ||
         _query_ctx->get_query_source() == QuerySource::GROUP_COMMIT_LOAD)) {
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

        LOG_INFO("Query {} fragment {} profile:\n {}", print_id(_query_id), _fragment_id, ss.str());
    }

    if (_query_ctx->enable_profile()) {
        _query_ctx->add_fragment_profile(_fragment_id, collect_realtime_profile(),
                                         collect_realtime_load_channel_profile());
    }

    // all submitted tasks done
    _exec_env->fragment_mgr()->remove_pipeline_context({_query_id, _fragment_id});
}

void PipelineFragmentContext::decrement_running_task(PipelineId pipeline_id) {
    // If all tasks of this pipeline has been closed, upstream tasks is never needed, and we just make those runnable here
    DCHECK(_pip_id_to_pipeline.contains(pipeline_id));
    if (_pip_id_to_pipeline[pipeline_id]->close_task()) {
        if (_dag.contains(pipeline_id)) {
            for (auto dep : _dag[pipeline_id]) {
                _pip_id_to_pipeline[dep]->make_all_runnable();
            }
        }
    }
    std::lock_guard<std::mutex> l(_task_mutex);
    ++_closed_tasks;
    if (_closed_tasks == _total_tasks) {
        _close_fragment_instance();
    }
}

std::string PipelineFragmentContext::get_load_error_url() {
    if (const auto& str = _runtime_state->get_error_log_file_path(); !str.empty()) {
        return to_load_error_http_path(str);
    }
    for (auto& task_states : _task_runtime_states) {
        for (auto& task_state : task_states) {
            if (!task_state) {
                continue;
            }
            if (const auto& str = task_state->get_error_log_file_path(); !str.empty()) {
                return to_load_error_http_path(str);
            }
        }
    }
    return "";
}

Status PipelineFragmentContext::send_report(bool done) {
    Status exec_status = _query_ctx->exec_status();
    // If plan is done successfully, but _is_report_success is false,
    // no need to send report.
    // Load will set _is_report_success to true because load wants to know
    // the process.
    if (!_is_report_success && done && exec_status.ok()) {
        return Status::NeedSendAgain("");
    }

    // If both _is_report_success and _is_report_on_cancel are false,
    // which means no matter query is success or failed, no report is needed.
    // This may happen when the query limit reached and
    // a internal cancellation being processed
    // When limit is reached the fragment is also cancelled, but _is_report_on_cancel will
    // be set to false, to avoid sending fault report to FE.
    if (!_is_report_success && !_is_report_on_cancel) {
        return Status::NeedSendAgain("");
    }

    std::vector<RuntimeState*> runtime_states;

    for (auto& task_states : _task_runtime_states) {
        for (auto& task_state : task_states) {
            if (task_state) {
                runtime_states.push_back(task_state.get());
            }
        }
    }

    std::string load_eror_url = _query_ctx->get_load_error_url().empty()
                                        ? get_load_error_url()
                                        : _query_ctx->get_load_error_url();

    ReportStatusRequest req {exec_status,
                             runtime_states,
                             done || !exec_status.ok(),
                             _query_ctx->coord_addr,
                             _query_id,
                             _fragment_id,
                             TUniqueId(),
                             -1,
                             _runtime_state.get(),
                             load_eror_url,
                             [this](const Status& reason) { cancel(reason); }};

    return _report_status_cb(
            req, std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()));
}

size_t PipelineFragmentContext::get_revocable_size(bool* has_running_task) const {
    size_t res = 0;
    // _tasks will be cleared during ~PipelineFragmentContext, so that it's safe
    // here to traverse the vector.
    for (const auto& task_instances : _tasks) {
        for (const auto& task : task_instances) {
            if (task->is_running() || task->is_revoking()) {
                LOG_EVERY_N(INFO, 50) << "Query: " << print_id(_query_id)
                                      << " is running, task: " << (void*)task.get()
                                      << ", is_revoking: " << task->is_revoking()
                                      << ", is_running: " << task->is_running();
                *has_running_task = true;
                return 0;
            }

            size_t revocable_size = task->get_revocable_size();
            if (revocable_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                res += revocable_size;
            }
        }
    }
    return res;
}

std::vector<PipelineTask*> PipelineFragmentContext::get_revocable_tasks() const {
    std::vector<PipelineTask*> revocable_tasks;
    for (const auto& task_instances : _tasks) {
        for (const auto& task : task_instances) {
            size_t revocable_size_ = task->get_revocable_size();
            if (revocable_size_ >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                revocable_tasks.emplace_back(task.get());
            }
        }
    }
    return revocable_tasks;
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
PipelineFragmentContext::collect_realtime_profile() const {
    std::vector<std::shared_ptr<TRuntimeProfileTree>> res;

    // we do not have mutex to protect pipeline_id_to_profile
    // so we need to make sure this funciton is invoked after fragment context
    // has already been prepared.
    if (!_prepared) {
        std::string msg =
                "Query " + print_id(_query_id) + " collecting profile, but its not prepared";
        DCHECK(false) << msg;
        LOG_ERROR(msg);
        return res;
    }

    // Make sure first profile is fragment level profile
    auto fragment_profile = std::make_shared<TRuntimeProfileTree>();
    _fragment_level_profile->to_thrift(fragment_profile.get(), _runtime_state->profile_level());
    res.push_back(fragment_profile);

    // pipeline_id_to_profile is initialized in prepare stage
    for (auto pipeline_profile : _runtime_state->pipeline_id_to_profile()) {
        auto profile_ptr = std::make_shared<TRuntimeProfileTree>();
        pipeline_profile->to_thrift(profile_ptr.get(), _runtime_state->profile_level());
        res.push_back(profile_ptr);
    }

    return res;
}

std::shared_ptr<TRuntimeProfileTree>
PipelineFragmentContext::collect_realtime_load_channel_profile() const {
    // we do not have mutex to protect pipeline_id_to_profile
    // so we need to make sure this funciton is invoked after fragment context
    // has already been prepared.
    if (!_prepared) {
        std::string msg =
                "Query " + print_id(_query_id) + " collecting profile, but its not prepared";
        DCHECK(false) << msg;
        LOG_ERROR(msg);
        return nullptr;
    }

    for (auto& runtime_states : _task_runtime_states) {
        for (auto& runtime_state : runtime_states) {
            if (runtime_state == nullptr || runtime_state->runtime_profile() == nullptr) {
                continue;
            }

            auto tmp_load_channel_profile = std::make_shared<TRuntimeProfileTree>();

            runtime_state->runtime_profile()->to_thrift(tmp_load_channel_profile.get(),
                                                        _runtime_state->profile_level());
            _runtime_state->load_channel_profile()->update(*tmp_load_channel_profile);
        }
    }

    auto load_channel_profile = std::make_shared<TRuntimeProfileTree>();
    _runtime_state->load_channel_profile()->to_thrift(load_channel_profile.get(),
                                                      _runtime_state->profile_level());
    return load_channel_profile;
}
#include "common/compile_check_end.h"
} // namespace doris::pipeline
