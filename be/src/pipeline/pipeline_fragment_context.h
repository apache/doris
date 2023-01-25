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

#pragma once

#include "io/fs/stream_load_pipe.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_task.h"
#include "runtime/runtime_state.h"

namespace doris {
class ExecNode;
class DataSink;

namespace vectorized {
template <bool is_intersect>
class VSetOperationNode;
}

namespace pipeline {

class PipelineTask;

class PipelineFragmentContext : public std::enable_shared_from_this<PipelineFragmentContext> {
public:
    PipelineFragmentContext(const TUniqueId& query_id, const TUniqueId& instance_id,
                            int backend_num, std::shared_ptr<QueryFragmentsCtx> query_ctx,
                            ExecEnv* exec_env,
                            std::function<void(RuntimeState*, Status*)> call_back);

    ~PipelineFragmentContext();

    PipelinePtr add_pipeline();

    TUniqueId get_fragment_id() { return _fragment_id; }

    RuntimeState* get_runtime_state() { return _runtime_state.get(); }

    // should be protected by lock?
    bool is_canceled() const { return _runtime_state->is_cancelled(); }

    int32_t next_operator_builder_id() { return _next_operator_builder_id++; }

    Status prepare(const doris::TExecPlanFragmentParams& request);

    Status submit();

    void close_if_prepare_failed();

    void set_is_report_success(bool is_report_success) { _is_report_success = is_report_success; }

    ExecNode*& plan() { return _root_plan; }

    void set_need_wait_execution_trigger() { _need_wait_execution_trigger = true; }

    void cancel(const PPlanFragmentCancelReason& reason = PPlanFragmentCancelReason::INTERNAL_ERROR,
                const std::string& msg = "");

    // TODO: Support pipeline runtime filter

    QueryFragmentsCtx* get_query_context() { return _query_ctx.get(); }

    TUniqueId get_query_id() const { return _query_id; }

    void close_a_pipeline();

    std::string to_http_path(const std::string& file_name);

    void set_merge_controller_handler(
            std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {
        _merge_controller_handler = handler;
    }

    void send_report(bool);

    void set_pipe(std::shared_ptr<io::StreamLoadPipe> pipe) { _pipe = pipe; }
    std::shared_ptr<io::StreamLoadPipe> get_pipe() const { return _pipe; }

private:
    // Id of this query
    TUniqueId _query_id;
    TUniqueId _fragment_id;

    int _backend_num;

    ExecEnv* _exec_env;

    bool _prepared = false;
    bool _submitted = false;

    std::mutex _status_lock;
    Status _exec_status;
    PPlanFragmentCancelReason _cancel_reason;
    std::string _cancel_msg;

    Pipelines _pipelines;
    PipelineId _next_pipeline_id = 0;
    std::atomic_int _closed_tasks = 0;
    // After prepared, `_total_tasks` is equal to the size of `_tasks`.
    // When submit fail, `_total_tasks` is equal to the number of tasks submitted.
    std::atomic_int _total_tasks = 0;
    std::vector<std::unique_ptr<PipelineTask>> _tasks;

    int32_t _next_operator_builder_id = 10000;

    PipelinePtr _root_pipeline;

    std::unique_ptr<RuntimeProfile> _runtime_profile;
    bool _is_report_success = false;

    std::unique_ptr<RuntimeState> _runtime_state;

    ExecNode* _root_plan = nullptr; // lives in _runtime_state->obj_pool()
    std::unique_ptr<DataSink> _sink;

    std::shared_ptr<QueryFragmentsCtx> _query_ctx;

    // If set the true, this plan fragment will be executed only after FE send execution start rpc.
    bool _need_wait_execution_trigger = false;
    std::shared_ptr<RuntimeFilterMergeControllerEntity> _merge_controller_handler;

    MonotonicStopWatch _fragment_watcher;
    RuntimeProfile::Counter* _start_timer;
    RuntimeProfile::Counter* _prepare_timer;

    std::shared_ptr<io::StreamLoadPipe> _pipe;

    Status _create_sink(const TDataSink& t_data_sink);
    Status _build_pipelines(ExecNode*, PipelinePtr);
    Status _build_pipeline_tasks(const doris::TExecPlanFragmentParams& request);

    template <bool is_intersect>
    Status _build_operators_for_set_operation_node(ExecNode*, PipelinePtr);
    std::function<void(RuntimeState*, Status*)> _call_back;
    void _close_action();
    std::once_flag _close_once_flag;
};
} // namespace pipeline
} // namespace doris