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

#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_task.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"

namespace doris {
class ExecNode;
class DataSink;
struct ReportStatusRequest;
class ExecEnv;
class RuntimeFilterMergeControllerEntity;
class TDataSink;
class TPipelineFragmentParams;

namespace pipeline {

class PipelineFragmentContext : public std::enable_shared_from_this<PipelineFragmentContext> {
public:
    // Callback to report execution status of plan fragment.
    // 'profile' is the cumulative profile, 'done' indicates whether the execution
    // is done or still continuing.
    // Note: this does not take a const RuntimeProfile&, because it might need to call
    // functions like PrettyPrint() or to_thrift(), neither of which is const
    // because they take locks.
    using report_status_callback = std::function<Status(
            const ReportStatusRequest, std::shared_ptr<pipeline::PipelineFragmentContext>&&)>;
    PipelineFragmentContext(const TUniqueId& query_id, const TUniqueId& instance_id,
                            const int fragment_id, int backend_num,
                            std::shared_ptr<QueryContext> query_ctx, ExecEnv* exec_env,
                            const std::function<void(RuntimeState*, Status*)>& call_back,
                            const report_status_callback& report_status_cb,
                            bool group_commit = false);

    virtual ~PipelineFragmentContext();

    PipelinePtr add_pipeline();

    TUniqueId get_fragment_instance_id() { return _fragment_instance_id; }

    virtual RuntimeState* get_runtime_state(UniqueId /*fragment_instance_id*/) {
        return _runtime_state.get();
    }

    // should be protected by lock?
    [[nodiscard]] bool is_canceled() const { return _runtime_state->is_cancelled(); }

    int32_t next_operator_builder_id() { return _next_operator_builder_id++; }

    Status prepare(const doris::TPipelineFragmentParams& request, const size_t idx);

    virtual Status prepare(const doris::TPipelineFragmentParams& request) {
        return Status::InternalError("Pipeline fragment context do not implement prepare");
    }

    virtual Status submit();

    virtual void close_if_prepare_failed();
    virtual void close_sink();

    void set_is_report_success(bool is_report_success) { _is_report_success = is_report_success; }

    virtual void cancel(
            const PPlanFragmentCancelReason& reason = PPlanFragmentCancelReason::INTERNAL_ERROR,
            const std::string& msg = "");

    // TODO: Support pipeline runtime filter

    QueryContext* get_query_context() { return _query_ctx.get(); }

    TUniqueId get_query_id() const { return _query_id; }

    [[nodiscard]] int get_fragment_id() const { return _fragment_id; }

    void close_a_pipeline();

    std::string to_http_path(const std::string& file_name);

    void set_merge_controller_handler(
            std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {
        _merge_controller_handler = handler;
    }

    virtual void add_merge_controller_handler(
            std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {}

    virtual Status send_report(bool);

    Status update_status(Status status) {
        std::lock_guard<std::mutex> l(_status_lock);
        if (!status.ok() && _query_ctx->exec_status().ok()) {
            _query_ctx->set_exec_status(status);
        }
        return _query_ctx->exec_status();
    }

    [[nodiscard]] taskgroup::TaskGroupPipelineTaskEntity* get_task_group_entity() const {
        return _task_group_entity;
    }
    void trigger_report_if_necessary();

    bool is_group_commit() { return _group_commit; }
    virtual void instance_ids(std::vector<TUniqueId>& ins_ids) const {
        ins_ids.resize(1);
        ins_ids[0] = _fragment_instance_id;
    }
    virtual void instance_ids(std::vector<string>& ins_ids) const {
        ins_ids.resize(1);
        ins_ids[0] = print_id(_fragment_instance_id);
    }
    void refresh_next_report_time();

protected:
    Status _create_sink(int sender_id, const TDataSink& t_data_sink, RuntimeState* state);
    Status _build_pipelines(ExecNode*, PipelinePtr);
    virtual Status _build_pipeline_tasks(const doris::TPipelineFragmentParams& request);
    template <bool is_intersect>
    Status _build_operators_for_set_operation_node(ExecNode*, PipelinePtr);
    virtual void _close_action();
    void _init_next_report_time();
    void _set_is_report_on_cancel(bool val) { _is_report_on_cancel = val; }

    // Id of this query
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    int _fragment_id;

    int _backend_num;

    ExecEnv* _exec_env = nullptr;

    bool _prepared = false;
    bool _submitted = false;

    std::mutex _status_lock;

    Pipelines _pipelines;
    PipelineId _next_pipeline_id = 0;
    std::mutex _task_mutex;
    int _closed_tasks = 0;
    // After prepared, `_total_tasks` is equal to the size of `_tasks`.
    // When submit fail, `_total_tasks` is equal to the number of tasks submitted.
    int _total_tasks = 0;

    int32_t _next_operator_builder_id = 10000;

    PipelinePtr _root_pipeline;

    std::unique_ptr<RuntimeProfile> _runtime_profile;
    bool _is_report_success = false;

    std::unique_ptr<RuntimeState> _runtime_state;

    ExecNode* _root_plan = nullptr; // lives in _runtime_state->obj_pool()
    // TODO: remove the _sink and _multi_cast_stream_sink_senders to set both
    // of it in pipeline task not the fragment_context
    std::unique_ptr<DataSink> _sink;
    std::vector<std::unique_ptr<DataSink>> _multi_cast_stream_sink_senders;

    std::shared_ptr<QueryContext> _query_ctx;

    taskgroup::TaskGroupPipelineTaskEntity* _task_group_entity = nullptr;

    std::shared_ptr<RuntimeFilterMergeControllerEntity> _merge_controller_handler;

    MonotonicStopWatch _fragment_watcher;
    RuntimeProfile::Counter* _start_timer;
    RuntimeProfile::Counter* _prepare_timer;

    std::function<void(RuntimeState*, Status*)> _call_back;
    std::once_flag _close_once_flag;

    // If this is set to false, and '_is_report_success' is false as well,
    // This executor will not report status to FE on being cancelled.
    bool _is_report_on_cancel;

    // 0 indicates reporting is in progress or not required
    std::atomic_bool _disable_period_report = true;
    std::atomic_uint64_t _previous_report_time = 0;

    // profile reporting-related
    report_status_callback _report_status_cb;

private:
    bool _has_inverted_index(TOlapTableSink sink);
    std::vector<std::unique_ptr<PipelineTask>> _tasks;
    bool _group_commit;
};
} // namespace pipeline
} // namespace doris