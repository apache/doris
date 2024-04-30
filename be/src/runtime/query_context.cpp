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

#include "runtime/query_context.h"

#include <fmt/core.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <exception>
#include <memory>
#include <mutex>
#include <sstream>
#include <utility>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "pipeline/dependency.h"
#include "pipeline/pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "util/mem_info.h"
#include "util/uid_util.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris {

class DelayReleaseToken : public Runnable {
    ENABLE_FACTORY_CREATOR(DelayReleaseToken);

public:
    DelayReleaseToken(std::unique_ptr<ThreadPoolToken>&& token) { token_ = std::move(token); }
    ~DelayReleaseToken() override = default;
    void run() override {}
    std::unique_ptr<ThreadPoolToken> token_;
};

QueryContext::QueryContext(TUniqueId query_id, int total_fragment_num, ExecEnv* exec_env,
                           const TQueryOptions& query_options, TNetworkAddress coord_addr,
                           bool is_pipeline, bool is_nereids)
        : fragment_num(total_fragment_num),
          timeout_second(-1),
          _query_id(query_id),
          _exec_env(exec_env),
          _is_pipeline(is_pipeline),
          _is_nereids(is_nereids),
          _query_options(query_options) {
    _init_query_mem_tracker();
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(query_mem_tracker);
    this->coord_addr = coord_addr;
    _start_time = VecDateTimeValue::local_time();
    _shared_hash_table_controller.reset(new vectorized::SharedHashTableController());
    _shared_scanner_controller.reset(new vectorized::SharedScannerController());
    _execution_dependency =
            pipeline::Dependency::create_unique(-1, -1, "ExecutionDependency", this);
    _runtime_filter_mgr = std::make_unique<RuntimeFilterMgr>(
            TUniqueId(), RuntimeFilterParamsContext::create(this), query_mem_tracker);

    timeout_second = query_options.execution_timeout;

    register_memory_statistics();
    register_cpu_statistics();
}

void QueryContext::_init_query_mem_tracker() {
    bool has_query_mem_limit = _query_options.__isset.mem_limit && (_query_options.mem_limit > 0);
    int64_t _bytes_limit = has_query_mem_limit ? _query_options.mem_limit : -1;
    if (_bytes_limit > MemInfo::mem_limit()) {
        VLOG_NOTICE << "Query memory limit " << PrettyPrinter::print(_bytes_limit, TUnit::BYTES)
                    << " exceeds process memory limit of "
                    << PrettyPrinter::print(MemInfo::mem_limit(), TUnit::BYTES)
                    << ". Using process memory limit instead";
        _bytes_limit = MemInfo::mem_limit();
    }
    if (_query_options.query_type == TQueryType::SELECT) {
        query_mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::QUERY, fmt::format("Query#Id={}", print_id(_query_id)),
                _bytes_limit);
    } else if (_query_options.query_type == TQueryType::LOAD) {
        query_mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::LOAD, fmt::format("Load#Id={}", print_id(_query_id)),
                _bytes_limit);
    } else { // EXTERNAL
        query_mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::LOAD, fmt::format("External#Id={}", print_id(_query_id)),
                _bytes_limit);
    }
    if (_query_options.__isset.is_report_success && _query_options.is_report_success) {
        query_mem_tracker->enable_print_log_usage();
    }
}

QueryContext::~QueryContext() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(query_mem_tracker);
    // query mem tracker consumption is equal to 0, it means that after QueryContext is created,
    // it is found that query already exists in _query_ctx_map, and query mem tracker is not used.
    // query mem tracker consumption is not equal to 0 after use, because there is memory consumed
    // on query mem tracker, released on other trackers.
    std::string mem_tracker_msg;
    if (query_mem_tracker->peak_consumption() != 0) {
        mem_tracker_msg = fmt::format(
                ", deregister query/load memory tracker, queryId={}, Limit={}, CurrUsed={}, "
                "PeakUsed={}",
                print_id(_query_id), MemTracker::print_bytes(query_mem_tracker->limit()),
                MemTracker::print_bytes(query_mem_tracker->consumption()),
                MemTracker::print_bytes(query_mem_tracker->peak_consumption()));
    }
    uint64_t group_id = 0;
    if (_workload_group) {
        group_id = _workload_group->id(); // before remove
        _workload_group->remove_mem_tracker_limiter(query_mem_tracker);
        _workload_group->remove_query(_query_id);
    }

    _exec_env->runtime_query_statistics_mgr()->set_query_finished(print_id(_query_id));

    if (enable_profile()) {
        _report_query_profile();
    }

    // Not release the the thread token in query context's dector method, because the query
    // conext may be dectored in the thread token it self. It is very dangerous and may core.
    // And also thread token need shutdown, it may take some time, may cause the thread that
    // release the token hang, the thread maybe a pipeline task scheduler thread.
    if (_thread_token) {
        Status submit_st = ExecEnv::GetInstance()->lazy_release_obj_pool()->submit(
                DelayReleaseToken::create_shared(std::move(_thread_token)));
        if (!submit_st.ok()) {
            LOG(WARNING) << "Failed to release query context thread token, query_id "
                         << print_id(_query_id) << ", error status " << submit_st;
        }
    }

    //TODO: check if pipeline and tracing both enabled
    if (_is_pipeline && ExecEnv::GetInstance()->pipeline_tracer_context()->enabled()) [[unlikely]] {
        try {
            ExecEnv::GetInstance()->pipeline_tracer_context()->end_query(_query_id, group_id);
        } catch (std::exception& e) {
            LOG(WARNING) << "Dump trace log failed bacause " << e.what();
        }
    }
    _runtime_filter_mgr.reset();
    _execution_dependency.reset();
    _shared_hash_table_controller.reset();
    _shared_scanner_controller.reset();
    _runtime_predicates.clear();
    file_scan_range_params_map.clear();
    obj_pool.clear();

    _exec_env->spill_stream_mgr()->async_cleanup_query(_query_id);

    LOG_INFO("Query {} deconstructed, {}", print_id(this->_query_id), mem_tracker_msg);
}

void QueryContext::set_ready_to_execute(bool is_cancelled) {
    set_execution_dependency_ready();
    {
        std::lock_guard<std::mutex> l(_start_lock);
        if (!_is_cancelled) {
            _is_cancelled = is_cancelled;
        }
        _ready_to_execute = true;
    }
    if (query_mem_tracker && is_cancelled) {
        query_mem_tracker->set_is_query_cancelled(is_cancelled);
    }
    _start_cond.notify_all();
}

void QueryContext::set_ready_to_execute_only() {
    set_execution_dependency_ready();
    {
        std::lock_guard<std::mutex> l(_start_lock);
        _ready_to_execute = true;
    }
    _start_cond.notify_all();
}

void QueryContext::set_execution_dependency_ready() {
    _execution_dependency->set_ready();
}

void QueryContext::cancel(std::string msg, Status new_status, int fragment_id) {
    // we must get this wrong status once query ctx's `_is_cancelled` = true.
    set_exec_status(new_status);
    // Just for CAS need a left value
    bool false_cancel = false;
    if (!_is_cancelled.compare_exchange_strong(false_cancel, true)) {
        return;
    }
    DCHECK(!false_cancel && _is_cancelled);

    set_ready_to_execute(true);
    std::vector<std::weak_ptr<pipeline::PipelineFragmentContext>> ctx_to_cancel;
    {
        std::lock_guard<std::mutex> lock(_pipeline_map_write_lock);
        for (auto& [f_id, f_context] : _fragment_id_to_pipeline_ctx) {
            if (fragment_id == f_id) {
                continue;
            }
            ctx_to_cancel.push_back(f_context);
        }
    }
    // Must not add lock here. There maybe dead lock because it will call fragment
    // ctx cancel and fragment ctx will call query ctx cancel.
    for (auto& f_context : ctx_to_cancel) {
        if (auto pipeline_ctx = f_context.lock()) {
            pipeline_ctx->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, msg);
        }
    }
}

void QueryContext::cancel_all_pipeline_context(const PPlanFragmentCancelReason& reason,
                                               const std::string& msg) {
    std::vector<std::weak_ptr<pipeline::PipelineFragmentContext>> ctx_to_cancel;
    {
        std::lock_guard<std::mutex> lock(_pipeline_map_write_lock);
        for (auto& [f_id, f_context] : _fragment_id_to_pipeline_ctx) {
            ctx_to_cancel.push_back(f_context);
        }
    }
    for (auto& f_context : ctx_to_cancel) {
        if (auto pipeline_ctx = f_context.lock()) {
            pipeline_ctx->cancel(reason, msg);
        }
    }
}

Status QueryContext::cancel_pipeline_context(const int fragment_id,
                                             const PPlanFragmentCancelReason& reason,
                                             const std::string& msg) {
    std::weak_ptr<pipeline::PipelineFragmentContext> ctx_to_cancel;
    {
        std::lock_guard<std::mutex> lock(_pipeline_map_write_lock);
        if (!_fragment_id_to_pipeline_ctx.contains(fragment_id)) {
            return Status::InternalError("fragment_id_to_pipeline_ctx is empty!");
        }
        ctx_to_cancel = _fragment_id_to_pipeline_ctx[fragment_id];
    }
    if (auto pipeline_ctx = ctx_to_cancel.lock()) {
        pipeline_ctx->cancel(reason, msg);
    }
    return Status::OK();
}

void QueryContext::set_pipeline_context(
        const int fragment_id, std::shared_ptr<pipeline::PipelineFragmentContext> pip_ctx) {
    std::lock_guard<std::mutex> lock(_pipeline_map_write_lock);
    _fragment_id_to_pipeline_ctx.insert({fragment_id, pip_ctx});
}

void QueryContext::register_query_statistics(std::shared_ptr<QueryStatistics> qs) {
    _exec_env->runtime_query_statistics_mgr()->register_query_statistics(print_id(_query_id), qs,
                                                                         coord_addr);
}

std::shared_ptr<QueryStatistics> QueryContext::get_query_statistics() {
    return _exec_env->runtime_query_statistics_mgr()->get_runtime_query_statistics(
            print_id(_query_id));
}

void QueryContext::register_memory_statistics() {
    if (query_mem_tracker) {
        std::shared_ptr<QueryStatistics> qs = query_mem_tracker->get_query_statistics();
        std::string query_id = print_id(_query_id);
        if (qs) {
            _exec_env->runtime_query_statistics_mgr()->register_query_statistics(query_id, qs,
                                                                                 coord_addr);
        } else {
            LOG(INFO) << " query " << query_id << " get memory query statistics failed ";
        }
    }
}

void QueryContext::register_cpu_statistics() {
    if (!_cpu_statistics) {
        _cpu_statistics = std::make_shared<QueryStatistics>();
        _exec_env->runtime_query_statistics_mgr()->register_query_statistics(
                print_id(_query_id), _cpu_statistics, coord_addr);
    }
}

doris::pipeline::TaskScheduler* QueryContext::get_pipe_exec_scheduler() {
    if (_workload_group) {
        if (_task_scheduler) {
            return _task_scheduler;
        }
    }
    return _exec_env->pipeline_task_scheduler();
}

ThreadPool* QueryContext::get_non_pipe_exec_thread_pool() {
    if (_workload_group) {
        return _non_pipe_thread_pool;
    } else {
        return nullptr;
    }
}

Status QueryContext::set_workload_group(WorkloadGroupPtr& tg) {
    _workload_group = tg;
    // Should add query first, then the workload group will not be deleted.
    // see task_group_manager::delete_workload_group_by_ids
    _workload_group->add_mem_tracker_limiter(query_mem_tracker);
    _workload_group->get_query_scheduler(&_task_scheduler, &_scan_task_scheduler,
                                         &_non_pipe_thread_pool, &_remote_scan_task_scheduler);
    return Status::OK();
}

void QueryContext::add_fragment_profile_x(
        int fragment_id, const std::vector<std::shared_ptr<TRuntimeProfileTree>>& pipeline_profiles,
        std::shared_ptr<TRuntimeProfileTree> load_channel_profile) {
    if (pipeline_profiles.empty()) {
        std::string msg = fmt::format("Add pipeline profile failed, query {}, fragment {}",
                                      print_id(this->_query_id), fragment_id);
        LOG_ERROR(msg);
        DCHECK(false) << msg;
        return;
    }

#ifndef NDEBUG
    for (const auto& p : pipeline_profiles) {
        DCHECK(p != nullptr) << fmt::format("Add pipeline profile failed, query {}, fragment {}",
                                            print_id(this->_query_id), fragment_id);
    }
#endif

    std::lock_guard<std::mutex> l(_profile_mutex);
    LOG_INFO("Query X add fragment profile, query {}, fragment {}, pipeline profile count {} ",
             print_id(this->_query_id), fragment_id, pipeline_profiles.size());

    _profile_map_x.insert(std::make_pair(fragment_id, pipeline_profiles));

    if (load_channel_profile != nullptr) {
        _load_channel_profile_map_x.insert(std::make_pair(fragment_id, load_channel_profile));
    }
}

void QueryContext::add_instance_profile(const TUniqueId& instance_id,
                                        std::shared_ptr<TRuntimeProfileTree> profile,
                                        std::shared_ptr<TRuntimeProfileTree> load_channel_profile) {
    DCHECK(profile != nullptr) << print_id(instance_id);

    std::lock_guard<std::mutex> lg(_profile_mutex);
    _profile_map.insert(std::make_pair(instance_id, profile));
    if (load_channel_profile != nullptr) {
        _load_channel_profile_map.insert(std::make_pair(instance_id, load_channel_profile));
    }
}

void QueryContext::_report_query_profile() {
    _report_query_profile_x();
    _report_query_profile_non_pipeline();
}

void QueryContext::_report_query_profile_non_pipeline() {
    if (enable_pipeline_x_exec()) {
        return;
    }

    std::lock_guard<std::mutex> lg(_profile_mutex);
    LOG_INFO("Query {}, register query profile, instance profile count {}", print_id(_query_id),
             _profile_map.size());

    for (auto& [instance_id, instance_profile] : _profile_map) {
        std::shared_ptr<TRuntimeProfileTree> load_channel_profile = nullptr;
        if (_load_channel_profile_map.contains(instance_id)) {
            load_channel_profile = _load_channel_profile_map[instance_id];
        }

        ExecEnv::GetInstance()->runtime_query_statistics_mgr()->register_instance_profile(
                _query_id, this->coord_addr, instance_id, instance_profile, load_channel_profile);
    }

    ExecEnv::GetInstance()->runtime_query_statistics_mgr()->trigger_report_profile();
}

void QueryContext::_report_query_profile_x() {
    if (!enable_pipeline_x_exec()) {
        return;
    }

    std::lock_guard<std::mutex> lg(_profile_mutex);
    LOG_INFO(
            "Pipeline x query context, register query profile, query {}, fragment profile count {}",
            print_id(_query_id), _profile_map_x.size());

    for (auto& [fragment_id, fragment_profile] : _profile_map_x) {
        std::shared_ptr<TRuntimeProfileTree> load_channel_profile = nullptr;

        if (_load_channel_profile_map_x.contains(fragment_id)) {
            load_channel_profile = _load_channel_profile_map_x[fragment_id];
        }

        ExecEnv::GetInstance()->runtime_query_statistics_mgr()->register_fragment_profile_x(
                _query_id, this->coord_addr, fragment_id, fragment_profile, load_channel_profile);
    }

    ExecEnv::GetInstance()->runtime_query_statistics_mgr()->trigger_report_profile();
}

std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>>
QueryContext::_collect_realtime_query_profile_x() const {
    std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>> res;

    if (!enable_pipeline_x_exec()) {
        return res;
    }

    for (auto& [fragment_id, fragment_ctx_wptr] : _fragment_id_to_pipeline_ctx) {
        if (auto fragment_ctx = fragment_ctx_wptr.lock()) {
            if (fragment_ctx == nullptr) {
                std::string msg =
                        fmt::format("PipelineFragmentContext is nullptr, query {} fragment_id: {}",
                                    print_id(_query_id), fragment_id);
                LOG_ERROR(msg);
                DCHECK(false) << msg;
                continue;
            }

            auto profile = fragment_ctx->collect_realtime_profile_x();

            if (profile.empty()) {
                std::string err_msg = fmt::format(
                        "Get nothing when collecting profile, query {}, fragment_id: {}",
                        print_id(_query_id), fragment_id);
                LOG_ERROR(err_msg);
                DCHECK(false) << err_msg;
                continue;
            }

            res.insert(std::make_pair(fragment_id, profile));
        }
    }

    return res;
}

TReportExecStatusParams QueryContext::get_realtime_exec_status_x() const {
    TReportExecStatusParams exec_status;

    if (enable_pipeline_x_exec()) {
        auto realtime_query_profile = _collect_realtime_query_profile_x();
        std::vector<std::shared_ptr<TRuntimeProfileTree>> load_channel_profiles;

        for (auto load_channel_profile : _load_channel_profile_map_x) {
            if (load_channel_profile.second != nullptr) {
                load_channel_profiles.push_back(load_channel_profile.second);
            }
        }

        exec_status = RuntimeQueryStatiticsMgr::create_report_exec_status_params_x(
                this->_query_id, realtime_query_profile, load_channel_profiles);
    } else {
        auto msg = fmt::format("Query {} is not pipelineX query", print_id(_query_id));
        LOG_ERROR(msg);
        DCHECK(false) << msg;
    }

    return exec_status;
}

} // namespace doris
