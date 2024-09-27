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

#include <algorithm>
#include <exception>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
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

const std::string toString(QuerySource queryType) {
    switch (queryType) {
    case QuerySource::INTERNAL_FRONTEND:
        return "INTERNAL_FRONTEND";
    case QuerySource::STREAM_LOAD:
        return "STREAM_LOAD";
    case QuerySource::GROUP_COMMIT_LOAD:
        return "EXTERNAL_QUERY";
    case QuerySource::ROUTINE_LOAD:
        return "ROUTINE_LOAD";
    case QuerySource::EXTERNAL_CONNECTOR:
        return "EXTERNAL_CONNECTOR";
    default:
        return "UNKNOWN";
    }
}

QueryContext::QueryContext(TUniqueId query_id, ExecEnv* exec_env,
                           const TQueryOptions& query_options, TNetworkAddress coord_addr,
                           bool is_pipeline, bool is_nereids, TNetworkAddress current_connect_fe,
                           QuerySource query_source)
        : _timeout_second(-1),
          _query_id(std::move(query_id)),
          _exec_env(exec_env),
          _is_pipeline(is_pipeline),
          _is_nereids(is_nereids),
          _query_options(query_options),
          _query_source(query_source) {
    _init_query_mem_tracker();
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(query_mem_tracker);
    _query_watcher.start();
    _shared_hash_table_controller.reset(new vectorized::SharedHashTableController());
    _execution_dependency = pipeline::Dependency::create_unique(-1, -1, "ExecutionDependency");
    _memory_sufficient_dependency =
            pipeline::Dependency::create_unique(-1, -1, "MemorySufficientDependency", true);

    _runtime_filter_mgr = std::make_unique<RuntimeFilterMgr>(
            TUniqueId(), RuntimeFilterParamsContext::create(this), query_mem_tracker);

    _timeout_second = query_options.execution_timeout;

    bool is_query_type_valid = query_options.query_type == TQueryType::SELECT ||
                               query_options.query_type == TQueryType::LOAD ||
                               query_options.query_type == TQueryType::EXTERNAL;
    DCHECK_EQ(is_query_type_valid, true);

    this->coord_addr = coord_addr;
    // current_connect_fe is used for report query statistics
    this->current_connect_fe = current_connect_fe;
    // external query has no current_connect_fe
    if (query_options.query_type != TQueryType::EXTERNAL) {
        bool is_report_fe_addr_valid =
                !this->current_connect_fe.hostname.empty() && this->current_connect_fe.port != 0;
        DCHECK_EQ(is_report_fe_addr_valid, true);
    }
    clock_gettime(CLOCK_MONOTONIC, &this->_query_arrival_timestamp);
    register_memory_statistics();
    register_cpu_statistics();
}

void QueryContext::_init_query_mem_tracker() {
    bool has_query_mem_limit = _query_options.__isset.mem_limit && (_query_options.mem_limit > 0);
    int64_t bytes_limit = has_query_mem_limit ? _query_options.mem_limit : -1;
    if (bytes_limit > MemInfo::mem_limit() || bytes_limit == -1) {
        VLOG_NOTICE << "Query memory limit " << PrettyPrinter::print(bytes_limit, TUnit::BYTES)
                    << " exceeds process memory limit of "
                    << PrettyPrinter::print(MemInfo::mem_limit(), TUnit::BYTES)
                    << " OR is -1. Using process memory limit instead.";
        bytes_limit = MemInfo::mem_limit();
    }
    // If the query is a pure load task(streamload, routine load, group commit), then it should not use
    // memlimit per query to limit their memory usage.
    if (is_pure_load_task()) {
        bytes_limit = MemInfo::mem_limit();
    }
    if (_query_options.query_type == TQueryType::SELECT) {
        query_mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::QUERY, fmt::format("Query#Id={}", print_id(_query_id)),
                bytes_limit);
    } else if (_query_options.query_type == TQueryType::LOAD) {
        query_mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::LOAD, fmt::format("Load#Id={}", print_id(_query_id)),
                bytes_limit);
    } else { // EXTERNAL
        query_mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::LOAD, fmt::format("External#Id={}", print_id(_query_id)),
                bytes_limit);
    }
    if (_query_options.__isset.is_report_success && _query_options.is_report_success) {
        query_mem_tracker->enable_print_log_usage();
    }
    _user_set_mem_limit = bytes_limit;
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
                ", deregister query/load memory tracker, queryId={}, CurrUsed={}, "
                "PeakUsed={}",
                print_id(_query_id), MemCounter::print_bytes(query_mem_tracker->limit()),
                MemCounter::print_bytes(query_mem_tracker->consumption()),
                MemCounter::print_bytes(query_mem_tracker->peak_consumption()));
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
    _memory_sufficient_dependency.reset();
    _shared_hash_table_controller.reset();
    _runtime_predicates.clear();
    file_scan_range_params_map.clear();
    obj_pool.clear();
    _merge_controller_handler.reset();

    _exec_env->spill_stream_mgr()->async_cleanup_query(_query_id);

    LOG_INFO("Query {} deconstructed, {}", print_id(this->_query_id), mem_tracker_msg);
}

void QueryContext::set_ready_to_execute(Status reason) {
    set_execution_dependency_ready();
    _exec_status.update(reason);
    if (query_mem_tracker && !reason.ok()) {
        query_mem_tracker->set_is_query_cancelled(!reason.ok());
    }
}

void QueryContext::set_ready_to_execute_only() {
    set_execution_dependency_ready();
}

void QueryContext::set_execution_dependency_ready() {
    _execution_dependency->set_ready();
}

void QueryContext::set_memory_sufficient(bool sufficient) {
    if (sufficient) {
        {
            std::lock_guard l(_paused_mutex);
            _paused_reason = Status::OK();
        }
        _memory_sufficient_dependency->set_ready();
    } else {
        _memory_sufficient_dependency->block();
    }
}

void QueryContext::cancel(Status new_status, int fragment_id) {
    if (!_exec_status.update(new_status)) {
        return;
    }

    set_ready_to_execute(new_status);
    cancel_all_pipeline_context(new_status, fragment_id);
}

void QueryContext::cancel_all_pipeline_context(const Status& reason, int fragment_id) {
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
    for (auto& f_context : ctx_to_cancel) {
        if (auto pipeline_ctx = f_context.lock()) {
            pipeline_ctx->cancel(reason);
        }
    }
}

std::string QueryContext::print_all_pipeline_context() {
    std::vector<std::weak_ptr<pipeline::PipelineFragmentContext>> ctx_to_print;
    fmt::memory_buffer debug_string_buffer;
    size_t i = 0;
    {
        fmt::format_to(debug_string_buffer, "{} pipeline fragment contexts in query {}. \n",
                       _fragment_id_to_pipeline_ctx.size(), print_id(_query_id));

        {
            std::lock_guard<std::mutex> lock(_pipeline_map_write_lock);
            for (auto& [f_id, f_context] : _fragment_id_to_pipeline_ctx) {
                ctx_to_print.push_back(f_context);
            }
        }
        for (auto& f_context : ctx_to_print) {
            if (auto pipeline_ctx = f_context.lock()) {
                auto elapsed = pipeline_ctx->elapsed_time() / 1000000000.0;
                fmt::format_to(debug_string_buffer,
                               "No.{} (elapse_second={}s, fragment_id={}) : {}\n", i, elapsed,
                               pipeline_ctx->get_fragment_id(), pipeline_ctx->debug_string());
                i++;
            }
        }
    }
    return fmt::to_string(debug_string_buffer);
}

void QueryContext::set_pipeline_context(
        const int fragment_id, std::shared_ptr<pipeline::PipelineFragmentContext> pip_ctx) {
    std::lock_guard<std::mutex> lock(_pipeline_map_write_lock);
    _fragment_id_to_pipeline_ctx.insert({fragment_id, pip_ctx});
}

void QueryContext::register_query_statistics(std::shared_ptr<QueryStatistics> qs) {
    _exec_env->runtime_query_statistics_mgr()->register_query_statistics(
            print_id(_query_id), qs, current_connect_fe, _query_options.query_type);
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
            _exec_env->runtime_query_statistics_mgr()->register_query_statistics(
                    query_id, qs, current_connect_fe, _query_options.query_type);
        } else {
            LOG(INFO) << " query " << query_id << " get memory query statistics failed ";
        }
    }
}

void QueryContext::register_cpu_statistics() {
    if (!_cpu_statistics) {
        _cpu_statistics = std::make_shared<QueryStatistics>();
        _exec_env->runtime_query_statistics_mgr()->register_query_statistics(
                print_id(_query_id), _cpu_statistics, current_connect_fe,
                _query_options.query_type);
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

ThreadPool* QueryContext::get_memtable_flush_pool() {
    if (_workload_group) {
        return _memtable_flush_pool;
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
                                         &_memtable_flush_pool, &_remote_scan_task_scheduler);
    return Status::OK();
}

void QueryContext::add_fragment_profile(
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

    _profile_map.insert(std::make_pair(fragment_id, pipeline_profiles));

    if (load_channel_profile != nullptr) {
        _load_channel_profile_map.insert(std::make_pair(fragment_id, load_channel_profile));
    }
}

void QueryContext::_report_query_profile() {
    std::lock_guard<std::mutex> lg(_profile_mutex);
    LOG_INFO(
            "Pipeline x query context, register query profile, query {}, fragment profile count {}",
            print_id(_query_id), _profile_map.size());

    for (auto& [fragment_id, fragment_profile] : _profile_map) {
        std::shared_ptr<TRuntimeProfileTree> load_channel_profile = nullptr;

        if (_load_channel_profile_map.contains(fragment_id)) {
            load_channel_profile = _load_channel_profile_map[fragment_id];
        }

        ExecEnv::GetInstance()->runtime_query_statistics_mgr()->register_fragment_profile(
                _query_id, this->coord_addr, fragment_id, fragment_profile, load_channel_profile);
    }

    ExecEnv::GetInstance()->runtime_query_statistics_mgr()->trigger_report_profile();
}

void QueryContext::get_revocable_info(size_t* revocable_size, size_t* memory_usage,
                                      bool* has_running_task) const {
    *revocable_size = 0;
    for (auto&& [fragment_id, fragment_wptr] : _fragment_id_to_pipeline_ctx) {
        auto fragment_ctx = fragment_wptr.lock();
        if (!fragment_ctx) {
            continue;
        }

        *revocable_size += fragment_ctx->get_revocable_size(has_running_task);

        // Should wait for all tasks are not running before revoking memory.
        if (*has_running_task) {
            break;
        }
    }

    *memory_usage = query_mem_tracker->consumption();
}

size_t QueryContext::get_revocable_size() const {
    size_t revocable_size = 0;
    for (auto&& [fragment_id, fragment_wptr] : _fragment_id_to_pipeline_ctx) {
        auto fragment_ctx = fragment_wptr.lock();
        if (!fragment_ctx) {
            continue;
        }

        bool has_running_task = false;
        revocable_size += fragment_ctx->get_revocable_size(&has_running_task);

        // Should wait for all tasks are not running before revoking memory.
        if (has_running_task) {
            return 0;
        }
    }
    return revocable_size;
}

Status QueryContext::revoke_memory() {
    std::vector<std::pair<size_t, pipeline::PipelineTask*>> tasks;
    std::vector<std::shared_ptr<pipeline::PipelineFragmentContext>> fragments;
    for (auto&& [fragment_id, fragment_wptr] : _fragment_id_to_pipeline_ctx) {
        auto fragment_ctx = fragment_wptr.lock();
        if (!fragment_ctx) {
            continue;
        }

        auto tasks_of_fragment = fragment_ctx->get_revocable_tasks();
        for (auto* task : tasks_of_fragment) {
            tasks.emplace_back(task->get_revocable_size(), task);
        }
        fragments.emplace_back(std::move(fragment_ctx));
    }

    std::sort(tasks.begin(), tasks.end(), [](auto&& l, auto&& r) { return l.first > r.first; });

    // Do not use memlimit, use current memory usage.
    // For example, if current limit is 1.6G, but current used is 1G, if reserve failed
    // should free 200MB memory, not 300MB
    const auto target_revoking_size = query_mem_tracker->consumption() * 0.2;
    size_t revoked_size = 0;

    std::vector<pipeline::PipelineTask*> chosen_tasks;
    for (auto&& [revocable_size, task] : tasks) {
        chosen_tasks.emplace_back(task);

        revoked_size += revocable_size;
        if (revoked_size >= target_revoking_size) {
            break;
        }
    }

    std::weak_ptr<QueryContext> this_ctx = shared_from_this();
    auto spill_context = std::make_shared<pipeline::SpillContext>(
            chosen_tasks.size(), _query_id, [this_ctx](pipeline::SpillContext* context) {
                auto query_context = this_ctx.lock();
                if (!query_context) {
                    return;
                }

                LOG(INFO) << "query: " << print_id(query_context->_query_id)
                          << ", context: " << ((void*)context)
                          << " all revoking tasks done, resumt it.";
                query_context->set_memory_sufficient(true);
            });

    LOG(INFO) << "query: " << print_id(_query_id) << ", context: " << ((void*)spill_context.get())
              << " total revoked size: " << revoked_size << ", tasks count: " << chosen_tasks.size()
              << "/" << tasks.size();
    for (auto* task : chosen_tasks) {
        RETURN_IF_ERROR(task->revoke_memory(spill_context));
    }
    return Status::OK();
}

void QueryContext::decrease_revoking_tasks_count() {
    _revoking_tasks_count.fetch_sub(1);
}

std::vector<pipeline::PipelineTask*> QueryContext::get_revocable_tasks() const {
    std::vector<pipeline::PipelineTask*> tasks;
    for (auto&& [fragment_id, fragment_wptr] : _fragment_id_to_pipeline_ctx) {
        auto fragment_ctx = fragment_wptr.lock();
        if (!fragment_ctx) {
            continue;
        }
        auto tasks_of_fragment = fragment_ctx->get_revocable_tasks();
        tasks.insert(tasks.end(), tasks_of_fragment.cbegin(), tasks_of_fragment.cend());
    }
    return tasks;
}

std::string QueryContext::debug_string() {
    std::lock_guard l(_paused_mutex);
    return fmt::format(
            "MemTracker Label={}, Used={}, Limit={}, Peak={}, running revoke task count {}, "
            "MemorySufficient={}, PausedReason={}",
            query_mem_tracker->label(),
            PrettyPrinter::print(query_mem_tracker->consumption(), TUnit::BYTES),
            PrettyPrinter::print(query_mem_tracker->limit(), TUnit::BYTES),
            PrettyPrinter::print(query_mem_tracker->peak_consumption(), TUnit::BYTES),
            _revoking_tasks_count, _memory_sufficient_dependency->ready(),
            _paused_reason.to_string());
}

std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>>
QueryContext::_collect_realtime_query_profile() const {
    std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>> res;

    for (const auto& [fragment_id, fragment_ctx_wptr] : _fragment_id_to_pipeline_ctx) {
        if (auto fragment_ctx = fragment_ctx_wptr.lock()) {
            if (fragment_ctx == nullptr) {
                std::string msg =
                        fmt::format("PipelineFragmentContext is nullptr, query {} fragment_id: {}",
                                    print_id(_query_id), fragment_id);
                LOG_ERROR(msg);
                DCHECK(false) << msg;
                continue;
            }

            auto profile = fragment_ctx->collect_realtime_profile();

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

TReportExecStatusParams QueryContext::get_realtime_exec_status() const {
    TReportExecStatusParams exec_status;

    auto realtime_query_profile = _collect_realtime_query_profile();
    std::vector<std::shared_ptr<TRuntimeProfileTree>> load_channel_profiles;

    for (auto load_channel_profile : _load_channel_profile_map) {
        if (load_channel_profile.second != nullptr) {
            load_channel_profiles.push_back(load_channel_profile.second);
        }
    }

    exec_status = RuntimeQueryStatisticsMgr::create_report_exec_status_params(
            this->_query_id, std::move(realtime_query_profile), std::move(load_channel_profiles),
            /*is_done=*/false);

    return exec_status;
}

} // namespace doris
