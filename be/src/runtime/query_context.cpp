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
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "pipeline/dependency.h"
#include "pipeline/pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/heap_profiler.h"
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

std::unique_ptr<TaskController> QueryContext::QueryTaskController::create(QueryContext* query_ctx) {
    return QueryContext::QueryTaskController::create_unique(query_ctx->shared_from_this());
}

bool QueryContext::QueryTaskController::is_cancelled() const {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return true;
    }
    return query_ctx->is_cancelled();
}

Status QueryContext::QueryTaskController::cancel(const Status& reason, int fragment_id) {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return Status::InternalError("QueryContext is destroyed");
    }
    query_ctx->cancel(reason, fragment_id);
    return Status::OK();
}

std::unique_ptr<MemoryContext> QueryContext::QueryMemoryContext::create() {
    return QueryContext::QueryMemoryContext::create_unique();
}

std::shared_ptr<QueryContext> QueryContext::create(TUniqueId query_id, ExecEnv* exec_env,
                                                   const TQueryOptions& query_options,
                                                   TNetworkAddress coord_addr, bool is_nereids,
                                                   TNetworkAddress current_connect_fe,
                                                   QuerySource query_type) {
    auto ctx = QueryContext::create_shared(query_id, exec_env, query_options, coord_addr,
                                           is_nereids, current_connect_fe, query_type);
    ctx->init_query_task_controller();
    return ctx;
}

QueryContext::QueryContext(TUniqueId query_id, ExecEnv* exec_env,
                           const TQueryOptions& query_options, TNetworkAddress coord_addr,
                           bool is_nereids, TNetworkAddress current_connect_fe,
                           QuerySource query_source)
        : _timeout_second(-1),
          _query_id(std::move(query_id)),
          _exec_env(exec_env),
          _is_nereids(is_nereids),
          _query_options(query_options),
          _query_source(query_source) {
    _init_resource_context();
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(query_mem_tracker());
    _query_watcher.start();
    _shared_hash_table_controller.reset(new vectorized::SharedHashTableController());
    _execution_dependency = pipeline::QueryGlobalDependency::create_unique("ExecutionDependency");
    _memory_sufficient_dependency =
            pipeline::QueryGlobalDependency::create_unique("MemorySufficientDependency", true);

    _runtime_filter_mgr = std::make_unique<RuntimeFilterMgr>(
            TUniqueId(), RuntimeFilterParamsContext::create(this), query_mem_tracker(), true);

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
    DorisMetrics::instance()->query_ctx_cnt->increment(1);
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
    std::shared_ptr<MemTrackerLimiter> query_mem_tracker;
    if (_query_options.query_type == TQueryType::SELECT) {
        query_mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::QUERY, fmt::format("Query#Id={}", print_id(_query_id)),
                bytes_limit);
    } else if (_query_options.query_type == TQueryType::LOAD) {
        query_mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::LOAD, fmt::format("Load#Id={}", print_id(_query_id)),
                bytes_limit);
    } else if (_query_options.query_type == TQueryType::EXTERNAL) { // spark/flink/etc..
        query_mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::QUERY, fmt::format("External#Id={}", print_id(_query_id)),
                bytes_limit);
    } else {
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }
    if (_query_options.__isset.is_report_success && _query_options.is_report_success) {
        query_mem_tracker->enable_print_log_usage();
    }

    query_mem_tracker->set_enable_reserve_memory(_query_options.__isset.enable_reserve_memory &&
                                                 _query_options.enable_reserve_memory);
    _user_set_mem_limit = bytes_limit;
    _adjusted_mem_limit = bytes_limit;

    _resource_ctx->memory_context()->set_mem_tracker(query_mem_tracker);
}

void QueryContext::_init_resource_context() {
    _resource_ctx = ResourceContext::create_shared();
    _resource_ctx->set_memory_context(QueryContext::QueryMemoryContext::create());
    _init_query_mem_tracker();
}

void QueryContext::init_query_task_controller() {
    _resource_ctx->set_task_controller(QueryContext::QueryTaskController::create(this));
    _resource_ctx->task_controller()->set_task_id(_query_id);
    _resource_ctx->task_controller()->set_fe_addr(current_connect_fe);
    _resource_ctx->task_controller()->set_query_type(_query_options.query_type);
#ifndef BE_TEST
    _exec_env->runtime_query_statistics_mgr()->register_resource_context(print_id(_query_id),
                                                                         _resource_ctx);
#endif
}

QueryContext::~QueryContext() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(query_mem_tracker());
    // query mem tracker consumption is equal to 0, it means that after QueryContext is created,
    // it is found that query already exists in _query_ctx_map, and query mem tracker is not used.
    // query mem tracker consumption is not equal to 0 after use, because there is memory consumed
    // on query mem tracker, released on other trackers.
    std::string mem_tracker_msg;
    if (query_mem_tracker()->peak_consumption() != 0) {
        mem_tracker_msg = fmt::format(
                "deregister query/load memory tracker, queryId={}, Limit={}, CurrUsed={}, "
                "PeakUsed={}",
                print_id(_query_id), MemCounter::print_bytes(query_mem_tracker()->limit()),
                MemCounter::print_bytes(query_mem_tracker()->consumption()),
                MemCounter::print_bytes(query_mem_tracker()->peak_consumption()));
    }
    [[maybe_unused]] uint64_t group_id = 0;
    if (workload_group()) {
        group_id = workload_group()->id(); // before remove
    }

    _resource_ctx->task_controller()->finish();

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
#ifndef BE_TEST
    if (ExecEnv::GetInstance()->pipeline_tracer_context()->enabled()) [[unlikely]] {
        try {
            ExecEnv::GetInstance()->pipeline_tracer_context()->end_query(_query_id, group_id);
        } catch (std::exception& e) {
            LOG(WARNING) << "Dump trace log failed bacause " << e.what();
        }
    }
#endif
    _runtime_filter_mgr.reset();
    _execution_dependency.reset();
    _shared_hash_table_controller.reset();
    _runtime_predicates.clear();
    file_scan_range_params_map.clear();
    obj_pool.clear();
    _merge_controller_handler.reset();

#ifndef BE_TEST
    _exec_env->spill_stream_mgr()->async_cleanup_query(_query_id);
#endif
    DorisMetrics::instance()->query_ctx_cnt->increment(-1);
    // the only one msg shows query's end. any other msg should append to it if need.
    LOG_INFO("Query {} deconstructed, mem_tracker: {}", print_id(this->_query_id), mem_tracker_msg);
}

void QueryContext::set_ready_to_execute(Status reason) {
    set_execution_dependency_ready();
    _exec_status.update(reason);
    if (query_mem_tracker() && !reason.ok()) {
        query_mem_tracker()->set_is_query_cancelled(!reason.ok());
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
            _memory_sufficient_dependency->set_ready();
            std::lock_guard l(_paused_mutex);
            _paused_reason = Status::OK();
        }
    } else {
        _memory_sufficient_dependency->block();
        ++_paused_count;
    }
}

void QueryContext::cancel(Status new_status, int fragment_id) {
    if (!_exec_status.update(new_status)) {
        return;
    }
    // Tasks should be always runnable.
    _execution_dependency->set_always_ready();
    _memory_sufficient_dependency->set_always_ready();
    if ((new_status.is<ErrorCode::MEM_LIMIT_EXCEEDED>() ||
         new_status.is<ErrorCode::MEM_ALLOC_FAILED>()) &&
        _query_options.__isset.dump_heap_profile_when_mem_limit_exceeded &&
        _query_options.dump_heap_profile_when_mem_limit_exceeded) {
        // if query is cancelled because of query mem limit exceeded, dump heap profile
        // at the time of cancellation can get the most accurate memory usage for problem analysis
        auto wg = workload_group();
        auto log_str = fmt::format(
                "Query {} canceled because of memory limit exceeded, dumping memory "
                "detail profiles. wg: {}. {}",
                print_id(_query_id), wg ? wg->debug_string() : "null",
                doris::ProcessProfile::instance()->memory_profile()->process_memory_detail_str());
        LOG_LONG_STRING(INFO, log_str);
        std::string dot = HeapProfiler::instance()->dump_heap_profile_to_dot();
        if (!dot.empty()) {
            dot += "\n-------------------------------------------------------\n";
            dot += "Copy the text after `digraph` in the above output to "
                   "http://www.webgraphviz.com to generate a dot graph.\n"
                   "after start heap profiler, if there is no operation, will print `No nodes "
                   "to "
                   "print`."
                   "If there are many errors: `addr2line: Dwarf Error`,"
                   "or other FAQ, reference doc: "
                   "https://doris.apache.org/community/developer-guide/debug-tool/#4-qa\n";
            auto log_str =
                    fmt::format("Query {}, dump heap profile to dot: {}", print_id(_query_id), dot);
            LOG_LONG_STRING(INFO, log_str);
        }
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

doris::pipeline::TaskScheduler* QueryContext::get_pipe_exec_scheduler() {
    if (workload_group()) {
        if (_task_scheduler) {
            return _task_scheduler;
        }
    }
    return _exec_env->pipeline_task_scheduler();
}

ThreadPool* QueryContext::get_memtable_flush_pool() {
    if (workload_group()) {
        return _memtable_flush_pool;
    } else {
        return nullptr;
    }
}

void QueryContext::set_workload_group(WorkloadGroupPtr& wg) {
    _resource_ctx->set_workload_group(wg);
    // Should add query first, then the workload group will not be deleted.
    // see task_group_manager::delete_workload_group_by_ids
    workload_group()->add_mem_tracker_limiter(query_mem_tracker());
    workload_group()->get_query_scheduler(&_task_scheduler, &_scan_task_scheduler,
                                          &_memtable_flush_pool, &_remote_scan_task_scheduler);
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
    VLOG_ROW << fmt::format(
            "Query add fragment profile, query {}, fragment {}, pipeline profile count {} ",
            print_id(this->_query_id), fragment_id, pipeline_profiles.size());

    _profile_map.insert(std::make_pair(fragment_id, pipeline_profiles));

    if (load_channel_profile != nullptr) {
        _load_channel_profile_map.insert(std::make_pair(fragment_id, load_channel_profile));
    }
}

void QueryContext::_report_query_profile() {
    std::lock_guard<std::mutex> lg(_profile_mutex);

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

    *memory_usage = query_mem_tracker()->consumption();
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
    const auto target_revoking_size = (int64_t)(query_mem_tracker()->consumption() * 0.2);
    size_t revoked_size = 0;
    size_t total_revokable_size = 0;

    std::vector<pipeline::PipelineTask*> chosen_tasks;
    for (auto&& [revocable_size, task] : tasks) {
        // Only revoke the largest task to ensure memory is used as much as possible
        // break;
        if (revoked_size < target_revoking_size) {
            chosen_tasks.emplace_back(task);
            revoked_size += revocable_size;
        }
        total_revokable_size += revocable_size;
    }

    std::weak_ptr<QueryContext> this_ctx = shared_from_this();
    auto spill_context = std::make_shared<pipeline::SpillContext>(
            chosen_tasks.size(), _query_id, [this_ctx](pipeline::SpillContext* context) {
                auto query_context = this_ctx.lock();
                if (!query_context) {
                    return;
                }

                LOG(INFO) << query_context->debug_string() << ", context: " << ((void*)context)
                          << " all spill tasks done, resume it.";
                query_context->set_memory_sufficient(true);
            });

    LOG(INFO) << fmt::format(
            "{}, spill context: {}, revokable mem: {}/{}, tasks count: {}/{}", this->debug_string(),
            ((void*)spill_context.get()), PrettyPrinter::print_bytes(revoked_size),
            PrettyPrinter::print_bytes(total_revokable_size), chosen_tasks.size(), tasks.size());

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
            "QueryId={}, Memory [Used={}, Limit={}, Peak={}], Spill[RunningSpillTaskCnt={}, "
            "TotalPausedPeriodSecs={}, LatestPausedReason={}]",
            print_id(_query_id),
            PrettyPrinter::print(query_mem_tracker()->consumption(), TUnit::BYTES),
            PrettyPrinter::print(query_mem_tracker()->limit(), TUnit::BYTES),
            PrettyPrinter::print(query_mem_tracker()->peak_consumption(), TUnit::BYTES),
            _revoking_tasks_count,
            _memory_sufficient_dependency->watcher_elapse_time() / NANOS_PER_SEC,
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
