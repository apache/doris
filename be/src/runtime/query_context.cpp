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

#include "pipeline/pipeline_fragment_context.h"
#include "pipeline/pipeline_x/dependency.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/task_group/task_group_manager.h"

namespace doris {

class DelayReleaseToken : public Runnable {
public:
    DelayReleaseToken(std::unique_ptr<ThreadPoolToken>&& token) { token_ = std::move(token); }
    ~DelayReleaseToken() override = default;
    void run() override {}
    std::unique_ptr<ThreadPoolToken> token_;
};

QueryContext::QueryContext(TUniqueId query_id, int total_fragment_num, ExecEnv* exec_env,
                           const TQueryOptions& query_options)
        : fragment_num(total_fragment_num),
          timeout_second(-1),
          _query_id(query_id),
          _exec_env(exec_env),
          _query_options(query_options) {
    _start_time = VecDateTimeValue::local_time();
    _shared_hash_table_controller.reset(new vectorized::SharedHashTableController());
    _shared_scanner_controller.reset(new vectorized::SharedScannerController());
    _execution_dependency =
            pipeline::Dependency::create_unique(-1, -1, "ExecutionDependency", this);
    _runtime_filter_mgr.reset(
            new RuntimeFilterMgr(TUniqueId(), RuntimeFilterParamsContext::create(this)));
}

QueryContext::~QueryContext() {
    // query mem tracker consumption is equal to 0, it means that after QueryContext is created,
    // it is found that query already exists in _query_ctx_map, and query mem tracker is not used.
    // query mem tracker consumption is not equal to 0 after use, because there is memory consumed
    // on query mem tracker, released on other trackers.
    std::string mem_tracker_msg {""};
    if (query_mem_tracker->peak_consumption() != 0) {
        mem_tracker_msg = fmt::format(
                ", deregister query/load memory tracker, queryId={}, Limit={}, CurrUsed={}, "
                "PeakUsed={}",
                print_id(_query_id), MemTracker::print_bytes(query_mem_tracker->limit()),
                MemTracker::print_bytes(query_mem_tracker->consumption()),
                MemTracker::print_bytes(query_mem_tracker->peak_consumption()));
    }
    if (_task_group) {
        _task_group->remove_mem_tracker_limiter(query_mem_tracker);
    }

    LOG_INFO("Query {} deconstructed, {}", print_id(_query_id), mem_tracker_msg);
    // Not release the the thread token in query context's dector method, because the query
    // conext may be dectored in the thread token it self. It is very dangerous and may core.
    // And also thread token need shutdown, it may take some time, may cause the thread that
    // release the token hang, the thread maybe a pipeline task scheduler thread.
    if (_thread_token) {
        static_cast<void>(ExecEnv::GetInstance()->lazy_release_obj_pool()->submit(
                std::make_shared<DelayReleaseToken>(std::move(_thread_token))));
    }
    _exec_env->runtime_query_statistics_mgr()->set_query_finished(print_id(_query_id));
}

void QueryContext::set_ready_to_execute(bool is_cancelled) {
    _execution_dependency->set_ready();
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
    _execution_dependency->set_ready();
    {
        std::lock_guard<std::mutex> l(_start_lock);
        _ready_to_execute = true;
    }
    _start_cond.notify_all();
}

bool QueryContext::cancel(bool v, std::string msg, Status new_status, int fragment_id) {
    if (_is_cancelled) {
        return false;
    }
    set_exec_status(new_status);
    _is_cancelled.store(v);

    set_ready_to_execute(true);
    {
        std::lock_guard<std::mutex> plock(pipeline_lock);
        for (auto& ctx : fragment_id_to_pipeline_ctx) {
            if (fragment_id == ctx.first) {
                continue;
            }
            ctx.second->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, msg);
        }
    }
    return true;
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

void QueryContext::set_query_scheduler(uint64_t tg_id) {
    auto* tg_mgr = _exec_env->task_group_manager();
    tg_mgr->get_query_scheduler(tg_id, &_task_scheduler, &_scan_task_scheduler,
                                &_non_pipe_thread_pool);
}

doris::pipeline::TaskScheduler* QueryContext::get_pipe_exec_scheduler() {
    if (!config::enable_cgroup_cpu_soft_limit) {
        return _exec_env->pipeline_task_group_scheduler();
    } else if (_task_scheduler) {
        return _task_scheduler;
    } else {
        return _exec_env->pipeline_task_scheduler();
    }
}

ThreadPool* QueryContext::get_non_pipe_exec_thread_pool() {
    if (_task_group) {
        return _non_pipe_thread_pool;
    } else {
        return nullptr;
    }
}

} // namespace doris
