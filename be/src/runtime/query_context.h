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

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>

#include <atomic>
#include <memory>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/factory_creator.h"
#include "common/object_pool.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_predicate.h"
#include "task_group/task_group.h"
#include "util/pretty_printer.h"
#include "util/threadpool.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/runtime/shared_hash_table_controller.h"
#include "vec/runtime/shared_scanner_controller.h"

namespace doris {
struct ReportStatusRequest {
    bool is_pipeline_x;
    const Status status;
    std::vector<RuntimeState*> runtime_states;
    RuntimeProfile* profile;
    RuntimeProfile* load_channel_profile;
    bool done;
    TNetworkAddress coord_addr;
    TUniqueId query_id;
    int fragment_id;
    TUniqueId fragment_instance_id;
    int backend_num;
    RuntimeState* runtime_state;
    std::function<Status(Status)> update_fn;
    std::function<void(const PPlanFragmentCancelReason&, const std::string&)> cancel_fn;
};
// Save the common components of fragments in a query.
// Some components like DescriptorTbl may be very large
// that will slow down each execution of fragments when DeSer them every time.
class DescriptorTbl;
class QueryContext {
    ENABLE_FACTORY_CREATOR(QueryContext);

public:
    QueryContext(TUniqueId query_id, int total_fragment_num, ExecEnv* exec_env,
                 const TQueryOptions& query_options)
            : fragment_num(total_fragment_num),
              timeout_second(-1),
              _query_id(query_id),
              _exec_env(exec_env),
              _runtime_filter_mgr(new RuntimeFilterMgr(TUniqueId(), this)),
              _query_options(query_options) {
        _start_time = VecDateTimeValue::local_time();
        _shared_hash_table_controller.reset(new vectorized::SharedHashTableController());
        _shared_scanner_controller.reset(new vectorized::SharedScannerController());
    }

    ~QueryContext() {
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
    }

    // Notice. For load fragments, the fragment_num sent by FE has a small probability of 0.
    // this may be a bug, bug <= 1 in theory it shouldn't cause any problems at this stage.
    bool countdown(int instance_num) {
        return fragment_num.fetch_sub(instance_num) <= instance_num;
    }

    ExecEnv* exec_env() { return _exec_env; }

    bool is_timeout(const VecDateTimeValue& now) const {
        if (timeout_second <= 0) {
            return false;
        }
        if (now.second_diff(_start_time) > timeout_second) {
            return true;
        }
        return false;
    }

    void set_thread_token(int concurrency, bool is_serial) {
        _thread_token = _exec_env->scanner_scheduler()->new_limited_scan_pool_token(
                is_serial ? ThreadPool::ExecutionMode::SERIAL
                          : ThreadPool::ExecutionMode::CONCURRENT,
                concurrency);
    }

    ThreadPoolToken* get_token() { return _thread_token.get(); }

    void set_ready_to_execute(bool is_cancelled) {
        {
            std::lock_guard<std::mutex> l(_start_lock);
            _is_cancelled = is_cancelled;
            _ready_to_execute = true;
        }
        if (query_mem_tracker && is_cancelled) {
            query_mem_tracker->set_is_query_cancelled(is_cancelled);
        }
        _start_cond.notify_all();
    }

    [[nodiscard]] bool is_cancelled() const { return _is_cancelled.load(); }
    bool cancel(bool v, std::string msg, Status new_status) {
        if (_is_cancelled) {
            return false;
        }
        set_exec_status(new_status);
        _is_cancelled.store(v);

        set_ready_to_execute(true);
        return true;
    }

    void set_exec_status(Status new_status) {
        if (new_status.ok()) {
            return;
        }
        std::lock_guard<std::mutex> l(_exec_status_lock);
        if (!_exec_status.ok()) {
            return;
        }
        _exec_status = new_status;
    }

    [[nodiscard]] Status exec_status() {
        std::lock_guard<std::mutex> l(_exec_status_lock);
        return _exec_status;
    }

    void set_ready_to_execute_only() {
        {
            std::lock_guard<std::mutex> l(_start_lock);
            _ready_to_execute = true;
        }
        _start_cond.notify_all();
    }

    bool is_ready_to_execute() {
        std::lock_guard<std::mutex> l(_start_lock);
        return _ready_to_execute;
    }

    bool wait_for_start() {
        int wait_time = config::max_fragment_start_wait_time_seconds;
        std::unique_lock<std::mutex> l(_start_lock);
        while (!_ready_to_execute.load() && !_is_cancelled.load() && --wait_time > 0) {
            _start_cond.wait_for(l, std::chrono::seconds(1));
        }
        return _ready_to_execute.load() && !_is_cancelled.load();
    }

    std::shared_ptr<vectorized::SharedHashTableController> get_shared_hash_table_controller() {
        return _shared_hash_table_controller;
    }

    std::shared_ptr<vectorized::SharedScannerController> get_shared_scanner_controller() {
        return _shared_scanner_controller;
    }

    vectorized::RuntimePredicate& get_runtime_predicate() { return _runtime_predicate; }

    void set_task_group(taskgroup::TaskGroupPtr& tg) { _task_group = tg; }

    taskgroup::TaskGroup* get_task_group() const { return _task_group.get(); }

    int execution_timeout() const {
        return _query_options.__isset.execution_timeout ? _query_options.execution_timeout
                                                        : _query_options.query_timeout;
    }

    int32_t runtime_filter_wait_time_ms() const {
        return _query_options.runtime_filter_wait_time_ms;
    }

    bool enable_pipeline_exec() const {
        return _query_options.__isset.enable_pipeline_engine &&
               _query_options.enable_pipeline_engine;
    }

    int be_exec_version() const {
        if (!_query_options.__isset.be_exec_version) {
            return 0;
        }
        return _query_options.be_exec_version;
    }

    [[nodiscard]] int64_t get_fe_process_uuid() const { return _query_options.fe_process_uuid; }

    RuntimeFilterMgr* runtime_filter_mgr() { return _runtime_filter_mgr.get(); }

    TUniqueId query_id() const { return _query_id; }

    void set_task_scheduler(pipeline::TaskScheduler* task_scheduler) {
        _task_scheduler = task_scheduler;
    }

    pipeline::TaskScheduler* get_task_scheduler() { return _task_scheduler; }

    void set_scan_task_scheduler(vectorized::SimplifiedScanScheduler* scan_task_scheduler) {
        _scan_task_scheduler = scan_task_scheduler;
    }

    vectorized::SimplifiedScanScheduler* get_scan_scheduler() { return _scan_task_scheduler; }

public:
    DescriptorTbl* desc_tbl;
    bool set_rsc_info = false;
    std::string user;
    std::string group;
    TNetworkAddress coord_addr;
    TQueryGlobals query_globals;

    /// In the current implementation, for multiple fragments executed by a query on the same BE node,
    /// we store some common components in QueryContext, and save QueryContext in FragmentMgr.
    /// When all Fragments are executed, QueryContext needs to be deleted from FragmentMgr.
    /// Here we use a counter to store the number of Fragments that have not yet been completed,
    /// and after each Fragment is completed, this value will be reduced by one.
    /// When the last Fragment is completed, the counter is cleared, and the worker thread of the last Fragment
    /// will clean up QueryContext.
    std::atomic<int> fragment_num;
    int timeout_second;
    ObjectPool obj_pool;
    // MemTracker that is shared by all fragment instances running on this host.
    std::shared_ptr<MemTrackerLimiter> query_mem_tracker;

    std::vector<TUniqueId> fragment_instance_ids;

    // plan node id -> TFileScanRangeParams
    // only for file scan node
    std::map<int, TFileScanRangeParams> file_scan_range_params_map;

private:
    TUniqueId _query_id;
    ExecEnv* _exec_env;
    VecDateTimeValue _start_time;

    // A token used to submit olap scanner to the "_limited_scan_thread_pool",
    // This thread pool token is created from "_limited_scan_thread_pool" from exec env.
    // And will be shared by all instances of this query.
    // So that we can control the max thread that a query can be used to execute.
    // If this token is not set, the scanner will be executed in "_scan_thread_pool" in exec env.
    std::unique_ptr<ThreadPoolToken> _thread_token;

    std::mutex _start_lock;
    std::condition_variable _start_cond;
    // Only valid when _need_wait_execution_trigger is set to true in PlanFragmentExecutor.
    // And all fragments of this query will start execution when this is set to true.
    std::atomic<bool> _ready_to_execute {false};
    std::atomic<bool> _is_cancelled {false};

    std::shared_ptr<vectorized::SharedHashTableController> _shared_hash_table_controller;
    std::shared_ptr<vectorized::SharedScannerController> _shared_scanner_controller;
    vectorized::RuntimePredicate _runtime_predicate;

    taskgroup::TaskGroupPtr _task_group;
    std::unique_ptr<RuntimeFilterMgr> _runtime_filter_mgr;
    const TQueryOptions _query_options;

    std::mutex _exec_status_lock;
    // All pipeline tasks use the same query context to report status. So we need a `_exec_status`
    // to report the real message if failed.
    Status _exec_status = Status::OK();

    pipeline::TaskScheduler* _task_scheduler = nullptr;
    vectorized::SimplifiedScanScheduler* _scan_task_scheduler = nullptr;
};

} // namespace doris
