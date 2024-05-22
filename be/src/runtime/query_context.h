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
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/Types_types.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/config.h"
#include "common/factory_creator.h"
#include "common/object_pool.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_predicate.h"
#include "util/hash_util.hpp"
#include "util/threadpool.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/runtime/shared_hash_table_controller.h"
#include "vec/runtime/shared_scanner_controller.h"
#include "workload_group/workload_group.h"

namespace doris {

namespace pipeline {
class PipelineFragmentContext;
} // namespace pipeline

struct ReportStatusRequest {
    bool is_pipeline_x;
    const Status status;
    std::vector<RuntimeState*> runtime_states;
    RuntimeProfile* profile = nullptr;
    RuntimeProfile* load_channel_profile = nullptr;
    bool done;
    TNetworkAddress coord_addr;
    TUniqueId query_id;
    int fragment_id;
    TUniqueId fragment_instance_id;
    int backend_num;
    RuntimeState* runtime_state;
    std::function<void(const Status&)> cancel_fn;
};

// Save the common components of fragments in a query.
// Some components like DescriptorTbl may be very large
// that will slow down each execution of fragments when DeSer them every time.
class DescriptorTbl;
class QueryContext {
    ENABLE_FACTORY_CREATOR(QueryContext);

public:
    QueryContext(TUniqueId query_id, int total_fragment_num, ExecEnv* exec_env,
                 const TQueryOptions& query_options, TNetworkAddress coord_addr, bool is_pipeline,
                 bool is_nereids);

    ~QueryContext();

    // Notice. For load fragments, the fragment_num sent by FE has a small probability of 0.
    // this may be a bug, bug <= 1 in theory it shouldn't cause any problems at this stage.
    bool countdown(int instance_num) {
        return fragment_num.fetch_sub(instance_num) <= instance_num;
    }

    ExecEnv* exec_env() { return _exec_env; }

    bool is_timeout(timespec now) const {
        if (_timeout_second <= 0) {
            return false;
        }
        return _query_watcher.elapsed_time_seconds(now) > _timeout_second;
    }

    void set_thread_token(int concurrency, bool is_serial) {
        _thread_token = _exec_env->scanner_scheduler()->new_limited_scan_pool_token(
                is_serial ? ThreadPool::ExecutionMode::SERIAL
                          : ThreadPool::ExecutionMode::CONCURRENT,
                concurrency);
    }

    ThreadPoolToken* get_token() { return _thread_token.get(); }

    void set_ready_to_execute(Status reason);

    [[nodiscard]] bool is_cancelled() const { return !_exec_status.ok(); }

    void cancel_all_pipeline_context(const Status& reason);
    Status cancel_pipeline_context(const int fragment_id, const Status& reason);
    void set_pipeline_context(const int fragment_id,
                              std::shared_ptr<pipeline::PipelineFragmentContext> pip_ctx);
    void cancel(Status new_status, int fragment_id = -1);

    void set_exec_status(Status new_status) { _exec_status.update(new_status); }

    [[nodiscard]] Status exec_status() { return _exec_status.status(); }

    void set_execution_dependency_ready();

    void set_ready_to_execute_only();

    bool is_ready_to_execute() {
        std::lock_guard<std::mutex> l(_start_lock);
        return _ready_to_execute;
    }

    bool wait_for_start() {
        int wait_time = config::max_fragment_start_wait_time_seconds;
        std::unique_lock<std::mutex> l(_start_lock);
        while (!_ready_to_execute.load() && _exec_status.ok() && --wait_time > 0) {
            _start_cond.wait_for(l, std::chrono::seconds(1));
        }
        return _ready_to_execute.load() && _exec_status.ok();
    }

    std::shared_ptr<vectorized::SharedHashTableController> get_shared_hash_table_controller() {
        return _shared_hash_table_controller;
    }

    std::shared_ptr<vectorized::SharedScannerController> get_shared_scanner_controller() {
        return _shared_scanner_controller;
    }

    vectorized::RuntimePredicate& get_runtime_predicate(int source_node_id) {
        DCHECK(_runtime_predicates.contains(source_node_id) || _runtime_predicates.contains(0));
        if (_runtime_predicates.contains(source_node_id)) {
            return _runtime_predicates[source_node_id];
        }
        return _runtime_predicates[0];
    }

    void init_runtime_predicates(std::vector<int> source_node_ids) {
        for (int id : source_node_ids) {
            _runtime_predicates.try_emplace(id);
        }
    }

    Status set_workload_group(WorkloadGroupPtr& tg);

    int execution_timeout() const {
        return _query_options.__isset.execution_timeout ? _query_options.execution_timeout
                                                        : _query_options.query_timeout;
    }

    int32_t runtime_filter_wait_time_ms() const {
        return _query_options.runtime_filter_wait_time_ms;
    }

    bool runtime_filter_wait_infinitely() const {
        return _query_options.__isset.runtime_filter_wait_infinitely &&
               _query_options.runtime_filter_wait_infinitely;
    }

    bool enable_pipeline_x_exec() const {
        return (_query_options.__isset.enable_pipeline_x_engine &&
                _query_options.enable_pipeline_x_engine) ||
               (_query_options.__isset.enable_pipeline_engine &&
                _query_options.enable_pipeline_engine);
    }

    int be_exec_version() const {
        if (!_query_options.__isset.be_exec_version) {
            return 0;
        }
        return _query_options.be_exec_version;
    }

    [[nodiscard]] int64_t get_fe_process_uuid() const {
        return _query_options.__isset.fe_process_uuid ? _query_options.fe_process_uuid : 0;
    }

    // global runtime filter mgr, the runtime filter have remote target or
    // need local merge should regist here. before publish() or push_to_remote()
    // the runtime filter should do the local merge work
    RuntimeFilterMgr* runtime_filter_mgr() { return _runtime_filter_mgr.get(); }

    TUniqueId query_id() const { return _query_id; }

    vectorized::SimplifiedScanScheduler* get_scan_scheduler() { return _scan_task_scheduler; }

    vectorized::SimplifiedScanScheduler* get_remote_scan_scheduler() {
        return _remote_scan_task_scheduler;
    }

    pipeline::Dependency* get_execution_dependency() { return _execution_dependency.get(); }

    void register_query_statistics(std::shared_ptr<QueryStatistics> qs);

    std::shared_ptr<QueryStatistics> get_query_statistics();

    void register_memory_statistics();

    void register_cpu_statistics();

    std::shared_ptr<QueryStatistics> get_cpu_statistics() { return _cpu_statistics; }

    doris::pipeline::TaskScheduler* get_pipe_exec_scheduler();

    ThreadPool* get_non_pipe_exec_thread_pool();

    std::vector<TUniqueId> get_fragment_instance_ids() const { return fragment_instance_ids; }

    int64_t mem_limit() const { return _bytes_limit; }

    void set_merge_controller_handler(
            std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {
        _merge_controller_handler = handler;
    }

    bool is_nereids() const { return _is_nereids; }

    WorkloadGroupPtr workload_group() const { return _workload_group; }

    void inc_running_big_mem_op_num() {
        _running_big_mem_op_num.fetch_add(1, std::memory_order_relaxed);
    }
    void dec_running_big_mem_op_num() {
        _running_big_mem_op_num.fetch_sub(1, std::memory_order_relaxed);
    }
    int32_t get_running_big_mem_op_num() {
        return _running_big_mem_op_num.load(std::memory_order_relaxed);
    }

    void set_weighted_mem(int64_t weighted_limit, int64_t weighted_consumption) {
        std::lock_guard<std::mutex> l(_weighted_mem_lock);
        _weighted_consumption = weighted_consumption;
        _weighted_limit = weighted_limit;
    }
    void get_weighted_mem_info(int64_t& weighted_limit, int64_t& weighted_consumption) {
        std::lock_guard<std::mutex> l(_weighted_mem_lock);
        weighted_limit = _weighted_limit;
        weighted_consumption = _weighted_consumption;
    }

    DescriptorTbl* desc_tbl = nullptr;
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
    ObjectPool obj_pool;
    // MemTracker that is shared by all fragment instances running on this host.
    std::shared_ptr<MemTrackerLimiter> query_mem_tracker;

    std::vector<TUniqueId> fragment_instance_ids;

    // plan node id -> TFileScanRangeParams
    // only for file scan node
    std::map<int, TFileScanRangeParams> file_scan_range_params_map;

private:
    int _timeout_second;
    TUniqueId _query_id;
    ExecEnv* _exec_env = nullptr;
    MonotonicStopWatch _query_watcher;
    int64_t _bytes_limit = 0;
    bool _is_pipeline = false;
    bool _is_nereids = false;
    std::atomic<int> _running_big_mem_op_num = 0;

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

    void _init_query_mem_tracker();

    std::shared_ptr<vectorized::SharedHashTableController> _shared_hash_table_controller;
    std::shared_ptr<vectorized::SharedScannerController> _shared_scanner_controller;
    std::unordered_map<int, vectorized::RuntimePredicate> _runtime_predicates;

    WorkloadGroupPtr _workload_group = nullptr;
    std::unique_ptr<RuntimeFilterMgr> _runtime_filter_mgr;
    const TQueryOptions _query_options;

    // All pipeline tasks use the same query context to report status. So we need a `_exec_status`
    // to report the real message if failed.
    AtomicStatus _exec_status;

    doris::pipeline::TaskScheduler* _task_scheduler = nullptr;
    vectorized::SimplifiedScanScheduler* _scan_task_scheduler = nullptr;
    ThreadPool* _non_pipe_thread_pool = nullptr;
    vectorized::SimplifiedScanScheduler* _remote_scan_task_scheduler = nullptr;
    std::unique_ptr<pipeline::Dependency> _execution_dependency;

    std::shared_ptr<QueryStatistics> _cpu_statistics = nullptr;
    // This shared ptr is never used. It is just a reference to hold the object.
    // There is a weak ptr in runtime filter manager to reference this object.
    std::shared_ptr<RuntimeFilterMergeControllerEntity> _merge_controller_handler;

    std::map<int, std::weak_ptr<pipeline::PipelineFragmentContext>> _fragment_id_to_pipeline_ctx;
    std::mutex _pipeline_map_write_lock;

    std::mutex _weighted_mem_lock;
    int64_t _weighted_consumption = 0;
    int64_t _weighted_limit = 0;

    std::mutex _profile_mutex;

    // when fragment of pipeline x is closed, it will register its profile to this map by using add_fragment_profile_x
    // flatten profile of one fragment:
    // Pipeline 0
    //      PipelineTask 0
    //              Operator 1
    //              Operator 2
    //              Scanner
    //      PipelineTask 1
    //              Operator 1
    //              Operator 2
    //              Scanner
    // Pipeline 1
    //      PipelineTask 2
    //              Operator 3
    //      PipelineTask 3
    //              Operator 3
    // fragment_id -> list<profile>
    std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>> _profile_map_x;
    std::unordered_map<int, std::shared_ptr<TRuntimeProfileTree>> _load_channel_profile_map_x;

    // instance_id -> profile
    std::unordered_map<TUniqueId, std::shared_ptr<TRuntimeProfileTree>> _profile_map;
    std::unordered_map<TUniqueId, std::shared_ptr<TRuntimeProfileTree>> _load_channel_profile_map;

    void _report_query_profile();
    void _report_query_profile_non_pipeline();
    void _report_query_profile_x();

    std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>>
    _collect_realtime_query_profile_x() const;

    std::unordered_map<TUniqueId, std::vector<std::shared_ptr<TRuntimeProfileTree>>>
    _collect_realtime_query_profile_non_pipeline() const;

public:
    // when fragment of pipeline x is closed, it will register its profile to this map by using add_fragment_profile_x
    void add_fragment_profile_x(
            int fragment_id,
            const std::vector<std::shared_ptr<TRuntimeProfileTree>>& pipeline_profile,
            std::shared_ptr<TRuntimeProfileTree> load_channel_profile);

    void add_instance_profile(const TUniqueId& iid, std::shared_ptr<TRuntimeProfileTree> profile,
                              std::shared_ptr<TRuntimeProfileTree> load_channel_profile);

    TReportExecStatusParams get_realtime_exec_status_x() const;

    bool enable_profile() const {
        return _query_options.__isset.enable_profile && _query_options.enable_profile;
    }
};

} // namespace doris
