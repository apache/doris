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
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/config.h"
#include "common/factory_creator.h"
#include "common/object_pool.h"
#include "pipeline/dependency.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_predicate.h"
#include "util/hash_util.hpp"
#include "util/threadpool.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/runtime/shared_hash_table_controller.h"
#include "workload_group/workload_group.h"

namespace doris {

namespace pipeline {
class PipelineFragmentContext;
class PipelineTask;
} // namespace pipeline

struct ReportStatusRequest {
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

enum class QuerySource {
    INTERNAL_FRONTEND,
    STREAM_LOAD,
    GROUP_COMMIT_LOAD,
    ROUTINE_LOAD,
    EXTERNAL_CONNECTOR
};

const std::string toString(QuerySource query_source);

// Save the common components of fragments in a query.
// Some components like DescriptorTbl may be very large
// that will slow down each execution of fragments when DeSer them every time.
class DescriptorTbl;
class QueryContext : public std::enable_shared_from_this<QueryContext> {
    ENABLE_FACTORY_CREATOR(QueryContext);

public:
    QueryContext(TUniqueId query_id, ExecEnv* exec_env, const TQueryOptions& query_options,
                 TNetworkAddress coord_addr, bool is_pipeline, bool is_nereids,
                 TNetworkAddress current_connect_fe, QuerySource query_type);

    ~QueryContext();

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

    void cancel_all_pipeline_context(const Status& reason, int fragment_id = -1);
    std::string print_all_pipeline_context();
    void set_pipeline_context(const int fragment_id,
                              std::shared_ptr<pipeline::PipelineFragmentContext> pip_ctx);
    void cancel(Status new_status, int fragment_id = -1);

    [[nodiscard]] Status exec_status() { return _exec_status.status(); }

    void set_execution_dependency_ready();

    void set_memory_sufficient(bool sufficient);

    void set_ready_to_execute_only();

    std::shared_ptr<vectorized::SharedHashTableController> get_shared_hash_table_controller() {
        return _shared_hash_table_controller;
    }

    bool has_runtime_predicate(int source_node_id) {
        return _runtime_predicates.contains(source_node_id);
    }

    vectorized::RuntimePredicate& get_runtime_predicate(int source_node_id) {
        DCHECK(has_runtime_predicate(source_node_id));
        return _runtime_predicates.find(source_node_id)->second;
    }

    void init_runtime_predicates(const std::vector<TTopnFilterDesc>& topn_filter_descs) {
        for (auto desc : topn_filter_descs) {
            _runtime_predicates.try_emplace(desc.source_node_id, desc);
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

    pipeline::Dependency* get_memory_sufficient_dependency() {
        return _memory_sufficient_dependency.get();
    }

    std::vector<pipeline::PipelineTask*> get_revocable_tasks() const;

    Status revoke_memory();

    void register_query_statistics(std::shared_ptr<QueryStatistics> qs);

    std::shared_ptr<QueryStatistics> get_query_statistics();

    void register_memory_statistics();

    void register_cpu_statistics();

    std::shared_ptr<QueryStatistics> get_cpu_statistics() { return _cpu_statistics; }

    doris::pipeline::TaskScheduler* get_pipe_exec_scheduler();

    ThreadPool* get_memtable_flush_pool();

    std::vector<TUniqueId> get_fragment_instance_ids() const { return fragment_instance_ids; }

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

    void increase_revoking_tasks_count() { _revoking_tasks_count.fetch_add(1); }

    void decrease_revoking_tasks_count();

    int get_revoking_tasks_count() const { return _revoking_tasks_count.load(); }

    void get_revocable_info(size_t* revocable_size, size_t* memory_usage,
                            bool* has_running_task) const;
    size_t get_revocable_size() const;

    // This method is called by workload group manager to set query's memlimit using slot
    // If user set query limit explicitly, then should use less one
    void set_mem_limit(int64_t new_mem_limit) {
        query_mem_tracker->set_limit(std::min<int64_t>(new_mem_limit, _user_set_mem_limit));
    }

    int64_t get_mem_limit() const { return query_mem_tracker->limit(); }

    std::shared_ptr<MemTrackerLimiter>& get_mem_tracker() { return query_mem_tracker; }

    int32_t get_slot_count() const {
        return _query_options.__isset.query_slot_count ? _query_options.query_slot_count : 1;
    }

    bool enable_query_slot_hard_limit() const {
        return _query_options.__isset.enable_query_slot_hard_limit
                       ? _query_options.enable_query_slot_hard_limit
                       : false;
    }
    DescriptorTbl* desc_tbl = nullptr;
    bool set_rsc_info = false;
    std::string user;
    std::string group;
    TNetworkAddress coord_addr;
    TNetworkAddress current_connect_fe;
    TQueryGlobals query_globals;

    ObjectPool obj_pool;
    // MemTracker that is shared by all fragment instances running on this host.
    std::shared_ptr<MemTrackerLimiter> query_mem_tracker;

    std::vector<TUniqueId> fragment_instance_ids;

    // plan node id -> TFileScanRangeParams
    // only for file scan node
    std::map<int, TFileScanRangeParams> file_scan_range_params_map;

    void update_wg_cpu_adder(int64_t delta_cpu_time) {
        if (_workload_group != nullptr) {
            _workload_group->update_cpu_adder(delta_cpu_time);
        }
    }

    // Query will run in low memory mode when
    // 1. the query is enable spill and wg's low water mark reached, if not release buffer, it will trigger spill disk, it is very expensive.
    // 2. the query is not enable spill, but wg's high water mark reached, if not release buffer, the query will be cancelled.
    // 3. the process reached soft mem_limit, if not release these, if not release buffer, the query will be cancelled.
    // 4. If the query reserve memory failed.
    // Under low memory mode, the query should release some buffers such as scan operator block queue, union operator queue, exchange buffer size, streaming agg
    void update_low_memory_mode() {
        if (_low_memory_mode) {
            return;
        }

        // If less than 100MB left, then it is low memory mode
        if (doris::GlobalMemoryArbitrator::is_exceed_soft_mem_limit(100 * 1024 * 1024)) {
            _low_memory_mode = true;
            LOG(INFO) << "Query " << print_id(_query_id)
                      << " goes to low memory mode due to exceed process soft memory limit";
            return;
        }

        if (_workload_group) {
            bool is_low_wartermark = false;
            bool is_high_wartermark = false;
            _workload_group->check_mem_used(&is_low_wartermark, &is_high_wartermark);
            // If the wg is not enable hard limit, this will also take effect to lower down the memory usage.
            if (is_high_wartermark) {
                LOG(INFO)
                        << "Query " << print_id(_query_id)
                        << " goes to low memory mode due to workload group high water mark reached";
                _low_memory_mode = true;
                return;
            }

            if (is_low_wartermark &&
                ((_query_options.__isset.enable_join_spill && _query_options.enable_join_spill) ||
                 (_query_options.__isset.enable_sort_spill && _query_options.enable_sort_spill) ||
                 (_query_options.__isset.enable_agg_spill && _query_options.enable_agg_spill))) {
                LOG(INFO) << "Query " << print_id(_query_id)
                          << " goes to low memory mode due to workload group low water mark "
                             "reached and the query enable spill";
                _low_memory_mode = true;
                return;
            }
        }
    }

    void set_low_memory_mode() { _low_memory_mode = true; }

    bool low_memory_mode() { return _low_memory_mode; }

    void update_paused_reason(const Status& st) {
        std::lock_guard l(_paused_mutex);
        if (_paused_reason.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
            return;
        } else if (_paused_reason.is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>()) {
            if (st.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
                _paused_reason = st;
                return;
            } else {
                return;
            }
        } else {
            _paused_reason = st;
        }
    }

    Status paused_reason() {
        std::lock_guard l(_paused_mutex);
        return _paused_reason;
    }

    bool is_pure_load_task() {
        return _query_source == QuerySource::STREAM_LOAD ||
               _query_source == QuerySource::ROUTINE_LOAD ||
               _query_source == QuerySource::GROUP_COMMIT_LOAD;
    }

    std::string debug_string();

private:
    int _timeout_second;
    TUniqueId _query_id;
    ExecEnv* _exec_env = nullptr;
    MonotonicStopWatch _query_watcher;
    bool _is_pipeline = false;
    bool _is_nereids = false;
    std::atomic<int> _running_big_mem_op_num = 0;

    std::mutex _revoking_tasks_mutex;
    std::atomic<int> _revoking_tasks_count = 0;

    // A token used to submit olap scanner to the "_limited_scan_thread_pool",
    // This thread pool token is created from "_limited_scan_thread_pool" from exec env.
    // And will be shared by all instances of this query.
    // So that we can control the max thread that a query can be used to execute.
    // If this token is not set, the scanner will be executed in "_scan_thread_pool" in exec env.
    std::unique_ptr<ThreadPoolToken> _thread_token;

    void _init_query_mem_tracker();

    std::shared_ptr<vectorized::SharedHashTableController> _shared_hash_table_controller;
    std::unordered_map<int, vectorized::RuntimePredicate> _runtime_predicates;

    WorkloadGroupPtr _workload_group = nullptr;
    std::unique_ptr<RuntimeFilterMgr> _runtime_filter_mgr;
    const TQueryOptions _query_options;

    // All pipeline tasks use the same query context to report status. So we need a `_exec_status`
    // to report the real message if failed.
    AtomicStatus _exec_status;

    doris::pipeline::TaskScheduler* _task_scheduler = nullptr;
    vectorized::SimplifiedScanScheduler* _scan_task_scheduler = nullptr;
    ThreadPool* _memtable_flush_pool = nullptr;
    vectorized::SimplifiedScanScheduler* _remote_scan_task_scheduler = nullptr;
    std::unique_ptr<pipeline::Dependency> _execution_dependency;

    std::unique_ptr<pipeline::Dependency> _memory_sufficient_dependency;
    std::vector<std::weak_ptr<pipeline::PipelineTask>> _pipeline_tasks;

    std::shared_ptr<QueryStatistics> _cpu_statistics = nullptr;
    // This shared ptr is never used. It is just a reference to hold the object.
    // There is a weak ptr in runtime filter manager to reference this object.
    std::shared_ptr<RuntimeFilterMergeControllerEntity> _merge_controller_handler;

    std::map<int, std::weak_ptr<pipeline::PipelineFragmentContext>> _fragment_id_to_pipeline_ctx;
    std::mutex _pipeline_map_write_lock;

    std::atomic<bool> _low_memory_mode = false;
    int64_t _user_set_mem_limit = 0;

    std::mutex _profile_mutex;
    timespec _query_arrival_timestamp;
    // Distinguish the query source, for query that comes from fe, we will have some memory structure on FE to
    // help us manage the query.
    QuerySource _query_source;

    Status _paused_reason;
    std::mutex _paused_mutex;

    // when fragment of pipeline is closed, it will register its profile to this map by using add_fragment_profile
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
    std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>> _profile_map;
    std::unordered_map<int, std::shared_ptr<TRuntimeProfileTree>> _load_channel_profile_map;

    void _report_query_profile();

    std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>>
    _collect_realtime_query_profile() const;

public:
    // when fragment of pipeline is closed, it will register its profile to this map by using add_fragment_profile
    void add_fragment_profile(
            int fragment_id,
            const std::vector<std::shared_ptr<TRuntimeProfileTree>>& pipeline_profile,
            std::shared_ptr<TRuntimeProfileTree> load_channel_profile);

    TReportExecStatusParams get_realtime_exec_status() const;

    bool enable_profile() const {
        return _query_options.__isset.enable_profile && _query_options.enable_profile;
    }

    timespec get_query_arrival_timestamp() const { return this->_query_arrival_timestamp; }
    QuerySource get_query_source() const { return this->_query_source; }
};

} // namespace doris
