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
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/config.h"
#include "common/factory_creator.h"
#include "common/object_pool.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_predicate.h"
#include "runtime/workload_management/resource_context.h"
#include "runtime_filter/runtime_filter_mgr.h"
#include "util/hash_util.hpp"
#include "util/threadpool.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "workload_group/workload_group.h"

namespace doris {

namespace pipeline {
class PipelineFragmentContext;
class PipelineTask;
class Dependency;
} // namespace pipeline

struct ReportStatusRequest {
    const Status status;
    std::vector<RuntimeState*> runtime_states;
    bool done;
    TNetworkAddress coord_addr;
    TUniqueId query_id;
    int fragment_id;
    TUniqueId fragment_instance_id;
    int backend_num;
    RuntimeState* runtime_state;
    std::string load_error_url;
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
    static std::shared_ptr<QueryContext> create(TUniqueId query_id, ExecEnv* exec_env,
                                                const TQueryOptions& query_options,
                                                TNetworkAddress coord_addr, bool is_nereids,
                                                TNetworkAddress current_connect_fe,
                                                QuerySource query_type);

    // use QueryContext::create, cannot be made private because of ENABLE_FACTORY_CREATOR::create_shared.
    QueryContext(TUniqueId query_id, ExecEnv* exec_env, const TQueryOptions& query_options,
                 TNetworkAddress coord_addr, bool is_nereids, TNetworkAddress current_connect_fe,
                 QuerySource query_type);

    ~QueryContext();

    void init_query_task_controller();

    ExecEnv* exec_env() const { return _exec_env; }

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

    void set_workload_group(WorkloadGroupPtr& wg);

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

    bool ignore_runtime_filter_error() const {
        return _query_options.__isset.ignore_runtime_filter_error
                       ? _query_options.ignore_runtime_filter_error
                       : false;
    }

    bool enable_force_spill() const {
        return _query_options.__isset.enable_force_spill && _query_options.enable_force_spill;
    }
    const TQueryOptions& query_options() const { return _query_options; }

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

    doris::pipeline::TaskScheduler* get_pipe_exec_scheduler();

    void set_merge_controller_handler(
            std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {
        _merge_controller_handler = handler;
    }
    std::shared_ptr<RuntimeFilterMergeControllerEntity> get_merge_controller_handler() const {
        return _merge_controller_handler;
    }

    bool is_nereids() const { return _is_nereids; }

    WorkloadGroupPtr workload_group() const { return _resource_ctx->workload_group(); }
    std::shared_ptr<MemTrackerLimiter> query_mem_tracker() const {
        DCHECK(_resource_ctx->memory_context()->mem_tracker() != nullptr);
        return _resource_ctx->memory_context()->mem_tracker();
    }

    int32_t get_slot_count() const {
        return _query_options.__isset.query_slot_count ? _query_options.query_slot_count : 1;
    }

    DescriptorTbl* desc_tbl = nullptr;
    bool set_rsc_info = false;
    std::string user;
    std::string group;
    TNetworkAddress coord_addr;
    TNetworkAddress current_connect_fe;
    TQueryGlobals query_globals;
    const TQueryGlobals get_query_globals() const { return query_globals; }

    ObjectPool obj_pool;

    std::shared_ptr<ResourceContext> resource_ctx() { return _resource_ctx; }

    // plan node id -> TFileScanRangeParams
    // only for file scan node
    std::map<int, TFileScanRangeParams> file_scan_range_params_map;

    void add_using_brpc_stub(const TNetworkAddress& network_address,
                             std::shared_ptr<PBackendService_Stub> brpc_stub) {
        if (network_address.port == 0) {
            return;
        }
        std::lock_guard<std::mutex> lock(_brpc_stubs_mutex);
        if (!_using_brpc_stubs.contains(network_address)) {
            _using_brpc_stubs.emplace(network_address, brpc_stub);
        }

        DCHECK_EQ(_using_brpc_stubs[network_address].get(), brpc_stub.get());
    }

    std::unordered_map<TNetworkAddress, std::shared_ptr<PBackendService_Stub>>
    get_using_brpc_stubs() {
        std::lock_guard<std::mutex> lock(_brpc_stubs_mutex);
        return _using_brpc_stubs;
    }

    void set_low_memory_mode() {
        // will not return from low memory mode to non-low memory mode.
        _resource_ctx->task_controller()->set_low_memory_mode(true);
    }
    bool low_memory_mode() { return _resource_ctx->task_controller()->low_memory_mode(); }

    bool is_pure_load_task() {
        return _query_source == QuerySource::STREAM_LOAD ||
               _query_source == QuerySource::ROUTINE_LOAD ||
               _query_source == QuerySource::GROUP_COMMIT_LOAD;
    }

    void set_load_error_url(std::string error_url);
    std::string get_load_error_url();

private:
    friend class QueryTaskController;

    int _timeout_second;
    TUniqueId _query_id;
    ExecEnv* _exec_env = nullptr;
    MonotonicStopWatch _query_watcher;
    bool _is_nereids = false;

    std::shared_ptr<ResourceContext> _resource_ctx;

    // A token used to submit olap scanner to the "_limited_scan_thread_pool",
    // This thread pool token is created from "_limited_scan_thread_pool" from exec env.
    // And will be shared by all instances of this query.
    // So that we can control the max thread that a query can be used to execute.
    // If this token is not set, the scanner will be executed in "_scan_thread_pool" in exec env.
    std::unique_ptr<ThreadPoolToken> _thread_token {nullptr};

    void _init_resource_context();
    void _init_query_mem_tracker();

    std::unordered_map<int, vectorized::RuntimePredicate> _runtime_predicates;

    std::unique_ptr<RuntimeFilterMgr> _runtime_filter_mgr;
    const TQueryOptions _query_options;

    // All pipeline tasks use the same query context to report status. So we need a `_exec_status`
    // to report the real message if failed.
    AtomicStatus _exec_status;

    doris::pipeline::TaskScheduler* _task_scheduler = nullptr;
    vectorized::SimplifiedScanScheduler* _scan_task_scheduler = nullptr;
    vectorized::SimplifiedScanScheduler* _remote_scan_task_scheduler = nullptr;
    // This dependency indicates if the 2nd phase RPC received from FE.
    std::unique_ptr<pipeline::Dependency> _execution_dependency;
    // This dependency indicates if memory is sufficient to execute.
    std::unique_ptr<pipeline::Dependency> _memory_sufficient_dependency;

    // This shared ptr is never used. It is just a reference to hold the object.
    // There is a weak ptr in runtime filter manager to reference this object.
    std::shared_ptr<RuntimeFilterMergeControllerEntity> _merge_controller_handler;

    std::map<int, std::weak_ptr<pipeline::PipelineFragmentContext>> _fragment_id_to_pipeline_ctx;
    std::mutex _pipeline_map_write_lock;

    std::mutex _profile_mutex;
    timespec _query_arrival_timestamp;
    // Distinguish the query source, for query that comes from fe, we will have some memory structure on FE to
    // help us manage the query.
    QuerySource _query_source;

    std::mutex _brpc_stubs_mutex;
    std::unordered_map<TNetworkAddress, std::shared_ptr<PBackendService_Stub>> _using_brpc_stubs;

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
    _collect_realtime_query_profile();

    std::mutex _error_url_lock;
    std::string _load_error_url;

public:
    // when fragment of pipeline is closed, it will register its profile to this map by using add_fragment_profile
    void add_fragment_profile(
            int fragment_id,
            const std::vector<std::shared_ptr<TRuntimeProfileTree>>& pipeline_profile,
            std::shared_ptr<TRuntimeProfileTree> load_channel_profile);

    TReportExecStatusParams get_realtime_exec_status();

    bool enable_profile() const {
        return _query_options.__isset.enable_profile && _query_options.enable_profile;
    }

    timespec get_query_arrival_timestamp() const { return this->_query_arrival_timestamp; }
    QuerySource get_query_source() const { return this->_query_source; }

    const TQueryOptions get_query_options() const { return _query_options; }
};

} // namespace doris
