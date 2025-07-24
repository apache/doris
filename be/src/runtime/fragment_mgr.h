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

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/QueryPlanExtra_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/be_mock_util.h"
#include "common/status.h"
#include "gutil/ref_counted.h"
#include "http/rest_monitor_iface.h"
#include "runtime/query_context.h"
#include "runtime_filter/runtime_filter_mgr.h"
#include "util/countdown_latch.h"
#include "util/hash_util.hpp" // IWYU pragma: keep
#include "util/metrics.h"

namespace butil {
class IOBufAsZeroCopyInputStream;
}

namespace doris {
#include "common/compile_check_begin.h"
extern bvar::Adder<uint64_t> g_fragment_executing_count;
extern bvar::Status<uint64_t> g_fragment_last_active_time;

namespace pipeline {
class PipelineFragmentContext;
} // namespace pipeline
class QueryContext;
class ExecEnv;
class ThreadPool;
class TExecPlanFragmentParams;
class PExecPlanFragmentStartRequest;
class PMergeFilterRequest;
class RuntimeProfile;
class RuntimeState;
class TPipelineFragmentParams;
class TPipelineInstanceParams;
class TScanColumnDesc;
class TScanOpenParams;
class Thread;
class WorkloadQueryInfo;

std::string to_load_error_http_path(const std::string& file_name);

template <typename Key, typename Value, typename ValueType>
class ConcurrentContextMap {
public:
    using ApplyFunction = std::function<Status(phmap::flat_hash_map<Key, Value>&)>;
    ConcurrentContextMap();
    Value find(const Key& query_id);
    void insert(const Key& query_id, std::shared_ptr<ValueType>);
    void clear();
    void erase(const Key& query_id);
    size_t num_items() const {
        size_t n = 0;
        for (auto& pair : _internal_map) {
            std::shared_lock lock(*pair.first);
            auto& map = pair.second;
            n += map.size();
        }
        return n;
    }
    void apply(ApplyFunction&& function) {
        for (auto& pair : _internal_map) {
            // TODO: Now only the cancel worker do the GC the _query_ctx_map. each query must
            // do erase the finish query unless in _query_ctx_map. Rethink the logic is ok
            std::unique_lock lock(*pair.first);
            static_cast<void>(function(pair.second));
        }
    }

    Status apply_if_not_exists(const Key& query_id, std::shared_ptr<ValueType>& query_ctx,
                               ApplyFunction&& function);

private:
    // The lock should only be used to protect the structures in fragment manager. Has to be
    // used in a very small scope because it may dead lock. For example, if the _lock is used
    // in prepare stage, the call path is  prepare --> expr prepare --> may call allocator
    // when allocate failed, allocator may call query_is_cancelled, query is callced will also
    // call _lock, so that there is dead lock.
    std::vector<std::pair<std::unique_ptr<std::shared_mutex>, phmap::flat_hash_map<Key, Value>>>
            _internal_map;
};

// This class used to manage all the fragment execute in this instance
class FragmentMgr : public RestMonitorIface {
public:
    using FinishCallback = std::function<void(RuntimeState*, Status*)>;

    FragmentMgr(ExecEnv* exec_env);
    ~FragmentMgr() override;

    void stop();

    // execute one plan fragment
    Status exec_plan_fragment(const TExecPlanFragmentParams& params, const QuerySource query_type);

    Status exec_plan_fragment(const TPipelineFragmentParams& params, const QuerySource query_type,
                              const TPipelineFragmentParamsList& parent);

    void remove_pipeline_context(std::pair<TUniqueId, int> key);

    // TODO(zc): report this is over
    Status exec_plan_fragment(const TExecPlanFragmentParams& params, const QuerySource query_type,
                              const FinishCallback& cb);

    Status exec_plan_fragment(const TPipelineFragmentParams& params, const QuerySource query_type,
                              const FinishCallback& cb, const TPipelineFragmentParamsList& parent);

    Status start_query_execution(const PExecPlanFragmentStartRequest* request);

    Status trigger_pipeline_context_report(const ReportStatusRequest,
                                           std::shared_ptr<pipeline::PipelineFragmentContext>&&);

    // Can be used in both version.
    MOCK_FUNCTION void cancel_query(const TUniqueId query_id, const Status reason);

    void cancel_worker();

    void debug(std::stringstream& ss) override;

    // input: TQueryPlanInfo fragment_instance_id
    // output: selected_columns
    // execute external query, all query info are packed in TScanOpenParams
    Status exec_external_plan_fragment(const TScanOpenParams& params,
                                       const TQueryPlanInfo& t_query_plan_info,
                                       const TUniqueId& query_id,
                                       const TUniqueId& fragment_instance_id,
                                       std::vector<TScanColumnDesc>* selected_columns);

    Status apply_filterv2(const PPublishFilterRequestV2* request,
                          butil::IOBufAsZeroCopyInputStream* attach_data);

    Status merge_filter(const PMergeFilterRequest* request,
                        butil::IOBufAsZeroCopyInputStream* attach_data);

    Status send_filter_size(const PSendFilterSizeRequest* request);

    Status sync_filter_size(const PSyncFilterSizeRequest* request);

    std::string to_http_path(const std::string& file_name);

    void coordinator_callback(const ReportStatusRequest& req);

    ThreadPool* get_thread_pool() { return _thread_pool.get(); }

    // When fragment mgr is going to stop, the _stop_background_threads_latch is set to 0
    // and other module that use fragment mgr's thread pool should get this signal and exit.
    bool shutting_down() { return _stop_background_threads_latch.count() == 0; }

    int32_t running_query_num() { return cast_set<int32_t>(_query_ctx_map.num_items()); }

    std::string dump_pipeline_tasks(int64_t duration = 0);
    std::string dump_pipeline_tasks(TUniqueId& query_id);

    void get_runtime_query_info(std::vector<std::weak_ptr<ResourceContext>>* _resource_ctx_list);

    Status get_realtime_exec_status(const TUniqueId& query_id,
                                    TReportExecStatusParams* exec_status);
    // get the query statistics of with a given query id
    Status get_query_statistics(const TUniqueId& query_id, TQueryStatistics* query_stats);

    std::shared_ptr<QueryContext> get_query_ctx(const TUniqueId& query_id);

private:
    struct BrpcItem {
        TNetworkAddress network_address;
        std::vector<std::weak_ptr<QueryContext>> queries;
    };

    Status _get_or_create_query_ctx(const TPipelineFragmentParams& params,
                                    const TPipelineFragmentParamsList& parent,
                                    QuerySource query_type,
                                    std::shared_ptr<QueryContext>& query_ctx);

    void _check_brpc_available(const std::shared_ptr<PBackendService_Stub>& brpc_stub,
                               const BrpcItem& brpc_item);

    // This is input params
    ExecEnv* _exec_env = nullptr;

    // (QueryID, FragmentID) -> PipelineFragmentContext
    ConcurrentContextMap<std::pair<TUniqueId, int>,
                         std::shared_ptr<pipeline::PipelineFragmentContext>,
                         pipeline::PipelineFragmentContext>
            _pipeline_map;

    // query id -> QueryContext
    ConcurrentContextMap<TUniqueId, std::weak_ptr<QueryContext>, QueryContext> _query_ctx_map;
    std::unordered_map<TUniqueId, std::unordered_map<int, int64_t>> _bf_size_map;

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _cancel_thread;
    // This pool is used as global async task pool
    std::unique_ptr<ThreadPool> _thread_pool;

    std::shared_ptr<MetricEntity> _entity;
    UIntGauge* timeout_canceled_fragment_count = nullptr;
};

uint64_t get_fragment_executing_count();
uint64_t get_fragment_last_active_time();
#include "common/compile_check_end.h"
} // namespace doris
