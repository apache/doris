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
#include <stdint.h>

#include <condition_variable>
#include <functional>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gutil/ref_counted.h"
#include "http/rest_monitor_iface.h"
#include "runtime/query_context.h"
#include "runtime_filter_mgr.h"
#include "util/countdown_latch.h"
#include "util/hash_util.hpp" // IWYU pragma: keep
#include "util/metrics.h"

namespace butil {
class IOBufAsZeroCopyInputStream;
}

namespace doris {

namespace pipeline {
class PipelineFragmentContext;
class PipelineXFragmentContext;
} // namespace pipeline
class QueryContext;
class ExecEnv;
class PlanFragmentExecutor;
class ThreadPool;
class TExecPlanFragmentParams;
class PExecPlanFragmentStartRequest;
class PMergeFilterRequest;
class PPublishFilterRequest;
class RuntimeProfile;
class RuntimeState;
class TPipelineFragmentParams;
class TPipelineInstanceParams;
class TScanColumnDesc;
class TScanOpenParams;
class Thread;

std::string to_load_error_http_path(const std::string& file_name);

// This class used to manage all the fragment execute in this instance
class FragmentMgr : public RestMonitorIface {
public:
    using FinishCallback = std::function<void(RuntimeState*, Status*)>;

    FragmentMgr(ExecEnv* exec_env);
    ~FragmentMgr() override;

    // execute one plan fragment
    Status exec_plan_fragment(const TExecPlanFragmentParams& params);

    Status exec_plan_fragment(const TPipelineFragmentParams& params);

    void remove_pipeline_context(
            std::shared_ptr<pipeline::PipelineFragmentContext> pipeline_context);

    void remove_pipeline_context(
            std::shared_ptr<pipeline::PipelineXFragmentContext> pipeline_context);

    // TODO(zc): report this is over
    Status exec_plan_fragment(const TExecPlanFragmentParams& params, const FinishCallback& cb);

    Status exec_plan_fragment(const TPipelineFragmentParams& params, const FinishCallback& cb);

    Status start_query_execution(const PExecPlanFragmentStartRequest* request);

    void cancel(const TUniqueId& fragment_id) {
        cancel(fragment_id, PPlanFragmentCancelReason::INTERNAL_ERROR);
    }

    void cancel(const TUniqueId& fragment_id, const PPlanFragmentCancelReason& reason,
                const std::string& msg = "");

    void cancel_query(const TUniqueId& query_id, const PPlanFragmentCancelReason& reason,
                      const std::string& msg = "");

    bool query_is_canceled(const TUniqueId& query_id);

    void cancel_worker();

    void debug(std::stringstream& ss) override;

    // input: TScanOpenParams fragment_instance_id
    // output: selected_columns
    // execute external query, all query info are packed in TScanOpenParams
    Status exec_external_plan_fragment(const TScanOpenParams& params,
                                       const TUniqueId& fragment_instance_id,
                                       std::vector<TScanColumnDesc>* selected_columns);

    Status apply_filter(const PPublishFilterRequest* request,
                        butil::IOBufAsZeroCopyInputStream* attach_data);

    Status apply_filterv2(const PPublishFilterRequestV2* request,
                          butil::IOBufAsZeroCopyInputStream* attach_data);

    Status merge_filter(const PMergeFilterRequest* request,
                        butil::IOBufAsZeroCopyInputStream* attach_data);

    std::string to_http_path(const std::string& file_name);

    void coordinator_callback(const ReportStatusRequest& req);

    ThreadPool* get_thread_pool() { return _thread_pool.get(); }

private:
    void _exec_actual(std::shared_ptr<PlanFragmentExecutor> fragment_executor,
                      const FinishCallback& cb);

    template <typename Param>
    void _set_scan_concurrency(const Param& params, QueryContext* query_ctx);

    void _setup_shared_hashtable_for_broadcast_join(const TExecPlanFragmentParams& params,
                                                    QueryContext* query_ctx);

    void _setup_shared_hashtable_for_broadcast_join(const TPipelineFragmentParams& params,
                                                    const TPipelineInstanceParams& local_params,
                                                    QueryContext* query_ctx);

    template <typename Params>
    Status _get_query_ctx(const Params& params, TUniqueId query_id, bool pipeline,
                          std::shared_ptr<QueryContext>& query_ctx);

    // This is input params
    ExecEnv* _exec_env;

    std::mutex _lock;

    std::condition_variable _cv;

    // Make sure that remove this before no data reference PlanFragmentExecutor
    std::unordered_map<TUniqueId, std::shared_ptr<PlanFragmentExecutor>> _fragment_map;

    std::unordered_map<TUniqueId, std::shared_ptr<pipeline::PipelineFragmentContext>> _pipeline_map;

    // query id -> QueryContext
    std::unordered_map<TUniqueId, std::shared_ptr<QueryContext>> _query_ctx_map;
    std::unordered_map<TUniqueId, std::unordered_map<int, int64_t>> _bf_size_map;

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _cancel_thread;
    // every job is a pool
    std::unique_ptr<ThreadPool> _thread_pool;

    std::shared_ptr<MetricEntity> _entity = nullptr;
    UIntGauge* timeout_canceled_fragment_count = nullptr;

    RuntimeFilterMergeController _runtimefilter_controller;
};

} // namespace doris
