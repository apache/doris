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

#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/DorisExternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/ref_counted.h"
#include "http/rest_monitor_iface.h"
#include "runtime_filter_mgr.h"
#include "util/countdown_latch.h"
#include "util/hash_util.hpp"
#include "util/metrics.h"
#include "util/thread.h"

namespace butil {
class IOBufAsZeroCopyInputStream;
}

namespace doris {

namespace pipeline {
class PipelineFragmentContext;
}

class QueryFragmentsCtx;
class ExecEnv;
class FragmentExecState;
class PlanFragmentExecutor;
class ThreadPool;
class TExecPlanFragmentParams;
class TExecPlanFragmentParamsList;
class TUniqueId;
class RuntimeFilterMergeController;
class StreamLoadPipe;

std::string to_load_error_http_path(const std::string& file_name);

// This class used to manage all the fragment execute in this instance
class FragmentMgr : public RestMonitorIface {
public:
    using FinishCallback = std::function<void(PlanFragmentExecutor*)>;

    FragmentMgr(ExecEnv* exec_env);
    virtual ~FragmentMgr();

    // execute one plan fragment
    Status exec_plan_fragment(const TExecPlanFragmentParams& params);

    Status exec_pipeline(const TExecPlanFragmentParams& params);

    void remove_pipeline_context(
            std::shared_ptr<pipeline::PipelineFragmentContext> pipeline_context);

    // TODO(zc): report this is over
    Status exec_plan_fragment(const TExecPlanFragmentParams& params, FinishCallback cb);

    Status start_query_execution(const PExecPlanFragmentStartRequest* request);

    void cancel(const TUniqueId& fragment_id) {
        cancel(fragment_id, PPlanFragmentCancelReason::INTERNAL_ERROR);
    }

    void cancel(const TUniqueId& fragment_id, const PPlanFragmentCancelReason& reason,
                const std::string& msg = "");

    void cancel_worker();

    virtual void debug(std::stringstream& ss);

    // input: TScanOpenParams fragment_instance_id
    // output: selected_columns
    // execute external query, all query info are packed in TScanOpenParams
    Status exec_external_plan_fragment(const TScanOpenParams& params,
                                       const TUniqueId& fragment_instance_id,
                                       std::vector<TScanColumnDesc>* selected_columns);

    Status apply_filter(const PPublishFilterRequest* request,
                        butil::IOBufAsZeroCopyInputStream* attach_data);

    Status merge_filter(const PMergeFilterRequest* request,
                        butil::IOBufAsZeroCopyInputStream* attach_data);

    void set_pipe(const TUniqueId& fragment_instance_id, std::shared_ptr<StreamLoadPipe> pipe);

    std::shared_ptr<StreamLoadPipe> get_pipe(const TUniqueId& fragment_instance_id);

private:
    void _exec_actual(std::shared_ptr<FragmentExecState> exec_state, FinishCallback cb);

    void _set_scan_concurrency(const TExecPlanFragmentParams& params,
                               QueryFragmentsCtx* fragments_ctx);

    bool _is_scan_node(const TPlanNodeType::type& type);

    // This is input params
    ExecEnv* _exec_env;

    std::mutex _lock;

    std::condition_variable _cv;

    std::mutex _lock_for_shared_hash_table;
    std::condition_variable _cv_for_sharing_hashtable;

    // Make sure that remove this before no data reference FragmentExecState
    std::unordered_map<TUniqueId, std::shared_ptr<FragmentExecState>> _fragment_map;

    std::unordered_map<TUniqueId, std::shared_ptr<pipeline::PipelineFragmentContext>> _pipeline_map;

    // query id -> QueryFragmentsCtx
    std::unordered_map<TUniqueId, std::shared_ptr<QueryFragmentsCtx>> _fragments_ctx_map;
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
