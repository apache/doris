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

#include <gen_cpp/Data_types.h>
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/Types_types.h>

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "gutil/integral_types.h"
#include "runtime/workload_management/resource_context.h"
#include "runtime/workload_management/workload_condition.h"

namespace doris {

namespace vectorized {
class Block;
} // namespace vectorized

class RuntimeQueryStatisticsMgr {
public:
    RuntimeQueryStatisticsMgr() = default;
    ~RuntimeQueryStatisticsMgr() = default;

    static TReportExecStatusParams create_report_exec_status_params(
            const TUniqueId& q_id,
            std::unordered_map<int32, std::vector<std::shared_ptr<TRuntimeProfileTree>>>
                    fragment_id_to_profile,
            std::vector<std::shared_ptr<TRuntimeProfileTree>> load_channel_profile, bool is_done);

    void register_resource_context(std::string query_id,
                                   std::shared_ptr<ResourceContext> resource_ctx);

    void report_runtime_query_statistics();

    // used for backend_active_tasks
    void get_active_be_tasks_block(vectorized::Block* block);

    void start_report_thread();
    void report_query_profiles_thread();
    void trigger_report_profile();
    void stop_report_thread();

    void register_fragment_profile(const TUniqueId& query_id, const TNetworkAddress& const_addr,
                                   int32_t fragment_id,
                                   std::vector<std::shared_ptr<TRuntimeProfileTree>> p_profiles,
                                   std::shared_ptr<TRuntimeProfileTree> load_channel_profile_x);

private:
    std::shared_mutex _resource_contexts_map_lock;
    // Must be shared_ptr of ResourceContext, because ResourceContext can only be removed from
    // _resource_contexts_map after QueryStatistics is reported to FE,
    // at which time the Query may have ended.
    std::map<std::string, std::shared_ptr<ResourceContext>> _resource_contexts_map;

    std::mutex _report_profile_mutex;
    std::atomic_bool started = false;
    std::vector<std::unique_ptr<std::thread>> _report_profile_threads;
    std::condition_variable _report_profile_cv;
    bool _report_profile_thread_stop = false;

    void _report_query_profiles_function();

    std::shared_mutex _query_profile_map_lock;

    // query_id -> {coordinator_addr, {fragment_id -> std::vectpr<pipeline_profile>}}
    std::unordered_map<
            TUniqueId,
            std::tuple<TNetworkAddress,
                       std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>>>>
            _profile_map;
    std::unordered_map<std::pair<TUniqueId, int32_t>, std::shared_ptr<TRuntimeProfileTree>>
            _load_channel_profile_map;
};

} // namespace doris