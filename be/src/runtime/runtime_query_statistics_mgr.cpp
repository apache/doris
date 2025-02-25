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

#include "runtime/runtime_query_statistics_mgr.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <thrift/TApplicationException.h>

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/logging.h"
#include "exec/schema_scanner/schema_scanner_helper.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/debug_util.h"
#include "util/thrift_client.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/core/block.h"

namespace doris {
// TODO: Currently this function is only used to report profile.
// In the future, all exec status and query statistics should be reported
// thorough this function.
static Status _do_report_exec_stats_rpc(const TNetworkAddress& coor_addr,
                                        const TReportExecStatusParams& req,
                                        TReportExecStatusResult& res) {
    Status client_status;
    FrontendServiceConnection rpc_client(ExecEnv::GetInstance()->frontend_client_cache(), coor_addr,
                                         &client_status);
    if (!client_status.ok()) {
        LOG_WARNING(
                "Could not get client rpc client of {} when reporting profiles, reason is {}, "
                "not reporting, profile will be lost",
                PrintThriftNetworkAddress(coor_addr), client_status.to_string());
        return Status::RpcError("Client rpc client failed");
    }

    VLOG_DEBUG << "Sending profile";

    try {
        try {
            rpc_client->reportExecStatus(res, req);
        } catch (const apache::thrift::transport::TTransportException& e) {
            LOG_WARNING("Transport exception from {}, reason: {}, reopening",
                        PrintThriftNetworkAddress(coor_addr), e.what());
            client_status = rpc_client.reopen(config::thrift_rpc_timeout_ms);
            if (!client_status.ok()) {
                LOG_WARNING("Reopen failed, reason: {}", client_status.to_string());
                return Status::RpcError("Open rpc client failed");
            }

            rpc_client->reportExecStatus(res, req);
        }
    } catch (apache::thrift::TApplicationException& e) {
        if (e.getType() == e.UNKNOWN_METHOD) {
            LOG_WARNING(
                    "Failed to report query profile to {} due to {}, usually because the frontend "
                    "is not upgraded, check the version",
                    PrintThriftNetworkAddress(coor_addr), e.what());
        } else {
            LOG_WARNING(
                    "Failed to report query profile to {}, reason: {}, you can see fe log for "
                    "details.",
                    PrintThriftNetworkAddress(coor_addr), e.what());
        }
        return Status::RpcError("Send stats failed");
    } catch (std::exception& e) {
        LOG_WARNING(
                "Failed to report query profile to {}, reason: {}, you can see fe log for details.",
                PrintThriftNetworkAddress(coor_addr), e.what());
        return Status::RpcError("Send report query profile failed");
    }

    return Status::OK();
}

TReportExecStatusParams RuntimeQueryStatisticsMgr::create_report_exec_status_params(
        const TUniqueId& query_id,
        std::unordered_map<int32, std::vector<std::shared_ptr<TRuntimeProfileTree>>>
                fragment_id_to_profile,
        std::vector<std::shared_ptr<TRuntimeProfileTree>> load_channel_profiles, bool is_done) {
    // This function will clear the data of fragment_id_to_profile and load_channel_profiles.
    TQueryProfile profile;
    profile.__set_query_id(query_id);

    std::map<int32_t, std::vector<TDetailedReportParams>> fragment_id_to_profile_req;

    for (const auto& entry : fragment_id_to_profile) {
        int32_t fragment_id = entry.first;
        const std::vector<std::shared_ptr<TRuntimeProfileTree>>& fragment_profile = entry.second;
        std::vector<TDetailedReportParams> detailed_params;
        bool is_first = true;
        for (auto pipeline_profile : fragment_profile) {
            if (pipeline_profile == nullptr) {
                auto msg = fmt::format("Register fragment profile {} {} failed, profile is null",
                                       print_id(query_id), fragment_id);
                DCHECK(false) << msg;
                LOG_ERROR(msg);
                continue;
            }

            TDetailedReportParams tmp;
            THRIFT_MOVE_VALUES(tmp, profile, *pipeline_profile);
            // First profile is fragment level
            tmp.__set_is_fragment_level(is_first);
            is_first = false;
            // tmp.fragment_instance_id is not needed for pipeline x
            detailed_params.push_back(std::move(tmp));
        }

        fragment_id_to_profile_req[fragment_id] = std::move(detailed_params);
    }

    if (fragment_id_to_profile_req.empty()) {
        LOG_WARNING("No fragment profile found for query {}", print_id(query_id));
    }

    profile.__set_fragment_id_to_profile(fragment_id_to_profile_req);

    std::vector<TRuntimeProfileTree> load_channel_profiles_req;
    for (auto load_channel_profile : load_channel_profiles) {
        if (load_channel_profile == nullptr) {
            auto msg = fmt::format(
                    "Register fragment profile {} {} failed, load channel profile is null",
                    print_id(query_id), -1);
            DCHECK(false) << msg;
            LOG_ERROR(msg);
            continue;
        }

        load_channel_profiles_req.push_back(std::move(*load_channel_profile));
    }

    if (!load_channel_profiles_req.empty()) {
        THRIFT_MOVE_VALUES(profile, load_channel_profiles, load_channel_profiles_req);
    }

    TReportExecStatusParams req;
    THRIFT_MOVE_VALUES(req, query_profile, profile);
    req.__set_backend_id(ExecEnv::GetInstance()->cluster_info()->backend_id);
    // invalid query id to avoid API compatibility during upgrade
    req.__set_query_id(TUniqueId());
    req.__set_done(is_done);

    return req;
}

void RuntimeQueryStatisticsMgr::start_report_thread() {
    if (started.load()) {
        DCHECK(false) << "report thread has been started";
        LOG_ERROR("report thread has been started");
        return;
    }

    started.store(true);

    for (size_t i = 0; i < config::report_exec_status_thread_num; ++i) {
        this->_report_profile_threads.emplace_back(std::make_unique<std::thread>(
                &RuntimeQueryStatisticsMgr::report_query_profiles_thread, this));
    }
}

void RuntimeQueryStatisticsMgr::report_query_profiles_thread() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(_report_profile_mutex);

            while (_profile_map.empty() && !_report_profile_thread_stop) {
                _report_profile_cv.wait_for(lock, std::chrono::seconds(3));
            }
        }

        _report_query_profiles_function();

        {
            std::lock_guard<std::mutex> lg(_report_profile_mutex);

            if (_report_profile_thread_stop) {
                LOG_INFO("Report profile thread stopped");
                return;
            }
        }
    }
}

void RuntimeQueryStatisticsMgr::trigger_report_profile() {
    std::unique_lock<std::mutex> lock(_report_profile_mutex);
    _report_profile_cv.notify_one();
}

void RuntimeQueryStatisticsMgr::stop_report_thread() {
    if (!started) {
        return;
    }

    {
        std::unique_lock<std::mutex> lock(_report_profile_mutex);
        _report_profile_thread_stop = true;
        LOG_INFO("All report threads are going to stop");
        _report_profile_cv.notify_all();
    }

    for (const auto& thread : _report_profile_threads) {
        thread->join();
    }

    LOG_INFO("All report threads stopped");
}

void RuntimeQueryStatisticsMgr::register_fragment_profile(
        const TUniqueId& query_id, const TNetworkAddress& coor_addr, int32_t fragment_id,
        std::vector<std::shared_ptr<TRuntimeProfileTree>> p_profiles,
        std::shared_ptr<TRuntimeProfileTree> load_channel_profile_x) {
    for (const auto& p : p_profiles) {
        if (p == nullptr) {
            auto msg = fmt::format("Register fragment profile {} {} failed, profile is null",
                                   print_id(query_id), fragment_id);
            DCHECK(false) << msg;
            LOG_ERROR(msg);
            return;
        }
    }

    std::lock_guard<std::shared_mutex> lg(_query_profile_map_lock);

    if (!_profile_map.contains(query_id)) {
        _profile_map[query_id] = std::make_tuple(
                coor_addr,
                std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>>());
    }

    std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>>&
            fragment_profile_map = std::get<1>(_profile_map[query_id]);
    fragment_profile_map.insert(std::make_pair(fragment_id, p_profiles));

    if (load_channel_profile_x != nullptr) {
        _load_channel_profile_map[std::make_pair(query_id, fragment_id)] = load_channel_profile_x;
    }

    LOG_INFO("register x profile done {}, fragment {}, profiles {}", print_id(query_id),
             fragment_id, p_profiles.size());
}

void RuntimeQueryStatisticsMgr::_report_query_profiles_function() {
    decltype(_profile_map) profile_copy;
    decltype(_load_channel_profile_map) load_channel_profile_copy;
    VLOG_DEBUG << "Beging reporting profile";
    {
        std::lock_guard<std::shared_mutex> lg(_query_profile_map_lock);
        _profile_map.swap(profile_copy);
        _load_channel_profile_map.swap(load_channel_profile_copy);
    }
    VLOG_DEBUG << "After swap profile map";
    // query_id -> {coordinator_addr, {fragment_id -> std::vectpr<pipeline_profile>}}
    for (auto& entry : profile_copy) {
        const auto& query_id = entry.first;
        const auto& coor_addr = std::get<0>(entry.second);
        auto& fragment_profile_map = std::get<1>(entry.second);

        if (fragment_profile_map.empty()) {
            auto msg = fmt::format("Query {} does not have profile", print_id(query_id));
            DCHECK(false) << msg;
            LOG_ERROR(msg);
            continue;
        }

        std::vector<std::shared_ptr<TRuntimeProfileTree>> load_channel_profiles;
        for (auto load_channel_profile : load_channel_profile_copy) {
            if (load_channel_profile.second == nullptr) {
                auto msg = fmt::format(
                        "Register fragment profile {} {} failed, load channel profile is null",
                        print_id(query_id), -1);
                DCHECK(false) << msg;
                LOG_ERROR(msg);
                continue;
            }

            load_channel_profiles.push_back(load_channel_profile.second);
        }

        TReportExecStatusParams req = create_report_exec_status_params(
                query_id, std::move(fragment_profile_map), std::move(load_channel_profiles),
                /*is_done=*/true);
        TReportExecStatusResult res;

        auto rpc_status = _do_report_exec_stats_rpc(coor_addr, req, res);

        if (res.status.status_code != TStatusCode::OK || !rpc_status.ok()) {
            LOG_WARNING("Query {} send profile to {} failed", print_id(query_id),
                        PrintThriftNetworkAddress(coor_addr));
        } else {
            LOG_INFO("Send {} profile succeed", print_id(query_id));
        }
    }
}

void RuntimeQueryStatisticsMgr::register_resource_context(
        std::string query_id, std::shared_ptr<ResourceContext> resource_ctx) {
    std::lock_guard<std::shared_mutex> write_lock(_resource_contexts_map_lock);
    // Note: `group_commit_insert` will use the same `query_id` to submit multiple load tasks in sequence.
    // After the previous load task ends but QueryStatistics has not been reported to FE,
    // if the next load task with the same `query_id` starts to execute, `register_resource_context` will
    // find that `query_id` already exists in _resource_contexts_map.
    // At this time, directly overwriting the `resource_ctx` corresponding to the `query_id`
    // in `register_resource_context` will cause the previous load task not to be reported to FE.
    // DCHECK(_resource_contexts_map.find(query_id) == _resource_contexts_map.end());
    _resource_contexts_map[query_id] = resource_ctx;
}

void RuntimeQueryStatisticsMgr::report_runtime_query_statistics() {
    int64_t be_id = ExecEnv::GetInstance()->cluster_info()->backend_id;
    // 1 get query statistics map
    std::map<TNetworkAddress, std::map<std::string, TQueryStatistics>> fe_qs_map;
    std::map<std::string, std::pair<bool, bool>> qs_status; // <finished, timeout>
    {
        std::lock_guard<std::shared_mutex> write_lock(_resource_contexts_map_lock);
        int64_t current_time = MonotonicMillis();
        int64_t conf_qs_timeout = config::query_statistics_reserve_timeout_ms;
        for (auto& [query_id, resource_ctx] : _resource_contexts_map) {
            if (resource_ctx->task_controller()->query_type() == TQueryType::EXTERNAL) {
                continue;
            }
            if (fe_qs_map.find(resource_ctx->task_controller()->fe_addr()) == fe_qs_map.end()) {
                std::map<std::string, TQueryStatistics> tmp_map;
                fe_qs_map[resource_ctx->task_controller()->fe_addr()] = std::move(tmp_map);
            }

            TQueryStatistics ret_t_qs;
            resource_ctx->to_thrift_query_statistics(&ret_t_qs);
            fe_qs_map.at(resource_ctx->task_controller()->fe_addr())[query_id] = ret_t_qs;

            bool is_query_finished = resource_ctx->task_controller()->is_finished();
            bool is_timeout_after_finish = false;
            if (is_query_finished) {
                is_timeout_after_finish =
                        (current_time - resource_ctx->task_controller()->finish_time()) >
                        conf_qs_timeout;
            }
            qs_status[query_id] = std::make_pair(is_query_finished, is_timeout_after_finish);
        }
    }

    // 2 report query statistics to fe
    std::map<TNetworkAddress, bool> rpc_result;
    for (auto& [addr, qs_map] : fe_qs_map) {
        rpc_result[addr] = false;
        // 2.1 get client
        Status coord_status;
        FrontendServiceConnection coord(ExecEnv::GetInstance()->frontend_client_cache(), addr,
                                        &coord_status);
        std::string add_str = PrintThriftNetworkAddress(addr);
        if (!coord_status.ok()) {
            std::stringstream ss;
            LOG(WARNING) << "[report_query_statistics]could not get client " << add_str
                         << " when report workload runtime stats, reason:"
                         << coord_status.to_string();
            continue;
        }

        // 2.2 send report
        TReportWorkloadRuntimeStatusParams report_runtime_params;
        report_runtime_params.__set_backend_id(be_id);
        report_runtime_params.__set_query_statistics_map(qs_map);

        TReportExecStatusParams params;
        params.__set_report_workload_runtime_status(report_runtime_params);

        TReportExecStatusResult res;
        Status rpc_status;

        try {
            try {
                coord->reportExecStatus(res, params);
                rpc_result[addr] = true;
            } catch (apache::thrift::transport::TTransportException& e) {
                LOG_WARNING(
                        "[report_query_statistics] report to fe {} failed, reason:{}, try reopen.",
                        add_str, e.what());
                rpc_status = coord.reopen();
                if (!rpc_status.ok()) {
                    LOG_WARNING(
                            "[report_query_statistics]reopen thrift client failed when report "
                            "workload runtime statistics to {}, reason: {}",
                            add_str, rpc_status.to_string());
                } else {
                    coord->reportExecStatus(res, params);
                    rpc_result[addr] = true;
                }
            }
        } catch (apache::thrift::TApplicationException& e) {
            LOG_WARNING(
                    "[report_query_statistics]fe {} throw exception when report statistics, "
                    "reason:{}, you can see fe log for details.",
                    add_str, e.what());
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG_WARNING(
                    "[report_query_statistics]report workload runtime statistics to {} failed,  "
                    "reason: {}",
                    add_str, e.what());
        } catch (std::exception& e) {
            LOG_WARNING(
                    "[report_query_statistics]unknown exception when report workload runtime "
                    "statistics to {}, reason:{}. ",
                    add_str, e.what());
        }
    }

    //  3 when query is finished and (last rpc is send success), remove finished query statistics
    if (fe_qs_map.empty()) {
        return;
    }

    {
        std::lock_guard<std::shared_mutex> write_lock(_resource_contexts_map_lock);
        for (auto& [addr, qs_map] : fe_qs_map) {
            bool is_rpc_success = rpc_result[addr];
            for (auto& [query_id, qs] : qs_map) {
                auto& qs_status_pair = qs_status[query_id];
                bool is_query_finished = qs_status_pair.first;
                bool is_timeout_after_finish = qs_status_pair.second;
                if ((is_rpc_success && is_query_finished) || is_timeout_after_finish) {
                    _resource_contexts_map.erase(query_id);
                }
            }
        }
    }
}

void RuntimeQueryStatisticsMgr::get_active_be_tasks_block(vectorized::Block* block) {
    std::shared_lock<std::shared_mutex> read_lock(_resource_contexts_map_lock);
    int64_t be_id = ExecEnv::GetInstance()->cluster_info()->backend_id;

    // block's schema come from SchemaBackendActiveTasksScanner::_s_tbls_columns
    for (auto& [query_id, resource_ctx] : _resource_contexts_map) {
        TQueryStatistics tqs;
        resource_ctx->to_thrift_query_statistics(&tqs);
        SchemaScannerHelper::insert_int64_value(0, be_id, block);
        SchemaScannerHelper::insert_string_value(
                1, resource_ctx->task_controller()->fe_addr().hostname, block);
        auto wg = resource_ctx->workload_group();
        SchemaScannerHelper::insert_int64_value(2, wg ? wg->id() : -1, block);
        SchemaScannerHelper::insert_string_value(3, query_id, block);

        int64_t task_time =
                resource_ctx->task_controller()->is_finished()
                        ? resource_ctx->task_controller()->finish_time() -
                                  resource_ctx->task_controller()->start_time()
                        : MonotonicMillis() - resource_ctx->task_controller()->start_time();
        SchemaScannerHelper::insert_int64_value(4, task_time, block);
        SchemaScannerHelper::insert_int64_value(5, tqs.cpu_ms, block);
        SchemaScannerHelper::insert_int64_value(6, tqs.scan_rows, block);
        SchemaScannerHelper::insert_int64_value(7, tqs.scan_bytes, block);
        SchemaScannerHelper::insert_int64_value(8, tqs.max_peak_memory_bytes, block);
        SchemaScannerHelper::insert_int64_value(9, tqs.current_used_memory_bytes, block);
        SchemaScannerHelper::insert_int64_value(10, tqs.shuffle_send_bytes, block);
        SchemaScannerHelper::insert_int64_value(11, tqs.shuffle_send_rows, block);

        std::stringstream ss;
        ss << resource_ctx->task_controller()->query_type();
        SchemaScannerHelper::insert_string_value(12, ss.str(), block);
        SchemaScannerHelper::insert_int64_value(13, tqs.spill_write_bytes_to_local_storage, block);
        SchemaScannerHelper::insert_int64_value(14, tqs.spill_read_bytes_from_local_storage, block);
    }
}

} // namespace doris
