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

#include "runtime/fragment_mgr.h"

#include <brpc/controller.h>
#include <bvar/latency_recorder.h>
#include <exprs/runtime_filter.h>
#include <fmt/format.h>
#include <gen_cpp/DorisExternalService_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Planner_types.h>
#include <gen_cpp/QueryPlanExtra_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <pthread.h>
#include <stddef.h>
#include <sys/time.h>
#include <thrift/TApplicationException.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TTransportException.h>
#include <time.h>

#include <algorithm>
#include <atomic>

#include "common/status.h"
#include "pipeline/pipeline_x/pipeline_x_fragment_context.h"
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/utils.h"
#include "gutil/strings/substitute.h"
#include "io/fs/stream_load_pipe.h"
#include "pipeline/pipeline_fragment_context.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/frontend_info.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/primitive_type.h"
#include "runtime/query_context.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "runtime/workload_management/workload_query_info.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/debug_points.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/hash_util.hpp"
#include "util/mem_info.h"
#include "util/network_util.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
#include "util/url_coding.h"
#include "vec/runtime/shared_hash_table_controller.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(fragment_instance_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(timeout_canceled_fragment_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(fragment_thread_pool_queue_size, MetricUnit::NOUNIT);
bvar::LatencyRecorder g_fragmentmgr_prepare_latency("doris_FragmentMgr", "prepare");
bvar::Adder<int64_t> g_pipeline_fragment_instances_count("doris_pipeline_fragment_instances_count");

std::string to_load_error_http_path(const std::string& file_name) {
    if (file_name.empty()) {
        return "";
    }
    std::stringstream url;
    url << "http://" << get_host_port(BackendOptions::get_localhost(), config::webserver_port)
        << "/api/_load_error_log?"
        << "file=" << file_name;
    return url.str();
}

using apache::thrift::TException;
using apache::thrift::transport::TTransportException;

static Status _do_fetch_running_queries_rpc(const FrontendInfo& fe_info,
                                            std::unordered_set<TUniqueId>& query_set) {
    TFetchRunningQueriesResult rpc_result;
    TFetchRunningQueriesRequest rpc_request;

    Status client_status;
    const int32 timeout_ms = 3 * 1000;
    FrontendServiceConnection rpc_client(ExecEnv::GetInstance()->frontend_client_cache(),
                                         fe_info.info.coordinator_address, timeout_ms,
                                         &client_status);
    // Abort this fe.
    if (!client_status.ok()) {
        LOG_WARNING("Failed to get client for {}, reason is {}",
                    PrintThriftNetworkAddress(fe_info.info.coordinator_address),
                    client_status.to_string());
        return Status::InternalError("Failed to get client for {}, reason is {}",
                                     PrintThriftNetworkAddress(fe_info.info.coordinator_address),
                                     client_status.to_string());
    }

    // do rpc
    try {
        try {
            rpc_client->fetchRunningQueries(rpc_result, rpc_request);
        } catch (const apache::thrift::transport::TTransportException& e) {
            LOG_WARNING("Transport exception reason: {}, reopening", e.what());
            client_status = rpc_client.reopen(config::thrift_rpc_timeout_ms);
            if (!client_status.ok()) {
                LOG_WARNING("Reopen failed, reason: {}", client_status.to_string_no_stack());
                return Status::InternalError("Reopen failed, reason: {}",
                                             client_status.to_string_no_stack());
            }

            rpc_client->fetchRunningQueries(rpc_result, rpc_request);
        }
    } catch (apache::thrift::TException& e) {
        // During upgrading cluster or meet any other network error.
        LOG_WARNING("Failed to fetch running queries from {}, reason: {}",
                    PrintThriftNetworkAddress(fe_info.info.coordinator_address), e.what());
        return Status::InternalError("Failed to fetch running queries from {}, reason: {}",
                                     PrintThriftNetworkAddress(fe_info.info.coordinator_address),
                                     e.what());
    }

    // Avoid logic error in frontend.
    if (rpc_result.__isset.status == false || rpc_result.status.status_code != TStatusCode::OK) {
        LOG_WARNING("Failed to fetch running queries from {}, reason: {}",
                    PrintThriftNetworkAddress(fe_info.info.coordinator_address),
                    doris::to_string(rpc_result.status.status_code));
        return Status::InternalError("Failed to fetch running queries from {}, reason: {}",
                                     PrintThriftNetworkAddress(fe_info.info.coordinator_address),
                                     doris::to_string(rpc_result.status.status_code));
    }

    if (rpc_result.__isset.running_queries == false) {
        return Status::InternalError("Failed to fetch running queries from {}, reason: {}",
                                     PrintThriftNetworkAddress(fe_info.info.coordinator_address),
                                     "running_queries is not set");
    }

    query_set = std::unordered_set<TUniqueId>(rpc_result.running_queries.begin(),
                                              rpc_result.running_queries.end());

    return Status::OK();
};

static std::map<int64_t, std::unordered_set<TUniqueId>> _get_all_running_queries_from_fe() {
    const std::map<TNetworkAddress, FrontendInfo>& running_fes =
            ExecEnv::GetInstance()->get_running_frontends();

    std::map<int64_t, std::unordered_set<TUniqueId>> result;
    std::vector<FrontendInfo> qualified_fes;

    for (const auto& fe : running_fes) {
        // Only consider normal frontend.
        if (fe.first.port != 0 && fe.second.info.process_uuid != 0) {
            qualified_fes.push_back(fe.second);
        } else {
            return {};
        }
    }

    for (const auto& fe_addr : qualified_fes) {
        const int64_t process_uuid = fe_addr.info.process_uuid;
        std::unordered_set<TUniqueId> query_set;
        Status st = _do_fetch_running_queries_rpc(fe_addr, query_set);
        if (!st.ok()) {
            // Empty result, cancel worker will not do anything
            return {};
        }

        // frontend_info and process_uuid has been checked in rpc threads.
        result[process_uuid] = query_set;
    }

    return result;
}

inline uint32_t get_map_id(const TUniqueId& query_id, size_t capacity) {
    uint32_t value = HashUtil::hash(&query_id.lo, 8, 0);
    value = HashUtil::hash(&query_id.hi, 8, value);
    return value % capacity;
}

inline uint32_t get_map_id(std::pair<TUniqueId, int> key, size_t capacity) {
    uint32_t value = HashUtil::hash(&key.first.lo, 8, 0);
    value = HashUtil::hash(&key.first.hi, 8, value);
    return value % capacity;
}

template <typename Key, typename Value, typename ValueType>
ConcurrentContextMap<Key, Value, ValueType>::ConcurrentContextMap() {
    _internal_map.resize(config::num_query_ctx_map_partitions);
    for (size_t i = 0; i < config::num_query_ctx_map_partitions; i++) {
        _internal_map[i] = {std::make_unique<std::shared_mutex>(),
                            phmap::flat_hash_map<Key, Value>()};
    }
}

template <typename Key, typename Value, typename ValueType>
Value ConcurrentContextMap<Key, Value, ValueType>::find(const Key& query_id) {
    auto id = get_map_id(query_id, _internal_map.size());
    {
        std::shared_lock lock(*_internal_map[id].first);
        auto& map = _internal_map[id].second;
        auto search = map.find(query_id);
        if (search != map.end()) {
            return search->second;
        }
        return std::shared_ptr<ValueType>(nullptr);
    }
}

template <typename Key, typename Value, typename ValueType>
Status ConcurrentContextMap<Key, Value, ValueType>::apply_if_not_exists(
        const Key& query_id, std::shared_ptr<ValueType>& query_ctx, ApplyFunction&& function) {
    auto id = get_map_id(query_id, _internal_map.size());
    {
        std::unique_lock lock(*_internal_map[id].first);
        auto& map = _internal_map[id].second;
        auto search = map.find(query_id);
        if (search != map.end()) {
            query_ctx = search->second;
        }
        if (!query_ctx) {
            return function(map);
        }
        return Status::OK();
    }
}

template <typename Key, typename Value, typename ValueType>
void ConcurrentContextMap<Key, Value, ValueType>::erase(const Key& query_id) {
    auto id = get_map_id(query_id, _internal_map.size());
    {
        std::unique_lock lock(*_internal_map[id].first);
        auto& map = _internal_map[id].second;
        map.erase(query_id);
    }
}

template <typename Key, typename Value, typename ValueType>
void ConcurrentContextMap<Key, Value, ValueType>::insert(const Key& query_id,
                                                         std::shared_ptr<ValueType> query_ctx) {
    auto id = get_map_id(query_id, _internal_map.size());
    {
        std::unique_lock lock(*_internal_map[id].first);
        auto& map = _internal_map[id].second;
        map.insert({query_id, query_ctx});
    }
}

template <typename Key, typename Value, typename ValueType>
void ConcurrentContextMap<Key, Value, ValueType>::clear() {
    for (auto& pair : _internal_map) {
        std::unique_lock lock(*pair.first);
        auto& map = pair.second;
        map.clear();
    }
}

FragmentMgr::FragmentMgr(ExecEnv* exec_env)
        : _exec_env(exec_env), _stop_background_threads_latch(1) {
    _entity = DorisMetrics::instance()->metric_registry()->register_entity("FragmentMgr");
    INT_UGAUGE_METRIC_REGISTER(_entity, timeout_canceled_fragment_count);
    REGISTER_HOOK_METRIC(fragment_instance_count,
                         [this]() { return _fragment_instance_map.num_items(); });

    auto s = Thread::create(
            "FragmentMgr", "cancel_timeout_plan_fragment", [this]() { this->cancel_worker(); },
            &_cancel_thread);
    CHECK(s.ok()) << s.to_string();

    // TODO(zc): we need a better thread-pool
    // now one user can use all the thread pool, others have no resource.
    s = ThreadPoolBuilder("FragmentMgrThreadPool")
                .set_min_threads(config::fragment_pool_thread_num_min)
                .set_max_threads(config::fragment_pool_thread_num_max)
                .set_max_queue_size(config::fragment_pool_queue_size)
                .build(&_thread_pool);

    REGISTER_HOOK_METRIC(fragment_thread_pool_queue_size,
                         [this]() { return _thread_pool->get_queue_size(); });
    CHECK(s.ok()) << s.to_string();

    s = ThreadPoolBuilder("FragmentInstanceReportThreadPool")
                .set_min_threads(48)
                .set_max_threads(512)
                .set_max_queue_size(102400)
                .build(&_async_report_thread_pool);
    CHECK(s.ok()) << s.to_string();
}

FragmentMgr::~FragmentMgr() = default;

void FragmentMgr::stop() {
    DEREGISTER_HOOK_METRIC(fragment_instance_count);
    DEREGISTER_HOOK_METRIC(fragment_thread_pool_queue_size);
    _stop_background_threads_latch.count_down();
    if (_cancel_thread) {
        _cancel_thread->join();
    }
    // Stop all the worker, should wait for a while?
    // _thread_pool->wait_for();
    _thread_pool->shutdown();

    // Only me can delete
    _fragment_instance_map.clear();
    _pipeline_map.apply(
            [&](phmap::flat_hash_map<TUniqueId, std::shared_ptr<pipeline::PipelineFragmentContext>>&
                        map) -> Status {
                for (auto& pipeline : map) {
                    pipeline.second->close_sink();
                }
                return Status::OK();
            });
    _pipeline_map.clear();
    _query_ctx_map.clear();
    _async_report_thread_pool->shutdown();
}

std::string FragmentMgr::to_http_path(const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port
        << "/api/_download_load?"
        << "token=" << _exec_env->token() << "&file=" << file_name;
    return url.str();
}

Status FragmentMgr::trigger_pipeline_context_report(
        const ReportStatusRequest req, std::shared_ptr<pipeline::PipelineFragmentContext>&& ctx) {
    return _async_report_thread_pool->submit_func([this, req, ctx]() {
        SCOPED_ATTACH_TASK(ctx->get_query_ctx()->query_mem_tracker);
        coordinator_callback(req);
        if (!req.done) {
            ctx->refresh_next_report_time();
        }
    });
}

// There can only be one of these callbacks in-flight at any moment, because
// it is only invoked from the executor's reporting thread.
// Also, the reported status will always reflect the most recent execution status,
// including the final status when execution finishes.
void FragmentMgr::coordinator_callback(const ReportStatusRequest& req) {
    DCHECK(req.status.ok() || req.done); // if !status.ok() => done
    Status exec_status = req.update_fn(req.status);
    Status coord_status;
    FrontendServiceConnection coord(_exec_env->frontend_client_cache(), req.coord_addr,
                                    &coord_status);
    if (!coord_status.ok()) {
        std::stringstream ss;
        UniqueId uid(req.query_id.hi, req.query_id.lo);
        static_cast<void>(req.update_fn(Status::InternalError(
                "query_id: {}, couldn't get a client for {}, reason is {}", uid.to_string(),
                PrintThriftNetworkAddress(req.coord_addr), coord_status.to_string())));
        return;
    }

    TReportExecStatusParams params;
    params.protocol_version = FrontendServiceVersion::V1;
    params.__set_query_id(req.query_id);
    params.__set_backend_num(req.backend_num);
    params.__set_fragment_instance_id(req.fragment_instance_id);
    params.__set_fragment_id(req.fragment_id);
    params.__set_status(exec_status.to_thrift());
    params.__set_done(req.done);
    params.__set_query_type(req.runtime_state->query_type());

    DCHECK(req.runtime_state != nullptr);

    if (req.runtime_state->query_type() == TQueryType::LOAD && !req.done && req.status.ok()) {
        // this is a load plan, and load is not finished, just make a brief report
        params.__set_loaded_rows(req.runtime_state->num_rows_load_total());
        params.__set_loaded_bytes(req.runtime_state->num_bytes_load_total());
    } else {
        if (req.runtime_state->query_type() == TQueryType::LOAD) {
            params.__set_loaded_rows(req.runtime_state->num_rows_load_total());
            params.__set_loaded_bytes(req.runtime_state->num_bytes_load_total());
        }
        if (req.is_pipeline_x) {
            params.__isset.detailed_report = true;
            DCHECK(!req.runtime_states.empty());
            const bool enable_profile = (*req.runtime_states.begin())->enable_profile();
            if (enable_profile) {
                params.__isset.profile = true;
                params.__isset.loadChannelProfile = false;
                for (auto* rs : req.runtime_states) {
                    DCHECK(req.load_channel_profile);
                    TDetailedReportParams detailed_param;
                    rs->load_channel_profile()->to_thrift(&detailed_param.loadChannelProfile);
                    // merge all runtime_states.loadChannelProfile to req.load_channel_profile
                    req.load_channel_profile->update(detailed_param.loadChannelProfile);
                }
                req.load_channel_profile->to_thrift(&params.loadChannelProfile);
            } else {
                params.__isset.profile = false;
            }

            if (enable_profile) {
                DCHECK(req.profile != nullptr);
                TDetailedReportParams detailed_param;
                detailed_param.__isset.fragment_instance_id = false;
                detailed_param.__isset.profile = true;
                detailed_param.__isset.loadChannelProfile = false;
                detailed_param.__set_is_fragment_level(true);
                req.profile->to_thrift(&detailed_param.profile);
                params.detailed_report.push_back(detailed_param);
                for (auto& pipeline_profile : req.runtime_state->pipeline_id_to_profile()) {
                    TDetailedReportParams detailed_param;
                    detailed_param.__isset.fragment_instance_id = false;
                    detailed_param.__isset.profile = true;
                    detailed_param.__isset.loadChannelProfile = false;
                    pipeline_profile->to_thrift(&detailed_param.profile);
                    params.detailed_report.push_back(detailed_param);
                }
            }
        } else {
            if (req.profile != nullptr) {
                req.profile->to_thrift(&params.profile);
                if (req.load_channel_profile) {
                    req.load_channel_profile->to_thrift(&params.loadChannelProfile);
                }
                params.__isset.profile = true;
                params.__isset.loadChannelProfile = true;
            } else {
                params.__isset.profile = false;
            }
        }

        if (!req.runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : req.runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(it));
            }
        } else if (!req.runtime_states.empty()) {
            for (auto* rs : req.runtime_states) {
                for (auto& it : rs->output_files()) {
                    params.delta_urls.push_back(to_http_path(it));
                }
            }
            if (!params.delta_urls.empty()) {
                params.__isset.delta_urls = true;
            }
        }

        // load rows
        static std::string s_dpp_normal_all = "dpp.norm.ALL";
        static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";
        static std::string s_unselected_rows = "unselected.rows";
        int64_t num_rows_load_success = 0;
        int64_t num_rows_load_filtered = 0;
        int64_t num_rows_load_unselected = 0;
        if (req.runtime_state->num_rows_load_total() > 0 ||
            req.runtime_state->num_rows_load_filtered() > 0 ||
            req.runtime_state->num_finished_range() > 0) {
            params.__isset.load_counters = true;

            num_rows_load_success = req.runtime_state->num_rows_load_success();
            num_rows_load_filtered = req.runtime_state->num_rows_load_filtered();
            num_rows_load_unselected = req.runtime_state->num_rows_load_unselected();
            params.__isset.fragment_instance_reports = true;
            TFragmentInstanceReport t;
            t.__set_fragment_instance_id(req.runtime_state->fragment_instance_id());
            t.__set_num_finished_range(req.runtime_state->num_finished_range());
            t.__set_loaded_rows(req.runtime_state->num_rows_load_total());
            t.__set_loaded_bytes(req.runtime_state->num_bytes_load_total());
            params.fragment_instance_reports.push_back(t);
        } else if (!req.runtime_states.empty()) {
            for (auto* rs : req.runtime_states) {
                if (rs->num_rows_load_total() > 0 || rs->num_rows_load_filtered() > 0 ||
                    req.runtime_state->num_finished_range() > 0) {
                    params.__isset.load_counters = true;
                    num_rows_load_success += rs->num_rows_load_success();
                    num_rows_load_filtered += rs->num_rows_load_filtered();
                    num_rows_load_unselected += rs->num_rows_load_unselected();
                    params.__isset.fragment_instance_reports = true;
                    TFragmentInstanceReport t;
                    t.__set_fragment_instance_id(rs->fragment_instance_id());
                    t.__set_num_finished_range(rs->num_finished_range());
                    t.__set_loaded_rows(rs->num_rows_load_total());
                    t.__set_loaded_bytes(rs->num_bytes_load_total());
                    params.fragment_instance_reports.push_back(t);
                }
            }
        }
        params.load_counters.emplace(s_dpp_normal_all, std::to_string(num_rows_load_success));
        params.load_counters.emplace(s_dpp_abnormal_all, std::to_string(num_rows_load_filtered));
        params.load_counters.emplace(s_unselected_rows, std::to_string(num_rows_load_unselected));

        if (!req.runtime_state->get_error_log_file_path().empty()) {
            params.__set_tracking_url(
                    to_load_error_http_path(req.runtime_state->get_error_log_file_path()));
        } else if (!req.runtime_states.empty()) {
            for (auto* rs : req.runtime_states) {
                if (!rs->get_error_log_file_path().empty()) {
                    params.__set_tracking_url(
                            to_load_error_http_path(rs->get_error_log_file_path()));
                }
            }
        }
        if (!req.runtime_state->export_output_files().empty()) {
            params.__isset.export_files = true;
            params.export_files = req.runtime_state->export_output_files();
        } else if (!req.runtime_states.empty()) {
            for (auto* rs : req.runtime_states) {
                if (!rs->export_output_files().empty()) {
                    params.__isset.export_files = true;
                    params.export_files.insert(params.export_files.end(),
                                               rs->export_output_files().begin(),
                                               rs->export_output_files().end());
                }
            }
        }
        if (!req.runtime_state->tablet_commit_infos().empty()) {
            params.__isset.commitInfos = true;
            params.commitInfos.reserve(req.runtime_state->tablet_commit_infos().size());
            for (auto& info : req.runtime_state->tablet_commit_infos()) {
                params.commitInfos.push_back(info);
            }
        } else if (!req.runtime_states.empty()) {
            for (auto* rs : req.runtime_states) {
                if (!rs->tablet_commit_infos().empty()) {
                    params.__isset.commitInfos = true;
                    params.commitInfos.insert(params.commitInfos.end(),
                                              rs->tablet_commit_infos().begin(),
                                              rs->tablet_commit_infos().end());
                }
            }
        }
        if (!req.runtime_state->error_tablet_infos().empty()) {
            params.__isset.errorTabletInfos = true;
            params.errorTabletInfos.reserve(req.runtime_state->error_tablet_infos().size());
            for (auto& info : req.runtime_state->error_tablet_infos()) {
                params.errorTabletInfos.push_back(info);
            }
        } else if (!req.runtime_states.empty()) {
            for (auto* rs : req.runtime_states) {
                if (!rs->error_tablet_infos().empty()) {
                    params.__isset.errorTabletInfos = true;
                    params.errorTabletInfos.insert(params.errorTabletInfos.end(),
                                                   rs->error_tablet_infos().begin(),
                                                   rs->error_tablet_infos().end());
                }
            }
        }

        if (!req.runtime_state->hive_partition_updates().empty()) {
            params.__isset.hive_partition_updates = true;
            params.hive_partition_updates.reserve(
                    req.runtime_state->hive_partition_updates().size());
            for (auto& hive_partition_update : req.runtime_state->hive_partition_updates()) {
                params.hive_partition_updates.push_back(hive_partition_update);
            }
        } else if (!req.runtime_states.empty()) {
            for (auto* rs : req.runtime_states) {
                if (!rs->hive_partition_updates().empty()) {
                    params.__isset.hive_partition_updates = true;
                    params.hive_partition_updates.insert(params.hive_partition_updates.end(),
                                                         rs->hive_partition_updates().begin(),
                                                         rs->hive_partition_updates().end());
                }
            }
        }

        if (!req.runtime_state->iceberg_commit_datas().empty()) {
            params.__isset.iceberg_commit_datas = true;
            params.iceberg_commit_datas.reserve(req.runtime_state->iceberg_commit_datas().size());
            for (auto& iceberg_commit_data : req.runtime_state->iceberg_commit_datas()) {
                params.iceberg_commit_datas.push_back(iceberg_commit_data);
            }
        } else if (!req.runtime_states.empty()) {
            for (auto* rs : req.runtime_states) {
                if (!rs->iceberg_commit_datas().empty()) {
                    params.__isset.iceberg_commit_datas = true;
                    params.iceberg_commit_datas.insert(params.iceberg_commit_datas.end(),
                                                       rs->iceberg_commit_datas().begin(),
                                                       rs->iceberg_commit_datas().end());
                }
            }
        }

        // Send new errors to coordinator
        req.runtime_state->get_unreported_errors(&(params.error_log));
        params.__isset.error_log = (params.error_log.size() > 0);
    }

    if (_exec_env->master_info()->__isset.backend_id) {
        params.__set_backend_id(_exec_env->master_info()->backend_id);
    }

    TReportExecStatusResult res;
    Status rpc_status;

    VLOG_DEBUG << "reportExecStatus params is "
               << apache::thrift::ThriftDebugString(params).c_str();
    if (!exec_status.ok()) {
        LOG(WARNING) << "report error status: " << exec_status.msg()
                     << " to coordinator: " << req.coord_addr
                     << ", query id: " << print_id(req.query_id)
                     << ", instance id: " << print_id(req.fragment_instance_id);
    }
    try {
        try {
            coord->reportExecStatus(res, params);
        } catch (TTransportException& e) {
            LOG(WARNING) << "Retrying ReportExecStatus. query id: " << print_id(req.query_id)
                         << ", instance id: " << print_id(req.fragment_instance_id) << " to "
                         << req.coord_addr << ", err: " << e.what();
            rpc_status = coord.reopen();

            if (!rpc_status.ok()) {
                // we need to cancel the execution of this fragment
                static_cast<void>(req.update_fn(rpc_status));
                req.cancel_fn(PPlanFragmentCancelReason::INTERNAL_ERROR, "report rpc fail");
                return;
            }
            coord->reportExecStatus(res, params);
        }

        rpc_status = Status::create<false>(res.status);
    } catch (TException& e) {
        rpc_status = Status::InternalError("ReportExecStatus() to {} failed: {}",
                                           PrintThriftNetworkAddress(req.coord_addr), e.what());
    }

    if (!rpc_status.ok()) {
        LOG_INFO("Going to cancel instance {} since report exec status got rpc failed: {}",
                 print_id(req.fragment_instance_id), rpc_status.to_string());
        // we need to cancel the execution of this fragment
        static_cast<void>(req.update_fn(rpc_status));
        req.cancel_fn(PPlanFragmentCancelReason::INTERNAL_ERROR, rpc_status.msg());
    }
}

static void empty_function(RuntimeState*, Status*) {}

void FragmentMgr::_exec_actual(std::shared_ptr<PlanFragmentExecutor> fragment_executor,
                               const FinishCallback& cb) {
    VLOG_DEBUG << fmt::format("Instance {}|{} executing", print_id(fragment_executor->query_id()),
                              print_id(fragment_executor->fragment_instance_id()));

    Status st = fragment_executor->execute();
    if (!st.ok()) {
        fragment_executor->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR,
                                  "fragment_executor execute failed");
    }

    std::shared_ptr<QueryContext> query_ctx = fragment_executor->get_query_ctx();
    bool all_done = false;
    if (query_ctx != nullptr) {
        // decrease the number of unfinished fragments
        all_done = query_ctx->countdown(1);
    }

    // remove exec state after this fragment finished
    {
        _fragment_instance_map.erase(fragment_executor->fragment_instance_id());

        LOG_INFO("Instance {} finished", print_id(fragment_executor->fragment_instance_id()));
    }
    if (all_done && query_ctx) {
        _query_ctx_map.erase(query_ctx->query_id());
        LOG_INFO("Query {} finished", print_id(query_ctx->query_id()));
    }

    // Callback after remove from this id
    auto status = fragment_executor->status();
    cb(fragment_executor->runtime_state(), &status);
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params,
                                       const QuerySource query_source) {
    if (params.txn_conf.need_txn) {
        std::shared_ptr<StreamLoadContext> stream_load_ctx =
                std::make_shared<StreamLoadContext>(_exec_env);
        stream_load_ctx->db = params.txn_conf.db;
        stream_load_ctx->db_id = params.txn_conf.db_id;
        stream_load_ctx->table = params.txn_conf.tbl;
        stream_load_ctx->txn_id = params.txn_conf.txn_id;
        stream_load_ctx->id = UniqueId(params.params.query_id);
        stream_load_ctx->put_result.params = params;
        stream_load_ctx->put_result.__isset.params = true;
        stream_load_ctx->use_streaming = true;
        stream_load_ctx->load_type = TLoadType::MANUL_LOAD;
        stream_load_ctx->load_src_type = TLoadSourceType::RAW;
        stream_load_ctx->label = params.import_label;
        stream_load_ctx->format = TFileFormatType::FORMAT_CSV_PLAIN;
        stream_load_ctx->timeout_second = 3600;
        stream_load_ctx->auth.token = params.txn_conf.token;
        stream_load_ctx->need_commit_self = true;
        stream_load_ctx->need_rollback = true;
        auto pipe = std::make_shared<io::StreamLoadPipe>(
                io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
                -1 /* total_length */, true /* use_proto */);
        stream_load_ctx->body_sink = pipe;
        stream_load_ctx->pipe = pipe;
        stream_load_ctx->max_filter_ratio = params.txn_conf.max_filter_ratio;

        RETURN_IF_ERROR(
                _exec_env->new_load_stream_mgr()->put(stream_load_ctx->id, stream_load_ctx));

        RETURN_IF_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(stream_load_ctx));
        return Status::OK();
    } else {
        return exec_plan_fragment(params, query_source, empty_function);
    }
}

Status FragmentMgr::exec_plan_fragment(const TPipelineFragmentParams& params,
                                       const QuerySource query_source) {
    if (params.txn_conf.need_txn) {
        std::shared_ptr<StreamLoadContext> stream_load_ctx =
                std::make_shared<StreamLoadContext>(_exec_env);
        stream_load_ctx->db = params.txn_conf.db;
        stream_load_ctx->db_id = params.txn_conf.db_id;
        stream_load_ctx->table = params.txn_conf.tbl;
        stream_load_ctx->txn_id = params.txn_conf.txn_id;
        stream_load_ctx->id = UniqueId(params.query_id);
        stream_load_ctx->put_result.pipeline_params = params;
        stream_load_ctx->use_streaming = true;
        stream_load_ctx->load_type = TLoadType::MANUL_LOAD;
        stream_load_ctx->load_src_type = TLoadSourceType::RAW;
        stream_load_ctx->label = params.import_label;
        stream_load_ctx->format = TFileFormatType::FORMAT_CSV_PLAIN;
        stream_load_ctx->timeout_second = 3600;
        stream_load_ctx->auth.token = params.txn_conf.token;
        stream_load_ctx->need_commit_self = true;
        stream_load_ctx->need_rollback = true;
        auto pipe = std::make_shared<io::StreamLoadPipe>(
                io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
                -1 /* total_length */, true /* use_proto */);
        stream_load_ctx->body_sink = pipe;
        stream_load_ctx->pipe = pipe;
        stream_load_ctx->max_filter_ratio = params.txn_conf.max_filter_ratio;

        RETURN_IF_ERROR(
                _exec_env->new_load_stream_mgr()->put(stream_load_ctx->id, stream_load_ctx));

        RETURN_IF_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(stream_load_ctx));
        return Status::OK();
    } else {
        return exec_plan_fragment(params, query_source, empty_function);
    }
}

Status FragmentMgr::start_query_execution(const PExecPlanFragmentStartRequest* request) {
    TUniqueId query_id;
    query_id.__set_hi(request->query_id().hi());
    query_id.__set_lo(request->query_id().lo());
    std::shared_ptr<QueryContext> q_ctx = nullptr;
    {
        TUniqueId query_id;
        query_id.__set_hi(request->query_id().hi());
        query_id.__set_lo(request->query_id().lo());
        q_ctx = _query_ctx_map.find(query_id);
        if (q_ctx == nullptr) {
            return Status::InternalError(
                    "Failed to get query fragments context. Query may be "
                    "timeout or be cancelled. host: {}",
                    BackendOptions::get_localhost());
        }
    }
    q_ctx->set_ready_to_execute(false);
    LOG_INFO("Query {} start execution", print_id(query_id));
    return Status::OK();
}

void FragmentMgr::remove_pipeline_context(
        std::shared_ptr<pipeline::PipelineFragmentContext> f_context) {
    auto* q_context = f_context->get_query_ctx();
    bool all_done = false;
    TUniqueId query_id = f_context->get_query_id();
    {
        std::vector<TUniqueId> ins_ids;
        f_context->instance_ids(ins_ids);
        all_done = q_context->countdown(ins_ids.size());
        for (const auto& ins_id : ins_ids) {
            LOG_INFO("Removing query {} instance {}, all done? {}", print_id(query_id),
                     print_id(ins_id), all_done);
            _pipeline_map.erase(ins_id);
            g_pipeline_fragment_instances_count << -1;
        }
    }
    if (all_done) {
        _query_ctx_map.erase(query_id);
        LOG_INFO("Query {} finished", print_id(query_id));
    }
}

template <typename Params>
Status FragmentMgr::_get_query_ctx(const Params& params, TUniqueId query_id, bool pipeline,
                                   QuerySource query_source,
                                   std::shared_ptr<QueryContext>& query_ctx) {
    DBUG_EXECUTE_IF("FragmentMgr._get_query_ctx.failed",
                    { return Status::InternalError("FragmentMgr._get_query_ctx.failed"); });
    if (params.is_simplified_param) {
        // Get common components from _query_ctx_map
        query_ctx = _query_ctx_map.find(query_id);
        if (query_ctx == nullptr) {
            return Status::InternalError(
                    "Failed to get query fragments context. Query may be "
                    "timeout or be cancelled. host: {}",
                    BackendOptions::get_localhost());
        }
    } else {
        // Find _query_ctx_map, in case some other request has already
        // create the query fragments context.
        RETURN_IF_ERROR(_query_ctx_map.apply_if_not_exists(
                query_id, query_ctx,
                [&](phmap::flat_hash_map<TUniqueId, std::shared_ptr<QueryContext>>& map) -> Status {
                    TNetworkAddress current_connect_fe_addr;
                    // for gray upragde between 2.1 version, fe may not set current_connect_fe,
                    // then use coord addr instead
                    if (params.__isset.current_connect_fe) {
                        current_connect_fe_addr = params.current_connect_fe;
                    } else {
                        current_connect_fe_addr = params.coord;
                    }

                    LOG(INFO) << "query_id: " << print_id(query_id)
                              << ", coord_addr: " << params.coord
                              << ", total fragment num on current host: "
                              << params.fragment_num_on_host
                              << ", fe process uuid: " << params.query_options.fe_process_uuid
                              << ", query type: " << params.query_options.query_type
                              << ", report audit fe:" << current_connect_fe_addr;

                    // This may be a first fragment request of the query.
                    // Create the query fragments context.
                    query_ctx = QueryContext::create_shared(
                            query_id, params.fragment_num_on_host, _exec_env, params.query_options,
                            params.coord, pipeline, params.is_nereids, current_connect_fe_addr,
                            query_source);
                    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(query_ctx->query_mem_tracker);
                    RETURN_IF_ERROR(DescriptorTbl::create(&(query_ctx->obj_pool), params.desc_tbl,
                                                          &(query_ctx->desc_tbl)));
                    // set file scan range params
                    if (params.__isset.file_scan_params) {
                        query_ctx->file_scan_range_params_map = params.file_scan_params;
                    }

                    query_ctx->query_globals = params.query_globals;

                    if (params.__isset.resource_info) {
                        query_ctx->user = params.resource_info.user;
                        query_ctx->group = params.resource_info.group;
                        query_ctx->set_rsc_info = true;
                    }

                    query_ctx->get_shared_hash_table_controller()->set_pipeline_engine_enabled(
                            pipeline);
                    _set_scan_concurrency(params, query_ctx.get());
                    const bool is_pipeline = std::is_same_v<TPipelineFragmentParams, Params>;

                    if (params.__isset.workload_groups && !params.workload_groups.empty()) {
                        uint64_t tg_id = params.workload_groups[0].id;
                        WorkloadGroupPtr workload_group_ptr =
                                _exec_env->workload_group_mgr()->get_task_group_by_id(tg_id);
                        if (workload_group_ptr != nullptr) {
                            RETURN_IF_ERROR(workload_group_ptr->add_query(query_id, query_ctx));
                            RETURN_IF_ERROR(query_ctx->set_workload_group(workload_group_ptr));
                            _exec_env->runtime_query_statistics_mgr()->set_workload_group_id(
                                    print_id(query_id), tg_id);

                            LOG(INFO) << "Query/load id: " << print_id(query_ctx->query_id())
                                      << ", use workload group: "
                                      << workload_group_ptr->debug_string()
                                      << ", is pipeline: " << ((int)is_pipeline);
                        } else {
                            LOG(INFO) << "Query/load id: " << print_id(query_ctx->query_id())
                                      << " carried group info but can not find group in be";
                        }
                    }
                    // There is some logic in query ctx's dctor, we could not check if exists and delete the
                    // temp query ctx now. For example, the query id maybe removed from workload group's queryset.
                    map.insert({query_id, query_ctx});
                    LOG(INFO) << "Register query/load memory tracker, query/load id: "
                              << print_id(query_ctx->query_id()) << " limit: "
                              << PrettyPrinter::print(query_ctx->mem_limit(), TUnit::BYTES);
                    return Status::OK();
                }));
    }
    return Status::OK();
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params,
                                       QuerySource query_source, const FinishCallback& cb) {
    VLOG_ROW << "exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(params).c_str();
    // sometimes TExecPlanFragmentParams debug string is too long and glog
    // will truncate the log line, so print query options seperately for debuggin purpose
    VLOG_ROW << "query options is "
             << apache::thrift::ThriftDebugString(params.query_options).c_str();
    const TUniqueId& fragment_instance_id = params.params.fragment_instance_id;
    {
        auto iter = _fragment_instance_map.find(fragment_instance_id);
        if (iter != nullptr) {
            // Duplicated
            LOG(WARNING) << "duplicate fragment instance id: " << print_id(fragment_instance_id);
            return Status::OK();
        }
    }

    std::shared_ptr<QueryContext> query_ctx;
    bool pipeline_engine_enabled = params.query_options.__isset.enable_pipeline_engine &&
                                   params.query_options.enable_pipeline_engine;

    RETURN_IF_ERROR(_get_query_ctx(params, params.params.query_id, pipeline_engine_enabled,
                                   query_source, query_ctx));
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(query_ctx->query_mem_tracker);
    {
        // Need lock here, because it will modify fragment ids and std::vector may resize and reallocate
        // memory, but query_is_canncelled will traverse the vector, it will core.
        // query_is_cancelled is called in allocator, we has to avoid dead lock.
        query_ctx->push_instance_ids(fragment_instance_id);
    }

    auto fragment_executor = std::make_shared<PlanFragmentExecutor>(
            _exec_env, query_ctx, params.params.fragment_instance_id, -1, params.backend_num,
            std::bind<void>(std::mem_fn(&FragmentMgr::coordinator_callback), this,
                            std::placeholders::_1));
    if (params.__isset.need_wait_execution_trigger && params.need_wait_execution_trigger) {
        // set need_wait_execution_trigger means this instance will not actually being executed
        // until the execPlanFragmentStart RPC trigger to start it.
        fragment_executor->set_need_wait_execution_trigger();
    }

    int64_t duration_ns = 0;
    DCHECK(!pipeline_engine_enabled);
    {
        SCOPED_RAW_TIMER(&duration_ns);
        RETURN_IF_ERROR(fragment_executor->prepare(params));
    }
    g_fragmentmgr_prepare_latency << (duration_ns / 1000);
    std::shared_ptr<RuntimeFilterMergeControllerEntity> handler;
    // TODO need check the status, but when I add return_if_error the P0 will not pass
    static_cast<void>(_runtimefilter_controller.add_entity(
            params.params, params.params.query_id, params.query_options, &handler,
            RuntimeFilterParamsContext::create(fragment_executor->runtime_state())));
    {
        if (handler) {
            query_ctx->set_merge_controller_handler(handler);
        }
        _fragment_instance_map.insert(params.params.fragment_instance_id, fragment_executor);
    }

    auto st = _thread_pool->submit_func([this, fragment_executor, cb]() {
#ifndef BE_TEST
        SCOPED_ATTACH_TASK(fragment_executor->runtime_state());
#endif
        _exec_actual(fragment_executor, cb);
    });
    if (!st.ok()) {
        {
            // Remove the exec state added
            _fragment_instance_map.erase(params.params.fragment_instance_id);
        }
        fragment_executor->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR,
                                  "push plan fragment to thread pool failed");
        return Status::InternalError(
                strings::Substitute("push plan fragment $0 to thread pool failed. err = $1, BE: $2",
                                    print_id(params.params.fragment_instance_id), st.to_string(),
                                    BackendOptions::get_localhost()));
    }

    return Status::OK();
}

std::string FragmentMgr::dump_pipeline_tasks(int64_t duration) {
    fmt::memory_buffer debug_string_buffer;
    auto t = MonotonicNanos();
    size_t i = 0;
    {
        fmt::format_to(debug_string_buffer,
                       "{} pipeline fragment contexts are still running! duration_limit={}\n",
                       _pipeline_map.num_items(), duration);

        timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        _pipeline_map.apply(
                [&](phmap::flat_hash_map<TUniqueId,
                                         std::shared_ptr<pipeline::PipelineFragmentContext>>& map)
                        -> Status {
                    for (auto& it : map) {
                        auto elapsed = (t - it.second->create_time()) / 1000000000.0;
                        if (elapsed < duration) {
                            // Only display tasks which has been running for more than {duration} seconds.
                            continue;
                        }
                        auto timeout_second = it.second->timeout_second();
                        fmt::format_to(
                                debug_string_buffer,
                                "No.{} (elapse_second={}s, query_timeout_second={}s, instance_id="
                                "{}) : {}\n",
                                i, elapsed, timeout_second, print_id(it.first),
                                it.second->debug_string());
                        i++;
                    }
                    return Status::OK();
                });
    }
    return fmt::to_string(debug_string_buffer);
}

std::string FragmentMgr::dump_pipeline_tasks(TUniqueId& query_id) {
    if (auto q_ctx = get_query_context(query_id)) {
        return q_ctx->print_all_pipeline_context();
    } else {
        return fmt::format("Query context (query id = {}) not found. \n", print_id(query_id));
    }
}

Status FragmentMgr::exec_plan_fragment(const TPipelineFragmentParams& params,
                                       QuerySource query_source, const FinishCallback& cb) {
    VLOG_ROW << "query: " << print_id(params.query_id) << " exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(params).c_str();
    // sometimes TExecPlanFragmentParams debug string is too long and glog
    // will truncate the log line, so print query options seperately for debuggin purpose
    VLOG_ROW << "query: " << print_id(params.query_id) << "query options is "
             << apache::thrift::ThriftDebugString(params.query_options).c_str();

    std::shared_ptr<QueryContext> query_ctx;
    RETURN_IF_ERROR(_get_query_ctx(params, params.query_id, true, query_source, query_ctx));
    SCOPED_ATTACH_TASK(query_ctx.get());
    const bool enable_pipeline_x = params.query_options.__isset.enable_pipeline_x_engine &&
                                   params.query_options.enable_pipeline_x_engine;
    if (enable_pipeline_x) {
        _setup_shared_hashtable_for_broadcast_join(params, query_ctx.get());
        int64_t duration_ns = 0;
        std::shared_ptr<pipeline::PipelineFragmentContext> context =
                std::make_shared<pipeline::PipelineXFragmentContext>(
                        query_ctx->query_id(), params.fragment_id, query_ctx, _exec_env, cb,
                        std::bind<Status>(
                                std::mem_fn(&FragmentMgr::trigger_pipeline_context_report), this,
                                std::placeholders::_1, std::placeholders::_2));
        {
            SCOPED_RAW_TIMER(&duration_ns);
            auto prepare_st = context->prepare(params, _thread_pool.get());
            if (!prepare_st.ok()) {
                context->close_if_prepare_failed(prepare_st);
                query_ctx->set_execution_dependency_ready();
                return prepare_st;
            }
        }
        g_fragmentmgr_prepare_latency << (duration_ns / 1000);

        DBUG_EXECUTE_IF("FragmentMgr.exec_plan_fragment.failed",
                        { return Status::Aborted("FragmentMgr.exec_plan_fragment.failed"); });

        std::shared_ptr<RuntimeFilterMergeControllerEntity> handler;
        RETURN_IF_ERROR(_runtimefilter_controller.add_entity(
                params.local_params[0], params.query_id, params.query_options, &handler,
                RuntimeFilterParamsContext::create(context->get_runtime_state())));
        if (handler) {
            query_ctx->set_merge_controller_handler(handler);
        }

        for (const auto& local_param : params.local_params) {
            const TUniqueId& fragment_instance_id = local_param.fragment_instance_id;
            auto iter = _pipeline_map.find(fragment_instance_id);
            if (iter != nullptr) {
                return Status::InternalError(
                        "exec_plan_fragment input duplicated fragment_instance_id({})",
                        UniqueId(fragment_instance_id).to_string());
            }
            query_ctx->push_instance_ids(fragment_instance_id);
        }

        if (!params.__isset.need_wait_execution_trigger || !params.need_wait_execution_trigger) {
            query_ctx->set_ready_to_execute_only();
        }

        {
            std::vector<TUniqueId> ins_ids;
            reinterpret_cast<pipeline::PipelineXFragmentContext*>(context.get())
                    ->instance_ids(ins_ids);
            // TODO: simplify this mapping
            for (const auto& ins_id : ins_ids) {
                _pipeline_map.insert(ins_id, context);
            }
        }
        query_ctx->set_pipeline_context(params.fragment_id, context);

        RETURN_IF_ERROR(context->submit());
        return Status::OK();
    } else {
        auto pre_and_submit = [&](int i) {
            const auto& local_params = params.local_params[i];

            const TUniqueId& fragment_instance_id = local_params.fragment_instance_id;
            {
                auto res = _pipeline_map.find(fragment_instance_id);
                if (res != nullptr) {
                    // Duplicated
                    return Status::OK();
                }
                query_ctx->push_instance_ids(fragment_instance_id);
            }

            int64_t duration_ns = 0;
            if (!params.__isset.need_wait_execution_trigger ||
                !params.need_wait_execution_trigger) {
                query_ctx->set_ready_to_execute_only();
            }
            _setup_shared_hashtable_for_broadcast_join(params, local_params, query_ctx.get());
            std::shared_ptr<pipeline::PipelineFragmentContext> context =
                    std::make_shared<pipeline::PipelineFragmentContext>(
                            query_ctx->query_id(), fragment_instance_id, params.fragment_id,
                            local_params.backend_num, query_ctx, _exec_env, cb,
                            std::bind<Status>(
                                    std::mem_fn(&FragmentMgr::trigger_pipeline_context_report),
                                    this, std::placeholders::_1, std::placeholders::_2));
            {
                SCOPED_RAW_TIMER(&duration_ns);
                auto prepare_st = context->prepare(params, i);
                if (!prepare_st.ok()) {
                    LOG(WARNING) << "Prepare failed: " << prepare_st.to_string();
                    context->close_if_prepare_failed(prepare_st);
                    static_cast<void>(context->update_status(prepare_st));
                    return prepare_st;
                }
            }
            g_fragmentmgr_prepare_latency << (duration_ns / 1000);

            DBUG_EXECUTE_IF("FragmentMgr.exec_plan_fragment.failed",
                            { return Status::Aborted("FragmentMgr.exec_plan_fragment.failed"); });

            std::shared_ptr<RuntimeFilterMergeControllerEntity> handler;
            RETURN_IF_ERROR(_runtimefilter_controller.add_entity(
                    local_params, params.query_id, params.query_options, &handler,
                    RuntimeFilterParamsContext::create(context->get_runtime_state())));
            if (i == 0 && handler) {
                query_ctx->set_merge_controller_handler(handler);
            }
            _pipeline_map.insert(fragment_instance_id, context);

            return context->submit();
        };

        int target_size = params.local_params.size();
        g_pipeline_fragment_instances_count << target_size;

        const auto& local_params = params.local_params[0];
        if (local_params.__isset.runtime_filter_params) {
            if (local_params.__isset.runtime_filter_params) {
                query_ctx->runtime_filter_mgr()->set_runtime_filter_params(
                        local_params.runtime_filter_params);
            }
        }
        if (local_params.__isset.topn_filter_source_node_ids) {
            query_ctx->init_runtime_predicates(local_params.topn_filter_source_node_ids);
        } else {
            query_ctx->init_runtime_predicates({0});
        }

        if (target_size > 1) {
            int prepare_done = {0};
            Status prepare_status[target_size];
            std::mutex m;
            std::condition_variable cv;

            for (size_t i = 0; i < target_size; i++) {
                RETURN_IF_ERROR(_thread_pool->submit_func([&, i]() {
                    SCOPED_ATTACH_TASK(query_ctx.get());
                    prepare_status[i] = pre_and_submit(i);
                    std::unique_lock<std::mutex> lock(m);
                    prepare_done++;
                    if (prepare_done == target_size) {
                        cv.notify_one();
                    }
                }));
            }

            std::unique_lock<std::mutex> lock(m);
            if (prepare_done != target_size) {
                cv.wait(lock);

                for (size_t i = 0; i < target_size; i++) {
                    if (!prepare_status[i].ok()) {
                        return prepare_status[i];
                    }
                }
            }
            return Status::OK();
        } else {
            return pre_and_submit(0);
        }
    }
    return Status::OK();
}

template <typename Param>
void FragmentMgr::_set_scan_concurrency(const Param& params, QueryContext* query_ctx) {
#ifndef BE_TEST
    // If the token is set, the scan task will use limited_scan_pool in scanner scheduler.
    // Otherwise, the scan task will use local/remote scan pool in scanner scheduler
    if (params.query_options.__isset.resource_limit &&
        params.query_options.resource_limit.__isset.cpu_limit) {
        query_ctx->set_thread_token(params.query_options.resource_limit.cpu_limit, false);
    }
#endif
}

std::shared_ptr<QueryContext> FragmentMgr::get_query_context(const TUniqueId& query_id) {
    return _query_ctx_map.find(query_id);
}

void FragmentMgr::cancel_query(const TUniqueId& query_id, const PPlanFragmentCancelReason& reason,
                               const std::string& msg) {
    std::shared_ptr<QueryContext> query_ctx;
    std::vector<TUniqueId> all_instance_ids;
    {
        query_ctx = _query_ctx_map.find(query_id);

        if (query_ctx == nullptr) {
            LOG(WARNING) << "Query " << print_id(query_id)
                         << " does not exists, failed to cancel it";
            return;
        }
        // Copy instanceids to avoid concurrent modification.
        // And to reduce the scope of lock.
        all_instance_ids = query_ctx->fragment_instance_ids;
    }
    if (query_ctx->enable_pipeline_x_exec()) {
        query_ctx->cancel_all_pipeline_context(reason, msg);
    } else {
        for (auto it : all_instance_ids) {
            cancel_instance(it, reason, msg);
        }
    }

    query_ctx->cancel(msg, Status::Cancelled(msg));
    _query_ctx_map.erase(query_id);
    LOG(INFO) << "Query " << print_id(query_id) << " is cancelled and removed. Reason: " << msg;
}

void FragmentMgr::cancel_instance(const TUniqueId& instance_id,
                                  const PPlanFragmentCancelReason& reason, const std::string& msg) {
    std::shared_ptr<pipeline::PipelineFragmentContext> pipeline_ctx;
    std::shared_ptr<PlanFragmentExecutor> non_pipeline_ctx;
    {
        pipeline_ctx = _pipeline_map.find(instance_id);
        if (!pipeline_ctx) {
            non_pipeline_ctx = _fragment_instance_map.find(instance_id);
            if (non_pipeline_ctx == nullptr) {
                LOG(WARNING) << "Could not find the fragment instance id:" << print_id(instance_id)
                             << " to cancel";
                return;
            }
        }
    }

    if (pipeline_ctx != nullptr) {
        pipeline_ctx->cancel(reason, msg);
    } else if (non_pipeline_ctx != nullptr) {
        // calling PlanFragmentExecutor::cancel
        non_pipeline_ctx->cancel(reason, msg);
    }
}

void FragmentMgr::cancel_fragment(const TUniqueId& query_id, int32_t fragment_id,
                                  const PPlanFragmentCancelReason& reason, const std::string& msg) {
    auto res = _query_ctx_map.find(query_id);
    if (res != nullptr) {
        // Has to use value to keep the shared ptr not deconstructed.
        WARN_IF_ERROR(res->cancel_pipeline_context(fragment_id, reason, msg),
                      "fail to cancel fragment");
    } else {
        LOG(WARNING) << "Could not find the query id:" << print_id(query_id)
                     << " fragment id:" << fragment_id << " to cancel";
    }
}

void FragmentMgr::cancel_worker() {
    LOG(INFO) << "FragmentMgr cancel worker start working.";

    timespec check_invalid_query_last_timestamp;
    clock_gettime(CLOCK_MONOTONIC, &check_invalid_query_last_timestamp);

    do {
        std::vector<TUniqueId> queries_timeout;
        std::vector<TUniqueId> queries_to_cancel;
        std::vector<TUniqueId> queries_pipeline_task_leak;
        // Fe process uuid -> set<QueryId>
        std::map<int64_t, std::unordered_set<TUniqueId>> running_queries_on_all_fes;
        const std::map<TNetworkAddress, FrontendInfo>& running_fes =
                ExecEnv::GetInstance()->get_running_frontends();

        timespec now_for_check_invalid_query;
        clock_gettime(CLOCK_MONOTONIC, &now_for_check_invalid_query);

        if (config::enable_pipeline_task_leakage_detect &&
            now_for_check_invalid_query.tv_sec - check_invalid_query_last_timestamp.tv_sec >
                    config::pipeline_task_leakage_detect_period_secs) {
            check_invalid_query_last_timestamp = now_for_check_invalid_query;
            running_queries_on_all_fes = _get_all_running_queries_from_fe();
        } else {
            running_queries_on_all_fes.clear();
        }

        VecDateTimeValue now = VecDateTimeValue::local_time();
        std::unordered_map<std::shared_ptr<PBackendService_Stub>, BrpcItem> brpc_stub_with_queries;
        {
            _fragment_instance_map.apply(
                    [&](phmap::flat_hash_map<TUniqueId, std::shared_ptr<PlanFragmentExecutor>>& map)
                            -> Status {
                        for (auto& fragment_instance_itr : map) {
                            if (fragment_instance_itr.second->is_timeout(now)) {
                                queries_timeout.push_back(
                                        fragment_instance_itr.second->fragment_instance_id());
                            }
                        }
                        return Status::OK();
                    });
            _pipeline_map.apply(
                    [&](phmap::flat_hash_map<
                            TUniqueId, std::shared_ptr<pipeline::PipelineFragmentContext>>& map)
                            -> Status {
                        for (auto& pipeline_itr : map) {
                            if (pipeline_itr.second->is_timeout(now)) {
                                std::vector<TUniqueId> ins_ids;
                                reinterpret_cast<pipeline::PipelineXFragmentContext*>(
                                        pipeline_itr.second.get())
                                        ->instance_ids(ins_ids);
                                for (auto& ins_id : ins_ids) {
                                    queries_timeout.push_back(ins_id);
                                }
                            } else {
                                pipeline_itr.second->clear_finished_tasks();
                            }
                        }
                        return Status::OK();
                    });
        }
        {
            _query_ctx_map.apply([&](phmap::flat_hash_map<TUniqueId, std::shared_ptr<QueryContext>>&
                                             map) -> Status {
                for (auto it = map.begin(); it != map.end();) {
                    if (it->second->is_timeout(now)) {
                        LOG_WARNING("Query {} is timeout", print_id(it->first));
                        it = map.erase(it);
                    } else {
                        if (config::enable_brpc_connection_check) {
                            auto brpc_stubs = it->second->get_using_brpc_stubs();
                            for (auto& item : brpc_stubs) {
                                if (!brpc_stub_with_queries.contains(item.second)) {
                                    brpc_stub_with_queries.emplace(
                                            item.second, BrpcItem {item.first, {it->second}});
                                } else {
                                    brpc_stub_with_queries[item.second].queries.emplace_back(
                                            it->second);
                                }
                            }
                        }
                        ++it;
                    }
                }
                return Status::OK();
            });
        }
        {
            // We use a very conservative cancel strategy.
            // 0. If there are no running frontends, do not cancel any queries.
            // 1. If query's process uuid is zero, do not cancel
            // 2. If same process uuid, do not cancel
            // 3. If fe has zero process uuid, do not cancel
            if (running_fes.empty() && _query_ctx_map.num_items() != 0) {
                LOG_EVERY_N(WARNING, 10)
                        << "Could not find any running frontends, maybe we are upgrading or "
                           "starting? "
                        << "We will not cancel any outdated queries in this situation.";
            } else {
                _query_ctx_map.apply([&](phmap::flat_hash_map<TUniqueId,
                                                              std::shared_ptr<QueryContext>>& map)
                                             -> Status {
                    for (const auto& q : map) {
                        auto q_ctx = q.second;
                        const int64_t fe_process_uuid = q_ctx->get_fe_process_uuid();

                        if (fe_process_uuid == 0) {
                            // zero means this query is from a older version fe or
                            // this fe is starting
                            continue;
                        }

                        // If the query is not running on the any frontends, cancel it.
                        if (auto itr = running_queries_on_all_fes.find(fe_process_uuid);
                            itr != running_queries_on_all_fes.end()) {
                            // Query not found on this frontend, and the query arrives before the last check
                            if (itr->second.find(q_ctx->query_id()) == itr->second.end() &&
                                // tv_nsec represents the number of nanoseconds that have elapsed since the time point stored in tv_sec.
                                // tv_sec is enough, we do not need to check tv_nsec.
                                q_ctx->get_query_arrival_timestamp().tv_sec <
                                        check_invalid_query_last_timestamp.tv_sec &&
                                q_ctx->get_query_source() == QuerySource::INTERNAL_FRONTEND) {
                                if (q_ctx->enable_pipeline_x_exec()) {
                                    queries_pipeline_task_leak.push_back(q_ctx->query_id());
                                    LOG_INFO(
                                            "Query {}, type {} is not found on any frontends, "
                                            "maybe it "
                                            "is leaked.",
                                            print_id(q_ctx->query_id()),
                                            toString(q_ctx->get_query_source()));
                                    continue;
                                }
                            }
                        }

                        auto query_context = q.second;

                        auto itr = running_fes.find(query_context->coord_addr);
                        if (itr != running_fes.end()) {
                            if (q.second->get_fe_process_uuid() == itr->second.info.process_uuid ||
                                itr->second.info.process_uuid == 0) {
                                continue;
                            } else {
                                LOG_WARNING(
                                        "Coordinator of query {} restarted, going to cancel it.",
                                        print_id(q.second->query_id()));
                            }
                        } else {
                            // In some rear cases, the rpc port of follower is not updated in time,
                            // then the port of this follower will be zero, but acutally it is still running,
                            // and be has already received the query from follower.
                            // So we need to check if host is in running_fes.
                            bool fe_host_is_standing = std::any_of(
                                    running_fes.begin(), running_fes.end(),
                                    [query_context](const auto& fe) {
                                        return fe.first.hostname ==
                                                       query_context->coord_addr.hostname &&
                                               fe.first.port == 0;
                                    });

                            if (fe_host_is_standing) {
                                LOG_WARNING(
                                        "Coordinator {}:{} is not found, but its host is still "
                                        "running with an unstable rpc port, not going to cancel "
                                        "it.",
                                        query_context->coord_addr.hostname,
                                        query_context->coord_addr.port,
                                        print_id(query_context->query_id()));
                                continue;
                            } else {
                                LOG_WARNING(
                                        "Could not find target coordinator {}:{} of query {}, "
                                        "going to "
                                        "cancel it.",
                                        query_context->coord_addr.hostname,
                                        query_context->coord_addr.port,
                                        print_id(query_context->query_id()));
                            }
                        }

                        // Coorninator of this query has already dead.
                        queries_to_cancel.push_back(q.first);
                    }
                    return Status::OK();
                });
            }
        }

        // TODO(zhiqiang): It seems that timeout_canceled_fragment_count is
        // designed to count canceled fragment of non-pipeline query.
        timeout_canceled_fragment_count->increment(queries_timeout.size());
        for (auto& id : queries_timeout) {
            cancel_instance(id, PPlanFragmentCancelReason::TIMEOUT, "Query timeout");
            LOG(INFO) << "FragmentMgr cancel worker going to cancel timeout instance "
                      << print_id(id);
        }

        for (const auto& qid : queries_pipeline_task_leak) {
            // Cancel the query, and maybe try to report debug info to fe so that we can
            // collect debug info by sql or http api instead of search log.
            cancel_query(qid, PPlanFragmentCancelReason::INTERNAL_ERROR,
                         std::string("Pipeline task leak."));
        }

        if (!queries_to_cancel.empty()) {
            LOG(INFO) << "There are " << queries_to_cancel.size()
                      << " queries need to be cancelled, coordinator dead or restarted.";
        }

        for (const auto& qid : queries_to_cancel) {
            cancel_query(qid, PPlanFragmentCancelReason::INTERNAL_ERROR,
                         std::string("Coordinator dead."));
        }

        if (config::enable_brpc_connection_check) {
            for (auto it : brpc_stub_with_queries) {
                if (!it.first) {
                    continue;
                }
                _check_brpc_available(it.first, it.second);
            }
        }
    } while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::fragment_mgr_cancel_worker_interval_seconds)));
    LOG(INFO) << "FragmentMgr cancel worker is going to exit.";
}

void FragmentMgr::debug(std::stringstream& ss) {
    // Keep things simple
    ss << "FragmentMgr have " << _fragment_instance_map.num_items() << " jobs.\n";
    ss << "job_id\t\tstart_time\t\texecute_time(s)\n";
    VecDateTimeValue now = VecDateTimeValue::local_time();
    _fragment_instance_map.apply(
            [&](phmap::flat_hash_map<TUniqueId, std::shared_ptr<PlanFragmentExecutor>>& map)
                    -> Status {
                for (auto& it : map) {
                    ss << it.first << "\t" << it.second->start_time().debug_string() << "\t"
                       << now.second_diff(it.second->start_time()) << "\n";
                }
                return Status::OK();
            });
}

void FragmentMgr::_check_brpc_available(const std::shared_ptr<PBackendService_Stub>& brpc_stub,
                                        const BrpcItem& brpc_item) {
    const std::string message = "hello doris!";
    std::string error_message;
    int32_t failed_count = 0;
    const int64_t check_timeout_ms =
            std::max<int64_t>(100, config::brpc_connection_check_timeout_ms);

    while (true) {
        PHandShakeRequest request;
        request.set_hello(message);
        PHandShakeResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(check_timeout_ms);
        cntl.set_max_retry(10);
        brpc_stub->hand_shake(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            error_message = cntl.ErrorText();
            LOG(WARNING) << "brpc stub: " << brpc_item.network_address.hostname << ":"
                         << brpc_item.network_address.port << " check failed: " << error_message;
        } else if (response.has_status() && response.status().status_code() == 0) {
            break;
        } else {
            error_message = response.DebugString();
            LOG(WARNING) << "brpc stub: " << brpc_item.network_address.hostname << ":"
                         << brpc_item.network_address.port << " check failed: " << error_message;
        }
        failed_count++;
        if (failed_count == 2) {
            for (const auto& query_wptr : brpc_item.queries) {
                auto query = query_wptr.lock();
                if (query && !query->is_cancelled()) {
                    cancel_query(query->query_id(), PPlanFragmentCancelReason::INTERNAL_ERROR,
                                 fmt::format("brpc(dest: {}:{}) check failed: {}",
                                             brpc_item.network_address.hostname,
                                             brpc_item.network_address.port, error_message));
                }
            }

            LOG(WARNING) << "remove brpc stub from cache: " << brpc_item.network_address.hostname
                         << ":" << brpc_item.network_address.port << ", error: " << error_message;
            ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                    brpc_item.network_address.hostname, brpc_item.network_address.port);
            break;
        }
    }
}

/*
 * 1. resolve opaqued_query_plan to thrift structure
 * 2. build TExecPlanFragmentParams
 */
Status FragmentMgr::exec_external_plan_fragment(const TScanOpenParams& params,
                                                const TUniqueId& fragment_instance_id,
                                                std::vector<TScanColumnDesc>* selected_columns) {
    const std::string& opaqued_query_plan = params.opaqued_query_plan;
    std::string query_plan_info;
    // base64 decode query plan
    if (!base64_decode(opaqued_query_plan, &query_plan_info)) {
        LOG(WARNING) << "open context error: base64_decode decode opaqued_query_plan failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " validate error, should not be modified after returned Doris FE processed";
        return Status::InvalidArgument(msg.str());
    }
    TQueryPlanInfo t_query_plan_info;
    const uint8_t* buf = (const uint8_t*)query_plan_info.data();
    uint32_t len = query_plan_info.size();
    // deserialize TQueryPlanInfo
    auto st = deserialize_thrift_msg(buf, &len, false, &t_query_plan_info);
    if (!st.ok()) {
        LOG(WARNING) << "open context error: deserialize TQueryPlanInfo failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " deserialize error, should not be modified after returned Doris FE processed";
        return Status::InvalidArgument(msg.str());
    }

    // set up desc tbl
    DescriptorTbl* desc_tbl = nullptr;
    ObjectPool obj_pool;
    st = DescriptorTbl::create(&obj_pool, t_query_plan_info.desc_tbl, &desc_tbl);
    if (!st.ok()) {
        LOG(WARNING) << "open context error: extract DescriptorTbl failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " create DescriptorTbl error, should not be modified after returned Doris FE "
               "processed";
        return Status::InvalidArgument(msg.str());
    }
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    if (tuple_desc == nullptr) {
        LOG(WARNING) << "open context error: extract TupleDescriptor failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " get  TupleDescriptor error, should not be modified after returned Doris FE "
               "processed";
        return Status::InvalidArgument(msg.str());
    }
    // process selected columns form slots
    for (const SlotDescriptor* slot : tuple_desc->slots()) {
        TScanColumnDesc col;
        col.__set_name(slot->col_name());
        col.__set_type(to_thrift(slot->type().type));
        selected_columns->emplace_back(std::move(col));
    }

    VLOG_QUERY << "BackendService execute open()  TQueryPlanInfo: "
               << apache::thrift::ThriftDebugString(t_query_plan_info);
    // assign the param used to execute PlanFragment
    TExecPlanFragmentParams exec_fragment_params;
    exec_fragment_params.protocol_version = (PaloInternalServiceVersion::type)0;
    exec_fragment_params.__set_is_simplified_param(false);
    exec_fragment_params.__set_fragment(t_query_plan_info.plan_fragment);
    exec_fragment_params.__set_desc_tbl(t_query_plan_info.desc_tbl);

    // assign the param used for executing of PlanFragment-self
    TPlanFragmentExecParams fragment_exec_params;
    fragment_exec_params.query_id = t_query_plan_info.query_id;
    fragment_exec_params.fragment_instance_id = fragment_instance_id;
    std::map<::doris::TPlanNodeId, std::vector<TScanRangeParams>> per_node_scan_ranges;
    std::vector<TScanRangeParams> scan_ranges;
    std::vector<int64_t> tablet_ids = params.tablet_ids;
    TNetworkAddress address;
    address.hostname = BackendOptions::get_localhost();
    address.port = doris::config::be_port;
    std::map<int64_t, TTabletVersionInfo> tablet_info = t_query_plan_info.tablet_info;
    for (auto tablet_id : params.tablet_ids) {
        TPaloScanRange scan_range;
        scan_range.db_name = params.database;
        scan_range.table_name = params.table;
        auto iter = tablet_info.find(tablet_id);
        if (iter != tablet_info.end()) {
            TTabletVersionInfo info = iter->second;
            scan_range.tablet_id = tablet_id;
            scan_range.version = std::to_string(info.version);
            // Useless but it is required field in TPaloScanRange
            scan_range.version_hash = "0";
            scan_range.schema_hash = std::to_string(info.schema_hash);
            scan_range.hosts.push_back(address);
        } else {
            std::stringstream msg;
            msg << "tablet_id: " << tablet_id << " not found";
            LOG(WARNING) << "tablet_id [ " << tablet_id << " ] not found";
            return Status::NotFound(msg.str());
        }
        TScanRange doris_scan_range;
        doris_scan_range.__set_palo_scan_range(scan_range);
        TScanRangeParams scan_range_params;
        scan_range_params.scan_range = doris_scan_range;
        scan_ranges.push_back(scan_range_params);
    }
    per_node_scan_ranges.insert(std::make_pair((::doris::TPlanNodeId)0, scan_ranges));
    fragment_exec_params.per_node_scan_ranges = per_node_scan_ranges;
    exec_fragment_params.__set_params(fragment_exec_params);
    TQueryOptions query_options;
    query_options.batch_size = params.batch_size;
    query_options.execution_timeout = params.execution_timeout;
    query_options.mem_limit = params.mem_limit;
    query_options.query_type = TQueryType::EXTERNAL;
    exec_fragment_params.__set_query_options(query_options);
    VLOG_ROW << "external exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(exec_fragment_params).c_str();
    return exec_plan_fragment(exec_fragment_params, QuerySource::EXTERNAL_CONNECTOR);
}

Status FragmentMgr::apply_filter(const PPublishFilterRequest* request,
                                 butil::IOBufAsZeroCopyInputStream* attach_data) {
    bool is_pipeline = request->has_is_pipeline() && request->is_pipeline();

    UniqueId fragment_instance_id = request->fragment_instance_id();
    TUniqueId tfragment_instance_id = fragment_instance_id.to_thrift();

    std::shared_ptr<PlanFragmentExecutor> fragment_executor;
    std::shared_ptr<pipeline::PipelineFragmentContext> pip_context;
    QueryThreadContext query_thread_context;

    RuntimeFilterMgr* runtime_filter_mgr = nullptr;
    if (is_pipeline) {
        pip_context = _pipeline_map.find(tfragment_instance_id);
        if (pip_context == nullptr) {
            VLOG_CRITICAL << "unknown.... fragment-id:" << fragment_instance_id;
            return Status::InvalidArgument("fragment-id: {}", fragment_instance_id.to_string());
        }

        DCHECK(pip_context != nullptr);
        runtime_filter_mgr = pip_context->get_query_ctx()->runtime_filter_mgr();
        query_thread_context = {pip_context->get_query_ctx()->query_id(),
                                pip_context->get_query_ctx()->query_mem_tracker};
    } else {
        fragment_executor = _fragment_instance_map.find(tfragment_instance_id);
        if (fragment_executor == nullptr) {
            VLOG_CRITICAL << "unknown.... fragment instance id:" << print_id(tfragment_instance_id);
            return Status::InvalidArgument("fragment-id: {}", print_id(tfragment_instance_id));
        }

        DCHECK(fragment_executor != nullptr);
        runtime_filter_mgr =
                fragment_executor->runtime_state()->get_query_ctx()->runtime_filter_mgr();
        query_thread_context = {fragment_executor->get_query_ctx()->query_id(),
                                fragment_executor->get_query_ctx()->query_mem_tracker};
    }

    SCOPED_ATTACH_TASK(query_thread_context);
    return runtime_filter_mgr->update_filter(request, attach_data);
}

Status FragmentMgr::apply_filterv2(const PPublishFilterRequestV2* request,
                                   butil::IOBufAsZeroCopyInputStream* attach_data) {
    bool is_pipeline = request->has_is_pipeline() && request->is_pipeline();
    int64_t start_apply = MonotonicMillis();

    std::shared_ptr<PlanFragmentExecutor> fragment_executor;
    std::shared_ptr<pipeline::PipelineFragmentContext> pip_context;
    QueryThreadContext query_thread_context;

    RuntimeFilterMgr* runtime_filter_mgr = nullptr;
    ObjectPool* pool = nullptr;

    const auto& fragment_instance_ids = request->fragment_instance_ids();
    {
        for (UniqueId fragment_instance_id : fragment_instance_ids) {
            TUniqueId tfragment_instance_id = fragment_instance_id.to_thrift();

            if (is_pipeline) {
                pip_context = _pipeline_map.find(tfragment_instance_id);
                if (pip_context == nullptr) {
                    continue;
                }

                DCHECK(pip_context != nullptr);
                runtime_filter_mgr = pip_context->get_query_ctx()->runtime_filter_mgr();
                pool = &pip_context->get_query_ctx()->obj_pool;
                query_thread_context = {pip_context->get_query_ctx()->query_id(),
                                        pip_context->get_query_ctx()->query_mem_tracker,
                                        pip_context->get_query_ctx()->workload_group()};
            } else {
                fragment_executor = _fragment_instance_map.find(tfragment_instance_id);
                if (fragment_executor == nullptr) {
                    continue;
                }

                DCHECK(fragment_executor != nullptr);
                runtime_filter_mgr = fragment_executor->get_query_ctx()->runtime_filter_mgr();
                pool = &fragment_executor->get_query_ctx()->obj_pool;
                query_thread_context = {fragment_executor->get_query_ctx()->query_id(),
                                        fragment_executor->get_query_ctx()->query_mem_tracker};
            }
            break;
        }
    }

    if (runtime_filter_mgr == nullptr) {
        // all instance finished
        return Status::OK();
    }

    SCOPED_ATTACH_TASK(query_thread_context);
    // 1. get the target filters
    std::vector<IRuntimeFilter*> filters;
    RETURN_IF_ERROR(runtime_filter_mgr->get_consume_filters(request->filter_id(), filters));

    // 2. create the filter wrapper to replace or ignore the target filters
    if (!filters.empty()) {
        UpdateRuntimeFilterParamsV2 params {request, attach_data, pool, filters[0]->column_type()};
        RuntimePredicateWrapper* filter_wrapper = nullptr;
        RETURN_IF_ERROR(IRuntimeFilter::create_wrapper(&params, &filter_wrapper));

        std::ranges::for_each(filters, [&](auto& filter) {
            filter->update_filter(filter_wrapper, request->merge_time(), start_apply);
        });
    }

    return Status::OK();
}

Status FragmentMgr::send_filter_size(const PSendFilterSizeRequest* request) {
    UniqueId queryid = request->query_id();

    std::shared_ptr<QueryContext> query_ctx;
    {
        TUniqueId query_id;
        query_id.__set_hi(queryid.hi);
        query_id.__set_lo(queryid.lo);
        query_ctx = _query_ctx_map.find(query_id);
        if (query_ctx == nullptr) {
            return Status::EndOfFile("Query context (query-id: {}) not found, maybe finished",
                                     queryid.to_string());
        }
    }

    std::shared_ptr<RuntimeFilterMergeControllerEntity> filter_controller;
    RETURN_IF_ERROR(_runtimefilter_controller.acquire(queryid, &filter_controller));
    auto merge_status = filter_controller->send_filter_size(request);
    return merge_status;
}

Status FragmentMgr::sync_filter_size(const PSyncFilterSizeRequest* request) {
    UniqueId queryid = request->query_id();
    std::shared_ptr<QueryContext> query_ctx;
    {
        TUniqueId query_id;
        query_id.__set_hi(queryid.hi);
        query_id.__set_lo(queryid.lo);
        query_ctx = _query_ctx_map.find(query_id);
        if (query_ctx == nullptr) {
            return Status::InvalidArgument("query-id: {}", queryid.to_string());
        }
    }
    return query_ctx->runtime_filter_mgr()->sync_filter_size(request);
}

Status FragmentMgr::merge_filter(const PMergeFilterRequest* request,
                                 butil::IOBufAsZeroCopyInputStream* attach_data) {
    UniqueId queryid = request->query_id();
    bool opt_remote_rf = request->has_opt_remote_rf() && request->opt_remote_rf();
    std::shared_ptr<RuntimeFilterMergeControllerEntity> filter_controller;
    RETURN_IF_ERROR(_runtimefilter_controller.acquire(queryid, &filter_controller));

    std::shared_ptr<QueryContext> query_ctx;
    {
        TUniqueId query_id;
        query_id.__set_hi(queryid.hi);
        query_id.__set_lo(queryid.lo);
        query_ctx = _query_ctx_map.find(query_id);
        if (query_ctx == nullptr) {
            return Status::InvalidArgument("query-id: {}", queryid.to_string());
        }
    }
    SCOPED_ATTACH_TASK(query_ctx.get());
    auto merge_status = filter_controller->merge(request, attach_data, opt_remote_rf);
    return merge_status;
}

void FragmentMgr::_setup_shared_hashtable_for_broadcast_join(const TExecPlanFragmentParams& params,
                                                             QueryContext* query_ctx) {
    if (!params.query_options.__isset.enable_share_hash_table_for_broadcast_join ||
        !params.query_options.enable_share_hash_table_for_broadcast_join) {
        return;
    }

    if (!params.__isset.fragment || !params.fragment.__isset.plan ||
        params.fragment.plan.nodes.empty()) {
        return;
    }
    for (auto& node : params.fragment.plan.nodes) {
        if (node.node_type != TPlanNodeType::HASH_JOIN_NODE ||
            !node.hash_join_node.__isset.is_broadcast_join ||
            !node.hash_join_node.is_broadcast_join) {
            continue;
        }

        if (params.build_hash_table_for_broadcast_join) {
            query_ctx->get_shared_hash_table_controller()->set_builder_and_consumers(
                    params.params.fragment_instance_id, node.node_id);
        }
    }
}

void FragmentMgr::_setup_shared_hashtable_for_broadcast_join(
        const TPipelineFragmentParams& params, const TPipelineInstanceParams& local_params,
        QueryContext* query_ctx) {
    if (!params.query_options.__isset.enable_share_hash_table_for_broadcast_join ||
        !params.query_options.enable_share_hash_table_for_broadcast_join) {
        return;
    }

    if (!params.__isset.fragment || !params.fragment.__isset.plan ||
        params.fragment.plan.nodes.empty()) {
        return;
    }
    for (auto& node : params.fragment.plan.nodes) {
        if (node.node_type != TPlanNodeType::HASH_JOIN_NODE ||
            !node.hash_join_node.__isset.is_broadcast_join ||
            !node.hash_join_node.is_broadcast_join) {
            continue;
        }

        if (local_params.build_hash_table_for_broadcast_join) {
            query_ctx->get_shared_hash_table_controller()->set_builder_and_consumers(
                    local_params.fragment_instance_id, node.node_id);
        }
    }
}

void FragmentMgr::_setup_shared_hashtable_for_broadcast_join(const TPipelineFragmentParams& params,
                                                             QueryContext* query_ctx) {
    if (!params.query_options.__isset.enable_share_hash_table_for_broadcast_join ||
        !params.query_options.enable_share_hash_table_for_broadcast_join) {
        return;
    }

    if (!params.__isset.fragment || !params.fragment.__isset.plan ||
        params.fragment.plan.nodes.empty()) {
        return;
    }
    for (auto& node : params.fragment.plan.nodes) {
        if (node.node_type != TPlanNodeType::HASH_JOIN_NODE ||
            !node.hash_join_node.__isset.is_broadcast_join ||
            !node.hash_join_node.is_broadcast_join) {
            continue;
        }

        for (auto& local_param : params.local_params) {
            if (local_param.build_hash_table_for_broadcast_join) {
                query_ctx->get_shared_hash_table_controller()->set_builder_and_consumers(
                        local_param.fragment_instance_id, node.node_id);
            }
        }
    }
}

void FragmentMgr::get_runtime_query_info(std::vector<WorkloadQueryInfo>* query_info_list) {
    _query_ctx_map.apply(
            [&](phmap::flat_hash_map<TUniqueId, std::shared_ptr<QueryContext>>& map) -> Status {
                for (const auto& q : map) {
                    WorkloadQueryInfo workload_query_info;
                    workload_query_info.query_id = print_id(q.first);
                    workload_query_info.tquery_id = q.first;
                    workload_query_info.wg_id = q.second->workload_group() == nullptr
                                                        ? -1
                                                        : q.second->workload_group()->id();
                    query_info_list->push_back(workload_query_info);
                }
                return Status::OK();
            });
}

} // namespace doris
