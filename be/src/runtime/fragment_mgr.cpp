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
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <pthread.h>
#include <sys/time.h>
#include <thrift/TApplicationException.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TTransportException.h>
#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <ctime>

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
#include "common/exception.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/utils.h"
#include "io/fs/stream_load_pipe.h"
#include "pipeline/pipeline_fragment_context.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/frontend_info.h"
#include "runtime/primitive_type.h"
#include "runtime/query_context.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "runtime_filter/runtime_filter_consumer.h"
#include "runtime_filter/runtime_filter_mgr.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/debug_points.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/network_util.h"
#include "util/runtime_profile.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(fragment_instance_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(timeout_canceled_fragment_count, MetricUnit::NOUNIT);

bvar::LatencyRecorder g_fragmentmgr_prepare_latency("doris_FragmentMgr", "prepare");

bvar::Adder<uint64_t> g_fragment_executing_count("fragment_executing_count");
bvar::Status<uint64_t> g_fragment_last_active_time(
        "fragment_last_active_time", duration_cast<std::chrono::milliseconds>(
                                             std::chrono::system_clock::now().time_since_epoch())
                                             .count());

uint64_t get_fragment_executing_count() {
    return g_fragment_executing_count.get_value();
}
uint64_t get_fragment_last_active_time() {
    return g_fragment_last_active_time.get_value();
}

std::string to_load_error_http_path(const std::string& file_name) {
    if (file_name.empty()) {
        return "";
    }
    if (file_name.compare(0, 4, "http") == 0) {
        return file_name;
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
    const int32_t timeout_ms = 3 * 1000;
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
    if (!rpc_result.__isset.status || rpc_result.status.status_code != TStatusCode::OK) {
        LOG_WARNING("Failed to fetch running queries from {}, reason: {}",
                    PrintThriftNetworkAddress(fe_info.info.coordinator_address),
                    doris::to_string(rpc_result.status.status_code));
        return Status::InternalError("Failed to fetch running queries from {}, reason: {}",
                                     PrintThriftNetworkAddress(fe_info.info.coordinator_address),
                                     doris::to_string(rpc_result.status.status_code));
    }

    if (!rpc_result.__isset.running_queries) {
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
            query_ctx = search->second.lock();
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

    auto s = Thread::create(
            "FragmentMgr", "cancel_timeout_plan_fragment", [this]() { this->cancel_worker(); },
            &_cancel_thread);
    CHECK(s.ok()) << s.to_string();

    s = ThreadPoolBuilder("FragmentMgrAsyncWorkThreadPool")
                .set_min_threads(config::fragment_mgr_async_work_pool_thread_num_min)
                .set_max_threads(config::fragment_mgr_async_work_pool_thread_num_max)
                .set_max_queue_size(config::fragment_mgr_async_work_pool_queue_size)
                .build(&_thread_pool);
    CHECK(s.ok()) << s.to_string();
}

FragmentMgr::~FragmentMgr() = default;

void FragmentMgr::stop() {
    DEREGISTER_HOOK_METRIC(fragment_instance_count);
    _stop_background_threads_latch.count_down();
    if (_cancel_thread) {
        _cancel_thread->join();
    }

    _thread_pool->shutdown();
    // Only me can delete
    _query_ctx_map.clear();
    _pipeline_map.clear();
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
    return _thread_pool->submit_func([this, req, ctx]() {
        SCOPED_ATTACH_TASK(ctx->get_query_ctx()->query_mem_tracker());
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
    if (req.coord_addr.hostname == "external") {
        // External query (flink/spark read tablets) not need to report to FE.
        return;
    }
    int callback_retries = 10;
    const int sleep_ms = 1000;
    Status exec_status = req.status;
    Status coord_status;
    std::unique_ptr<FrontendServiceConnection> coord = nullptr;
    do {
        coord = std::make_unique<FrontendServiceConnection>(_exec_env->frontend_client_cache(),
                                                            req.coord_addr, &coord_status);
        if (!coord_status.ok()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        }
    } while (!coord_status.ok() && callback_retries-- > 0);

    if (!coord_status.ok()) {
        std::stringstream ss;
        UniqueId uid(req.query_id.hi, req.query_id.lo);
        static_cast<void>(req.cancel_fn(Status::InternalError(
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
    params.__isset.profile = false;

    DCHECK(req.runtime_state != nullptr);

    if (req.runtime_state->query_type() == TQueryType::LOAD) {
        params.__set_loaded_rows(req.runtime_state->num_rows_load_total());
        params.__set_loaded_bytes(req.runtime_state->num_bytes_load_total());
    } else {
        DCHECK(!req.runtime_states.empty());
        if (!req.runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : req.runtime_state->output_files()) {
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
        t.__set_num_finished_range(cast_set<int>(req.runtime_state->num_finished_range()));
        t.__set_loaded_rows(req.runtime_state->num_rows_load_total());
        t.__set_loaded_bytes(req.runtime_state->num_bytes_load_total());
        params.fragment_instance_reports.push_back(t);
    } else if (!req.runtime_states.empty()) {
        for (auto* rs : req.runtime_states) {
            if (rs->num_rows_load_total() > 0 || rs->num_rows_load_filtered() > 0 ||
                rs->num_finished_range() > 0) {
                params.__isset.load_counters = true;
                num_rows_load_success += rs->num_rows_load_success();
                num_rows_load_filtered += rs->num_rows_load_filtered();
                num_rows_load_unselected += rs->num_rows_load_unselected();
                params.__isset.fragment_instance_reports = true;
                TFragmentInstanceReport t;
                t.__set_fragment_instance_id(rs->fragment_instance_id());
                t.__set_num_finished_range(cast_set<int>(rs->num_finished_range()));
                t.__set_loaded_rows(rs->num_rows_load_total());
                t.__set_loaded_bytes(rs->num_bytes_load_total());
                params.fragment_instance_reports.push_back(t);
            }
        }
    }
    params.load_counters.emplace(s_dpp_normal_all, std::to_string(num_rows_load_success));
    params.load_counters.emplace(s_dpp_abnormal_all, std::to_string(num_rows_load_filtered));
    params.load_counters.emplace(s_unselected_rows, std::to_string(num_rows_load_unselected));

    if (!req.load_error_url.empty()) {
        params.__set_tracking_url(req.load_error_url);
    }
    for (auto* rs : req.runtime_states) {
        if (rs->wal_id() > 0) {
            params.__set_txn_id(rs->wal_id());
            params.__set_label(rs->import_label());
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
    if (auto tci = req.runtime_state->tablet_commit_infos(); !tci.empty()) {
        params.__isset.commitInfos = true;
        params.commitInfos.insert(params.commitInfos.end(), tci.begin(), tci.end());
    } else if (!req.runtime_states.empty()) {
        for (auto* rs : req.runtime_states) {
            if (auto rs_tci = rs->tablet_commit_infos(); !rs_tci.empty()) {
                params.__isset.commitInfos = true;
                params.commitInfos.insert(params.commitInfos.end(), rs_tci.begin(), rs_tci.end());
            }
        }
    }
    if (auto eti = req.runtime_state->error_tablet_infos(); !eti.empty()) {
        params.__isset.errorTabletInfos = true;
        params.errorTabletInfos.insert(params.errorTabletInfos.end(), eti.begin(), eti.end());
    } else if (!req.runtime_states.empty()) {
        for (auto* rs : req.runtime_states) {
            if (auto rs_eti = rs->error_tablet_infos(); !rs_eti.empty()) {
                params.__isset.errorTabletInfos = true;
                params.errorTabletInfos.insert(params.errorTabletInfos.end(), rs_eti.begin(),
                                               rs_eti.end());
            }
        }
    }
    if (auto hpu = req.runtime_state->hive_partition_updates(); !hpu.empty()) {
        params.__isset.hive_partition_updates = true;
        params.hive_partition_updates.insert(params.hive_partition_updates.end(), hpu.begin(),
                                             hpu.end());
    } else if (!req.runtime_states.empty()) {
        for (auto* rs : req.runtime_states) {
            if (auto rs_hpu = rs->hive_partition_updates(); !rs_hpu.empty()) {
                params.__isset.hive_partition_updates = true;
                params.hive_partition_updates.insert(params.hive_partition_updates.end(),
                                                     rs_hpu.begin(), rs_hpu.end());
            }
        }
    }
    if (auto icd = req.runtime_state->iceberg_commit_datas(); !icd.empty()) {
        params.__isset.iceberg_commit_datas = true;
        params.iceberg_commit_datas.insert(params.iceberg_commit_datas.end(), icd.begin(),
                                           icd.end());
    } else if (!req.runtime_states.empty()) {
        for (auto* rs : req.runtime_states) {
            if (auto rs_icd = rs->iceberg_commit_datas(); !rs_icd.empty()) {
                params.__isset.iceberg_commit_datas = true;
                params.iceberg_commit_datas.insert(params.iceberg_commit_datas.end(),
                                                   rs_icd.begin(), rs_icd.end());
            }
        }
    }

    // Send new errors to coordinator
    req.runtime_state->get_unreported_errors(&(params.error_log));
    params.__isset.error_log = (!params.error_log.empty());

    if (_exec_env->cluster_info()->backend_id != 0) {
        params.__set_backend_id(_exec_env->cluster_info()->backend_id);
    }

    TReportExecStatusResult res;
    Status rpc_status;

    VLOG_DEBUG << "reportExecStatus params is "
               << apache::thrift::ThriftDebugString(params).c_str();
    if (!exec_status.ok()) {
        LOG(WARNING) << "report error status: " << exec_status.msg()
                     << " to coordinator: " << req.coord_addr
                     << ", query id: " << print_id(req.query_id);
    }
    try {
        try {
            (*coord)->reportExecStatus(res, params);
        } catch ([[maybe_unused]] TTransportException& e) {
#ifndef ADDRESS_SANITIZER
            LOG(WARNING) << "Retrying ReportExecStatus. query id: " << print_id(req.query_id)
                         << ", instance id: " << print_id(req.fragment_instance_id) << " to "
                         << req.coord_addr << ", err: " << e.what();
#endif
            rpc_status = coord->reopen();

            if (!rpc_status.ok()) {
                // we need to cancel the execution of this fragment
                req.cancel_fn(rpc_status);
                return;
            }
            (*coord)->reportExecStatus(res, params);
        }

        rpc_status = Status::create<false>(res.status);
    } catch (TException& e) {
        rpc_status = Status::InternalError("ReportExecStatus() to {} failed: {}",
                                           PrintThriftNetworkAddress(req.coord_addr), e.what());
    }

    if (!rpc_status.ok()) {
        LOG_INFO("Going to cancel query {} since report exec status got rpc failed: {}",
                 print_id(req.query_id), rpc_status.to_string());
        // we need to cancel the execution of this fragment
        req.cancel_fn(rpc_status);
    }
}

static void empty_function(RuntimeState*, Status*) {}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params,
                                       const QuerySource query_source) {
    return Status::InternalError("Non-pipeline is disabled!");
}

Status FragmentMgr::exec_plan_fragment(const TPipelineFragmentParams& params,
                                       const QuerySource query_source,
                                       const TPipelineFragmentParamsList& parent) {
    if (params.txn_conf.need_txn) {
        std::shared_ptr<StreamLoadContext> stream_load_ctx =
                std::make_shared<StreamLoadContext>(_exec_env);
        stream_load_ctx->db = params.txn_conf.db;
        stream_load_ctx->db_id = params.txn_conf.db_id;
        stream_load_ctx->table = params.txn_conf.tbl;
        stream_load_ctx->txn_id = params.txn_conf.txn_id;
        stream_load_ctx->id = UniqueId(params.query_id);
        stream_load_ctx->put_result.__set_pipeline_params(params);
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

        RETURN_IF_ERROR(
                _exec_env->stream_load_executor()->execute_plan_fragment(stream_load_ctx, parent));
        return Status::OK();
    } else {
        return exec_plan_fragment(params, query_source, empty_function, parent);
    }
}

// Stage 2. prepare finished. then get FE instruction to execute
Status FragmentMgr::start_query_execution(const PExecPlanFragmentStartRequest* request) {
    TUniqueId query_id;
    query_id.__set_hi(request->query_id().hi());
    query_id.__set_lo(request->query_id().lo());
    auto q_ctx = get_query_ctx(query_id);
    if (q_ctx) {
        q_ctx->set_ready_to_execute(Status::OK());
        LOG_INFO("Query {} start execution", print_id(query_id));
    } else {
        return Status::InternalError(
                "Failed to get query fragments context. Query {} may be "
                "timeout or be cancelled. host: {}",
                print_id(query_id), BackendOptions::get_localhost());
    }
    return Status::OK();
}

void FragmentMgr::remove_pipeline_context(std::pair<TUniqueId, int> key) {
    int64_t now = duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    g_fragment_executing_count << -1;
    g_fragment_last_active_time.set_value(now);

    _pipeline_map.erase(key);
}

std::shared_ptr<QueryContext> FragmentMgr::get_query_ctx(const TUniqueId& query_id) {
    auto val = _query_ctx_map.find(query_id);
    if (auto q_ctx = val.lock()) {
        return q_ctx;
    }
    return nullptr;
}

Status FragmentMgr::_get_or_create_query_ctx(const TPipelineFragmentParams& params,
                                             const TPipelineFragmentParamsList& parent,
                                             QuerySource query_source,
                                             std::shared_ptr<QueryContext>& query_ctx) {
    auto query_id = params.query_id;
    DBUG_EXECUTE_IF("FragmentMgr._get_query_ctx.failed", {
        return Status::InternalError("FragmentMgr._get_query_ctx.failed, query id {}",
                                     print_id(query_id));
    });

    // Find _query_ctx_map, in case some other request has already
    // create the query fragments context.
    query_ctx = get_query_ctx(query_id);
    if (params.is_simplified_param) {
        // Get common components from _query_ctx_map
        if (!query_ctx) {
            return Status::InternalError(
                    "Failed to get query fragments context. Query {} may be timeout or be "
                    "cancelled. host: {}",
                    print_id(query_id), BackendOptions::get_localhost());
        }
    } else {
        if (!query_ctx) {
            RETURN_IF_ERROR(_query_ctx_map.apply_if_not_exists(
                    query_id, query_ctx,
                    [&](phmap::flat_hash_map<TUniqueId, std::weak_ptr<QueryContext>>& map)
                            -> Status {
                        WorkloadGroupPtr workload_group_ptr = nullptr;
                        std::vector<uint64_t> wg_id_set;
                        if (params.__isset.workload_groups && !params.workload_groups.empty()) {
                            for (auto& wg : params.workload_groups) {
                                wg_id_set.push_back(wg.id);
                            }
                        }
                        workload_group_ptr = _exec_env->workload_group_mgr()->get_group(wg_id_set);

                        // First time a fragment of a query arrived. print logs.
                        LOG(INFO) << "query_id: " << print_id(query_id)
                                  << ", coord_addr: " << params.coord
                                  << ", total fragment num on current host: "
                                  << params.fragment_num_on_host
                                  << ", fe process uuid: " << params.query_options.fe_process_uuid
                                  << ", query type: " << params.query_options.query_type
                                  << ", report audit fe:" << params.current_connect_fe
                                  << ", use wg:" << workload_group_ptr->id() << ","
                                  << workload_group_ptr->name();

                        // This may be a first fragment request of the query.
                        // Create the query fragments context.
                        query_ctx = QueryContext::create(query_id, _exec_env, params.query_options,
                                                         params.coord, params.is_nereids,
                                                         params.current_connect_fe, query_source);
                        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(query_ctx->query_mem_tracker());
                        RETURN_IF_ERROR(DescriptorTbl::create(
                                &(query_ctx->obj_pool), params.desc_tbl, &(query_ctx->desc_tbl)));
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

                        if (params.__isset.llm_resources) {
                            query_ctx->set_llm_resources(params.llm_resources);
                        }

                        RETURN_IF_ERROR(query_ctx->set_workload_group(workload_group_ptr));

                        if (parent.__isset.runtime_filter_info) {
                            auto info = parent.runtime_filter_info;
                            if (info.__isset.runtime_filter_params) {
                                if (!info.runtime_filter_params.rid_to_runtime_filter.empty()) {
                                    auto handler =
                                            std::make_shared<RuntimeFilterMergeControllerEntity>();
                                    RETURN_IF_ERROR(
                                            handler->init(query_ctx, info.runtime_filter_params));
                                    query_ctx->set_merge_controller_handler(handler);
                                }

                                query_ctx->runtime_filter_mgr()->set_runtime_filter_params(
                                        info.runtime_filter_params);
                            }
                            if (info.__isset.topn_filter_descs) {
                                query_ctx->init_runtime_predicates(info.topn_filter_descs);
                            }
                        }

                        // There is some logic in query ctx's dctor, we could not check if exists and delete the
                        // temp query ctx now. For example, the query id maybe removed from workload group's queryset.
                        map.insert({query_id, query_ctx});
                        return Status::OK();
                    }));
        }
    }
    return Status::OK();
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params,
                                       QuerySource query_source, const FinishCallback& cb) {
    return Status::InternalError("Non-pipeline is disabled!");
}

std::string FragmentMgr::dump_pipeline_tasks(int64_t duration) {
    fmt::memory_buffer debug_string_buffer;
    size_t i = 0;
    {
        fmt::format_to(debug_string_buffer,
                       "{} pipeline fragment contexts are still running! duration_limit={}\n",
                       _pipeline_map.num_items(), duration);
        timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);

        _pipeline_map.apply([&](phmap::flat_hash_map<
                                    std::pair<TUniqueId, int>,
                                    std::shared_ptr<pipeline::PipelineFragmentContext>>& map)
                                    -> Status {
            for (auto& it : map) {
                auto elapsed = it.second->elapsed_time() / 1000000000;
                if (elapsed < duration) {
                    // Only display tasks which has been running for more than {duration} seconds.
                    continue;
                }
                auto timeout_second = it.second->timeout_second();
                fmt::format_to(
                        debug_string_buffer,
                        "No.{} (elapse_second={}s, query_timeout_second={}s, is_timeout={}) : {}\n",
                        i, elapsed, timeout_second, it.second->is_timeout(now),
                        it.second->debug_string());
                i++;
            }
            return Status::OK();
        });
    }
    return fmt::to_string(debug_string_buffer);
}

std::string FragmentMgr::dump_pipeline_tasks(TUniqueId& query_id) {
    if (auto q_ctx = get_query_ctx(query_id)) {
        return q_ctx->print_all_pipeline_context();
    } else {
        return fmt::format(
                "Dump pipeline tasks failed: Query context (query id = {}) not found. \n",
                print_id(query_id));
    }
}

Status FragmentMgr::exec_plan_fragment(const TPipelineFragmentParams& params,
                                       QuerySource query_source, const FinishCallback& cb,
                                       const TPipelineFragmentParamsList& parent) {
    VLOG_ROW << "Query: " << print_id(params.query_id) << " exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(params).c_str();
    // sometimes TExecPlanFragmentParams debug string is too long and glog
    // will truncate the log line, so print query options seperately for debuggin purpose
    VLOG_ROW << "Query: " << print_id(params.query_id) << "query options is "
             << apache::thrift::ThriftDebugString(params.query_options).c_str();

    std::shared_ptr<QueryContext> query_ctx;
    RETURN_IF_ERROR(_get_or_create_query_ctx(params, parent, query_source, query_ctx));
    SCOPED_ATTACH_TASK(query_ctx.get()->resource_ctx());
    int64_t duration_ns = 0;
    std::shared_ptr<pipeline::PipelineFragmentContext> context =
            std::make_shared<pipeline::PipelineFragmentContext>(
                    query_ctx->query_id(), params.fragment_id, query_ctx, _exec_env, cb,
                    std::bind<Status>(std::mem_fn(&FragmentMgr::trigger_pipeline_context_report),
                                      this, std::placeholders::_1, std::placeholders::_2));
    {
        SCOPED_RAW_TIMER(&duration_ns);
        Status prepare_st = Status::OK();
        ASSIGN_STATUS_IF_CATCH_EXCEPTION(prepare_st = context->prepare(params, _thread_pool.get()),
                                         prepare_st);
        DBUG_EXECUTE_IF("FragmentMgr.exec_plan_fragment.prepare_failed", {
            prepare_st = Status::Aborted("FragmentMgr.exec_plan_fragment.prepare_failed");
        });
        if (!prepare_st.ok()) {
            query_ctx->cancel(prepare_st, params.fragment_id);
            return prepare_st;
        }
    }
    g_fragmentmgr_prepare_latency << (duration_ns / 1000);

    DBUG_EXECUTE_IF("FragmentMgr.exec_plan_fragment.failed",
                    { return Status::Aborted("FragmentMgr.exec_plan_fragment.failed"); });
    {
        int64_t now = duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
        g_fragment_executing_count << 1;
        g_fragment_last_active_time.set_value(now);

        // (query_id, fragment_id) is executed only on one BE, locks _pipeline_map.
        auto res = _pipeline_map.find({params.query_id, params.fragment_id});
        if (res != nullptr) {
            return Status::InternalError(
                    "exec_plan_fragment query_id({}) input duplicated fragment_id({})",
                    print_id(params.query_id), params.fragment_id);
        }
        _pipeline_map.insert({params.query_id, params.fragment_id}, context);
    }

    if (!params.__isset.need_wait_execution_trigger || !params.need_wait_execution_trigger) {
        query_ctx->set_ready_to_execute_only();
    }

    query_ctx->set_pipeline_context(params.fragment_id, context);

    RETURN_IF_ERROR(context->submit());
    return Status::OK();
}

void FragmentMgr::cancel_query(const TUniqueId query_id, const Status reason) {
    std::shared_ptr<QueryContext> query_ctx = nullptr;
    {
        if (auto q_ctx = get_query_ctx(query_id)) {
            query_ctx = q_ctx;
        } else {
            LOG(WARNING) << "Query " << print_id(query_id)
                         << " does not exists, failed to cancel it";
            return;
        }
    }
    query_ctx->cancel(reason);
    _query_ctx_map.erase(query_id);
    LOG(INFO) << "Query " << print_id(query_id)
              << " is cancelled and removed. Reason: " << reason.to_string();
}

void FragmentMgr::cancel_worker() {
    LOG(INFO) << "FragmentMgr cancel worker start working.";

    timespec check_invalid_query_last_timestamp;
    clock_gettime(CLOCK_MONOTONIC, &check_invalid_query_last_timestamp);

    do {
        std::vector<TUniqueId> queries_lost_coordinator;
        std::vector<TUniqueId> queries_timeout;
        std::vector<TUniqueId> queries_pipeline_task_leak;
        // Fe process uuid -> set<QueryId>
        std::map<int64_t, std::unordered_set<TUniqueId>> running_queries_on_all_fes;
        const std::map<TNetworkAddress, FrontendInfo>& running_fes =
                ExecEnv::GetInstance()->get_running_frontends();

        timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);

        if (config::enable_pipeline_task_leakage_detect &&
            now.tv_sec - check_invalid_query_last_timestamp.tv_sec >
                    config::pipeline_task_leakage_detect_period_secs) {
            check_invalid_query_last_timestamp = now;
            running_queries_on_all_fes = _get_all_running_queries_from_fe();
        } else {
            running_queries_on_all_fes.clear();
        }

        std::vector<std::shared_ptr<pipeline::PipelineFragmentContext>> ctx;
        _pipeline_map.apply(
                [&](phmap::flat_hash_map<std::pair<TUniqueId, int>,
                                         std::shared_ptr<pipeline::PipelineFragmentContext>>& map)
                        -> Status {
                    ctx.reserve(ctx.size() + map.size());
                    for (auto& pipeline_itr : map) {
                        ctx.push_back(pipeline_itr.second);
                    }
                    return Status::OK();
                });
        for (auto& c : ctx) {
            c->clear_finished_tasks();
        }

        std::unordered_map<std::shared_ptr<PBackendService_Stub>, BrpcItem> brpc_stub_with_queries;
        {
            _query_ctx_map.apply([&](phmap::flat_hash_map<TUniqueId, std::weak_ptr<QueryContext>>&
                                             map) -> Status {
                for (auto it = map.begin(); it != map.end();) {
                    if (auto q_ctx = it->second.lock()) {
                        if (q_ctx->is_timeout(now)) {
                            LOG_WARNING("Query {} is timeout", print_id(it->first));
                            queries_timeout.push_back(it->first);
                        } else if (config::enable_brpc_connection_check) {
                            auto brpc_stubs = q_ctx->get_using_brpc_stubs();
                            for (auto& item : brpc_stubs) {
                                if (!brpc_stub_with_queries.contains(item.second)) {
                                    brpc_stub_with_queries.emplace(item.second,
                                                                   BrpcItem {item.first, {q_ctx}});
                                } else {
                                    brpc_stub_with_queries[item.second].queries.emplace_back(q_ctx);
                                }
                            }
                        }
                        ++it;
                    } else {
                        it = map.erase(it);
                    }
                }
                return Status::OK();
            });

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
                                                              std::weak_ptr<QueryContext>>& map)
                                             -> Status {
                    for (const auto& it : map) {
                        if (auto q_ctx = it.second.lock()) {
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
                                if (itr->second.find(it.first) == itr->second.end() &&
                                    // tv_nsec represents the number of nanoseconds that have elapsed since the time point stored in tv_sec.
                                    // tv_sec is enough, we do not need to check tv_nsec.
                                    q_ctx->get_query_arrival_timestamp().tv_sec <
                                            check_invalid_query_last_timestamp.tv_sec &&
                                    q_ctx->get_query_source() == QuerySource::INTERNAL_FRONTEND) {
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

                            auto itr = running_fes.find(q_ctx->coord_addr);
                            if (itr != running_fes.end()) {
                                if (fe_process_uuid == itr->second.info.process_uuid ||
                                    itr->second.info.process_uuid == 0) {
                                    continue;
                                } else {
                                    LOG_WARNING(
                                            "Coordinator of query {} restarted, going to cancel "
                                            "it.",
                                            print_id(q_ctx->query_id()));
                                }
                            } else {
                                // In some rear cases, the rpc port of follower is not updated in time,
                                // then the port of this follower will be zero, but acutally it is still running,
                                // and be has already received the query from follower.
                                // So we need to check if host is in running_fes.
                                bool fe_host_is_standing =
                                        std::any_of(running_fes.begin(), running_fes.end(),
                                                    [&q_ctx](const auto& fe) {
                                                        return fe.first.hostname ==
                                                                       q_ctx->coord_addr.hostname &&
                                                               fe.first.port == 0;
                                                    });
                                if (fe_host_is_standing) {
                                    LOG_WARNING(
                                            "Coordinator {}:{} is not found, but its host is still "
                                            "running with an unstable brpc port, not going to "
                                            "cancel "
                                            "it.",
                                            q_ctx->coord_addr.hostname, q_ctx->coord_addr.port,
                                            print_id(q_ctx->query_id()));
                                    continue;
                                } else {
                                    LOG_WARNING(
                                            "Could not find target coordinator {}:{} of query {}, "
                                            "going to "
                                            "cancel it.",
                                            q_ctx->coord_addr.hostname, q_ctx->coord_addr.port,
                                            print_id(q_ctx->query_id()));
                                }
                            }
                        }
                        // Coordinator of this query has already dead or query context has been released.
                        queries_lost_coordinator.push_back(it.first);
                    }
                    return Status::OK();
                });
            }
        }

        if (config::enable_brpc_connection_check) {
            for (auto it : brpc_stub_with_queries) {
                if (!it.first) {
                    LOG(WARNING) << "brpc stub is nullptr, skip it.";
                    continue;
                }
                _check_brpc_available(it.first, it.second);
            }
        }

        if (!queries_lost_coordinator.empty()) {
            LOG(INFO) << "There are " << queries_lost_coordinator.size()
                      << " queries need to be cancelled, coordinator dead or restarted.";
        }

        for (const auto& qid : queries_timeout) {
            cancel_query(qid,
                         Status::Error<ErrorCode::TIMEOUT>(
                                 "FragmentMgr cancel worker going to cancel timeout instance "));
        }

        for (const auto& qid : queries_pipeline_task_leak) {
            // Cancel the query, and maybe try to report debug info to fe so that we can
            // collect debug info by sql or http api instead of search log.
            cancel_query(qid, Status::Error<ErrorCode::ILLEGAL_STATE>(
                                      "Potential pipeline task leakage"));
        }

        for (const auto& qid : queries_lost_coordinator) {
            cancel_query(qid, Status::Error<ErrorCode::CANCELLED>(
                                      "Source frontend is not running or restarted"));
        }

    } while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::fragment_mgr_cancel_worker_interval_seconds)));
    LOG(INFO) << "FragmentMgr cancel worker is going to exit.";
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
                    query->cancel(Status::InternalError("brpc(dest: {}:{}) check failed: {}",
                                                        brpc_item.network_address.hostname,
                                                        brpc_item.network_address.port,
                                                        error_message));
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

void FragmentMgr::debug(std::stringstream& ss) {}
/*
 * 1. resolve opaqued_query_plan to thrift structure
 * 2. build TExecPlanFragmentParams
 */
Status FragmentMgr::exec_external_plan_fragment(const TScanOpenParams& params,
                                                const TQueryPlanInfo& t_query_plan_info,
                                                const TUniqueId& query_id,
                                                const TUniqueId& fragment_instance_id,
                                                std::vector<TScanColumnDesc>* selected_columns) {
    // set up desc tbl
    DescriptorTbl* desc_tbl = nullptr;
    ObjectPool obj_pool;
    Status st = DescriptorTbl::create(&obj_pool, t_query_plan_info.desc_tbl, &desc_tbl);
    if (!st.ok()) {
        LOG(WARNING) << "open context error: extract DescriptorTbl failure";
        std::stringstream msg;
        msg << " create DescriptorTbl error, should not be modified after returned Doris FE "
               "processed";
        return Status::InvalidArgument(msg.str());
    }
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    if (tuple_desc == nullptr) {
        LOG(WARNING) << "open context error: extract TupleDescriptor failure";
        std::stringstream msg;
        msg << " get  TupleDescriptor error, should not be modified after returned Doris FE "
               "processed";
        return Status::InvalidArgument(msg.str());
    }
    // process selected columns form slots
    for (const SlotDescriptor* slot : tuple_desc->slots()) {
        TScanColumnDesc col;
        col.__set_name(slot->col_name());
        col.__set_type(to_thrift(slot->type()->get_primitive_type()));
        selected_columns->emplace_back(std::move(col));
    }

    VLOG_QUERY << "BackendService execute open()  TQueryPlanInfo: "
               << apache::thrift::ThriftDebugString(t_query_plan_info);
    // assign the param used to execute PlanFragment
    TPipelineFragmentParams exec_fragment_params;
    exec_fragment_params.protocol_version = (PaloInternalServiceVersion::type)0;
    exec_fragment_params.__set_is_simplified_param(false);
    exec_fragment_params.__set_fragment(t_query_plan_info.plan_fragment);
    exec_fragment_params.__set_desc_tbl(t_query_plan_info.desc_tbl);

    // assign the param used for executing of PlanFragment-self
    TPipelineInstanceParams fragment_exec_params;
    exec_fragment_params.query_id = query_id;
    fragment_exec_params.fragment_instance_id = fragment_instance_id;
    exec_fragment_params.coord.hostname = "external";
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
    exec_fragment_params.local_params.push_back(fragment_exec_params);
    TQueryOptions query_options;
    query_options.batch_size = params.batch_size;
    query_options.execution_timeout = params.execution_timeout;
    query_options.mem_limit = params.mem_limit;
    query_options.query_type = TQueryType::EXTERNAL;
    query_options.be_exec_version = BeExecVersionManager::get_newest_version();
    exec_fragment_params.__set_query_options(query_options);
    VLOG_ROW << "external exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(exec_fragment_params).c_str();

    TPipelineFragmentParamsList mocked;
    return exec_plan_fragment(exec_fragment_params, QuerySource::EXTERNAL_CONNECTOR, mocked);
}

Status FragmentMgr::apply_filterv2(const PPublishFilterRequestV2* request,
                                   butil::IOBufAsZeroCopyInputStream* attach_data) {
    UniqueId queryid = request->query_id();
    TUniqueId query_id;
    query_id.__set_hi(queryid.hi);
    query_id.__set_lo(queryid.lo);
    if (auto q_ctx = get_query_ctx(query_id)) {
        SCOPED_ATTACH_TASK(q_ctx.get());
        RuntimeFilterMgr* runtime_filter_mgr = q_ctx->runtime_filter_mgr();
        DCHECK(runtime_filter_mgr != nullptr);

        // 1. get the target filters
        std::vector<std::shared_ptr<RuntimeFilterConsumer>> filters =
                runtime_filter_mgr->get_consume_filters(request->filter_id());

        // 2. create the filter wrapper to replace or ignore/disable the target filters
        if (!filters.empty()) {
            RETURN_IF_ERROR(filters[0]->assign(*request, attach_data));
            std::ranges::for_each(filters, [&](auto& filter) { filter->signal(filters[0].get()); });
        }
    }
    return Status::OK();
}

Status FragmentMgr::send_filter_size(const PSendFilterSizeRequest* request) {
    UniqueId queryid = request->query_id();
    TUniqueId query_id;
    query_id.__set_hi(queryid.hi);
    query_id.__set_lo(queryid.lo);

    if (config::enable_debug_points &&
        DebugPoints::instance()->is_enable("FragmentMgr::send_filter_size.return_eof")) {
        return Status::EndOfFile("inject FragmentMgr::send_filter_size.return_eof");
    }

    if (auto q_ctx = get_query_ctx(query_id)) {
        return q_ctx->get_merge_controller_handler()->send_filter_size(q_ctx, request);
    } else {
        return Status::EndOfFile(
                "Send filter size failed: Query context (query-id: {}) not found, maybe "
                "finished",
                queryid.to_string());
    }
}

Status FragmentMgr::sync_filter_size(const PSyncFilterSizeRequest* request) {
    UniqueId queryid = request->query_id();
    TUniqueId query_id;
    query_id.__set_hi(queryid.hi);
    query_id.__set_lo(queryid.lo);
    if (auto q_ctx = get_query_ctx(query_id)) {
        try {
            return q_ctx->runtime_filter_mgr()->sync_filter_size(request);
        } catch (const Exception& e) {
            return Status::InternalError(
                    "Sync filter size failed: Query context (query-id: {}) error: {}",
                    queryid.to_string(), e.what());
        }
    } else {
        return Status::EndOfFile(
                "Sync filter size failed: Query context (query-id: {}) already finished",
                queryid.to_string());
    }
}

Status FragmentMgr::merge_filter(const PMergeFilterRequest* request,
                                 butil::IOBufAsZeroCopyInputStream* attach_data) {
    UniqueId queryid = request->query_id();

    TUniqueId query_id;
    query_id.__set_hi(queryid.hi);
    query_id.__set_lo(queryid.lo);
    if (auto q_ctx = get_query_ctx(query_id)) {
        SCOPED_ATTACH_TASK(q_ctx.get());
        if (!q_ctx->get_merge_controller_handler()) {
            return Status::InternalError("Merge filter failed: Merge controller handler is null");
        }
        return q_ctx->get_merge_controller_handler()->merge(q_ctx, request, attach_data);
    } else {
        return Status::EndOfFile(
                "Merge filter size failed: Query context (query-id: {}) already finished",
                queryid.to_string());
    }
}

void FragmentMgr::get_runtime_query_info(
        std::vector<std::weak_ptr<ResourceContext>>* _resource_ctx_list) {
    _query_ctx_map.apply(
            [&](phmap::flat_hash_map<TUniqueId, std::weak_ptr<QueryContext>>& map) -> Status {
                for (auto iter = map.begin(); iter != map.end();) {
                    if (auto q_ctx = iter->second.lock()) {
                        _resource_ctx_list->push_back(q_ctx->resource_ctx());
                        iter++;
                    } else {
                        iter = map.erase(iter);
                    }
                }
                return Status::OK();
            });
}

Status FragmentMgr::get_realtime_exec_status(const TUniqueId& query_id,
                                             TReportExecStatusParams* exec_status) {
    if (exec_status == nullptr) {
        return Status::InvalidArgument("exes_status is nullptr");
    }

    std::shared_ptr<QueryContext> query_context = get_query_ctx(query_id);
    if (query_context == nullptr) {
        return Status::NotFound("Query {} not found or released", print_id(query_id));
    }

    *exec_status = query_context->get_realtime_exec_status();

    return Status::OK();
}

Status FragmentMgr::get_query_statistics(const TUniqueId& query_id, TQueryStatistics* query_stats) {
    if (query_stats == nullptr) {
        return Status::InvalidArgument("query_stats is nullptr");
    }

    return ExecEnv::GetInstance()->runtime_query_statistics_mgr()->get_query_statistics(
            print_id(query_id), query_stats);
}

#include "common/compile_check_end.h"

} // namespace doris
