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
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <pthread.h>
#include <stddef.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TTransportException.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>

#include "common/status.h"
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "cloud/config.h"
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

FragmentMgr::FragmentMgr(ExecEnv* exec_env)
        : _exec_env(exec_env), _stop_background_threads_latch(1) {
    _entity = DorisMetrics::instance()->metric_registry()->register_entity("FragmentMgr");
    INT_UGAUGE_METRIC_REGISTER(_entity, timeout_canceled_fragment_count);

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
    {
        std::lock_guard<std::mutex> lock(_lock);
        _query_ctx_map.clear();
        _pipeline_map.clear();
    }
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
    Status exec_status = req.status;
    Status coord_status;
    FrontendServiceConnection coord(_exec_env->frontend_client_cache(), req.coord_addr,
                                    &coord_status);
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
            for (auto pipeline_profile : req.runtime_state->pipeline_id_to_profile()) {
                TDetailedReportParams detailed_param;
                detailed_param.__isset.fragment_instance_id = false;
                detailed_param.__isset.profile = true;
                detailed_param.__isset.loadChannelProfile = false;
                pipeline_profile->to_thrift(&detailed_param.profile);
                params.detailed_report.push_back(std::move(detailed_param));
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
        int64_t num_finished_ranges = 0;
        if (req.runtime_state->num_rows_load_total() > 0 ||
            req.runtime_state->num_rows_load_filtered() > 0 ||
            req.runtime_state->num_finished_range() > 0) {
            params.__isset.load_counters = true;

            num_rows_load_success = req.runtime_state->num_rows_load_success();
            num_rows_load_filtered = req.runtime_state->num_rows_load_filtered();
            num_rows_load_unselected = req.runtime_state->num_rows_load_unselected();
            num_finished_ranges = req.runtime_state->num_finished_range();
        } else if (!req.runtime_states.empty()) {
            for (auto* rs : req.runtime_states) {
                if (rs->num_rows_load_total() > 0 || rs->num_rows_load_filtered() > 0 ||
                    req.runtime_state->num_finished_range() > 0) {
                    params.__isset.load_counters = true;
                    num_rows_load_success += rs->num_rows_load_success();
                    num_rows_load_filtered += rs->num_rows_load_filtered();
                    num_rows_load_unselected += rs->num_rows_load_unselected();
                    num_finished_ranges += rs->num_finished_range();
                }
            }
        }
        params.__set_finished_scan_ranges(num_finished_ranges);
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
                if (rs->wal_id() > 0) {
                    params.__set_txn_id(rs->wal_id());
                    params.__set_label(rs->import_label());
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
                req.cancel_fn(rpc_status);
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
        req.cancel_fn(rpc_status);
    }
}

static void empty_function(RuntimeState*, Status*) {}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params) {
    return Status::InternalError("Non-pipeline is disabled!");
}

Status FragmentMgr::exec_plan_fragment(const TPipelineFragmentParams& params) {
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

        RETURN_IF_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(stream_load_ctx));
        return Status::OK();
    } else {
        return exec_plan_fragment(params, empty_function);
    }
}

Status FragmentMgr::start_query_execution(const PExecPlanFragmentStartRequest* request) {
    std::lock_guard<std::mutex> lock(_lock);
    TUniqueId query_id;
    query_id.__set_hi(request->query_id().hi());
    query_id.__set_lo(request->query_id().lo());
    if (auto q_ctx = _get_or_erase_query_ctx(query_id)) {
        q_ctx->set_ready_to_execute(Status::OK());
    } else {
        return Status::InternalError(
                "Failed to get query fragments context. Query may be "
                "timeout or be cancelled. host: {}",
                BackendOptions::get_localhost());
    }
    return Status::OK();
}

void FragmentMgr::remove_pipeline_context(
        std::shared_ptr<pipeline::PipelineFragmentContext> f_context) {
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto query_id = f_context->get_query_id();
        std::vector<TUniqueId> ins_ids;
        f_context->instance_ids(ins_ids);
        int64 now = duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
        g_fragment_executing_count << -1;
        g_fragment_last_active_time.set_value(now);
        for (const auto& ins_id : ins_ids) {
            LOG_INFO("Removing query {} instance {}", print_id(query_id), print_id(ins_id));
            _pipeline_map.erase(ins_id);
            g_pipeline_fragment_instances_count << -1;
        }
    }
}

std::shared_ptr<QueryContext> FragmentMgr::_get_or_erase_query_ctx(const TUniqueId& query_id) {
    auto search = _query_ctx_map.find(query_id);
    if (search != _query_ctx_map.end()) {
        if (auto q_ctx = search->second.lock()) {
            return q_ctx;
        } else {
            LOG(WARNING) << "Query context (query id = " << print_id(query_id)
                         << ") has been released.";
            _query_ctx_map.erase(search);
            return nullptr;
        }
    }
    return nullptr;
}

std::shared_ptr<QueryContext> FragmentMgr::get_or_erase_query_ctx_with_lock(
        const TUniqueId& query_id) {
    std::unique_lock<std::mutex> lock(_lock);
    return _get_or_erase_query_ctx(query_id);
}

template <typename Params>
Status FragmentMgr::_get_query_ctx(const Params& params, TUniqueId query_id, bool pipeline,
                                   std::shared_ptr<QueryContext>& query_ctx) {
    if (params.is_simplified_param) {
        // Get common components from _query_ctx_map
        std::lock_guard<std::mutex> lock(_lock);
        if (auto q_ctx = _get_or_erase_query_ctx(query_id)) {
            query_ctx = q_ctx;
        } else {
            return Status::InternalError(
                    "Failed to get query fragments context. Query may be "
                    "timeout or be cancelled. host: {}",
                    BackendOptions::get_localhost());
        }
    } else {
        // Find _query_ctx_map, in case some other request has already
        // create the query fragments context.
        std::lock_guard<std::mutex> lock(_lock);
        if (auto q_ctx = _get_or_erase_query_ctx(query_id)) {
            query_ctx = q_ctx;
            return Status::OK();
        }

        LOG(INFO) << "query_id: " << print_id(query_id) << ", coord_addr: " << params.coord
                  << ", total fragment num on current host: " << params.fragment_num_on_host
                  << ", fe process uuid: " << params.query_options.fe_process_uuid
                  << ", query type: " << params.query_options.query_type
                  << ", report audit fe:" << params.current_connect_fe;

        // This may be a first fragment request of the query.
        // Create the query fragments context.
        query_ctx =
                QueryContext::create_shared(query_id, _exec_env, params.query_options, params.coord,
                                            pipeline, params.is_nereids, params.current_connect_fe);
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

        _set_scan_concurrency(params, query_ctx.get());
        const bool is_pipeline = std::is_same_v<TPipelineFragmentParams, Params>;

        if (params.__isset.workload_groups && !params.workload_groups.empty()) {
            uint64_t tg_id = params.workload_groups[0].id;
            WorkloadGroupPtr workload_group_ptr =
                    _exec_env->workload_group_mgr()->get_task_group_by_id(tg_id);
            if (workload_group_ptr != nullptr) {
                RETURN_IF_ERROR(workload_group_ptr->add_query(query_id, query_ctx));
                RETURN_IF_ERROR(query_ctx->set_workload_group(workload_group_ptr));
                _exec_env->runtime_query_statistics_mgr()->set_workload_group_id(print_id(query_id),
                                                                                 tg_id);

                LOG(INFO) << "Query/load id: " << print_id(query_ctx->query_id())
                          << ", use workload group: " << workload_group_ptr->debug_string()
                          << ", is pipeline: " << ((int)is_pipeline);
            } else {
                LOG(INFO) << "Query/load id: " << print_id(query_ctx->query_id())
                          << " carried group info but can not find group in be";
            }
        }
        // There is some logic in query ctx's dctor, we could not check if exists and delete the
        // temp query ctx now. For example, the query id maybe removed from workload group's queryset.
        _query_ctx_map.insert(std::make_pair(query_ctx->query_id(), query_ctx));
        LOG(INFO) << "Register query/load memory tracker, query/load id: "
                  << print_id(query_ctx->query_id())
                  << " limit: " << PrettyPrinter::print(query_ctx->mem_limit(), TUnit::BYTES);
    }
    return Status::OK();
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params,
                                       const FinishCallback& cb) {
    return Status::InternalError("Non-pipeline is disabled!");
}

std::string FragmentMgr::dump_pipeline_tasks(int64_t duration) {
    fmt::memory_buffer debug_string_buffer;
    size_t i = 0;
    {
        std::lock_guard<std::mutex> lock(_lock);
        fmt::format_to(debug_string_buffer,
                       "{} pipeline fragment contexts are still running! duration_limit={}\n",
                       _pipeline_map.size(), duration);

        timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        for (auto& it : _pipeline_map) {
            auto elapsed = it.second->elapsed_time() / 1000000000.0;
            if (elapsed < duration) {
                // Only display tasks which has been running for more than {duration} seconds.
                continue;
            }
            auto timeout_second = it.second->timeout_second();
            fmt::format_to(debug_string_buffer,
                           "No.{} (elapse_second={}s, query_timeout_second={}s, instance_id="
                           "{}, is_timeout={}) : {}\n",
                           i, elapsed, timeout_second, print_id(it.first),
                           it.second->is_timeout(now), it.second->debug_string());
            i++;
        }
    }
    return fmt::to_string(debug_string_buffer);
}

std::string FragmentMgr::dump_pipeline_tasks(TUniqueId& query_id) {
    if (auto q_ctx = _get_or_erase_query_ctx(query_id)) {
        return q_ctx->print_all_pipeline_context();
    } else {
        return fmt::format("Query context (query id = {}) not found. \n", print_id(query_id));
    }
}

Status FragmentMgr::exec_plan_fragment(const TPipelineFragmentParams& params,
                                       const FinishCallback& cb) {
    VLOG_ROW << "query: " << print_id(params.query_id) << " exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(params).c_str();
    // sometimes TExecPlanFragmentParams debug string is too long and glog
    // will truncate the log line, so print query options seperately for debuggin purpose
    VLOG_ROW << "query: " << print_id(params.query_id) << "query options is "
             << apache::thrift::ThriftDebugString(params.query_options).c_str();

    std::shared_ptr<QueryContext> query_ctx;
    RETURN_IF_ERROR(_get_query_ctx(params, params.query_id, true, query_ctx));
    SCOPED_ATTACH_TASK(query_ctx.get());
    int64_t duration_ns = 0;
    std::shared_ptr<pipeline::PipelineFragmentContext> context =
            std::make_shared<pipeline::PipelineFragmentContext>(
                    query_ctx->query_id(), params.fragment_id, query_ctx, _exec_env, cb,
                    std::bind<Status>(std::mem_fn(&FragmentMgr::trigger_pipeline_context_report),
                                      this, std::placeholders::_1, std::placeholders::_2));
    {
        SCOPED_RAW_TIMER(&duration_ns);
        Status prepare_st = Status::OK();
        ASSIGN_STATUS_IF_CATCH_EXCEPTION(prepare_st = context->prepare(params), prepare_st);
        if (!prepare_st.ok()) {
            query_ctx->cancel(prepare_st, params.fragment_id);
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
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _pipeline_map.find(fragment_instance_id);
        if (iter != _pipeline_map.end()) {
            return Status::InternalError(
                    "exec_plan_fragment input duplicated fragment_instance_id({})",
                    UniqueId(fragment_instance_id).to_string());
        }
        query_ctx->fragment_instance_ids.push_back(fragment_instance_id);
    }

    if (!params.__isset.need_wait_execution_trigger || !params.need_wait_execution_trigger) {
        query_ctx->set_ready_to_execute_only();
    }

    int64 now = duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
    {
        g_fragment_executing_count << 1;
        g_fragment_last_active_time.set_value(now);
        std::lock_guard<std::mutex> lock(_lock);
        std::vector<TUniqueId> ins_ids;
        context->instance_ids(ins_ids);
        // TODO: simplify this mapping
        for (const auto& ins_id : ins_ids) {
            _pipeline_map.insert({ins_id, context});
        }
    }
    query_ctx->set_pipeline_context(params.fragment_id, context);

    RETURN_IF_ERROR(context->submit());
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

void FragmentMgr::cancel_query(const TUniqueId query_id, const Status reason) {
    std::shared_ptr<QueryContext> query_ctx = nullptr;
    std::vector<TUniqueId> all_instance_ids;
    {
        std::lock_guard<std::mutex> state_lock(_lock);
        if (auto q_ctx = _get_or_erase_query_ctx(query_id)) {
            query_ctx = q_ctx;
            // Copy instanceids to avoid concurrent modification.
            // And to reduce the scope of lock.
            all_instance_ids = query_ctx->fragment_instance_ids;
        } else {
            LOG(WARNING) << "Query " << print_id(query_id)
                         << " does not exists, failed to cancel it";
            return;
        }
    }
    query_ctx->cancel(reason);
    {
        std::lock_guard<std::mutex> state_lock(_lock);
        _query_ctx_map.erase(query_id);
    }
    LOG(INFO) << "Query " << print_id(query_id)
              << " is cancelled and removed. Reason: " << reason.to_string();
}

void FragmentMgr::cancel_instance(const TUniqueId instance_id, const Status reason) {
    std::shared_ptr<pipeline::PipelineFragmentContext> pipeline_ctx;
    {
        std::lock_guard<std::mutex> state_lock(_lock);
        DCHECK(!_pipeline_map.contains(instance_id))
                << " Pipeline tasks should be canceled by query instead of instance! Query ID: "
                << print_id(_pipeline_map[instance_id]->get_query_id());
        const bool is_pipeline_instance = _pipeline_map.contains(instance_id);
        if (is_pipeline_instance) {
            auto itr = _pipeline_map.find(instance_id);
            if (itr != _pipeline_map.end()) {
                pipeline_ctx = itr->second;
            } else {
                LOG(WARNING) << "Could not find the pipeline instance id:" << print_id(instance_id)
                             << " to cancel";
                return;
            }
        }
    }

    if (pipeline_ctx != nullptr) {
        pipeline_ctx->cancel(reason);
    }
}

void FragmentMgr::cancel_worker() {
    LOG(INFO) << "FragmentMgr cancel worker start working.";
    do {
        std::vector<TUniqueId> queries_lost_coordinator;
        std::vector<TUniqueId> queries_timeout;

        timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        {
            std::lock_guard<std::mutex> lock(_lock);
            for (auto& pipeline_itr : _pipeline_map) {
                pipeline_itr.second->clear_finished_tasks();
            }
            for (auto it = _query_ctx_map.begin(); it != _query_ctx_map.end();) {
                if (auto q_ctx = it->second.lock()) {
                    if (q_ctx->is_timeout(now)) {
                        LOG_WARNING("Query {} is timeout", print_id(it->first));
                        queries_timeout.push_back(it->first);
                        ++it;
                    } else {
                        ++it;
                    }
                } else {
                    it = _query_ctx_map.erase(it);
                }
            }

            const auto& running_fes = ExecEnv::GetInstance()->get_running_frontends();

            // We use a very conservative cancel strategy.
            // 0. If there are no running frontends, do not cancel any queries.
            // 1. If query's process uuid is zero, do not cancel
            // 2. If same process uuid, do not cancel
            // 3. If fe has zero process uuid, do not cancel
            if (running_fes.empty() && !_query_ctx_map.empty()) {
                LOG_EVERY_N(WARNING, 10)
                        << "Could not find any running frontends, maybe we are upgrading or "
                           "starting? "
                        << "We will not cancel any outdated queries in this situation.";
            } else {
                for (const auto& it : _query_ctx_map) {
                    if (auto q_ctx = it.second.lock()) {
                        if (q_ctx->get_fe_process_uuid() == 0) {
                            // zero means this query is from a older version fe or
                            // this fe is starting
                            continue;
                        }

                        auto itr = running_fes.find(q_ctx->coord_addr);
                        if (itr != running_fes.end()) {
                            if (q_ctx->get_fe_process_uuid() == itr->second.info.process_uuid ||
                                itr->second.info.process_uuid == 0) {
                                continue;
                            } else {
                                LOG_WARNING(
                                        "Coordinator of query {} restarted, going to cancel it.",
                                        print_id(q_ctx->query_id()));
                            }
                        } else {
                            // In some rear cases, the rpc port of follower is not updated in time,
                            // then the port of this follower will be zero, but acutally it is still running,
                            // and be has already received the query from follower.
                            // So we need to check if host is in running_fes.
                            bool fe_host_is_standing = std::any_of(
                                    running_fes.begin(), running_fes.end(),
                                    [&q_ctx](const auto& fe) {
                                        return fe.first.hostname == q_ctx->coord_addr.hostname &&
                                               fe.first.port == 0;
                                    });
                            if (fe_host_is_standing) {
                                LOG_WARNING(
                                        "Coordinator {}:{} is not found, but its host is still "
                                        "running with an unstable brpc port, not going to cancel "
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

        for (const auto& qid : queries_lost_coordinator) {
            cancel_query(qid, Status::InternalError("Coordinator dead."));
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(1)));
    LOG(INFO) << "FragmentMgr cancel worker is going to exit.";
}

void FragmentMgr::debug(std::stringstream& ss) {}
/*
 * 1. resolve opaqued_query_plan to thrift structure
 * 2. build TExecPlanFragmentParams
 */
Status FragmentMgr::exec_external_plan_fragment(const TScanOpenParams& params,
                                                const TQueryPlanInfo& t_query_plan_info,
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
        col.__set_type(to_thrift(slot->type().type));
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
    exec_fragment_params.query_id = t_query_plan_info.query_id;
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
    return exec_plan_fragment(exec_fragment_params);
}

Status FragmentMgr::apply_filterv2(const PPublishFilterRequestV2* request,
                                   butil::IOBufAsZeroCopyInputStream* attach_data) {
    bool is_pipeline = request->has_is_pipeline() && request->is_pipeline();
    int64_t start_apply = MonotonicMillis();

    std::shared_ptr<pipeline::PipelineFragmentContext> pip_context;
    QueryThreadContext query_thread_context;

    RuntimeFilterMgr* runtime_filter_mgr = nullptr;

    const auto& fragment_instance_ids = request->fragment_instance_ids();
    {
        std::unique_lock<std::mutex> lock(_lock);
        for (UniqueId fragment_instance_id : fragment_instance_ids) {
            TUniqueId tfragment_instance_id = fragment_instance_id.to_thrift();

            if (is_pipeline) {
                auto iter = _pipeline_map.find(tfragment_instance_id);
                if (iter == _pipeline_map.end()) {
                    continue;
                }
                pip_context = iter->second;

                DCHECK(pip_context != nullptr);
                runtime_filter_mgr = pip_context->get_query_ctx()->runtime_filter_mgr();
                query_thread_context = {pip_context->get_query_ctx()->query_id(),
                                        pip_context->get_query_ctx()->query_mem_tracker,
                                        pip_context->get_query_ctx()->workload_group()};
            } else {
                return Status::InternalError("Non-pipeline is disabled!");
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
    std::vector<std::shared_ptr<IRuntimeFilter>> filters;
    RETURN_IF_ERROR(runtime_filter_mgr->get_consume_filters(request->filter_id(), filters));

    // 2. create the filter wrapper to replace or ignore the target filters
    if (!filters.empty()) {
        UpdateRuntimeFilterParamsV2 params {request, attach_data, filters[0]->column_type()};
        std::shared_ptr<RuntimePredicateWrapper> filter_wrapper;
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
        std::lock_guard<std::mutex> lock(_lock);
        if (auto q_ctx = _get_or_erase_query_ctx(query_id)) {
            query_ctx = q_ctx;
        } else {
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
        std::lock_guard<std::mutex> lock(_lock);
        if (auto q_ctx = _get_or_erase_query_ctx(query_id)) {
            query_ctx = q_ctx;
        } else {
            return Status::InvalidArgument("Query context (query-id: {}) not found",
                                           queryid.to_string());
        }
    }
    return query_ctx->runtime_filter_mgr()->sync_filter_size(request);
}

Status FragmentMgr::merge_filter(const PMergeFilterRequest* request,
                                 butil::IOBufAsZeroCopyInputStream* attach_data) {
    UniqueId queryid = request->query_id();
    std::shared_ptr<RuntimeFilterMergeControllerEntity> filter_controller;
    RETURN_IF_ERROR(_runtimefilter_controller.acquire(queryid, &filter_controller));

    std::shared_ptr<QueryContext> query_ctx;
    {
        TUniqueId query_id;
        query_id.__set_hi(queryid.hi);
        query_id.__set_lo(queryid.lo);
        std::lock_guard<std::mutex> lock(_lock);
        if (auto q_ctx = _get_or_erase_query_ctx(query_id)) {
            query_ctx = q_ctx;
        } else {
            return Status::InvalidArgument("Query context (query-id: {}) not found",
                                           queryid.to_string());
        }
    }
    SCOPED_ATTACH_TASK(query_ctx.get());
    auto merge_status = filter_controller->merge(request, attach_data);
    return merge_status;
}

void FragmentMgr::get_runtime_query_info(std::vector<WorkloadQueryInfo>* query_info_list) {
    {
        std::lock_guard<std::mutex> lock(_lock);
        for (auto iter = _query_ctx_map.begin(); iter != _query_ctx_map.end();) {
            if (auto q_ctx = iter->second.lock()) {
                WorkloadQueryInfo workload_query_info;
                workload_query_info.query_id = print_id(iter->first);
                workload_query_info.tquery_id = iter->first;
                workload_query_info.wg_id =
                        q_ctx->workload_group() == nullptr ? -1 : q_ctx->workload_group()->id();
                query_info_list->push_back(workload_query_info);
                iter++;
            } else {
                iter = _query_ctx_map.erase(iter);
            }
        }
    }
}

Status FragmentMgr::get_realtime_exec_status(const TUniqueId& query_id,
                                             TReportExecStatusParams* exec_status) {
    if (exec_status == nullptr) {
        return Status::InvalidArgument("exes_status is nullptr");
    }

    std::shared_ptr<QueryContext> query_context = nullptr;

    {
        std::lock_guard<std::mutex> lock(_lock);
        if (auto q_ctx = _get_or_erase_query_ctx(query_id)) {
            query_context = q_ctx;
        } else {
            return Status::NotFound("Query {} has been released", print_id(query_id));
        }
    }

    if (query_context == nullptr) {
        return Status::NotFound("Query {} not found", print_id(query_id));
    }

    *exec_status = query_context->get_realtime_exec_status();

    return Status::OK();
}

} // namespace doris
