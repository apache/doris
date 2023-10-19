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
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/tracer.h>
#include <pthread.h>
#include <stddef.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TTransportException.h>

#include <atomic>

#include "common/status.h"
#include "pipeline/pipeline_x/pipeline_x_fragment_context.h"
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/utils.h"
#include "gutil/strings/substitute.h"
#include "io/fs/stream_load_pipe.h"
#include "opentelemetry/trace/scope.h"
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
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/task_group/task_group.h"
#include "runtime/task_group/task_group_manager.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "service/backend_options.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/hash_util.hpp"
#include "util/mem_info.h"
#include "util/network_util.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "util/telemetry/telemetry.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
#include "util/url_coding.h"
#include "vec/runtime/shared_hash_table_controller.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(plan_fragment_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(timeout_canceled_fragment_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(fragment_thread_pool_queue_size, MetricUnit::NOUNIT);
bvar::LatencyRecorder g_fragmentmgr_prepare_latency("doris_FragmentMgr", "prepare");

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

FragmentMgr::FragmentMgr(ExecEnv* exec_env)
        : _exec_env(exec_env), _stop_background_threads_latch(1) {
    _entity = DorisMetrics::instance()->metric_registry()->register_entity("FragmentMgr");
    INT_UGAUGE_METRIC_REGISTER(_entity, timeout_canceled_fragment_count);
    REGISTER_HOOK_METRIC(plan_fragment_count, [this]() { return _fragment_map.size(); });

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
    DEREGISTER_HOOK_METRIC(plan_fragment_count);
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
        _fragment_map.clear();
        _query_ctx_map.clear();
        for (auto& pipeline : _pipeline_map) {
            pipeline.second->close_sink();
        }
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
    exec_status.set_t_status(&params);
    params.__set_done(req.done);
    params.__set_query_type(req.runtime_state->query_type());
    params.__set_finished_scan_ranges(req.runtime_state->num_finished_range());

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
            for (auto* rs : req.runtime_states) {
                TDetailedReportParams detailed_param;
                detailed_param.__set_fragment_instance_id(rs->fragment_instance_id());
                detailed_param.__isset.fragment_instance_id = true;

                if (rs->enable_profile()) {
                    detailed_param.__isset.profile = true;
                    detailed_param.__isset.loadChannelProfile = true;

                    rs->runtime_profile()->to_thrift(&detailed_param.profile);
                    rs->load_channel_profile()->to_thrift(&detailed_param.loadChannelProfile);
                }

                params.detailed_report.push_back(detailed_param);
            }
        }

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

        if (!req.runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : req.runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(it));
            }
        } else if (!req.runtime_states.empty()) {
            params.__isset.delta_urls = true;
            for (auto* rs : req.runtime_states) {
                for (auto& it : rs->output_files()) {
                    params.delta_urls.push_back(to_http_path(it));
                }
            }
        }
        if (req.runtime_state->num_rows_load_total() > 0 ||
            req.runtime_state->num_rows_load_filtered() > 0) {
            params.__isset.load_counters = true;

            static std::string s_dpp_normal_all = "dpp.norm.ALL";
            static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";
            static std::string s_unselected_rows = "unselected.rows";

            params.load_counters.emplace(
                    s_dpp_normal_all, std::to_string(req.runtime_state->num_rows_load_success()));
            params.load_counters.emplace(
                    s_dpp_abnormal_all,
                    std::to_string(req.runtime_state->num_rows_load_filtered()));
            params.load_counters.emplace(
                    s_unselected_rows,
                    std::to_string(req.runtime_state->num_rows_load_unselected()));
        } else if (!req.runtime_states.empty()) {
            static std::string s_dpp_normal_all = "dpp.norm.ALL";
            static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";
            static std::string s_unselected_rows = "unselected.rows";
            int64_t num_rows_load_success = 0;
            int64_t num_rows_load_filtered = 0;
            int64_t num_rows_load_unselected = 0;
            for (auto* rs : req.runtime_states) {
                if (rs->num_rows_load_total() > 0 || rs->num_rows_load_filtered() > 0) {
                    params.__isset.load_counters = true;
                    num_rows_load_success += rs->num_rows_load_success();
                    num_rows_load_filtered += rs->num_rows_load_filtered();
                    num_rows_load_unselected += rs->num_rows_load_unselected();
                }
            }
            params.load_counters.emplace(s_dpp_normal_all, std::to_string(num_rows_load_success));
            params.load_counters.emplace(s_dpp_abnormal_all,
                                         std::to_string(num_rows_load_filtered));
            params.load_counters.emplace(s_unselected_rows,
                                         std::to_string(num_rows_load_unselected));
        }
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
        LOG(WARNING) << "report error status: " << exec_status.to_string()
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
        // we need to cancel the execution of this fragment
        static_cast<void>(req.update_fn(rpc_status));
        req.cancel_fn(PPlanFragmentCancelReason::INTERNAL_ERROR, "rpc fail 2");
    }
}

static void empty_function(RuntimeState*, Status*) {}

void FragmentMgr::_exec_actual(std::shared_ptr<PlanFragmentExecutor> fragment_executor,
                               const FinishCallback& cb) {
    std::string func_name {"PlanFragmentExecutor::_exec_actual"};
#ifndef BE_TEST
    SCOPED_ATTACH_TASK(fragment_executor->runtime_state());
#endif

    LOG_INFO(func_name)
            .tag("query_id", fragment_executor->query_id())
            .tag("instance_id", fragment_executor->fragment_instance_id())
            .tag("pthread_id", (uintptr_t)pthread_self());

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
        std::lock_guard<std::mutex> lock(_lock);
        _fragment_map.erase(fragment_executor->fragment_instance_id());
        if (all_done && query_ctx) {
            _query_ctx_map.erase(query_ctx->query_id());
        }
    }

    // Callback after remove from this id
    auto status = fragment_executor->status();
    cb(fragment_executor->runtime_state(), &status);
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params) {
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
        return exec_plan_fragment(params, empty_function);
    }
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
        return exec_plan_fragment(params, empty_function);
    }
}

Status FragmentMgr::start_query_execution(const PExecPlanFragmentStartRequest* request) {
    std::lock_guard<std::mutex> lock(_lock);
    TUniqueId query_id;
    query_id.__set_hi(request->query_id().hi());
    query_id.__set_lo(request->query_id().lo());
    auto search = _query_ctx_map.find(query_id);
    if (search == _query_ctx_map.end()) {
        return Status::InternalError(
                "Failed to get query fragments context. Query may be "
                "timeout or be cancelled. host: {}",
                BackendOptions::get_localhost());
    }
    search->second->set_ready_to_execute(false);
    return Status::OK();
}

void FragmentMgr::remove_pipeline_context(
        std::shared_ptr<pipeline::PipelineFragmentContext> f_context) {
    std::lock_guard<std::mutex> lock(_lock);
    auto query_id = f_context->get_query_id();
    auto* q_context = f_context->get_query_context();
    std::vector<TUniqueId> ins_ids;
    f_context->instance_ids(ins_ids);
    bool all_done = q_context->countdown(ins_ids.size());
    for (const auto& ins_id : ins_ids) {
        VLOG_DEBUG << "remove pipeline context " << print_id(ins_id) << ", all_done:" << all_done;
        _pipeline_map.erase(ins_id);
    }
    if (all_done) {
        LOG(INFO) << "remove query context " << print_id(query_id);
        _query_ctx_map.erase(query_id);
    }
}

template <typename Params>
Status FragmentMgr::_get_query_ctx(const Params& params, TUniqueId query_id, bool pipeline,
                                   std::shared_ptr<QueryContext>& query_ctx) {
    if (params.is_simplified_param) {
        // Get common components from _query_ctx_map
        std::lock_guard<std::mutex> lock(_lock);
        auto search = _query_ctx_map.find(query_id);
        if (search == _query_ctx_map.end()) {
            return Status::InternalError(
                    "Failed to get query fragments context. Query may be "
                    "timeout or be cancelled. host: {}",
                    BackendOptions::get_localhost());
        }
        query_ctx = search->second;
    } else {
        {
            // Find _query_ctx_map, in case some other request has already
            // create the query fragments context.
            std::lock_guard<std::mutex> lock(_lock);
            auto search = _query_ctx_map.find(query_id);
            if (search != _query_ctx_map.end()) {
                query_ctx = search->second;
                return Status::OK();
            }
        }

        // This may be a first fragment request of the query.
        // Create the query fragments context.
        query_ctx = QueryContext::create_shared(query_id, params.fragment_num_on_host, _exec_env,
                                                params.query_options);
        RETURN_IF_ERROR(DescriptorTbl::create(&(query_ctx->obj_pool), params.desc_tbl,
                                              &(query_ctx->desc_tbl)));
        query_ctx->coord_addr = params.coord;
        // set file scan range params
        if (params.__isset.file_scan_params) {
            query_ctx->file_scan_range_params_map = params.file_scan_params;
        }

        LOG(INFO) << "query_id: " << UniqueId(query_ctx->query_id().hi, query_ctx->query_id().lo)
                  << " coord_addr " << query_ctx->coord_addr
                  << " total fragment num on current host: " << params.fragment_num_on_host
                  << " fe process uuid: " << params.query_options.fe_process_uuid;
        query_ctx->query_globals = params.query_globals;

        if (params.__isset.resource_info) {
            query_ctx->user = params.resource_info.user;
            query_ctx->group = params.resource_info.group;
            query_ctx->set_rsc_info = true;
        }

        query_ctx->get_shared_hash_table_controller()->set_pipeline_engine_enabled(pipeline);
        query_ctx->timeout_second = params.query_options.execution_timeout;
        _set_scan_concurrency(params, query_ctx.get());

        bool has_query_mem_tracker =
                params.query_options.__isset.mem_limit && (params.query_options.mem_limit > 0);
        int64_t bytes_limit = has_query_mem_tracker ? params.query_options.mem_limit : -1;
        if (bytes_limit > MemInfo::mem_limit()) {
            VLOG_NOTICE << "Query memory limit " << PrettyPrinter::print(bytes_limit, TUnit::BYTES)
                        << " exceeds process memory limit of "
                        << PrettyPrinter::print(MemInfo::mem_limit(), TUnit::BYTES)
                        << ". Using process memory limit instead";
            bytes_limit = MemInfo::mem_limit();
        }
        if (params.query_options.query_type == TQueryType::SELECT) {
            query_ctx->query_mem_tracker = std::make_shared<MemTrackerLimiter>(
                    MemTrackerLimiter::Type::QUERY,
                    fmt::format("Query#Id={}", print_id(query_ctx->query_id())), bytes_limit);
        } else if (params.query_options.query_type == TQueryType::LOAD) {
            query_ctx->query_mem_tracker = std::make_shared<MemTrackerLimiter>(
                    MemTrackerLimiter::Type::LOAD,
                    fmt::format("Load#Id={}", print_id(query_ctx->query_id())), bytes_limit);
        } else { // EXTERNAL
            query_ctx->query_mem_tracker = std::make_shared<MemTrackerLimiter>(
                    MemTrackerLimiter::Type::LOAD,
                    fmt::format("External#Id={}", print_id(query_ctx->query_id())), bytes_limit);
        }
        if (params.query_options.__isset.is_report_success &&
            params.query_options.is_report_success) {
            query_ctx->query_mem_tracker->enable_print_log_usage();
        }

        if constexpr (std::is_same_v<TPipelineFragmentParams, Params>) {
            if (params.__isset.workload_groups && !params.workload_groups.empty()) {
                taskgroup::TaskGroupInfo task_group_info;
                int query_cpu_hard_limit = -1;
                auto status = taskgroup::TaskGroupInfo::parse_group_info(
                        params.workload_groups[0], &task_group_info, &query_cpu_hard_limit);
                if (status.ok()) {
                    auto tg = _exec_env->task_group_manager()->get_or_create_task_group(
                            task_group_info);
                    tg->add_mem_tracker_limiter(query_ctx->query_mem_tracker);
                    query_ctx->set_task_group(tg);
                    LOG(INFO) << "Query/load id: " << print_id(query_ctx->query_id())
                              << " use task group: " << tg->debug_string()
                              << " query_cpu_hard_limit: " << query_cpu_hard_limit
                              << " cpu_share:" << task_group_info.cpu_share;
                    if (query_cpu_hard_limit > 0 && _exec_env->get_cgroup_cpu_ctl() != nullptr) {
                        _exec_env->get_cgroup_cpu_ctl()->update_cpu_hard_limit(
                                query_cpu_hard_limit);
                    }
                }
            } else {
                VLOG_DEBUG << "Query/load id: " << print_id(query_ctx->query_id())
                           << " does not use task group.";
            }
        }

        {
            // Find _query_ctx_map again, in case some other request has already
            // create the query fragments context.
            std::lock_guard<std::mutex> lock(_lock);
            auto search = _query_ctx_map.find(query_id);
            if (search == _query_ctx_map.end()) {
                _query_ctx_map.insert(std::make_pair(query_ctx->query_id(), query_ctx));
                LOG(INFO) << "Register query/load memory tracker, query/load id: "
                          << print_id(query_ctx->query_id())
                          << " limit: " << PrettyPrinter::print(bytes_limit, TUnit::BYTES);
            } else {
                // Already has a query fragments context, use it
                query_ctx = search->second;
            }
        }
    }
    return Status::OK();
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params,
                                       const FinishCallback& cb) {
    auto tracer = telemetry::is_current_span_valid() ? telemetry::get_tracer("tracer")
                                                     : telemetry::get_noop_tracer();
    auto cur_span = opentelemetry::trace::Tracer::GetCurrentSpan();
    cur_span->SetAttribute("query_id", print_id(params.params.query_id));
    cur_span->SetAttribute("instance_id", print_id(params.params.fragment_instance_id));

    VLOG_ROW << "exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(params).c_str();
    // sometimes TExecPlanFragmentParams debug string is too long and glog
    // will truncate the log line, so print query options seperately for debuggin purpose
    VLOG_ROW << "query options is "
             << apache::thrift::ThriftDebugString(params.query_options).c_str();
    const TUniqueId& fragment_instance_id = params.params.fragment_instance_id;
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(fragment_instance_id);
        if (iter != _fragment_map.end()) {
            // Duplicated
            LOG(WARNING) << "duplicate fragment instance id: " << print_id(fragment_instance_id);
            return Status::OK();
        }
    }

    std::shared_ptr<QueryContext> query_ctx;
    bool pipeline_engine_enabled = params.query_options.__isset.enable_pipeline_engine &&
                                   params.query_options.enable_pipeline_engine;
    RETURN_IF_ERROR(
            _get_query_ctx(params, params.params.query_id, pipeline_engine_enabled, query_ctx));
    {
        // Need lock here, because it will modify fragment ids and std::vector may resize and reallocate
        // memory, but query_is_canncelled will traverse the vector, it will core.
        // query_is_cancelled is called in allocator, we has to avoid dead lock.
        std::lock_guard<std::mutex> lock(_lock);
        query_ctx->fragment_ids.push_back(fragment_instance_id);
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
    static_cast<void>(_runtimefilter_controller.add_entity(params, &handler,
                                                           fragment_executor->runtime_state()));
    fragment_executor->set_merge_controller_handler(handler);
    {
        std::lock_guard<std::mutex> lock(_lock);
        _fragment_map.insert(std::make_pair(params.params.fragment_instance_id, fragment_executor));
        _cv.notify_all();
    }
    auto st = _thread_pool->submit_func(
            [this, fragment_executor, cb,
             parent_span = opentelemetry::trace::Tracer::GetCurrentSpan()] {
                OpentelemetryScope scope {parent_span};
                _exec_actual(fragment_executor, cb);
            });
    if (!st.ok()) {
        {
            // Remove the exec state added
            std::lock_guard<std::mutex> lock(_lock);
            _fragment_map.erase(params.params.fragment_instance_id);
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

Status FragmentMgr::exec_plan_fragment(const TPipelineFragmentParams& params,
                                       const FinishCallback& cb) {
    auto tracer = telemetry::is_current_span_valid() ? telemetry::get_tracer("tracer")
                                                     : telemetry::get_noop_tracer();
    auto cur_span = opentelemetry::trace::Tracer::GetCurrentSpan();
    cur_span->SetAttribute("query_id", print_id(params.query_id));

    VLOG_ROW << "exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(params).c_str();
    // sometimes TExecPlanFragmentParams debug string is too long and glog
    // will truncate the log line, so print query options seperately for debuggin purpose
    VLOG_ROW << "query options is "
             << apache::thrift::ThriftDebugString(params.query_options).c_str();

    std::shared_ptr<QueryContext> query_ctx;
    RETURN_IF_ERROR(_get_query_ctx(params, params.query_id, true, query_ctx));

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
                                std::placeholders::_1, std::placeholders::_2),
                        params.group_commit);
        {
            SCOPED_RAW_TIMER(&duration_ns);
            auto prepare_st = context->prepare(params);
            if (!prepare_st.ok()) {
                context->close_if_prepare_failed();
                return prepare_st;
            }
        }
        g_fragmentmgr_prepare_latency << (duration_ns / 1000);

        for (size_t i = 0; i < params.local_params.size(); i++) {
            std::shared_ptr<RuntimeFilterMergeControllerEntity> handler;
            static_cast<void>(
                    _runtimefilter_controller.add_entity(params, params.local_params[i], &handler,
                                                         context->get_runtime_state(UniqueId())));
            context->set_merge_controller_handler(handler);
            const TUniqueId& fragment_instance_id = params.local_params[i].fragment_instance_id;
            {
                std::lock_guard<std::mutex> lock(_lock);
                auto iter = _pipeline_map.find(fragment_instance_id);
                if (iter != _pipeline_map.end()) {
                    // Duplicated
                    return Status::OK();
                }
                query_ctx->fragment_ids.push_back(fragment_instance_id);
            }
            START_AND_SCOPE_SPAN(tracer, span, "exec_instance");
            span->SetAttribute("instance_id", print_id(fragment_instance_id));

            if (!params.__isset.need_wait_execution_trigger ||
                !params.need_wait_execution_trigger) {
                query_ctx->set_ready_to_execute_only();
            }
        }
        {
            std::lock_guard<std::mutex> lock(_lock);
            std::vector<TUniqueId> ins_ids;
            reinterpret_cast<pipeline::PipelineXFragmentContext*>(context.get())
                    ->instance_ids(ins_ids);
            // TODO: simplify this mapping
            for (const auto& ins_id : ins_ids) {
                _pipeline_map.insert({ins_id, context});
            }

            _cv.notify_all();
        }

        RETURN_IF_ERROR(context->submit());
        return Status::OK();
    } else {
        auto pre_and_submit = [&](int i) {
            const auto& local_params = params.local_params[i];

            const TUniqueId& fragment_instance_id = local_params.fragment_instance_id;
            {
                std::lock_guard<std::mutex> lock(_lock);
                auto iter = _pipeline_map.find(fragment_instance_id);
                if (iter != _pipeline_map.end()) {
                    // Duplicated
                    return Status::OK();
                }
                query_ctx->fragment_ids.push_back(fragment_instance_id);
            }
            START_AND_SCOPE_SPAN(tracer, span, "exec_instance");
            span->SetAttribute("instance_id", print_id(fragment_instance_id));

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
                    context->close_if_prepare_failed();
                    static_cast<void>(context->update_status(prepare_st));
                    return prepare_st;
                }
            }
            g_fragmentmgr_prepare_latency << (duration_ns / 1000);

            std::shared_ptr<RuntimeFilterMergeControllerEntity> handler;
            static_cast<void>(_runtimefilter_controller.add_entity(
                    params, local_params, &handler, context->get_runtime_state(UniqueId())));
            context->set_merge_controller_handler(handler);

            {
                std::lock_guard<std::mutex> lock(_lock);
                _pipeline_map.insert(std::make_pair(fragment_instance_id, context));
                _cv.notify_all();
            }

            return context->submit();
        };

        int target_size = params.local_params.size();
        if (target_size > 1) {
            int prepare_done = {0};
            Status prepare_status[target_size];
            std::mutex m;
            std::condition_variable cv;

            for (size_t i = 0; i < target_size; i++) {
                static_cast<void>(_thread_pool->submit_func([&, i]() {
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

void FragmentMgr::cancel_query(const TUniqueId& query_id, const PPlanFragmentCancelReason& reason,
                               const std::string& msg) {
    std::unique_lock<std::mutex> state_lock(_lock);
    return cancel_query_unlocked(query_id, reason, state_lock, msg);
}

// Cancel all instances/fragments of query, and set query_ctx of the query canceled at last.
void FragmentMgr::cancel_query_unlocked(const TUniqueId& query_id,
                                        const PPlanFragmentCancelReason& reason,
                                        const std::unique_lock<std::mutex>& state_lock,
                                        const std::string& msg) {
    auto ctx = _query_ctx_map.find(query_id);

    if (ctx == _query_ctx_map.end()) {
        LOG(WARNING) << "Query " << print_id(query_id) << " does not exists, failed to cancel it";
        return;
    }

    if (ctx->second->enable_pipeline_exec()) {
        for (auto it : ctx->second->fragment_ids) {
            // instance_id will not be removed from query_context.instance_ids currently
            // and it will be removed from fragment_mgr::_pipeline_map only.
            // so we add this check to avoid too many WARNING log.
            if (_pipeline_map.contains(it)) {
                cancel_instance_unlocked(it, reason, state_lock, msg);
            }
        }
    } else {
        for (auto it : ctx->second->fragment_ids) {
            cancel_fragment_unlocked(it, reason, state_lock, msg);
        }
    }

    ctx->second->cancel(true, msg, Status::Cancelled(msg));
    _query_ctx_map.erase(query_id);
    LOG(INFO) << "Query " << print_id(query_id) << " is cancelled. Reason: " << msg;
}

void FragmentMgr::cancel_fragment(const TUniqueId& fragment_id,
                                  const PPlanFragmentCancelReason& reason, const std::string& msg) {
    std::unique_lock<std::mutex> state_lock(_lock);
    return cancel_fragment_unlocked(fragment_id, reason, state_lock, msg);
}

void FragmentMgr::cancel_fragment_unlocked(const TUniqueId& fragment_id,
                                           const PPlanFragmentCancelReason& reason,
                                           const std::unique_lock<std::mutex>& state_lock,
                                           const std::string& msg) {
    return cancel_unlocked_impl(fragment_id, reason, state_lock, false /*not pipeline query*/, msg);
}

void FragmentMgr::cancel_instance(const TUniqueId& instance_id,
                                  const PPlanFragmentCancelReason& reason, const std::string& msg) {
    std::unique_lock<std::mutex> state_lock(_lock);
    return cancel_instance_unlocked(instance_id, reason, state_lock, msg);
}

void FragmentMgr::cancel_instance_unlocked(const TUniqueId& instance_id,
                                           const PPlanFragmentCancelReason& reason,
                                           const std::unique_lock<std::mutex>& state_lock,
                                           const std::string& msg) {
    return cancel_unlocked_impl(instance_id, reason, state_lock, true /*pipeline query*/, msg);
}

void FragmentMgr::cancel_unlocked_impl(const TUniqueId& id, const PPlanFragmentCancelReason& reason,
                                       const std::unique_lock<std::mutex>& /*state_lock*/,
                                       bool is_pipeline, const std::string& msg) {
    if (is_pipeline) {
        const TUniqueId& instance_id = id;
        auto itr = _pipeline_map.find(instance_id);

        if (itr != _pipeline_map.end()) {
            // calling PipelineFragmentContext::cancel
            itr->second->cancel(reason, msg);
        } else {
            LOG(WARNING) << "Could not find the instance id:" << print_id(instance_id)
                         << " to cancel";
        }
    } else {
        const TUniqueId& fragment_id = id;
        auto itr = _fragment_map.find(fragment_id);

        if (itr != _fragment_map.end()) {
            // calling PlanFragmentExecutor::cancel
            itr->second->cancel(reason, msg);
        } else {
            LOG(WARNING) << "Could not find the fragment id:" << print_id(fragment_id)
                         << " to cancel";
        }
    }
}

bool FragmentMgr::query_is_canceled(const TUniqueId& query_id) {
    std::lock_guard<std::mutex> lock(_lock);
    auto ctx = _query_ctx_map.find(query_id);

    if (ctx != _query_ctx_map.end()) {
        const bool is_pipeline_version = ctx->second->enable_pipeline_exec();
        for (auto itr : ctx->second->fragment_ids) {
            if (is_pipeline_version) {
                auto pipeline_ctx_iter = _pipeline_map.find(itr);
                if (pipeline_ctx_iter != _pipeline_map.end() && pipeline_ctx_iter->second) {
                    return pipeline_ctx_iter->second->is_canceled();
                }
            } else {
                auto fragment_executor_iter = _fragment_map.find(itr);
                if (fragment_executor_iter != _fragment_map.end() &&
                    fragment_executor_iter->second) {
                    return fragment_executor_iter->second->is_canceled();
                }
            }
        }
    }

    return true;
}

void FragmentMgr::cancel_worker() {
    LOG(INFO) << "FragmentMgr cancel worker start working.";
    do {
        std::vector<TUniqueId> to_cancel;
        std::vector<TUniqueId> queries_to_cancel;
        vectorized::VecDateTimeValue now = vectorized::VecDateTimeValue::local_time();
        {
            std::lock_guard<std::mutex> lock(_lock);
            for (auto& it : _fragment_map) {
                if (it.second->is_timeout(now)) {
                    to_cancel.push_back(it.second->fragment_instance_id());
                }
            }
            for (auto it = _query_ctx_map.begin(); it != _query_ctx_map.end();) {
                if (it->second->is_timeout(now)) {
                    it = _query_ctx_map.erase(it);
                } else {
                    ++it;
                }
            }

            const auto& running_fes = ExecEnv::GetInstance()->get_running_frontends();

            // We use a very conservative cancel strategy.
            // 0. If there are no running frontends, do not cancel any queries.
            // 1. If query's process uuid is zero, do not cancel
            // 2. If same process uuid, do not cancel
            // 3. If fe has zero process uuid, do not cancel
            if (running_fes.empty()) {
                LOG_EVERY_N(WARNING, 10)
                        << "Could not find any running frontends, maybe we are upgrading? "
                        << "We will not cancel any running queries in this situation.";
            } else {
                for (const auto& q : _query_ctx_map) {
                    if (q.second->get_fe_process_uuid() == 0) {
                        continue;
                    }

                    auto itr = running_fes.find(q.second->coord_addr);
                    if (itr != running_fes.end()) {
                        if (q.second->get_fe_process_uuid() == itr->second.info.process_uuid ||
                            itr->second.info.process_uuid == 0) {
                            continue;
                        }
                    }

                    // Coorninator of this query has already dead.
                    queries_to_cancel.push_back(q.first);
                }
            }
        }

        // TODO(zhiqiang): It seems that timeout_canceled_fragment_count is
        // designed to count canceled fragment of non-pipeline query.
        timeout_canceled_fragment_count->increment(to_cancel.size());
        for (auto& id : to_cancel) {
            cancel_fragment(id, PPlanFragmentCancelReason::TIMEOUT);
            LOG(INFO) << "FragmentMgr cancel worker going to cancel timeout fragment "
                      << print_id(id);
        }

        if (!queries_to_cancel.empty()) {
            LOG(INFO) << "There are " << queries_to_cancel.size()
                      << " queries need to be cancelled, coordinator dead.";
        }

        for (const auto& qid : queries_to_cancel) {
            cancel_query(qid, PPlanFragmentCancelReason::INTERNAL_ERROR,
                         std::string("Coordinator dead."));
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(1)));
    LOG(INFO) << "FragmentMgr cancel worker is going to exit.";
}

void FragmentMgr::debug(std::stringstream& ss) {
    // Keep things simple
    std::lock_guard<std::mutex> lock(_lock);

    ss << "FragmentMgr have " << _fragment_map.size() << " jobs.\n";
    ss << "job_id\t\tstart_time\t\texecute_time(s)\n";
    vectorized::VecDateTimeValue now = vectorized::VecDateTimeValue::local_time();
    for (auto& it : _fragment_map) {
        ss << it.first << "\t" << it.second->start_time().debug_string() << "\t"
           << now.second_diff(it.second->start_time()) << "\n";
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
    return exec_plan_fragment(exec_fragment_params);
}

Status FragmentMgr::apply_filter(const PPublishFilterRequest* request,
                                 butil::IOBufAsZeroCopyInputStream* attach_data) {
    bool is_pipeline = request->has_is_pipeline() && request->is_pipeline();

    UniqueId fragment_instance_id = request->fragment_id();
    TUniqueId tfragment_instance_id = fragment_instance_id.to_thrift();

    std::shared_ptr<PlanFragmentExecutor> fragment_executor;
    std::shared_ptr<pipeline::PipelineFragmentContext> pip_context;

    RuntimeFilterMgr* runtime_filter_mgr = nullptr;
    if (is_pipeline) {
        std::unique_lock<std::mutex> lock(_lock);
        auto iter = _pipeline_map.find(tfragment_instance_id);
        if (iter == _pipeline_map.end()) {
            VLOG_CRITICAL << "unknown.... fragment-id:" << fragment_instance_id;
            return Status::InvalidArgument("fragment-id: {}", fragment_instance_id.to_string());
        }
        pip_context = iter->second;

        DCHECK(pip_context != nullptr);
        runtime_filter_mgr =
                pip_context->get_runtime_state(fragment_instance_id)->runtime_filter_mgr();
    } else {
        std::unique_lock<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(tfragment_instance_id);
        if (iter == _fragment_map.end()) {
            VLOG_CRITICAL << "unknown.... fragment-id:" << fragment_instance_id;
            return Status::InvalidArgument("fragment-id: {}", fragment_instance_id.to_string());
        }
        fragment_executor = iter->second;

        DCHECK(fragment_executor != nullptr);
        runtime_filter_mgr = fragment_executor->runtime_state()->runtime_filter_mgr();
    }

    return runtime_filter_mgr->update_filter(request, attach_data);
}

Status FragmentMgr::apply_filterv2(const PPublishFilterRequestV2* request,
                                   butil::IOBufAsZeroCopyInputStream* attach_data) {
    bool is_pipeline = request->has_is_pipeline() && request->is_pipeline();
    int64_t start_apply = MonotonicMillis();

    const auto& fragment_instance_ids = request->fragment_instance_ids();
    if (fragment_instance_ids.size() > 0) {
        UniqueId fragment_instance_id = fragment_instance_ids[0];
        TUniqueId tfragment_instance_id = fragment_instance_id.to_thrift();

        std::shared_ptr<PlanFragmentExecutor> fragment_executor;
        std::shared_ptr<pipeline::PipelineFragmentContext> pip_context;

        RuntimeFilterMgr* runtime_filter_mgr = nullptr;
        ObjectPool* pool = nullptr;
        if (is_pipeline) {
            std::unique_lock<std::mutex> lock(_lock);
            auto iter = _pipeline_map.find(tfragment_instance_id);
            if (iter == _pipeline_map.end()) {
                VLOG_CRITICAL << "unknown.... fragment-id:" << fragment_instance_id;
                return Status::InvalidArgument("fragment-id: {}", fragment_instance_id.to_string());
            }
            pip_context = iter->second;

            DCHECK(pip_context != nullptr);
            runtime_filter_mgr = pip_context->get_runtime_state(fragment_instance_id)
                                         ->get_query_ctx()
                                         ->runtime_filter_mgr();
            pool = &pip_context->get_query_context()->obj_pool;
        } else {
            std::unique_lock<std::mutex> lock(_lock);
            auto iter = _fragment_map.find(tfragment_instance_id);
            if (iter == _fragment_map.end()) {
                VLOG_CRITICAL << "unknown.... fragment-id:" << fragment_instance_id;
                return Status::InvalidArgument("fragment-id: {}", fragment_instance_id.to_string());
            }
            fragment_executor = iter->second;

            DCHECK(fragment_executor != nullptr);
            runtime_filter_mgr = fragment_executor->get_query_ctx()->runtime_filter_mgr();
            pool = &fragment_executor->get_query_ctx()->obj_pool;
        }

        UpdateRuntimeFilterParamsV2 params(request, attach_data, pool);
        int filter_id = request->filter_id();
        std::vector<IRuntimeFilter*> filters;
        RETURN_IF_ERROR(runtime_filter_mgr->get_consume_filters(filter_id, filters));

        IRuntimeFilter* first_filter = nullptr;
        for (auto filter : filters) {
            if (!first_filter) {
                RETURN_IF_ERROR(filter->update_filter(&params, start_apply));
                first_filter = filter;
            } else {
                filter->copy_from_other(first_filter);
                filter->signal();
            }
        }
    }

    return Status::OK();
}

Status FragmentMgr::merge_filter(const PMergeFilterRequest* request,
                                 butil::IOBufAsZeroCopyInputStream* attach_data) {
    UniqueId queryid = request->query_id();
    bool is_pipeline = request->has_is_pipeline() && request->is_pipeline();
    bool opt_remote_rf = request->has_opt_remote_rf() && request->opt_remote_rf();
    std::shared_ptr<RuntimeFilterMergeControllerEntity> filter_controller;
    RETURN_IF_ERROR(_runtimefilter_controller.acquire(queryid, &filter_controller));

    auto fragment_instance_id = filter_controller->instance_id();
    TUniqueId tfragment_instance_id = fragment_instance_id.to_thrift();
    std::shared_ptr<PlanFragmentExecutor> fragment_executor;
    std::shared_ptr<pipeline::PipelineFragmentContext> pip_context;
    if (is_pipeline) {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _pipeline_map.find(tfragment_instance_id);
        if (iter == _pipeline_map.end()) {
            VLOG_CRITICAL << "unknown fragment-id:" << fragment_instance_id;
            return Status::InvalidArgument("fragment-id: {}", fragment_instance_id.to_string());
        }

        // hold reference to pip_context, or else runtime_state can be destroyed
        // when filter_controller->merge is still in progress
        pip_context = iter->second;
    } else {
        std::unique_lock<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(tfragment_instance_id);
        if (iter == _fragment_map.end()) {
            VLOG_CRITICAL << "unknown fragment-id:" << fragment_instance_id;
            return Status::InvalidArgument("fragment-id: {}", fragment_instance_id.to_string());
        }

        // hold reference to fragment_executor, or else runtime_state can be destroyed
        // when filter_controller->merge is still in progress
        fragment_executor = iter->second;
    }
    RETURN_IF_ERROR(filter_controller->merge(request, attach_data, opt_remote_rf));
    return Status::OK();
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

} // namespace doris
