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

#include <gperftools/profiler.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <memory>
#include <sstream>

#include "agent/cgroups_mgr.h"
#include "common/object_pool.h"
#include "common/resource_tls.h"
#include "common/signal_handler.h"
#include "exprs/bloomfilter_predicate.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "opentelemetry/trace/scope.h"
#include "runtime/client_cache.h"
#include "runtime/datetime_value.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/stopwatch.hpp"
#include "util/telemetry/telemetry.h"
#include "util/threadpool.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
#include "util/url_coding.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(plan_fragment_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(timeout_canceled_fragment_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(fragment_thread_pool_queue_size, MetricUnit::NOUNIT);

std::string to_load_error_http_path(const std::string& file_name) {
    if (file_name.empty()) {
        return "";
    }
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port
        << "/api/_load_error_log?"
        << "file=" << file_name;
    return url.str();
}

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::transport::TTransportException;

class RuntimeProfile;
class FragmentExecState {
public:
    // Constructor by using QueryFragmentsCtx
    FragmentExecState(const TUniqueId& query_id, const TUniqueId& instance_id, int backend_num,
                      ExecEnv* exec_env, std::shared_ptr<QueryFragmentsCtx> fragments_ctx);

    FragmentExecState(const TUniqueId& query_id, const TUniqueId& instance_id, int backend_num,
                      ExecEnv* exec_env, const TNetworkAddress& coord_addr);

    Status prepare(const TExecPlanFragmentParams& params);

    // just no use now
    void callback(const Status& status, RuntimeProfile* profile, bool done);

    std::string to_http_path(const std::string& file_name);

    Status execute();

    Status cancel(const PPlanFragmentCancelReason& reason, const std::string& msg = "");
    TUniqueId fragment_instance_id() const { return _fragment_instance_id; }

    TUniqueId query_id() const { return _query_id; }

    PlanFragmentExecutor* executor() { return &_executor; }

    const DateTimeValue& start_time() const { return _start_time; }

    void set_merge_controller_handler(
            std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {
        _merge_controller_handler = handler;
    }

    // Update status of this fragment execute
    Status update_status(Status status) {
        std::lock_guard<std::mutex> l(_status_lock);
        if (!status.ok() && _exec_status.ok()) {
            _exec_status = status;
        }
        return _exec_status;
    }

    void set_group(const TResourceInfo& info) {
        _set_rsc_info = true;
        _user = info.user;
        _group = info.group;
    }

    bool is_timeout(const DateTimeValue& now) const {
        if (_timeout_second <= 0) {
            return false;
        }
        if (now.second_diff(_start_time) > _timeout_second) {
            return true;
        }
        return false;
    }

    int get_timeout_second() const { return _timeout_second; }

    std::shared_ptr<QueryFragmentsCtx> get_fragments_ctx() { return _fragments_ctx; }

    void set_pipe(std::shared_ptr<StreamLoadPipe> pipe) { _pipe = pipe; }
    std::shared_ptr<StreamLoadPipe> get_pipe() const { return _pipe; }

    void set_need_wait_execution_trigger() { _need_wait_execution_trigger = true; }

private:
    void coordinator_callback(const Status& status, RuntimeProfile* profile, bool done);

    // Id of this query
    TUniqueId _query_id;
    // Id of this instance
    TUniqueId _fragment_instance_id;
    // Used to report to coordinator which backend is over
    int _backend_num;
    ExecEnv* _exec_env;
    TNetworkAddress _coord_addr;

    PlanFragmentExecutor _executor;
    DateTimeValue _start_time;

    std::mutex _status_lock;
    Status _exec_status;

    bool _set_rsc_info = false;
    std::string _user;
    std::string _group;

    int _timeout_second;
    bool _cancelled = false;

    // This context is shared by all fragments of this host in a query
    std::shared_ptr<QueryFragmentsCtx> _fragments_ctx;

    std::shared_ptr<RuntimeFilterMergeControllerEntity> _merge_controller_handler;
    // The pipe for data transfering, such as insert.
    std::shared_ptr<StreamLoadPipe> _pipe;

    // If set the true, this plan fragment will be executed only after FE send execution start rpc.
    bool _need_wait_execution_trigger = false;
};

FragmentExecState::FragmentExecState(const TUniqueId& query_id,
                                     const TUniqueId& fragment_instance_id, int backend_num,
                                     ExecEnv* exec_env,
                                     std::shared_ptr<QueryFragmentsCtx> fragments_ctx)
        : _query_id(query_id),
          _fragment_instance_id(fragment_instance_id),
          _backend_num(backend_num),
          _exec_env(exec_env),
          _executor(exec_env, std::bind<void>(std::mem_fn(&FragmentExecState::coordinator_callback),
                                              this, std::placeholders::_1, std::placeholders::_2,
                                              std::placeholders::_3)),
          _set_rsc_info(false),
          _timeout_second(-1),
          _fragments_ctx(std::move(fragments_ctx)) {
    _start_time = DateTimeValue::local_time();
    _coord_addr = _fragments_ctx->coord_addr;
}

FragmentExecState::FragmentExecState(const TUniqueId& query_id,
                                     const TUniqueId& fragment_instance_id, int backend_num,
                                     ExecEnv* exec_env, const TNetworkAddress& coord_addr)
        : _query_id(query_id),
          _fragment_instance_id(fragment_instance_id),
          _backend_num(backend_num),
          _exec_env(exec_env),
          _coord_addr(coord_addr),
          _executor(exec_env, std::bind<void>(std::mem_fn(&FragmentExecState::coordinator_callback),
                                              this, std::placeholders::_1, std::placeholders::_2,
                                              std::placeholders::_3)),
          _timeout_second(-1) {
    _start_time = DateTimeValue::local_time();
}

Status FragmentExecState::prepare(const TExecPlanFragmentParams& params) {
    if (params.__isset.query_options) {
        _timeout_second = params.query_options.query_timeout;
    }

    if (_fragments_ctx == nullptr) {
        if (params.__isset.resource_info) {
            set_group(params.resource_info);
        }
    }

    if (_fragments_ctx == nullptr) {
        return _executor.prepare(params);
    } else {
        return _executor.prepare(params, _fragments_ctx.get());
    }
}

Status FragmentExecState::execute() {
    if (_need_wait_execution_trigger) {
        // if _need_wait_execution_trigger is true, which means this instance
        // is prepared but need to wait for the signal to do the rest execution.
        if (!_fragments_ctx->wait_for_start()) {
            return cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "wait fragment start timeout");
        }
    }
#ifndef BE_TEST
    if (_executor.runtime_state()->is_cancelled()) {
        return Status::Cancelled("cancelled before execution");
    }
#endif
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        CgroupsMgr::apply_system_cgroup();
        opentelemetry::trace::Tracer::GetCurrentSpan()->AddEvent("start executing Fragment");
        WARN_IF_ERROR(_executor.open(), strings::Substitute("Got error while opening fragment $0",
                                                            print_id(_fragment_instance_id)));

        _executor.close();
    }
    DorisMetrics::instance()->fragment_requests_total->increment(1);
    DorisMetrics::instance()->fragment_request_duration_us->increment(duration_ns / 1000);
    return Status::OK();
}

Status FragmentExecState::cancel(const PPlanFragmentCancelReason& reason, const std::string& msg) {
    if (!_cancelled) {
        std::lock_guard<std::mutex> l(_status_lock);
        if (reason == PPlanFragmentCancelReason::LIMIT_REACH) {
            _executor.set_is_report_on_cancel(false);
        }
        _executor.cancel(reason, msg);
        if (_pipe != nullptr) {
            _pipe->cancel(PPlanFragmentCancelReason_Name(reason));
        }
        _cancelled = true;
    }
    return Status::OK();
}

void FragmentExecState::callback(const Status& status, RuntimeProfile* profile, bool done) {}

std::string FragmentExecState::to_http_path(const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port
        << "/api/_download_load?"
        << "token=" << _exec_env->token() << "&file=" << file_name;
    return url.str();
}

// There can only be one of these callbacks in-flight at any moment, because
// it is only invoked from the executor's reporting thread.
// Also, the reported status will always reflect the most recent execution status,
// including the final status when execution finishes.
void FragmentExecState::coordinator_callback(const Status& status, RuntimeProfile* profile,
                                             bool done) {
    DCHECK(status.ok() || done); // if !status.ok() => done
    Status exec_status = update_status(status);

    Status coord_status;
    FrontendServiceConnection coord(_exec_env->frontend_client_cache(), _coord_addr, &coord_status);
    if (!coord_status.ok()) {
        std::stringstream ss;
        UniqueId uid(_query_id.hi, _query_id.lo);
        ss << "couldn't get a client for " << _coord_addr << ", reason: " << coord_status;
        LOG(WARNING) << "query_id: " << uid << ", " << ss.str();
        update_status(Status::InternalError(ss.str()));
        return;
    }

    TReportExecStatusParams params;
    params.protocol_version = FrontendServiceVersion::V1;
    params.__set_query_id(_query_id);
    params.__set_backend_num(_backend_num);
    params.__set_fragment_instance_id(_fragment_instance_id);
    exec_status.set_t_status(&params);
    params.__set_done(done);

    RuntimeState* runtime_state = _executor.runtime_state();
    DCHECK(runtime_state != nullptr);
    if (runtime_state->query_type() == TQueryType::LOAD && !done && status.ok()) {
        // this is a load plan, and load is not finished, just make a brief report
        params.__set_loaded_rows(runtime_state->num_rows_load_total());
        params.__set_loaded_bytes(runtime_state->num_bytes_load_total());
    } else {
        if (runtime_state->query_type() == TQueryType::LOAD) {
            params.__set_loaded_rows(runtime_state->num_rows_load_total());
            params.__set_loaded_bytes(runtime_state->num_bytes_load_total());
        }
        if (profile == nullptr) {
            params.__isset.profile = false;
        } else {
            profile->to_thrift(&params.profile);
            params.__isset.profile = true;
        }

        if (!runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(it));
            }
        }
        if (runtime_state->num_rows_load_total() > 0 ||
            runtime_state->num_rows_load_filtered() > 0) {
            params.__isset.load_counters = true;

            static std::string s_dpp_normal_all = "dpp.norm.ALL";
            static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";
            static std::string s_unselected_rows = "unselected.rows";

            params.load_counters.emplace(s_dpp_normal_all,
                                         std::to_string(runtime_state->num_rows_load_success()));
            params.load_counters.emplace(s_dpp_abnormal_all,
                                         std::to_string(runtime_state->num_rows_load_filtered()));
            params.load_counters.emplace(s_unselected_rows,
                                         std::to_string(runtime_state->num_rows_load_unselected()));
        }
        if (!runtime_state->get_error_log_file_path().empty()) {
            params.__set_tracking_url(
                    to_load_error_http_path(runtime_state->get_error_log_file_path()));
        }
        if (!runtime_state->export_output_files().empty()) {
            params.__isset.export_files = true;
            params.export_files = runtime_state->export_output_files();
        }
        if (!runtime_state->tablet_commit_infos().empty()) {
            params.__isset.commitInfos = true;
            params.commitInfos.reserve(runtime_state->tablet_commit_infos().size());
            for (auto& info : runtime_state->tablet_commit_infos()) {
                params.commitInfos.push_back(info);
            }
        }
        if (!runtime_state->error_tablet_infos().empty()) {
            params.__isset.errorTabletInfos = true;
            params.errorTabletInfos.reserve(runtime_state->error_tablet_infos().size());
            for (auto& info : runtime_state->error_tablet_infos()) {
                params.errorTabletInfos.push_back(info);
            }
        }

        // Send new errors to coordinator
        runtime_state->get_unreported_errors(&(params.error_log));
        params.__isset.error_log = (params.error_log.size() > 0);
    }

    if (_exec_env->master_info()->__isset.backend_id) {
        params.__set_backend_id(_exec_env->master_info()->backend_id);
    }

    TReportExecStatusResult res;
    Status rpc_status;

    VLOG_DEBUG << "reportExecStatus params is "
               << apache::thrift::ThriftDebugString(params).c_str();
    try {
        try {
            coord->reportExecStatus(res, params);
        } catch (TTransportException& e) {
            LOG(WARNING) << "Retrying ReportExecStatus. query id: " << print_id(_query_id)
                         << ", instance id: " << print_id(_fragment_instance_id) << " to "
                         << _coord_addr << ", err: " << e.what();
            rpc_status = coord.reopen();

            if (!rpc_status.ok()) {
                // we need to cancel the execution of this fragment
                update_status(rpc_status);
                _executor.cancel();
                return;
            }
            coord->reportExecStatus(res, params);
        }

        rpc_status = Status(res.status);
    } catch (TException& e) {
        std::stringstream msg;
        msg << "ReportExecStatus() to " << _coord_addr << " failed:\n" << e.what();
        LOG(WARNING) << msg.str();
        rpc_status = Status::InternalError(msg.str());
    }

    if (!rpc_status.ok()) {
        // we need to cancel the execution of this fragment
        update_status(rpc_status);
        _executor.cancel();
    }
}

FragmentMgr::FragmentMgr(ExecEnv* exec_env)
        : _exec_env(exec_env),
          _fragment_map(),
          _fragments_ctx_map(),
          _bf_size_map(),
          _stop_background_threads_latch(1) {
    _entity = DorisMetrics::instance()->metric_registry()->register_entity("FragmentMgr");
    INT_UGAUGE_METRIC_REGISTER(_entity, timeout_canceled_fragment_count);
    REGISTER_HOOK_METRIC(plan_fragment_count, [this]() {
        // std::lock_guard<std::mutex> lock(_lock);
        return _fragment_map.size();
    });

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
}

FragmentMgr::~FragmentMgr() {
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
        _fragments_ctx_map.clear();
        _bf_size_map.clear();
    }
}

static void empty_function(PlanFragmentExecutor* exec) {}

void FragmentMgr::_exec_actual(std::shared_ptr<FragmentExecState> exec_state, FinishCallback cb) {
    std::string func_name {"PlanFragmentExecutor::_exec_actual"};
#ifndef BE_TEST
    auto span = exec_state->executor()->runtime_state()->get_tracer()->StartSpan(func_name);
    SCOPED_ATTACH_TASK(exec_state->executor()->runtime_state());
#else
    auto span = telemetry::get_noop_tracer()->StartSpan(func_name);
#endif
    auto scope = opentelemetry::trace::Scope {span};
    span->SetAttribute("query_id", print_id(exec_state->query_id()));
    span->SetAttribute("instance_id", print_id(exec_state->fragment_instance_id()));

    // these two are used to output query_id when be cored dump.
    doris::signal::query_id_hi = exec_state->query_id().hi;
    doris::signal::query_id_lo = exec_state->query_id().lo;

    LOG_INFO(func_name)
            .tag("query_id", exec_state->query_id())
            .tag("instance_id", exec_state->fragment_instance_id())
            .tag("pthread_id", (uintptr_t)pthread_self());

    exec_state->execute();

    std::shared_ptr<QueryFragmentsCtx> fragments_ctx = exec_state->get_fragments_ctx();
    bool all_done = false;
    if (fragments_ctx != nullptr) {
        // decrease the number of unfinished fragments
        all_done = fragments_ctx->countdown();
    }

    // remove exec state after this fragment finished
    {
        std::lock_guard<std::mutex> lock(_lock);
        _fragment_map.erase(exec_state->fragment_instance_id());
        if (all_done && fragments_ctx) {
            _fragments_ctx_map.erase(fragments_ctx->query_id);
            _bf_size_map.erase(fragments_ctx->query_id);
        }
    }

    // Callback after remove from this id
    cb(exec_state->executor());
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params) {
    if (params.txn_conf.need_txn) {
        StreamLoadContext* stream_load_cxt = new StreamLoadContext(_exec_env);
        stream_load_cxt->db = params.txn_conf.db;
        stream_load_cxt->db_id = params.txn_conf.db_id;
        stream_load_cxt->table = params.txn_conf.tbl;
        stream_load_cxt->txn_id = params.txn_conf.txn_id;
        stream_load_cxt->id = UniqueId(params.params.query_id);
        stream_load_cxt->put_result.params = params;
        stream_load_cxt->use_streaming = true;
        stream_load_cxt->load_type = TLoadType::MANUL_LOAD;
        stream_load_cxt->load_src_type = TLoadSourceType::RAW;
        stream_load_cxt->label = params.import_label;
        stream_load_cxt->format = TFileFormatType::FORMAT_CSV_PLAIN;
        stream_load_cxt->timeout_second = 3600;
        stream_load_cxt->auth.auth_code_uuid = params.txn_conf.auth_code_uuid;
        stream_load_cxt->need_commit_self = true;
        stream_load_cxt->need_rollback = true;
        // total_length == -1 means read one message from pipe in once time, don't care the length.
        auto pipe = std::make_shared<StreamLoadPipe>(kMaxPipeBufferedBytes /* max_buffered_bytes */,
                                                     64 * 1024 /* min_chunk_size */,
                                                     -1 /* total_length */, true /* use_proto */);
        stream_load_cxt->body_sink = pipe;
        stream_load_cxt->max_filter_ratio = params.txn_conf.max_filter_ratio;

        RETURN_IF_ERROR(_exec_env->load_stream_mgr()->put(stream_load_cxt->id, pipe));

        RETURN_IF_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(stream_load_cxt));
        set_pipe(params.params.fragment_instance_id, pipe);
        return Status::OK();
    } else {
        return exec_plan_fragment(params, std::bind<void>(&empty_function, std::placeholders::_1));
    }
}

Status FragmentMgr::start_query_execution(const PExecPlanFragmentStartRequest* request) {
    std::lock_guard<std::mutex> lock(_lock);
    TUniqueId query_id;
    query_id.__set_hi(request->query_id().hi());
    query_id.__set_lo(request->query_id().lo());
    auto search = _fragments_ctx_map.find(query_id);
    if (search == _fragments_ctx_map.end()) {
        return Status::InternalError(
                "Failed to get query fragments context. Query may be "
                "timeout or be cancelled. host: {}",
                BackendOptions::get_localhost());
    }
    search->second->set_ready_to_execute(false);
    return Status::OK();
}

void FragmentMgr::set_pipe(const TUniqueId& fragment_instance_id,
                           std::shared_ptr<StreamLoadPipe> pipe) {
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(fragment_instance_id);
        if (iter != _fragment_map.end()) {
            _fragment_map[fragment_instance_id]->set_pipe(std::move(pipe));
        }
    }
}

std::shared_ptr<StreamLoadPipe> FragmentMgr::get_pipe(const TUniqueId& fragment_instance_id) {
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(fragment_instance_id);
        if (iter != _fragment_map.end()) {
            return _fragment_map[fragment_instance_id]->get_pipe();
        } else {
            return nullptr;
        }
    }
}

Status FragmentMgr::exec_plan_fragment(const TExecPlanFragmentParams& params, FinishCallback cb) {
    auto tracer = telemetry::is_current_span_valid() ? telemetry::get_tracer("tracer")
                                                     : telemetry::get_noop_tracer();
    START_AND_SCOPE_SPAN(tracer, span, "FragmentMgr::exec_plan_fragment");
    const TUniqueId& fragment_instance_id = params.params.fragment_instance_id;
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(fragment_instance_id);
        if (iter != _fragment_map.end()) {
            // Duplicated
            return Status::OK();
        }
    }

    std::shared_ptr<FragmentExecState> exec_state;
    std::shared_ptr<QueryFragmentsCtx> fragments_ctx;
    if (params.is_simplified_param) {
        // Get common components from _fragments_ctx_map
        std::lock_guard<std::mutex> lock(_lock);
        auto search = _fragments_ctx_map.find(params.params.query_id);
        if (search == _fragments_ctx_map.end()) {
            return Status::InternalError(
                    "Failed to get query fragments context. Query may be "
                    "timeout or be cancelled. host: {}",
                    BackendOptions::get_localhost());
        }
        fragments_ctx = search->second;
        _set_scan_concurrency(params, fragments_ctx.get());
    } else {
        // This may be a first fragment request of the query.
        // Create the query fragments context.
        fragments_ctx.reset(new QueryFragmentsCtx(params.fragment_num_on_host, _exec_env));
        fragments_ctx->query_id = params.params.query_id;
        RETURN_IF_ERROR(DescriptorTbl::create(&(fragments_ctx->obj_pool), params.desc_tbl,
                                              &(fragments_ctx->desc_tbl)));
        fragments_ctx->coord_addr = params.coord;
        LOG(INFO) << "query_id: "
                  << UniqueId(fragments_ctx->query_id.hi, fragments_ctx->query_id.lo)
                  << " coord_addr " << fragments_ctx->coord_addr;
        fragments_ctx->query_globals = params.query_globals;

        if (params.__isset.resource_info) {
            fragments_ctx->user = params.resource_info.user;
            fragments_ctx->group = params.resource_info.group;
            fragments_ctx->set_rsc_info = true;
        }

        fragments_ctx->timeout_second = params.query_options.query_timeout;
        _set_scan_concurrency(params, fragments_ctx.get());

        {
            // Find _fragments_ctx_map again, in case some other request has already
            // create the query fragments context.
            std::lock_guard<std::mutex> lock(_lock);
            auto search = _fragments_ctx_map.find(params.params.query_id);
            if (search == _fragments_ctx_map.end()) {
                _fragments_ctx_map.insert(std::make_pair(fragments_ctx->query_id, fragments_ctx));
            } else {
                // Already has a query fragmentscontext, use it
                fragments_ctx = search->second;
            }
        }
    }

    exec_state.reset(new FragmentExecState(fragments_ctx->query_id,
                                           params.params.fragment_instance_id, params.backend_num,
                                           _exec_env, fragments_ctx));
    if (params.__isset.need_wait_execution_trigger && params.need_wait_execution_trigger) {
        // set need_wait_execution_trigger means this instance will not actually being executed
        // until the execPlanFragmentStart RPC trigger to start it.
        exec_state->set_need_wait_execution_trigger();
    }

    std::shared_ptr<RuntimeFilterMergeControllerEntity> handler;
    _runtimefilter_controller.add_entity(params, &handler);
    exec_state->set_merge_controller_handler(handler);

    RETURN_IF_ERROR(exec_state->prepare(params));
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto& runtime_filter_params = params.params.runtime_filter_params;
        if (!runtime_filter_params.rid_to_runtime_filter.empty()) {
            auto bf_size_for_cur_query = _bf_size_map.find(fragments_ctx->query_id);
            if (bf_size_for_cur_query == _bf_size_map.end()) {
                _bf_size_map.insert({fragments_ctx->query_id, {}});
            }
            for (auto& filterid_to_desc : runtime_filter_params.rid_to_runtime_filter) {
                int filter_id = filterid_to_desc.first;
                const auto& target_iter = runtime_filter_params.rid_to_target_param.find(filter_id);
                if (target_iter == runtime_filter_params.rid_to_target_param.end()) {
                    continue;
                }
                const auto& build_iter =
                        runtime_filter_params.runtime_filter_builder_num.find(filter_id);
                if (build_iter == runtime_filter_params.runtime_filter_builder_num.end()) {
                    continue;
                }
                if (filterid_to_desc.second.__isset.bloom_filter_size_bytes) {
                    _bf_size_map[fragments_ctx->query_id].insert(
                            {filter_id, filterid_to_desc.second.bloom_filter_size_bytes});
                }
            }
        }
        _fragment_map.insert(std::make_pair(params.params.fragment_instance_id, exec_state));
        _cv.notify_all();
    }

    auto st = _thread_pool->submit_func(
            [this, exec_state, cb, parent_span = opentelemetry::trace::Tracer::GetCurrentSpan()] {
                OpentelemetryScope scope {parent_span};
                _exec_actual(exec_state, cb);
            });
    if (!st.ok()) {
        {
            // Remove the exec state added
            std::lock_guard<std::mutex> lock(_lock);
            _fragment_map.erase(params.params.fragment_instance_id);
            _bf_size_map.erase(fragments_ctx->query_id);
        }
        exec_state->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR,
                           "push plan fragment to thread pool failed");
        return Status::InternalError(
                strings::Substitute("push plan fragment $0 to thread pool failed. err = $1, BE: $2",
                                    print_id(params.params.fragment_instance_id),
                                    st.get_error_msg(), BackendOptions::get_localhost()));
    }

    return Status::OK();
}

void FragmentMgr::_set_scan_concurrency(const TExecPlanFragmentParams& params,
                                        QueryFragmentsCtx* fragments_ctx) {
#ifndef BE_TEST
    // set thread token
    // the thread token will be set if
    // 1. the cpu_limit is set, or
    // 2. the limit is very small ( < 1024)
    int concurrency = 1;
    bool is_serial = false;
    if (params.query_options.__isset.resource_limit &&
        params.query_options.resource_limit.__isset.cpu_limit) {
        concurrency = params.query_options.resource_limit.cpu_limit;
    } else {
        concurrency = config::doris_scanner_thread_pool_thread_num;
    }
    if (params.__isset.fragment && params.fragment.__isset.plan &&
        params.fragment.plan.nodes.size() > 0) {
        for (auto& node : params.fragment.plan.nodes) {
            // Only for SCAN NODE
            if (!_is_scan_node(node.node_type)) {
                continue;
            }
            if (node.__isset.conjuncts && !node.conjuncts.empty()) {
                // If the scan node has where predicate, do not set concurrency
                continue;
            }
            if (node.limit > 0 && node.limit < 1024) {
                concurrency = 1;
                is_serial = true;
                break;
            }
        }
    }
    fragments_ctx->set_thread_token(concurrency, is_serial);
#endif
}

bool FragmentMgr::_is_scan_node(const TPlanNodeType::type& type) {
    return type == TPlanNodeType::OLAP_SCAN_NODE || type == TPlanNodeType::MYSQL_SCAN_NODE ||
           type == TPlanNodeType::SCHEMA_SCAN_NODE || type == TPlanNodeType::META_SCAN_NODE ||
           type == TPlanNodeType::BROKER_SCAN_NODE || type == TPlanNodeType::ES_SCAN_NODE ||
           type == TPlanNodeType::ES_HTTP_SCAN_NODE || type == TPlanNodeType::ODBC_SCAN_NODE ||
           type == TPlanNodeType::TABLE_VALUED_FUNCTION_SCAN_NODE ||
           type == TPlanNodeType::FILE_SCAN_NODE || type == TPlanNodeType::JDBC_SCAN_NODE;
}

Status FragmentMgr::cancel(const TUniqueId& fragment_id, const PPlanFragmentCancelReason& reason,
                           const std::string& msg) {
    std::shared_ptr<FragmentExecState> exec_state;
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(fragment_id);
        if (iter == _fragment_map.end()) {
            // No match
            return Status::OK();
        }
        exec_state = iter->second;
    }
    exec_state->cancel(reason, msg);

    return Status::OK();
}

void FragmentMgr::cancel_worker() {
    LOG(INFO) << "FragmentMgr cancel worker start working.";
    do {
        std::vector<TUniqueId> to_cancel;
        std::vector<TUniqueId> to_cancel_queries;
        DateTimeValue now = DateTimeValue::local_time();
        {
            std::lock_guard<std::mutex> lock(_lock);
            for (auto& it : _fragment_map) {
                if (it.second->is_timeout(now)) {
                    to_cancel.push_back(it.second->fragment_instance_id());
                }
            }
            for (auto it = _fragments_ctx_map.begin(); it != _fragments_ctx_map.end();) {
                if (it->second->is_timeout(now)) {
                    it = _fragments_ctx_map.erase(it);
                } else {
                    ++it;
                }
            }
        }
        timeout_canceled_fragment_count->increment(to_cancel.size());
        for (auto& id : to_cancel) {
            cancel(id, PPlanFragmentCancelReason::TIMEOUT);
            LOG(INFO) << "FragmentMgr cancel worker going to cancel timeout fragment "
                      << print_id(id);
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(1)));
    LOG(INFO) << "FragmentMgr cancel worker is going to exit.";
}

void FragmentMgr::debug(std::stringstream& ss) {
    // Keep things simple
    std::lock_guard<std::mutex> lock(_lock);

    ss << "FragmentMgr have " << _fragment_map.size() << " jobs.\n";
    ss << "job_id\t\tstart_time\t\texecute_time(s)\n";
    DateTimeValue now = DateTimeValue::local_time();
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
    // batch_size for one RowBatch
    TQueryOptions query_options;
    query_options.batch_size = params.batch_size;
    query_options.query_timeout = params.query_timeout;
    query_options.mem_limit = params.mem_limit;
    query_options.query_type = TQueryType::EXTERNAL;
    exec_fragment_params.__set_query_options(query_options);
    VLOG_ROW << "external exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(exec_fragment_params).c_str();
    return exec_plan_fragment(exec_fragment_params);
}

Status FragmentMgr::apply_filter(const PPublishFilterRequest* request, const char* data) {
    UniqueId fragment_instance_id = request->fragment_id();
    TUniqueId tfragment_instance_id = fragment_instance_id.to_thrift();
    std::shared_ptr<FragmentExecState> fragment_state;

    {
        std::unique_lock<std::mutex> lock(_lock);
        if (!_fragment_map.count(tfragment_instance_id)) {
            VLOG_NOTICE << "wait for fragment start execute, fragment-id:" << fragment_instance_id;
            _cv.wait_for(lock, std::chrono::milliseconds(1000),
                         [&] { return _fragment_map.count(tfragment_instance_id); });
        }

        auto iter = _fragment_map.find(tfragment_instance_id);
        if (iter == _fragment_map.end()) {
            VLOG_CRITICAL << "unknown.... fragment-id:" << fragment_instance_id;
            return Status::InvalidArgument("fragment-id: {}", fragment_instance_id.to_string());
        }
        fragment_state = iter->second;
    }

    DCHECK(fragment_state != nullptr);
    RuntimeFilterMgr* runtime_filter_mgr =
            fragment_state->executor()->runtime_state()->runtime_filter_mgr();

    Status st = runtime_filter_mgr->update_filter(request, data);
    return st;
}

Status FragmentMgr::merge_filter(const PMergeFilterRequest* request, const char* attach_data) {
    UniqueId queryid = request->query_id();
    std::shared_ptr<RuntimeFilterMergeControllerEntity> filter_controller;
    RETURN_IF_ERROR(_runtimefilter_controller.acquire(queryid, &filter_controller));
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto bf_size_for_cur_query = _bf_size_map.find(queryid.to_thrift());
        if (bf_size_for_cur_query != _bf_size_map.end()) {
            for (auto& iter : bf_size_for_cur_query->second) {
                auto bf = filter_controller->get_filter(iter.first)->filter->get_bloomfilter();
                DCHECK(bf != nullptr);
                bf->init_with_fixed_length(iter.second);
            }
        }
    }
    RETURN_IF_ERROR(filter_controller->merge(request, attach_data));
    return Status::OK();
}

} // namespace doris
