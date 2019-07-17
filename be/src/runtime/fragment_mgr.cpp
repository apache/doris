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

#include <memory>
#include <sstream>

#include <gperftools/profiler.h>
#include <boost/bind.hpp>

#include "agent/cgroups_mgr.h"
#include "common/resource_tls.h"
#include "service/backend_options.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/exec_env.h"
#include "runtime/datetime_value.h"
#include "util/stopwatch.hpp"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/thrift_util.h"
#include "runtime/client_cache.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/FrontendService.h"
#include <thrift/protocol/TDebugProtocol.h>

namespace doris {

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
    FragmentExecState(
        const TUniqueId& query_id,
        const TUniqueId& instance_id,
        int backend_num,
        ExecEnv* exec_env,
        const TNetworkAddress& coord_hostport);

    ~FragmentExecState();

    Status prepare(const TExecPlanFragmentParams& params);

    // just no use now
    void callback(const Status& status, RuntimeProfile* profile, bool done);

    std::string to_http_path(const std::string& file_name);

    Status execute();

    Status cancel(const PPlanFragmentCancelReason& reason);

    TUniqueId fragment_instance_id() const {
        return _fragment_instance_id;
    }

    PlanFragmentExecutor* executor() {
        return &_executor;
    }

    const DateTimeValue& start_time() const {
        return _start_time;
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

private:
    void coordinator_callback(const Status& status, RuntimeProfile* profile, bool done);

    // Id of this query
    TUniqueId _query_id;
    // Id of this instance
    TUniqueId _fragment_instance_id;
    // Used to reoprt to coordinator which backend is over
    int _backend_num;
    ExecEnv* _exec_env;
    TNetworkAddress _coord_addr;

    PlanFragmentExecutor _executor;
    DateTimeValue _start_time;

    std::mutex _status_lock;
    Status _exec_status;

    bool _set_rsc_info;
    std::string _user;
    std::string _group;

    int _timeout_second;

    std::unique_ptr<std::thread> _exec_thread;
};

FragmentExecState::FragmentExecState(
        const TUniqueId& query_id,
        const TUniqueId& fragment_instance_id,
        int backend_num,
        ExecEnv* exec_env,
        const TNetworkAddress& coord_addr) :
            _query_id(query_id),
            _fragment_instance_id(fragment_instance_id),
            _backend_num(backend_num),
            _exec_env(exec_env),
            _coord_addr(coord_addr),
            _executor(exec_env, boost::bind<void>(
                    boost::mem_fn(&FragmentExecState::coordinator_callback), this, _1, _2, _3)),
            _set_rsc_info(false),
            _timeout_second(-1) {
    _start_time = DateTimeValue::local_time();
}

FragmentExecState::~FragmentExecState() {
}

Status FragmentExecState::prepare(const TExecPlanFragmentParams& params) {
    if (params.__isset.query_options) {
        _timeout_second = params.query_options.query_timeout;
    }

    if (params.__isset.resource_info) {
        set_group(params.resource_info);
    }

    return _executor.prepare(params);
}

static void register_cgroups(const std::string& user, const std::string& group) {
    TResourceInfo* new_info = new TResourceInfo();
    new_info->user = user;
    new_info->group = group;
    int ret = ResourceTls::set_resource_tls(new_info);
    if (ret != 0) {
        delete new_info;
        return;
    }
    CgroupsMgr::apply_cgroup(new_info->user, new_info->group);
}

Status FragmentExecState::execute() {
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        if (_set_rsc_info) {
            register_cgroups(_user, _group);
        } else {
            CgroupsMgr::apply_system_cgroup();
        }

        _executor.open();
        _executor.close();
    }
    DorisMetrics::fragment_requests_total.increment(1);
    DorisMetrics::fragment_request_duration_us.increment(duration_ns / 1000);
    return Status::OK();
}

Status FragmentExecState::cancel(const PPlanFragmentCancelReason& reason) {
    std::lock_guard<std::mutex> l(_status_lock);
    RETURN_IF_ERROR(_exec_status);
    if (reason == PPlanFragmentCancelReason::LIMIT_REACH) {
        _executor.set_is_report_on_cancel(false);
    }
    _executor.cancel();
    return Status::OK();
}

void FragmentExecState::callback(const Status& status, RuntimeProfile* profile, bool done) {
}

std::string FragmentExecState::to_http_path(const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port
        << "/api/_download_load?"
        << "token=" << _exec_env->token()
        << "&file=" << file_name;
    return url.str();
}

// There can only be one of these callbacks in-flight at any moment, because
// it is only invoked from the executor's reporting thread.
// Also, the reported status will always reflect the most recent execution status,
// including the final status when execution finishes.
void FragmentExecState::coordinator_callback(
        const Status& status,
        RuntimeProfile* profile,
        bool done) {
    DCHECK(status.ok() || done);  // if !status.ok() => done
    Status exec_status = update_status(status);

    Status coord_status;
    FrontendServiceConnection coord(
        _exec_env->frontend_client_cache(), _coord_addr, &coord_status);
    if (!coord_status.ok()) {
        std::stringstream ss;
        ss << "couldn't get a client for " << _coord_addr;
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
    profile->to_thrift(&params.profile);
    params.__isset.profile = true;

    RuntimeState* runtime_state = _executor.runtime_state();
    if (!runtime_state->output_files().empty()) {
        params.__isset.delta_urls = true;
        for (auto& it : runtime_state->output_files()) {
            params.delta_urls.push_back(to_http_path(it));
        }
    }
    if (runtime_state->num_rows_load_total() > 0 ||
            runtime_state->num_rows_load_filtered() > 0) {
        params.__isset.load_counters = true;
        // TODO(zc)
        static std::string s_dpp_normal_all = "dpp.norm.ALL";
        static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";

        params.load_counters.emplace(
            s_dpp_normal_all, std::to_string(runtime_state->num_rows_load_success()));
        params.load_counters.emplace(
            s_dpp_abnormal_all, std::to_string(runtime_state->num_rows_load_filtered()));
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
    DCHECK(runtime_state != NULL);

    // Send new errors to coordinator
    runtime_state->get_unreported_errors(&(params.error_log));
    params.__isset.error_log = (params.error_log.size() > 0);

    TReportExecStatusResult res;
    Status rpc_status;

    VLOG_ROW << "debug: reportExecStatus params is "
            << apache::thrift::ThriftDebugString(params).c_str();
    try {
        try {
            coord->reportExecStatus(res, params);
        } catch (TTransportException& e) {
            LOG(WARNING) << "Retrying ReportExecStatus: " << e.what();
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

FragmentMgr::FragmentMgr(ExecEnv* exec_env) :
        _exec_env(exec_env),
        _fragment_map(),
        _stop(false),
        _cancel_thread(std::bind<void>(&FragmentMgr::cancel_worker, this)),
        // TODO(zc): we need a better thread-pool
        // now one user can use all the thread pool, others have no resource.
        _thread_pool(config::fragment_pool_thread_num, config::fragment_pool_queue_size) {
}

FragmentMgr::~FragmentMgr() {
    // stop thread
    _stop = true;
    _cancel_thread.join();
    // Stop all the worker
    _thread_pool.drain_and_shutdown();

    // Only me can delete
    {
        std::lock_guard<std::mutex> lock(_lock);
        _fragment_map.clear();
    }
}

static void empty_function(PlanFragmentExecutor* exec) {
}

void FragmentMgr::exec_actual(
        std::shared_ptr<FragmentExecState> exec_state,
        FinishCallback cb) {
    exec_state->execute();

    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(exec_state->fragment_instance_id());
        if (iter != _fragment_map.end()) {
            _fragment_map.erase(iter);
        } else {
            // Impossible
            LOG(WARNING) << "missing entry in fragment exec state map: instance_id="
                << exec_state->fragment_instance_id();
        }
    }
    // Callback after remove from this id
    cb(exec_state->executor());
    // NOTE: 'exec_state' is desconstructed here without lock
}

Status FragmentMgr::exec_plan_fragment(
        const TExecPlanFragmentParams& params) {
    return exec_plan_fragment(params, std::bind<void>(&empty_function, std::placeholders::_1));
}

static void* fragment_executor(void* param) {
    ThreadPool::WorkFunction* func = (ThreadPool::WorkFunction*)param;
    (*func)();
    delete func;
    return nullptr;
}

Status FragmentMgr::exec_plan_fragment(
        const TExecPlanFragmentParams& params,
        FinishCallback cb) {
    const TUniqueId& fragment_instance_id = params.params.fragment_instance_id;
    std::shared_ptr<FragmentExecState> exec_state;
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(fragment_instance_id);
        if (iter != _fragment_map.end()) {
            // Duplicated
            return Status::OK();
        }
    }
    exec_state.reset(new FragmentExecState(
            params.params.query_id,
            fragment_instance_id,
            params.backend_num,
            _exec_env,
            params.coord));
    RETURN_IF_ERROR(exec_state->prepare(params));
    bool use_pool = true;
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(fragment_instance_id);
        if (iter != _fragment_map.end()) {
            // Duplicated
            return Status::InternalError("Double execute");
        }
        // register exec_state before starting exec thread
        _fragment_map.insert(std::make_pair(fragment_instance_id, exec_state));

        // Now, we the fragement is
        if (_fragment_map.size() >= config::fragment_pool_thread_num) {
            use_pool = false;
        }
    }

    if (use_pool) {
        if (!_thread_pool.offer(
                boost::bind<void>(&FragmentMgr::exec_actual, this, exec_state, cb))) {
            {
                // Remove the exec state added
                std::lock_guard<std::mutex> lock(_lock);
                _fragment_map.erase(fragment_instance_id);
            }
            return Status::InternalError("Put planfragment to failed.");
        }
    } else {
        pthread_t id;
        int ret = pthread_create(&id,
                       nullptr,
                       fragment_executor,
                       new ThreadPool::WorkFunction(
                           std::bind<void>(&FragmentMgr::exec_actual, this, exec_state, cb)));
        if (ret != 0) {
            std::string err_msg("Could not create thread.");
            err_msg.append(strerror(ret));
            err_msg.append(",");
            err_msg.append(std::to_string(ret));
            return Status::InternalError(err_msg);
        }
        pthread_detach(id);
    }

    return Status::OK();
}

Status FragmentMgr::cancel(const TUniqueId& id, const PPlanFragmentCancelReason& reason) {
    std::shared_ptr<FragmentExecState> exec_state;
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.find(id);
        if (iter == _fragment_map.end()) {
            // No match
            return Status::OK();
        }
        exec_state = iter->second;
    }
    exec_state->cancel(reason);

    return Status::OK();
}

//
void FragmentMgr::cancel_worker() {
    LOG(INFO) << "FragmentMgr cancel worker start working.";
    while (!_stop) {
        std::vector<TUniqueId> to_delete;
        DateTimeValue now = DateTimeValue::local_time();
        {
            std::lock_guard<std::mutex> lock(_lock);
            for (auto& it : _fragment_map) {
                if (it.second->is_timeout(now)) {
                    to_delete.push_back(it.second->fragment_instance_id());
                }
            }
        }
        for (auto& id : to_delete) {
            LOG(INFO) << "FragmentMgr cancel worker going to cancel fragment " << id;
            cancel(id);
        }

        // check every ten seconds
        sleep(1);
    }
    LOG(INFO) << "FragmentMgr cancel worker is going to exit.";
}

Status FragmentMgr::trigger_profile_report(const PTriggerProfileReportRequest* request) {
    if (request->instance_ids_size() > 0) {
        for (int i = 0; i < request->instance_ids_size(); i++) {
            const PUniqueId& p_fragment_id = request->instance_ids(i);
            TUniqueId id;
            id.__set_hi(p_fragment_id.hi());
            id.__set_lo(p_fragment_id.lo());
            {
                std::lock_guard<std::mutex> lock(_lock);
                auto iter = _fragment_map.find(id);
                if (iter != _fragment_map.end()) {
                    iter->second->executor()->report_profile_once();
                }
            }
        }
    } else {
        std::lock_guard<std::mutex> lock(_lock);
        auto iter = _fragment_map.begin();
        for (; iter != _fragment_map.end(); iter++) {
            iter->second->executor()->report_profile_once();
        }
    }
    return Status::OK();
}


void FragmentMgr::debug(std::stringstream& ss) {
    // Keep things simple
    std::lock_guard<std::mutex> lock(_lock);

    ss << "FragmentMgr have " << _fragment_map.size() << " jobs.\n";
    ss << "job_id\t\tstart_time\t\texecute_time(s)\n";
    DateTimeValue now = DateTimeValue::local_time();
    for (auto& it : _fragment_map) {
        ss << it.first
            << "\t" << it.second->start_time().debug_string()
            << "\t" << now.second_diff(it.second->start_time())
            << "\n";
    }
}

}

