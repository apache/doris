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

#include "service/backend_service.h"

#include <boost/shared_ptr.hpp>
#include <gperftools/heap-profiler.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/concurrency/PosixThreadFactory.h>

#include "olap/storage_engine.h"
#include "service/backend_options.h"
#include "util/network_util.h"
#include "util/thrift_util.h"
#include "util/thrift_server.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "runtime/fragment_mgr.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/pull_load_task_mgr.h"
#include "runtime/export_task_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"

namespace doris {

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::transport::TTransportException;
using apache::thrift::concurrency::ThreadFactory;
using apache::thrift::concurrency::PosixThreadFactory;

BackendService::BackendService(ExecEnv* exec_env) :
        _exec_env(exec_env),
        _agent_server(new AgentServer(exec_env, *exec_env->master_info())) {
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
    // tcmalloc and address sanitizer can not be used together
    if (!config::heap_profile_dir.empty()) {
        HeapProfilerStart(config::heap_profile_dir.c_str());
    }
#endif
    char buf[64];
    DateTimeValue value = DateTimeValue::local_time();
    value.to_string(buf);
}

Status BackendService::create_service(ExecEnv* exec_env, int port, ThriftServer** server) {
    boost::shared_ptr<BackendService> handler(new BackendService(exec_env));

    // TODO: do we want a BoostThreadFactory?
    // TODO: we want separate thread factories here, so that fe requests can't starve
    // be requests
    boost::shared_ptr<ThreadFactory> thread_factory(new PosixThreadFactory());

    boost::shared_ptr<TProcessor> be_processor(new BackendServiceProcessor(handler));
    *server = new ThriftServer("backend",
                               be_processor,
                               port,
                               exec_env->metrics(),
                               config::be_service_threads);

    LOG(INFO) << "DorisInternalService listening on " << port;

    return Status::OK();
}

void BackendService::exec_plan_fragment(TExecPlanFragmentResult& return_val,
                                        const TExecPlanFragmentParams& params) {
    LOG(INFO) << "exec_plan_fragment() instance_id=" << params.params.fragment_instance_id
        << " coord=" << params.coord << " backend#=" << params.backend_num;
    VLOG_ROW << "exec_plan_fragment params is "
            << apache::thrift::ThriftDebugString(params).c_str();
    start_plan_fragment_execution(params).set_t_status(&return_val);
}

Status BackendService::start_plan_fragment_execution(const TExecPlanFragmentParams& exec_params) {
    if (!exec_params.fragment.__isset.output_sink) {
        return Status::InternalError("missing sink in plan fragment");
    }
    return _exec_env->fragment_mgr()->exec_plan_fragment(exec_params);
}

void BackendService::cancel_plan_fragment(TCancelPlanFragmentResult& return_val,
                                          const TCancelPlanFragmentParams& params) {
    LOG(INFO) << "cancel_plan_fragment(): instance_id=" << params.fragment_instance_id;
    _exec_env->fragment_mgr()->cancel(params.fragment_instance_id).set_t_status(&return_val);
}

void BackendService::transmit_data(TTransmitDataResult& return_val,
                                   const TTransmitDataParams& params) {
    VLOG_ROW << "transmit_data(): instance_id=" << params.dest_fragment_instance_id
             << " node_id=" << params.dest_node_id
             << " #rows=" << params.row_batch.num_rows
             << " eos=" << (params.eos ? "true" : "false");
    // VLOG_ROW << "transmit_data params: " << apache::thrift::ThriftDebugString(params).c_str();

    if (params.__isset.packet_seq) {
        return_val.__set_packet_seq(params.packet_seq);
        return_val.__set_dest_fragment_instance_id(params.dest_fragment_instance_id);
        return_val.__set_dest_node_id(params.dest_node_id);
    }

    // TODO: fix Thrift so we can simply take ownership of thrift_batch instead
    // of having to copy its data
    if (params.row_batch.num_rows > 0) {
        // Status status = _exec_env->stream_mgr()->add_data(
        //         params.dest_fragment_instance_id,
        //         params.dest_node_id,
        //         params.row_batch,
        //         params.sender_id);
        // status.set_t_status(&return_val);

        // if (!status.ok()) {
        //     // should we close the channel here as well?
        //     return;
        // }
    }

    if (params.eos) {
        // Status status = _exec_env->stream_mgr()->close_sender(
        //        params.dest_fragment_instance_id,
        //        params.dest_node_id,
        //        params.sender_id,
        //        params.be_number);
        //VLOG_ROW << "params.eos: " << (params.eos ? "true" : "false")
        //        << " close_sender status: " << status.get_error_msg();
        //status.set_t_status(&return_val);
    }
}

void BackendService::fetch_data(TFetchDataResult& return_val,
                                const TFetchDataParams& params) {
    // maybe hang in this function
    Status status = _exec_env->result_mgr()->fetch_data(params.fragment_instance_id, &return_val);
    status.set_t_status(&return_val);
}

void BackendService::register_pull_load_task(
        TStatus& t_status, const TUniqueId& id, int num_senders) {
    Status status = _exec_env->pull_load_task_mgr()->register_task(id, num_senders);
    status.to_thrift(&t_status);
}

void BackendService::deregister_pull_load_task(TStatus& t_status, const TUniqueId& id) {
    Status status = _exec_env->pull_load_task_mgr()->deregister_task(id);
    status.to_thrift(&t_status);
}

void BackendService::report_pull_load_sub_task_info(
        TStatus& t_status, const TPullLoadSubTaskInfo& task_info) {
    Status status = _exec_env->pull_load_task_mgr()->report_sub_task_info(task_info);
    status.to_thrift(&t_status);
}

void BackendService::fetch_pull_load_task_info(
        TFetchPullLoadTaskInfoResult& result, const TUniqueId& id) {
    Status status = _exec_env->pull_load_task_mgr()->fetch_task_info(id, &result);
    status.to_thrift(&result.status);
}

void BackendService::fetch_all_pull_load_task_infos(
        TFetchAllPullLoadTaskInfosResult& result) {
    Status status = _exec_env->pull_load_task_mgr()->fetch_all_task_infos(&result);
    status.to_thrift(&result.status);
}

void BackendService::submit_export_task(TStatus& t_status, const TExportTaskRequest& request) {
//    VLOG_ROW << "submit_export_task. request  is "
//            << apache::thrift::ThriftDebugString(request).c_str();
//
//    Status status = _exec_env->export_task_mgr()->start_task(request);
//    if (status.ok()) {
//        VLOG_RPC << "start export task successfull id="
//            << request.params.params.fragment_instance_id;
//    } else {
//        VLOG_RPC << "start export task failed id="
//            << request.params.params.fragment_instance_id
//            << " and err_msg=" << status.get_error_msg();
//    }
//    status.to_thrift(&t_status);
}

void BackendService::get_export_status(TExportStatusResult& result, const TUniqueId& task_id) {
//    VLOG_ROW << "get_export_status. task_id  is " << task_id;
//    Status status = _exec_env->export_task_mgr()->get_task_state(task_id, &result);
//    if (!status.ok()) {
//        LOG(WARNING) << "get export task state failed. [id=" << task_id << "]";
//    } else {
//        VLOG_RPC << "get export task state successful. [id=" << task_id
//            << ",status=" << result.status.status_code
//            << ",state=" << result.state
//            << ",files=";
//        for (auto& item : result.files) {
//            VLOG_RPC << item << ", ";
//        }
//        VLOG_RPC << "]";
//    }
//    status.to_thrift(&result.status);
//    result.__set_state(TExportState::RUNNING);
}

void BackendService::erase_export_task(TStatus& t_status, const TUniqueId& task_id) {
//    VLOG_ROW << "erase_export_task. task_id  is " << task_id;
//    Status status = _exec_env->export_task_mgr()->erase_task(task_id);
//    if (!status.ok()) {
//        LOG(WARNING) << "delete export task failed. because "
//            << status.get_error_msg() << " with task_id " << task_id;
//    } else {
//        VLOG_RPC << "delete export task successful with task_id " << task_id;
//    }
//    status.to_thrift(&t_status);
}

void BackendService::get_tablet_stat(TTabletStatResult& result) {
    StorageEngine::instance()->tablet_manager()->get_tablet_stat(result);
}

void BackendService::submit_routine_load_task(
        TStatus& t_status, const std::vector<TRoutineLoadTask>& tasks) {
    
    for (auto& task : tasks) {
        Status st = _exec_env->routine_load_task_executor()->submit_task(task);
        if (!st.ok()) {
            LOG(WARNING) << "failed to submit routine load task. job id: " <<  task.job_id
                    << " task id: " << task.id;
        }
    }

    // we do not care about each task's submit result. just return OK.
    // FE will handle the failure.
    return Status::OK().to_thrift(&t_status);
}

} // namespace doris
