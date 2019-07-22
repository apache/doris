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
#include <memory>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/processor/TMultiplexedProcessor.h>

#include "common/logging.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/TDorisExternalService.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/DorisExternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/storage_engine.h"
#include "runtime/fragment_mgr.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/pull_load_task_mgr.h"
#include "runtime/export_task_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/primitive_type.h"
#include "service/backend_options.h"
#include "util/blocking_queue.hpp"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
#include "util/url_coding.h"
#include "util/network_util.h"
#include "util/thrift_util.h"
#include "util/thrift_server.h"



namespace doris {

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::TMultiplexedProcessor;
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
    _is_stop = false;
    LOG(INFO) << "DorisExternalService ctor init ";
    _scan_context_gc_interval = doris::config::scan_context_gc_interval;
    // start the reaper thread for gc the expired context
    _keep_alive_reaper.reset(
            new boost::thread(
                    boost::bind<void>(boost::mem_fn(&BackendService::expired_context_gc), this)));
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

/*
 * 1. validate user privilege (todo)
 * 2. resolve opaqued_query_plan to thrift structure
 * 3. build TExecPlanFragmentParams
 * 4. FragmentMgr#exec_plan_fragment
 */
void BackendService::open(TScanOpenResult& result_, const TScanOpenParams& params) {
    LOG(INFO) << "BackendService open ";
    std::string opaqued_query_plan = params.opaqued_query_plan;
    std::string query_plan_info;
    TStatus t_status;
    // base64 decode query plan
    if (!base64_decode(opaqued_query_plan, &query_plan_info)) {
        LOG(ERROR) << "open context error: base64_decode decode opaqued_query_plan failure";
        t_status.status_code = TStatusCode::INVALID_ARGUMENT;
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info << " validate error, should not be modified after returned Doris FE processed";
        t_status.error_msgs.push_back(msg.str());
        result_.status = t_status;
        return;
    }
    TQueryPlanInfo t_query_plan_info;
    const uint8_t* buf = (const uint8_t*)query_plan_info.data();
    uint32_t len = query_plan_info.size();
    // deserialize TQueryPlanInfo
    auto st = deserialize_thrift_msg(buf, &len, false, &t_query_plan_info);
    if (!st.ok()) {
        LOG(ERROR) << "open context error: deserialize TQueryPlanInfo failure";
        t_status.status_code = TStatusCode::INVALID_ARGUMENT;
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info << " deserialize error, should not be modified after returned Doris FE processed";
        t_status.error_msgs.push_back(msg.str());
        result_.status = t_status;
        return;
    }

    // set up desc tbl
    DescriptorTbl* desc_tbl = NULL;
    ObjectPool obj_pool;
    st = DescriptorTbl::create(&obj_pool, t_query_plan_info.desc_tbl, &desc_tbl);
    if (!st.ok()) {
        LOG(ERROR) << "open context error: extract DescriptorTbl failure";
        t_status.status_code = TStatusCode::INVALID_ARGUMENT;
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info << " create DescriptorTbl error, should not be modified after returned Doris FE processed";
        t_status.error_msgs.push_back(msg.str());
        result_.status = t_status;
        return;
    }
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    if (tuple_desc == nullptr) {
        LOG(ERROR) << "open context error: extract TupleDescriptor failure";
        t_status.status_code = TStatusCode::INVALID_ARGUMENT;
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info << " get  TupleDescriptor error, should not be modified after returned Doris FE processed";
        t_status.error_msgs.push_back(msg.str());
        result_.status = t_status;
        return;
    }
    // process selected columns form slots
    std::vector<TScanColumnDesc> selected_columns;
    for (const SlotDescriptor* slot : tuple_desc->slots()) {
        TScanColumnDesc col;
        col.__set_name(slot->col_name());
        col.__set_type(to_thrift(slot->type().type));
        selected_columns.emplace_back(std::move(col));
    }

    LOG(INFO) << "BackendService execute open()  TQueryPlanInfo: " << apache::thrift::ThriftDebugString(t_query_plan_info);
    // assign the param used to execute PlanFragment
    TExecPlanFragmentParams exec_fragment_params;
    exec_fragment_params.protocol_version = (PaloInternalServiceVersion::type)0;
    exec_fragment_params.__set_fragment(t_query_plan_info.plan_fragment);
    exec_fragment_params.__set_desc_tbl(t_query_plan_info.desc_tbl);

    // assign the param used for executing of PlanFragment-self
    TPlanFragmentExecParams fragment_exec_params;
    fragment_exec_params.query_id = t_query_plan_info.query_id;
    fragment_exec_params.fragment_instance_id = generate_uuid();
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
            scan_range.version_hash = std::to_string(info.version_hash);
            scan_range.schema_hash = std::to_string(info.schema_hash);
            scan_range.hosts.push_back(address);
        } else {
            t_status.status_code = TStatusCode::NOT_FOUND;
            std::stringstream msg;
            msg << "tablet_id: " << tablet_id << " not found";
            t_status.error_msgs.push_back(msg.str());
            result_.status = t_status;
            return;
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
    exec_fragment_params.__set_query_options(query_options);
    std::string context_id = generate_uuid_string();
    std::shared_ptr<Context> context(new Context(fragment_exec_params.fragment_instance_id, 0));
    context->last_access_time  = time(NULL);
    {
        boost::lock_guard<boost::mutex> l(_lock);        
        _active_contexts.insert(std::make_pair(context_id, context));
    }
    // start the scan procedure
    Status exec_st = _exec_env->fragment_mgr()->exec_plan_fragment(exec_fragment_params);
    VLOG_ROW << "external exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(exec_fragment_params).c_str();
    //return status
    t_status.status_code = TStatusCode::OK;
    result_.status = t_status;
    result_.__set_context_id(context_id);
    result_.__set_selected_columns(selected_columns);
}

// fetch result from polling the queue, should always maintaince the context offset, otherwise inconsistent result
void BackendService::getNext(TScanBatchResult& result_, const TScanNextBatchParams& params) {
    std::string context_id = params.context_id;
    u_int64_t offset = params.offset;
    TStatus t_status;
    std::shared_ptr<Context> context;
    {
        boost::lock_guard<boost::mutex> l(_lock);        
        auto iter = _active_contexts.find(context_id);
        if (iter != _active_contexts.end()) {
            context = iter->second;
        } else {
            LOG(ERROR) << "getNext error: context id [ " << context_id << " ] not found";
            t_status.status_code = TStatusCode::NOT_FOUND;
            std::stringstream msg;
            msg << "context_id: " << context_id << " not found"; 
            t_status.error_msgs.push_back(msg.str());
            result_.status = t_status;
        }
    }
    if (offset != context->offset) {
        LOG(ERROR) << "getNext error: context offset [" <<  context->offset<<" ]" << " ,client offset [ " << offset << " ]";
        // invalid offset
        t_status.status_code = TStatusCode::NOT_FOUND;
        std::stringstream msg;
        msg << "context_id: " << context_id << " send offset: " << offset << "diff with context offset: " << context->offset; 
        t_status.error_msgs.push_back(msg.str());
        result_.status = t_status;
    } else {
        // during accessing, should disabled last_access_time
        context->last_access_time = -1;
        TUniqueId fragment_instance_id = context->fragment_instance_id;
        std::shared_ptr<TScanRowBatch> rows_per_col;
        bool eos;
        Status st = _exec_env->result_queue_mgr()->fetch_result(fragment_instance_id, &rows_per_col, &eos);
        if (st.ok()) {
            result_.__set_eos(eos);
            // when eos = true  rows_per_col = nullptr
            if (!eos) {
                result_.__set_rows(*rows_per_col);
                context->offset += (*rows_per_col).num_rows;
            }
            t_status.status_code = TStatusCode::OK;
            result_.status = t_status;
        } else {
            result_.status = t_status;
        }
    }
    context->last_access_time = time(NULL);
}

void BackendService::close(TScanCloseResult& result_, const TScanCloseParams& params) {
    std::string context_id = params.context_id;
     TStatus t_status;
    std::shared_ptr<Context> context;
    {
        boost::lock_guard<boost::mutex> l(_lock);        
        auto iter = _active_contexts.find(context_id);
        if (iter != _active_contexts.end()) {
            context = iter->second;
        } else {
            t_status.status_code = TStatusCode::NOT_FOUND;
            std::stringstream msg;
            msg << "context_id: " << context_id << " not found"; 
            t_status.error_msgs.push_back(msg.str());
            result_.status = t_status;
            return;
        }
    }
    {
        // maybe do not this context-local-lock
        boost::lock_guard<boost::mutex> l(context->_local_lock);
        // first cancel the fragment instance, just ignore return status
        _exec_env->fragment_mgr()->cancel(context->fragment_instance_id);
        // clear the fragment instance's related result queue
        _exec_env->result_queue_mgr()->cancel(context->fragment_instance_id);
    }
    t_status.status_code = TStatusCode::OK;
    result_.status = t_status;
}

// schdule gc proceess per 1min
void BackendService::expired_context_gc() {
    while (!_is_stop) {
        boost::this_thread::sleep(boost::posix_time::minutes(_scan_context_gc_interval));
        time_t current_time = time(NULL);
        for(auto iter = _active_contexts.begin(); iter != _active_contexts.end(); ) {
            TUniqueId fragment_instance_id = iter->second->fragment_instance_id;
            auto context = iter->second;
            {
                // This lock maybe should deleted, all right? 
                // here we do not need lock guard in fact
                boost::lock_guard<boost::mutex> l(context->_local_lock);        
                // being processed or timeout is disabled
                if (context->last_access_time == -1) {
                    continue;
                }
                // free context since last accesstime is 5 minutes ago
                if (current_time - context->last_access_time > 5 * 60) {
                    // must cancel the fragment instance, otherwise return thrift transport TTransportException
                    _exec_env->fragment_mgr()->cancel(context->fragment_instance_id);
                    _exec_env->result_queue_mgr()->cancel(context->fragment_instance_id);
                    iter = _active_contexts.erase(iter);
                }
            }
        }
    }
}

} // namespace doris
