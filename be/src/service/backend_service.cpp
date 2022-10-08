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

#include <arrow/record_batch.h>
#include <gperftools/heap-profiler.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/processor/TMultiplexedProcessor.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <map>
#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gen_cpp/DorisExternalService_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/TDorisExternalService.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "olap/storage_engine.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/export_task_mgr.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/primitive_type.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/stream_load/stream_load_context.h"
#include "service/backend_options.h"
#include "util/arrow/row_batch.h"
#include "util/blocking_queue.hpp"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/network_util.h"
#include "util/thrift_server.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
#include "util/url_coding.h"

namespace doris {

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::TMultiplexedProcessor;
using apache::thrift::transport::TTransportException;
using apache::thrift::concurrency::ThreadFactory;

BackendService::BackendService(ExecEnv* exec_env)
        : _exec_env(exec_env), _agent_server(new AgentServer(exec_env, *exec_env->master_info())) {}

Status BackendService::create_service(ExecEnv* exec_env, int port, ThriftServer** server) {
    std::shared_ptr<BackendService> handler(new BackendService(exec_env));
    // TODO: do we want a BoostThreadFactory?
    // TODO: we want separate thread factories here, so that fe requests can't starve
    // be requests
    std::shared_ptr<ThreadFactory> thread_factory(new ThreadFactory());

    std::shared_ptr<TProcessor> be_processor(new BackendServiceProcessor(handler));

    *server = new ThriftServer("backend", be_processor, port, config::be_service_threads);

    LOG(INFO) << "Doris BackendService listening on " << port;

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
             << " node_id=" << params.dest_node_id << " #rows=" << params.row_batch.num_rows
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

void BackendService::fetch_data(TFetchDataResult& return_val, const TFetchDataParams& params) {
    // maybe hang in this function
    Status status = _exec_env->result_mgr()->fetch_data(params.fragment_instance_id, &return_val);
    status.set_t_status(&return_val);
}

void BackendService::submit_export_task(TStatus& t_status, const TExportTaskRequest& request) {
    //    VLOG_ROW << "submit_export_task. request  is "
    //            << apache::thrift::ThriftDebugString(request).c_str();
    //
    //    Status status = _exec_env->export_task_mgr()->start_task(request);
    //    if (status.ok()) {
    //        VLOG_RPC << "start export task successful id="
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
    StorageEngine::instance()->tablet_manager()->get_tablet_stat(&result);
}

int64_t BackendService::get_trash_used_capacity() {
    int64_t result = 0;

    std::vector<DataDirInfo> data_dir_infos;
    StorageEngine::instance()->get_all_data_dir_info(&data_dir_infos, false /*do not update */);

    for (const auto& root_path_info : data_dir_infos) {
        auto trash_path = fmt::format("{}/{}", root_path_info.path, TRASH_PREFIX);
        result += StorageEngine::instance()->get_file_or_directory_size(trash_path);
    }
    return result;
}

void BackendService::get_disk_trash_used_capacity(std::vector<TDiskTrashInfo>& diskTrashInfos) {
    std::vector<DataDirInfo> data_dir_infos;
    StorageEngine::instance()->get_all_data_dir_info(&data_dir_infos, false /*do not update */);

    for (const auto& root_path_info : data_dir_infos) {
        TDiskTrashInfo diskTrashInfo;

        diskTrashInfo.__set_root_path(root_path_info.path);

        diskTrashInfo.__set_state(root_path_info.is_used ? "ONLINE" : "OFFLINE");

        auto trash_path = fmt::format("{}/{}", root_path_info.path, TRASH_PREFIX);
        diskTrashInfo.__set_trash_used_capacity(
                StorageEngine::instance()->get_file_or_directory_size(trash_path));

        diskTrashInfos.push_back(diskTrashInfo);
    }
}

void BackendService::submit_routine_load_task(TStatus& t_status,
                                              const std::vector<TRoutineLoadTask>& tasks) {
    for (auto& task : tasks) {
        Status st = _exec_env->routine_load_task_executor()->submit_task(task);
        if (!st.ok()) {
            LOG(WARNING) << "failed to submit routine load task. job id: " << task.job_id
                         << " task id: " << task.id;
            return st.to_thrift(&t_status);
        }
    }

    return Status::OK().to_thrift(&t_status);
}

/*
 * 1. validate user privilege (todo)
 * 2. FragmentMgr#exec_plan_fragment
 */
void BackendService::open_scanner(TScanOpenResult& result_, const TScanOpenParams& params) {
    TStatus t_status;
    TUniqueId fragment_instance_id = generate_uuid();
    std::shared_ptr<ScanContext> p_context;
    _exec_env->external_scan_context_mgr()->create_scan_context(&p_context);
    p_context->fragment_instance_id = fragment_instance_id;
    p_context->offset = 0;
    p_context->last_access_time = time(nullptr);
    if (params.__isset.keep_alive_min) {
        p_context->keep_alive_min = params.keep_alive_min;
    } else {
        p_context->keep_alive_min = 5;
    }
    std::vector<TScanColumnDesc> selected_columns;
    // start the scan procedure
    Status exec_st = _exec_env->fragment_mgr()->exec_external_plan_fragment(
            params, fragment_instance_id, &selected_columns);
    exec_st.to_thrift(&t_status);
    //return status
    // t_status.status_code = TStatusCode::OK;
    result_.status = t_status;
    result_.__set_context_id(p_context->context_id);
    result_.__set_selected_columns(selected_columns);
}

// fetch result from polling the queue, should always maintain the context offset, otherwise inconsistent result
void BackendService::get_next(TScanBatchResult& result_, const TScanNextBatchParams& params) {
    std::string context_id = params.context_id;
    u_int64_t offset = params.offset;
    TStatus t_status;
    std::shared_ptr<ScanContext> context;
    Status st = _exec_env->external_scan_context_mgr()->get_scan_context(context_id, &context);
    if (!st.ok()) {
        st.to_thrift(&t_status);
        result_.status = t_status;
        return;
    }
    if (offset != context->offset) {
        LOG(ERROR) << "getNext error: context offset [" << context->offset << " ]"
                   << " ,client offset [ " << offset << " ]";
        // invalid offset
        t_status.status_code = TStatusCode::NOT_FOUND;
        t_status.error_msgs.push_back(
                strings::Substitute("context_id=$0, send_offset=$1, context_offset=$2", context_id,
                                    offset, context->offset));
        result_.status = t_status;
    } else {
        // during accessing, should disabled last_access_time
        context->last_access_time = -1;
        TUniqueId fragment_instance_id = context->fragment_instance_id;
        std::shared_ptr<arrow::RecordBatch> record_batch;
        bool eos;

        st = _exec_env->result_queue_mgr()->fetch_result(fragment_instance_id, &record_batch, &eos);
        if (st.ok()) {
            result_.__set_eos(eos);
            if (!eos) {
                std::string record_batch_str;
                st = serialize_record_batch(*record_batch, &record_batch_str);
                st.to_thrift(&t_status);
                if (st.ok()) {
                    // avoid copy large string
                    result_.rows = std::move(record_batch_str);
                    // set __isset
                    result_.__isset.rows = true;
                    context->offset += record_batch->num_rows();
                }
            }
        } else {
            LOG(WARNING) << "fragment_instance_id [" << print_id(fragment_instance_id)
                         << "] fetch result status [" << st.to_string() + "]";
            st.to_thrift(&t_status);
            result_.status = t_status;
        }
    }
    context->last_access_time = time(nullptr);
}

void BackendService::close_scanner(TScanCloseResult& result_, const TScanCloseParams& params) {
    std::string context_id = params.context_id;
    TStatus t_status;
    Status st = _exec_env->external_scan_context_mgr()->clear_scan_context(context_id);
    st.to_thrift(&t_status);
    result_.status = t_status;
}

void BackendService::get_stream_load_record(TStreamLoadRecordResult& result,
                                            const int64_t last_stream_record_time) {
    auto stream_load_recorder = StorageEngine::instance()->get_stream_load_recorder();
    if (stream_load_recorder != nullptr) {
        std::map<std::string, std::string> records;
        auto st = stream_load_recorder->get_batch(std::to_string(last_stream_record_time),
                                                  config::stream_load_record_batch_size, &records);
        if (st.ok()) {
            LOG(INFO) << "get_batch stream_load_record rocksdb successfully. records size: "
                      << records.size()
                      << ", last_stream_load_timestamp: " << last_stream_record_time;
            std::map<std::string, TStreamLoadRecord> stream_load_record_batch;
            std::map<std::string, std::string>::iterator it = records.begin();
            for (; it != records.end(); ++it) {
                TStreamLoadRecord stream_load_item;
                StreamLoadContext::parse_stream_load_record(it->second, stream_load_item);
                stream_load_record_batch.emplace(it->first.c_str(), stream_load_item);
            }
            result.__set_stream_load_record(stream_load_record_batch);
        }
    } else {
        LOG(WARNING) << "stream_load_recorder is null.";
    }
}

void BackendService::clean_trash() {
    StorageEngine::instance()->start_trash_sweep(nullptr, true);
}

void BackendService::check_storage_format(TCheckStorageFormatResult& result) {
    StorageEngine::instance()->tablet_manager()->get_all_tablets_storage_format(&result);
}
} // namespace doris
