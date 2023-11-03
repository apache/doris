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
#include <fmt/format.h>
#include <gen_cpp/BackendService.h>
#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/Data_types.h>
#include <gen_cpp/DorisExternalService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Planner_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <sys/types.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <time.h>

#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/txn_manager.h"
#include "runtime/exec_env.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "util/arrow/row_batch.h"
#include "util/defer_op.h"
#include "util/thrift_server.h"
#include "util/uid_util.h"

namespace apache {
namespace thrift {
class TException;
class TMultiplexedProcessor;
class TProcessor;
namespace transport {
class TTransportException;
} // namespace transport
} // namespace thrift
} // namespace apache

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
    LOG(INFO) << "exec_plan_fragment() instance_id=" << print_id(params.params.fragment_instance_id)
              << " coord=" << params.coord << " backend#=" << params.backend_num;
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
    LOG(INFO) << "cancel_plan_fragment(): instance_id=" << print_id(params.fragment_instance_id);
    _exec_env->fragment_mgr()->cancel(params.fragment_instance_id);
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
        //        << " close_sender status: " << status;
        //status.set_t_status(&return_val);
    }
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
    //            << " and err_msg=" << status;
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
    //            << status << " with task_id " << task_id;
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

    // uses excute sql `show trash`, then update backend trash capacity too.
    StorageEngine::instance()->notify_listener(TaskWorkerPool::TaskWorkerType::REPORT_DISK_STATE);

    for (const auto& root_path_info : data_dir_infos) {
        result += root_path_info.trash_used_capacity;
    }

    return result;
}

void BackendService::get_disk_trash_used_capacity(std::vector<TDiskTrashInfo>& diskTrashInfos) {
    std::vector<DataDirInfo> data_dir_infos;
    StorageEngine::instance()->get_all_data_dir_info(&data_dir_infos, false /*do not update */);

    // uses excute sql `show trash on <be>`, then update backend trash capacity too.
    StorageEngine::instance()->notify_listener(TaskWorkerPool::TaskWorkerType::REPORT_DISK_STATE);

    for (const auto& root_path_info : data_dir_infos) {
        TDiskTrashInfo diskTrashInfo;
        diskTrashInfo.__set_root_path(root_path_info.path);
        diskTrashInfo.__set_state(root_path_info.is_used ? "ONLINE" : "OFFLINE");
        diskTrashInfo.__set_trash_used_capacity(root_path_info.trash_used_capacity);
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
    StorageEngine::instance()->notify_listener(TaskWorkerPool::TaskWorkerType::REPORT_DISK_STATE);
}

void BackendService::check_storage_format(TCheckStorageFormatResult& result) {
    StorageEngine::instance()->tablet_manager()->get_all_tablets_storage_format(&result);
}

void BackendService::ingest_binlog(TIngestBinlogResult& result,
                                   const TIngestBinlogRequest& request) {
    LOG(INFO) << "ingest binlog. request: " << apache::thrift::ThriftDebugString(request);

    constexpr uint64_t kMaxTimeoutMs = 1000;

    TStatus tstatus;
    Defer defer {[&result, &tstatus]() {
        result.__set_status(tstatus);
        LOG(INFO) << "ingest binlog. result: " << apache::thrift::ThriftDebugString(result);
    }};

    auto set_tstatus = [&tstatus](TStatusCode::type code, std::string error_msg) {
        tstatus.__set_status_code(code);
        tstatus.__isset.error_msgs = true;
        tstatus.error_msgs.push_back(std::move(error_msg));
    };

    if (!config::enable_feature_binlog) {
        set_tstatus(TStatusCode::RUNTIME_ERROR, "enable feature binlog is false");
        return;
    }

    /// Check args: txn_id, remote_tablet_id, binlog_version, remote_host, remote_port, partition_id, load_id
    if (!request.__isset.txn_id) {
        auto error_msg = "txn_id is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.remote_tablet_id) {
        auto error_msg = "remote_tablet_id is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.binlog_version) {
        auto error_msg = "binlog_version is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.remote_host) {
        auto error_msg = "remote_host is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.remote_port) {
        auto error_msg = "remote_port is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.partition_id) {
        auto error_msg = "partition_id is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }
    if (!request.__isset.load_id) {
        auto error_msg = "load_id is empty";
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::ANALYSIS_ERROR, error_msg);
        return;
    }

    auto txn_id = request.txn_id;
    // Step 1: get local tablet
    auto const& local_tablet_id = request.local_tablet_id;
    auto local_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(local_tablet_id);
    if (local_tablet == nullptr) {
        auto error_msg = fmt::format("tablet {} not found", local_tablet_id);
        LOG(WARNING) << error_msg;
        set_tstatus(TStatusCode::TABLET_MISSING, std::move(error_msg));
        return;
    }

    // Step 2: check txn, create txn, prepare_txn will check it
    auto partition_id = request.partition_id;
    auto& load_id = request.load_id;
    auto is_ingrest = true;
    PUniqueId p_load_id;
    p_load_id.set_hi(load_id.hi);
    p_load_id.set_lo(load_id.lo);
    auto status = StorageEngine::instance()->txn_manager()->prepare_txn(
            partition_id, local_tablet, txn_id, p_load_id, is_ingrest);
    if (!status.ok()) {
        LOG(WARNING) << "prepare txn failed. txn_id=" << txn_id
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }

    // Step 3: get binlog info
    auto binlog_api_url = fmt::format("http://{}:{}/api/_binlog/_download", request.remote_host,
                                      request.remote_port);
    constexpr int max_retry = 3;

    auto get_binlog_info_url =
            fmt::format("{}?method={}&tablet_id={}&binlog_version={}", binlog_api_url,
                        "get_binlog_info", request.remote_tablet_id, request.binlog_version);
    std::string binlog_info;
    auto get_binlog_info_cb = [&get_binlog_info_url, &binlog_info](HttpClient* client) {
        RETURN_IF_ERROR(client->init(get_binlog_info_url));
        client->set_timeout_ms(kMaxTimeoutMs);
        return client->execute(&binlog_info);
    };
    status = HttpClient::execute_with_retry(max_retry, 1, get_binlog_info_cb);
    if (!status.ok()) {
        LOG(WARNING) << "failed to get binlog info from " << get_binlog_info_url
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }

    std::vector<std::string> binlog_info_parts = strings::Split(binlog_info, ":");
    // TODO(Drogon): check binlog info content is right
    DCHECK(binlog_info_parts.size() == 2);
    const std::string& remote_rowset_id = binlog_info_parts[0];
    int64_t num_segments = std::stoll(binlog_info_parts[1]);

    // Step 4: get rowset meta
    auto get_rowset_meta_url = fmt::format(
            "{}?method={}&tablet_id={}&rowset_id={}&binlog_version={}", binlog_api_url,
            "get_rowset_meta", request.remote_tablet_id, remote_rowset_id, request.binlog_version);
    std::string rowset_meta_str;
    auto get_rowset_meta_cb = [&get_rowset_meta_url, &rowset_meta_str](HttpClient* client) {
        RETURN_IF_ERROR(client->init(get_rowset_meta_url));
        client->set_timeout_ms(kMaxTimeoutMs);
        return client->execute(&rowset_meta_str);
    };
    status = HttpClient::execute_with_retry(max_retry, 1, get_rowset_meta_cb);
    if (!status.ok()) {
        LOG(WARNING) << "failed to get rowset meta from " << get_rowset_meta_url
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }
    RowsetMetaPB rowset_meta_pb;
    if (!rowset_meta_pb.ParseFromString(rowset_meta_str)) {
        LOG(WARNING) << "failed to parse rowset meta from " << get_rowset_meta_url;
        status = Status::InternalError("failed to parse rowset meta");
        status.to_thrift(&tstatus);
        return;
    }
    // rewrite rowset meta
    rowset_meta_pb.set_tablet_id(local_tablet_id);
    rowset_meta_pb.set_partition_id(partition_id);
    rowset_meta_pb.set_tablet_schema_hash(local_tablet->tablet_meta()->schema_hash());
    rowset_meta_pb.set_txn_id(txn_id);
    rowset_meta_pb.set_rowset_state(RowsetStatePB::COMMITTED);
    auto rowset_meta = std::make_shared<RowsetMeta>();
    if (!rowset_meta->init_from_pb(rowset_meta_pb)) {
        LOG(WARNING) << "failed to init rowset meta from " << get_rowset_meta_url;
        status = Status::InternalError("failed to init rowset meta");
        status.to_thrift(&tstatus);
        return;
    }
    RowsetId new_rowset_id = StorageEngine::instance()->next_rowset_id();
    rowset_meta->set_rowset_id(new_rowset_id);
    rowset_meta->set_tablet_uid(local_tablet->tablet_uid());

    // Step 5: get all segment files
    // Step 5.1: get all segment files size
    std::vector<std::string> segment_file_urls;
    segment_file_urls.reserve(num_segments);
    std::vector<uint64_t> segment_file_sizes;
    segment_file_sizes.reserve(num_segments);
    for (int64_t segment_index = 0; segment_index < num_segments; ++segment_index) {
        auto get_segment_file_size_url = fmt::format(
                "{}?method={}&tablet_id={}&rowset_id={}&segment_index={}", binlog_api_url,
                "get_segment_file", request.remote_tablet_id, remote_rowset_id, segment_index);
        uint64_t segment_file_size;
        auto get_segment_file_size_cb = [&get_segment_file_size_url,
                                         &segment_file_size](HttpClient* client) {
            RETURN_IF_ERROR(client->init(get_segment_file_size_url));
            client->set_timeout_ms(kMaxTimeoutMs);
            RETURN_IF_ERROR(client->head());
            return client->get_content_length(&segment_file_size);
        };

        status = HttpClient::execute_with_retry(max_retry, 1, get_segment_file_size_cb);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get segment file size from " << get_segment_file_size_url
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }

        segment_file_sizes.push_back(segment_file_size);
        segment_file_urls.push_back(std::move(get_segment_file_size_url));
    }

    // Step 5.2: check data capacity
    uint64_t total_size = std::accumulate(segment_file_sizes.begin(), segment_file_sizes.end(), 0);
    if (!local_tablet->can_add_binlog(total_size)) {
        LOG(WARNING) << "failed to add binlog, no enough space, total_size=" << total_size
                     << ", tablet=" << local_tablet->full_name();
        status = Status::InternalError("no enough space");
        status.to_thrift(&tstatus);
        return;
    }

    // Step 5.3: get all segment files
    for (int64_t segment_index = 0; segment_index < num_segments; ++segment_index) {
        auto segment_file_size = segment_file_sizes[segment_index];
        auto get_segment_file_url = segment_file_urls[segment_index];

        uint64_t estimate_timeout =
                segment_file_size / config::download_low_speed_limit_kbps / 1024;
        if (estimate_timeout < config::download_low_speed_time) {
            estimate_timeout = config::download_low_speed_time;
        }

        std::string local_segment_path =
                fmt::format("{}/{}_{}.dat", local_tablet->tablet_path(),
                            rowset_meta->rowset_id().to_string(), segment_index);
        LOG(INFO) << fmt::format("download segment file from {} to {}", get_segment_file_url,
                                 local_segment_path);
        auto get_segment_file_cb = [&get_segment_file_url, &local_segment_path, segment_file_size,
                                    estimate_timeout](HttpClient* client) {
            RETURN_IF_ERROR(client->init(get_segment_file_url));
            client->set_timeout_ms(estimate_timeout * 1000);
            RETURN_IF_ERROR(client->download(local_segment_path));

            std::error_code ec;
            // Check file length
            uint64_t local_file_size = std::filesystem::file_size(local_segment_path, ec);
            if (ec) {
                LOG(WARNING) << "download file error" << ec.message();
                return Status::IOError("can't retrive file_size of {}, due to {}",
                                       local_segment_path, ec.message());
            }
            if (local_file_size != segment_file_size) {
                LOG(WARNING) << "download file length error"
                             << ", get_segment_file_url=" << get_segment_file_url
                             << ", file_size=" << segment_file_size
                             << ", local_file_size=" << local_file_size;
                return Status::InternalError("downloaded file size is not equal");
            }
            chmod(local_segment_path.c_str(), S_IRUSR | S_IWUSR);
            return Status::OK();
        };

        auto status = HttpClient::execute_with_retry(max_retry, 1, get_segment_file_cb);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get segment file from " << get_segment_file_url
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }
    }

    // Step 6: create rowset && calculate delete bitmap && commit
    // Step 6.1: create rowset
    RowsetSharedPtr rowset;
    status = RowsetFactory::create_rowset(local_tablet->tablet_schema(),
                                          local_tablet->tablet_path(), rowset_meta, &rowset);

    if (!status) {
        LOG(WARNING) << "failed to create rowset from rowset meta for remote tablet"
                     << ". rowset_id: " << rowset_meta_pb.rowset_id()
                     << ", rowset_type: " << rowset_meta_pb.rowset_type()
                     << ", remote_tablet_id=" << rowset_meta_pb.tablet_id() << ", txn_id=" << txn_id
                     << ", status=" << status.to_string();
        status.to_thrift(&tstatus);
        return;
    }

    // Step 6.2 calculate delete bitmap before commit
    auto calc_delete_bitmap_token =
            StorageEngine::instance()->calc_delete_bitmap_executor()->create_token();
    DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(local_tablet_id);
    RowsetIdUnorderedSet pre_rowset_ids;
    if (local_tablet->enable_unique_key_merge_on_write()) {
        auto beta_rowset = reinterpret_cast<BetaRowset*>(rowset.get());
        std::vector<segment_v2::SegmentSharedPtr> segments;
        status = beta_rowset->load_segments(&segments);
        if (!status) {
            LOG(WARNING) << "failed to load segments from rowset"
                         << ". rowset_id: " << beta_rowset->rowset_id() << ", txn_id=" << txn_id
                         << ", status=" << status.to_string();
            status.to_thrift(&tstatus);
            return;
        }
        if (segments.size() > 1) {
            // calculate delete bitmap between segments
            status = local_tablet->calc_delete_bitmap_between_segments(rowset, segments,
                                                                       delete_bitmap);
            if (!status) {
                LOG(WARNING) << "failed to calculate delete bitmap"
                             << ". tablet_id: " << local_tablet->tablet_id()
                             << ". rowset_id: " << rowset->rowset_id() << ", txn_id=" << txn_id
                             << ", status=" << status.to_string();
                status.to_thrift(&tstatus);
                return;
            }
        }

        local_tablet->commit_phase_update_delete_bitmap(rowset, pre_rowset_ids, delete_bitmap,
                                                        segments, txn_id,
                                                        calc_delete_bitmap_token.get(), nullptr);
        calc_delete_bitmap_token->wait();
    }

    // Step 6.3: commit txn
    Status commit_txn_status = StorageEngine::instance()->txn_manager()->commit_txn(
            local_tablet->data_dir()->get_meta(), rowset_meta->partition_id(),
            rowset_meta->txn_id(), rowset_meta->tablet_id(), rowset_meta->tablet_schema_hash(),
            local_tablet->tablet_uid(), rowset_meta->load_id(), rowset, false);
    if (!commit_txn_status && !commit_txn_status.is<ErrorCode::PUSH_TRANSACTION_ALREADY_EXIST>()) {
        auto err_msg = fmt::format(
                "failed to commit txn for remote tablet. rowset_id: {}, remote_tablet_id={}, "
                "txn_id={}, status={}",
                rowset_meta->rowset_id().to_string(), rowset_meta->tablet_id(),
                rowset_meta->txn_id(), commit_txn_status.to_string());
        LOG(WARNING) << err_msg;
        set_tstatus(TStatusCode::RUNTIME_ERROR, std::move(err_msg));
        return;
    }

    if (local_tablet->enable_unique_key_merge_on_write()) {
        StorageEngine::instance()->txn_manager()->set_txn_related_delete_bitmap(
                partition_id, txn_id, local_tablet_id, local_tablet->schema_hash(),
                local_tablet->tablet_uid(), true, delete_bitmap, pre_rowset_ids, nullptr);
    }

    tstatus.__set_status_code(TStatusCode::OK);
}
} // namespace doris
