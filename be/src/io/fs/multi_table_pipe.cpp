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

#include "multi_table_pipe.h"

#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <type_traits>

#include "common/status.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_util.h"
#include "util/time.h"

namespace doris {
namespace io {

Status MultiTablePipe::append_with_line_delimiter(const char* data, size_t size) {
    const std::string& table = parse_dst_table(data, size);
    if (table.empty()) {
        return Status::InternalError("table name is empty");
    }
    size_t prefix_len = table.length() + 1;
    AppendFunc cb = &KafkaConsumerPipe::append_with_line_delimiter;
    return dispatch(table, data + prefix_len, size - prefix_len, cb);
}

Status MultiTablePipe::append_json(const char* data, size_t size) {
    const std::string& table = parse_dst_table(data, size);
    if (table.empty()) {
        return Status::InternalError("table name is empty");
    }
    size_t prefix_len = table.length() + 1;
    AppendFunc cb = &KafkaConsumerPipe::append_json;
    return dispatch(table, data + prefix_len, size - prefix_len, cb);
}

KafkaConsumerPipePtr MultiTablePipe::get_pipe_by_table(const std::string& table) {
    auto pipe = _planned_pipes.find(table);
    DCHECK(pipe != _planned_pipes.end());
    return pipe->second;
}

static std::string_view get_first_part(const char* dat, char delimiter) {
    const char* delimiterPos = std::strchr(dat, delimiter);

    if (delimiterPos != nullptr) {
        std::ptrdiff_t length = delimiterPos - dat;
        return std::string_view(dat, length);
    } else {
        return std::string_view(dat);
    }
}

Status MultiTablePipe::finish() {
    for (auto& pair : _planned_pipes) {
        RETURN_IF_ERROR(pair.second->finish());
    }
    return Status::OK();
}

void MultiTablePipe::cancel(const std::string& reason) {
    for (auto& pair : _planned_pipes) {
        pair.second->cancel(reason);
    }
}

std::string MultiTablePipe::parse_dst_table(const char* data, size_t size) {
    return std::string(get_first_part(data, '|'));
}

Status MultiTablePipe::dispatch(const std::string& table, const char* data, size_t size,
                                AppendFunc cb) {
    if (size == 0 || strlen(data) == 0) {
        LOG(WARNING) << "empty data for table: " << table;
        return Status::InternalError("empty data");
    }
    KafkaConsumerPipePtr pipe = nullptr;
    auto iter = _planned_pipes.find(table);
    if (iter != _planned_pipes.end()) {
        pipe = iter->second;
        RETURN_NOT_OK_STATUS_WITH_WARN((pipe.get()->*cb)(data, size),
                                       "append failed in planned kafka pipe");
    } else {
        iter = _unplanned_pipes.find(table);
        if (iter == _unplanned_pipes.end()) {
            pipe = std::make_shared<io::KafkaConsumerPipe>();
            LOG(INFO) << "create new unplanned pipe: " << pipe.get();
            _unplanned_pipes.emplace(table, pipe);
        } else {
            pipe = iter->second;
        }
        RETURN_NOT_OK_STATUS_WITH_WARN((pipe.get()->*cb)(data, size),
                                       "append failed in unplanned kafka pipe");

        ++_unplanned_row_cnt;
        size_t threshold = config::multi_table_batch_plan_threshold;
        if (_unplanned_row_cnt >= threshold) {
            LOG(INFO) << fmt::format("unplanned row cnt={} reach threshold={}, plan them",
                                     _unplanned_row_cnt, threshold);
            Status st = request_and_exec_plans();
            _unplanned_row_cnt = 0;
            if (!st.ok()) {
                return st;
            }
        }
    }
    return Status::OK();
}

#ifndef BE_TEST
Status MultiTablePipe::request_and_exec_plans() {
    if (_unplanned_pipes.empty()) {
        return Status::OK();
    }

    // get list of table names in unplanned pipes
    std::vector<std::string> tables;
    fmt::memory_buffer log_buffer;
    log_buffer.clear();
    fmt::format_to(log_buffer, "request plans for {} tables: [ ", _unplanned_pipes.size());
    for (auto& pair : _unplanned_pipes) {
        tables.push_back(pair.first);
        fmt::format_to(log_buffer, "{} ", pair.first);
    }
    fmt::format_to(log_buffer, "]");
    LOG(INFO) << fmt::to_string(log_buffer);

    TStreamLoadPutRequest request;
    set_request_auth(&request, _ctx->auth);
    request.db = _ctx->db;
    request.table_names = tables;
    request.__isset.table_names = true;
    request.txnId = _ctx->txn_id;
    request.formatType = _ctx->format;
    request.__set_compress_type(_ctx->compress_type);
    request.__set_header_type(_ctx->header_type);
    request.__set_loadId(_ctx->id.to_thrift());
    request.fileType = TFileType::FILE_STREAM;
    request.__set_thrift_rpc_timeout_ms(config::thrift_rpc_timeout_ms);
    // no need to register new_load_stream_mgr coz it is already done in routineload submit task

    // plan this load
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    TNetworkAddress master_addr = exec_env->master_info()->network_address;
    int64_t stream_load_put_start_time = MonotonicNanos();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, this](FrontendServiceConnection& client) {
                client->streamLoadMultiTablePut(_ctx->multi_table_put_result, request);
            }));
    _ctx->stream_load_put_cost_nanos = MonotonicNanos() - stream_load_put_start_time;

    Status plan_status(Status::create(_ctx->multi_table_put_result.status));
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status << _ctx->brief();
        return plan_status;
    }

    Status st;
    if (_ctx->multi_table_put_result.__isset.params &&
        !_ctx->multi_table_put_result.__isset.pipeline_params) {
        st = exec_plans(exec_env, _ctx->multi_table_put_result.params);
    } else if (!_ctx->multi_table_put_result.__isset.params &&
               _ctx->multi_table_put_result.__isset.pipeline_params) {
        st = exec_plans(exec_env, _ctx->multi_table_put_result.pipeline_params);
    } else {
        return Status::Aborted("too many or too few params are set in multi_table_put_result.");
    }

    return st;
}

template <typename ExecParam>
Status MultiTablePipe::exec_plans(ExecEnv* exec_env, std::vector<ExecParam> params) {
    // put unplanned pipes into planned pipes and clear unplanned pipes
    for (auto& pipe : _unplanned_pipes) {
        _ctx->table_list.push_back(pipe.first);
        _planned_pipes.emplace(pipe.first, pipe.second);
    }
    LOG(INFO) << fmt::format("{} tables plan complete, planned table cnt={}, returned plan cnt={}",
                             _unplanned_pipes.size(), _planned_pipes.size(), params.size());
    _unplanned_pipes.clear();

    _inflight_plan_cnt += params.size();
    for (auto& plan : params) {
        if (!plan.__isset.table_name ||
            _planned_pipes.find(plan.table_name) == _planned_pipes.end()) {
            return Status::Aborted("Missing vital param: table_name");
        }

        if constexpr (std::is_same_v<ExecParam, TExecPlanFragmentParams>) {
            RETURN_IF_ERROR(
                    putPipe(plan.params.fragment_instance_id, _planned_pipes[plan.table_name]));
            LOG(INFO) << "fragment_instance_id=" << print_id(plan.params.fragment_instance_id)
                      << " table=" << plan.table_name;
        } else if constexpr (std::is_same_v<ExecParam, TPipelineFragmentParams>) {
            auto pipe_id = calculate_pipe_id(plan.query_id, plan.fragment_id);
            RETURN_IF_ERROR(putPipe(pipe_id, _planned_pipes[plan.table_name]));
            LOG(INFO) << "pipe_id=" << pipe_id << "table=" << plan.table_name;
        } else {
            LOG(WARNING) << "illegal exec param type, need `TExecPlanFragmentParams` or "
                            "`TPipelineFragmentParams`, will crash";
            CHECK(false);
        }

        RETURN_IF_ERROR(exec_env->fragment_mgr()->exec_plan_fragment(
                plan, [this](RuntimeState* state, Status* status) {
                    {
                        std::lock_guard<std::mutex> l(_tablet_commit_infos_lock);
                        _tablet_commit_infos.insert(_tablet_commit_infos.end(),
                                                    state->tablet_commit_infos().begin(),
                                                    state->tablet_commit_infos().end());
                    }
                    _number_total_rows += state->num_rows_load_total();
                    _number_loaded_rows += state->num_rows_load_success();
                    _number_filtered_rows += state->num_rows_load_filtered();
                    _number_unselected_rows += state->num_rows_load_unselected();

                    // check filtered ratio for this plan fragment
                    int64_t num_selected_rows =
                            state->num_rows_load_total() - state->num_rows_load_unselected();
                    if (num_selected_rows > 0 &&
                        (double)state->num_rows_load_filtered() / num_selected_rows >
                                _ctx->max_filter_ratio) {
                        *status = Status::InternalError("too many filtered rows");
                    }
                    if (_number_filtered_rows > 0 && !state->get_error_log_file_path().empty()) {
                        _ctx->error_url = to_load_error_http_path(state->get_error_log_file_path());
                    }

                    // if any of the plan fragment exec failed, set the status to the first failed plan
                    if (!status->ok()) {
                        LOG(WARNING)
                                << "plan fragment exec failed. errmsg=" << *status << _ctx->brief();
                        _status = *status;
                    }

                    --_inflight_plan_cnt;
                    if (_inflight_plan_cnt == 0 && is_consume_finished()) {
                        _ctx->number_total_rows = _number_total_rows;
                        _ctx->number_loaded_rows = _number_loaded_rows;
                        _ctx->number_filtered_rows = _number_filtered_rows;
                        _ctx->number_unselected_rows = _number_unselected_rows;
                        _ctx->commit_infos = _tablet_commit_infos;
                        LOG(INFO) << "all plan for multi-table load complete. number_total_rows="
                                  << _ctx->number_total_rows
                                  << " number_loaded_rows=" << _ctx->number_loaded_rows
                                  << " number_filtered_rows=" << _ctx->number_filtered_rows
                                  << " number_unselected_rows=" << _ctx->number_unselected_rows;
                        _ctx->promise.set_value(
                                _status); // when all done, finish the routine load task
                    }
                }));
    }

    return Status::OK();
}

#else
Status MultiTablePipe::request_and_exec_plans() {
    // put unplanned pipes into planned pipes
    for (auto& pipe : _unplanned_pipes) {
        _planned_pipes.emplace(pipe.first, pipe.second);
    }
    LOG(INFO) << fmt::format("{} tables plan complete, planned table cnt={}",
                             _unplanned_pipes.size(), _planned_pipes.size());
    _unplanned_pipes.clear();
    return Status::OK();
}

template <typename ExecParam>
Status MultiTablePipe::exec_plans(ExecEnv* exec_env, std::vector<ExecParam> params) {
    return Status::OK();
}

#endif

Status MultiTablePipe::putPipe(const TUniqueId& pipe_id, std::shared_ptr<io::StreamLoadPipe> pipe) {
    std::lock_guard<std::mutex> l(_pipe_map_lock);
    auto it = _pipe_map.find(pipe_id);
    if (it != std::end(_pipe_map)) {
        return Status::InternalError("id already exist");
    }
    _pipe_map.emplace(pipe_id, pipe);
    return Status::OK();
}

std::shared_ptr<io::StreamLoadPipe> MultiTablePipe::getPipe(const TUniqueId& pipe_id) {
    std::lock_guard<std::mutex> l(_pipe_map_lock);
    auto it = _pipe_map.find(pipe_id);
    if (it == std::end(_pipe_map)) {
        return std::shared_ptr<io::StreamLoadPipe>(nullptr);
    }
    return it->second;
}

void MultiTablePipe::removePipe(const TUniqueId& pipe_id) {
    std::lock_guard<std::mutex> l(_pipe_map_lock);
    auto it = _pipe_map.find(pipe_id);
    if (it != std::end(_pipe_map)) {
        _pipe_map.erase(it);
        VLOG_NOTICE << "remove stream load pipe: " << pipe_id;
    }
}

template Status MultiTablePipe::exec_plans(ExecEnv* exec_env,
                                           std::vector<TExecPlanFragmentParams> params);
template Status MultiTablePipe::exec_plans(ExecEnv* exec_env,
                                           std::vector<TPipelineFragmentParams> params);

} // namespace io
} // namespace doris
