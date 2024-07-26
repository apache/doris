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
#include "util/debug_points.h"
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
    auto pair = _planned_tables.find(table);
    DCHECK(pair != _planned_tables.end());
    return std::static_pointer_cast<io::KafkaConsumerPipe>(pair->second->pipe);
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
    for (auto& pair : _planned_tables) {
        RETURN_IF_ERROR(pair.second->pipe->finish());
    }
    return Status::OK();
}

void MultiTablePipe::cancel(const std::string& reason) {
    for (auto& pair : _planned_tables) {
        pair.second->pipe->cancel(reason);
    }
}

std::string MultiTablePipe::parse_dst_table(const char* data, size_t size) {
    return std::string(get_first_part(data, '|'));
}

Status MultiTablePipe::dispatch(const std::string& table, const char* data, size_t size,
                                AppendFunc cb) {
    if (size == 0 || strlen(data) == 0) {
        LOG(WARNING) << "empty data for table: " << table << ", ctx: " << _ctx->brief();
        return Status::InternalError("empty data");
    }
    KafkaConsumerPipePtr pipe = nullptr;
    auto iter = _planned_tables.find(table);
    if (iter != _planned_tables.end()) {
        pipe = std::static_pointer_cast<io::KafkaConsumerPipe>(iter->second->pipe);
        RETURN_NOT_OK_STATUS_WITH_WARN((pipe.get()->*cb)(data, size),
                                       "append failed in planned kafka pipe");
    } else {
        iter = _unplanned_tables.find(table);
        if (iter == _unplanned_tables.end()) {
            std::shared_ptr<StreamLoadContext> ctx =
                    std::make_shared<StreamLoadContext>(doris::ExecEnv::GetInstance());
            ctx->id = UniqueId::gen_uid();
            pipe = std::make_shared<io::KafkaConsumerPipe>();
            ctx->pipe = pipe;
#ifndef BE_TEST
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    doris::ExecEnv::GetInstance()->new_load_stream_mgr()->put(ctx->id, ctx),
                    "put stream load ctx error");
#endif
            _unplanned_tables.emplace(table, ctx);
            LOG(INFO) << "create new unplanned table ctx, table: " << table
                      << "load id: " << ctx->id << ", txn id: " << _ctx->txn_id;
        } else {
            pipe = std::static_pointer_cast<io::KafkaConsumerPipe>(iter->second->pipe);
        }

        // It is necessary to determine whether the sum of pipe_current_capacity and size is greater than pipe_max_capacity,
        // otherwise the following situation may occur:
        // the pipe is full but still cannot trigger the request and exec plan condition,
        // causing one stream multi table load can not finish
        ++_unplanned_row_cnt;
        auto pipe_current_capacity = pipe->current_capacity();
        auto pipe_max_capacity = pipe->max_capacity();
        if (_unplanned_row_cnt >= _row_threshold ||
            _unplanned_tables.size() >= _wait_tables_threshold ||
            pipe_current_capacity + size > pipe_max_capacity) {
            LOG(INFO) << fmt::format(
                                 "unplanned row cnt={} reach row_threshold={} or "
                                 "wait_plan_table_threshold={}, or the sum of "
                                 "pipe_current_capacity {} "
                                 "and size {} is greater than pipe_max_capacity {}, "
                                 "plan them",
                                 _unplanned_row_cnt, _row_threshold, _wait_tables_threshold,
                                 pipe_current_capacity, size, pipe_max_capacity)
                      << ", ctx: " << _ctx->brief();
            Status st = request_and_exec_plans();
            _unplanned_row_cnt = 0;
            if (!st.ok()) {
                return st;
            }
        }

        RETURN_NOT_OK_STATUS_WITH_WARN((pipe.get()->*cb)(data, size),
                                       "append failed in unplanned kafka pipe");
    }

    return Status::OK();
}

#ifndef BE_TEST
Status MultiTablePipe::request_and_exec_plans() {
    if (_unplanned_tables.empty()) {
        return Status::OK();
    }

    fmt::memory_buffer log_buffer;
    log_buffer.clear();
    fmt::format_to(log_buffer, "request plans for {} tables: [ ", _unplanned_tables.size());
    for (auto& pair : _unplanned_tables) {
        fmt::format_to(log_buffer, "{} ", pair.first);
    }
    fmt::format_to(log_buffer, "]");
    LOG(INFO) << fmt::to_string(log_buffer);

    Status st;
    for (auto& pair : _unplanned_tables) {
        TStreamLoadPutRequest request;
        set_request_auth(&request, _ctx->auth);
        std::vector<std::string> tables;
        tables.push_back(pair.first);
        request.db = _ctx->db;
        request.table_names = tables;
        request.__isset.table_names = true;
        request.txnId = _ctx->txn_id;
        request.formatType = _ctx->format;
        request.__set_compress_type(_ctx->compress_type);
        request.__set_header_type(_ctx->header_type);
        request.__set_loadId((pair.second->id).to_thrift());
        request.fileType = TFileType::FILE_STREAM;
        request.__set_thrift_rpc_timeout_ms(config::thrift_rpc_timeout_ms);
        request.__set_memtable_on_sink_node(_ctx->memtable_on_sink_node);
        request.__set_user(_ctx->qualified_user);
        request.__set_cloud_cluster(_ctx->cloud_cluster);
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

        if (_ctx->multi_table_put_result.__isset.params &&
            !_ctx->multi_table_put_result.__isset.pipeline_params) {
            st = exec_plans(exec_env, _ctx->multi_table_put_result.params);
        } else if (!_ctx->multi_table_put_result.__isset.params &&
                   _ctx->multi_table_put_result.__isset.pipeline_params) {
            st = exec_plans(exec_env, _ctx->multi_table_put_result.pipeline_params);
        } else {
            return Status::Aborted("too many or too few params are set in multi_table_put_result.");
        }
        if (!st.ok()) {
            return st;
        }
    }
    _unplanned_tables.clear();
    return st;
}

template <typename ExecParam>
Status MultiTablePipe::exec_plans(ExecEnv* exec_env, std::vector<ExecParam> params) {
    // put unplanned pipes into planned pipes and clear unplanned pipes
    for (auto& pair : _unplanned_tables) {
        _ctx->table_list.push_back(pair.first);
        _planned_tables.emplace(pair.first, pair.second);
    }
    LOG(INFO) << fmt::format("{} tables plan complete, planned table cnt={}, returned plan cnt={}",
                             _unplanned_tables.size(), _planned_tables.size(), params.size())
              << ", ctx: " << _ctx->brief();

    for (auto& plan : params) {
        DBUG_EXECUTE_IF("MultiTablePipe.exec_plans.failed",
                        { return Status::Aborted("MultiTablePipe.exec_plans.failed"); });
        if (!plan.__isset.table_name ||
            _unplanned_tables.find(plan.table_name) == _unplanned_tables.end()) {
            return Status::Aborted("Missing vital param: table_name");
        }

        _inflight_cnt++;

        RETURN_IF_ERROR(exec_env->fragment_mgr()->exec_plan_fragment(
                plan, [this, plan](RuntimeState* state, Status* status) {
                    DCHECK(state);
                    auto pair = _planned_tables.find(plan.table_name);
                    if (pair == _planned_tables.end()) {
                        LOG(WARNING) << "failed to get ctx, table: " << plan.table_name;
                    } else {
                        doris::ExecEnv::GetInstance()->new_load_stream_mgr()->remove(
                                pair->second->id);
                    }

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
                        *status = Status::DataQualityError("too many filtered rows");
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

                    auto inflight_cnt = _inflight_cnt.fetch_sub(1);
                    if (inflight_cnt == 1 && is_consume_finished()) {
                        _handle_consumer_finished();
                    }
                }));
    }

    return Status::OK();
}

#else
Status MultiTablePipe::request_and_exec_plans() {
    // put unplanned pipes into planned pipes
    for (auto& pipe : _unplanned_tables) {
        _planned_tables.emplace(pipe.first, pipe.second);
    }
    LOG(INFO) << fmt::format("{} tables plan complete, planned table cnt={}",
                             _unplanned_tables.size(), _planned_tables.size());
    _unplanned_tables.clear();
    return Status::OK();
}

template <typename ExecParam>
Status MultiTablePipe::exec_plans(ExecEnv* exec_env, std::vector<ExecParam> params) {
    return Status::OK();
}

#endif

void MultiTablePipe::_handle_consumer_finished() {
    _ctx->number_total_rows = _number_total_rows;
    _ctx->number_loaded_rows = _number_loaded_rows;
    _ctx->number_filtered_rows = _number_filtered_rows;
    _ctx->number_unselected_rows = _number_unselected_rows;
    _ctx->commit_infos = _tablet_commit_infos;

    // remove ctx to avoid memory leak.
    for (const auto& pair : _planned_tables) {
        if (pair.second) {
            doris::ExecEnv::GetInstance()->new_load_stream_mgr()->remove(pair.second->id);
        }
    }
    for (const auto& pair : _unplanned_tables) {
        if (pair.second) {
            doris::ExecEnv::GetInstance()->new_load_stream_mgr()->remove(pair.second->id);
        }
    }

    LOG(INFO) << "all plan for multi-table load complete. number_total_rows="
              << _ctx->number_total_rows << " number_loaded_rows=" << _ctx->number_loaded_rows
              << " number_filtered_rows=" << _ctx->number_filtered_rows
              << " number_unselected_rows=" << _ctx->number_unselected_rows
              << ", ctx: " << _ctx->brief();
    _ctx->promise.set_value(_status); // when all done, finish the routine load task
}

template Status MultiTablePipe::exec_plans(ExecEnv* exec_env,
                                           std::vector<TExecPlanFragmentParams> params);
template Status MultiTablePipe::exec_plans(ExecEnv* exec_env,
                                           std::vector<TPipelineFragmentParams> params);

} // namespace io
} // namespace doris
