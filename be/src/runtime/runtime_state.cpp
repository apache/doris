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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/runtime-state.cpp
// and modified by Doris

#include "runtime/runtime_state.h"

#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>

#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/query_context.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/thread_context.h"
#include "util/timezone_utils.h"
#include "util/uid_util.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
using namespace ErrorCode;

// for ut only
RuntimeState::RuntimeState(const TUniqueId& fragment_instance_id,
                           const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                           ExecEnv* exec_env)
        : _profile("Fragment " + print_id(fragment_instance_id)),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _runtime_filter_mgr(new RuntimeFilterMgr(TUniqueId(), this)),
          _data_stream_recvrs_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _is_cancelled(false),
          _per_fragment_instance_idx(0),
          _num_rows_load_total(0),
          _num_rows_load_filtered(0),
          _num_rows_load_unselected(0),
          _num_print_error_rows(0),
          _num_bytes_load_total(0),
          _num_finished_scan_range(0),
          _load_job_id(-1),
          _normal_row_number(0),
          _error_row_number(0),
          _error_log_file(nullptr) {
    Status status = init(fragment_instance_id, query_options, query_globals, exec_env);
    DCHECK(status.ok());
}

RuntimeState::RuntimeState(const TPlanFragmentExecParams& fragment_exec_params,
                           const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                           ExecEnv* exec_env)
        : _profile("Fragment " + print_id(fragment_exec_params.fragment_instance_id)),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _runtime_filter_mgr(new RuntimeFilterMgr(fragment_exec_params.query_id, this)),
          _data_stream_recvrs_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _query_id(fragment_exec_params.query_id),
          _is_cancelled(false),
          _per_fragment_instance_idx(0),
          _num_rows_load_total(0),
          _num_rows_load_filtered(0),
          _num_rows_load_unselected(0),
          _num_print_error_rows(0),
          _num_bytes_load_total(0),
          _num_finished_scan_range(0),
          _normal_row_number(0),
          _error_row_number(0),
          _error_log_file(nullptr) {
    if (fragment_exec_params.__isset.runtime_filter_params) {
        _runtime_filter_mgr->set_runtime_filter_params(fragment_exec_params.runtime_filter_params);
    }
    Status status =
            init(fragment_exec_params.fragment_instance_id, query_options, query_globals, exec_env);
    DCHECK(status.ok());
}

RuntimeState::RuntimeState(const TPipelineInstanceParams& pipeline_params,
                           const TUniqueId& query_id, int32_t fragment_id,
                           const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                           ExecEnv* exec_env)
        : _profile("Fragment " + print_id(pipeline_params.fragment_instance_id)),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _runtime_filter_mgr(new RuntimeFilterMgr(query_id, this)),
          _data_stream_recvrs_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _query_id(query_id),
          _fragment_id(fragment_id),
          _is_cancelled(false),
          _per_fragment_instance_idx(0),
          _num_rows_load_total(0),
          _num_rows_load_filtered(0),
          _num_rows_load_unselected(0),
          _num_rows_filtered_in_strict_mode_partial_update(0),
          _num_print_error_rows(0),
          _num_bytes_load_total(0),
          _num_finished_scan_range(0),
          _normal_row_number(0),
          _error_row_number(0),
          _error_log_file(nullptr) {
    if (pipeline_params.__isset.runtime_filter_params) {
        _runtime_filter_mgr->set_runtime_filter_params(pipeline_params.runtime_filter_params);
    }
    Status status =
            init(pipeline_params.fragment_instance_id, query_options, query_globals, exec_env);
    DCHECK(status.ok());
}

RuntimeState::RuntimeState(const TUniqueId& query_id, int32_t fragment_id,
                           const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                           ExecEnv* exec_env)
        : _profile("PipelineX  " + std::to_string(fragment_id)),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _runtime_filter_mgr(new RuntimeFilterMgr(query_id, this)),
          _data_stream_recvrs_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _query_id(query_id),
          _fragment_id(fragment_id),
          _is_cancelled(false),
          _per_fragment_instance_idx(0),
          _num_rows_load_total(0),
          _num_rows_load_filtered(0),
          _num_rows_load_unselected(0),
          _num_rows_filtered_in_strict_mode_partial_update(0),
          _num_print_error_rows(0),
          _num_bytes_load_total(0),
          _num_finished_scan_range(0),
          _normal_row_number(0),
          _error_row_number(0),
          _error_log_file(nullptr) {
    // TODO: do we really need instance id?
    Status status = init(TUniqueId(), query_options, query_globals, exec_env);
    DCHECK(status.ok());
}

RuntimeState::RuntimeState(const TQueryGlobals& query_globals)
        : _profile("<unnamed>"),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _data_stream_recvrs_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _is_cancelled(false),
          _per_fragment_instance_idx(0) {
    _query_options.batch_size = DEFAULT_BATCH_SIZE;
    if (query_globals.__isset.time_zone && query_globals.__isset.nano_seconds) {
        _timezone = query_globals.time_zone;
        _timestamp_ms = query_globals.timestamp_ms;
        _nano_seconds = query_globals.nano_seconds;
    } else if (query_globals.__isset.time_zone) {
        _timezone = query_globals.time_zone;
        _timestamp_ms = query_globals.timestamp_ms;
        _nano_seconds = 0;
    } else if (!query_globals.now_string.empty()) {
        _timezone = TimezoneUtils::default_time_zone;
        vectorized::VecDateTimeValue dt;
        dt.from_date_str(query_globals.now_string.c_str(), query_globals.now_string.size());
        int64_t timestamp;
        dt.unix_timestamp(&timestamp, _timezone);
        _timestamp_ms = timestamp * 1000;
        _nano_seconds = 0;
    } else {
        //Unit test may set into here
        _timezone = TimezoneUtils::default_time_zone;
        _timestamp_ms = 0;
        _nano_seconds = 0;
    }
    TimezoneUtils::find_cctz_time_zone(_timezone, _timezone_obj);
}

RuntimeState::RuntimeState()
        : _profile("<unnamed>"),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _data_stream_recvrs_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _is_cancelled(false),
          _per_fragment_instance_idx(0) {
    _query_options.batch_size = DEFAULT_BATCH_SIZE;
    _timezone = TimezoneUtils::default_time_zone;
    _timestamp_ms = 0;
    _nano_seconds = 0;
    TimezoneUtils::find_cctz_time_zone(_timezone, _timezone_obj);
    _exec_env = ExecEnv::GetInstance();
}

RuntimeState::~RuntimeState() {
    // close error log file
    if (_error_log_file != nullptr && _error_log_file->is_open()) {
        _error_log_file->close();
        delete _error_log_file;
        _error_log_file = nullptr;
    }

    _obj_pool->clear();
    _runtime_filter_mgr.reset();
}

Status RuntimeState::init(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                          const TQueryGlobals& query_globals, ExecEnv* exec_env) {
    _fragment_instance_id = fragment_instance_id;
    _query_options = query_options;
    if (query_globals.__isset.time_zone && query_globals.__isset.nano_seconds) {
        _timezone = query_globals.time_zone;
        _timestamp_ms = query_globals.timestamp_ms;
        _nano_seconds = query_globals.nano_seconds;
    } else if (query_globals.__isset.time_zone) {
        _timezone = query_globals.time_zone;
        _timestamp_ms = query_globals.timestamp_ms;
        _nano_seconds = 0;
    } else if (!query_globals.now_string.empty()) {
        _timezone = TimezoneUtils::default_time_zone;
        vectorized::VecDateTimeValue dt;
        dt.from_date_str(query_globals.now_string.c_str(), query_globals.now_string.size());
        int64_t timestamp;
        dt.unix_timestamp(&timestamp, _timezone);
        _timestamp_ms = timestamp * 1000;
        _nano_seconds = 0;
    } else {
        //Unit test may set into here
        _timezone = TimezoneUtils::default_time_zone;
        _timestamp_ms = 0;
        _nano_seconds = 0;
    }
    TimezoneUtils::find_cctz_time_zone(_timezone, _timezone_obj);

    if (query_globals.__isset.load_zero_tolerance) {
        _load_zero_tolerance = query_globals.load_zero_tolerance;
    }

    _exec_env = exec_env;

    if (_query_options.max_errors <= 0) {
        // TODO: fix linker error and uncomment this
        //_query_options.max_errors = config::max_errors;
        _query_options.max_errors = 100;
    }

    if (_query_options.batch_size <= 0) {
        _query_options.batch_size = DEFAULT_BATCH_SIZE;
    }

    _db_name = "insert_stmt";
    _import_label = print_id(fragment_instance_id);

    return Status::OK();
}

void RuntimeState::init_mem_trackers(const TUniqueId& id, const std::string& name) {
    _query_mem_tracker = std::make_shared<MemTrackerLimiter>(
            MemTrackerLimiter::Type::EXPERIMENTAL, fmt::format("{}#Id={}", name, print_id(id)));
}

std::shared_ptr<MemTrackerLimiter> RuntimeState::query_mem_tracker() const {
    if (!_query_mem_tracker) {
        return _exec_env->orphan_mem_tracker();
    }
    return _query_mem_tracker;
}

bool RuntimeState::log_error(const std::string& error) {
    std::lock_guard<std::mutex> l(_error_log_lock);

    if (_error_log.size() < _query_options.max_errors) {
        _error_log.push_back(error);
        return true;
    }

    return false;
}

void RuntimeState::get_unreported_errors(std::vector<std::string>* new_errors) {
    std::lock_guard<std::mutex> l(_error_log_lock);

    if (_unreported_error_idx < _error_log.size()) {
        new_errors->assign(_error_log.begin() + _unreported_error_idx, _error_log.end());
        _unreported_error_idx = _error_log.size();
    }
}

Status RuntimeState::query_status() {
    auto st = _query_ctx->exec_status();
    RETURN_IF_ERROR(st);
    std::lock_guard<std::mutex> l(_process_status_lock);
    return _process_status;
}

bool RuntimeState::is_cancelled() const {
    return _is_cancelled.load() || (_query_ctx && _query_ctx->is_cancelled());
}

Status RuntimeState::set_mem_limit_exceeded(const std::string& msg) {
    {
        std::lock_guard<std::mutex> l(_process_status_lock);
        if (_process_status.ok()) {
            _process_status = Status::MemoryLimitExceeded(msg);
        }
    }
    DCHECK(_process_status.is<MEM_LIMIT_EXCEEDED>());
    return _process_status;
}

Status RuntimeState::check_query_state(const std::string& msg) {
    // TODO: it would be nice if this also checked for cancellation, but doing so breaks
    // cases where we use Status::Cancelled("Cancelled") to indicate that the limit was reached.
    //
    // If the thread MemTrackerLimiter exceeds the limit, an error status is returned.
    // Usually used after SCOPED_ATTACH_TASK, during query execution.
    if (thread_context()->thread_mem_tracker()->limit_exceeded() &&
        !config::enable_query_memory_overcommit) {
        auto failed_msg =
                fmt::format("{}, {}", msg,
                            thread_context()->thread_mem_tracker()->tracker_limit_exceeded_str());
        thread_context()->thread_mem_tracker()->print_log_usage(failed_msg);
        log_error(failed_msg);
        return Status::MemoryLimitExceeded(failed_msg);
    }
    return query_status();
}

const int64_t MAX_ERROR_NUM = 50;

Status RuntimeState::create_error_log_file() {
    static_cast<void>(_exec_env->load_path_mgr()->get_load_error_file_name(
            _db_name, _import_label, _fragment_instance_id, &_error_log_file_path));
    std::string error_log_absolute_path =
            _exec_env->load_path_mgr()->get_load_error_absolute_path(_error_log_file_path);
    _error_log_file = new std::ofstream(error_log_absolute_path, std::ifstream::out);
    if (!_error_log_file->is_open()) {
        std::stringstream error_msg;
        error_msg << "Fail to open error file: [" << _error_log_file_path << "].";
        LOG(WARNING) << error_msg.str();
        return Status::InternalError(error_msg.str());
    }
    VLOG_FILE << "create error log file: " << _error_log_file_path;

    return Status::OK();
}

Status RuntimeState::append_error_msg_to_file(std::function<std::string()> line,
                                              std::function<std::string()> error_msg,
                                              bool* stop_processing, bool is_summary) {
    *stop_processing = false;
    if (query_type() != TQueryType::LOAD) {
        return Status::OK();
    }
    // If file haven't been opened, open it here
    if (_error_log_file == nullptr) {
        Status status = create_error_log_file();
        if (!status.ok()) {
            LOG(WARNING) << "Create error file log failed. because: " << status;
            if (_error_log_file != nullptr) {
                _error_log_file->close();
                delete _error_log_file;
                _error_log_file = nullptr;
            }
            return status;
        }
    }

    // if num of printed error row exceeds the limit, and this is not a summary message,
    // if _load_zero_tolerance, return Error to stop the load process immediately.
    if (_num_print_error_rows.fetch_add(1, std::memory_order_relaxed) > MAX_ERROR_NUM &&
        !is_summary) {
        if (_load_zero_tolerance) {
            *stop_processing = true;
        }
        return Status::OK();
    }

    fmt::memory_buffer out;
    if (is_summary) {
        fmt::format_to(out, "Summary: {}", error_msg());
    } else {
        if (_error_row_number < MAX_ERROR_NUM) {
            // Note: export reason first in case src line too long and be truncated.
            fmt::format_to(out, "Reason: {}. src line [{}]; ", error_msg(), line());
        } else if (_error_row_number == MAX_ERROR_NUM) {
            fmt::format_to(out, "TOO MUCH ERROR! already reach {}. show no more next error.",
                           MAX_ERROR_NUM);
        }
    }

    if (out.size() > 0) {
        (*_error_log_file) << fmt::to_string(out) << std::endl;
    }
    return Status::OK();
}

int64_t RuntimeState::get_load_mem_limit() {
    // TODO: the code is abandoned, it can be deleted after v1.3
    if (_query_options.__isset.load_mem_limit && _query_options.load_mem_limit > 0) {
        return _query_options.load_mem_limit;
    } else {
        return _query_mem_tracker->limit();
    }
}

void RuntimeState::emplace_local_state(
        int id, std::shared_ptr<doris::pipeline::PipelineXLocalStateBase> state) {
    std::unique_lock<std::mutex> l(_local_state_lock);
    _op_id_to_local_state.emplace(id, state);
}

std::shared_ptr<doris::pipeline::PipelineXLocalStateBase> RuntimeState::get_local_state(int id) {
    std::unique_lock<std::mutex> l(_local_state_lock);
    if (_op_id_to_local_state.find(id) == _op_id_to_local_state.end()) {
        return nullptr;
    }
    return _op_id_to_local_state[id];
}

RuntimeState::ResultLocalState RuntimeState::get_local_state_result(int id) {
    std::unique_lock<std::mutex> l(_local_state_lock);
    if (_op_id_to_local_state.find(id) == _op_id_to_local_state.end()) {
        return ResultError(Status::InternalError("could not find local state id {}", id));
    }
    return _op_id_to_local_state[id];
}

void RuntimeState::emplace_sink_local_state(
        int id, std::shared_ptr<doris::pipeline::PipelineXSinkLocalStateBase> state) {
    std::unique_lock<std::mutex> l(_local_sink_state_lock);
    _op_id_to_sink_local_state.emplace(id, state);
}

std::shared_ptr<doris::pipeline::PipelineXSinkLocalStateBase> RuntimeState::get_sink_local_state(
        int id) {
    std::unique_lock<std::mutex> l(_local_sink_state_lock);
    if (_op_id_to_sink_local_state.find(id) == _op_id_to_sink_local_state.end()) {
        return nullptr;
    }
    return _op_id_to_sink_local_state[id];
}

RuntimeState::ResultSinkLocalState RuntimeState::get_sink_local_state_result(int id) {
    std::unique_lock<std::mutex> l(_local_sink_state_lock);
    if (_op_id_to_sink_local_state.find(id) == _op_id_to_sink_local_state.end()) {
        return ResultError(Status::InternalError("could not find sink local state id {}", id));
    }
    return _op_id_to_sink_local_state[id];
}

bool RuntimeState::enable_page_cache() const {
    return !config::disable_storage_page_cache &&
           (_query_options.__isset.enable_page_cache && _query_options.enable_page_cache);
}

} // end namespace doris
