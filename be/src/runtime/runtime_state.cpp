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

#include <boost/algorithm/string/join.hpp>
#include <fmt/format.h>
#include <sstream>
#include <string>

#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "runtime/buffered_block_mgr2.h"
#include "runtime/bufferpool/reservation_tracker.h"
#include "runtime/bufferpool/reservation_util.h"
#include "runtime/exec_env.h"
#include "runtime/initial_reservations.h"
#include "runtime/load_path_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/mem_tracker_task_pool.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/file_utils.h"
#include "util/load_error_hub.h"
#include "util/pretty_printer.h"
#include "util/timezone_utils.h"
#include "util/uid_util.h"

namespace doris {

// for ut only
RuntimeState::RuntimeState(const TUniqueId& fragment_instance_id,
                           const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                           ExecEnv* exec_env)
        : _profile("Fragment " + print_id(fragment_instance_id)),
          _obj_pool(new ObjectPool()),
          _runtime_filter_mgr(new RuntimeFilterMgr(TUniqueId(), this)),
          _data_stream_recvrs_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _is_cancelled(false),
          _per_fragment_instance_idx(0),
          _root_node_id(-1),
          _num_rows_load_total(0),
          _num_rows_load_filtered(0),
          _num_rows_load_unselected(0),
          _num_print_error_rows(0),
          _num_bytes_load_total(0),
          _load_job_id(-1),
          _normal_row_number(0),
          _error_row_number(0),
          _error_log_file_path(""),
          _error_log_file(nullptr),
          _instance_buffer_reservation(new ReservationTracker) {
    Status status = init(fragment_instance_id, query_options, query_globals, exec_env);
    DCHECK(status.ok());
}

RuntimeState::RuntimeState(const TPlanFragmentExecParams& fragment_exec_params,
                           const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                           ExecEnv* exec_env)
        : _profile("Fragment " + print_id(fragment_exec_params.fragment_instance_id)),
          _obj_pool(new ObjectPool()),
          _runtime_filter_mgr(new RuntimeFilterMgr(fragment_exec_params.query_id, this)),
          _data_stream_recvrs_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _query_id(fragment_exec_params.query_id),
          _is_cancelled(false),
          _per_fragment_instance_idx(0),
          _root_node_id(-1),
          _num_rows_load_total(0),
          _num_rows_load_filtered(0),
          _num_rows_load_unselected(0),
          _num_print_error_rows(0),
          _num_bytes_load_total(0),
          _normal_row_number(0),
          _error_row_number(0),
          _error_log_file_path(""),
          _error_log_file(nullptr),
          _instance_buffer_reservation(new ReservationTracker) {
    if (fragment_exec_params.__isset.runtime_filter_params) {
        _runtime_filter_mgr->set_runtime_filter_params(fragment_exec_params.runtime_filter_params);
    }
    Status status =
            init(fragment_exec_params.fragment_instance_id, query_options, query_globals, exec_env);
    DCHECK(status.ok());
}

RuntimeState::RuntimeState(const TQueryGlobals& query_globals)
        : _profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _data_stream_recvrs_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _is_cancelled(false),
          _per_fragment_instance_idx(0) {
    _query_options.batch_size = DEFAULT_BATCH_SIZE;
    if (query_globals.__isset.time_zone) {
        _timezone = query_globals.time_zone;
        _timestamp_ms = query_globals.timestamp_ms;
    } else if (!query_globals.now_string.empty()) {
        _timezone = TimezoneUtils::default_time_zone;
        DateTimeValue dt;
        dt.from_date_str(query_globals.now_string.c_str(), query_globals.now_string.size());
        int64_t timestamp;
        dt.unix_timestamp(&timestamp, _timezone);
        _timestamp_ms = timestamp * 1000;
    } else {
        //Unit test may set into here
        _timezone = TimezoneUtils::default_time_zone;
        _timestamp_ms = 0;
    }
    TimezoneUtils::find_cctz_time_zone(_timezone, _timezone_obj);
}

RuntimeState::~RuntimeState() {
    _block_mgr2.reset();
    // close error log file
    if (_error_log_file != nullptr && _error_log_file->is_open()) {
        _error_log_file->close();
        delete _error_log_file;
        _error_log_file = nullptr;
    }

    if (_error_hub != nullptr) {
        _error_hub->close();
    }

    // Release the reservation, which should be unused at the point.
    if (_instance_buffer_reservation != nullptr) {
        _instance_buffer_reservation->Close();
    }

    if (_initial_reservations != nullptr) {
        _initial_reservations->ReleaseResources();
    }

    if (_buffer_reservation != nullptr) {
        _buffer_reservation->Close();
    }
}

Status RuntimeState::init(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                          const TQueryGlobals& query_globals, ExecEnv* exec_env) {
    _fragment_instance_id = fragment_instance_id;
    _query_options = query_options;
    if (query_globals.__isset.time_zone) {
        _timezone = query_globals.time_zone;
        _timestamp_ms = query_globals.timestamp_ms;
    } else if (!query_globals.now_string.empty()) {
        _timezone = TimezoneUtils::default_time_zone;
        DateTimeValue dt;
        dt.from_date_str(query_globals.now_string.c_str(), query_globals.now_string.size());
        int64_t timestamp;
        dt.unix_timestamp(&timestamp, _timezone);
        _timestamp_ms = timestamp * 1000;
    } else {
        //Unit test may set into here
        _timezone = TimezoneUtils::default_time_zone;
        _timestamp_ms = 0;
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

Status RuntimeState::init_mem_trackers(const TUniqueId& query_id) {
    bool has_query_mem_tracker = _query_options.__isset.mem_limit && (_query_options.mem_limit > 0);
    int64_t bytes_limit = has_query_mem_tracker ? _query_options.mem_limit : -1;
    if (bytes_limit > MemTracker::get_process_tracker()->limit()) {
        VLOG_NOTICE << "Query memory limit " << PrettyPrinter::print(bytes_limit, TUnit::BYTES)
                    << " exceeds process memory limit of "
                    << PrettyPrinter::print(MemTracker::get_process_tracker()->limit(),
                                            TUnit::BYTES)
                    << ". Using process memory limit instead";
        bytes_limit = MemTracker::get_process_tracker()->limit();
    }
    auto mem_tracker_counter = ADD_COUNTER(&_profile, "MemoryLimit", TUnit::BYTES);
    mem_tracker_counter->set(bytes_limit);

    if (query_type() == TQueryType::SELECT) {
        _query_mem_tracker =
                _exec_env->task_pool_mem_tracker_registry()->register_query_mem_tracker(
                        print_id(query_id), bytes_limit);
    } else if (query_type() == TQueryType::LOAD) {
        _query_mem_tracker = _exec_env->task_pool_mem_tracker_registry()->register_load_mem_tracker(
                print_id(query_id), bytes_limit);
    } else {
        DCHECK(false);
    }

    _instance_mem_tracker = MemTracker::create_tracker(
            bytes_limit, "RuntimeState:instance:" + print_id(_fragment_instance_id),
            _query_mem_tracker, MemTrackerLevel::INSTANCE, &_profile);

    RETURN_IF_ERROR(init_buffer_poolstate());

    _initial_reservations = _obj_pool->add(
            new InitialReservations(_obj_pool.get(), _buffer_reservation, nullptr,
                                    _query_options.initial_reservation_total_claims));
    RETURN_IF_ERROR(_initial_reservations->Init(_query_id, min_reservation()));
    DCHECK_EQ(0, _initial_reservation_refcnt.load());

    if (_instance_buffer_reservation != nullptr) {
        _instance_buffer_reservation->InitChildTracker(&_profile, _buffer_reservation, nullptr,
                                                       std::numeric_limits<int64_t>::max());
    }

    // filter manager depends _instance_mem_tracker
    _runtime_filter_mgr->init();
    return Status::OK();
}

Status RuntimeState::init_instance_mem_tracker() {
    _instance_mem_tracker = MemTracker::create_tracker(-1, "RuntimeState");
    return Status::OK();
}

Status RuntimeState::init_buffer_poolstate() {
    ExecEnv* exec_env = ExecEnv::GetInstance();
    int64_t mem_limit = _query_mem_tracker->get_lowest_limit();
    int64_t max_reservation;
    if (query_options().__isset.buffer_pool_limit && query_options().buffer_pool_limit > 0) {
        max_reservation = query_options().buffer_pool_limit;
    } else if (mem_limit == -1) {
        // No query mem limit. The process-wide reservation limit is the only limit on
        // reservations.
        max_reservation = std::numeric_limits<int64_t>::max();
    } else {
        DCHECK_GE(mem_limit, 0);
        max_reservation = ReservationUtil::GetReservationLimitFromMemLimit(mem_limit);
    }

    VLOG_QUERY << "Buffer pool limit for " << print_id(_query_id) << ": " << max_reservation;

    _buffer_reservation = _obj_pool->add(new ReservationTracker);
    _buffer_reservation->InitChildTracker(nullptr, exec_env->buffer_reservation(), nullptr,
                                          max_reservation);

    return Status::OK();
}

Status RuntimeState::create_block_mgr() {
    DCHECK(_block_mgr2.get() == nullptr);

    int64_t block_mgr_limit = _query_mem_tracker->limit();
    if (block_mgr_limit < 0) {
        block_mgr_limit = std::numeric_limits<int64_t>::max();
    }
    RETURN_IF_ERROR(BufferedBlockMgr2::create(
            this, runtime_profile(), _exec_env->tmp_file_mgr(), block_mgr_limit,
            _exec_env->disk_io_mgr()->max_read_buffer_size(), &_block_mgr2));
    return Status::OK();
}

bool RuntimeState::error_log_is_empty() {
    std::lock_guard<std::mutex> l(_error_log_lock);
    return (_error_log.size() > 0);
}

std::string RuntimeState::error_log() {
    std::lock_guard<std::mutex> l(_error_log_lock);
    return boost::algorithm::join(_error_log, "\n");
}

bool RuntimeState::log_error(const std::string& error) {
    std::lock_guard<std::mutex> l(_error_log_lock);

    if (_error_log.size() < _query_options.max_errors) {
        _error_log.push_back(error);
        return true;
    }

    return false;
}

void RuntimeState::log_error(const Status& status) {
    if (status.ok()) {
        return;
    }

    log_error(status.get_error_msg());
}

void RuntimeState::get_unreported_errors(std::vector<std::string>* new_errors) {
    std::lock_guard<std::mutex> l(_error_log_lock);

    if (_unreported_error_idx < _error_log.size()) {
        new_errors->assign(_error_log.begin() + _unreported_error_idx, _error_log.end());
        _unreported_error_idx = _error_log.size();
    }
}

Status RuntimeState::set_mem_limit_exceeded(const std::string& msg) {
    {
        std::lock_guard<std::mutex> l(_process_status_lock);
        if (_process_status.ok()) {
            _process_status = Status::MemoryLimitExceeded(msg);
        }
    }
    DCHECK(_process_status.is_mem_limit_exceeded());
    return _process_status;
}

Status RuntimeState::check_query_state(const std::string& msg) {
    // TODO: it would be nice if this also checked for cancellation, but doing so breaks
    // cases where we use Status::Cancelled("Cancelled") to indicate that the limit was reached.
    RETURN_IF_LIMIT_EXCEEDED(_instance_mem_tracker, this, msg);
    return query_status();
}

const std::string ERROR_FILE_NAME = "error_log";
const int64_t MAX_ERROR_NUM = 50;

Status RuntimeState::create_load_dir() {
    if (!_load_dir.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_exec_env->load_path_mgr()->allocate_dir(_db_name, _import_label, &_load_dir));
    _load_dir += "/output";
    return FileUtils::create_dir(_load_dir);
}

Status RuntimeState::create_error_log_file() {
    // Make sure that load dir exists.
    // create_load_dir();

    _exec_env->load_path_mgr()->get_load_error_file_name(
            _db_name, _import_label, _fragment_instance_id, &_error_log_file_path);
    // std::stringstream ss;
    // ss << load_dir() << "/" << ERROR_FILE_NAME
    //     << "_" << std::hex << fragment_instance_id().hi
    //     << "_" << fragment_instance_id().lo;
    // _error_log_file_path = ss.str();
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
    // If file havn't been opened, open it here
    if (_error_log_file == nullptr) {
        Status status = create_error_log_file();
        if (!status.ok()) {
            LOG(WARNING) << "Create error file log failed. because: " << status.get_error_msg();
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
        export_load_error(fmt::to_string(out));
    }
    return Status::OK();
}

const int64_t HUB_MAX_ERROR_NUM = 10;

void RuntimeState::export_load_error(const std::string& err_msg) {
    if (_error_hub == nullptr) {
        std::lock_guard<std::mutex> lock(_create_error_hub_lock);
        if (_error_hub == nullptr) {
            if (_load_error_hub_info == nullptr) {
                return;
            }

            Status st = LoadErrorHub::create_hub(_exec_env, _load_error_hub_info.get(),
                                                 _error_log_file_path, &_error_hub);
            if (!st.ok()) {
                LOG(WARNING) << "failed to create load error hub: " << st.get_error_msg();
                return;
            }
        }
    }

    if (_error_row_number <= HUB_MAX_ERROR_NUM) {
        LoadErrorHub::ErrorMsg err(_load_job_id, err_msg);
        // TODO(lingbin): think if should check return value?
        _error_hub->export_error(err);
    }
}

int64_t RuntimeState::get_load_mem_limit() {
    if (_query_options.__isset.load_mem_limit && _query_options.load_mem_limit > 0) {
        return _query_options.load_mem_limit;
    } else {
        return _query_mem_tracker->limit();
    }
}

} // end namespace doris
