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
#include <glog/logging.h>

#include <fstream>
#include <memory>
#include <string>

#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "io/fs/s3_file_system.h"
#include "olap/storage_engine.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_task.h"
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

RuntimeState::RuntimeState(const TPlanFragmentExecParams& fragment_exec_params,
                           const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                           ExecEnv* exec_env, QueryContext* ctx,
                           const std::shared_ptr<MemTrackerLimiter>& query_mem_tracker)
        : _profile("Fragment " + print_id(fragment_exec_params.fragment_instance_id)),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _query_id(fragment_exec_params.query_id),
          _per_fragment_instance_idx(0),
          _num_rows_load_total(0),
          _num_rows_load_filtered(0),
          _num_rows_load_unselected(0),
          _num_print_error_rows(0),
          _num_bytes_load_total(0),
          _num_finished_scan_range(0),
          _normal_row_number(0),
          _error_row_number(0),
          _query_ctx(ctx) {
    Status status =
            init(fragment_exec_params.fragment_instance_id, query_options, query_globals, exec_env);
    DCHECK(status.ok());
    if (query_mem_tracker != nullptr) {
        _query_mem_tracker = query_mem_tracker;
    } else {
        DCHECK(ctx != nullptr);
        _query_mem_tracker = ctx->query_mem_tracker;
    }
#ifdef BE_TEST
    if (_query_mem_tracker == nullptr) {
        init_mem_trackers();
    }
#endif
    DCHECK(_query_mem_tracker != nullptr && _query_mem_tracker->label() != "Orphan");
    if (fragment_exec_params.__isset.runtime_filter_params) {
        _query_ctx->runtime_filter_mgr()->set_runtime_filter_params(
                fragment_exec_params.runtime_filter_params);
    }
}

RuntimeState::RuntimeState(const TUniqueId& instance_id, const TUniqueId& query_id,
                           int32_t fragment_id, const TQueryOptions& query_options,
                           const TQueryGlobals& query_globals, ExecEnv* exec_env, QueryContext* ctx)
        : _profile("Fragment " + print_id(instance_id)),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _query_id(query_id),
          _fragment_id(fragment_id),
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
          _query_ctx(ctx) {
    [[maybe_unused]] auto status = init(instance_id, query_options, query_globals, exec_env);
    DCHECK(status.ok());
    _query_mem_tracker = ctx->query_mem_tracker;
#ifdef BE_TEST
    if (_query_mem_tracker == nullptr) {
        init_mem_trackers();
    }
#endif
    DCHECK(_query_mem_tracker != nullptr && _query_mem_tracker->label() != "Orphan");
}

RuntimeState::RuntimeState(pipeline::PipelineFragmentContext*, const TUniqueId& instance_id,
                           const TUniqueId& query_id, int32_t fragment_id,
                           const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                           ExecEnv* exec_env, QueryContext* ctx)
        : _profile("Fragment " + print_id(instance_id)),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _runtime_filter_mgr(nullptr),
          _unreported_error_idx(0),
          _query_id(query_id),
          _fragment_id(fragment_id),
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
          _query_ctx(ctx) {
    [[maybe_unused]] auto status = init(instance_id, query_options, query_globals, exec_env);
    _query_mem_tracker = ctx->query_mem_tracker;
#ifdef BE_TEST
    if (_query_mem_tracker == nullptr) {
        init_mem_trackers();
    }
#endif
    DCHECK(_query_mem_tracker != nullptr && _query_mem_tracker->label() != "Orphan");
    DCHECK(status.ok());
}

RuntimeState::RuntimeState(const TUniqueId& query_id, int32_t fragment_id,
                           const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                           ExecEnv* exec_env, QueryContext* ctx)
        : _profile("PipelineX  " + std::to_string(fragment_id)),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _query_id(query_id),
          _fragment_id(fragment_id),
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
          _query_ctx(ctx) {
    // TODO: do we really need instance id?
    Status status = init(TUniqueId(), query_options, query_globals, exec_env);
    DCHECK(status.ok());
    _query_mem_tracker = ctx->query_mem_tracker;
#ifdef BE_TEST
    if (_query_mem_tracker == nullptr) {
        init_mem_trackers();
    }
#endif
    DCHECK(_query_mem_tracker != nullptr && _query_mem_tracker->label() != "Orphan");
}

RuntimeState::RuntimeState(const TQueryGlobals& query_globals)
        : _profile("<unnamed>"),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _unreported_error_idx(0),
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
        VecDateTimeValue dt;
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
    init_mem_trackers("<unnamed>");
}

RuntimeState::RuntimeState()
        : _profile("<unnamed>"),
          _load_channel_profile("<unnamed>"),
          _obj_pool(new ObjectPool()),
          _unreported_error_idx(0),
          _per_fragment_instance_idx(0) {
    _query_options.batch_size = DEFAULT_BATCH_SIZE;
    _query_options.be_exec_version = BeExecVersionManager::get_newest_version();
    _timezone = TimezoneUtils::default_time_zone;
    _timestamp_ms = 0;
    _nano_seconds = 0;
    TimezoneUtils::find_cctz_time_zone(_timezone, _timezone_obj);
    _exec_env = ExecEnv::GetInstance();
    init_mem_trackers("<unnamed>");
}

RuntimeState::~RuntimeState() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_mem_tracker);
    // close error log file
    if (_error_log_file != nullptr && _error_log_file->is_open()) {
        _error_log_file->close();
    }

    _obj_pool->clear();
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
        VecDateTimeValue dt;
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

void RuntimeState::init_mem_trackers(const std::string& name, const TUniqueId& id) {
    _query_mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, fmt::format("{}#Id={}", name, print_id(id)));
}

std::shared_ptr<MemTrackerLimiter> RuntimeState::query_mem_tracker() const {
    CHECK(_query_mem_tracker != nullptr);
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

bool RuntimeState::is_cancelled() const {
    // Maybe we should just return _is_cancelled.load()
    return !_exec_status.ok() || (_query_ctx && _query_ctx->is_cancelled());
}

Status RuntimeState::cancel_reason() const {
    if (!_exec_status.ok()) {
        return _exec_status.status();
    }

    if (_query_ctx) {
        return _query_ctx->exec_status();
    }

    return Status::Cancelled("Query cancelled");
}

const int64_t MAX_ERROR_NUM = 50;

Status RuntimeState::create_error_log_file() {
    if (config::save_load_error_log_to_s3 && config::is_cloud_mode()) {
        _s3_error_fs = std::dynamic_pointer_cast<io::S3FileSystem>(
                ExecEnv::GetInstance()->storage_engine().to_cloud().latest_fs());
        if (_s3_error_fs) {
            std::stringstream ss;
            // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
            // shorten the path as much as possible to prevent the length of the presigned URL from
            // exceeding the MySQL error packet size limit
            ss << "error_log/" << _import_label << "_" << std::hex << _fragment_instance_id.hi;
            _s3_error_log_file_path = ss.str();
        }
    }

    static_cast<void>(_exec_env->load_path_mgr()->get_load_error_file_name(
            _db_name, _import_label, _fragment_instance_id, &_error_log_file_path));
    std::string error_log_absolute_path =
            _exec_env->load_path_mgr()->get_load_error_absolute_path(_error_log_file_path);
    _error_log_file = std::make_unique<std::ofstream>(error_log_absolute_path, std::ifstream::out);
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

    size_t error_row_size = out.size();
    if (error_row_size > 0) {
        if (error_row_size > config::load_error_log_limit_bytes) {
            fmt::memory_buffer limit_byte_out;
            limit_byte_out.append(out.data(), out.data() + config::load_error_log_limit_bytes);
            (*_error_log_file) << fmt::to_string(limit_byte_out) + "error log is too long"
                               << std::endl;
        } else {
            (*_error_log_file) << fmt::to_string(out) << std::endl;
        }
    }
    return Status::OK();
}

std::string RuntimeState::get_error_log_file_path() {
    if (_s3_error_fs && _error_log_file && _error_log_file->is_open()) {
        // close error log file
        _error_log_file->close();
        std::string error_log_absolute_path =
                _exec_env->load_path_mgr()->get_load_error_absolute_path(_error_log_file_path);
        // upload error log file to s3
        Status st = _s3_error_fs->upload(error_log_absolute_path, _s3_error_log_file_path);
        if (st.ok()) {
            // remove local error log file
            std::filesystem::remove(error_log_absolute_path);
        } else {
            // upload failed and return local error log file path
            LOG(WARNING) << "Fail to upload error file to s3, error_log_file_path="
                         << _error_log_file_path << ", error=" << st;
            return _error_log_file_path;
        }
        // expiration must be less than a week (in seconds) for presigned url
        static const unsigned EXPIRATION_SECONDS = 7 * 24 * 60 * 60 - 1;
        // We should return a public endpoint to user.
        _error_log_file_path = _s3_error_fs->generate_presigned_url(_s3_error_log_file_path,
                                                                    EXPIRATION_SECONDS, true);
    }
    return _error_log_file_path;
}

void RuntimeState::resize_op_id_to_local_state(int operator_size) {
    _op_id_to_local_state.resize(-operator_size);
}

void RuntimeState::emplace_local_state(
        int id, std::unique_ptr<doris::pipeline::PipelineXLocalStateBase> state) {
    id = -id;
    DCHECK(id < _op_id_to_local_state.size());
    DCHECK(!_op_id_to_local_state[id]);
    _op_id_to_local_state[id] = std::move(state);
}

doris::pipeline::PipelineXLocalStateBase* RuntimeState::get_local_state(int id) {
    id = -id;
    return _op_id_to_local_state[id].get();
}

Result<RuntimeState::LocalState*> RuntimeState::get_local_state_result(int id) {
    id = -id;
    if (id >= _op_id_to_local_state.size()) {
        return ResultError(Status::InternalError("get_local_state out of range size:{} , id:{}",
                                                 _op_id_to_local_state.size(), id));
    }
    if (!_op_id_to_local_state[id]) {
        return ResultError(Status::InternalError("get_local_state id:{} is null", id));
    }
    return _op_id_to_local_state[id].get();
};

void RuntimeState::emplace_sink_local_state(
        int id, std::unique_ptr<doris::pipeline::PipelineXSinkLocalStateBase> state) {
    DCHECK(!_sink_local_state) << " id=" << id << " state: " << state->debug_string(0);
    _sink_local_state = std::move(state);
}

doris::pipeline::PipelineXSinkLocalStateBase* RuntimeState::get_sink_local_state() {
    return _sink_local_state.get();
}

Result<RuntimeState::SinkLocalState*> RuntimeState::get_sink_local_state_result() {
    if (!_sink_local_state) {
        return ResultError(Status::InternalError("_op_id_to_sink_local_state not exist"));
    }
    return _sink_local_state.get();
}

bool RuntimeState::enable_page_cache() const {
    return !config::disable_storage_page_cache &&
           (_query_options.__isset.enable_page_cache && _query_options.enable_page_cache);
}

RuntimeFilterMgr* RuntimeState::global_runtime_filter_mgr() {
    return _query_ctx->runtime_filter_mgr();
}

Status RuntimeState::register_producer_runtime_filter(
        const TRuntimeFilterDesc& desc, bool need_local_merge,
        std::shared_ptr<IRuntimeFilter>* producer_filter, bool build_bf_exactly) {
    if (desc.has_remote_targets || need_local_merge) {
        return global_runtime_filter_mgr()->register_local_merge_producer_filter(
                desc, query_options(), producer_filter, build_bf_exactly);
    } else {
        return local_runtime_filter_mgr()->register_producer_filter(
                desc, query_options(), producer_filter, build_bf_exactly);
    }
}

Status RuntimeState::register_consumer_runtime_filter(
        const doris::TRuntimeFilterDesc& desc, bool need_local_merge, int node_id,
        std::shared_ptr<IRuntimeFilter>* consumer_filter) {
    if (desc.has_remote_targets || need_local_merge) {
        return global_runtime_filter_mgr()->register_consumer_filter(desc, query_options(), node_id,
                                                                     consumer_filter, false, true);
    } else {
        return local_runtime_filter_mgr()->register_consumer_filter(desc, query_options(), node_id,
                                                                    consumer_filter, false, false);
    }
}

bool RuntimeState::is_nereids() const {
    return _query_ctx->is_nereids();
}

std::vector<std::shared_ptr<RuntimeProfile>> RuntimeState::pipeline_id_to_profile() {
    std::shared_lock lc(_pipeline_profile_lock);
    return _pipeline_id_to_profile;
}

std::vector<std::shared_ptr<RuntimeProfile>> RuntimeState::build_pipeline_profile(
        std::size_t pipeline_size) {
    std::unique_lock lc(_pipeline_profile_lock);
    if (!_pipeline_id_to_profile.empty()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "build_pipeline_profile can only be called once.");
    }
    _pipeline_id_to_profile.resize(pipeline_size);
    {
        size_t pip_idx = 0;
        for (auto& pipeline_profile : _pipeline_id_to_profile) {
            pipeline_profile =
                    std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_idx));
            pip_idx++;
        }
    }
    return _pipeline_id_to_profile;
}

} // end namespace doris
