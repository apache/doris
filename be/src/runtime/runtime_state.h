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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/runtime-state.h
// and modified by Doris

#pragma once

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/segment_v2.pb.h>
#include <stdint.h>

#include <atomic>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "cctz/time_zone.h"
// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/factory_creator.h"
#include "common/status.h"
#include "gutil/integral_types.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"
#include "util/telemetry/telemetry.h"

namespace doris {

namespace pipeline {
class PipelineXLocalStateBase;
class PipelineXSinkLocalStateBase;
} // namespace pipeline

class DescriptorTbl;
class ObjectPool;
class ExecEnv;
class RuntimeFilterMgr;
class MemTrackerLimiter;
class QueryContext;

// A collection of items that are part of the global state of a
// query and shared across all execution nodes of that query.
class RuntimeState {
    ENABLE_FACTORY_CREATOR(RuntimeState);

public:
    // for ut only
    RuntimeState(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env);

    RuntimeState(const TPlanFragmentExecParams& fragment_exec_params,
                 const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                 ExecEnv* exec_env);

    RuntimeState(const TPipelineInstanceParams& pipeline_params, const TUniqueId& query_id,
                 int32 fragment_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env);

    // Used by pipelineX. This runtime state is only used for setup.
    RuntimeState(const TUniqueId& query_id, int32 fragment_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env);

    // RuntimeState for executing expr in fe-support.
    RuntimeState(const TQueryGlobals& query_globals);

    // for job task only
    RuntimeState();

    // Empty d'tor to avoid issues with unique_ptr.
    ~RuntimeState();

    // Set per-query state.
    Status init(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                const TQueryGlobals& query_globals, ExecEnv* exec_env);

    // for ut and non-query.
    void set_exec_env(ExecEnv* exec_env) { _exec_env = exec_env; }
    void init_mem_trackers(const TUniqueId& id = TUniqueId(), const std::string& name = "unknown");

    const TQueryOptions& query_options() const { return _query_options; }
    int64_t scan_queue_mem_limit() const {
        return _query_options.__isset.scan_queue_mem_limit ? _query_options.scan_queue_mem_limit
                                                           : _query_options.mem_limit / 20;
    }
    ObjectPool* obj_pool() const { return _obj_pool.get(); }

    const DescriptorTbl& desc_tbl() const { return *_desc_tbl; }
    void set_desc_tbl(const DescriptorTbl* desc_tbl) { _desc_tbl = desc_tbl; }
    int batch_size() const { return _query_options.batch_size; }
    bool abort_on_error() const { return _query_options.abort_on_error; }
    bool abort_on_default_limit_exceeded() const {
        return _query_options.abort_on_default_limit_exceeded;
    }
    int query_parallel_instance_num() const { return _query_options.parallel_instance; }
    int max_errors() const { return _query_options.max_errors; }
    int execution_timeout() const {
        return _query_options.__isset.execution_timeout ? _query_options.execution_timeout
                                                        : _query_options.query_timeout;
    }
    int max_io_buffers() const { return _query_options.max_io_buffers; }
    int num_scanner_threads() const { return _query_options.num_scanner_threads; }
    TQueryType::type query_type() const { return _query_options.query_type; }
    int64_t timestamp_ms() const { return _timestamp_ms; }
    int32_t nano_seconds() const { return _nano_seconds; }
    const std::string& timezone() const { return _timezone; }
    const cctz::time_zone& timezone_obj() const { return _timezone_obj; }
    const std::string& user() const { return _user; }
    const TUniqueId& query_id() const { return _query_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    // should only be called in pipeline engine
    int32_t fragment_id() const { return _fragment_id; }
    ExecEnv* exec_env() { return _exec_env; }
    std::shared_ptr<MemTrackerLimiter> query_mem_tracker() const;

    // Returns runtime state profile
    RuntimeProfile* runtime_profile() { return &_profile; }
    RuntimeProfile* load_channel_profile() { return &_load_channel_profile; }

    bool enable_function_pushdown() const {
        return _query_options.__isset.enable_function_pushdown &&
               _query_options.enable_function_pushdown;
    }

    bool check_overflow_for_decimal() const {
        return _query_options.__isset.check_overflow_for_decimal &&
               _query_options.check_overflow_for_decimal;
    }

    bool enable_common_expr_pushdown() const {
        return _query_options.__isset.enable_common_expr_pushdown &&
               _query_options.enable_common_expr_pushdown;
    }

    Status query_status();

    // Appends error to the _error_log if there is space
    bool log_error(const std::string& error);

    // Returns true if the error log has not reached _max_errors.
    bool log_has_space() {
        std::lock_guard<std::mutex> l(_error_log_lock);
        return _error_log.size() < _query_options.max_errors;
    }

    // Append all _error_log[_unreported_error_idx+] to new_errors and set
    // _unreported_error_idx to _errors_log.size()
    void get_unreported_errors(std::vector<std::string>* new_errors);

    [[nodiscard]] bool is_cancelled() const;
    int codegen_level() const { return _query_options.codegen_level; }
    void set_is_cancelled(bool v, std::string msg) {
        _is_cancelled.store(v);
        // Create a error status, so that we could print error stack, and
        // we could know which path call cancel.
        LOG(WARNING) << "Task is cancelled, instance: "
                     << PrintInstanceStandardInfo(_query_id, _fragment_id, _fragment_instance_id)
                     << " st = " << Status::Error<ErrorCode::CANCELLED>(msg);
    }

    void set_backend_id(int64_t backend_id) { _backend_id = backend_id; }
    int64_t backend_id() const { return _backend_id; }

    void set_be_number(int be_number) { _be_number = be_number; }
    int be_number(void) const { return _be_number; }

    // Sets _process_status with err_msg if no error has been set yet.
    void set_process_status(const std::string& err_msg) {
        std::lock_guard<std::mutex> l(_process_status_lock);
        if (!_process_status.ok()) {
            return;
        }
        _process_status = Status::InternalError(err_msg);
    }

    void set_process_status(const Status& status) {
        if (status.ok()) {
            return;
        }
        std::lock_guard<std::mutex> l(_process_status_lock);
        if (!_process_status.ok()) {
            return;
        }
        _process_status = status;
    }

    // Sets _process_status to MEM_LIMIT_EXCEEDED.
    // Subsequent calls to this will be no-ops. Returns _process_status.
    // If 'msg' is non-nullptr, it will be appended to query_status_ in addition to the
    // generic "Memory limit exceeded" error.
    Status set_mem_limit_exceeded(const std::string& msg = "Memory limit exceeded");

    // Returns a non-OK status if query execution should stop (e.g., the query was cancelled
    // or a mem limit was exceeded). Exec nodes should check this periodically so execution
    // doesn't continue if the query terminates abnormally.
    Status check_query_state(const std::string& msg);

    std::vector<std::string>& output_files() { return _output_files; }

    void set_import_label(const std::string& import_label) { _import_label = import_label; }

    const std::vector<std::string>& export_output_files() const { return _export_output_files; }

    void add_export_output_file(const std::string& file) { _export_output_files.push_back(file); }

    void set_db_name(const std::string& db_name) { _db_name = db_name; }

    const std::string& db_name() { return _db_name; }

    void set_wal_id(int64_t wal_id) { _wal_id = wal_id; }

    int64_t wal_id() { return _wal_id; }

    const std::string& import_label() { return _import_label; }

    const std::string& load_dir() const { return _load_dir; }

    void set_load_job_id(int64_t job_id) { _load_job_id = job_id; }

    int64_t load_job_id() const { return _load_job_id; }

    const std::string get_error_log_file_path() const { return _error_log_file_path; }

    // append error msg and error line to file when loading data.
    // is_summary is true, means we are going to write the summary line
    // If we need to stop the processing, set stop_processing to true
    Status append_error_msg_to_file(std::function<std::string()> line,
                                    std::function<std::string()> error_msg, bool* stop_processing,
                                    bool is_summary = false);

    int64_t num_bytes_load_total() { return _num_bytes_load_total.load(); }

    int64_t num_finished_range() { return _num_finished_scan_range.load(); }

    int64_t num_rows_load_total() { return _num_rows_load_total.load(); }

    int64_t num_rows_load_filtered() { return _num_rows_load_filtered.load(); }

    int64_t num_rows_load_unselected() { return _num_rows_load_unselected.load(); }

    int64_t num_rows_filtered_in_strict_mode_partial_update() {
        return _num_rows_filtered_in_strict_mode_partial_update;
    }

    int64_t num_rows_load_success() {
        return num_rows_load_total() - num_rows_load_filtered() - num_rows_load_unselected();
    }

    void update_num_rows_load_total(int64_t num_rows) { _num_rows_load_total.fetch_add(num_rows); }

    void set_num_rows_load_total(int64_t num_rows) { _num_rows_load_total.store(num_rows); }

    void update_num_bytes_load_total(int64_t bytes_load) {
        _num_bytes_load_total.fetch_add(bytes_load);
    }

    void update_num_finished_scan_range(int64_t finished_range) {
        _num_finished_scan_range.fetch_add(finished_range);
    }

    void update_num_rows_load_filtered(int64_t num_rows) {
        _num_rows_load_filtered.fetch_add(num_rows);
    }

    void update_num_rows_load_unselected(int64_t num_rows) {
        _num_rows_load_unselected.fetch_add(num_rows);
    }

    void set_num_rows_filtered_in_strict_mode_partial_update(int64_t num_rows) {
        _num_rows_filtered_in_strict_mode_partial_update = num_rows;
    }

    void set_per_fragment_instance_idx(int idx) { _per_fragment_instance_idx = idx; }

    int per_fragment_instance_idx() const { return _per_fragment_instance_idx; }

    void set_num_per_fragment_instances(int num_instances) {
        _num_per_fragment_instances = num_instances;
    }

    int num_per_fragment_instances() const { return _num_per_fragment_instances; }

    bool disable_stream_preaggregations() const {
        return _query_options.disable_stream_preaggregations;
    }

    bool enable_spill() const { return _query_options.enable_spilling; }

    int32_t runtime_filter_wait_time_ms() const {
        return _query_options.runtime_filter_wait_time_ms;
    }

    int32_t runtime_filter_max_in_num() const { return _query_options.runtime_filter_max_in_num; }

    int be_exec_version() const {
        if (!_query_options.__isset.be_exec_version) {
            return 0;
        }
        return _query_options.be_exec_version;
    }
    bool enable_pipeline_exec() const {
        return _query_options.__isset.enable_pipeline_engine &&
               _query_options.enable_pipeline_engine;
    }

    bool trim_tailing_spaces_for_external_table_query() const {
        return _query_options.trim_tailing_spaces_for_external_table_query;
    }

    bool return_object_data_as_binary() const {
        return _query_options.return_object_data_as_binary;
    }

    bool enable_exchange_node_parallel_merge() const {
        return _query_options.enable_enable_exchange_node_parallel_merge;
    }

    segment_v2::CompressionTypePB fragement_transmission_compression_type() const {
        if (_query_options.__isset.fragment_transmission_compression_codec) {
            if (_query_options.fragment_transmission_compression_codec == "lz4") {
                return segment_v2::CompressionTypePB::LZ4;
            }
        }
        return segment_v2::CompressionTypePB::SNAPPY;
    }

    bool skip_storage_engine_merge() const {
        return _query_options.__isset.skip_storage_engine_merge &&
               _query_options.skip_storage_engine_merge;
    }

    bool skip_delete_predicate() const {
        return _query_options.__isset.skip_delete_predicate && _query_options.skip_delete_predicate;
    }

    bool skip_delete_bitmap() const {
        return _query_options.__isset.skip_delete_bitmap && _query_options.skip_delete_bitmap;
    }

    bool enable_page_cache() const;

    int partitioned_hash_join_rows_threshold() const {
        if (!_query_options.__isset.partitioned_hash_join_rows_threshold) {
            return 0;
        }
        return _query_options.partitioned_hash_join_rows_threshold;
    }

    int partitioned_hash_agg_rows_threshold() const {
        if (!_query_options.__isset.partitioned_hash_agg_rows_threshold) {
            return 0;
        }
        return _query_options.partitioned_hash_agg_rows_threshold;
    }

    const std::vector<TTabletCommitInfo>& tablet_commit_infos() const {
        return _tablet_commit_infos;
    }

    std::vector<TTabletCommitInfo>& tablet_commit_infos() { return _tablet_commit_infos; }

    const std::vector<TErrorTabletInfo>& error_tablet_infos() const { return _error_tablet_infos; }

    std::vector<TErrorTabletInfo>& error_tablet_infos() { return _error_tablet_infos; }

    // get mem limit for load channel
    // if load mem limit is not set, or is zero, using query mem limit instead.
    int64_t get_load_mem_limit();

    RuntimeFilterMgr* runtime_filter_mgr() { return _runtime_filter_mgr.get(); }

    void set_query_ctx(QueryContext* ctx) { _query_ctx = ctx; }

    QueryContext* get_query_ctx() { return _query_ctx; }

    void set_query_mem_tracker(const std::shared_ptr<MemTrackerLimiter>& tracker) {
        _query_mem_tracker = tracker;
    }

    OpentelemetryTracer get_tracer() { return _tracer; }

    void set_tracer(OpentelemetryTracer&& tracer) { _tracer = std::move(tracer); }

    bool enable_profile() const {
        return _query_options.__isset.enable_profile && _query_options.enable_profile;
    }

    bool enable_scan_node_run_serial() const {
        return _query_options.__isset.enable_scan_node_run_serial &&
               _query_options.enable_scan_node_run_serial;
    }

    bool enable_share_hash_table_for_broadcast_join() const {
        return _query_options.__isset.enable_share_hash_table_for_broadcast_join &&
               _query_options.enable_share_hash_table_for_broadcast_join;
    }

    bool enable_hash_join_early_start_probe() const {
        return _query_options.__isset.enable_hash_join_early_start_probe &&
               _query_options.enable_hash_join_early_start_probe;
    }

    int repeat_max_num() const {
#ifndef BE_TEST
        if (!_query_options.__isset.repeat_max_num) {
            return 10000;
        }
        return _query_options.repeat_max_num;
#else
        return 10;
#endif
    }

    int64_t external_sort_bytes_threshold() const {
        if (_query_options.__isset.external_sort_bytes_threshold) {
            return _query_options.external_sort_bytes_threshold;
        }
        return 0;
    }

    void set_be_exec_version(int32_t version) noexcept { _query_options.be_exec_version = version; }

    int64_t external_agg_bytes_threshold() const {
        return _query_options.__isset.external_agg_bytes_threshold
                       ? _query_options.external_agg_bytes_threshold
                       : 0;
    }

    inline bool enable_delete_sub_pred_v2() const {
        return _query_options.__isset.enable_delete_sub_predicate_v2 &&
               _query_options.enable_delete_sub_predicate_v2;
    }

    void emplace_local_state(int id,
                             std::shared_ptr<doris::pipeline::PipelineXLocalStateBase> state);

    std::shared_ptr<doris::pipeline::PipelineXLocalStateBase> get_local_state(int id);

    void emplace_sink_local_state(
            int id, std::shared_ptr<doris::pipeline::PipelineXSinkLocalStateBase> state);

    std::shared_ptr<doris::pipeline::PipelineXSinkLocalStateBase> get_sink_local_state(int id);

private:
    Status create_error_log_file();

    static const int DEFAULT_BATCH_SIZE = 2048;

    std::shared_ptr<MemTrackerLimiter> _query_mem_tracker = nullptr;

    // put runtime state before _obj_pool, so that it will be deconstructed after
    // _obj_pool. Because some of object in _obj_pool will use profile when deconstructing.
    RuntimeProfile _profile;
    RuntimeProfile _load_channel_profile;

    const DescriptorTbl* _desc_tbl;
    std::shared_ptr<ObjectPool> _obj_pool;

    // runtime filter
    std::unique_ptr<RuntimeFilterMgr> _runtime_filter_mgr;

    // Protects _data_stream_recvrs_pool
    std::mutex _data_stream_recvrs_lock;

    // Data stream receivers created by a plan fragment are gathered here to make sure
    // they are destroyed before _obj_pool (class members are destroyed in reverse order).
    // Receivers depend on the descriptor table and we need to guarantee that their control
    // blocks are removed from the data stream manager before the objects in the
    // descriptor table are destroyed.
    std::unique_ptr<ObjectPool> _data_stream_recvrs_pool;

    // Lock protecting _error_log and _unreported_error_idx
    std::mutex _error_log_lock;

    // Logs error messages.
    std::vector<std::string> _error_log;

    // _error_log[_unreported_error_idx+] has been not reported to the coordinator.
    int _unreported_error_idx;

    // Username of user that is executing the query to which this RuntimeState belongs.
    std::string _user;

    //Query-global timestamp_ms
    int64_t _timestamp_ms;
    int32_t _nano_seconds;
    std::string _timezone;
    cctz::time_zone _timezone_obj;

    TUniqueId _query_id;
    // fragment id for each TPipelineFragmentParams
    int32_t _fragment_id;
    TUniqueId _fragment_instance_id;
    TQueryOptions _query_options;
    ExecEnv* _exec_env = nullptr;

    // if true, execution should stop with a CANCELLED status
    std::atomic<bool> _is_cancelled;

    int _per_fragment_instance_idx;
    int _num_per_fragment_instances = 0;

    // The backend id on which this fragment instance runs
    int64_t _backend_id = -1;

    // used as send id
    int _be_number;

    // Non-OK if an error has occurred and query execution should abort. Used only for
    // asynchronously reporting such errors (e.g., when a UDF reports an error), so this
    // will not necessarily be set in all error cases.
    std::mutex _process_status_lock;
    Status _process_status;

    // put here to collect files??
    std::vector<std::string> _output_files;
    std::atomic<int64_t> _num_rows_load_total;      // total rows read from source
    std::atomic<int64_t> _num_rows_load_filtered;   // unqualified rows
    std::atomic<int64_t> _num_rows_load_unselected; // rows filtered by predicates
    std::atomic<int64_t> _num_rows_filtered_in_strict_mode_partial_update;
    std::atomic<int64_t> _num_print_error_rows;

    std::atomic<int64_t> _num_bytes_load_total; // total bytes read from source
    std::atomic<int64_t> _num_finished_scan_range;

    std::vector<std::string> _export_output_files;
    std::string _import_label;
    std::string _db_name;
    std::string _load_dir;
    int64_t _load_job_id;
    int64_t _wal_id = -1;

    // mini load
    int64_t _normal_row_number;
    int64_t _error_row_number;
    std::string _error_log_file_path;
    std::ofstream* _error_log_file = nullptr; // error file path, absolute path
    std::vector<TTabletCommitInfo> _tablet_commit_infos;
    std::vector<TErrorTabletInfo> _error_tablet_infos;

    std::map<int, std::shared_ptr<doris::pipeline::PipelineXLocalStateBase>> _op_id_to_local_state;
    std::map<int, std::shared_ptr<doris::pipeline::PipelineXSinkLocalStateBase>>
            _op_id_to_sink_local_state;

    std::mutex _local_state_lock;
    std::mutex _local_sink_state_lock;

    QueryContext* _query_ctx = nullptr;

    // true if max_filter_ratio is 0
    bool _load_zero_tolerance = false;

    OpentelemetryTracer _tracer = telemetry::get_noop_tracer();

    // prohibit copies
    RuntimeState(const RuntimeState&);
};

#define RETURN_IF_CANCELLED(state)                                                    \
    do {                                                                              \
        if (UNLIKELY((state)->is_cancelled())) return Status::Cancelled("Cancelled"); \
    } while (false)

} // namespace doris
