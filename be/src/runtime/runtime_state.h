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

#ifndef DORIS_BE_SRC_QUERY_RUNTIME_RUNTIME_STATE_H
#define DORIS_BE_SRC_QUERY_RUNTIME_RUNTIME_STATE_H

#include <atomic>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "cctz/time_zone.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "gen_cpp/PaloInternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"               // for TUniqueId
#include "runtime/mem_pool.h"
#include "runtime/query_fragments_ctx.h"
#include "runtime/thread_resource_mgr.h"
#include "util/logging.h"
#include "util/runtime_profile.h"

namespace doris {

class DescriptorTbl;
class ObjectPool;
class Status;
class ExecEnv;
class Expr;
class DateTimeValue;
class MemTracker;
class DataStreamRecvr;
class ResultBufferMgr;
class DiskIoMgrs;
class TmpFileMgr;
class BufferedBlockMgr;
class BufferedBlockMgr2;
class LoadErrorHub;
class ReservationTracker;
class InitialReservations;
class RowDescriptor;
class RuntimeFilterMgr;

// A collection of items that are part of the global state of a
// query and shared across all execution nodes of that query.
class RuntimeState {
public:
    // for ut only
    RuntimeState(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env);

    RuntimeState(const TPlanFragmentExecParams& fragment_exec_params,
                 const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                 ExecEnv* exec_env);

    // RuntimeState for executing expr in fe-support.
    RuntimeState(const TQueryGlobals& query_globals);

    // Empty d'tor to avoid issues with unique_ptr.
    ~RuntimeState();

    // Set per-query state.
    Status init(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                const TQueryGlobals& query_globals, ExecEnv* exec_env);

    // Set up four-level hierarchy of mem trackers: process, query, fragment instance.
    // The instance tracker is tied to our profile.
    // Specific parts of the fragment (i.e. exec nodes, sinks, data stream senders, etc)
    // will add a fourth level when they are initialized.
    Status init_mem_trackers(const TUniqueId& query_id);

    // for ut only
    Status init_instance_mem_tracker();

    /// Called from Init() to set up buffer reservations and the file group.
    Status init_buffer_poolstate();

    // Gets/Creates the query wide block mgr.
    Status create_block_mgr();

    Status create_load_dir();

    const TQueryOptions& query_options() const { return _query_options; }
    ObjectPool* obj_pool() const { return _obj_pool.get(); }

    std::shared_ptr<ObjectPool> obj_pool_ptr() const { return _obj_pool; }

    const DescriptorTbl& desc_tbl() const { return *_desc_tbl; }
    void set_desc_tbl(DescriptorTbl* desc_tbl) { _desc_tbl = desc_tbl; }
    int batch_size() const { return _query_options.batch_size; }
    bool abort_on_error() const { return _query_options.abort_on_error; }
    bool abort_on_default_limit_exceeded() const {
        return _query_options.abort_on_default_limit_exceeded;
    }
    int max_errors() const { return _query_options.max_errors; }
    int max_io_buffers() const { return _query_options.max_io_buffers; }
    int num_scanner_threads() const { return _query_options.num_scanner_threads; }
    TQueryType::type query_type() const { return _query_options.query_type; }
    int64_t timestamp_ms() const { return _timestamp_ms; }
    const std::string& timezone() const { return _timezone; }
    const cctz::time_zone& timezone_obj() const { return _timezone_obj; }
    const std::string& user() const { return _user; }
    const std::vector<std::string>& error_log() const { return _error_log; }
    const TUniqueId& query_id() const { return _query_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    ExecEnv* exec_env() { return _exec_env; }
    std::shared_ptr<MemTracker> query_mem_tracker() { return _query_mem_tracker; }
    std::shared_ptr<MemTracker> instance_mem_tracker() { return _instance_mem_tracker; }
    ThreadResourceMgr::ResourcePool* resource_pool() { return _resource_pool; }

    void set_fragment_root_id(PlanNodeId id) {
        DCHECK(_root_node_id == -1) << "Should not set this twice.";
        _root_node_id = id;
    }

    // The seed value to use when hashing tuples.
    // See comment on _root_node_id. We add one to prevent having a hash seed of 0.
    uint32_t fragment_hash_seed() const { return _root_node_id + 1; }

    // Returns runtime state profile
    RuntimeProfile* runtime_profile() { return &_profile; }

    // Returns true if codegen is enabled for this query.
    bool codegen_enabled() const { return !_query_options.disable_codegen; }

    // Create a codegen object in _codegen. No-op if it has already been called.
    // If codegen is enabled for the query, this is created when the runtime
    // state is created. If codegen is disabled for the query, this is created
    // on first use.
    Status create_codegen();

    BufferedBlockMgr2* block_mgr2() {
        DCHECK(_block_mgr2.get() != nullptr);
        return _block_mgr2.get();
    }

    Status query_status() {
        std::lock_guard<std::mutex> l(_process_status_lock);
        return _process_status;
    };

    // Appends error to the _error_log if there is space
    bool log_error(const std::string& error);

    // If !status.ok(), appends the error to the _error_log
    void log_error(const Status& status);

    // Returns true if the error log has not reached _max_errors.
    bool log_has_space() {
        std::lock_guard<std::mutex> l(_error_log_lock);
        return _error_log.size() < _query_options.max_errors;
    }

    // Return true if error log is empty.
    bool error_log_is_empty();

    // Returns the error log lines as a string joined with '\n'.
    std::string error_log();

    // Append all _error_log[_unreported_error_idx+] to new_errors and set
    // _unreported_error_idx to _errors_log.size()
    void get_unreported_errors(std::vector<std::string>* new_errors);

    bool is_cancelled() const { return _is_cancelled; }
    int codegen_level() const { return _query_options.codegen_level; }
    void set_is_cancelled(bool v) { _is_cancelled = v; }

    void set_backend_id(int64_t backend_id) { _backend_id = backend_id; }
    int64_t backend_id() const { return _backend_id; }

    void set_be_number(int be_number) { _be_number = be_number; }
    int be_number(void) { return _be_number; }

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

    const std::string& import_label() { return _import_label; }

    const std::vector<std::string>& export_output_files() const { return _export_output_files; }

    void add_export_output_file(const std::string& file) { _export_output_files.push_back(file); }

    void set_db_name(const std::string& db_name) { _db_name = db_name; }

    const std::string& db_name() { return _db_name; }

    const std::string& load_dir() const { return _load_dir; }

    void set_load_job_id(int64_t job_id) { _load_job_id = job_id; }

    const int64_t load_job_id() { return _load_job_id; }

    // we only initialize object for load jobs
    void set_load_error_hub_info(const TLoadErrorHubInfo& hub_info) {
        TLoadErrorHubInfo* info = new TLoadErrorHubInfo(hub_info);
        _load_error_hub_info.reset(info);
    }

    // only can be invoded after set its value
    const TLoadErrorHubInfo* load_error_hub_info() {
        // DCHECK(_load_error_hub_info != nullptr);
        return _load_error_hub_info.get();
    }

    const int64_t get_normal_row_number() const { return _normal_row_number; }

    const void set_normal_row_number(int64_t number) { _normal_row_number = number; }

    const int64_t get_error_row_number() const { return _error_row_number; }

    const void set_error_row_number(int64_t number) { _error_row_number = number; }

    const std::string get_error_log_file_path() const { return _error_log_file_path; }

    // append error msg and error line to file when loading data.
    // is_summary is true, means we are going to write the summary line
    // If we need to stop the processing, set stop_processing to true
    Status append_error_msg_to_file(std::function<std::string()> line,
                                    std::function<std::string()> error_msg, bool* stop_processing,
                                    bool is_summary = false);

    int64_t num_bytes_load_total() { return _num_bytes_load_total.load(); }

    int64_t num_rows_load_total() { return _num_rows_load_total.load(); }

    int64_t num_rows_load_filtered() { return _num_rows_load_filtered.load(); }

    int64_t num_rows_load_unselected() { return _num_rows_load_unselected.load(); }

    int64_t num_rows_load_success() {
        return num_rows_load_total() - num_rows_load_filtered() - num_rows_load_unselected();
    }

    void update_num_rows_load_total(int64_t num_rows) { _num_rows_load_total.fetch_add(num_rows); }

    void set_num_rows_load_total(int64_t num_rows) { _num_rows_load_total.store(num_rows); }

    void update_num_bytes_load_total(int64_t bytes_load) {
        _num_bytes_load_total.fetch_add(bytes_load);
    }

    void update_num_rows_load_filtered(int64_t num_rows) {
        _num_rows_load_filtered.fetch_add(num_rows);
    }

    void update_num_rows_load_unselected(int64_t num_rows) {
        _num_rows_load_unselected.fetch_add(num_rows);
    }

    void export_load_error(const std::string& error_msg);

    void set_per_fragment_instance_idx(int idx) { _per_fragment_instance_idx = idx; }

    int per_fragment_instance_idx() const { return _per_fragment_instance_idx; }

    void set_num_per_fragment_instances(int num_instances) {
        _num_per_fragment_instances = num_instances;
    }

    int num_per_fragment_instances() const { return _num_per_fragment_instances; }

    ReservationTracker* instance_buffer_reservation() { return _instance_buffer_reservation.get(); }

    int64_t min_reservation() { return _query_options.min_reservation; }

    int64_t max_reservation() { return _query_options.max_reservation; }

    bool disable_stream_preaggregations() { return _query_options.disable_stream_preaggregations; }

    bool enable_spill() const { return _query_options.enable_spilling; }

    int32_t runtime_filter_wait_time_ms() { return _query_options.runtime_filter_wait_time_ms; }

    int32_t runtime_filter_max_in_num() { return _query_options.runtime_filter_max_in_num; }

    bool enable_vectorized_exec() const { return _query_options.enable_vectorized_engine; }

    bool return_object_data_as_binary() const {
        return _query_options.return_object_data_as_binary;
    }

    bool enable_exchange_node_parallel_merge() const {
        return _query_options.enable_enable_exchange_node_parallel_merge;
    }

    // the following getters are only valid after Prepare()
    InitialReservations* initial_reservations() const { return _initial_reservations; }

    ReservationTracker* buffer_reservation() const { return _buffer_reservation; }

    const std::vector<TTabletCommitInfo>& tablet_commit_infos() const {
        return _tablet_commit_infos;
    }

    std::vector<TTabletCommitInfo>& tablet_commit_infos() { return _tablet_commit_infos; }

    const std::vector<TErrorTabletInfo>& error_tablet_infos() const { return _error_tablet_infos; }

    std::vector<TErrorTabletInfo>& error_tablet_infos() { return _error_tablet_infos; }

    /// Helper to call QueryState::StartSpilling().
    Status StartSpilling(MemTracker* mem_tracker);

    // get mem limit for load channel
    // if load mem limit is not set, or is zero, using query mem limit instead.
    int64_t get_load_mem_limit();

    RuntimeFilterMgr* runtime_filter_mgr() { return _runtime_filter_mgr.get(); }

    void set_query_fragments_ctx(QueryFragmentsCtx* ctx) { _query_ctx = ctx; }

    QueryFragmentsCtx* get_query_fragments_ctx() { return _query_ctx; }

private:
    // Use a custom block manager for the query for testing purposes.
    void set_block_mgr2(const std::shared_ptr<BufferedBlockMgr2>& block_mgr) {
        _block_mgr2 = block_mgr;
    }

    Status create_error_log_file();

    static const int DEFAULT_BATCH_SIZE = 2048;

    // MemTracker that is shared by all fragment instances running on this host.
    // The query mem tracker must be released after the _instance_mem_tracker.
    std::shared_ptr<MemTracker> _query_mem_tracker;

    // Memory usage of this fragment instance
    std::shared_ptr<MemTracker> _instance_mem_tracker;

    // put runtime state before _obj_pool, so that it will be deconstructed after
    // _obj_pool. Because some of object in _obj_pool will use profile when deconstructing.
    RuntimeProfile _profile;

    DescriptorTbl* _desc_tbl;
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
    std::string _timezone;
    cctz::time_zone _timezone_obj;

    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    TQueryOptions _query_options;
    ExecEnv* _exec_env = nullptr;

    // Thread resource management object for this fragment's execution.  The runtime
    // state is responsible for returning this pool to the thread mgr.
    ThreadResourceMgr::ResourcePool* _resource_pool;

    // if true, execution should stop with a CANCELLED status
    bool _is_cancelled;

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
    //std::unique_ptr<MemPool> _udf_pool;

    // BufferedBlockMgr object used to allocate and manage blocks of input data in memory
    // with a fixed memory budget.
    // The block mgr is shared by all fragments for this query.
    std::shared_ptr<BufferedBlockMgr2> _block_mgr2;

    // This is the node id of the root node for this plan fragment. This is used as the
    // hash seed and has two useful properties:
    // 1) It is the same for all exec nodes in a fragment, so the resulting hash values
    // can be shared (i.e. for _slot_bitmap_filters).
    // 2) It is different between different fragments, so we do not run into hash
    // collisions after data partitioning (across fragments). See IMPALA-219 for more
    // details.
    PlanNodeId _root_node_id;

    // put here to collect files??
    std::vector<std::string> _output_files;
    std::atomic<int64_t> _num_rows_load_total;      // total rows read from source
    std::atomic<int64_t> _num_rows_load_filtered;   // unqualified rows
    std::atomic<int64_t> _num_rows_load_unselected; // rows filtered by predicates
    std::atomic<int64_t> _num_print_error_rows;

    std::atomic<int64_t> _num_bytes_load_total; // total bytes read from source

    std::vector<std::string> _export_output_files;

    std::string _import_label;
    std::string _db_name;
    std::string _load_dir;
    int64_t _load_job_id;
    std::unique_ptr<TLoadErrorHubInfo> _load_error_hub_info;

    // mini load
    int64_t _normal_row_number;
    int64_t _error_row_number;
    std::string _error_log_file_path;
    std::ofstream* _error_log_file = nullptr; // error file path, absolute path
    std::unique_ptr<LoadErrorHub> _error_hub;
    std::mutex _create_error_hub_lock;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;
    std::vector<TErrorTabletInfo> _error_tablet_infos;

    //TODO chenhao , remove this to QueryState
    /// Pool of buffer reservations used to distribute initial reservations to operators
    /// in the query. Contains a ReservationTracker that is a child of
    /// 'buffer_reservation_'. Owned by 'obj_pool_'. Set in Prepare().
    ReservationTracker* _buffer_reservation = nullptr;

    /// Buffer reservation for this fragment instance - a child of the query buffer
    /// reservation. Non-nullptr if 'query_state_' is not nullptr.
    std::unique_ptr<ReservationTracker> _instance_buffer_reservation;

    /// Pool of buffer reservations used to distribute initial reservations to operators
    /// in the query. Contains a ReservationTracker that is a child of
    /// 'buffer_reservation_'. Owned by 'obj_pool_'. Set in Prepare().
    InitialReservations* _initial_reservations = nullptr;

    /// Number of fragment instances executing, which may need to claim
    /// from 'initial_reservations_'.
    /// TODO: not needed if we call ReleaseResources() in a timely manner (IMPALA-1575).
    std::atomic<int32_t> _initial_reservation_refcnt {0};

    QueryFragmentsCtx* _query_ctx;

    // true if max_filter_ratio is 0
    bool _load_zero_tolerance = false;

    // prohibit copies
    RuntimeState(const RuntimeState&);
};

#define RETURN_IF_CANCELLED(state)                                                    \
    do {                                                                              \
        if (UNLIKELY((state)->is_cancelled())) return Status::Cancelled("Cancelled"); \
    } while (false)

} // namespace doris

#endif // end of DORIS_BE_SRC_QUERY_RUNTIME_RUNTIME_STATE_H
