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

#ifndef DORIS_BE_RUNTIME_PLAN_FRAGMENT_EXECUTOR_H
#define DORIS_BE_RUNTIME_PLAN_FRAGMENT_EXECUTOR_H

#include <boost/scoped_ptr.hpp>
#include <condition_variable>
#include <functional>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/datetime_value.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_state.h"
#include "util/hash_util.hpp"
#include "util/time.h"
#include "vec/core/block.h"

namespace doris {

class QueryFragmentsCtx;
class HdfsFsCache;
class ExecNode;
class RowDescriptor;
class RowBatch;
class DataSink;
class DataStreamMgr;
class RuntimeProfile;
class RuntimeState;
class TNetworkAddress;
class TPlanExecRequest;
class TPlanFragment;
class TPlanFragmentExecParams;
class TPlanExecParams;

// PlanFragmentExecutor handles all aspects of the execution of a single plan fragment,
// including setup and tear-down, both in the success and error case.
// Tear-down frees all memory allocated for this plan fragment and closes all data
// streams; it happens automatically in the d'tor.
//
// The executor makes an aggregated profile for the entire fragment available,
// which includes profile information for the plan itself as well as the output
// sink, if any.
// The ReportStatusCallback passed into the c'tor is invoked periodically to report the
// execution status. The frequency of those reports is controlled by the flag
// status_report_interval; setting that flag to 0 disables periodic reporting altogether
// Regardless of the value of that flag, if a report callback is specified, it is
// invoked at least once at the end of execution with an overall status and profile
// (and 'done' indicator). The only exception is when execution is cancelled, in which
// case the callback is *not* invoked (the coordinator already knows that execution
// stopped, because it initiated the cancellation).
//
// Aside from Cancel(), which may be called asynchronously, this class is not
// thread-safe.
class PlanFragmentExecutor {
public:
    // Callback to report execution status of plan fragment.
    // 'profile' is the cumulative profile, 'done' indicates whether the execution
    // is done or still continuing.
    // Note: this does not take a const RuntimeProfile&, because it might need to call
    // functions like PrettyPrint() or to_thrift(), neither of which is const
    // because they take locks.
    typedef std::function<void(const Status& status, RuntimeProfile* profile, bool done)>
            report_status_callback;

    // report_status_cb, if !empty(), is used to report the accumulated profile
    // information periodically during execution (open() or get_next()).
    PlanFragmentExecutor(ExecEnv* exec_env, const report_status_callback& report_status_cb);

    // Closes the underlying plan fragment and frees up all resources allocated
    // in open()/get_next().
    // It is an error to delete a PlanFragmentExecutor with a report callback
    // before open()/get_next() (depending on whether the fragment has a sink)
    // indicated that execution is finished.
    ~PlanFragmentExecutor();

    // prepare for execution. Call this prior to open().
    // This call won't block.
    // runtime_state() and row_desc() will not be valid until prepare() is called.
    // If request.query_options.mem_limit > 0, it is used as an approximate limit on the
    // number of bytes this query can consume at runtime.
    // The query will be aborted (MEM_LIMIT_EXCEEDED) if it goes over that limit.
    // If fragments_ctx is not null, some components will be got from fragments_ctx.
    Status prepare(const TExecPlanFragmentParams& request,
                   const QueryFragmentsCtx* fragments_ctx = nullptr);

    // Start execution. Call this prior to get_next().
    // If this fragment has a sink, open() will send all rows produced
    // by the fragment to that sink. Therefore, open() may block until
    // all rows are produced (and a subsequent call to get_next() will not return
    // any rows).
    // This also starts the status-reporting thread, if the interval flag
    // is > 0 and a callback was specified in the c'tor.
    // If this fragment has a sink, report_status_cb will have been called for the final
    // time when open() returns, and the status-reporting thread will have been stopped.
    Status open();

    // Return results through 'batch'. Sets '*batch' to NULL if no more results.
    // '*batch' is owned by PlanFragmentExecutor and must not be deleted.
    // When *batch == NULL, get_next() should not be called anymore. Also, report_status_cb
    // will have been called for the final time and the status-reporting thread
    // will have been stopped.
    Status get_next(RowBatch** batch);

    // Closes the underlying plan fragment and frees up all resources allocated
    // in open()/get_next().
    void close();

    // Abort this execution. Must be called if we skip running open().
    // It will let DataSink node closed with error status, to avoid use resources which created in open() phase.
    // DataSink node should distinguish Aborted status from other error status.
    void set_abort();

    // Initiate cancellation. Must not be called until after prepare() returned.
    void cancel();

    // Releases the thread token for this fragment executor.
    void release_thread_token();

    // call these only after prepare()
    RuntimeState* runtime_state() { return _runtime_state.get(); }
    const RowDescriptor& row_desc();

    // Profile information for plan and output sink.
    RuntimeProfile* profile();

    const Status& status() const { return _status; }

    DataSink* get_sink() { return _sink.get(); }

    void set_is_report_on_cancel(bool val) { _is_report_on_cancel = val; }

private:
    ExecEnv* _exec_env; // not owned
    ExecNode* _plan;    // lives in _runtime_state->obj_pool()
    TUniqueId _query_id;
    std::shared_ptr<MemTracker> _mem_tracker;

    // profile reporting-related
    report_status_callback _report_status_cb;
    boost::thread _report_thread;
    std::mutex _report_thread_lock;

    // Indicates that profile reporting thread should stop.
    // Tied to _report_thread_lock.
    std::condition_variable _stop_report_thread_cv;

    // Indicates that profile reporting thread started.
    // Tied to _report_thread_lock.
    std::condition_variable _report_thread_started_cv;
    bool _report_thread_active; // true if we started the thread

    // true if _plan->get_next() indicated that it's done
    bool _done;

    // true if prepare() returned OK
    bool _prepared;

    // true if close() has been called
    bool _closed;

    // true if this fragment has not returned the thread token to the thread resource mgr
    bool _has_thread_token;

    bool _is_report_success;

    // If this is set to false, and '_is_report_success' is false as well,
    // This executor will not report status to FE on being cancelled.
    bool _is_report_on_cancel;

    // Overall execution status. Either ok() or set to the first error status that
    // was encountered.
    Status _status;

    // Protects _status
    // lock ordering:
    // 1. _report_thread_lock
    // 2. _status_lock
    std::mutex _status_lock;

    // note that RuntimeState should be constructed before and destructed after `_sink' and `_row_batch',
    // therefore we declare it before `_sink' and `_row_batch'
    boost::scoped_ptr<RuntimeState> _runtime_state;
    // Output sink for rows sent to this fragment. May not be set, in which case rows are
    // returned via get_next's row batch
    // Created in prepare (if required), owned by this object.
    boost::scoped_ptr<DataSink> _sink;
    boost::scoped_ptr<RowBatch> _row_batch;
    std::unique_ptr<doris::vectorized::Block> _block;

    // Number of rows returned by this fragment
    RuntimeProfile::Counter* _rows_produced_counter;

    RuntimeProfile::Counter* _fragment_cpu_timer;

    // Average number of thread tokens for the duration of the plan fragment execution.
    // Fragments that do a lot of cpu work (non-coordinator fragment) will have at
    // least 1 token.  Fragments that contain a hdfs scan node will have 1+ tokens
    // depending on system load.  Other nodes (e.g. hash join node) can also reserve
    // additional tokens.
    // This is a measure of how much CPU resources this fragment used during the course
    // of the execution.
    RuntimeProfile::Counter* _average_thread_tokens;

    // It is shared with BufferControlBlock and will be called in two different
    // threads. But their calls are all at different time, there is no problem of
    // multithreaded access.
    std::shared_ptr<QueryStatistics> _query_statistics;
    bool _collect_query_statistics_with_every_batch;

    bool _enable_vectorized_engine;

    ObjectPool* obj_pool() { return _runtime_state->obj_pool(); }

    // typedef for TPlanFragmentExecParams.per_node_scan_ranges
    typedef std::map<TPlanNodeId, std::vector<TScanRangeParams>> PerNodeScanRanges;

    // Main loop of profile reporting thread.
    // Exits when notified on _done_cv.
    // On exit, *no report is sent*, ie, this will not send the final report.
    void report_profile();

    // Invoked the report callback if there is a report callback and the current
    // status isn't CANCELLED. Sets 'done' to true in the callback invocation if
    // done == true or we have an error status.
    void send_report(bool done);

    // If _status.ok(), sets _status to status.
    // If we're transitioning to an error status, stops report thread and
    // sends a final report.
    void update_status(const Status& status);

    // Executes open() logic and returns resulting status. Does not set _status.
    // If this plan fragment has no sink, open_internal() does nothing.
    // If this plan fragment has a sink and open_internal() returns without an
    // error condition, all rows will have been sent to the sink, the sink will
    // have been closed, a final report will have been sent and the report thread will
    // have been stopped. _sink will be set to NULL after successful execution.
    Status open_internal();
    Status open_vectorized_internal();

    // Executes get_next() logic and returns resulting status.
    Status get_next_internal(RowBatch** batch);
    Status get_vectorized_internal(::doris::vectorized::Block** block);

    // Stops report thread, if one is running. Blocks until report thread terminates.
    // Idempotent.
    void stop_report_thread();

    const DescriptorTbl& desc_tbl() { return _runtime_state->desc_tbl(); }

    void _collect_query_statistics();
};

// Save the common components of fragments in a query.
// Some components like DescriptorTbl may be very large
// that will slow down each execution of fragments when DeSer them every time.
class QueryFragmentsCtx {
public:
    QueryFragmentsCtx(int total_fragment_num)
            : fragment_num(total_fragment_num), timeout_second(-1) {
        _start_time = DateTimeValue::local_time();
    }

    bool countdown() { return fragment_num.fetch_sub(1) == 1; }

    bool is_timeout(const DateTimeValue& now) const {
        if (timeout_second <= 0) {
            return false;
        }
        if (now.second_diff(_start_time) > timeout_second) {
            return true;
        }
        return false;
    }

public:
    TUniqueId query_id;
    DescriptorTbl* desc_tbl;
    bool set_rsc_info = false;
    std::string user;
    std::string group;
    TNetworkAddress coord_addr;
    TQueryGlobals query_globals;

    /// In the current implementation, for multiple fragments executed by a query on the same BE node,
    /// we store some common components in QueryFragmentsCtx, and save QueryFragmentsCtx in FragmentMgr.
    /// When all Fragments are executed, QueryFragmentsCtx needs to be deleted from FragmentMgr.
    /// Here we use a counter to store the number of Fragments that have not yet been completed,
    /// and after each Fragment is completed, this value will be reduced by one.
    /// When the last Fragment is completed, the counter is cleared, and the worker thread of the last Fragment
    /// will clean up QueryFragmentsCtx.
    std::atomic<int> fragment_num;
    int timeout_second;
    ObjectPool obj_pool;

private:
    DateTimeValue _start_time;
};

} // namespace doris

#endif
