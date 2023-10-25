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
// https://github.com/cloudera/Impala/blob/v0.7refresh/be/src/runtime/plan-fragment-executor.h
// and modified by Doris

#pragma once

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <condition_variable>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris {

class QueryContext;
class ExecNode;
class RowDescriptor;
class DataSink;
class DescriptorTbl;
class ExecEnv;
class ObjectPool;
class QueryStatistics;

namespace vectorized {
class Block;
} // namespace vectorized

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
    using report_status_callback =
            std::function<void(const Status&, RuntimeProfile*, RuntimeProfile*, bool)>;

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
    // If query_ctx is not null, some components will be got from query_ctx.
    Status prepare(const TExecPlanFragmentParams& request, QueryContext* query_ctx = nullptr);

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

    // Closes the underlying plan fragment and frees up all resources allocated
    // in open()/get_next().
    void close();

    // Initiate cancellation. Must not be called until after prepare() returned.
    void cancel(const PPlanFragmentCancelReason& reason = PPlanFragmentCancelReason::INTERNAL_ERROR,
                const std::string& msg = "");

    // call these only after prepare()
    RuntimeState* runtime_state() { return _runtime_state.get(); }
    const RowDescriptor& row_desc();

    // Profile information for plan and output sink.
    RuntimeProfile* profile();
    RuntimeProfile* load_channel_profile();

    const Status& status() const { return _status; }

    DataSink* get_sink() const { return _sink.get(); }

    void set_is_report_on_cancel(bool val) { _is_report_on_cancel = val; }

private:
    ExecEnv* _exec_env; // not owned
    ExecNode* _plan;    // lives in _runtime_state->obj_pool()
    TUniqueId _query_id;

    // profile reporting-related
    report_status_callback _report_status_cb;
    std::promise<bool> _report_thread_promise;
    std::future<bool> _report_thread_future;
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
    std::unique_ptr<RuntimeState> _runtime_state;
    // Output sink for rows sent to this fragment. May not be set, in which case rows are
    // returned via get_next's row batch
    // Created in prepare (if required), owned by this object.
    std::unique_ptr<DataSink> _sink;

    // Number of rows returned by this fragment
    RuntimeProfile::Counter* _rows_produced_counter;

    // Number of blocks returned by this fragment
    RuntimeProfile::Counter* _blocks_produced_counter;

    RuntimeProfile::Counter* _fragment_cpu_timer;

    // It is shared with BufferControlBlock and will be called in two different
    // threads. But their calls are all at different time, there is no problem of
    // multithreaded access.
    std::shared_ptr<QueryStatistics> _query_statistics;
    bool _collect_query_statistics_with_every_batch;

    // Record the cancel information when calling the cancel() method, return it to FE
    PPlanFragmentCancelReason _cancel_reason;
    std::string _cancel_msg;

    OpentelemetrySpan _span;
    
    DescriptorTbl* _desc_tbl;

    ObjectPool* obj_pool() { return _runtime_state->obj_pool(); }

    // typedef for TPlanFragmentExecParams.per_node_scan_ranges
    using PerNodeScanRanges = std::map<TPlanNodeId, std::vector<TScanRangeParams>>;

    // Main loop of profile reporting thread.
    // Exits when notified on _done_cv.
    // On exit, *no report is sent*, ie, this will not send the final report.
    void report_profile();

    // Invoked the report callback if there is a report callback and the current
    // status isn't CANCELLED. Sets 'done' to true in the callback invocation if
    // done == true or we have an error status.
    void send_report(bool done);

    // Executes open() logic and returns resulting status. Does not set _status.
    // If this plan fragment has no sink, open_internal() does nothing.
    // If this plan fragment has a sink and open_internal() returns without an
    // error condition, all rows will have been sent to the sink, the sink will
    // have been closed, a final report will have been sent and the report thread will
    // have been stopped. _sink will be set to nullptr after successful execution.
    Status open_vectorized_internal();

    // Executes get_next() logic and returns resulting status.
    Status get_vectorized_internal(::doris::vectorized::Block* block, bool* eos);

    // Stops report thread, if one is running. Blocks until report thread terminates.
    // Idempotent.
    void stop_report_thread();

    const DescriptorTbl& desc_tbl() const { return _runtime_state->desc_tbl(); }

    void _collect_query_statistics();

    void _collect_node_statistics();
};

} // namespace doris
