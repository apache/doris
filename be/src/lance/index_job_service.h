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

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>

#include "common/status.h"
#include "lance/index_job_supervisor.h"

namespace doris {

class ExecEnv;
class TStatus;
class TLanceIndexJobDispatch;
class TLanceIndexJobReport;
class TLanceIndexJobTerminationReport;
class MasterServerClient;

namespace lance {

// Thrift-facing component behind BackendService::submit_lance_index_job
// (plan D6/D14/D19, R2 §1/§3/§7).
//
// The service owns the IndexJobSupervisor and exposes exactly one bounded,
// non-blocking submission path. The thrift handler thread never executes
// anything itself: the dispatch is validated, deduplicated and enqueued, and
// the handler answers immediately. The wire contract is "OK = enqueued
// exactly once; ERROR = NOT enqueued and this invocation id never executes",
// so a degraded synchronous execution (the ingest-binlog pattern) is a
// contract violation and never happens here.
//
// Guard order inside submit_lance_index_job (D6):
//   1. no master FE heartbeat yet         -> synchronous ERROR (Cancelled)
//   2. isolation not verified (preflight) -> synchronous ERROR (CgroupError)
//   3. D4 payload limits violated         -> synchronous ERROR (InvalidArgument,
//                                            never enqueued)
//   4. deadline budget already exhausted  -> synchronous ERROR (Cancelled; D3
//      handler half — the supervisor re-checks at dequeue and answers with the
//      full NEVER_LAUNCHED envelope there)
//   5. duplicate invocation_id            -> OK (idempotent; the first enqueue
//                                            owns execution and the report)
//   6. bounded queue full                 -> synchronous ERROR (TooManyTasks;
//                                            Cancelled when the supervisor is
//                                            stopping)
//
// Callback seam (D14): the supervisor's report callbacks are bound to
// MasterServerClient with the finish_task discipline (3 attempts, sleep(1)
// between, success/failure counters), invoked synchronously on the supervisor
// executor thread. With lance_index_worker_max_inflight=1 the bounded stall
// is acceptable and is budgeted in lance_index_worker_report_margin_seconds.
// The supervisor delivers terminal reports even while stopping: the service
// then drops the RPC but still releases the invocation's _outstanding slot,
// so the gauges never overcount past a shutdown drain.
//
// Credentials discipline: request-level logs carry only job_id /
// invocation_id / mutation_type. storage_options keys/values and any
// ThriftDebugString of a dispatch are never logged.
class LanceIndexJobService {
public:
    explicit LanceIndexJobService(ExecEnv* exec_env);
    ~LanceIndexJobService();

    LanceIndexJobService(const LanceIndexJobService&) = delete;
    LanceIndexJobService& operator=(const LanceIndexJobService&) = delete;

    // Wires the supervisor callbacks, registers the metric hooks, and runs the
    // isolation preflight. A preflight failure is NOT fatal: the supervisor
    // logs it precisely, isolation stays unverified, and every later
    // submission is rejected. Called once from
    // BackendService::start_thrift_dependencies(), before the thrift server
    // starts accepting connections.
    Status start();

    // Best-effort symmetric stop (the stop_works precedent); correctness never
    // depends on it: the worker-side PDEATHSIG arm + getppid recheck is the
    // real BE-loss backstop, and the default doris_main exit path skips all
    // stop calls. Idempotent.
    void stop();

    // The thrift handler body: fills _return per the contract above and never
    // blocks on execution.
    void submit_lance_index_job(TStatus& _return, const TLanceIndexJobDispatch& dispatch);

    // Gauge feeders for the REGISTER_HOOK_METRIC hooks. The supervisor's queue
    // and executor state are private, so the service keeps its own accounting:
    // _outstanding counts accepted invocations whose terminal report (result
    // or termination) has not finished. With the executor threads always
    // blocked on the bounded queue, busy executors == min(outstanding,
    // max_inflight) and the remainder is waiting in the queue. The rare
    // evidence-insufficient paths (no termination proof) keep the invocation
    // counted, matching the FE-side possible-live slot semantics.
    int64_t queue_size() const;
    int64_t inflight_workers() const;

private:
    // finish_task discipline (task_worker_pool.cpp:153-172): 3 attempts,
    // sleep(1) between, success/failure counter increments. Runs synchronously
    // on the supervisor executor thread; releases the invocation's
    // _outstanding slot when the report finishes.
    void _report_with_retry(const char* report_kind, int64_t job_id,
                            const std::string& invocation_id,
                            const std::function<Status(MasterServerClient*, TStatus*)>& attempt);
    void _report_result(const TLanceIndexJobReport& report);
    void _report_termination(const TLanceIndexJobTerminationReport& report);

    ExecEnv* _exec_env; // not owned
    IndexJobSupervisor _supervisor;
    std::atomic<int64_t> _outstanding {0};
    std::atomic<bool> _stopping {false};
    bool _metrics_registered = false;
};

} // namespace lance
} // namespace doris
