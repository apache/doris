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

#include <gen_cpp/BackendService_types.h>

#include <atomic>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/metrics/metrics.h"
#include "common/status.h"
#include "lance/index/lance_index_supervisor.h"
#include "util/blocking_queue.hpp"

namespace doris {
class TLanceIndexMutationRequest;
class TLanceIndexMutationResult;

namespace lance {

// The synchronous thrift-facing component behind
// BaseBackendService::lance_index_mutate (plan D1/D4/D7/D8).
//
// Shape: the handler thread runs the receive rail and then BLOCKS on a bounded
// queue handoff; one of the N = lance_index_mutation_worker_max_concurrency
// executor threads dequeues the invocation, re-checks the budget (the dequeue
// rail), and runs LanceIndexSupervisor::execute() synchronously on its own
// thread. execute() returning IS the respond-after-reap boundary (D10): by the
// time the executor answers the waiting handler, the execution domain is (to
// the bounded evidence) terminated, so the reply leaves the BE only after
// domain termination. There is no callback channel and no second delivery.
//
// Blocking surface: at most max_concurrency running + max_queued queued = 18
// handler threads with the defaults (be_service_threads=64), a bounded
// occupation that is the designed behavior of the synchronous contract.
//
// Receive rail (handler thread, in order; every rejection is a BE-level
// never-launched answer = wire non-OK):
//   1. service not started / stopping          -> Cancelled
//   2. per-field value/cardinality validation  -> InvalidArgument (required-
//      field presence is enforced by the thrift decode itself; __isset
//      exists only for the optional fields)
//   3. epoch receipt check (request field 2 !=
//      HeartbeatServer::process_epoch())       -> InvalidArgument
//   4. invocation_id dedup (thrift :414-418 BE
//      half)                                  -> OK + phase 3 (see below)
//   5. expired-on-arrival deadline (D3 receive
//      rail: remaining <= kKillReservationMs +
//      kMinWorkerRunMs)                        -> Cancelled
//   6. bounded-queue try_put                   -> TooManyTasks (Cancelled
//                                                 when the stop race hits)
//
// Duplicate-delivery ruling (D4/S7, the thrift single-send contract's "one
// invocation_id is one execution" half): a duplicate is a protocol violation -
// logged, never executed a second time. The first delivery's answer went to
// the first connection, so this delivery's outcome is genuinely unknown to
// this process copy; answering non-OK would fabricate a "nothing executed"
// confirmation that may be false. The duplicate therefore answers
// OK + phase 3 (INDETERMINATE_AFTER_LAUNCH, no code): the FE converges it to
// an indeterminate outcome and never a false confirmed failure.
//
// Dequeue rail (executor thread): stop-drain rejection, then the D3 monotonic
// re-check (now_mono + kMinWorkerRunMs > kill_deadline_mono -> never
// launched), then the two-state slot accounting (D4): `reserved` from the
// dequeued-and-validated point, `launched` once supervisor execute() accepts
// the invocation past its own never-launched gates (execute() non-OK means
// provably never exec'd - the slot is released directly; execute() OK means
// the domain ran and is terminated at return - the slot is released at the
// respond-after-reap boundary). The executor thread itself is the slot, so
// N executor threads bound concurrent workers by construction.
//
// Metrics (N3): pull-fed gauges for queue depth and in-flight workers, and
// fixed-name outcome counters for the phase x code-band table (never-launched
// / pre-invocation-rejected / native-ok / native-error / indeterminate). No
// metric name or label carries an invocation id, an index name, or any user
// data.
//
// Credentials discipline: storage_options cross from the wire request into the
// internal dispatch frame and nowhere else - never a log line, never a result
// message, never a metric. Request-level logs carry only invocation_id /
// be_process_epoch / mutation_type.
class LanceIndexWorkerService {
public:
    LanceIndexWorkerService();
    ~LanceIndexWorkerService();

    LanceIndexWorkerService(const LanceIndexWorkerService&) = delete;
    LanceIndexWorkerService& operator=(const LanceIndexWorkerService&) = delete;

    // Runs the supervisor isolation preflight (non-fatal: a failure leaves
    // isolation unverified and every invocation rejected), samples the
    // structural config once, spawns the executor threads and registers the
    // metrics. Called before the thrift server starts accepting connections.
    Status start();

    // Drain-and-refuse shutdown (N4): every queued entry is answered with a
    // never-launched rejection, no new worker may launch after this point,
    // in-flight execute() calls converge on their own (executor) threads
    // bounded by their kill ladders, the executors are joined, and the
    // supervisor is stopped. Idempotent; correctness never depends on it (the
    // worker-side PDEATHSIG arm is the real BE-loss backstop and the default
    // doris_main exit path skips all stop calls).
    void stop();

    // The thrift handler body: fills `result` synchronously. Returns after a
    // receive-rail rejection or after the execution domain's termination is
    // confirmed (respond-after-reap). Never throws.
    void lance_index_mutate(TLanceIndexMutationResult& result,
                            const TLanceIndexMutationRequest& request);

    // Gauge feeders (registered as pull hooks in start()).
    int64_t queue_depth() const;
    int64_t inflight_workers() const;

    // Test hooks (the production paths read the plain atomics; supervisor.h
    // force_* precedent). force_be_process_epoch_for_test pins the receipt
    // check's epoch instead of the heartbeat-sourced one.
    void force_be_process_epoch_for_test(int64_t epoch) { _forced_be_epoch.store(epoch); }
    int64_t reserved_slots_for_test() const { return _reserved_slots.load(); }
    int64_t launched_total_for_test() const { return _launched_total.load(); }
    int64_t never_launched_total_for_test() const { return _never_launched_total.load(); }
    int64_t pre_invocation_rejected_total_for_test() const {
        return _pre_invocation_rejected_total.load();
    }
    int64_t native_ok_total_for_test() const { return _native_ok_total.load(); }
    int64_t native_error_total_for_test() const { return _native_error_total.load(); }
    int64_t indeterminate_total_for_test() const { return _indeterminate_total.load(); }
    LanceIndexSupervisor& supervisor_for_test() { return _supervisor; }

private:
    // One queued invocation: the wire request plus the deadlines computed
    // once at receipt (D3: never reset), plus the answer channel back to the
    // blocked handler thread. Shared-ownership wrapper because the answer
    // promise is move-only while BlockingQueue copies its elements.
    struct PendingInvocation {
        TLanceIndexMutationRequest request;
        int64_t kill_deadline_ms = 0;   // absolute epoch ms = deadline - kKillReservationMs
        int64_t kill_deadline_mono = 0; // monotonic ms, saturated add
        std::promise<TLanceIndexMutationResult> answer;
    };
    using PendingPtr = std::shared_ptr<PendingInvocation>;

    // Receive-rail steps. Each rejection helper fills the complete wire
    // result (status + identity echo) so no path can answer partially.
    Status _validate_request(const TLanceIndexMutationRequest& request) const;
    // Returns true when the invocation_id is fresh (and now retained until
    // deadline + kDuplicateRetentionMs); false when it is a duplicate.
    bool _admit_invocation_id(const TLanceIndexMutationRequest& request);
    void _forget_invocation_id(const std::string& invocation_id);

    int64_t _be_process_epoch() const;

    // Rejects with a BE-level never-launched status (wire non-OK = the FE's
    // confirmed-failure path; the invocation provably never executed).
    void _reject_never_launched(TLanceIndexMutationResult& result,
                                const TLanceIndexMutationRequest& request, const Status& status);
    // The duplicate-delivery answer: OK + phase 3, no code.
    void _reject_duplicate(TLanceIndexMutationResult& result,
                           const TLanceIndexMutationRequest& request);
    // Maps a completed supervisor execute() (Status + WorkerExecution) onto
    // the wire result per the D7/D8 table.
    void _fill_from_execution(TLanceIndexMutationResult& result,
                              const TLanceIndexMutationRequest& request, const Status& status,
                              const WorkerExecution& execution);

    // Executor-thread body: dequeue -> stop/budget rails -> two-state slot ->
    // dispatch build -> supervisor execute -> answer.
    void _executor_loop();
    void _execute_pending(const PendingPtr& pending);

    // Mirrors the 15 semantic request fields into the internal dispatch frame
    // and injects the internal-only facts: bounds 16/17 from BE config (the
    // worker reads no config) and the names-only snapshot of the armed
    // worker-fault debug points (field 18, armed-then-dispatched). Field 19
    // (worker_deadline_ms) is stamped by the supervisor from the kill
    // deadline it is handed.
    TLanceIndexWorkerDispatch _build_dispatch(const TLanceIndexMutationRequest& request) const;

    void _run_executor_answer(PendingInvocation& pending, TLanceIndexMutationResult result);

    // Outcome counters (phase x code band; fixed names, no user data). Each
    // increments the test-readable atomic AND the registered IntCounter.
    void _count_outcome_never_launched();
    void _count_outcome_pre_invocation_rejected();
    void _count_outcome_native_ok();
    void _count_outcome_native_error();
    void _count_outcome_indeterminate();
    void _bump(IntCounter* metric, std::atomic<int64_t>& counter);

    LanceIndexSupervisor _supervisor;

    std::unique_ptr<BlockingQueue<PendingPtr>> _queue;
    std::vector<std::thread> _executors;
    std::atomic<bool> _started {false};
    std::atomic<bool> _stopping {false};
    bool _metrics_registered = false;

    // Two-state slot accounting (D4) + the pull-gauge / outcome-counter
    // backing. Reserved counts dequeued-and-validated invocations between the
    // dequeue rail and the supervisor's launch decision; launched totals the
    // invocations execute() accepted past its never-launched gates.
    std::atomic<int64_t> _reserved_slots {0};
    std::atomic<int64_t> _inflight_executions {0};
    std::atomic<int64_t> _launched_total {0};
    std::atomic<int64_t> _never_launched_total {0};
    std::atomic<int64_t> _pre_invocation_rejected_total {0};
    std::atomic<int64_t> _native_ok_total {0};
    std::atomic<int64_t> _native_error_total {0};
    std::atomic<int64_t> _indeterminate_total {0};

    // Registered outcome counters (mirror the atomics; owned by the metrics
    // registry once registered).
    IntCounter* _metric_never_launched = nullptr;
    IntCounter* _metric_pre_invocation_rejected = nullptr;
    IntCounter* _metric_native_ok = nullptr;
    IntCounter* _metric_native_error = nullptr;
    IntCounter* _metric_indeterminate = nullptr;
    // Registered pull gauges (queue depth, in-flight executions).
    UIntGauge* _gauge_queue_depth = nullptr;
    UIntGauge* _gauge_inflight_workers = nullptr;

    // Test-only epoch override; 0 = use the heartbeat-sourced process epoch.
    std::atomic<int64_t> _forced_be_epoch {0};

    // invocation_id dedup set: id -> erase-at (epoch ms). Retained until the
    // request deadline plus a bounded window so a late redelivery of a
    // completed invocation can never re-execute external side effects.
    static constexpr int64_t kDuplicateRetentionMs = 10 * 60 * 1000;
    std::mutex _dedup_mutex;
    std::unordered_map<std::string, int64_t> _dedup_erase_after_ms;
};

} // namespace lance
} // namespace doris
