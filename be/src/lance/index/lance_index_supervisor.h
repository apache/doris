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

#include <gen_cpp/LanceIndexWorker_types.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <sys/types.h>
#include <vector>

#include "common/status.h"

namespace doris::lance {

// ---------------------------------------------------------------------------
// D3 unified budget formula, millisecond precision. The closed dispatcher
// series computed whole seconds as `remaining/1000 - margin_s - grace_s` with
// margin_s = 400 (a 3x async-callback RPC budget that no longer exists) and no
// explicit reap bound; the synchronous form has no callback channel, so the
// margin shrinks to the clock-skew allowance and the grace deduction is
// restored explicitly, plus the reap bound the ladder provably needs (review
// B1: without these deductions the kill ladder could run ~20s past the FE
// socket timeout and the respond-after-reap invariant would not hold).
//
//   RESERVATION_MS = SKEW_MARGIN + TERM_GRACE + REAP_BOUND = 22_000
//   receive rail (service): deadline_ms - now <= RESERVATION + MIN_RUN
//                            -> never-launched reject (second rail below is
//                               the supervisor-side twin)
//   kill deadline (service): kill_deadline_ms = deadline_ms - RESERVATION
//                            (absolute wall-clock ms, handed to execute())
//   dequeue rail (supervisor): kill_deadline_ms - now < MIN_RUN
//                            -> never-launched reject
//   ladder worst case       : TERM at kill_deadline -> grace wait -> KILL ->
//                            reap (<= 10s of the 11s reap bound) ->
//                            populated=0 (<= 1s) = 21s < RESERVATION - skew,
//                            so the execution domain is provably terminated
//                            before the FE socket timeout (D10) and the reply
//                            can always leave after the reap.
//
// kMinWorkerRunMs carries the concept of the closed series'
// MIN_EXECUTABLE_BUDGET_SECONDS = 5 (S3 reconciliation): an invocation whose
// remaining kill budget cannot fund a minimal worker run is rejected instead
// of being launched into certain expiry; the value follows the unified
// formula (receive-rail threshold = RESERVATION + MIN_RUN = 25s) rather than
// the old standalone 5s constant.
//
// The TERM grace and reap bound the ladder actually uses are
// lance_index_worker_term_grace_seconds (default 10s = kTermGraceMs) and the
// compile-time reap/populated bounds; raising the config above the default
// breaks the reservation assumption above and is operator error (the config
// comment repeats this).
// ---------------------------------------------------------------------------
inline constexpr int64_t kSkewMarginMs = 1000;
inline constexpr int64_t kTermGraceMs = 10 * 1000;
inline constexpr int64_t kReapBoundMs = 11 * 1000; // reap 10s + populated 1s
inline constexpr int64_t kKillReservationMs = kSkewMarginMs + kTermGraceMs + kReapBoundMs;
inline constexpr int64_t kMinWorkerRunMs = 3 * 1000;

// The supervisor's synchronous answer for one invocation, produced ONLY after
// the execution domain's termination ladder has fully run (kill, bounded reap,
// populated=0 wait): execute() returning IS the respond-after-reap boundary
// (D10) — the caller may build and send the wire reply knowing the domain is
// (to the bounded evidence) gone.
struct WorkerExecution {
    enum class Kind {
        // A complete result frame that passed the pipe-protocol contract
        // (phase/code band, decodable, exactly one struct). `result` holds it.
        TRUSTED_RESULT,
        // A handshake-stage (pre-FFI) protocol violation the supervisor
        // detected and killed, WITH a reap proof: the worker writes its
        // handshake before its first FFI call in program order, so no mutation
        // can have committed — a trusted pre-invocation rejection (S5#1).
        // `result` holds the supervisor-built rejection frame.
        PROVEN_PRE_INVOCATION_REJECTION,
        // No trusted result exists (worker lost, torn/garbled frame, deadline
        // expiry, OOM, signal, band-crossing protocol ambiguity, or a pre-FFI
        // violation without a reap proof). The handler synthesizes the
        // indeterminate wire phase; `result` is unset.
        INDETERMINATE
    };

    Kind kind = Kind::INDETERMINATE;
    // TRUSTED_RESULT: the validated frame. PROVEN_PRE_INVOCATION_REJECTION:
    // phase PRE_INVOCATION_REJECTED + LANCE_WORKER_REJECTED_RESOURCE_REJECTED.
    // INDETERMINATE: unset.
    std::optional<TLanceIndexWorkerResult> result;
    // Bounded sanitized message for every kind ("" = omit): static category
    // text plus verified identity fields only, never worker/provider text.
    std::string message;
    // Static failure category of a non-trusted ending (log-facing; nullptr
    // for TRUSTED_RESULT). Never freed (string literals).
    const char* failure_category = nullptr;
    // The D16 dual reap evidence (exact reap or CDC-raced pidfd termination
    // AND populated=0). Never rewrites the classification (D7); commit 5 logs
    // it and series 3 builds precise-kill proofs on it.
    bool child_reaped_proof = false;
};

// Supervisor of the cgroup-isolated one-shot Lance index worker processes.
//
// Boundary model (D5 — no soft fallback anywhere):
//   * preflight() runs once at BE startup and proves the whole cgroup v2
//     delegation chain (mount, writable delegated parent, controller
//     enablement, limit write/read-back, dummy-child migration). Any failure
//     leaves isolation_verified() == false and every execute() is rejected.
//   * Each accepted invocation gets its own cgroup (memory.max, swap=0,
//     pids.max, oom.group=1) named after the invocation id (series 3 maps
//     /proc/<pid>/cgroup membership back to the invocation), a fork barrier
//     (the child provably cannot exec before the parent migrated and verified
//     it), a pidfd-based wait channel (never a SIGCHLD handler), a
//     kill_deadline-driven TERM->KILL escalation, and a bounded two-frame
//     pipe protocol (dispatch in, handshake+result out) with a continuously
//     drained, bounded stderr ring.
//
// Synchronous shape (D1/D4): the supervisor owns NO queue and NO executor
// threads. execute() is called directly on the caller's (the worker service's
// executor) thread, blocks through launch -> supervision -> termination
// ladder -> reap confirmation, and returns the complete classification; there
// is no callback channel and no second delivery. Accountings that the closed
// series did through result/termination/silent callbacks are the caller's
// stack frame here (RAII), so nothing can leak them.
//
// Credentials discipline: storage_options travel only inside the stdin
// dispatch frame — never argv, never env, never logs. Request-level logs
// carry only invocation_id / be_process_epoch / mutation_type. Worker stderr
// is never logged raw; every message leaving this file is built from static
// category strings plus verified identity fields only, and any message that
// would contain a storage-option value substring is dropped whole.
class LanceIndexSupervisor {
public:
    LanceIndexSupervisor();
    ~LanceIndexSupervisor();

    LanceIndexSupervisor(const LanceIndexSupervisor&) = delete;
    LanceIndexSupervisor& operator=(const LanceIndexSupervisor&) = delete;

    // Startup probe. Any step failure is precise-logged and leaves
    // isolation_verified() == false. config::lance_index_isolation_preflight
    // = false only skips the probe — it never sets isolation_verified().
    Status preflight();
    bool isolation_verified() const { return _isolation_verified.load(); }

    // Synchronous one-invocation entry. `kill_deadline_ms` is the absolute
    // wall-clock (epoch ms) deadline the CALLER computed once from the
    // unified budget formula (deadline_ms - kKillReservationMs, D3); the
    // supervisor's watchdog triggers the TERM->KILL ladder at
    // min(kill_deadline_ms, now + lance_index_worker_wallclock_limit_seconds)
    // and stamps field 19 of the wire dispatch frame with the same value
    // (advisory to the worker; the monotonic watch here is the authority).
    //
    // Return contract (D7/D8, consumed by the worker service):
    //   non-OK Status — the invocation provably never exec'd (isolation
    //     unverified, stopping, empty identity, kill budget below the minimum,
    //     the reject_after_enqueue debug point, or any pre-fork/pre-barrier
    //     failure). Map to the wire's non-OK status (confirmed failure).
    //   OK + TRUSTED_RESULT — map phase/code verbatim.
    //   OK + PROVEN_PRE_INVOCATION_REJECTION — map phase 1 + the negative code.
    //   OK + INDETERMINATE — synthesize phase 3, no code.
    // execute() itself never throws; it is safe to call concurrently from
    // multiple caller threads (shared state is the stop gate and the in-flight
    // registry, both locked).
    Status execute(const TLanceIndexWorkerDispatch& dispatch, int64_t kill_deadline_ms,
                   WorkerExecution* execution);

    // Best-effort shutdown (N4): closes the launch gate (execute() after
    // stop() never starts a worker — a late launch would escape the kill
    // snapshot), signals in-flight workers TERM then KILL after a bounded
    // grace, and returns. In-flight execute() calls converge on their own
    // caller threads (their ladders are bounded); stop() does not join them —
    // their threads are not ours. Correctness never depends on stop(): the
    // worker-side PR_SET_PDEATHSIG arm + getppid recheck is the real BE-loss
    // backstop, and the default doris_main exit path skips all stop() calls.
    void stop();

    // Resolves the writable delegated parent cgroup: the configured value when
    // non-empty, otherwise the first ancestor of /proc/self/cgroup whose
    // cgroup.subtree_control accepts "+memory +pids". Left enabled on success
    // (invocation cgroups need it for the whole BE lifetime). Exposed
    // separately for unit tests; read-mostly probe, idempotent.
    static Status resolve_cgroup_parent(const std::string& configured_parent,
                                        std::string* resolved_parent);

    // Builds the bounded (<= 900 UTF-8 bytes, codepoint-safe truncation,
    // control chars escaped) operator message from a STATIC category string
    // and verified identity fields only. Second-line invariant: if the result
    // would contain any storage-option value substring, the whole message is
    // dropped (returns ""). An empty return means "omit the message field",
    // never "send raw text".
    static std::string sanitize_message(const std::string& static_category,
                                        const TLanceIndexWorkerDispatch& dispatch);

    // The invocation cgroup directory name: "lance-worker-<sanitized
    // invocation id>-<hash08x>". The sanitized id keeps [A-Za-z0-9._-] and 64
    // bytes of it; the raw-id hash suffix keeps different raw ids that
    // sanitize identically distinct, and lets series 3 map a cgroup path back
    // to its invocation (D5: /proc/<pid>/cgroup membership is the precise-kill
    // primitive).
    static std::string invocation_cgroup_name(const std::string& invocation_id);

    // Names-only debug-point handoff value (D11): filters the dispatch
    // frame's field-18 name list down to the fixed worker-fault allowlist
    // (LanceIndexWorker.hang / LanceIndexWorker.skip_result), in canonical
    // order, comma-joined. Empty return = "set no env". The service snapshots
    // the armed points into the frame at receipt (armed-then-dispatched);
    // this function is the supervisor-side filter that decides what may
    // actually cross the exec boundary.
    static std::string worker_debug_points_env_value(const std::vector<std::string>& names);

#ifdef BE_TEST
    // Test hooks (python_udf_runtime.h:97-101 style).
    void force_preflight_failure_for_test() { _force_preflight_failure.store(true); }
    void force_cgroup_migration_failure_for_test() {
        _force_cgroup_migration_failure.store(true);
    }
    // Overrides the worker exec target and argv (production default:
    // execve("/proc/self/exe", {"doris_be", "--lance-worker"}, ...)) so unit
    // tests can point the supervisor at a protocol-speaking fake worker
    // executable (cdc_client_mgr.cpp:242-245 BE_TEST short-circuit precedent).
    // Set before the first execute(); like the other force_* hooks it is not
    // synchronized against in-flight executions.
    void force_worker_exec_for_test(std::string path, std::vector<std::string> args);
    int inflight_count_for_test() const { return _inflight_count.load(); }
#endif

private:
    void _register_inflight_child(pid_t pid);
    void _unregister_inflight_child(pid_t pid);

    std::atomic<bool> _isolation_verified {false};
    // Absolute path of the verified delegated parent cgroup; set by preflight().
    std::string _cgroup_parent_abs;
    std::atomic<bool> _stopping {false};

    // In-flight children (pid == pgid) so stop() can signal them best-effort.
    std::mutex _inflight_mutex;
    std::vector<pid_t> _inflight_children;
    std::atomic<int> _inflight_count {0};

    // Test-only overrides; not BE_TEST-gated so the production code paths
    // read plain atomics (identical codegen).
    std::atomic<bool> _force_preflight_failure {false};
    std::atomic<bool> _force_cgroup_migration_failure {false};
#ifdef BE_TEST
    // Exec target override backing force_worker_exec_for_test; empty = the
    // production self-exec. BE_TEST-gated (unlike the atomic hooks above) so
    // production binaries carry no string/vector members for it.
    std::string _force_exec_path;
    std::vector<std::string> _force_exec_args;
#endif
};

} // namespace doris::lance
