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
// The receipt rail reserves 22 seconds for skew (1s), TERM grace (at most
// 10s), and normal reap/populated waits (11s). It converts the remaining
// wall-clock budget to a local monotonic deadline once. Queueing, launch
// setup and supervision consume that same deadline; the configured runtime
// cap can only shorten it. Cross-machine clock skew at receipt remains a
// documented assumption, but later wall-clock changes cannot extend the watch.
//
// A timeout of the cleanup waits is not termination evidence. cgroup.kill
// terminates the whole invocation domain, including descendants outside the
// original process group. If the kernel cannot yet prove child termination
// and populated=0, execute() retains its caller/executor slot until it can.
// The FE statement can independently time out; BE capacity is not reused.
// ---------------------------------------------------------------------------
inline constexpr int64_t kSkewMarginMs = 1000;
inline constexpr int64_t kTermGraceMs = 10 * 1000;
inline constexpr int64_t kReapBoundMs = 11 * 1000; // reap 10s + populated 1s
inline constexpr int64_t kKillReservationMs = kSkewMarginMs + kTermGraceMs + kReapBoundMs;
inline constexpr int64_t kMinWorkerRunMs = 3 * 1000;

// Produced only after the exact child is reaped (or a pidfd proves the CDC
// reaper already consumed it) and its entire cgroup is empty. A bounded wait
// alone never authorizes the caller to release capacity.
struct WorkerExecution {
    enum class Kind {
        // A complete result frame that passed the pipe-protocol contract
        // (phase/code band, decodable, exactly one struct). `result` holds it.
        TRUSTED_RESULT,
        // No trusted result exists (worker lost, torn/garbled frame, deadline
        // expiry, OOM, signal, band-crossing or handshake protocol ambiguity).
        // The handler synthesizes the indeterminate wire phase; `result` is unset.
        INDETERMINATE
    };

    Kind kind = Kind::INDETERMINATE;
    // TRUSTED_RESULT: the validated frame.
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
//     enablement, limit write/read-back, dummy-child migration and cgroup.kill).
//     Any failure
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

    // kill_deadline_ms is the advisory epoch-ms value stamped into the
    // internal frame. Production also passes the fixed receipt-time monotonic
    // deadline. The configured cap is sampled at entry and every launch step
    // consumes this absolute watch. The zero default converts wall time once
    // at entry for standalone callers and tests.
    //
    // Return contract (D7/D8, consumed by the worker service):
    //   non-OK Status — the invocation provably never exec'd (isolation
    //     unverified, stopping, empty identity, kill budget below the minimum,
    //     the reject_after_enqueue debug point, or any pre-fork/pre-barrier
    //     failure). Map to the wire's non-OK status (confirmed failure).
    //   OK + TRUSTED_RESULT — map phase/code verbatim.
    //   OK + INDETERMINATE — synthesize phase 3, no code.
    // Exceptions unwind only after a forked domain is terminated; the service
    // translates them into an indeterminate answer. Concurrent callers share
    // only the stop gate and the locked in-flight registry.
    Status execute(const TLanceIndexWorkerDispatch& dispatch, int64_t kill_deadline_ms,
                   WorkerExecution* execution, int64_t kill_deadline_mono = 0);

    // Best-effort shutdown (N4): closes the launch gate (execute() arriving
    // after stop() never starts a worker) and signals in-flight workers TERM
    // then KILL after a bounded grace. An execute() that was already inside
    // its launch prelude when stop() ran may still complete the launch — its
    // registry entry then missed both kill snapshots — so execute() itself
    // rechecks _stopping right after registration and kills its own domain.
    // The launched invocation converges indeterminate after termination is
    // proven; stop() never waits on it. In-flight execute() calls
    // retain their caller threads until the whole domain is proven terminated;
    // stop() does not join them — their threads are not ours. Correctness
    // never depends on stop(): the worker-side PR_SET_PDEATHSIG arm +
    // getppid recheck is the real BE-loss backstop, and the default
    // doris_main exit path skips all stop() calls.
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
    int retained_cleanups_for_test() const { return _retained_cleanup_count.load(); }
    static bool pipe_write_with_sigpipe_guard_for_test();
    void force_termination_proof_delay_for_test(bool delayed) {
        _force_termination_proof_delay.store(delayed);
    }
#endif

private:
    int _register_inflight_child(int pidfd, const std::string& kill_path);
    void _unregister_inflight_child(int registration);

    std::atomic<bool> _isolation_verified {false};
    // Absolute path of the verified delegated parent cgroup; set by preflight().
    std::string _cgroup_parent_abs;
    std::atomic<bool> _stopping {false};

    struct InflightChild {
        int pidfd; // a separately owned duplicate, stable across concurrent reap
        std::string kill_path;
    };
    // stop() uses stable kernel/cgroup identities, never a recycled integer pid.
    std::mutex _inflight_mutex;
    std::vector<InflightChild> _inflight_children;
    std::atomic<int> _inflight_count {0};
    std::atomic<int> _retained_cleanup_count {0};

    // Test-only overrides; not BE_TEST-gated so the production code paths
    // read plain atomics (identical codegen).
    std::atomic<bool> _force_preflight_failure {false};
    std::atomic<bool> _force_cgroup_migration_failure {false};
    std::atomic<bool> _force_termination_proof_delay {false};
#ifdef BE_TEST
    // Exec target override backing force_worker_exec_for_test; empty = the
    // production self-exec. BE_TEST-gated (unlike the atomic hooks above) so
    // production binaries carry no string/vector members for it.
    std::string _force_exec_path;
    std::vector<std::string> _force_exec_args;
#endif
};

} // namespace doris::lance
