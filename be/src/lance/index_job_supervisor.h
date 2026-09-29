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
#include <memory>
#include <mutex>
#include <string>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/status.h"

namespace doris {

class TLanceIndexJobDispatch;
class TLanceIndexJobReport;
class TLanceIndexJobTerminationReport;

template <typename T>
class BlockingQueue;

namespace lance {

// Supervisor of the cgroup-isolated one-shot Lance index worker processes.
//
// Boundary model (plan D2/D3/D4, R3 §5.1 — no soft fallback anywhere):
//   * preflight() runs once at BE startup and proves the whole cgroup v2
//     delegation chain (mount, writable delegated parent, controller
//     enablement, limit write/read-back, dummy-child migration). Any failure
//     leaves isolation_verified() == false and every submit() is rejected.
//   * Each accepted invocation gets its own cgroup (memory.max, swap=0,
//     pids.max, oom.group=1), a fork barrier (the child provably cannot exec
//     before the parent migrated and verified it), a pidfd-based wait channel
//     (never a SIGCHLD handler), a wall-clock deadline with TERM->KILL
//     escalation, and a bounded two-frame pipe protocol (dispatch in,
//     handshake+result out) with a continuously drained, bounded stderr ring.
//   * Termination reporting discipline (D5/D6/D16): a complete
//     identity-matched result frame is the only trusted result; everything
//     else is at most a termination proof, and CHILD_REAPED is reported only
//     when the exact child is provably reaped AND the invocation cgroup reads
//     populated=0. NEVER_LAUNCHED is used only when the invocation provably
//     never exec'd (pre-fork rejection or pre-barrier-release kill).
//
// Credentials discipline: storage_options travel only inside the stdin
// dispatch frame — never argv, never env, never logs. Request-level logs carry
// only job_id / invocation_id / mutation_type. Worker stderr is never logged
// raw; sanitized_message is built here from static category strings plus
// verified identity fields only (D12).
class IndexJobSupervisor {
public:
    // Commit 4 binds MasterServerClient (with the finish_task-style 3-retry
    // discipline) to these; commit 6 mocks them. Invoked synchronously on the
    // supervisor executor thread (plan §5 ruling).
    using ReportResultFn = std::function<void(const TLanceIndexJobReport&)>;
    using ReportTerminationFn = std::function<void(const TLanceIndexJobTerminationReport&)>;

    IndexJobSupervisor();
    ~IndexJobSupervisor();

    IndexJobSupervisor(const IndexJobSupervisor&) = delete;
    IndexJobSupervisor& operator=(const IndexJobSupervisor&) = delete;

    // Startup probe (D2 layer 3 / D7). Any step failure is precise-logged and
    // leaves isolation_verified() == false. config::lance_index_isolation_preflight
    // = false only skips the probe — it never sets isolation_verified().
    Status preflight();
    bool isolation_verified() const { return _isolation_verified.load(); }

    void set_report_result_callback(ReportResultFn fn);
    void set_report_termination_callback(ReportTerminationFn fn);

    // Accepts one invocation for asynchronous execution. Handler-visible
    // outcomes (commit 4 maps them to thrift):
    //   OK                  — accepted and queued (exactly-once via dedup set)
    //   AlreadyExist        — duplicate invocation_id (handler maps to OK)
    //   TooManyTasks        — bounded queue full
    //   CgroupError         — isolation not verified (preflight failed/skipped)
    //   Cancelled           — supervisor is stopping
    Status submit(const TLanceIndexJobDispatch& dispatch);

    // Best-effort shutdown: signals in-flight workers (TERM then KILL) and
    // joins the executor threads. Correctness never depends on this (D9): the
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
                                        const TLanceIndexJobDispatch& dispatch);

#ifdef BE_TEST
    // Test hooks (python_udf_runtime.h:97-101 style).
    void force_budgets_for_test(int64_t wallclock_seconds, int64_t term_grace_seconds,
                                int64_t report_margin_seconds);
    void force_preflight_failure_for_test() { _force_preflight_failure.store(true); }
    void force_cgroup_migration_failure_for_test() {
        _force_cgroup_migration_failure.store(true);
    }
    int inflight_count_for_test() const { return _inflight_count.load(); }
    uint32_t queue_depth_for_test() const;
#endif

private:
    Status _ensure_started();
    void _executor_loop();
    void _execute(const TLanceIndexJobDispatch& dispatch);
    // Full-envelope async rejection for an invocation that provably never
    // exec'd (D6 path 3): PRE_INVOCATION_RESOURCE_REJECTED + NEVER_LAUNCHED.
    void _report_never_launched(const TLanceIndexJobDispatch& dispatch,
                                const char* static_category);
    void _invoke_result_callback(const TLanceIndexJobReport& report);
    void _invoke_termination_callback(const TLanceIndexJobTerminationReport& report);
    void _register_inflight_child(pid_t pid);
    void _unregister_inflight_child(pid_t pid);

    std::atomic<bool> _isolation_verified {false};
    // Absolute path of the verified delegated parent cgroup; set by preflight().
    std::string _cgroup_parent_abs;

    std::mutex _callback_mutex;
    ReportResultFn _report_result_fn;
    ReportTerminationFn _report_termination_fn;

    // Lazily created on the first accepted submit() so a supervisor that never
    // passes preflight never spawns threads. Guarded by _lifecycle_mutex.
    mutable std::mutex _lifecycle_mutex;
    std::unique_ptr<BlockingQueue<TLanceIndexJobDispatch>> _queue;
    std::vector<std::thread> _executors;
    bool _started = false;
    std::atomic<bool> _stopping {false};

    // invocation_id -> epoch-ms after which the entry may be purged
    // (deadline_ms + retention grace; NOT removed at completion, D15).
    std::mutex _dedup_mutex;
    std::unordered_map<std::string, int64_t> _dedup_erase_after_ms;

    // In-flight children (pid == pgid) so stop() can signal them best-effort.
    std::mutex _inflight_mutex;
    std::vector<pid_t> _inflight_children;
    std::atomic<int> _inflight_count {0};

    // Test-only overrides; -1 means "use config". Not BE_TEST-gated so the
    // production code paths read plain atomics (identical codegen).
    std::atomic<int64_t> _force_wallclock_seconds {-1};
    std::atomic<int64_t> _force_term_grace_seconds {-1};
    std::atomic<int64_t> _force_report_margin_seconds {-1};
    std::atomic<bool> _force_preflight_failure {false};
    std::atomic<bool> _force_cgroup_migration_failure {false};
};

} // namespace lance
} // namespace doris
