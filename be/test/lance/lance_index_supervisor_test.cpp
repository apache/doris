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

#include "lance/index/lance_index_supervisor.h"

#include <gen_cpp/LanceIndexWorker_types.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

// Kernel 5.3/5.9 era constants the CI glibc 2.28 headers predate (same spellings
// as lance_index_supervisor.cpp).
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif
#ifdef P_PIDFD
static constexpr idtype_t kPidfdIdtype = P_PIDFD;
#else
static constexpr idtype_t kPidfdIdtype = static_cast<idtype_t>(3);
#endif

#include "common/config.h"
#include "common/logging.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

// LanceIndexSupervisorTest: drives the real supervisor end-to-end against a
// tiny protocol-speaking fake worker executable (lance/fake_worker/
// lance_fake_worker.c, exec'd through the BE_TEST-only
// force_worker_exec_for_test seam) inside REAL invocation cgroups. The fake
// workers self-report their own confinement facts exactly like the real
// worker, so every handshake cross-check, kill escalation and reaping-proof
// branch of the supervisor runs for real. The synchronous shape is asserted
// directly: execute() returns the complete WorkerExecution only after the
// termination ladder finished (respond-after-reap) — there are no callbacks
// to record.
//
// Delegation: the fork/exec cases need a writable delegated cgroup v2 parent
// AND the ability to migrate our own children into it — cgroup v2 requires
// write access on the common ancestor's cgroup.procs, so the test process
// itself must live under the delegated parent. That is exactly what
// `systemd-run --user --scope` provides (the scope lands under the delegated
// app.slice). Resolution order: $LANCE_TEST_CGROUP_PARENT (for hosts whose
// shell session already lives under the delegated parent — a foreign session
// passes resolution but fails migration, so only set it there), then the
// auto-scan of the /proc/self/cgroup ancestry. Without delegation the
// delegation-requiring cases SKIP loudly; the pure rejection-path cases
// (budget, preflight guards, sanitize_message, naming, debug-point filter)
// run everywhere.
//
// The Real* cases pin the kernel-enforced boundaries (OOM kill, pids.max,
// abort, CDC-reaper race, surviving descendant, BE loss) and MUST run under
// the delegated runner (recon4 §B3):
//   systemd-run --user --scope --quiet bash -c '
//     cd <repo> && source <env>; unset proxies; export DORIS_THIRDPARTY=$PWD/thirdparty
//     BUILD_TYPE_UT=RELEASE bash run-be-ut.sh --run --filter="LanceIndex*"'
//
// SIGCHLD hygiene (python_env_test.cpp:53-67 precedent): the fixture resets
// SIGCHLD to SIG_DFL and restores it afterwards; no test installs a SIGCHLD
// handler (the CDC-race case steals exit statuses with a waitpid(-1, WNOHANG)
// polling thread, never a handler).

namespace doris::lance {
namespace {

int64_t epoch_millis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
}

int64_t steady_millis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
}

bool wait_until(const std::function<bool()>& pred, int64_t deadline_ms) {
    const int64_t deadline = steady_millis() + deadline_ms;
    while (steady_millis() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

// The kill deadline exactly the way the worker service (a later commit)
// computes it from the request deadline via the D3 unified formula.
int64_t kill_deadline_for(int64_t request_deadline_ms) {
    return request_deadline_ms - kKillReservationMs;
}

struct SavedLanceConfig {
    std::string cgroup_parent;
    bool preflight;
    int64_t memory_limit;
    int64_t pids_max;
    int64_t wallclock;
    int64_t term_grace;
    int64_t as_limit;
    int32_t cpu_multiplier;
};

// GTEST_SKIP must live in a void function, hence the macro.
#define REQUIRE_DELEGATED_PARENT(parent_var)                                        \
    std::string parent_var = detect_delegated_parent();                             \
    if (parent_var.empty()) {                                                       \
        GTEST_SKIP() << "LANCE-ISOLATION-UT: no writable delegated cgroup v2 "      \
                        "parent found; set LANCE_TEST_CGROUP_PARENT or run under "  \
                        "`systemd-run --user --scope` to execute the Lance "        \
                        "isolation supervisor cases for real";                      \
    }

} // namespace

class LanceIndexSupervisorTest : public ::testing::Test {
protected:
    sighandler_t old_sigchld_ = SIG_DFL;
    std::string test_dir_;
    SavedLanceConfig saved_;
    std::vector<std::string> created_cgroup_dirs_;
    static std::atomic<int> s_invocation_counter;

    void SetUp() override {
        old_sigchld_ = signal(SIGCHLD, SIG_DFL);
        namespace fs = std::filesystem;
        test_dir_ = fs::temp_directory_path().string() + "/lance_sup_test_" +
                    std::to_string(getpid()) + "_" +
                    std::to_string(s_invocation_counter.fetch_add(1));
        fs::create_directories(test_dir_);
        saved_ = {config::lance_index_worker_cgroup_parent,
                  config::lance_index_isolation_preflight,
                  config::lance_index_worker_memory_limit_bytes,
                  config::lance_index_worker_pids_max,
                  config::lance_index_worker_wallclock_limit_seconds,
                  config::lance_index_worker_term_grace_seconds,
                  config::lance_index_worker_as_limit_bytes,
                  config::lance_index_worker_cpu_limit_multiplier};
        ASSERT_TRUE(std::filesystem::exists(fake_worker_path()))
                << "fake worker binary missing (build the lance_fake_worker target): "
                << fake_worker_path();
    }

    void TearDown() override {
        config::lance_index_worker_cgroup_parent = saved_.cgroup_parent;
        config::lance_index_isolation_preflight = saved_.preflight;
        config::lance_index_worker_memory_limit_bytes = saved_.memory_limit;
        config::lance_index_worker_pids_max = saved_.pids_max;
        config::lance_index_worker_wallclock_limit_seconds = saved_.wallclock;
        config::lance_index_worker_term_grace_seconds = saved_.term_grace;
        config::lance_index_worker_as_limit_bytes = saved_.as_limit;
        config::lance_index_worker_cpu_limit_multiplier = saved_.cpu_multiplier;
        // Best-effort sweep of invocation cgroups this test may have leaked
        // (only possible after a failed assertion; the supervisor itself
        // removes them once populated=0). Removes only dirs this test recorded.
        for (const std::string& dir : created_cgroup_dirs_) {
            sweep_cgroup_dir(dir, 15000);
        }
        signal(SIGCHLD, old_sigchld_);
        if (!test_dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(test_dir_, ec);
        }
    }

    // Removes a leftover invocation cgroup once it is empty; a surviving
    // descendant may outlive the failed assertion by a few seconds.
    static void sweep_cgroup_dir(const std::string& dir, int64_t deadline_ms) {
        if (::access(dir.c_str(), F_OK) != 0) {
            return;
        }
        const int64_t deadline = steady_millis() + deadline_ms;
        while (steady_millis() < deadline) {
            if (::rmdir(dir.c_str()) == 0) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        LOG(WARNING) << "lance supervisor test: could not remove leftover cgroup " << dir
                     << " (still populated); leaving it behind";
    }

    static std::string fake_worker_path() {
        char exe[4096];
        const ssize_t n = ::readlink("/proc/self/exe", exe, sizeof(exe) - 1);
        if (n <= 0) {
            return "";
        }
        exe[n] = '\0';
        const std::string path(exe, static_cast<size_t>(n));
        return path.substr(0, path.rfind('/')) + "/lance_fake_worker";
    }

    // "" when no writable delegated cgroup v2 parent is available here.
    static std::string detect_delegated_parent() {
        if (const char* env = std::getenv("LANCE_TEST_CGROUP_PARENT");
            env != nullptr && *env != '\0') {
            return env;
        }
        std::string resolved;
        if (LanceIndexSupervisor::resolve_cgroup_parent("", &resolved).ok()) {
            return resolved;
        }
        return "";
    }

    TLanceIndexWorkerDispatch make_dispatch(const std::string& tag, int64_t deadline_ms) {
        TLanceIndexWorkerDispatch dispatch;
        dispatch.invocation_id = tag + "-" + std::to_string(getpid()) + "-" +
                                 std::to_string(s_invocation_counter.fetch_add(1));
        dispatch.be_process_epoch = 777;
        dispatch.deadline_ms = deadline_ms;
        dispatch.mutation_type = TLanceIndexWorkerMutationType::CREATE;
        dispatch.index_name = "idx_v";
        dispatch.column_name = "v";
        dispatch.index_type = "IVF_PQ";
        dispatch.dataset_uri = "s3://bucket/dataset";
        dispatch.admitted_dataset_version = 9;
        dispatch.__set_schema_contract_json("{}");
        return dispatch;
    }

    // The persona argv the fake worker consumes. The result frame carries no
    // identity echo (the reply is matched to the request structurally), so
    // unlike the closed series nothing but the persona and its key=value
    // parameters crosses argv.
    static std::vector<std::string> persona_args(const std::string& persona,
                                                 const std::vector<std::string>& extra = {}) {
        std::vector<std::string> args = {persona};
        args.insert(args.end(), extra.begin(), extra.end());
        return args;
    }

    // The supervisor-side invocation cgroup dir (same formula as execute()).
    static std::string invocation_cgroup_dir(const std::string& parent,
                                             const TLanceIndexWorkerDispatch& dispatch) {
        return parent + "/" + LanceIndexSupervisor::invocation_cgroup_name(dispatch.invocation_id);
    }

    // Real preflight against the delegated parent; registers the invocation
    // cgroup dir for the TearDown sweep.
    void preflight_supervisor(LanceIndexSupervisor* supervisor, const std::string& parent,
                              const std::vector<TLanceIndexWorkerDispatch>& dispatches) {
        config::lance_index_worker_cgroup_parent = parent;
        Status status = supervisor->preflight();
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_TRUE(supervisor->isolation_verified());
        for (const auto& dispatch : dispatches) {
            created_cgroup_dirs_.push_back(invocation_cgroup_dir(parent, dispatch));
        }
    }
};

std::atomic<int> LanceIndexSupervisorTest::s_invocation_counter {0};

// ---------------------------------------------------------------------------
// Happy path: handshake + in-contract result frame, dual reap evidence.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, HappyResultWithReapProof) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("happy", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("happy"));

    WorkerExecution execution;
    const Status status = supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms),
                                             &execution);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::TRUSTED_RESULT);
    ASSERT_TRUE(execution.result.has_value());
    EXPECT_EQ(execution.result->phase, TLanceIndexWorkerResultPhase::NATIVE_RETURN);
    EXPECT_EQ(execution.result->lance_error_code, 0);
    // A plain native success carries no message category.
    EXPECT_TRUE(execution.message.empty());
    EXPECT_EQ(execution.failure_category, nullptr);
    // The child provably exited and the invocation cgroup drained — and since
    // execute() returned, this is already the respond-after-reap evidence.
    EXPECT_TRUE(execution.child_reaped_proof);
    EXPECT_EQ(supervisor.inflight_count_for_test(), 0);
    EXPECT_EQ(::access(invocation_cgroup_dir(parent, dispatch).c_str(), F_OK), -1)
            << "invocation cgroup should be gone";
}

// D12 second line: the supervisor rebuilds the message from its own category
// table; worker text is never forwarded.
TEST_F(LanceIndexSupervisorTest, WorkerMessageNeverForwarded) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("smuggle", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(
            fake_worker_path(), persona_args("happy", {"code=8", "msg=pwned-ss-credential-leak"}));

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::TRUSTED_RESULT);
    ASSERT_TRUE(execution.result.has_value());
    EXPECT_EQ(execution.result->phase, TLanceIndexWorkerResultPhase::NATIVE_RETURN);
    EXPECT_EQ(execution.result->lance_error_code, 8);
    EXPECT_EQ(execution.message,
              LanceIndexSupervisor::sanitize_message("lance native error", dispatch));
    EXPECT_EQ(execution.message.find("pwned"), std::string::npos);
}

// The trusted pre-invocation rejection band passes through verbatim.
TEST_F(LanceIndexSupervisorTest, TrustedRejectionBandPassesThrough) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("reject5", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("reject_unresolved"));

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::TRUSTED_RESULT);
    ASSERT_TRUE(execution.result.has_value());
    EXPECT_EQ(execution.result->phase, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED);
    EXPECT_EQ(execution.result->lance_error_code, -5);
    EXPECT_EQ(execution.message,
              LanceIndexSupervisor::sanitize_message("index unresolved", dispatch));
    EXPECT_TRUE(execution.child_reaped_proof);
}

// The authoritative pre-invocation IF no-op keeps its fixed static note (the
// one exact-match passthrough the second-line sanitizer allows).
TEST_F(LanceIndexSupervisorTest, AuthoritativeNoOpKeepsItsNote) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("noop", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("no_op"));

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::TRUSTED_RESULT);
    ASSERT_TRUE(execution.result.has_value());
    EXPECT_EQ(execution.result->phase, TLanceIndexWorkerResultPhase::NATIVE_RETURN);
    EXPECT_EQ(execution.result->lance_error_code, 0);
    EXPECT_EQ(execution.message,
              LanceIndexSupervisor::sanitize_message("index absent; drop if exists no-op",
                                                     dispatch));
}

// ---------------------------------------------------------------------------
// Pre-FFI protocol violations: malformed/undecodable handshakes and
// self-report mismatches all end as a PROVEN pre-invocation rejection (the
// child DID exec — never a never-launched status), riding the reap proof.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, PreFfiViolationsRejectedWithProof) {
    REQUIRE_DELEGATED_PARENT(parent);
    struct PersonaCase {
        const char* persona;
        const char* category;
    };
    const PersonaCase cases[] = {
            {"garbage", "undecodable handshake frame"},
            {"oversized", "malformed handshake frame"},
            {"deep_nest", "undecodable handshake frame"},
            {"bad_cgroup", "handshake cgroup path mismatch"},
            {"bad_limits", "handshake memory limit mismatch"},
    };
    for (const PersonaCase& c : cases) {
        LanceIndexSupervisor supervisor;
        const auto dispatch = make_dispatch(c.persona, epoch_millis() + 3600 * 1000);
        preflight_supervisor(&supervisor, parent, {dispatch});
        supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args(c.persona));

        WorkerExecution execution;
        const Status status =
                supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution);
        ASSERT_TRUE(status.ok()) << "persona " << c.persona << ": " << status.to_string();
        EXPECT_EQ(execution.kind, WorkerExecution::Kind::PROVEN_PRE_INVOCATION_REJECTION)
                << "persona " << c.persona;
        ASSERT_TRUE(execution.result.has_value()) << "persona " << c.persona;
        EXPECT_EQ(execution.result->phase, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED)
                << "persona " << c.persona;
        EXPECT_EQ(execution.result->lance_error_code, -4) << "persona " << c.persona;
        ASSERT_TRUE(execution.child_reaped_proof) << "persona " << c.persona;
        EXPECT_EQ(execution.message, LanceIndexSupervisor::sanitize_message(c.category, dispatch))
                << "persona " << c.persona;
    }
}

// ---------------------------------------------------------------------------
// Result-frame contract (band rules): a complete frame whose phase/code pair
// is outside the pipe contract (unknown phase value, phase 1 with a
// non-negative code, phase 2 with a negative code) is a protocol violation —
// never a trusted result, and never a pre-FFI rejection (the frame arrived
// past a valid handshake, so a mutation may have committed). Indeterminate.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, ResultBandOutsideContractDropped) {
    REQUIRE_DELEGATED_PARENT(parent);
    struct BadCase {
        const char* phase;
        const char* code;
    };
    const BadCase cases[] = {{"1", "0"}, {"2", "-4"}, {"99", "0"}};
    for (const BadCase& c : cases) {
        LanceIndexSupervisor supervisor;
        const auto dispatch = make_dispatch("badband", epoch_millis() + 3600 * 1000);
        preflight_supervisor(&supervisor, parent, {dispatch});
        supervisor.force_worker_exec_for_test(
                fake_worker_path(),
                persona_args("happy", {"phase=" + std::string(c.phase),
                                       "code=" + std::string(c.code)}));

        WorkerExecution execution;
        const Status status =
                supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution);
        ASSERT_TRUE(status.ok()) << "phase=" << c.phase << " code=" << c.code << ": "
                                 << status.to_string();
        EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE)
                << "phase=" << c.phase << " code=" << c.code;
        EXPECT_FALSE(execution.result.has_value())
                << "an out-of-contract frame was forwarded as a trusted result: phase="
                << c.phase << " code=" << c.code;
        EXPECT_NE(execution.failure_category, nullptr);
        EXPECT_TRUE(execution.child_reaped_proof);
    }
}

// ---------------------------------------------------------------------------
// Out-of-order worker: the handshake lands while a >64KiB dispatch frame is
// still draining into stdin. Closing stdin must not regress the phase back to
// READ_HANDSHAKE — the result frame keeps the result-side parsing and bounds.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, OutOfOrderHandshakeKeepsResultPhase) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    auto dispatch = make_dispatch("earlyhs", epoch_millis() + 3600 * 1000);
    // The dispatch frame must exceed the 64 KiB pipe buffer so the
    // supervisor's stdin write is still in flight when the handshake arrives.
    dispatch.dataset_uri = "s3://bucket/" + std::string(150 * 1024, 'x');
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("early_handshake"));

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::TRUSTED_RESULT)
            << "the result frame was misparsed after the stdin-close phase regression";
    ASSERT_TRUE(execution.result.has_value());
    EXPECT_EQ(execution.result->lance_error_code, 0);
}

// ---------------------------------------------------------------------------
// Silent deaths (no complete frame): never a trusted result; indeterminate,
// with the dual reap evidence when it holds.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, SilentExitIsIndeterminate) {
    REQUIRE_DELEGATED_PARENT(parent);
    for (const char* persona : {"exit_no_frame", "exit_partial", "skip_result"}) {
        LanceIndexSupervisor supervisor;
        const auto dispatch = make_dispatch(persona, epoch_millis() + 3600 * 1000);
        preflight_supervisor(&supervisor, parent, {dispatch});
        supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args(persona));

        WorkerExecution execution;
        const Status status =
                supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution);
        ASSERT_TRUE(status.ok()) << "persona " << persona << ": " << status.to_string();
        EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE) << "persona " << persona;
        EXPECT_FALSE(execution.result.has_value()) << "persona " << persona;
        ASSERT_TRUE(execution.child_reaped_proof) << "persona " << persona;
        ASSERT_FALSE(execution.message.empty()) << "persona " << persona;
    }
}

// The fake floods stdout+stderr (1 MiB each) without ever reading stdin: the
// supervisor must drain both streams in parallel (no pipe deadlock), bound the
// stderr capture, and converge on the garbage prefix as a pre-FFI violation.
TEST_F(LanceIndexSupervisorTest, FloodPipesDrainedNoDeadlock) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("flood", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("flood"));
    const int64_t started = steady_millis();

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok())
            << "the supervisor deadlocked on flooded pipes";
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::PROVEN_PRE_INVOCATION_REJECTION);
    EXPECT_LT(steady_millis() - started, 20000);
}

// ---------------------------------------------------------------------------
// Kill ladder: the watchdog fires at the caller-computed kill deadline
// (TERM -> grace -> KILL), and the configured wall-clock cap still bounds it.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, KillDeadlineDrivesTermThenKill) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_term_grace_seconds = 2;
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("hang", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("hang"));
    const int64_t started = steady_millis();

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, /*kill_deadline_ms=*/epoch_millis() + 6000,
                                   &execution)
                        .ok());
    const int64_t elapsed = steady_millis() - started;
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE);
    ASSERT_TRUE(execution.child_reaped_proof);
    // The TERM lands only at the 6s kill deadline; the hanging persona dies on
    // TERM (default disposition), well inside the 2s grace.
    EXPECT_GE(elapsed, 5500) << "the kill deadline fired early";
    EXPECT_LT(elapsed, 25000);
}

TEST_F(LanceIndexSupervisorTest, TermIgnoringWorkerKilledAfterGrace) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_term_grace_seconds = 2;
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("termign", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("term_ignore"));
    const int64_t started = steady_millis();

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, /*kill_deadline_ms=*/epoch_millis() + 6000,
                                   &execution)
                        .ok());
    const int64_t elapsed = steady_millis() - started;
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE);
    ASSERT_TRUE(execution.child_reaped_proof);
    // SIGTERM is ignored; the SIGKILL lands only after the 6s deadline + 2s grace.
    EXPECT_GE(elapsed, 7000) << "SIGKILL fired before the TERM grace elapsed";
    EXPECT_LT(elapsed, 30000);
}

TEST_F(LanceIndexSupervisorTest, WallclockConfigStillCapsTheKillDeadline) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_term_grace_seconds = 2;
    config::lance_index_worker_wallclock_limit_seconds = 6;
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("cfgcap", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("hang"));
    const int64_t started = steady_millis();

    WorkerExecution execution;
    // A kill deadline an hour out; the configured 6s ceiling must win.
    ASSERT_TRUE(supervisor.execute(dispatch, epoch_millis() + 3600 * 1000, &execution).ok());
    const int64_t elapsed = steady_millis() - started;
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE);
    EXPECT_GE(elapsed, 5500) << "the wall-clock cap fired early";
    EXPECT_LT(elapsed, 25000);
}

// ---------------------------------------------------------------------------
// Fork barrier: a cgroup migration failure must provably kill the child before
// exec (no exec side effects) and return a never-launched rejection.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, ForkBarrierKillsBeforeExec) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("barrier", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    const std::string mark = test_dir_ + "/child-execed.marker";
    supervisor.force_worker_exec_for_test(
            fake_worker_path(), persona_args("mark_hang", {"mark=" + mark}));
    supervisor.force_cgroup_migration_failure_for_test();
    const int64_t started = steady_millis();

    WorkerExecution execution;
    const Status status = supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms),
                                             &execution);
    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::CGROUP_ERROR>()) << status.to_string();
    // The launch-failure kill+reap path is bounded by the child's actual death,
    // never by a fixed timeout spin (a kill(pid,0)-only loop would burn the
    // whole 5s reap bound on a zombie).
    EXPECT_LT(steady_millis() - started, 4000)
            << "the launch-failure reap regressed to a full-timeout spin";
    EXPECT_EQ(status.to_string(),
              Status::CgroupError("{}",
                                  LanceIndexSupervisor::sanitize_message(
                                          "cgroup migration failed", dispatch))
                      .to_string());
    // The fork barrier held: the child was killed before exec, so its very
    // first side effect (the marker file) provably never happened.
    EXPECT_EQ(::access(mark.c_str(), F_OK), -1)
            << "the fake worker exec'd despite the migration failure";
    EXPECT_EQ(supervisor.inflight_count_for_test(), 0);
}

// ---------------------------------------------------------------------------
// D3 dequeue rail: an invocation whose remaining kill budget cannot fund a
// minimal worker run is rejected before fork. No delegation needed: the rail
// sits before any cgroup work.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, DeadlineBudgetExhaustedNeverLaunched) {
    LanceIndexSupervisor supervisor;
    supervisor._isolation_verified.store(true);
    const auto dispatch = make_dispatch("budget", epoch_millis() + 3600 * 1000);
    const std::string mark = test_dir_ + "/budget-execed.marker";
    supervisor.force_worker_exec_for_test(
            fake_worker_path(), persona_args("mark_hang", {"mark=" + mark}));

    // 2000ms of kill budget < kMinWorkerRunMs (3000ms).
    WorkerExecution execution;
    const Status status = supervisor.execute(dispatch, epoch_millis() + 2000, &execution);
    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::CGROUP_ERROR>()) << status.to_string();
    EXPECT_EQ(status.to_string(),
              Status::CgroupError("{}",
                                  LanceIndexSupervisor::sanitize_message(
                                          "deadline budget exhausted", dispatch))
                      .to_string());
    EXPECT_FALSE(execution.result.has_value());
    EXPECT_EQ(::access(mark.c_str(), F_OK), -1) << "a budget-rejected invocation forked";
    EXPECT_EQ(supervisor.inflight_count_for_test(), 0);
}

// ---------------------------------------------------------------------------
// stop() discipline (N4): after stop() no new worker may ever launch (a late
// launch would escape the kill snapshot); the in-flight domain is reaped by
// its own (caller-thread) execute() as usual.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, StopNeverLaunchesNewWorkers) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_term_grace_seconds = 2;
    LanceIndexSupervisor supervisor;
    const auto hanging = make_dispatch("stop-hang", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {hanging});
    const std::string hang_mark = test_dir_ + "/stop-hang.pid";
    supervisor.force_worker_exec_for_test(
            fake_worker_path(), persona_args("mark_hang", {"mark=" + hang_mark}));

    Status flight_status = Status::InternalError("unset");
    WorkerExecution flight_execution;
    std::thread flight([&] {
        flight_status = supervisor.execute(hanging, kill_deadline_for(hanging.deadline_ms),
                                           &flight_execution);
    });
    // Wait for the worker to provably exec (its marker carries its pid), so it
    // is registered in the in-flight set before stop() snapshots it.
    ASSERT_TRUE(wait_until([&] { return ::access(hang_mark.c_str(), F_OK) == 0; }, 15000))
            << "the in-flight worker never exec'd";

    const auto queued = make_dispatch("stop-queued", epoch_millis() + 3600 * 1000);
    const std::string queued_dir = invocation_cgroup_dir(parent, queued);
    const int64_t stop_started = steady_millis();
    supervisor.stop();
    EXPECT_LT(steady_millis() - stop_started, 20000) << "stop() hung on the in-flight domain";

    // The post-stop invocation is refused before any cgroup/fork work.
    WorkerExecution queued_execution;
    const Status queued_status =
            supervisor.execute(queued, kill_deadline_for(queued.deadline_ms), &queued_execution);
    EXPECT_TRUE(queued_status.is<ErrorCode::CANCELLED>()) << queued_status.to_string();
    EXPECT_FALSE(queued_execution.result.has_value());
    EXPECT_EQ(::access(queued_dir.c_str(), F_OK), -1)
            << "a post-stop invocation launched a worker";

    // The in-flight domain converged on its own thread: stop()'s TERM ended
    // the hanging worker and its execute() returned indeterminate after the
    // reap (respond-after-reap even on the stop path).
    flight.join();
    EXPECT_TRUE(flight_status.ok()) << flight_status.to_string();
    EXPECT_EQ(flight_execution.kind, WorkerExecution::Kind::INDETERMINATE);
    EXPECT_TRUE(flight_execution.child_reaped_proof);
}

// ---------------------------------------------------------------------------
// SF2: the stop()/launch race. The prelude between execute()'s entry
// _stopping check and the in-flight registration spans real milliseconds
// (frame encode, env build, four pipes, cgroup writes, fork, migration), and
// the case above never exercises it - it waits for the worker's exec marker,
// i.e. for the registration to have happened, before stopping. A stop() that
// runs entirely INSIDE the window snapshots an empty registry: both kill
// passes are no-ops, and the child registered afterwards used to escape them
// both - riding its own wall-clock ceiling (an hour by default) while the
// service's stop() joins the executor thread. The hang_before_launch fault
// point pins execute() deterministically inside the window so the race needs
// no timing luck: stop() must return promptly on the empty registry, and the
// released execute() must kill its own just-registered child (the
// post-registration _stopping recheck) and converge it - launched, killed,
// reaped, indeterminate - in bounded time.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, StopRacingTheLaunchPreludeStillKillsTheLateChild) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_term_grace_seconds = 1;
    // Without the fix the child would ride this ceiling (8s) before its own
    // ladder converges it; with the fix it dies immediately after the
    // registration. The bound below (5s) separates the two by construction.
    config::lance_index_worker_wallclock_limit_seconds = 8;
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("stop-race", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    const std::string mark = test_dir_ + "/stop-race.marker";
    supervisor.force_worker_exec_for_test(
            fake_worker_path(), persona_args("mark_hang", {"mark=" + mark}));

    constexpr const char* kPreludePoint = "LanceIndexSupervisor.hang_before_launch";
    const bool saved_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add(kPreludePoint);
    Defer defer([saved_debug_points] {
        DebugPoints::instance()->remove("LanceIndexSupervisor.hang_before_launch");
        config::enable_debug_points = saved_debug_points;
    });

    Status flight_status = Status::InternalError("unset");
    WorkerExecution flight_execution;
    std::thread flight([&] {
        flight_status = supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms),
                                           &flight_execution);
    });
    // execute() is provably inside the launch prelude once the in-flight
    // count rises: the block sits after the count guard and before any
    // frame/pipe/cgroup/fork work, so a risen count can only mean a thread
    // parked in the block - it already passed the entry _stopping check.
    if (!wait_until([&] { return supervisor.inflight_count_for_test() >= 1; }, 15000)) {
        DebugPoints::instance()->remove(kPreludePoint);
        flight.join();
        FAIL() << "execute() never reached the launch prelude";
    }

    // stop() against the provably still-empty registry: both kill snapshots
    // see nothing and the drain check breaks at once - the exact escape
    // window this test exists for.
    const int64_t stop_started = steady_millis();
    supervisor.stop();
    EXPECT_LT(steady_millis() - stop_started, 5000) << "stop() hung on an empty registry";

    // Release the prelude: launch -> registration -> the post-registration
    // _stopping recheck must kill the group itself, and supervise_child then
    // converges the invocation on this same thread.
    DebugPoints::instance()->remove(kPreludePoint);
    const int64_t released = steady_millis();
    flight.join();
    EXPECT_LT(steady_millis() - released, 5000)
            << "the late-registered child escaped the recheck and rode its own ladder";

    // The child was released past the barrier before the kill (a Cancelled
    // never-launched answer would be a lie): the honest ending is the
    // converged indeterminate WITH the reap proof - respond-after-reap holds
    // on the raced path too. The recheck's KILL legitimately races the exec
    // itself, so the marker file may or may not have appeared; the launched
    // classification is the invariant, not the marker.
    EXPECT_TRUE(flight_status.ok()) << flight_status.to_string();
    EXPECT_EQ(flight_execution.kind, WorkerExecution::Kind::INDETERMINATE);
    EXPECT_FALSE(flight_execution.result.has_value());
    EXPECT_TRUE(flight_execution.child_reaped_proof);
    EXPECT_EQ(supervisor.inflight_count_for_test(), 0);
    EXPECT_EQ(::access(invocation_cgroup_dir(parent, dispatch).c_str(), F_OK), -1)
            << "the raced invocation cgroup was left behind";
}

// ---------------------------------------------------------------------------
// Preflight guards (no delegation needed).
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, PreflightForcedFailureRejectsExecutions) {
    LanceIndexSupervisor supervisor;
    supervisor.force_preflight_failure_for_test();
    const Status status = supervisor.preflight();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::CGROUP_ERROR>()) << status.to_string();
    EXPECT_FALSE(supervisor.isolation_verified());
    const auto dispatch = make_dispatch("preflight-forced", epoch_millis() + 3600 * 1000);
    WorkerExecution execution;
    const Status execute_status = supervisor.execute(
            dispatch, kill_deadline_for(dispatch.deadline_ms), &execution);
    EXPECT_TRUE(execute_status.is<ErrorCode::CGROUP_ERROR>()) << execute_status.to_string();
}

TEST_F(LanceIndexSupervisorTest, PreflightDisabledStaysUnverified) {
    config::lance_index_isolation_preflight = false;
    LanceIndexSupervisor supervisor;
    // The switch only skips the probe; it never verifies isolation.
    EXPECT_TRUE(supervisor.preflight().ok());
    EXPECT_FALSE(supervisor.isolation_verified());
    const auto dispatch = make_dispatch("preflight-off", epoch_millis() + 3600 * 1000);
    WorkerExecution execution;
    const Status execute_status = supervisor.execute(
            dispatch, kill_deadline_for(dispatch.deadline_ms), &execution);
    EXPECT_TRUE(execute_status.is<ErrorCode::CGROUP_ERROR>()) << execute_status.to_string();
}

TEST_F(LanceIndexSupervisorTest, ResolveCgroupParentRejectsInvalidConfig) {
    std::string resolved;
    EXPECT_FALSE(LanceIndexSupervisor::resolve_cgroup_parent("/etc", &resolved).ok());
    EXPECT_FALSE(LanceIndexSupervisor::resolve_cgroup_parent("/sys/fs/cgroup/../cgroup", &resolved)
                         .ok());
    EXPECT_FALSE(LanceIndexSupervisor::resolve_cgroup_parent(
                         "/sys/fs/cgroup/lance-ut-definitely-missing-dir", &resolved)
                         .ok());
    // Prefix-boundary shapes: a path that merely SHARES the /sys/fs/cgroup
    // prefix is not under the cgroup root.
    EXPECT_FALSE(
            LanceIndexSupervisor::resolve_cgroup_parent("/sys/fs/cgroup-anything", &resolved)
                    .ok());
    EXPECT_FALSE(LanceIndexSupervisor::resolve_cgroup_parent("/sys/fs/cgroupx", &resolved).ok());
}

// The supervisor's termination proofs rest on pidfd_open (kernel 5.3) and
// waitid(P_PIDFD) (kernel 5.9); the preflight probes both and fails closed on
// older kernels. This pins that the UT host itself satisfies the minimum — no
// cgroup delegation needed for the bare primitive.
TEST_F(LanceIndexSupervisorTest, PidfdWaitidCapabilityProbePassesOnThisHost) {
    const pid_t child = ::fork();
    ASSERT_NE(child, -1) << strerror(errno);
    if (child == 0) {
        ::_exit(0);
    }
    const int pidfd = static_cast<int>(::syscall(SYS_pidfd_open, child, 0));
    ASSERT_GE(pidfd, 0) << "pidfd_open unsupported on this host: " << strerror(errno);
    siginfo_t info;
    std::memset(&info, 0, sizeof(info));
    // Blocking WEXITED wait on the pidfd: the child has already _exit(0)'d or is
    // about to; this can only hang if the kernel mis-delivers, which is exactly
    // what the probe exists to catch.
    const int rc = ::waitid(kPidfdIdtype, static_cast<id_t>(pidfd), &info, WEXITED);
    const int wait_errno = errno;
    ::close(pidfd);
    ASSERT_EQ(rc, 0) << "waitid(P_PIDFD) unsupported on this host (kernel >= 5.9 "
                        "required): "
                     << strerror(wait_errno);
    EXPECT_EQ(info.si_pid, child);
    EXPECT_EQ(info.si_code, CLD_EXITED);
    EXPECT_EQ(info.si_status, 0);
}

// Auto-resolution reflects the environment: inside a delegated scope (the
// systemd-run runner) a from-scratch preflight succeeds and leaves controllers
// enabled; in a plain shell the read-only ancestry is correctly classified and
// isolation stays unverified (no soft fallback anywhere).
TEST_F(LanceIndexSupervisorTest, PreflightAutoResolveReflectsEnvironment) {
    config::lance_index_worker_cgroup_parent = "";
    std::string resolved;
    const Status resolve_status = LanceIndexSupervisor::resolve_cgroup_parent("", &resolved);
    LanceIndexSupervisor supervisor;
    const Status preflight_status = supervisor.preflight();
    if (resolve_status.ok()) {
        ASSERT_TRUE(preflight_status.ok()) << preflight_status.to_string();
        EXPECT_TRUE(supervisor.isolation_verified());
        // The resolved parent keeps +memory +pids enabled for the BE lifetime.
        std::ifstream control(resolved + "/cgroup.subtree_control");
        std::string content((std::istreambuf_iterator<char>(control)),
                            std::istreambuf_iterator<char>());
        EXPECT_NE(content.find("memory"), std::string::npos);
        EXPECT_NE(content.find("pids"), std::string::npos);
        // The probe group was removed again: no lance-preflight-* leftovers.
        for (const auto& entry : std::filesystem::directory_iterator(resolved)) {
            EXPECT_EQ(entry.path().filename().string().find("lance-preflight-"),
                      std::string::npos)
                    << "preflight probe leaked " << entry.path();
        }
    } else {
        EXPECT_FALSE(preflight_status.ok());
        EXPECT_TRUE(preflight_status.is<ErrorCode::CGROUP_ERROR>())
                << preflight_status.to_string();
        EXPECT_FALSE(supervisor.isolation_verified());
        const auto dispatch = make_dispatch("preflight-auto", epoch_millis() + 3600 * 1000);
        WorkerExecution execution;
        EXPECT_TRUE(supervisor
                            .execute(dispatch, kill_deadline_for(dispatch.deadline_ms),
                                     &execution)
                            .is<ErrorCode::CGROUP_ERROR>());
        LOG(INFO) << "lance preflight auto-resolve correctly classified this environment: "
                  << resolve_status.to_string();
    }
}

// ---------------------------------------------------------------------------
// Preflight probe hygiene: the dummy-child reap is bounded by the child's
// actual death (no full-timeout spin — that regressed every BE startup by 5s),
// and an empty stale probe group left by a SIGKILLed BE of the same pid is
// reclaimed instead of failing the probe as a delegation error.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, PreflightReapIsBoundedByChildDeath) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_cgroup_parent = parent;
    LanceIndexSupervisor supervisor;
    const int64_t started = steady_millis();
    const Status status = supervisor.preflight();
    const int64_t elapsed = steady_millis() - started;
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(supervisor.isolation_verified());
    EXPECT_LT(elapsed, 4000) << "preflight regressed to the full-timeout reap spin";
}

TEST_F(LanceIndexSupervisorTest, PreflightReclaimsStaleProbeLeftover) {
    REQUIRE_DELEGATED_PARENT(parent);
    // Plant an empty leftover probe group with this pid's prefix (a BE SIGKILLed
    // mid-probe cannot remove its own probe group). The preflight must reclaim
    // it and succeed — a stale leftover is not a delegation failure.
    const std::string leftover =
            parent + "/lance-preflight-" + std::to_string(getpid()) + "-stale";
    ASSERT_EQ(::mkdir(leftover.c_str(), 0755), 0) << strerror(errno);
    config::lance_index_worker_cgroup_parent = parent;
    LanceIndexSupervisor supervisor;
    const Status status = supervisor.preflight();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(supervisor.isolation_verified());
    EXPECT_EQ(::access(leftover.c_str(), F_OK), -1)
            << "the stale probe group was not reclaimed: " << leftover;
    // And the probe's own uniquely-named group was removed at the end.
    for (const auto& entry : std::filesystem::directory_iterator(parent)) {
        EXPECT_EQ(entry.path().filename().string().find("lance-preflight-"), std::string::npos)
                << "preflight probe leaked " << entry.path();
    }
}

// ---------------------------------------------------------------------------
// sanitize_message (pure static; D12 invariants).
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, SanitizeMessageStaticCategoryAndIdentity) {
    const auto dispatch = make_dispatch("sanitize", epoch_millis() + 3600 * 1000);
    const std::string message =
            LanceIndexSupervisor::sanitize_message("stale admission", dispatch);
    EXPECT_EQ(message, "stale admission: invocation_id=" + dispatch.invocation_id +
                               " be_process_epoch=777");
}

TEST_F(LanceIndexSupervisorTest, SanitizeMessageEscapesControlChars) {
    auto dispatch = make_dispatch("sanitize-ctl", epoch_millis() + 3600 * 1000);
    dispatch.invocation_id = std::string("inv\nwith\ttab\x01\x7f");
    const std::string message =
            LanceIndexSupervisor::sanitize_message("resource rejected", dispatch);
    EXPECT_EQ(message.find('\n'), std::string::npos);
    EXPECT_EQ(message.find('\t'), std::string::npos);
    EXPECT_NE(message.find("\\x0a"), std::string::npos);
    EXPECT_NE(message.find("\\x01"), std::string::npos);
    EXPECT_NE(message.find("\\x7f"), std::string::npos);
}

TEST_F(LanceIndexSupervisorTest, SanitizeMessageTruncatesUtf8Safely) {
    auto dispatch = make_dispatch("sanitize-utf8", epoch_millis() + 3600 * 1000);
    // The raw message is "<category>: invocation_id=<inv> be_process_epoch=777".
    // Lay out <inv> so the 900-byte cut lands in the middle of a 3-byte UTF-8
    // character.
    const std::string prefix = "resource rejected: invocation_id=";
    std::string invocation(prefix.size() < 898 ? 898 - prefix.size() : 0, 'a');
    for (int i = 0; i < 200; ++i) {
        invocation += "\xE4\xB8\xAD"; // U+4E2D
    }
    dispatch.invocation_id = invocation;
    const std::string message =
            LanceIndexSupervisor::sanitize_message("resource rejected", dispatch);
    EXPECT_LE(message.size(), 900U);
    EXPECT_GE(message.size(), 890U);
    // Validate the truncated message is well-formed UTF-8 (no split sequence).
    const auto* bytes = reinterpret_cast<const unsigned char*>(message.data());
    size_t i = 0;
    while (i < message.size()) {
        const unsigned char c = bytes[i];
        size_t seq = 1;
        if ((c & 0x80) == 0) {
            seq = 1;
        } else if ((c & 0xE0) == 0xC0) {
            seq = 2;
        } else if ((c & 0xF0) == 0xE0) {
            seq = 3;
        } else if ((c & 0xF8) == 0xF0) {
            seq = 4;
        } else {
            FAIL() << "invalid UTF-8 start byte in truncated message";
        }
        ASSERT_LE(i + seq, message.size()) << "truncation split a UTF-8 sequence";
        for (size_t j = 1; j < seq; ++j) {
            ASSERT_EQ(bytes[i + j] & 0xC0, 0x80) << "invalid UTF-8 continuation";
        }
        i += seq;
    }
}

TEST_F(LanceIndexSupervisorTest, SanitizeMessageDropsOnStorageValueSubstring) {
    auto dispatch = make_dispatch("sanitize-secret", epoch_millis() + 3600 * 1000);
    dispatch.__set_storage_options({{"password", "s3cr3t-value-987"}});
    // The secret value rides inside an identity field (the exact leak shape the
    // second-line invariant defends against): the WHOLE message is dropped.
    dispatch.invocation_id = "inv-with-s3cr3t-value-987-inside";
    EXPECT_TRUE(LanceIndexSupervisor::sanitize_message("resource rejected", dispatch).empty());
}

TEST_F(LanceIndexSupervisorTest, SanitizeMessageKeepsShortValuesProtectedByConstruction) {
    auto dispatch = make_dispatch("sanitize-short", epoch_millis() + 3600 * 1000);
    // Values shorter than 4 bytes are not substring-checkable; the first-line
    // defense (static categories + numeric identity) is what protects them.
    dispatch.__set_storage_options({{"k", "abc"}});
    dispatch.invocation_id = "inv-abc";
    EXPECT_FALSE(LanceIndexSupervisor::sanitize_message("resource rejected", dispatch).empty());
}

TEST_F(LanceIndexSupervisorTest, SanitizeMessageEmptyCategoryMeansOmit) {
    const auto dispatch = make_dispatch("sanitize-empty", epoch_millis() + 3600 * 1000);
    EXPECT_TRUE(LanceIndexSupervisor::sanitize_message("", dispatch).empty());
}

// ---------------------------------------------------------------------------
// D5: the invocation cgroup is named after the invocation id (the series-3
// precise-kill primitive). Pure static; no delegation needed.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, InvocationCgroupNameSanitizesAndUniquifies) {
    const std::string plain = LanceIndexSupervisor::invocation_cgroup_name("inv-abc-123");
    EXPECT_TRUE(plain.rfind("lance-worker-", 0) == 0) << plain;
    EXPECT_NE(plain.find("inv-abc-123"), std::string::npos) << plain;

    // Hostile ids reduce to a path-safe projection: no '/', no control
    // characters, no '%'. Dots may survive (they stay inside the component),
    // but the fixed lance-worker- prefix plus the absence of '/' means the
    // name can never resolve as "." or ".." — the only dot shapes that
    // traverse.
    const std::string hostile =
            LanceIndexSupervisor::invocation_cgroup_name("../e\tv i\x01l%/x");
    EXPECT_EQ(hostile.find('/'), std::string::npos) << hostile;
    EXPECT_NE(hostile, ".");
    EXPECT_NE(hostile, "..");
    EXPECT_EQ(hostile.find('%'), std::string::npos) << hostile;
    for (const unsigned char c : hostile) {
        EXPECT_GE(c, 0x20u) << hostile;
        EXPECT_NE(c, 0x7fu) << hostile;
    }

    // Two raw ids that sanitize to the same projection stay distinct through
    // the raw-id hash suffix.
    const std::string a = LanceIndexSupervisor::invocation_cgroup_name("inv.1");
    const std::string b = LanceIndexSupervisor::invocation_cgroup_name("inv/1");
    EXPECT_NE(a, b);

    // A degenerate id still yields a usable directory name.
    const std::string degenerate = LanceIndexSupervisor::invocation_cgroup_name("///");
    EXPECT_TRUE(degenerate.rfind("lance-worker-", 0) == 0) << degenerate;
    EXPECT_LT(degenerate.size(), 255u);
}

// ---------------------------------------------------------------------------
// D11: the debug-point handoff is names-only and allowlist-filtered. Pure
// static; no delegation needed.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, WorkerDebugPointsEnvValueAllowlist) {
    using L = LanceIndexSupervisor;
    EXPECT_EQ(L::worker_debug_points_env_value({"LanceIndexWorker.hang",
                                                "LanceIndexWorker.skip_result"}),
              "LanceIndexWorker.hang,LanceIndexWorker.skip_result");
    // Canonical order regardless of input order; duplicates collapse.
    EXPECT_EQ(L::worker_debug_points_env_value({"LanceIndexWorker.skip_result",
                                                "LanceIndexWorker.hang",
                                                "LanceIndexWorker.hang"}),
              "LanceIndexWorker.hang,LanceIndexWorker.skip_result");
    // Unknown names never cross the exec boundary.
    EXPECT_EQ(L::worker_debug_points_env_value({"LanceIndexWorker.hang", "LanceIndexWorker.bogus",
                                                "DebugPoint.elsewhere"}),
              "LanceIndexWorker.hang");
    EXPECT_EQ(L::worker_debug_points_env_value({"LanceIndexWorker.bogus"}), "");
    EXPECT_EQ(L::worker_debug_points_env_value({}), "");
}

// ---------------------------------------------------------------------------
// D11: LanceIndexSupervisor.reject_after_enqueue fires at the synchronous
// entry, after the admission gates and before ANY cgroup/fork work — the
// deterministic never-launched rejection for every runner (no delegation
// needed). With the point removed the very next invocation proceeds to real
// (here: hermetic and failing) cgroup work with a different category.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, RejectAfterEnqueueDeterministicRejection) {
    const bool saved_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add("LanceIndexSupervisor.reject_after_enqueue");
    Defer defer([saved_debug_points] {
        DebugPoints::instance()->remove("LanceIndexSupervisor.reject_after_enqueue");
        config::enable_debug_points = saved_debug_points;
    });

    LanceIndexSupervisor supervisor;
    supervisor._isolation_verified.store(true);
    // Keep the post-point path hermetic: if the point ever stops firing, the
    // invocation lands in the test dir where the cgroup limit write fails (no
    // cgroupfs) and the rejection category differs from the debug one.
    supervisor._cgroup_parent_abs = test_dir_;
    const auto dispatch = make_dispatch("dbg-reject", epoch_millis() + 3600 * 1000);
    const std::string mark = test_dir_ + "/reject-execed.marker";
    supervisor.force_worker_exec_for_test(
            fake_worker_path(), persona_args("mark_hang", {"mark=" + mark}));

    WorkerExecution execution;
    const Status status = supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms),
                                             &execution);
    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::CGROUP_ERROR>()) << status.to_string();
    EXPECT_EQ(status.to_string(),
              Status::CgroupError("{}",
                                  LanceIndexSupervisor::sanitize_message(
                                          "debug point rejection", dispatch))
                      .to_string());
    // No fork happened: the marker never appeared and inflight never rose.
    EXPECT_EQ(::access(mark.c_str(), F_OK), -1) << "a debug-rejected invocation forked";
    EXPECT_EQ(supervisor.inflight_count_for_test(), 0);
    EXPECT_FALSE(execution.result.has_value());

    // With the point removed the same entry returns to the normal path: the
    // next invocation reaches the (hermetic, failing) cgroup setup instead of
    // the debug short-circuit — still no fork, but a different category.
    DebugPoints::instance()->remove("LanceIndexSupervisor.reject_after_enqueue");
    const auto second = make_dispatch("dbg-reject-off", epoch_millis() + 3600 * 1000);
    WorkerExecution second_execution;
    const Status second_status = supervisor.execute(
            second, kill_deadline_for(second.deadline_ms), &second_execution);
    ASSERT_FALSE(second_status.ok());
    EXPECT_EQ(second_status.to_string(),
              Status::CgroupError("{}",
                                  LanceIndexSupervisor::sanitize_message(
                                          "cgroup limit setup failed", second))
                      .to_string());
    EXPECT_EQ(::access(mark.c_str(), F_OK), -1);
    EXPECT_EQ(supervisor.inflight_count_for_test(), 0);
}

// ---------------------------------------------------------------------------
// D11: the dispatch frame's field 18 rides the controlled environment into
// the exec'd worker (DORIS_LANCE_WORKER_DEBUG_POINTS). The fake worker honors
// the same channel, so these cases drive the whole frame->env->behavior chain
// across a real fork+exec.
// ---------------------------------------------------------------------------

// hang: the fake completes the handshake and then hangs until the kill
// deadline converges it (the e2e indeterminate shape).
TEST_F(LanceIndexSupervisorTest, FrameDebugPointsHangRidesTheExec) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_term_grace_seconds = 2;
    LanceIndexSupervisor supervisor;
    auto dispatch = make_dispatch("dbg-hang", epoch_millis() + 3600 * 1000);
    dispatch.__set_debug_points({"LanceIndexWorker.hang"});
    preflight_supervisor(&supervisor, parent, {dispatch});
    // The happy persona would complete immediately; the env handoff is what
    // makes the worker hang (proving the point rode across the exec).
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("happy"));
    const int64_t started = steady_millis();

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, /*kill_deadline_ms=*/epoch_millis() + 6000,
                                   &execution)
                        .ok());
    const int64_t elapsed = steady_millis() - started;
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE);
    EXPECT_FALSE(execution.result.has_value());
    ASSERT_TRUE(execution.child_reaped_proof);
    // The handshake passed and the hang held until the 6s kill deadline.
    EXPECT_GE(elapsed, 5500) << "the kill deadline fired early";
    EXPECT_LT(elapsed, 25000);
}

// skip_result: the fake completes the handshake, then exits 0 with no result
// frame (a complete-exec-but-silent child) — indeterminate, never a result.
TEST_F(LanceIndexSupervisorTest, FrameDebugPointsSkipResultRidesTheExec) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    auto dispatch = make_dispatch("dbg-skip", epoch_millis() + 3600 * 1000);
    dispatch.__set_debug_points({"LanceIndexWorker.skip_result"});
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("happy"));

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE);
    EXPECT_FALSE(execution.result.has_value())
            << "a silent-exit worker must never produce a trusted result";
    ASSERT_TRUE(execution.child_reaped_proof);
}

// A name outside the allowlist never reaches the environment: the same happy
// persona completes normally instead of hanging or going silent.
TEST_F(LanceIndexSupervisorTest, FrameDebugPointsUnknownNameFiltered) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    auto dispatch = make_dispatch("dbg-unknown", epoch_millis() + 3600 * 1000);
    dispatch.__set_debug_points({"LanceIndexWorker.bogus", "DebugPoint.elsewhere"});
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("happy"));

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::TRUSTED_RESULT);
    ASSERT_TRUE(execution.result.has_value());
    EXPECT_EQ(execution.result->lance_error_code, 0);
}

// ---------------------------------------------------------------------------
// Real kernel-enforced boundary cases (the delegated runner executes the
// LanceIndexSupervisorTest.Real* filter).
// ---------------------------------------------------------------------------

// Real OOM: a 32 MiB invocation cgroup + malloc-bomb worker -> kernel oom_kill,
// indeterminate ending, supervisor (and a follow-up invocation) unaffected.
TEST_F(LanceIndexSupervisorTest, RealOomKillProofAndSupervisorSurvival) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_memory_limit_bytes = 32 * 1024 * 1024;
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("real-oom", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("malloc_bomb"));

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE);
    EXPECT_FALSE(execution.result.has_value()) << "an OOM-killed worker must not produce a result";
    EXPECT_TRUE(execution.child_reaped_proof);
    EXPECT_EQ(::access(invocation_cgroup_dir(parent, dispatch).c_str(), F_OK), -1)
            << "invocation cgroup left behind after populated=0";

    // The supervisor itself is unaffected: a follow-up happy invocation runs.
    const auto follow_up = make_dispatch("real-oom-next", epoch_millis() + 3600 * 1000);
    created_cgroup_dirs_.push_back(invocation_cgroup_dir(parent, follow_up));
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("happy"));
    WorkerExecution follow_up_execution;
    ASSERT_TRUE(supervisor
                        .execute(follow_up, kill_deadline_for(follow_up.deadline_ms),
                                 &follow_up_execution)
                        .ok());
    EXPECT_EQ(follow_up_execution.kind, WorkerExecution::Kind::TRUSTED_RESULT);
    EXPECT_EQ(follow_up_execution.result->lance_error_code, 0);
}

// Real pids.max: a thread-bomb worker is contained by the 16-pid bound.
TEST_F(LanceIndexSupervisorTest, RealPidsMaxContainsThreadBomb) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_pids_max = 16;
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("real-pids", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("thread_bomb"));

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE);
    EXPECT_FALSE(execution.result.has_value());
    EXPECT_TRUE(execution.child_reaped_proof);
}

// panic=abort shape: the worker dies of SIGABRT after its handshake; no
// trusted result, indeterminate.
TEST_F(LanceIndexSupervisorTest, RealAbortTerminationProof) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("real-abort", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("abort_now"));

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE);
    EXPECT_FALSE(execution.result.has_value());
    EXPECT_TRUE(execution.child_reaped_proof);
}

// CDC-reaper race: a side thread steals the child's exit status via
// waitpid(-1, WNOHANG) — exactly what the CDC manager's process-wide SIGCHLD
// reaper does. The supervisor must still converge: pidfd termination +
// populated=0 are the surviving evidence, and the trusted result stands.
TEST_F(LanceIndexSupervisorTest, RealCdcReaperRaceStillProvesReap) {
    REQUIRE_DELEGATED_PARENT(parent);
    // The thief guard joins the thread even when an assertion fires.
    struct ThiefGuard {
        std::atomic<bool> stop {false};
        std::atomic<int> stolen {0};
        std::thread thread;
        ThiefGuard()
                : thread([this] {
                      while (!stop.load(std::memory_order_relaxed)) {
                          int status = 0;
                          const pid_t reaped = ::waitpid(-1, &status, WNOHANG);
                          if (reaped > 0) {
                              stolen.fetch_add(1, std::memory_order_relaxed);
                              continue;
                          }
                          ::usleep(1000);
                      }
                  }) {}
            ~ThiefGuard() {
                stop.store(true);
                thread.join();
            }
    } thief;

    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("real-cdc", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("happy"));

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::TRUSTED_RESULT);
    ASSERT_TRUE(execution.result.has_value());
    EXPECT_EQ(execution.result->lance_error_code, 0);
    // Whoever consumed the exit status, the proof must stand on exact evidence.
    EXPECT_TRUE(execution.child_reaped_proof);
    LOG(INFO) << "cdc-race: thief thread reaped " << thief.stolen.load() << " child(ren)";
}

// Surviving descendant: the worker exits (silent death, no frames) while a
// grandchild holds the invocation cgroup populated. No fabricated proof: the
// ending is indeterminate WITHOUT child_reaped_proof, the populated cgroup is
// left in place (its limits still bind the leak), and execute() still returned
// — the respond-after-reap boundary holds with the bounded (not proven) wait.
// The leftover directory carries the invocation-id name (the series-3
// precise-kill primitive).
TEST_F(LanceIndexSupervisorTest, RealSurvivingDescendantNoFabricatedProof) {
    REQUIRE_DELEGATED_PARENT(parent);
    LanceIndexSupervisor supervisor;
    const auto dispatch = make_dispatch("real-orphan", epoch_millis() + 3600 * 1000);
    preflight_supervisor(&supervisor, parent, {dispatch});
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("orphan", {"sleep=8"}));
    const std::string cgroup_dir = invocation_cgroup_dir(parent, dispatch);

    WorkerExecution execution;
    ASSERT_TRUE(supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms), &execution)
                        .ok());
    EXPECT_EQ(execution.kind, WorkerExecution::Kind::INDETERMINATE);
    EXPECT_FALSE(execution.result.has_value());
    EXPECT_FALSE(execution.child_reaped_proof)
            << "the reap proof was fabricated without populated=0 evidence";
    EXPECT_EQ(::access(cgroup_dir.c_str(), F_OK), 0)
            << "the populated invocation cgroup must be left in place";
    EXPECT_EQ(cgroup_dir.substr(parent.size() + 1),
              LanceIndexSupervisor::invocation_cgroup_name(dispatch.invocation_id))
            << "the leftover cgroup lost the invocation-id name";

    // Cleanup discipline: once the grandchild exits (8s), the group drains and
    // the test removes what the supervisor intentionally left.
    sweep_cgroup_dir(cgroup_dir, 20000);
    EXPECT_EQ(::access(cgroup_dir.c_str(), F_OK), -1)
            << "leftover invocation cgroup never drained: " << cgroup_dir;
}

// BE loss: a SIGKILLed supervisor process must take its in-flight worker down
// through the worker-side PDEATHSIG arm — no stop(), no cleanup, exactly like
// a real crash. The harness forks a child that plays the BE role (supervisor +
// hanging fake worker); the test SIGKILLs that child and polls the worker pid
// (read from the marker the worker writes at exec) until kill(pid,0) reports
// ESRCH. A worker that outlives its supervisor is a boundary failure.
TEST_F(LanceIndexSupervisorTest, RealBeLossKillsWorkerViaPdeathsig) {
    REQUIRE_DELEGATED_PARENT(parent);
    const std::string mark = test_dir_ + "/beloss-worker.pid";
    int sync_pipe[2];
    ASSERT_EQ(::pipe(sync_pipe), 0) << strerror(errno);

    const auto dispatch = make_dispatch("beloss", epoch_millis() + 3600 * 1000);
    created_cgroup_dirs_.push_back(invocation_cgroup_dir(parent, dispatch));

    const pid_t be_child = ::fork();
    ASSERT_NE(be_child, -1) << strerror(errno);
    if (be_child == 0) {
        // The "BE" process. Arm its own parent-death guard first so a crashed
        // test process takes this subtree down too, then run a supervisor with
        // one hanging worker (on a worker thread, the synchronous shape's
        // in-flight domain) until the SIGKILL lands.
        ::close(sync_pipe[0]);
        ::prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0);
        if (::getppid() == 1) {
            ::_exit(2); // the test process is already gone
        }
        config::lance_index_worker_cgroup_parent = parent;
        LanceIndexSupervisor supervisor;
        if (!supervisor.preflight().ok()) {
            ::_exit(3);
        }
        supervisor.force_worker_exec_for_test(
                fake_worker_path(), persona_args("mark_hang", {"mark=" + mark}));
        std::thread flight([&] {
            WorkerExecution execution;
            (void)supervisor.execute(dispatch, kill_deadline_for(dispatch.deadline_ms),
                                     &execution);
        });
        // Announce once the worker has exec'd (its marker file exists; the pid
        // content is re-polled by the parent).
        for (int i = 0; i < 750 && ::access(mark.c_str(), F_OK) != 0; ++i) {
            ::usleep(20000);
        }
        const uint8_t ready = 0x42;
        if (::write(sync_pipe[1], &ready, 1) != 1) {
            ::_exit(5);
        }
        for (;;) {
            ::pause();
        }
    }
    ::close(sync_pipe[1]);
    // Reap-and-kill guard: every exit path of this test takes the subtree down
    // and reaps the supervising child.
    struct BeChildGuard {
        pid_t pid;
        ~BeChildGuard() {
            if (pid > 0) {
                ::kill(pid, SIGKILL);
                int status = 0;
                while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
                }
            }
        }
    } guard {be_child};

    // Bounded wait for the worker-up announcement.
    uint8_t ready = 0;
    ssize_t got = -1;
    ASSERT_TRUE(wait_until(
            [&] {
                struct pollfd pfd {sync_pipe[0], POLLIN, 0};
                if (::poll(&pfd, 1, 100) <= 0) {
                    return false;
                }
                got = ::read(sync_pipe[0], &ready, 1);
                return true;
            },
            30000))
            << "the supervising child never announced its worker";
    ::close(sync_pipe[0]);
    ASSERT_EQ(got, 1);
    ASSERT_EQ(ready, 0x42);

    // The worker pid from the marker (content lags the file's creation).
    pid_t worker_pid = -1;
    ASSERT_TRUE(wait_until(
            [&] {
                std::ifstream in(mark);
                long value = -1;
                if (!(in >> value)) {
                    return false;
                }
                worker_pid = static_cast<pid_t>(value);
                return worker_pid > 1;
            },
            10000))
            << "the worker never wrote its pid marker";
    ASSERT_EQ(::kill(worker_pid, 0), 0) << "the worker is not alive before the BE loss";

    // The BE loss itself: SIGKILL, then reap the supervising child.
    ASSERT_EQ(::kill(be_child, SIGKILL), 0);
    int status = 0;
    while (::waitpid(be_child, &status, 0) < 0 && errno == EINTR) {
    }
    guard.pid = -1;
    ASSERT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

    // The worker must die on its PDEATHSIG arm within a bounded window (the
    // delivery is immediate; the poll budget is generous for loaded runners).
    const bool worker_dead =
            wait_until([&] { return ::kill(worker_pid, 0) != 0 && errno == ESRCH; }, 15000);
    EXPECT_TRUE(worker_dead) << "worker pid " << worker_pid
                             << " outlived its SIGKILLed supervisor; the PDEATHSIG backstop "
                                "failed";
}

} // namespace doris::lance
