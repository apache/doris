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

#include "lance/index_job_supervisor.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <mutex>
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
// as index_job_supervisor.cpp).
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

// LanceIndexSupervisorTest: drives the real supervisor end-to-end against a
// tiny protocol-speaking fake worker executable (lance/fake_worker/
// lance_fake_worker.c, exec'd through the BE_TEST-only
// force_worker_exec_for_test seam) inside REAL invocation cgroups. The fake
// workers self-report their own confinement facts exactly like the real
// worker, so every handshake cross-check, kill escalation and reaping-proof
// branch of the supervisor runs for real.
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
// delegation-requiring cases SKIP_WITH_LOG loudly; the pure rejection-path
// cases (budget, preflight guards, sanitize_message) run everywhere.
//
// The Real* cases pin the kernel-enforced boundaries (OOM kill, pids.max,
// abort, CDC-reaper race, surviving descendant) and MUST run under a delegated
// runner:
//   systemd-run --user --scope --quiet bash -c 'cd <repo> && <recipe env> \
//     ./be/ut_build_RELEASE/test/doris_be_test --gtest_filter="LanceIndexSupervisorTest.Real*"'
//
// SIGCHLD hygiene (python_env_test.cpp:53-67 precedent): the fixture resets
// SIGCHLD to SIG_DFL and restores it afterwards; no test installs a SIGCHLD
// handler (the CDC-race case steals exit statuses with a waitpid(-1, WNOHANG)
// polling thread, never a handler).

namespace doris::lance {
namespace {

constexpr int64_t kCallbackDeadlineMs = 30000;

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

// Thread-safe sink for the supervisor callbacks (they fire on the supervisor
// executor threads). Readers take copies under the mutex.
struct ReportRecorder {
    std::mutex mu;
    std::vector<TLanceIndexJobReport> results;
    std::vector<TLanceIndexJobTerminationReport> terminations;

    void record_result(const TLanceIndexJobReport& report) {
        std::lock_guard<std::mutex> lock(mu);
        results.push_back(report);
    }
    void record_termination(const TLanceIndexJobTerminationReport& report) {
        std::lock_guard<std::mutex> lock(mu);
        terminations.push_back(report);
    }
    size_t result_count() {
        std::lock_guard<std::mutex> lock(mu);
        return results.size();
    }
    size_t termination_count() {
        std::lock_guard<std::mutex> lock(mu);
        return terminations.size();
    }
    TLanceIndexJobReport first_result() {
        std::lock_guard<std::mutex> lock(mu);
        return results.front();
    }
    TLanceIndexJobTerminationReport first_termination() {
        std::lock_guard<std::mutex> lock(mu);
        return terminations.front();
    }
    bool wait_results(size_t n, int64_t deadline_ms = kCallbackDeadlineMs) {
        return wait_until([&] { return result_count() >= n; }, deadline_ms);
    }
    bool wait_terminations(size_t n, int64_t deadline_ms = kCallbackDeadlineMs) {
        return wait_until([&] { return termination_count() >= n; }, deadline_ms);
    }
};

struct SavedLanceConfig {
    std::string cgroup_parent;
    bool preflight;
    int64_t memory_limit;
    int64_t pids_max;
    int64_t wallclock;
    int64_t term_grace;
    int64_t report_margin;
    int64_t as_limit;
    int32_t cpu_multiplier;
    int32_t max_inflight;
    int32_t queue_size;
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
                  config::lance_index_worker_report_margin_seconds,
                  config::lance_index_worker_as_limit_bytes,
                  config::lance_index_worker_cpu_limit_multiplier,
                  config::lance_index_worker_max_inflight,
                  config::lance_index_worker_queue_size};
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
        config::lance_index_worker_report_margin_seconds = saved_.report_margin;
        config::lance_index_worker_as_limit_bytes = saved_.as_limit;
        config::lance_index_worker_cpu_limit_multiplier = saved_.cpu_multiplier;
        config::lance_index_worker_max_inflight = saved_.max_inflight;
        config::lance_index_worker_queue_size = saved_.queue_size;
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
        if (IndexJobSupervisor::resolve_cgroup_parent("", &resolved).ok()) {
            return resolved;
        }
        return "";
    }

    TLanceIndexJobDispatch make_dispatch(const std::string& tag, int64_t deadline_ms,
                                         int64_t job_id) {
        TLanceIndexJobDispatch dispatch;
        dispatch.job_id = job_id;
        dispatch.dispatch_revision = 3;
        dispatch.invocation_id = tag + "-" + std::to_string(getpid()) + "-" +
                                 std::to_string(s_invocation_counter.fetch_add(1));
        dispatch.be_process_epoch = 777;
        dispatch.deadline_ms = deadline_ms;
        dispatch.mutation_type = TLanceIndexMutationType::CREATE;
        dispatch.index_name = "idx_v";
        dispatch.column_name = "v";
        dispatch.index_type = "IVF_PQ";
        dispatch.dataset_uri = "s3://bucket/dataset";
        dispatch.admitted_dataset_version = 9;
        dispatch.schema_contract_json = "{}";
        return dispatch;
    }

    // The persona argv the fake worker consumes; the invocation identity is
    // handed over via argv so the fake never has to decode the dispatch frame.
    static std::vector<std::string> persona_args(const std::string& persona,
                                                 const TLanceIndexJobDispatch& dispatch,
                                                 const std::vector<std::string>& extra = {}) {
        std::vector<std::string> args = {persona,
                                         "job=" + std::to_string(dispatch.job_id),
                                         "rev=" + std::to_string(dispatch.dispatch_revision),
                                         "inv=" + dispatch.invocation_id,
                                         "epoch=" + std::to_string(dispatch.be_process_epoch)};
        args.insert(args.end(), extra.begin(), extra.end());
        return args;
    }

    // The supervisor-side invocation cgroup dir (same formula as _execute()).
    static std::string invocation_cgroup_dir(const std::string& parent,
                                             const TLanceIndexJobDispatch& dispatch) {
        const uint32_t hash =
                static_cast<uint32_t>(std::hash<std::string> {}(dispatch.invocation_id));
        char name[128];
        std::snprintf(name, sizeof(name), "lance-worker-%lld-%08x",
                      static_cast<long long>(dispatch.job_id), hash);
        return parent + "/" + name;
    }

    void wire_callbacks(IndexJobSupervisor* supervisor, ReportRecorder* recorder) {
        supervisor->set_report_result_callback(
                [recorder](const TLanceIndexJobReport& r) { recorder->record_result(r); });
        supervisor->set_report_termination_callback(
                [recorder](const TLanceIndexJobTerminationReport& r) {
                    recorder->record_termination(r);
                });
    }

    // Real preflight against the delegated parent, then submit; registers the
    // invocation cgroup dir for assertions and the TearDown sweep.
    std::string preflight_and_submit(IndexJobSupervisor* supervisor, const std::string& parent,
                                     const TLanceIndexJobDispatch& dispatch) {
        config::lance_index_worker_cgroup_parent = parent;
        Status status = supervisor->preflight();
        EXPECT_TRUE(status.ok()) << status.to_string();
        EXPECT_TRUE(supervisor->isolation_verified());
        const std::string dir = invocation_cgroup_dir(parent, dispatch);
        created_cgroup_dirs_.push_back(dir);
        status = supervisor->submit(dispatch);
        EXPECT_TRUE(status.ok()) << status.to_string();
        return dir;
    }
};

std::atomic<int> LanceIndexSupervisorTest::s_invocation_counter {0};

// ---------------------------------------------------------------------------
// Happy path: handshake + identity-matched result frame, CHILD_REAPED proof.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, HappyResultWithReapProof) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("happy", epoch_millis() + 3600 * 1000, 9101);
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("happy", dispatch));
    const std::string cgroup_dir = preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_results(1));
    const TLanceIndexJobReport report = recorder.first_result();
    EXPECT_EQ(report.job_id, dispatch.job_id);
    EXPECT_EQ(report.dispatch_revision, dispatch.dispatch_revision);
    EXPECT_EQ(report.invocation_id, dispatch.invocation_id);
    EXPECT_EQ(report.be_process_epoch, dispatch.be_process_epoch);
    EXPECT_EQ(report.result_code, TLanceIndexJobResultCode::NATIVE_OK);
    // The child provably exited and the invocation cgroup drained.
    ASSERT_TRUE(report.__isset.termination_proof);
    EXPECT_EQ(report.termination_proof, TLanceIndexTerminationProof::CHILD_REAPED);
    // NATIVE_OK carries no message category.
    EXPECT_FALSE(report.__isset.sanitized_message);
    EXPECT_EQ(recorder.termination_count(), 0U);
    EXPECT_TRUE(wait_until([&] { return supervisor.inflight_count_for_test() == 0; }, 5000));
    // The invocation cgroup was removed after populated=0.
    EXPECT_EQ(::access(cgroup_dir.c_str(), F_OK), -1)
            << "invocation cgroup should be gone: " << cgroup_dir;
    supervisor.stop();
}

// D12: the supervisor rebuilds the message from a static category; worker text
// is never forwarded.
TEST_F(LanceIndexSupervisorTest, WorkerMessageNeverForwarded) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("smuggle", epoch_millis() + 3600 * 1000, 9102);
    supervisor.force_worker_exec_for_test(
            fake_worker_path(),
            persona_args("happy", dispatch,
                         {"code=" + std::to_string(TLanceIndexJobResultCode::NATIVE_IO),
                          "msg=pwned-ss-credential-leak"}));
    preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_results(1));
    const TLanceIndexJobReport report = recorder.first_result();
    EXPECT_EQ(report.result_code, TLanceIndexJobResultCode::NATIVE_IO);
    ASSERT_TRUE(report.__isset.sanitized_message);
    EXPECT_EQ(report.sanitized_message,
              IndexJobSupervisor::sanitize_message("lance native error", dispatch));
    EXPECT_EQ(report.sanitized_message.find("pwned"), std::string::npos);
    EXPECT_EQ(recorder.termination_count(), 0U);
    supervisor.stop();
}

// ---------------------------------------------------------------------------
// Pre-FFI protocol violations: malformed/undecodable handshakes and
// self-report mismatches all end as RESOURCE_REJECTED + CHILD_REAPED (never
// NEVER_LAUNCHED — the child did exec), and never a termination report.
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
    int64_t job_id = 9200;
    for (const PersonaCase& c : cases) {
        IndexJobSupervisor supervisor;
        ReportRecorder recorder;
        wire_callbacks(&supervisor, &recorder);
        const auto dispatch = make_dispatch(c.persona, epoch_millis() + 3600 * 1000, job_id++);
        supervisor.force_worker_exec_for_test(fake_worker_path(),
                                              persona_args(c.persona, dispatch));
        preflight_and_submit(&supervisor, parent, dispatch);

        ASSERT_TRUE(recorder.wait_results(1)) << "persona " << c.persona;
        const TLanceIndexJobReport report = recorder.first_result();
        EXPECT_EQ(report.result_code, TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED)
                << "persona " << c.persona;
        ASSERT_TRUE(report.__isset.termination_proof) << "persona " << c.persona;
        EXPECT_EQ(report.termination_proof, TLanceIndexTerminationProof::CHILD_REAPED)
                << "persona " << c.persona << ": the child DID exec; NEVER_LAUNCHED is wrong";
        ASSERT_TRUE(report.__isset.sanitized_message) << "persona " << c.persona;
        EXPECT_EQ(report.sanitized_message,
                  IndexJobSupervisor::sanitize_message(c.category, dispatch))
                << "persona " << c.persona;
        EXPECT_EQ(recorder.termination_count(), 0U) << "persona " << c.persona;
        supervisor.stop();
    }
}

// ---------------------------------------------------------------------------
// Result-code wire domain (review M8): a complete, identity-matched frame whose
// result_code is outside the known wire domain (TLanceIndexJobResultCode 1..12)
// is a protocol violation — never a trusted result, and never classified as a
// pre-FFI rejection (the frame arrived past a valid handshake, so a mutation may
// have committed). The supervisor drops it and converges via the termination
// proof on the evidence.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, ResultCodeOutsideWireDomainDropped) {
    REQUIRE_DELEGATED_PARENT(parent);
    int64_t job_id = 9450;
    for (const char* bad_code : {"99", "0", "-3"}) {
        IndexJobSupervisor supervisor;
        ReportRecorder recorder;
        wire_callbacks(&supervisor, &recorder);
        const auto dispatch =
                make_dispatch("badcode", epoch_millis() + 3600 * 1000, job_id++);
        supervisor.force_worker_exec_for_test(
                fake_worker_path(),
                persona_args("happy", dispatch, {"code=" + std::string(bad_code)}));
        preflight_and_submit(&supervisor, parent, dispatch);

        ASSERT_TRUE(recorder.wait_terminations(1)) << "code " << bad_code;
        EXPECT_EQ(recorder.first_termination().proof, TLanceIndexTerminationProof::CHILD_REAPED)
                << "code " << bad_code;
        EXPECT_EQ(recorder.result_count(), 0U)
                << "an out-of-domain result code was forwarded as a trusted result: code "
                << bad_code;
        supervisor.stop();
    }
}

// ---------------------------------------------------------------------------
// Out-of-order worker (review M4): the handshake lands while a >64KiB dispatch
// frame is still draining into stdin. Closing stdin must not regress the phase
// back to READ_HANDSHAKE — the result frame keeps the result-side parsing and
// bounds, and the identity-matched result is accepted.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, OutOfOrderHandshakeKeepsResultPhase) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    auto dispatch = make_dispatch("earlyhs", epoch_millis() + 3600 * 1000, 9460);
    // The dispatch frame must exceed the 64 KiB pipe buffer so the supervisor's
    // stdin write is still in flight when the handshake arrives.
    dispatch.dataset_uri = "s3://bucket/" + std::string(150 * 1024, 'x');
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("early_handshake", dispatch));
    preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_results(1));
    const TLanceIndexJobReport report = recorder.first_result();
    EXPECT_EQ(report.result_code, TLanceIndexJobResultCode::NATIVE_OK)
            << "the result frame was misparsed after the stdin-close phase regression";
    ASSERT_TRUE(report.__isset.termination_proof);
    EXPECT_EQ(report.termination_proof, TLanceIndexTerminationProof::CHILD_REAPED);
    EXPECT_EQ(recorder.termination_count(), 0U);
    supervisor.stop();
}

// ---------------------------------------------------------------------------
// Silent deaths (no complete frame): never a trusted result; a termination
// proof only, with CHILD_REAPED when the dual evidence (exact reap +
// populated=0) holds.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, SilentExitYieldsTerminationProofOnly) {
    REQUIRE_DELEGATED_PARENT(parent);
    int64_t job_id = 9300;
    for (const char* persona : {"exit_no_frame", "exit_partial"}) {
        IndexJobSupervisor supervisor;
        ReportRecorder recorder;
        wire_callbacks(&supervisor, &recorder);
        const auto dispatch = make_dispatch(persona, epoch_millis() + 3600 * 1000, job_id++);
        supervisor.force_worker_exec_for_test(fake_worker_path(),
                                              persona_args(persona, dispatch));
        preflight_and_submit(&supervisor, parent, dispatch);

        ASSERT_TRUE(recorder.wait_terminations(1)) << "persona " << persona;
        const TLanceIndexJobTerminationReport termination = recorder.first_termination();
        EXPECT_EQ(termination.invocation_id, dispatch.invocation_id);
        EXPECT_EQ(termination.proof, TLanceIndexTerminationProof::CHILD_REAPED)
                << "persona " << persona;
        EXPECT_EQ(recorder.result_count(), 0U) << "persona " << persona;
        supervisor.stop();
    }
}

// Identity mismatch after acceptance: the frame is dropped as untrusted; only
// the termination proof rides out.
TEST_F(LanceIndexSupervisorTest, IdentityMismatchDropped) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("mismatch", epoch_millis() + 3600 * 1000, 9401);
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("identity_mismatch", dispatch));
    preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_terminations(1));
    EXPECT_EQ(recorder.first_termination().proof, TLanceIndexTerminationProof::CHILD_REAPED);
    EXPECT_EQ(recorder.result_count(), 0U);
    supervisor.stop();
}

// The fake floods stdout+stderr (1 MiB each) without ever reading stdin: the
// supervisor must drain both streams in parallel (no pipe deadlock), bound the
// stderr capture, and kill/reap on the garbage prefix.
TEST_F(LanceIndexSupervisorTest, FloodPipesDrainedNoDeadlock) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("flood", epoch_millis() + 3600 * 1000, 9501);
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("flood", dispatch));
    const int64_t started = steady_millis();
    preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_results(1, 20000)) << "the supervisor deadlocked on flooded pipes";
    const TLanceIndexJobReport report = recorder.first_result();
    EXPECT_EQ(report.result_code, TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED);
    ASSERT_TRUE(report.__isset.termination_proof);
    EXPECT_EQ(report.termination_proof, TLanceIndexTerminationProof::CHILD_REAPED);
    EXPECT_LT(steady_millis() - started, 20000);
    supervisor.stop();
}

// ---------------------------------------------------------------------------
// Wall-clock deadline: TERM -> grace -> KILL escalation under forced budgets.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, WallClockDeadlineTermThenKill) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    supervisor.force_budgets_for_test(/*wallclock_seconds=*/6, /*term_grace_seconds=*/2,
                                      /*report_margin_seconds=*/0);
    const auto dispatch = make_dispatch("hang", epoch_millis() + 3600 * 1000, 9601);
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("hang", dispatch));
    const int64_t started = steady_millis();
    preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_terminations(1));
    const int64_t elapsed = steady_millis() - started;
    EXPECT_EQ(recorder.first_termination().proof, TLanceIndexTerminationProof::CHILD_REAPED);
    EXPECT_EQ(recorder.result_count(), 0U);
    // The TERM lands only at the 6s wall deadline; the hanging persona dies on
    // TERM (default disposition), well inside the 2s grace.
    EXPECT_GE(elapsed, 5500) << "the wall-clock deadline fired early";
    EXPECT_LT(elapsed, 25000);
    supervisor.stop();
}

TEST_F(LanceIndexSupervisorTest, TermIgnoringWorkerKilledAfterGrace) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    supervisor.force_budgets_for_test(/*wallclock_seconds=*/6, /*term_grace_seconds=*/2,
                                      /*report_margin_seconds=*/0);
    const auto dispatch = make_dispatch("termign", epoch_millis() + 3600 * 1000, 9602);
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("term_ignore", dispatch));
    const int64_t started = steady_millis();
    preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_terminations(1));
    const int64_t elapsed = steady_millis() - started;
    EXPECT_EQ(recorder.first_termination().proof, TLanceIndexTerminationProof::CHILD_REAPED);
    // SIGTERM is ignored; the SIGKILL lands only after the 6s wall + 2s grace.
    EXPECT_GE(elapsed, 7000) << "SIGKILL fired before the TERM grace elapsed";
    EXPECT_LT(elapsed, 30000);
    supervisor.stop();
}

// ---------------------------------------------------------------------------
// Fork barrier: a cgroup migration failure must provably kill the child before
// exec (no exec side effects) and produce the full NEVER_LAUNCHED envelope.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, ForkBarrierKillsBeforeExec) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("barrier", epoch_millis() + 3600 * 1000, 9701);
    const std::string mark = test_dir_ + "/child-execed.marker";
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("mark_hang", dispatch, {"mark=" + mark}));
    supervisor.force_cgroup_migration_failure_for_test();
    const int64_t started = steady_millis();
    preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_results(1));
    // The launch-failure kill+reap path is bounded by the child's actual death,
    // never by a fixed timeout spin (review M1: a kill(pid,0)-only loop would
    // burn the whole 5s reap bound on a zombie).
    EXPECT_LT(steady_millis() - started, 4000)
            << "the launch-failure reap regressed to a full-timeout spin";
    const TLanceIndexJobReport report = recorder.first_result();
    EXPECT_EQ(report.result_code, TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED);
    ASSERT_TRUE(report.__isset.termination_proof);
    EXPECT_EQ(report.termination_proof, TLanceIndexTerminationProof::NEVER_LAUNCHED);
    ASSERT_TRUE(report.__isset.sanitized_message);
    EXPECT_EQ(report.sanitized_message,
              IndexJobSupervisor::sanitize_message("cgroup migration failed", dispatch));
    // The fork barrier held: the child was killed before exec, so its very
    // first side effect (the marker file) provably never happened.
    EXPECT_EQ(::access(mark.c_str(), F_OK), -1)
            << "the fake worker exec'd despite the migration failure";
    EXPECT_EQ(recorder.termination_count(), 0U);
    supervisor.stop();
}

// ---------------------------------------------------------------------------
// D3 budget: a queued invocation whose remaining deadline budget is below the
// executable minimum is rejected before fork with the full async envelope.
// No delegation needed: the budget rail sits before any cgroup work.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, DeadlineBudgetExhaustedNeverLaunched) {
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    // Verify the budget rail directly without touching any cgroup.
    supervisor._isolation_verified.store(true);
    supervisor.force_budgets_for_test(/*wallclock_seconds=*/30, /*term_grace_seconds=*/0,
                                      /*report_margin_seconds=*/0);
    // available = remaining(3s) - margin(0) - grace(0) = 3s <= 5s minimum.
    const auto dispatch = make_dispatch("budget", epoch_millis() + 3000, 9801);
    const std::string mark = test_dir_ + "/budget-execed.marker";
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("mark_hang", dispatch, {"mark=" + mark}));
    ASSERT_TRUE(supervisor.submit(dispatch).ok());

    ASSERT_TRUE(recorder.wait_results(1));
    const TLanceIndexJobReport report = recorder.first_result();
    EXPECT_EQ(report.result_code, TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED);
    ASSERT_TRUE(report.__isset.termination_proof);
    EXPECT_EQ(report.termination_proof, TLanceIndexTerminationProof::NEVER_LAUNCHED);
    ASSERT_TRUE(report.__isset.sanitized_message);
    EXPECT_EQ(report.sanitized_message,
              IndexJobSupervisor::sanitize_message("deadline budget exhausted", dispatch));
    EXPECT_EQ(::access(mark.c_str(), F_OK), -1) << "a budget-rejected invocation forked";
    supervisor.stop();
}

// ---------------------------------------------------------------------------
// D15 dedup: the second submit of the same invocation_id is AlreadyExist; the
// entry survives completion (retained until deadline + grace).
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, DedupRetainedAfterCompletion) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("dedup", epoch_millis() + 3600 * 1000, 9901);
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("happy", dispatch));
    preflight_and_submit(&supervisor, parent, dispatch);

    Status status = supervisor.submit(dispatch);
    EXPECT_TRUE(status.is<ErrorCode::ALREADY_EXIST>()) << status.to_string();
    ASSERT_TRUE(recorder.wait_results(1));
    // Completion does NOT purge the dedup entry: a late redelivery of the same
    // invocation id must never re-execute external side effects.
    status = supervisor.submit(dispatch);
    EXPECT_TRUE(status.is<ErrorCode::ALREADY_EXIST>()) << status.to_string();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(recorder.result_count(), 1U) << "the redelivery re-executed the invocation";
    supervisor.stop();
}

// D15 dedup retention arithmetic (review M3): a pathological deadline_ms near
// INT64_MAX must saturate the retention timestamp instead of wrapping it into
// the past (a wrap would purge the entry immediately and let a late redelivery
// re-execute external side effects). No delegation needed: the dedup rail sits
// in submit() before any cgroup work.
TEST_F(LanceIndexSupervisorTest, DedupRetentionSaturatesAtDeadlineMax) {
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    supervisor._isolation_verified.store(true);
    // Keep the dequeue path hermetic: the invocation cgroup lands in the test
    // dir, the limit write/read-back fails there (no cgroupfs), and the
    // invocation is rejected pre-fork with the NEVER_LAUNCHED envelope.
    supervisor._cgroup_parent_abs = test_dir_;
    const auto dispatch = make_dispatch("dedup-max", INT64_MAX - 1, 9920);
    ASSERT_TRUE(supervisor.submit(dispatch).ok());
    // The saturated entry is retained: the redelivery is a dedup hit, not a new
    // execution.
    const Status status = supervisor.submit(dispatch);
    EXPECT_TRUE(status.is<ErrorCode::ALREADY_EXIST>()) << status.to_string();
    supervisor.stop();
    EXPECT_EQ(recorder.result_count() + recorder.termination_count(), 1U)
            << "the invocation reported exactly once (the redelivery never executed)";
}

// ---------------------------------------------------------------------------
// Bounded queue: with max_inflight=1 and capacity 2, one hanging in-flight
// worker + two queued submissions saturate the supervisor; the fourth submit
// is rejected synchronously.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, QueueFullRejectsFourth) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    // Note for stop(): BlockingQueue::blocking_get DRAINS queued items after
    // shutdown, but the executor drops them without executing (the _stopping
    // drain guard): no forks past the kill snapshot, and each drained dispatch
    // still earns its NEVER_LAUNCHED envelope. Their deadlines are near-past so
    // that even a regression of the drain guard is caught by the budget rail
    // instead of hanging the join.
    supervisor.force_budgets_for_test(/*wallclock_seconds=*/30, /*term_grace_seconds=*/1,
                                      /*report_margin_seconds=*/0);
    const auto hanging = make_dispatch("queue-hang", epoch_millis() + 3600 * 1000, 9951);
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("hang", hanging));
    preflight_and_submit(&supervisor, parent, hanging);
    ASSERT_TRUE(wait_until([&] { return supervisor.inflight_count_for_test() == 1; }, 10000));

    const auto queued1 = make_dispatch("queue-1", epoch_millis() + 1500, 9952);
    const auto queued2 = make_dispatch("queue-2", epoch_millis() + 1500, 9953);
    const auto overflow = make_dispatch("queue-3", epoch_millis() + 3600 * 1000, 9954);
    EXPECT_TRUE(supervisor.submit(queued1).ok());
    EXPECT_TRUE(supervisor.submit(queued2).ok());
    const Status status = supervisor.submit(overflow);
    EXPECT_TRUE(status.is<ErrorCode::TOO_MANY_TASKS>()) << status.to_string();
    EXPECT_EQ(supervisor.queue_depth_for_test(), 2U);
    // stop() best-effort TERM/KILLs the hanging worker (its termination report
    // still rides to the callback — the callback owner drops the RPC while
    // stopping), and the drained queue entries are dropped without launching.
    const int64_t stop_started = steady_millis();
    supervisor.stop();
    EXPECT_LT(steady_millis() - stop_started, 20000) << "stop() hung on queue drain";
}

// ---------------------------------------------------------------------------
// stop() discipline (review M2 + NIT batch): a dispatch still queued when stop()
// begins must NEVER launch a new worker past the kill snapshot. It provably
// never exec'd, so the drain emits the full NEVER_LAUNCHED envelope (whose
// callback delivery releases the invocation's accounting slot service-side);
// the in-flight worker is TERM/KILLed and its termination report is delivered
// rather than silently dropped at the supervisor seam.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, StopDoesNotLaunchQueuedDispatches) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    // Bound the failure mode: if the drain guard regressed and the queued
    // dispatch really launched during stop(), its 30s wall budget still
    // terminates it, so the test fails the 20s stop bound instead of hanging.
    supervisor.force_budgets_for_test(/*wallclock_seconds=*/30, /*term_grace_seconds=*/1,
                                      /*report_margin_seconds=*/0);
    const auto hanging = make_dispatch("stop-hang", epoch_millis() + 3600 * 1000, 9955);
    const std::string hang_mark = test_dir_ + "/stop-hang.pid";
    supervisor.force_worker_exec_for_test(
            fake_worker_path(), persona_args("mark_hang", hanging, {"mark=" + hang_mark}));
    preflight_and_submit(&supervisor, parent, hanging);
    // Wait for the worker to provably exec (its marker carries its pid), so it
    // is registered in the in-flight set before stop() snapshots it.
    ASSERT_TRUE(wait_until([&] { return ::access(hang_mark.c_str(), F_OK) == 0; }, 15000))
            << "the in-flight worker never exec'd";

    const auto queued = make_dispatch("stop-queued", epoch_millis() + 3600 * 1000, 9956);
    const std::string queued_cgroup_dir = invocation_cgroup_dir(parent, queued);
    ASSERT_TRUE(supervisor.submit(queued).ok());
    EXPECT_EQ(supervisor.queue_depth_for_test(), 1U);

    const int64_t stop_started = steady_millis();
    supervisor.stop();
    EXPECT_LT(steady_millis() - stop_started, 20000) << "stop() hung on the queued dispatch";

    // The queued dispatch never launched: its invocation cgroup was never even
    // created, and its terminal report is the NEVER_LAUNCHED envelope with the
    // stopping category.
    EXPECT_EQ(::access(queued_cgroup_dir.c_str(), F_OK), -1)
            << "a queued dispatch launched a worker during stop()";
    ASSERT_TRUE(recorder.wait_results(1));
    const TLanceIndexJobReport report = recorder.first_result();
    EXPECT_EQ(report.invocation_id, queued.invocation_id);
    EXPECT_EQ(report.result_code, TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED);
    ASSERT_TRUE(report.__isset.termination_proof);
    EXPECT_EQ(report.termination_proof, TLanceIndexTerminationProof::NEVER_LAUNCHED);
    ASSERT_TRUE(report.__isset.sanitized_message);
    EXPECT_EQ(report.sanitized_message,
              IndexJobSupervisor::sanitize_message("supervisor is stopping", queued));
    // The in-flight hanging worker was terminated and its report delivered.
    ASSERT_TRUE(recorder.wait_terminations(1));
    EXPECT_EQ(recorder.first_termination().invocation_id, hanging.invocation_id);
    EXPECT_EQ(recorder.first_termination().proof, TLanceIndexTerminationProof::CHILD_REAPED);
}

// ---------------------------------------------------------------------------
// Preflight guards (no delegation needed).
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, PreflightForcedFailureRejectsSubmissions) {
    IndexJobSupervisor supervisor;
    supervisor.force_preflight_failure_for_test();
    const Status status = supervisor.preflight();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::CGROUP_ERROR>()) << status.to_string();
    EXPECT_FALSE(supervisor.isolation_verified());
    const auto dispatch = make_dispatch("preflight-forced", epoch_millis() + 3600 * 1000, 9961);
    const Status submit_status = supervisor.submit(dispatch);
    EXPECT_TRUE(submit_status.is<ErrorCode::CGROUP_ERROR>()) << submit_status.to_string();
}

TEST_F(LanceIndexSupervisorTest, PreflightDisabledStaysUnverified) {
    config::lance_index_isolation_preflight = false;
    IndexJobSupervisor supervisor;
    // The switch only skips the probe; it never verifies isolation.
    EXPECT_TRUE(supervisor.preflight().ok());
    EXPECT_FALSE(supervisor.isolation_verified());
    const auto dispatch = make_dispatch("preflight-off", epoch_millis() + 3600 * 1000, 9962);
    const Status submit_status = supervisor.submit(dispatch);
    EXPECT_TRUE(submit_status.is<ErrorCode::CGROUP_ERROR>()) << submit_status.to_string();
}

TEST_F(LanceIndexSupervisorTest, ResolveCgroupParentRejectsInvalidConfig) {
    std::string resolved;
    EXPECT_FALSE(IndexJobSupervisor::resolve_cgroup_parent("/etc", &resolved).ok());
    EXPECT_FALSE(
            IndexJobSupervisor::resolve_cgroup_parent("/sys/fs/cgroup/../cgroup", &resolved).ok());
    EXPECT_FALSE(IndexJobSupervisor::resolve_cgroup_parent(
                         "/sys/fs/cgroup/lance-ut-definitely-missing-dir", &resolved)
                         .ok());
    // Prefix-boundary shapes: a path that merely SHARES the /sys/fs/cgroup
    // prefix is not under the cgroup root (review N2).
    EXPECT_FALSE(
            IndexJobSupervisor::resolve_cgroup_parent("/sys/fs/cgroup-anything", &resolved).ok());
    EXPECT_FALSE(IndexJobSupervisor::resolve_cgroup_parent("/sys/fs/cgroupx", &resolved).ok());
}

// The supervisor's termination proofs rest on pidfd_open (kernel 5.3) and
// waitid(P_PIDFD) (kernel 5.9); the preflight probes both and fails closed on
// older kernels (review M6). This pins that the UT host itself satisfies the
// minimum — no cgroup delegation needed for the bare primitive.
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
    const Status resolve_status = IndexJobSupervisor::resolve_cgroup_parent("", &resolved);
    IndexJobSupervisor supervisor;
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
        const auto dispatch = make_dispatch("preflight-auto", epoch_millis() + 3600 * 1000, 9963);
        EXPECT_TRUE(supervisor.submit(dispatch).is<ErrorCode::CGROUP_ERROR>());
        LOG(INFO) << "lance preflight auto-resolve correctly classified this environment: "
                  << resolve_status.to_string();
    }
}

// ---------------------------------------------------------------------------
// Preflight probe hygiene (reviews M1/M5): the dummy-child reap is bounded by
// the child's actual death (no full-timeout spin — that regressed every BE
// startup by 5s), and an empty stale probe group left by a SIGKILLed BE of the
// same pid is reclaimed instead of failing the probe as a delegation error.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, PreflightReapIsBoundedByChildDeath) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_cgroup_parent = parent;
    IndexJobSupervisor supervisor;
    const int64_t started = steady_millis();
    const Status status = supervisor.preflight();
    const int64_t elapsed = steady_millis() - started;
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(supervisor.isolation_verified());
    // The pre-F2 dummy reap spun the full 5000ms timeout on the zombie child;
    // the whole probe (mkdir, limit write/read-back, migration, reap, rmdir)
    // completes in well under a second now. 4000ms separates the two cleanly.
    EXPECT_LT(elapsed, 4000) << "preflight regressed to the full-timeout reap spin";
    supervisor.stop();
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
    IndexJobSupervisor supervisor;
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
    supervisor.stop();
}

// ---------------------------------------------------------------------------
// sanitize_message (pure static; D12 invariants).
// ---------------------------------------------------------------------------

TEST_F(LanceIndexSupervisorTest, SanitizeMessageStaticCategoryAndIdentity) {
    const auto dispatch = make_dispatch("sanitize", epoch_millis() + 3600 * 1000, 9971);
    const std::string message =
            IndexJobSupervisor::sanitize_message("stale admission", dispatch);
    EXPECT_EQ(message, "stale admission: job_id=9971 dispatch_revision=3 invocation_id=" +
                               dispatch.invocation_id + " be_process_epoch=777");
}

TEST_F(LanceIndexSupervisorTest, SanitizeMessageEscapesControlChars) {
    auto dispatch = make_dispatch("sanitize-ctl", epoch_millis() + 3600 * 1000, 9972);
    dispatch.invocation_id = std::string("inv\nwith\ttab\x01\x7f");
    const std::string message =
            IndexJobSupervisor::sanitize_message("resource rejected", dispatch);
    EXPECT_EQ(message.find('\n'), std::string::npos);
    EXPECT_EQ(message.find('\t'), std::string::npos);
    EXPECT_NE(message.find("\\x0a"), std::string::npos);
    EXPECT_NE(message.find("\\x01"), std::string::npos);
    EXPECT_NE(message.find("\\x7f"), std::string::npos);
}

TEST_F(LanceIndexSupervisorTest, SanitizeMessageTruncatesUtf8Safely) {
    auto dispatch = make_dispatch("sanitize-utf8", epoch_millis() + 3600 * 1000, 9973);
    // The raw message is "<category>: job_id=9973 dispatch_revision=3
    // invocation_id=<inv> be_process_epoch=777". Lay out <inv> so the 900-byte
    // cut lands in the middle of a 3-byte UTF-8 character.
    const std::string prefix =
            "resource rejected: job_id=9973 dispatch_revision=3 invocation_id=";
    std::string invocation(prefix.size() < 898 ? 898 - prefix.size() : 0, 'a');
    for (int i = 0; i < 200; ++i) {
        invocation += "\xE4\xB8\xAD"; // U+4E2D
    }
    dispatch.invocation_id = invocation;
    const std::string message =
            IndexJobSupervisor::sanitize_message("resource rejected", dispatch);
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
    auto dispatch = make_dispatch("sanitize-secret", epoch_millis() + 3600 * 1000, 9974);
    dispatch.__set_storage_options({{"password", "s3cr3t-value-987"}});
    // The secret value rides inside an identity field (the exact leak shape the
    // second-line invariant defends against): the WHOLE message is dropped.
    dispatch.invocation_id = "inv-with-s3cr3t-value-987-inside";
    EXPECT_TRUE(IndexJobSupervisor::sanitize_message("resource rejected", dispatch).empty());
}

TEST_F(LanceIndexSupervisorTest, SanitizeMessageKeepsShortValuesProtectedByConstruction) {
    auto dispatch = make_dispatch("sanitize-short", epoch_millis() + 3600 * 1000, 9975);
    // Values shorter than 4 bytes are not substring-checkable; the first-line
    // defense (static categories + numeric identity) is what protects them.
    dispatch.__set_storage_options({{"k", "abc"}});
    dispatch.invocation_id = "inv-abc";
    EXPECT_FALSE(IndexJobSupervisor::sanitize_message("resource rejected", dispatch).empty());
}

TEST_F(LanceIndexSupervisorTest, SanitizeMessageEmptyCategoryMeansOmit) {
    const auto dispatch = make_dispatch("sanitize-empty", epoch_millis() + 3600 * 1000, 9976);
    EXPECT_TRUE(IndexJobSupervisor::sanitize_message("", dispatch).empty());
}

// ---------------------------------------------------------------------------
// Real kernel-enforced boundary cases (the delegated runner executes the
// LanceIndexSupervisorTest.Real* filter).
// ---------------------------------------------------------------------------

// Real OOM: a 32 MiB invocation cgroup + malloc-bomb worker -> kernel oom_kill,
// termination proof, supervisor (and a follow-up invocation) unaffected.
TEST_F(LanceIndexSupervisorTest, RealOomKillProofAndSupervisorSurvival) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_memory_limit_bytes = 32 * 1024 * 1024;
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("real-oom", epoch_millis() + 3600 * 1000, 9981);
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("malloc_bomb", dispatch));
    const std::string cgroup_dir = preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_terminations(1));
    const TLanceIndexJobTerminationReport termination = recorder.first_termination();
    EXPECT_EQ(termination.invocation_id, dispatch.invocation_id);
    // The kernel killed the bomb (SIGKILL via the cgroup OOM killer); the exact
    // child was reaped and the group drained.
    EXPECT_EQ(termination.proof, TLanceIndexTerminationProof::CHILD_REAPED);
    EXPECT_EQ(recorder.result_count(), 0U) << "an OOM-killed worker must not produce a result";
    EXPECT_EQ(::access(cgroup_dir.c_str(), F_OK), -1)
            << "invocation cgroup left behind after populated=0: " << cgroup_dir;

    // The supervisor itself is unaffected: a follow-up happy invocation runs.
    const auto follow_up = make_dispatch("real-oom-next", epoch_millis() + 3600 * 1000, 9982);
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("happy", follow_up));
    created_cgroup_dirs_.push_back(invocation_cgroup_dir(parent, follow_up));
    ASSERT_TRUE(supervisor.submit(follow_up).ok());
    ASSERT_TRUE(recorder.wait_results(1));
    EXPECT_EQ(recorder.first_result().result_code, TLanceIndexJobResultCode::NATIVE_OK);
    supervisor.stop();
}

// Real pids.max: a thread-bomb worker is contained by the 16-pid bound; the
// termination proof rides on the reap + populated=0 evidence.
TEST_F(LanceIndexSupervisorTest, RealPidsMaxContainsThreadBomb) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_worker_pids_max = 16;
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("real-pids", epoch_millis() + 3600 * 1000, 9983);
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("thread_bomb", dispatch));
    preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_terminations(1));
    EXPECT_EQ(recorder.first_termination().proof, TLanceIndexTerminationProof::CHILD_REAPED);
    EXPECT_EQ(recorder.result_count(), 0U);
    supervisor.stop();
}

// panic=abort shape: the worker dies of SIGABRT after its handshake; no
// trusted result, termination proof only.
TEST_F(LanceIndexSupervisorTest, RealAbortTerminationProof) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("real-abort", epoch_millis() + 3600 * 1000, 9984);
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("abort_now", dispatch));
    preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_terminations(1));
    EXPECT_EQ(recorder.first_termination().proof, TLanceIndexTerminationProof::CHILD_REAPED);
    EXPECT_EQ(recorder.result_count(), 0U);
    supervisor.stop();
}

// CDC-reaper race (R3 §1.9): a side thread steals the child's exit status via
// waitpid(-1, WNOHANG) — exactly what the CDC manager's process-wide SIGCHLD
// reaper does. The supervisor must still converge: pidfd termination +
// populated=0 are the surviving evidence, and CHILD_REAPED is legal on them.
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

    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("real-cdc", epoch_millis() + 3600 * 1000, 9985);
    supervisor.force_worker_exec_for_test(fake_worker_path(), persona_args("happy", dispatch));
    preflight_and_submit(&supervisor, parent, dispatch);

    ASSERT_TRUE(recorder.wait_results(1));
    const TLanceIndexJobReport report = recorder.first_result();
    EXPECT_EQ(report.result_code, TLanceIndexJobResultCode::NATIVE_OK);
    // Whoever consumed the exit status, the proof must stand on exact evidence.
    ASSERT_TRUE(report.__isset.termination_proof);
    EXPECT_EQ(report.termination_proof, TLanceIndexTerminationProof::CHILD_REAPED);
    LOG(INFO) << "cdc-race: thief thread reaped " << thief.stolen.load() << " child(ren)";
    supervisor.stop();
}

// Surviving descendant: the worker exits (silent death, no frames) while a
// grandchild holds the invocation cgroup populated. CHILD_REAPED must NOT be
// fabricated without the dual evidence: no termination report at all, and the
// populated cgroup is left in place (its limits still bind the leak).
TEST_F(LanceIndexSupervisorTest, RealSurvivingDescendantNoFabricatedProof) {
    REQUIRE_DELEGATED_PARENT(parent);
    IndexJobSupervisor supervisor;
    ReportRecorder recorder;
    wire_callbacks(&supervisor, &recorder);
    const auto dispatch = make_dispatch("real-orphan", epoch_millis() + 3600 * 1000, 9986);
    supervisor.force_worker_exec_for_test(fake_worker_path(),
                                          persona_args("orphan", dispatch, {"sleep=8"}));
    const std::string cgroup_dir = preflight_and_submit(&supervisor, parent, dispatch);

    // The supervisor finishes its supervision (worker reaped) but the
    // populated flag stays 1 while the grandchild sleeps. Two-phase wait:
    // inflight must first rise (execution started) and then fall (supervision
    // complete) — a bare inflight==0 poll is trivially true before the
    // executor ever dequeues the dispatch.
    ASSERT_TRUE(wait_until([&] { return supervisor.inflight_count_for_test() == 1; }, 15000));
    ASSERT_TRUE(wait_until([&] { return supervisor.inflight_count_for_test() == 0; }, 15000));
    EXPECT_EQ(recorder.result_count(), 0U);
    EXPECT_EQ(recorder.termination_count(), 0U)
            << "CHILD_REAPED was fabricated without populated=0 evidence";
    EXPECT_EQ(::access(cgroup_dir.c_str(), F_OK), 0)
            << "the populated invocation cgroup must be left in place";
    supervisor.stop();
    // Cleanup discipline: once the grandchild exits (8s), the group drains and
    // the test removes what the supervisor intentionally left.
    sweep_cgroup_dir(cgroup_dir, 20000);
    EXPECT_EQ(::access(cgroup_dir.c_str(), F_OK), -1)
            << "leftover invocation cgroup never drained: " << cgroup_dir;
}

// BE loss (plan §8.1 fault row, amendment G11): a SIGKILLed supervisor process
// must take its in-flight worker down through the worker-side PDEATHSIG arm —
// no stop(), no cleanup, exactly like a real crash. The harness forks a child
// that plays the BE role (supervisor + hanging fake worker); the test SIGKILLs
// that child and polls the worker pid (read from the marker the worker writes
// at exec) until kill(pid,0) reports ESRCH. A worker that outlives its
// supervisor is a boundary failure.
TEST_F(LanceIndexSupervisorTest, RealBeLossKillsWorkerViaPdeathsig) {
    REQUIRE_DELEGATED_PARENT(parent);
    const std::string mark = test_dir_ + "/beloss-worker.pid";
    int sync_pipe[2];
    ASSERT_EQ(::pipe(sync_pipe), 0) << strerror(errno);

    const auto dispatch = make_dispatch("beloss", epoch_millis() + 3600 * 1000, 9990);
    created_cgroup_dirs_.push_back(invocation_cgroup_dir(parent, dispatch));

    const pid_t be_child = ::fork();
    ASSERT_NE(be_child, -1) << strerror(errno);
    if (be_child == 0) {
        // The "BE" process. Arm its own parent-death guard first so a crashed
        // test process takes this subtree down too, then run a supervisor with
        // one hanging worker until the SIGKILL lands.
        ::close(sync_pipe[0]);
        ::prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0);
        if (::getppid() == 1) {
            ::_exit(2); // the test process is already gone
        }
        config::lance_index_worker_cgroup_parent = parent;
        IndexJobSupervisor supervisor;
        ReportRecorder sink;
        wire_callbacks(&supervisor, &sink);
        if (!supervisor.preflight().ok()) {
            ::_exit(3);
        }
        supervisor.force_worker_exec_for_test(
                fake_worker_path(), persona_args("mark_hang", dispatch, {"mark=" + mark}));
        if (!supervisor.submit(dispatch).ok()) {
            ::_exit(4);
        }
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
    const bool worker_dead = wait_until(
            [&] { return ::kill(worker_pid, 0) != 0 && errno == ESRCH; }, 15000);
    EXPECT_TRUE(worker_dead) << "worker pid " << worker_pid
                             << " outlived its SIGKILLed supervisor; the PDEATHSIG backstop "
                                "failed";
}

} // namespace doris::lance
