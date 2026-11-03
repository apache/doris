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

#include "lance/index/lance_index_worker_service.h"

#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/LanceIndexWorker_types.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include "agent/heartbeat_server.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "lance/index/lance_index_supervisor.h"
#include "lance/index/lance_index_worker.h"
#include "util/debug_points.h"
#include "util/time.h"

// LanceIndexWorkerServiceTest: the synchronous thrift-facing service behind
// lance_index_mutate, tested in three layers.
//
//   1. Rail and mapping layer (every runner, no delegation): the receive
//      rails (validation, epoch receipt, dedup, the D3 budget boundary, queue
//      admission), the dequeue rail boundary, the duplicate ruling, the stop
//      drain, the dispatch-frame injection (bounds, debug points) and the
//      full phase x code-band result mapping are driven directly - the
//      private seams (_execute_pending, _fill_from_execution, the dedup map,
//      the queue) are reachable because the UT binary builds with
//      -fno-access-control, the same discipline the supervisor tests use.
//   2. Isolation-unverified execution layer (every runner): a started service
//      whose supervisor preflight was skipped answers every admitted
//      invocation with the never-launched isolation rejection - the
//      reserved-slot direct-release shape of the two-state accounting.
//   3. Real execution layer (delegated runner only): full
//      submit -> queue -> executor -> supervisor -> cgroup-isolated fake
//      worker -> respond-after-reap round trips against the same
//      lance_fake_worker personas the supervisor tests use (happy,
//      reject_unresolved, skip_result, hang). Without a writable delegated
//      cgroup v2 parent these cases SKIP loudly (recon4 §B3: run under
//      `systemd-run --user --scope`).
//
// SIGCHLD hygiene (supervisor test precedent): the fixture resets SIGCHLD to
// SIG_DFL and restores it afterwards; no test installs a handler.

namespace doris::lance {
namespace {

constexpr int64_t kTestBeEpoch = 424242;

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

// GTEST_SKIP must live in a void function, hence the macro (supervisor test
// precedent).
#define REQUIRE_DELEGATED_PARENT(parent_var)                                        \
    std::string parent_var = detect_delegated_parent();                             \
    if (parent_var.empty()) {                                                       \
        GTEST_SKIP() << "LANCE-ISOLATION-UT: no writable delegated cgroup v2 "      \
                        "parent found; set LANCE_TEST_CGROUP_PARENT or run under "  \
                        "`systemd-run --user --scope` to execute the Lance "        \
                        "worker service cases for real";                            \
    }

bool status_code_is(const TLanceIndexMutationResult& result, TStatusCode::type code) {
    // `status` is a required field and every service answer path sets it
    // before returning; required fields carry no __isset entries.
    return result.status.status_code == code;
}

} // namespace

class LanceIndexWorkerServiceTest : public ::testing::Test {
protected:
    sighandler_t old_sigchld_ = SIG_DFL;
    std::string test_dir_;
    // Config snapshot (struct + lambda restore keeps the list honest).
    int32_t concurrency_, queued_, bound_partitions_, bound_subvectors_;
    bool preflight_, debug_points_enabled_;
    int64_t term_grace_;
    std::string cgroup_parent_;
    std::vector<std::string> created_cgroup_dirs_;
    static std::atomic<int> s_counter;

    void SetUp() override {
        old_sigchld_ = signal(SIGCHLD, SIG_DFL);
        namespace fs = std::filesystem;
        test_dir_ = fs::temp_directory_path().string() + "/lance_svc_test_" +
                    std::to_string(getpid()) + "_" +
                    std::to_string(s_counter.fetch_add(1));
        fs::create_directories(test_dir_);
        concurrency_ = config::lance_index_mutation_worker_max_concurrency;
        queued_ = config::lance_index_mutation_worker_max_queued;
        bound_partitions_ = config::lance_index_max_num_partitions;
        bound_subvectors_ = config::lance_index_max_num_sub_vectors;
        preflight_ = config::lance_index_isolation_preflight;
        debug_points_enabled_ = config::enable_debug_points;
        term_grace_ = config::lance_index_worker_term_grace_seconds;
        cgroup_parent_ = config::lance_index_worker_cgroup_parent;
        // Deterministic default for the every-runner layers: the probe is
        // skipped, isolation stays unverified and every admitted invocation
        // takes the never-launched isolation rail. Delegated cases override.
        config::lance_index_isolation_preflight = false;
    }

    void TearDown() override {
        config::lance_index_mutation_worker_max_concurrency = concurrency_;
        config::lance_index_mutation_worker_max_queued = queued_;
        config::lance_index_max_num_partitions = bound_partitions_;
        config::lance_index_max_num_sub_vectors = bound_subvectors_;
        config::lance_index_isolation_preflight = preflight_;
        config::enable_debug_points = debug_points_enabled_;
        config::lance_index_worker_term_grace_seconds = term_grace_;
        config::lance_index_worker_cgroup_parent = cgroup_parent_;
        DebugPoints::instance()->remove(WORKER_DEBUG_POINT_HANG);
        DebugPoints::instance()->remove(WORKER_DEBUG_POINT_SKIP_RESULT);
        for (const std::string& dir : created_cgroup_dirs_) {
            sweep_cgroup_dir(dir);
        }
        signal(SIGCHLD, old_sigchld_);
        if (!test_dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(test_dir_, ec);
        }
    }

    static void sweep_cgroup_dir(const std::string& dir) {
        if (::access(dir.c_str(), F_OK) != 0) {
            return;
        }
        for (int i = 0; i < 75 && ::rmdir(dir.c_str()) != 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
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

    // A structurally valid request (all __set_, so __isset is true for every
    // required field exactly like a decoded frame).
    static TLanceIndexMutationRequest make_request(const std::string& tag, int64_t deadline_ms) {
        TLanceIndexMutationRequest request;
        request.__set_invocation_id(tag + "-" + std::to_string(getpid()) + "-" +
                                    std::to_string(s_counter.fetch_add(1)));
        request.__set_be_process_epoch(kTestBeEpoch);
        request.__set_deadline_ms(deadline_ms);
        request.__set_mutation_type(TLanceIndexMutationType::CREATE);
        request.__set_index_name("idx_v");
        request.__set_column_name("v");
        request.__set_index_type("IVF_PQ");
        request.__set_dataset_uri("s3://bucket/dataset");
        request.__set_admitted_dataset_version(9);
        return request;
    }

    // Starts a service with the forced epoch and (by default here) an
    // unverified isolation.
    static void start_service(LanceIndexWorkerService* service) {
        service->force_be_process_epoch_for_test(kTestBeEpoch);
        ASSERT_TRUE(service->start().ok()) << "service start failed";
    }

    // Real preflight + fake worker exec override; registers the invocation
    // cgroup dirs for the TearDown sweep.
    void arm_real_isolation(LanceIndexWorkerService* service, const std::string& parent,
                            const std::string& persona,
                            const std::vector<std::string>& extra_args = {}) {
        config::lance_index_worker_cgroup_parent = parent;
        config::lance_index_isolation_preflight = true;
        ASSERT_TRUE(service->supervisor_for_test().preflight().ok());
        EXPECT_TRUE(service->supervisor_for_test().isolation_verified());
        std::vector<std::string> args {persona};
        args.insert(args.end(), extra_args.begin(), extra_args.end());
        service->supervisor_for_test().force_worker_exec_for_test(fake_worker_path(),
                                                                  std::move(args));
    }

    std::string invocation_cgroup_dir(const std::string& parent,
                                      const TLanceIndexMutationRequest& request) {
        std::string dir = parent + "/" +
                          LanceIndexSupervisor::invocation_cgroup_name(request.invocation_id);
        created_cgroup_dirs_.push_back(dir);
        return dir;
    }
};

std::atomic<int> LanceIndexWorkerServiceTest::s_counter {0};

// ---------------------------------------------------------------------------
// Lifecycle rail.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, NotStartedRefusesWithNeverLaunched) {
    LanceIndexWorkerService service;
    service.force_be_process_epoch_for_test(kTestBeEpoch);
    const TLanceIndexMutationRequest request = make_request("nostart", epoch_millis() + 60000);
    TLanceIndexMutationResult result;
    service.lance_index_mutate(result, request);
    EXPECT_FALSE(status_code_is(result, TStatusCode::OK));
    EXPECT_TRUE(status_code_is(result, TStatusCode::CANCELLED));
    EXPECT_EQ(result.invocation_id, request.invocation_id);
    EXPECT_EQ(result.be_process_epoch, request.be_process_epoch);
    EXPECT_EQ(service.never_launched_total_for_test(), 1);
}

TEST_F(LanceIndexWorkerServiceTest, ReceiveRailValidatesEveryField) {
    LanceIndexWorkerService service;
    start_service(&service);

    // Required-field PRESENCE is enforced by the thrift decode (a frame
    // missing one never reaches the handler); this table drives the value
    // and cardinality checks the receive rail owns.
    struct Case {
        const char* name;
        std::function<void(TLanceIndexMutationRequest&)> mutate;
    };
    const Case cases[] = {
            {"empty-invocation-id",
             [](TLanceIndexMutationRequest& r) { r.__set_invocation_id(""); }},
            {"unknown-mutation-type", [](TLanceIndexMutationRequest& r) {
                 r.__set_mutation_type(static_cast<TLanceIndexMutationType::type>(7));
             }},
            {"zero-deadline", [](TLanceIndexMutationRequest& r) { r.__set_deadline_ms(0); }},
            {"negative-deadline", [](TLanceIndexMutationRequest& r) { r.__set_deadline_ms(-5); }},
            {"zero-admitted-version",
             [](TLanceIndexMutationRequest& r) { r.__set_admitted_dataset_version(0); }},
            {"negative-admitted-version",
             [](TLanceIndexMutationRequest& r) { r.__set_admitted_dataset_version(-1); }},
            {"empty-index-name", [](TLanceIndexMutationRequest& r) { r.__set_index_name(""); }},
            {"empty-dataset-uri", [](TLanceIndexMutationRequest& r) { r.__set_dataset_uri(""); }},
            {"oversized-storage-option-value", [](TLanceIndexMutationRequest& r) {
                 r.__set_storage_options({{"aws.session.token", std::string(4097, 'x')}});
             }},
            {"too-many-storage-options", [](TLanceIndexMutationRequest& r) {
                 std::map<std::string, std::string> options;
                 for (int i = 0; i < 65; ++i) {
                     options["opt" + std::to_string(i)] = "v";
                 }
                 r.__set_storage_options(std::move(options));
             }},
    };
    for (const Case& c : cases) {
        TLanceIndexMutationRequest request = make_request(c.name, epoch_millis() + 60000);
        c.mutate(request);
        TLanceIndexMutationResult result;
        service.lance_index_mutate(result, request);
        EXPECT_FALSE(result.status.status_code == TStatusCode::OK) << c.name;
        EXPECT_TRUE(status_code_is(result, TStatusCode::INVALID_ARGUMENT)) << c.name;
        // The identity echo is present on every answer shape and always the
        // request's own values (the FE echo-match can never fail on us).
        EXPECT_EQ(result.invocation_id, request.invocation_id) << c.name;
        EXPECT_EQ(result.be_process_epoch, request.be_process_epoch) << c.name;
        EXPECT_FALSE(result.__isset.worker_phase) << c.name;
    }
    EXPECT_EQ(service.never_launched_total_for_test(), static_cast<int64_t>(std::size(cases)));
}

// ---------------------------------------------------------------------------
// Epoch receipt check (N2): the request's field 2 must name THIS process
// incarnation (the FE mints it from the heartbeat's be_start_time).
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, EpochReceiptRejectsForeignIncarnation) {
    LanceIndexWorkerService service;
    start_service(&service);

    TLanceIndexMutationRequest stale = make_request("stale-epoch", epoch_millis() + 60000);
    stale.__set_be_process_epoch(kTestBeEpoch - 1);
    TLanceIndexMutationResult result;
    service.lance_index_mutate(result, stale);
    EXPECT_TRUE(status_code_is(result, TStatusCode::INVALID_ARGUMENT));
    EXPECT_EQ(result.be_process_epoch, stale.be_process_epoch);
    EXPECT_EQ(service.never_launched_total_for_test(), 1);

    // A matching epoch passes the epoch rail and reaches the (here
    // unverified) isolation rail - a different status code proves the rail
    // order, not just a blanket rejection.
    TLanceIndexMutationResult passed;
    service.lance_index_mutate(passed, make_request("fresh-epoch", epoch_millis() + 60000));
    EXPECT_TRUE(status_code_is(passed, TStatusCode::INTERNAL_ERROR));
}

TEST_F(LanceIndexWorkerServiceTest, ZeroEpochRejectsUntilHeartbeatExists) {
    if (HeartbeatServer::process_epoch() != 0) {
        GTEST_SKIP() << "a HeartbeatServer exists in this process; the zero-epoch "
                        "branch is not reachable here";
    }
    LanceIndexWorkerService service;
    ASSERT_TRUE(service.start().ok()); // no forced epoch: reads the (zero) process epoch
    TLanceIndexMutationResult result;
    service.lance_index_mutate(result, make_request("no-epoch", epoch_millis() + 60000));
    EXPECT_TRUE(status_code_is(result, TStatusCode::INVALID_ARGUMENT));
}

// ---------------------------------------------------------------------------
// D3 receive rail boundary: remaining <= kKillReservationMs + kMinWorkerRunMs
// (= 25s with the shipped constants) rejects on arrival. The dequeue rail
// below re-checks the SAME constants after the queue wait.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, ReceiveRailBudgetBoundary) {
    LanceIndexWorkerService service;
    start_service(&service);

    // remaining = threshold - 1: refused at receipt (never launched).
    TLanceIndexMutationRequest expired =
            make_request("expired", epoch_millis() + kKillReservationMs + kMinWorkerRunMs - 1);
    TLanceIndexMutationResult result;
    service.lance_index_mutate(result, expired);
    EXPECT_TRUE(status_code_is(result, TStatusCode::CANCELLED));
    EXPECT_EQ(result.invocation_id, expired.invocation_id);
    EXPECT_EQ(service.never_launched_total_for_test(), 1);

    // remaining = threshold + a queue-wait margin: passes the receive rail
    // (and lands on the unverified-isolation rail here - a different status
    // code proves it got past the budget rail, past the queue and past the
    // dequeue rail into an executor).
    TLanceIndexMutationRequest fresh =
            make_request("fresh", epoch_millis() + kKillReservationMs + kMinWorkerRunMs + 10000);
    TLanceIndexMutationResult passed;
    service.lance_index_mutate(passed, fresh);
    EXPECT_TRUE(status_code_is(passed, TStatusCode::INTERNAL_ERROR));
    EXPECT_EQ(service.never_launched_total_for_test(), 2);
}

// The dequeue rail twin, driven directly at its own boundary: the receive
// rail's threshold converts (remaining - kKillReservationMs) into the
// monotonic kill deadline, so "remaining = kKillReservationMs + kMinWorkerRunMs"
// IS "now_mono + kMinWorkerRunMs == kill_deadline_mono" - the two rails are
// one formula in two clocks, and the +/- cases below flip together.
TEST_F(LanceIndexWorkerServiceTest, DequeueRailBudgetBoundaryMatchesReceiveFormula) {
    LanceIndexWorkerService service;
    start_service(&service);

    auto dequeue_answer = [&](int64_t kill_deadline_mono) {
        auto pending = std::make_shared<LanceIndexWorkerService::PendingInvocation>();
        pending->request = make_request("dequeue", epoch_millis() + 60000);
        pending->kill_deadline_ms = epoch_millis() + 60000;
        pending->kill_deadline_mono = kill_deadline_mono;
        auto future = pending->answer.get_future();
        service._execute_pending(pending);
        return future.get();
    };

    // 100ms of runnable budget left: passes the dequeue rail (reaches the
    // isolation rail here). The +/-100ms window straddles the same
    // kMinWorkerRunMs boundary the receive rail's 25s threshold encodes,
    // while staying robust to scheduler jitter between the two clock reads.
    TLanceIndexMutationResult runnable =
            dequeue_answer(MonotonicMillis() + kMinWorkerRunMs + 100);
    EXPECT_TRUE(status_code_is(runnable, TStatusCode::INTERNAL_ERROR));

    // 100ms short of a minimal worker run: never launched at dequeue.
    TLanceIndexMutationResult exhausted =
            dequeue_answer(MonotonicMillis() + kMinWorkerRunMs - 100);
    EXPECT_TRUE(status_code_is(exhausted, TStatusCode::CANCELLED));
    EXPECT_EQ(service.never_launched_total_for_test(), 2);
}

// ---------------------------------------------------------------------------
// Dedup (S7, thrift :414-418 BE half): a redelivered invocation_id never
// re-executes; the duplicate answer is OK + phase 3 (the first delivery's
// answer went to its own connection, so this delivery's outcome is unknown to
// this process copy - never a fabricated confirmed failure).
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, DuplicateDeliveryRefusedWithoutReexecution) {
    LanceIndexWorkerService service;
    start_service(&service);
    const TLanceIndexMutationRequest request = make_request("dup", epoch_millis() + 60000);

    TLanceIndexMutationResult first;
    service.lance_index_mutate(first, request); // isolation rail -> never launched
    EXPECT_FALSE(status_code_is(first, TStatusCode::OK));

    TLanceIndexMutationResult second;
    service.lance_index_mutate(second, request);
    EXPECT_TRUE(status_code_is(second, TStatusCode::OK));
    EXPECT_TRUE(second.__isset.worker_phase);
    EXPECT_EQ(second.worker_phase, TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
    EXPECT_FALSE(second.__isset.lance_error_code) << "phase 3 never carries a code";
    EXPECT_NE(second.message.find("duplicate invocation_id"), std::string::npos);
    EXPECT_EQ(second.invocation_id, request.invocation_id);
    EXPECT_EQ(second.be_process_epoch, request.be_process_epoch);
    // Exactly one execution attempt ever happened (the first delivery).
    EXPECT_EQ(service.launched_total_for_test(), 0);
    EXPECT_EQ(service.never_launched_total_for_test(), 1);
    EXPECT_EQ(service.reserved_slots_for_test(), 0);
}

TEST_F(LanceIndexWorkerServiceTest, DuplicateRetentionWindowRecyclesExpiredIds) {
    LanceIndexWorkerService service;
    start_service(&service);
    const std::string id = "retention-" + std::to_string(getpid());

    // A stale retention entry (deadline + window in the past) is pruned on
    // the next receipt: the id is fresh again and proceeds past the dedup
    // rail (to the isolation rail here, NOT the duplicate answer).
    {
        std::lock_guard<std::mutex> lock(service._dedup_mutex);
        service._dedup_erase_after_ms[id] = epoch_millis() - 1;
    }
    TLanceIndexMutationRequest request = make_request("x", epoch_millis() + 60000);
    request.__set_invocation_id(id);
    TLanceIndexMutationResult result;
    service.lance_index_mutate(result, request);
    EXPECT_TRUE(status_code_is(result, TStatusCode::INTERNAL_ERROR));
    EXPECT_FALSE(result.__isset.worker_phase);

    // A live retention entry answers as a duplicate without executing.
    {
        std::lock_guard<std::mutex> lock(service._dedup_mutex);
        service._dedup_erase_after_ms[id] = epoch_millis() + 60000;
    }
    TLanceIndexMutationResult duplicate;
    service.lance_index_mutate(duplicate, request);
    EXPECT_TRUE(status_code_is(duplicate, TStatusCode::OK));
    EXPECT_EQ(duplicate.worker_phase, TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
}

// A request refused AFTER the dedup admission (budget rail) never executed,
// so its id must not poison a later corrected send of the same id.
TEST_F(LanceIndexWorkerServiceTest, BudgetRejectionDoesNotPoisonInvocationId) {
    LanceIndexWorkerService service;
    start_service(&service);
    const std::string id = "unpoison-" + std::to_string(getpid());

    TLanceIndexMutationRequest expired = make_request("x", epoch_millis() + 1000);
    expired.__set_invocation_id(id);
    TLanceIndexMutationResult first;
    service.lance_index_mutate(first, expired);
    EXPECT_TRUE(status_code_is(first, TStatusCode::CANCELLED));

    TLanceIndexMutationRequest retried = make_request("x", epoch_millis() + 60000);
    retried.__set_invocation_id(id);
    TLanceIndexMutationResult second;
    service.lance_index_mutate(second, retried);
    // Fresh admission: reaches the isolation rail, not the duplicate answer.
    EXPECT_TRUE(status_code_is(second, TStatusCode::INTERNAL_ERROR));
}

// The dedup guard is bounded (N3): at the entry cap a FRESH id is refused as
// backpressure - never launched on the wire (non-OK), never executed, and
// NOT retained (nothing executed, so a corrected retry stays free) - while an
// already-retained id still gets its duplicate answer. The queue-full rail's
// twin, not a duplicate ruling.
TEST_F(LanceIndexWorkerServiceTest, DedupSaturationRefusesFreshIdsAsBackpressure) {
    LanceIndexWorkerService service;
    start_service(&service);
    service.force_dedup_max_entries_for_test(2);

    // Seed two live retention entries directly (the dedup map is the private
    // seam the retention test above already drives).
    const std::string retained_a = "sat-a-" + std::to_string(getpid());
    const std::string retained_b = "sat-b-" + std::to_string(getpid());
    {
        std::lock_guard<std::mutex> lock(service._dedup_mutex);
        service._dedup_erase_after_ms[retained_a] = epoch_millis() + 60000;
        service._dedup_erase_after_ms[retained_b] = epoch_millis() + 60000;
    }

    // A fresh id at the cap: refused before enqueue, never launched.
    const TLanceIndexMutationRequest saturated_req =
            make_request("sat", epoch_millis() + 60000);
    TLanceIndexMutationResult saturated;
    service.lance_index_mutate(saturated, saturated_req);
    EXPECT_TRUE(status_code_is(saturated, TStatusCode::TOO_MANY_TASKS));
    EXPECT_FALSE(saturated.__isset.worker_phase) << "backpressure is never launched: no phase";
    EXPECT_EQ(saturated.invocation_id, saturated_req.invocation_id);
    EXPECT_EQ(saturated.be_process_epoch, saturated_req.be_process_epoch);
    EXPECT_EQ(service.never_launched_total_for_test(), 1);

    // The refusal retained nothing: the same fresh id is refused the same way
    // again (still backpressure, not the duplicate answer).
    TLanceIndexMutationResult saturated_again;
    service.lance_index_mutate(saturated_again, saturated_req);
    EXPECT_TRUE(status_code_is(saturated_again, TStatusCode::TOO_MANY_TASKS));
    EXPECT_FALSE(saturated_again.__isset.worker_phase);
    EXPECT_EQ(service.never_launched_total_for_test(), 2);

    // A retained id is unaffected by the cap: the duplicate answer still is
    // OK + phase 3, no code, no execution.
    TLanceIndexMutationRequest duplicate_req = make_request("x", epoch_millis() + 60000);
    duplicate_req.__set_invocation_id(retained_a);
    TLanceIndexMutationResult duplicate;
    service.lance_index_mutate(duplicate, duplicate_req);
    EXPECT_TRUE(status_code_is(duplicate, TStatusCode::OK));
    EXPECT_EQ(duplicate.worker_phase, TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
    EXPECT_FALSE(duplicate.__isset.lance_error_code);
    EXPECT_EQ(service.launched_total_for_test(), 0);
}

// The executor exception guard (N5): an exception escaping the execution path
// (a bad_alloc in the dispatch build is the realistic shape; the fault point
// stands in for it deterministically) is answered - OK + phase 3, no code -
// and the executor thread survives to serve the next invocation. Before the
// guard the escape crossed the thread entry and std::terminate'd the whole
// BE process.
TEST_F(LanceIndexWorkerServiceTest, ExecutorExceptionIsAnsweredAndSurvived) {
    LanceIndexWorkerService service;
    start_service(&service);
    constexpr const char* kThrowPoint = "LanceIndexWorkerService.executor_throw";
    const bool saved_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add(kThrowPoint);

    const TLanceIndexMutationRequest request = make_request("throw", epoch_millis() + 60000);
    TLanceIndexMutationResult thrown;
    service.lance_index_mutate(thrown, request);
    EXPECT_TRUE(status_code_is(thrown, TStatusCode::OK));
    EXPECT_TRUE(thrown.__isset.worker_phase);
    EXPECT_EQ(thrown.worker_phase, TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
    EXPECT_FALSE(thrown.__isset.lance_error_code) << "the guard never invents a code";
    EXPECT_NE(thrown.message.find("executor failed"), std::string::npos);
    EXPECT_EQ(thrown.invocation_id, request.invocation_id);
    EXPECT_EQ(thrown.be_process_epoch, request.be_process_epoch);
    EXPECT_EQ(service.indeterminate_total_for_test(), 1);

    // The executor survived: with the point gone the next invocation runs the
    // normal path through the SAME thread pool - here the isolation rail's
    // never-launched answer (preflight was skipped by start_service).
    DebugPoints::instance()->remove(kThrowPoint);
    config::enable_debug_points = saved_debug_points;
    TLanceIndexMutationResult recovered;
    service.lance_index_mutate(recovered, make_request("recovered", epoch_millis() + 60000));
    EXPECT_TRUE(status_code_is(recovered, TStatusCode::INTERNAL_ERROR));
    EXPECT_FALSE(recovered.__isset.worker_phase);
    EXPECT_EQ(service.never_launched_total_for_test(), 1);
    EXPECT_EQ(service.indeterminate_total_for_test(), 1);
    EXPECT_EQ(service.reserved_slots_for_test(), 0);
    EXPECT_EQ(service.inflight_workers(), 0);
}

// ---------------------------------------------------------------------------
// Two-state slots (D4): reserved (direct release on pre-launch failure) vs
// launched (released only after the domain-termination confirmation).
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, ReservedSlotDirectReleaseOnUnverifiedIsolation) {
    LanceIndexWorkerService service;
    start_service(&service); // preflight skipped: isolation unverified

    TLanceIndexMutationResult result;
    service.lance_index_mutate(result, make_request("unverified", epoch_millis() + 60000));
    EXPECT_TRUE(status_code_is(result, TStatusCode::INTERNAL_ERROR));
    // The reserved slot was released directly: nothing is reserved, nothing
    // launched, nothing in flight, and the never-launched bucket counted it.
    EXPECT_EQ(service.reserved_slots_for_test(), 0);
    EXPECT_EQ(service.launched_total_for_test(), 0);
    EXPECT_EQ(service.inflight_workers(), 0);
    EXPECT_EQ(service.never_launched_total_for_test(), 1);
}

TEST_F(LanceIndexWorkerServiceTest, LaunchedSlotReleasedAfterDomainTermination) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_mutation_worker_max_concurrency = 1;
    LanceIndexWorkerService service;
    service.force_be_process_epoch_for_test(kTestBeEpoch);
    ASSERT_TRUE(service.start().ok());
    const TLanceIndexMutationRequest request = make_request("launched", epoch_millis() + 60000);
    arm_real_isolation(&service, parent, "happy");
    const std::string cgroup = invocation_cgroup_dir(parent, request);

    TLanceIndexMutationResult result;
    service.lance_index_mutate(result, request);
    // The trusted native success.
    EXPECT_TRUE(status_code_is(result, TStatusCode::OK));
    EXPECT_TRUE(result.__isset.worker_phase);
    EXPECT_EQ(result.worker_phase, TLanceIndexWorkerPhase::NATIVE_RETURN);
    ASSERT_TRUE(result.__isset.lance_error_code);
    EXPECT_EQ(result.lance_error_code, 0);
    // Respond-after-reap evidence: mutate() has RETURNED, so the invocation
    // cgroup must already be gone - the reply left only after the execution
    // domain's termination was confirmed, and the slot is released.
    EXPECT_EQ(::access(cgroup.c_str(), F_OK), -1)
            << "the reply returned before the execution domain terminated";
    EXPECT_EQ(service.reserved_slots_for_test(), 0);
    EXPECT_EQ(service.inflight_workers(), 0);
    EXPECT_EQ(service.launched_total_for_test(), 1);
    EXPECT_EQ(service.queue_depth(), 0);
}

// ---------------------------------------------------------------------------
// Bounded queue: full = never-launched refusal at receipt.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, FullQueueRefusesAtReceipt) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_mutation_worker_max_concurrency = 1;
    config::lance_index_mutation_worker_max_queued = 1;
    config::lance_index_worker_term_grace_seconds = 2;
    LanceIndexWorkerService service;
    service.force_be_process_epoch_for_test(kTestBeEpoch);
    ASSERT_TRUE(service.start().ok());
    arm_real_isolation(&service, parent, "hang");

    // A hangs inside its (isolated) worker until its kill deadline: with a
    // request deadline of now+26s the kill deadline is now+4s, so the single
    // executor slot is provably occupied while the queue holds one entry.
    const TLanceIndexMutationRequest hanging = make_request("q-full-a", epoch_millis() + 26000);
    const TLanceIndexMutationRequest queued = make_request("q-full-b", epoch_millis() + 26000);
    invocation_cgroup_dir(parent, hanging);
    invocation_cgroup_dir(parent, queued);
    auto hanging_answer = std::async(std::launch::async, [&] {
        TLanceIndexMutationResult result;
        service.lance_index_mutate(result, hanging);
        return result;
    });
    ASSERT_TRUE(wait_until([&] { return service.inflight_workers() == 1; }, 15000))
            << "the hanging invocation never occupied the executor slot";

    auto queued_answer = std::async(std::launch::async, [&] {
        TLanceIndexMutationResult result;
        service.lance_index_mutate(result, queued);
        return result;
    });
    ASSERT_TRUE(wait_until([&] { return service.queue_depth() == 1; }, 15000))
            << "the second invocation never reached the queue";

    // Third delivery: the queue (capacity 1) is full -> never launched at
    // receipt, answered immediately while the first two are still in flight.
    TLanceIndexMutationResult refused;
    service.lance_index_mutate(refused, make_request("q-full-c", epoch_millis() + 26000));
    EXPECT_TRUE(status_code_is(refused, TStatusCode::TOO_MANY_TASKS));
    EXPECT_TRUE(refused.__isset.invocation_id);
    EXPECT_FALSE(refused.__isset.worker_phase);

    const TLanceIndexMutationResult a = hanging_answer.get();
    const TLanceIndexMutationResult b = queued_answer.get();
    // Both admitted invocations converged. The hanging one was killed by its
    // ladder (OK + phase 3). The queued one spent its whole budget waiting
    // behind the hanging invocation, so the dequeue rail refused it before
    // launch - the double-reject pair doing exactly its job through the real
    // queue path (never launched, cancelled, no phase).
    EXPECT_TRUE(status_code_is(a, TStatusCode::OK));
    EXPECT_EQ(a.worker_phase, TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
    EXPECT_TRUE(status_code_is(b, TStatusCode::CANCELLED));
    EXPECT_FALSE(b.__isset.worker_phase);
    EXPECT_EQ(service.never_launched_total_for_test(), 2);
    EXPECT_EQ(service.queue_depth(), 0);
    EXPECT_EQ(service.inflight_workers(), 0);
}

// ---------------------------------------------------------------------------
// Stop semantics (N4): drain the queue with never-launched rejections, join
// the executors, never launch a new worker afterwards.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, StopRefusesNewSubmissions) {
    LanceIndexWorkerService service;
    start_service(&service);
    service.stop(); // joins the (idle) executors and returns

    TLanceIndexMutationResult result;
    service.lance_index_mutate(result, make_request("post-stop", epoch_millis() + 60000));
    EXPECT_TRUE(status_code_is(result, TStatusCode::CANCELLED));
    EXPECT_EQ(service.never_launched_total_for_test(), 1);
    service.stop(); // idempotent
}

TEST_F(LanceIndexWorkerServiceTest, StopDrainsQueuedEntriesAndNeverLaunches) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_mutation_worker_max_concurrency = 1;
    config::lance_index_mutation_worker_max_queued = 4;
    config::lance_index_worker_term_grace_seconds = 2;
    LanceIndexWorkerService service;
    service.force_be_process_epoch_for_test(kTestBeEpoch);
    ASSERT_TRUE(service.start().ok());
    arm_real_isolation(&service, parent, "hang");

    // One in-flight hanging invocation (long budget, so only stop() can end
    // it) plus one queued entry that must be drain-rejected by the stop.
    const TLanceIndexMutationRequest hanging = make_request("stop-hang", epoch_millis() + 120000);
    const TLanceIndexMutationRequest queued = make_request("stop-queued",
                                                           epoch_millis() + 120000);
    invocation_cgroup_dir(parent, hanging);
    invocation_cgroup_dir(parent, queued);
    auto hanging_answer = std::async(std::launch::async, [&] {
        TLanceIndexMutationResult result;
        service.lance_index_mutate(result, hanging);
        return result;
    });
    ASSERT_TRUE(wait_until([&] { return service.inflight_workers() == 1; }, 15000));
    auto queued_answer = std::async(std::launch::async, [&] {
        TLanceIndexMutationResult result;
        service.lance_index_mutate(result, queued);
        return result;
    });
    ASSERT_TRUE(wait_until([&] { return service.queue_depth() == 1; }, 15000));

    service.stop(); // drain-reject + supervisor TERM + join; must not hang

    const TLanceIndexMutationResult a = hanging_answer.get();
    const TLanceIndexMutationResult b = queued_answer.get();
    // The in-flight domain converged indeterminate on its own executor thread
    // (stop()'s TERM ended the hanging worker; the reply still left after the
    // reap).
    EXPECT_TRUE(status_code_is(a, TStatusCode::OK));
    EXPECT_EQ(a.worker_phase, TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
    // The queued entry was answered with a never-launched rejection: it
    // provably never executed, and its handler thread never waited forever.
    EXPECT_TRUE(status_code_is(b, TStatusCode::CANCELLED));
    EXPECT_FALSE(b.__isset.worker_phase);

    // Post-stop submissions are refused before the queue - provably no fork.
    const TLanceIndexMutationRequest post = make_request("stop-post", epoch_millis() + 120000);
    const std::string post_cgroup = invocation_cgroup_dir(parent, post);
    TLanceIndexMutationResult refused;
    service.lance_index_mutate(refused, post);
    EXPECT_TRUE(status_code_is(refused, TStatusCode::CANCELLED));
    EXPECT_EQ(::access(post_cgroup.c_str(), F_OK), -1) << "a post-stop invocation launched";
    EXPECT_EQ(service.supervisor_for_test().inflight_count_for_test(), 0);
    EXPECT_EQ(service.queue_depth(), 0);
    EXPECT_EQ(service.inflight_workers(), 0);
}

// ---------------------------------------------------------------------------
// Result mapping (D7/D8): the full phase x code-band table, the identity
// echo, and the credentials discipline - driven directly on the mapper.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, ResultMappingTableCoversPhasesAndBands) {
    LanceIndexWorkerService service;
    start_service(&service);
    TLanceIndexMutationRequest request = make_request("map", epoch_millis() + 60000);
    request.__set_storage_options({{"aws.secret.access.key", "super-secret-value-9f1a"}});

    auto map = [&](const Status& status, const WorkerExecution& execution) {
        TLanceIndexMutationResult result;
        service._fill_from_execution(result, request, status, execution);
        return result;
    };
    auto trusted = [&](TLanceIndexWorkerResultPhase::type phase, int32_t code,
                       std::optional<int64_t> observed) {
        WorkerExecution execution;
        execution.kind = WorkerExecution::Kind::TRUSTED_RESULT;
        TLanceIndexWorkerResult frame;
        frame.phase = phase;
        frame.lance_error_code = code;
        if (observed.has_value()) {
            frame.__set_observed_latest_version(*observed);
        }
        execution.result = std::move(frame);
        return execution;
    };

    // Non-OK: BE-level never-launched, no phase, no code, echo present.
    TLanceIndexMutationResult never = map(Status::CgroupError("isolation"), WorkerExecution {});
    EXPECT_FALSE(status_code_is(never, TStatusCode::OK));
    EXPECT_FALSE(never.__isset.worker_phase);
    EXPECT_EQ(never.invocation_id, request.invocation_id);
    EXPECT_EQ(never.be_process_epoch, request.be_process_epoch);

    // OK + phase 1 + a negative worker code: verbatim, observed forwarded.
    WorkerExecution rejected = trusted(TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, -5,
                                       std::optional<int64_t>(12));
    rejected.message = LanceIndexSupervisor::sanitize_message("index unresolved",
                                                              service._build_dispatch(request));
    TLanceIndexMutationResult mapped_reject = map(Status::OK(), rejected);
    EXPECT_TRUE(status_code_is(mapped_reject, TStatusCode::OK));
    EXPECT_EQ(mapped_reject.worker_phase, TLanceIndexWorkerPhase::PRE_INVOCATION_REJECTED);
    EXPECT_EQ(mapped_reject.lance_error_code, -5);
    ASSERT_TRUE(mapped_reject.__isset.observed_latest_version);
    EXPECT_EQ(mapped_reject.observed_latest_version, 12);
    EXPECT_EQ(mapped_reject.invocation_id, request.invocation_id);
    EXPECT_EQ(mapped_reject.be_process_epoch, request.be_process_epoch);

    // OK + phase 1 without an observed version: field 7 stays unset.
    TLanceIndexMutationResult mapped_no_observed = map(
            Status::OK(),
            trusted(TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, -1, std::nullopt));
    EXPECT_FALSE(mapped_no_observed.__isset.observed_latest_version);

    // OK + phase 2 + code 0: the authoritative success shape (no-op same).
    TLanceIndexMutationResult ok_native = map(
            Status::OK(), trusted(TLanceIndexWorkerResultPhase::NATIVE_RETURN, 0, std::nullopt));
    EXPECT_EQ(ok_native.worker_phase, TLanceIndexWorkerPhase::NATIVE_RETURN);
    EXPECT_EQ(ok_native.lance_error_code, 0);
    EXPECT_FALSE(ok_native.__isset.observed_latest_version);

    // OK + phase 2 + code 8 / another positive code: the code rides verbatim;
    // the FE owns the confirmed-vs-indeterminate split.
    for (const int32_t code : {8, 5}) {
        TLanceIndexMutationResult native_error = map(
                Status::OK(),
                trusted(TLanceIndexWorkerResultPhase::NATIVE_RETURN, code, std::nullopt));
        EXPECT_EQ(native_error.lance_error_code, code);
        EXPECT_EQ(native_error.worker_phase, TLanceIndexWorkerPhase::NATIVE_RETURN);
    }

    // OK + PROVEN pre-invocation rejection: phase 1 + the resource-rejected
    // code from the supervisor-synthesized frame.
    WorkerExecution proven;
    proven.kind = WorkerExecution::Kind::PROVEN_PRE_INVOCATION_REJECTION;
    proven.result = TLanceIndexWorkerResult {};
    proven.result->phase = TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED;
    proven.result->lance_error_code = -4;
    TLanceIndexMutationResult mapped_proven = map(Status::OK(), proven);
    EXPECT_EQ(mapped_proven.worker_phase, TLanceIndexWorkerPhase::PRE_INVOCATION_REJECTED);
    EXPECT_EQ(mapped_proven.lance_error_code, -4);

    // OK + INDETERMINATE: phase 3, never a code.
    WorkerExecution indeterminate;
    indeterminate.kind = WorkerExecution::Kind::INDETERMINATE;
    indeterminate.message = LanceIndexSupervisor::sanitize_message(
            "worker exited without a result frame", service._build_dispatch(request));
    TLanceIndexMutationResult mapped_indeterminate = map(Status::OK(), indeterminate);
    EXPECT_EQ(mapped_indeterminate.worker_phase, TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
    EXPECT_FALSE(mapped_indeterminate.__isset.lance_error_code);

    // Credentials discipline at this layer: the only message text the service
    // ever copies is the supervisor's sanitized static category (verified
    // identity fields only), and nothing else it adds (status, echo, counters)
    // can contain a storage-option value.
    for (const TLanceIndexMutationResult* mapped :
         {&never, &mapped_reject, &ok_native, &mapped_proven, &mapped_indeterminate}) {
        if (mapped->__isset.message) {
            EXPECT_EQ(mapped->message.find("super-secret-value-9f1a"), std::string::npos);
        }
    }

    // Outcome buckets: 1 never-launched + 3 pre-invocation (-5, the no-observed
    // -1, and the proven -4) + 1 native ok + 2 native error (8, 5) + 1
    // indeterminate.
    EXPECT_EQ(service.never_launched_total_for_test(), 1);
    EXPECT_EQ(service.pre_invocation_rejected_total_for_test(), 3);
    EXPECT_EQ(service.native_ok_total_for_test(), 1);
    EXPECT_EQ(service.native_error_total_for_test(), 2);
    EXPECT_EQ(service.indeterminate_total_for_test(), 1);
}

// ---------------------------------------------------------------------------
// Dispatch-frame injection: bounds from BE config, debug points names-only.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, DispatchBuildInjectsBoundsAndDebugPoints) {
    LanceIndexWorkerService service;
    start_service(&service);
    config::lance_index_max_num_partitions = 77;
    config::lance_index_max_num_sub_vectors = 88;

    const TLanceIndexMutationRequest request = make_request("bounds", epoch_millis() + 60000);
    const TLanceIndexWorkerDispatch dispatch = service._build_dispatch(request);

    // The 15-field semantic mirror.
    EXPECT_EQ(dispatch.invocation_id, request.invocation_id);
    EXPECT_EQ(dispatch.be_process_epoch, request.be_process_epoch);
    EXPECT_EQ(dispatch.deadline_ms, request.deadline_ms);
    EXPECT_EQ(dispatch.mutation_type, TLanceIndexWorkerMutationType::CREATE);
    EXPECT_EQ(dispatch.index_name, request.index_name);
    EXPECT_EQ(dispatch.column_name, request.column_name);
    EXPECT_EQ(dispatch.index_type, request.index_type);
    EXPECT_EQ(dispatch.dataset_uri, request.dataset_uri);
    EXPECT_EQ(dispatch.admitted_dataset_version, request.admitted_dataset_version);
    EXPECT_FALSE(dispatch.__isset.worker_deadline_ms)
            << "field 19 is the supervisor's stamp, never the service's";

    // The injected bounds: positive (else the worker rejects the frame) and
    // equal to the current config mirrors.
    ASSERT_TRUE(dispatch.__isset.max_num_partitions);
    ASSERT_TRUE(dispatch.__isset.max_num_sub_vectors);
    EXPECT_EQ(dispatch.max_num_partitions, 77);
    EXPECT_EQ(dispatch.max_num_sub_vectors, 88);

    // Nothing armed: field 18 stays unset.
    EXPECT_FALSE(dispatch.__isset.debug_points);

    // The armed allowlist point rides names-only; a foreign name never does.
    config::enable_debug_points = true;
    DebugPoints::instance()->add(WORKER_DEBUG_POINT_HANG);
    const TLanceIndexWorkerDispatch armed = service._build_dispatch(request);
    ASSERT_TRUE(armed.__isset.debug_points);
    EXPECT_EQ(armed.debug_points, std::vector<std::string> {WORKER_DEBUG_POINT_HANG});
    DebugPoints::instance()->remove(WORKER_DEBUG_POINT_HANG);
    DebugPoints::instance()->add("LanceIndexWorker.bogus");
    EXPECT_FALSE(service._build_dispatch(request).__isset.debug_points);
    DebugPoints::instance()->remove("LanceIndexWorker.bogus");

    // Optional wire fields cross only when set.
    TLanceIndexMutationRequest extended = request;
    extended.__set_properties_json("{\"num_partitions\":4}");
    extended.__set_if_not_exists(true);
    extended.__set_storage_options({{"k", "v"}});
    extended.__set_index_display_name("MyIndex");
    const TLanceIndexWorkerDispatch mirrored = service._build_dispatch(extended);
    EXPECT_EQ(mirrored.properties_json, "{\"num_partitions\":4}");
    EXPECT_TRUE(mirrored.__isset.if_not_exists && mirrored.if_not_exists);
    EXPECT_EQ(mirrored.storage_options.at("k"), "v");
    EXPECT_EQ(mirrored.index_display_name, "MyIndex");
}

// ---------------------------------------------------------------------------
// Metrics (N3): the gauges and the outcome counters exist on the server
// entity and the buckets count exactly the driven outcomes.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, MetricsRegisteredAndOutcomeBucketsCount) {
    LanceIndexWorkerService service;
    start_service(&service);
    auto* entity = DorisMetrics::instance()->server_entity();
    EXPECT_NE(entity->get_metric("lance_index_mutation_queue_depth"), nullptr);
    EXPECT_NE(entity->get_metric("lance_index_mutation_inflight_workers"), nullptr);
    EXPECT_NE(entity->get_metric("lance_index_mutation_outcome_never_launched"), nullptr);
    EXPECT_NE(entity->get_metric("lance_index_mutation_outcome_pre_invocation_rejected"),
              nullptr);
    EXPECT_NE(entity->get_metric("lance_index_mutation_outcome_native_ok"), nullptr);
    EXPECT_NE(entity->get_metric("lance_index_mutation_outcome_native_error"), nullptr);
    EXPECT_NE(entity->get_metric("lance_index_mutation_outcome_indeterminate"), nullptr);

    // One malformed delivery, one duplicate, one unverified-isolation run.
    TLanceIndexMutationResult invalid_answer;
    TLanceIndexMutationRequest invalid = make_request("metrics", epoch_millis() + 60000);
    invalid.__set_index_name("");
    service.lance_index_mutate(invalid_answer, invalid);
    const TLanceIndexMutationRequest once = make_request("metrics", epoch_millis() + 60000);
    TLanceIndexMutationResult executed;
    service.lance_index_mutate(executed, once);
    TLanceIndexMutationResult duplicate;
    service.lance_index_mutate(duplicate, once);

    EXPECT_EQ(service.never_launched_total_for_test(), 2);
    EXPECT_EQ(service.indeterminate_total_for_test(), 1);
    EXPECT_EQ(service.native_ok_total_for_test(), 0);
    EXPECT_EQ(service.queue_depth(), 0);
    EXPECT_EQ(service.inflight_workers(), 0);

    service.stop();
    EXPECT_EQ(entity->get_metric("lance_index_mutation_queue_depth"), nullptr)
            << "the gauges deregister at stop";
    // The outcome counters deliberately stay registered past stop(): stop()
    // joins no thrift handler threads, and an RPC racing the shutdown still
    // bumps a bucket through _bump - the member pointers must never dangle
    // (the entity owns the counters for its process lifetime).
    EXPECT_NE(entity->get_metric("lance_index_mutation_outcome_never_launched"), nullptr)
            << "the outcome counters stay registered past stop";
    EXPECT_NE(entity->get_metric("lance_index_mutation_outcome_indeterminate"), nullptr)
            << "the outcome counters stay registered past stop";
}

// ---------------------------------------------------------------------------
// Real execution round trips (delegated runner): the service's answer for the
// fake worker personas - trusted rejection, no-op note, silent worker - plus
// the armed debug point riding the service's frame-18 snapshot.
// ---------------------------------------------------------------------------

TEST_F(LanceIndexWorkerServiceTest, EndToEndTrustedRejectionAndSilentWorkerShapes) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_mutation_worker_max_concurrency = 1;
    LanceIndexWorkerService service;
    service.force_be_process_epoch_for_test(kTestBeEpoch);
    ASSERT_TRUE(service.start().ok());

    // PRE_INVOCATION_REJECTED rides verbatim (phase 1, negative band) with
    // the supervisor-sanitized message.
    const TLanceIndexMutationRequest rejected_request =
            make_request("e2e-reject", epoch_millis() + 60000);
    arm_real_isolation(&service, parent, "reject_unresolved");
    invocation_cgroup_dir(parent, rejected_request);
    TLanceIndexMutationResult rejected;
    service.lance_index_mutate(rejected, rejected_request);
    EXPECT_TRUE(status_code_is(rejected, TStatusCode::OK));
    EXPECT_EQ(rejected.worker_phase, TLanceIndexWorkerPhase::PRE_INVOCATION_REJECTED);
    EXPECT_EQ(rejected.lance_error_code, -5);
    EXPECT_TRUE(rejected.__isset.message);

    // skip_result: handshake then silent exit - phase 3, no code, even though
    // the child provably exec'd and exited cleanly.
    service.supervisor_for_test().force_worker_exec_for_test(fake_worker_path(), {"skip_result"});
    const TLanceIndexMutationRequest silent_request =
            make_request("e2e-silent", epoch_millis() + 60000);
    invocation_cgroup_dir(parent, silent_request);
    TLanceIndexMutationResult silent;
    service.lance_index_mutate(silent, silent_request);
    EXPECT_TRUE(status_code_is(silent, TStatusCode::OK));
    EXPECT_EQ(silent.worker_phase, TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
    EXPECT_FALSE(silent.__isset.lance_error_code);

    EXPECT_EQ(service.pre_invocation_rejected_total_for_test(), 1);
    EXPECT_EQ(service.indeterminate_total_for_test(), 1);
    EXPECT_EQ(service.launched_total_for_test(), 2);
}

TEST_F(LanceIndexWorkerServiceTest, ArmedDebugPointRidesTheServiceDispatch) {
    REQUIRE_DELEGATED_PARENT(parent);
    config::lance_index_mutation_worker_max_concurrency = 1;
    config::enable_debug_points = true;
    DebugPoints::instance()->add(WORKER_DEBUG_POINT_HANG);
    LanceIndexWorkerService service;
    service.force_be_process_epoch_for_test(kTestBeEpoch);
    ASSERT_TRUE(service.start().ok());
    // The happy persona would complete immediately; only the service's
    // armed-then-dispatched field-18 snapshot can make the worker hang. The
    // kill deadline is deadline - 22s = now + 4s, so the ladder ends the hang
    // quickly.
    arm_real_isolation(&service, parent, "happy");
    const TLanceIndexMutationRequest request = make_request("e2e-hang", epoch_millis() + 26000);
    invocation_cgroup_dir(parent, request);

    TLanceIndexMutationResult result;
    service.lance_index_mutate(result, request);
    EXPECT_TRUE(status_code_is(result, TStatusCode::OK));
    EXPECT_EQ(result.worker_phase, TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
    EXPECT_FALSE(result.__isset.lance_error_code);
    EXPECT_EQ(service.launched_total_for_test(), 1);
}

} // namespace doris::lance
