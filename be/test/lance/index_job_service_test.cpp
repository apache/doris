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

#include "lance/index_job_service.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Status_types.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <signal.h>

#include "agent/utils.h"
#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "util/blocking_queue.hpp"
#include "util/dns_cache.h"

// LanceIndexJobServiceTest: the thrift handler guards of submit_lance_index_job
// (task_worker_pool_test.cpp precedent: direct construction, ExecEnv stub,
// -fno-access-control private access; the thrift server itself never starts in
// UT). The supervisor seam is faked per case — the isolation flag is forced,
// or the bounded queue is pre-filled with _started flipped so no executor
// thread ever runs — so these cases need no cgroup delegation at all.
//
// Coverage note for review: the report path binds MasterServerClient through
// the lambdas wired in start(); the retry test below fires the REAL bound
// lambda against a dead master address, so the 3-attempt + sleep(1) + counter
// discipline and the outstanding-slot release run end-to-end. What no UT can
// cover without a live FE is the SUCCESS branch of the thrift call itself
// (that remains for the regression e2e).

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

TLanceIndexJobDispatch make_dispatch(const std::string& invocation_id) {
    TLanceIndexJobDispatch dispatch;
    dispatch.job_id = 31337;
    dispatch.dispatch_revision = 1;
    dispatch.invocation_id = invocation_id;
    dispatch.be_process_epoch = 42;
    dispatch.deadline_ms = epoch_millis() + 3600 * 1000;
    dispatch.mutation_type = TLanceIndexMutationType::CREATE;
    dispatch.index_name = "idx_v";
    dispatch.column_name = "v";
    dispatch.index_type = "IVF_PQ";
    dispatch.dataset_uri = "s3://bucket/dataset";
    dispatch.admitted_dataset_version = 9;
    dispatch.schema_contract_json = "{}";
    // A fresh FE dispatch always carries the per-dispatch secret.
    dispatch.__set_invocation_secret("be-ut-invocation-secret-0123456789abcdef");
    return dispatch;
}

std::string joined_errors(const TStatus& status) {
    std::string joined;
    if (status.__isset.error_msgs) {
        for (const std::string& msg : status.error_msgs) {
            joined += msg;
            joined += '\n';
        }
    }
    return joined;
}

} // namespace

class LanceIndexJobServiceTest : public ::testing::Test {
protected:
    ClusterInfo cluster_info_;
    ClusterInfo* saved_cluster_info_ = nullptr;
    sighandler_t old_sigchld_ = SIG_DFL;
    bool saved_preflight_switch_ = true;

    void SetUp() override {
        old_sigchld_ = signal(SIGCHLD, SIG_DFL);
        saved_cluster_info_ = ExecEnv::GetInstance()->cluster_info();
        // Default: no master FE heartbeat (empty hostname, port 0).
        ExecEnv::GetInstance()->set_cluster_info(&cluster_info_);
        saved_preflight_switch_ = config::lance_index_isolation_preflight;
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_cluster_info(saved_cluster_info_);
        config::lance_index_isolation_preflight = saved_preflight_switch_;
        signal(SIGCHLD, old_sigchld_);
    }

    void set_heartbeat(const std::string& host = "127.0.0.1", int port = 19030) {
        cluster_info_.master_fe_addr.__set_hostname(host);
        cluster_info_.master_fe_addr.__set_port(port);
    }

    // A service whose supervisor accepts submits into a bounded queue that
    // never drains (no executor threads): deterministic dedup/queue-full
    // semantics without forking anything.
    static void fake_started_queue(LanceIndexJobService* service, uint32_t capacity) {
        service->_supervisor._isolation_verified.store(true);
        service->_supervisor._queue =
                std::make_unique<BlockingQueue<TLanceIndexJobDispatch>>(capacity);
        service->_supervisor._started = true;
    }
};

// Guard 1: no master FE heartbeat -> synchronous Cancelled, nothing enqueued.
TEST_F(LanceIndexJobServiceTest, NoHeartbeatRejectedNotEnqueued) {
    LanceIndexJobService service(ExecEnv::GetInstance());
    const auto dispatch = make_dispatch("svc-no-heartbeat");

    // Case 1: empty master address (the fixture default).
    TStatus status;
    service.submit_lance_index_job(status, dispatch);
    EXPECT_EQ(status.status_code, TStatusCode::CANCELLED);
    EXPECT_NE(joined_errors(status).find("heartbeat"), std::string::npos)
            << joined_errors(status);
    EXPECT_EQ(service._supervisor.queue_depth_for_test(), 0U);
    EXPECT_EQ(service.queue_size(), 0);

    // Case 2: hostname set but port 0 is still "no heartbeat".
    cluster_info_.master_fe_addr.__set_hostname("127.0.0.1");
    cluster_info_.master_fe_addr.__set_port(0);
    TStatus status2;
    service.submit_lance_index_job(status2, dispatch);
    EXPECT_EQ(status2.status_code, TStatusCode::CANCELLED);
    EXPECT_EQ(service._outstanding.load(), 0);
}

// Guard 2: isolation not verified -> synchronous CgroupError-class ERROR.
TEST_F(LanceIndexJobServiceTest, IsolationNotVerifiedRejected) {
    set_heartbeat();
    LanceIndexJobService service(ExecEnv::GetInstance());
    // No start(), no preflight: isolation_verified() is false.
    const auto dispatch = make_dispatch("svc-no-isolation");
    TStatus status;
    service.submit_lance_index_job(status, dispatch);
    // CGROUP_ERROR (-7411) has no TStatusCode counterpart and maps to
    // INTERNAL_ERROR; the class is pinned by the message.
    EXPECT_EQ(status.status_code, TStatusCode::INTERNAL_ERROR);
    EXPECT_NE(joined_errors(status).find("isolation is not verified"), std::string::npos)
            << joined_errors(status);
    EXPECT_EQ(service._supervisor.queue_depth_for_test(), 0U);
    EXPECT_EQ(service._outstanding.load(), 0);
}

// Guard 3: D4 payload limits -> synchronous InvalidArgument, never enqueued;
// error strings carry sizes only, never key/value content.
TEST_F(LanceIndexJobServiceTest, PayloadViolationsRejectedSizesOnly) {
    set_heartbeat();
    LanceIndexJobService service(ExecEnv::GetInstance());
    service._supervisor._isolation_verified.store(true);

    struct PayloadCase {
        std::string name;
        TLanceIndexJobDispatch dispatch;
        std::string expected_size_hint;
        std::string forbidden_substring;
    };
    std::vector<PayloadCase> cases;

    // Oversized frame: a 600 KiB dataset_uri pushes the compact payload past
    // the 512 KiB dispatch cap.
    auto oversized = make_dispatch("svc-oversized-frame");
    oversized.dataset_uri = std::string(600 * 1024, 'x');
    cases.push_back({"oversized frame", oversized, "524288", std::string(64, 'x')});

    // Too many storage options (65 > 64).
    auto too_many = make_dispatch("svc-too-many-options");
    std::map<std::string, std::string> options;
    for (int i = 0; i < 65; ++i) {
        options.emplace("key" + std::to_string(i), "TOPSECRET-value-" + std::to_string(i));
    }
    too_many.__set_storage_options(options);
    cases.push_back({"too many options", too_many, "65", "TOPSECRET"});

    // Storage-option key over the 256-byte limit.
    auto long_key = make_dispatch("svc-long-key");
    long_key.__set_storage_options({{std::string(200, 'k') + "SECRETKEYMARKER" +
                                             std::string(100, 'k'),
                                     "v"}});
    cases.push_back({"long key", long_key, "256", "SECRETKEYMARKER"});

    // Storage-option value over the 4096-byte limit.
    auto long_value = make_dispatch("svc-long-value");
    long_value.__set_storage_options(
            {{"password", "s3cr3t-payload-marker" + std::string(4096, 'v')}});
    cases.push_back({"long value", long_value, "4096", "s3cr3t-payload-marker"});

    for (const PayloadCase& c : cases) {
        TStatus status;
        service.submit_lance_index_job(status, c.dispatch);
        EXPECT_EQ(status.status_code, TStatusCode::INVALID_ARGUMENT) << c.name;
        const std::string errors = joined_errors(status);
        EXPECT_NE(errors.find(c.expected_size_hint), std::string::npos)
                << c.name << " error should carry the size numbers: " << errors;
        EXPECT_EQ(errors.find(c.forbidden_substring), std::string::npos)
                << c.name << " error must not leak payload content: " << errors;
        // NOT enqueued: the supervisor saw nothing (never even started).
        EXPECT_FALSE(service._supervisor._started) << c.name;
        EXPECT_EQ(service._supervisor.queue_depth_for_test(), 0U) << c.name;
        EXPECT_EQ(service._outstanding.load(), 0) << c.name;
    }
}

// Guard 4: duplicate invocation_id -> OK idempotent; the first enqueue owns
// execution, the redelivery is a no-op.
TEST_F(LanceIndexJobServiceTest, DuplicateInvocationIdempotentOk) {
    set_heartbeat();
    LanceIndexJobService service(ExecEnv::GetInstance());
    fake_started_queue(&service, 2);

    const auto dispatch = make_dispatch("svc-dup");
    TStatus first;
    service.submit_lance_index_job(first, dispatch);
    ASSERT_EQ(first.status_code, TStatusCode::OK);
    EXPECT_EQ(service._outstanding.load(), 1);

    TStatus second;
    service.submit_lance_index_job(second, dispatch);
    EXPECT_EQ(second.status_code, TStatusCode::OK) << joined_errors(second);
    // Enqueued exactly once: the redelivery did not add a queue entry.
    EXPECT_EQ(service._supervisor.queue_depth_for_test(), 1U);
    EXPECT_EQ(service._outstanding.load(), 1);
}

// Guard 5: bounded queue full -> synchronous TooManyTasks, and the dedup entry
// is rolled back (a retry after the failure is again TooManyTasks, not a
// phantom "already accepted").
TEST_F(LanceIndexJobServiceTest, QueueFullRejectedAndDedupRolledBack) {
    set_heartbeat();
    LanceIndexJobService service(ExecEnv::GetInstance());
    fake_started_queue(&service, 2);
    // Capacity 2 saturated directly at the supervisor seam.
    ASSERT_TRUE(service._supervisor._queue->try_put(make_dispatch("svc-prefill-1")));
    ASSERT_TRUE(service._supervisor._queue->try_put(make_dispatch("svc-prefill-2")));

    const auto dispatch = make_dispatch("svc-overflow");
    TStatus status;
    service.submit_lance_index_job(status, dispatch);
    EXPECT_EQ(status.status_code, TStatusCode::TOO_MANY_TASKS) << joined_errors(status);
    EXPECT_EQ(service._outstanding.load(), 0);

    TStatus retry;
    service.submit_lance_index_job(retry, dispatch);
    EXPECT_EQ(retry.status_code, TStatusCode::TOO_MANY_TASKS)
            << "the dedup entry must be rolled back on enqueue failure";
    EXPECT_EQ(service._supervisor.queue_depth_for_test(), 2U);
}

// Guard 4 (D3, handler half of the budget double-check): a dispatch whose
// remaining deadline cannot cover the report margin + termination grace +
// minimum executable budget is rejected SYNCHRONOUSLY (never enqueued), while
// one with an ample budget is enqueued normally.
TEST_F(LanceIndexJobServiceTest, DeadlineBudgetRailRejectsNearExpiredSynchronously) {
    set_heartbeat();
    LanceIndexJobService service(ExecEnv::GetInstance());
    fake_started_queue(&service, 2);

    // Near-expired: remaining(3s) - margin - grace is far below the minimum
    // executable budget under any shipped config.
    auto expired = make_dispatch("svc-budget-exhausted");
    expired.deadline_ms = epoch_millis() + 3000;
    TStatus status;
    service.submit_lance_index_job(status, expired);
    EXPECT_EQ(status.status_code, TStatusCode::CANCELLED) << joined_errors(status);
    EXPECT_NE(joined_errors(status).find("deadline budget"), std::string::npos)
            << joined_errors(status);
    EXPECT_EQ(service._supervisor.queue_depth_for_test(), 0U)
            << "a budget-exhausted dispatch must not occupy a queue slot";
    EXPECT_EQ(service._outstanding.load(), 0);
    // Nothing entered the dedup set either: a fresh-budget redelivery of the
    // same invocation id is accepted on its own terms.
    auto retry = make_dispatch("svc-budget-exhausted");
    retry.deadline_ms = epoch_millis() + 3600 * 1000;
    TStatus retry_status;
    service.submit_lance_index_job(retry_status, retry);
    EXPECT_EQ(retry_status.status_code, TStatusCode::OK) << joined_errors(retry_status);
    EXPECT_EQ(service._supervisor.queue_depth_for_test(), 1U);
    EXPECT_EQ(service._outstanding.load(), 1);

    // Ample budget: enqueued normally.
    const auto ample = make_dispatch("svc-budget-ample");
    TStatus ok_status;
    service.submit_lance_index_job(ok_status, ample);
    EXPECT_EQ(ok_status.status_code, TStatusCode::OK) << joined_errors(ok_status);
    EXPECT_EQ(service._supervisor.queue_depth_for_test(), 2U);
    EXPECT_EQ(service._outstanding.load(), 2);
}

// The report retry discipline pins the finish_task precedent
// (task_worker_pool.cpp): the loop consults ONLY the client-side Status of the
// thrift call. The TStatus payload is deliberately NOT inspected — the only
// non-OK value this channel can produce is NOT_MASTER, and a report aimed at a
// stale master counts as delivered (the new master's transfer sweep converges
// the job; the epoch sweep releases the slot). The direct _report_with_retry
// seam (injected attempt function) pins both the NOT_MASTER-tolerance and the
// plain-success branch without a live FE.
TEST_F(LanceIndexJobServiceTest, ReportRetryIgnoresTStatusPayloadNotMasterTreatedAsDelivered) {
    LanceIndexJobService service(ExecEnv::GetInstance());
    // The retry loop calls the attempt only when the client singleton exists;
    // our injected attempt never dereferences it, so a bare create on our own
    // ClusterInfo suffices (no DNSCache needed — no real connection is made).
    if (MasterServerClient::instance() == nullptr) {
        MasterServerClient::create(&cluster_info_);
    }

    // Success branch: OK payload, exactly one attempt, slot released.
    service._outstanding.store(1);
    int attempts = 0;
    service._report_with_retry("result", 31337, "svc-report-success",
                               [&attempts](MasterServerClient*, TStatus* status) {
                                   ++attempts;
                                   status->status_code = TStatusCode::OK;
                                   return Status::OK();
                               });
    EXPECT_EQ(attempts, 1);
    EXPECT_EQ(service._outstanding.load(), 0);

    // NOT_MASTER branch: non-OK payload with a successful thrift call is
    // delivered — no retry, slot released.
    service._outstanding.store(1);
    attempts = 0;
    service._report_with_retry("result", 31337, "svc-report-not-master",
                               [&attempts](MasterServerClient*, TStatus* status) {
                                   ++attempts;
                                   status->status_code = TStatusCode::NOT_MASTER;
                                   return Status::OK();
                               });
    EXPECT_EQ(attempts, 1)
            << "a non-OK TStatus payload must not be retried (finish_task discipline)";
    EXPECT_EQ(service._outstanding.load(), 0);
}

// The stopping-drop path (F13c): while the service is stopping, terminal
// reports skip the RPC but still release the invocation's outstanding slot.
TEST_F(LanceIndexJobServiceTest, StoppingDropReleasesOutstandingSlot) {
    config::lance_index_isolation_preflight = false; // keep start() hermetic
    LanceIndexJobService service(ExecEnv::GetInstance());
    ASSERT_TRUE(service.start().ok());

    TLanceIndexJobReport report;
    report.job_id = 31337;
    report.dispatch_revision = 1;
    report.invocation_id = "svc-stopping-drop";
    report.be_process_epoch = 42;
    report.result_code = TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED;

    service._outstanding.store(2);
    service.stop(); // flips _stopping; the supervisor was never started
    const int64_t started = steady_millis();
    service._supervisor._report_result_fn(report);
    EXPECT_EQ(service._outstanding.load(), 1)
            << "a stopping-dropped result report still releases the gauge slot";
    EXPECT_LT(steady_millis() - started, 1000) << "the dropped report attempted no RPC";

    TLanceIndexJobTerminationReport termination;
    termination.job_id = 31337;
    termination.dispatch_revision = 1;
    termination.invocation_id = "svc-stopping-drop";
    termination.be_process_epoch = 42;
    termination.proof = TLanceIndexTerminationProof::CHILD_REAPED;
    service._supervisor._report_termination_fn(termination);
    EXPECT_EQ(service._outstanding.load(), 0);
}

// The silent-ending release (P2-1): an unprovable termination emits NO report
// at all, so the supervisor's silent callback is the invocation's only
// terminal accounting — the gauge slot must be released there (mirroring the
// stopping-drop case above) or the worker would read busy until BE restart.
TEST_F(LanceIndexJobServiceTest, SilentEndingReleasesOutstandingSlot) {
    config::lance_index_isolation_preflight = false; // keep start() hermetic
    LanceIndexJobService service(ExecEnv::GetInstance());
    ASSERT_TRUE(service.start().ok());

    ASSERT_TRUE(static_cast<bool>(service._supervisor._report_silent_fn))
            << "start() must wire the silent-ending callback";
    service._outstanding.store(1);
    const int64_t started = steady_millis();
    service._supervisor._report_silent_fn(31337, "svc-silent-ending");
    EXPECT_EQ(service._outstanding.load(), 0) << "a silent ending must release the gauge slot";
    EXPECT_EQ(service.queue_size(), 0);
    EXPECT_EQ(service.inflight_workers(), 0)
            << "with max_inflight=1 a leaked slot would read the worker busy forever";
    EXPECT_LT(steady_millis() - started, 1000) << "the silent path attempts no RPC";
}

// Gauge feeders (D19): outstanding -> min/max accounting.
TEST_F(LanceIndexJobServiceTest, GaugeMathFollowsOutstanding) {
    LanceIndexJobService service(ExecEnv::GetInstance());
    const int32_t max_inflight = config::lance_index_worker_max_inflight;
    service._outstanding.store(0);
    EXPECT_EQ(service.queue_size(), 0);
    EXPECT_EQ(service.inflight_workers(), 0);
    service._outstanding.store(1);
    EXPECT_EQ(service.queue_size(), std::max<int64_t>(0, 1 - max_inflight));
    EXPECT_EQ(service.inflight_workers(), std::min<int64_t>(1, max_inflight));
    service._outstanding.store(max_inflight + 2);
    EXPECT_EQ(service.queue_size(), 2);
    EXPECT_EQ(service.inflight_workers(), max_inflight);
}

// The report callback binding (D14): fire the REAL lambdas start() wired into
// the supervisor and watch the finish_task-style discipline (3 attempts,
// sleep(1) between, success/failure counters) run end-to-end; the invocation's
// outstanding slot is released regardless of the outcome.
//
// Speed note: when the MasterServerClient singleton does not exist yet (the
// usual case in a focused run), attempts fail instantly with Uninitialized —
// the 3-attempt + counter + slot-release discipline is identical and the test
// costs ~3s per report. When another suite already created the singleton
// (full-binary runs), its ClusterInfo may be dead, so we rebind it to ours
// with an empty master hostname (fast DNS failure; the upstream
// ClientConnection retry/backoff dominates at ~3s per attempt). Either way
// every attempt fails — the success branch of the thrift call itself needs a
// live FE and stays with the regression e2e.
TEST_F(LanceIndexJobServiceTest, ReportRetryCountersAndSlotRelease) {
    config::lance_index_isolation_preflight = false; // keep start() hermetic
    LanceIndexJobService service(ExecEnv::GetInstance());
    ASSERT_TRUE(service.start().ok());
    if (MasterServerClient::instance() != nullptr) {
        // Rebind the singleton to our own (alive) ClusterInfo with an
        // unresolvable master address; see the speed note above. The DNS cache
        // stub: ExecEnv::init() (which UT never runs) is the only place it is
        // created, and ClientCacheHelper::get_client dereferences it
        // unconditionally. Leaked on purpose (process lifetime): the refresh
        // thread checks its stop flag only once a minute, so deleting the
        // cache would stall TearDown on join().
        if (ExecEnv::GetInstance()->dns_cache() == nullptr) {
            ExecEnv::GetInstance()->_dns_cache = new DNSCache();
        }
        MasterServerClient::create(&cluster_info_); // master_fe_addr stays empty
    }

    IntCounter* total = DorisMetrics::instance()->lance_index_job_report_requests_total;
    IntCounter* failed = DorisMetrics::instance()->lance_index_job_report_requests_failed;
    ASSERT_NE(total, nullptr);
    ASSERT_NE(failed, nullptr);
    const int64_t total0 = total->value();
    const int64_t failed0 = failed->value();

    TLanceIndexJobReport report;
    report.job_id = 31337;
    report.dispatch_revision = 1;
    report.invocation_id = "svc-report-result";
    report.be_process_epoch = 42;
    report.result_code = TLanceIndexJobResultCode::NATIVE_IO;

    service._outstanding.store(1);
    const int64_t started = steady_millis();
    service._supervisor._report_result_fn(report); // the lambda start() bound
    const int64_t elapsed_result = steady_millis() - started;
    EXPECT_EQ(total->value(), total0 + 3) << "exactly 3 attempts";
    EXPECT_EQ(failed->value(), failed0 + 3);
    EXPECT_GE(elapsed_result, 2000) << "the sleep(1) discipline between attempts regressed";
    EXPECT_LT(elapsed_result, 45000);
    EXPECT_EQ(service._outstanding.load(), 0)
            << "the invocation slot is released even when every attempt failed";

    // The termination report rides the same _report_with_retry; its lambda
    // must be wired by start() as well. Fire it once to prove the binding.
    ASSERT_TRUE(static_cast<bool>(service._supervisor._report_termination_fn));
    TLanceIndexJobTerminationReport termination;
    termination.job_id = 31337;
    termination.dispatch_revision = 1;
    termination.invocation_id = "svc-report-termination";
    termination.be_process_epoch = 42;
    termination.proof = TLanceIndexTerminationProof::CHILD_REAPED;

    service._outstanding.store(1);
    service._supervisor._report_termination_fn(termination);
    EXPECT_EQ(total->value(), total0 + 6);
    EXPECT_EQ(failed->value(), failed0 + 6);
    EXPECT_EQ(service._outstanding.load(), 0);
    service.stop();
}

} // namespace doris::lance
