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
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "agent/utils.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"

namespace doris {

// Queue depth and in-flight worker gauges (D19), pull-fed from the service's
// own accounting via the REGISTER_HOOK_METRIC pattern of
// internal_service.cpp:148-150/252-254.
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(lance_index_job_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(lance_index_job_inflight_workers, MetricUnit::NOUNIT);

namespace lance {
namespace {

// D4 payload limits (cpp_interface_contract §2): validated by the handler
// before anything is enqueued; the supervisor re-checks the frame bound at
// dispatch time and the worker re-checks everything at parse time.
constexpr uint32_t MAX_DISPATCH_FRAME_BYTES = 512 * 1024;
constexpr size_t MAX_STORAGE_OPTIONS = 64;
constexpr size_t MAX_STORAGE_OPTION_KEY_BYTES = 256;
constexpr size_t MAX_STORAGE_OPTION_VALUE_BYTES = 4096;

bool encode_compact(const TLanceIndexJobDispatch& dispatch, std::vector<uint8_t>* out) {
    try {
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        apache::thrift::protocol::TCompactProtocol protocol(transport);
        dispatch.write(&protocol);
        uint8_t* buffer = nullptr;
        uint32_t size = 0;
        transport->getBuffer(&buffer, &size);
        out->assign(buffer, buffer + size);
    } catch (...) {
        return false;
    }
    return true;
}

int64_t epoch_millis_now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
}

// Payload validation runs BEFORE enqueue; a failure here is a definitive
// NOT-enqueued answer. Error messages carry sizes only — never a
// storage-option key or value.
Status validate_dispatch_payload(const TLanceIndexJobDispatch& dispatch) {
    std::vector<uint8_t> payload;
    if (!encode_compact(dispatch, &payload)) {
        return Status::InvalidArgument("lance index dispatch failed thrift-compact serialization");
    }
    if (payload.size() > MAX_DISPATCH_FRAME_BYTES) {
        return Status::InvalidArgument(
                "lance index dispatch frame is too large: {} bytes (limit {})", payload.size(),
                MAX_DISPATCH_FRAME_BYTES);
    }
    if (dispatch.__isset.storage_options) {
        if (dispatch.storage_options.size() > MAX_STORAGE_OPTIONS) {
            return Status::InvalidArgument(
                    "lance index dispatch carries too many storage options: {} (limit {})",
                    dispatch.storage_options.size(), MAX_STORAGE_OPTIONS);
        }
        for (const auto& option : dispatch.storage_options) {
            if (option.first.size() > MAX_STORAGE_OPTION_KEY_BYTES ||
                option.second.size() > MAX_STORAGE_OPTION_VALUE_BYTES) {
                return Status::InvalidArgument(
                        "lance index dispatch storage option exceeds the key/value size limit "
                        "(key <= {} bytes, value <= {} bytes)",
                        MAX_STORAGE_OPTION_KEY_BYTES, MAX_STORAGE_OPTION_VALUE_BYTES);
            }
        }
    }
    return Status::OK();
}

} // namespace

LanceIndexJobService::LanceIndexJobService(ExecEnv* exec_env) : _exec_env(exec_env) {}

LanceIndexJobService::~LanceIndexJobService() {
    if (_metrics_registered) {
        DEREGISTER_HOOK_METRIC(lance_index_job_queue_size);
        DEREGISTER_HOOK_METRIC(lance_index_job_inflight_workers);
        _metrics_registered = false;
    }
    stop();
}

Status LanceIndexJobService::start() {
    // The callback seam is wired before the thrift server can accept any
    // connection (start_thrift_dependencies runs before the processor is
    // constructed), so the supervisor never drops a report for want of a
    // callback.
    _supervisor.set_report_result_callback(
            [this](const TLanceIndexJobReport& report) { _report_result(report); });
    _supervisor.set_report_termination_callback(
            [this](const TLanceIndexJobTerminationReport& report) { _report_termination(report); });
    _supervisor.set_report_silent_callback(
            [this](int64_t job_id, const std::string& invocation_id) {
                _release_slot_on_silent_ending(job_id, invocation_id);
            });

    REGISTER_HOOK_METRIC(lance_index_job_queue_size, [this]() { return queue_size(); });
    REGISTER_HOOK_METRIC(lance_index_job_inflight_workers, [this]() { return inflight_workers(); });
    _metrics_registered = true;

    // The startup probe (D2/D7). A failure is non-fatal for BE startup: the
    // supervisor logs the precise step, isolation stays unverified, and every
    // submission is synchronously rejected with CGROUP_ERROR.
    static_cast<void>(_supervisor.preflight());
    return Status::OK();
}

void LanceIndexJobService::stop() {
    if (_stopping.exchange(true)) {
        return;
    }
    _supervisor.stop();
}

int64_t LanceIndexJobService::queue_size() const {
    const int64_t outstanding = _outstanding.load();
    return std::max<int64_t>(0, outstanding - config::lance_index_worker_max_inflight);
}

int64_t LanceIndexJobService::inflight_workers() const {
    const int64_t outstanding = _outstanding.load();
    return std::min<int64_t>(outstanding, config::lance_index_worker_max_inflight);
}

void LanceIndexJobService::submit_lance_index_job(TStatus& _return,
                                                  const TLanceIndexJobDispatch& dispatch) {
    // Guard 1: no master FE heartbeat yet (agent_server.cpp:286-290 precedent).
    // Without a master address the later result report could not even be
    // attempted, so the dispatch is refused synchronously.
    const ClusterInfo* cluster_info = _exec_env->cluster_info();
    if (cluster_info == nullptr || cluster_info->master_fe_addr.hostname.empty() ||
        cluster_info->master_fe_addr.port == 0) {
        LOG(WARNING) << "rejecting lance index dispatch before the first FE master heartbeat: "
                        "job_id="
                     << dispatch.job_id << " invocation_id=" << dispatch.invocation_id
                     << " mutation_type=" << to_string(dispatch.mutation_type);
        Status::Cancelled("Have not get FE Master heartbeat yet").to_thrift(&_return);
        return;
    }

    // Guard 2: the startup isolation preflight failed or was skipped (D2/D7 —
    // no soft fallback anywhere).
    if (!_supervisor.isolation_verified()) {
        LOG(WARNING) << "rejecting lance index dispatch: worker isolation is not verified: "
                        "job_id="
                     << dispatch.job_id << " invocation_id=" << dispatch.invocation_id
                     << " mutation_type=" << to_string(dispatch.mutation_type);
        Status::CgroupError(
                "lance index worker isolation is not verified (startup preflight failed or was "
                "skipped); rejecting submission")
                .to_thrift(&_return);
        return;
    }

    // Guard 3: D4 payload limits, checked before anything is enqueued.
    Status payload_status = validate_dispatch_payload(dispatch);
    if (!payload_status.ok()) {
        LOG(WARNING) << "rejecting lance index dispatch: payload validation failed: job_id="
                     << dispatch.job_id << " invocation_id=" << dispatch.invocation_id
                     << " mutation_type=" << to_string(dispatch.mutation_type)
                     << " error=" << payload_status.to_string();
        payload_status.to_thrift(&_return);
        return;
    }

    // Guard 4 (D3, handler half of the double-check): the deadline budget rail.
    // Same formula as the supervisor's dequeue rail, reading the same configs:
    // available = remaining - report_margin - term_grace, clamped by the
    // wall-clock ceiling, must exceed the shared minimum executable budget. A
    // dispatch that is already (near-)expired on arrival fails fast here —
    // synchronously and without occupying a queue slot — instead of riding the
    // queue to the asynchronous NEVER_LAUNCHED envelope. The supervisor
    // re-evaluates at dequeue (queue wait consumes budget), so nothing hinges
    // on this check passing later. Ordering note: this rail sits before the
    // supervisor's dedup check — safe because a same-invocation redelivery is
    // an RPC-layer duplicate that lands within milliseconds of the first send
    // (a fresh FE dispatch attempt always mints a new invocation id), so a
    // redelivery can never arrive with an exhausted budget.
    const int64_t remaining_ms = dispatch.deadline_ms - epoch_millis_now();
    const int64_t available_s = remaining_ms / 1000 -
                                config::lance_index_worker_report_margin_seconds -
                                config::lance_index_worker_term_grace_seconds;
    const int64_t handler_wall_s =
            std::min<int64_t>(config::lance_index_worker_wallclock_limit_seconds, available_s);
    if (handler_wall_s <= MIN_EXECUTABLE_BUDGET_SECONDS) {
        LOG(WARNING) << "rejecting lance index dispatch: insufficient deadline budget on "
                        "arrival: job_id="
                     << dispatch.job_id << " invocation_id=" << dispatch.invocation_id
                     << " mutation_type=" << to_string(dispatch.mutation_type)
                     << " remaining_ms=" << remaining_ms;
        Status::Cancelled(
                "lance index dispatch deadline budget is already exhausted on arrival "
                "(remaining budget does not cover the report margin, the termination grace "
                "and the minimum executable budget); rejecting submission")
                .to_thrift(&_return);
        return;
    }

    // Guards 5 (invocation dedup) and 6 (bounded try_put) live inside the
    // supervisor. The gauge slot is reserved BEFORE submit so the terminal
    // callback can never observe a negative balance: an accepted dispatch is
    // visible to the executor threads the moment try_put succeeds inside
    // submit().
    _outstanding.fetch_add(1);
    Status submit_status = _supervisor.submit(dispatch);
    if (!submit_status.ok()) {
        _outstanding.fetch_sub(1);
        if (submit_status.is<ErrorCode::ALREADY_EXIST>()) {
            // Contract "OK = enqueued exactly once": the first enqueue owns
            // execution and the terminal report; a redelivery of the same
            // invocation id is an idempotent no-op.
            LOG(INFO) << "lance index invocation redelivered; answering OK from the dedup set: "
                         "job_id="
                      << dispatch.job_id << " invocation_id=" << dispatch.invocation_id
                      << " mutation_type=" << to_string(dispatch.mutation_type);
            Status::OK().to_thrift(&_return);
            return;
        }
        LOG(WARNING) << "lance index dispatch rejected at enqueue: job_id=" << dispatch.job_id
                     << " invocation_id=" << dispatch.invocation_id
                     << " mutation_type=" << to_string(dispatch.mutation_type)
                     << " error=" << submit_status.to_string();
        submit_status.to_thrift(&_return);
        return;
    }
    Status::OK().to_thrift(&_return);
}

void LanceIndexJobService::_report_with_retry(
        const char* report_kind, int64_t job_id, const std::string& invocation_id,
        const std::function<Status(MasterServerClient*, TStatus*)>& attempt) {
    // finish_task discipline (task_worker_pool.cpp:153-172): 3 attempts,
    // sleep(1) between, success/failure counters. Runs synchronously on the
    // supervisor executor thread (plan §5 ruling); the worst-case stall is
    // budgeted in lance_index_worker_report_margin_seconds.
    constexpr int REPORT_MAX_RETRY = 3;
    uint32_t try_time = 0;
    while (try_time < REPORT_MAX_RETRY) {
        DorisMetrics::instance()->lance_index_job_report_requests_total->increment(1);
        TStatus status;
        MasterServerClient* client = MasterServerClient::instance();
        Status client_status =
                client == nullptr ? Status::Uninitialized("master server client is not created")
                                  : attempt(client, &status);
        if (client_status.ok()) {
            break;
        }
        DorisMetrics::instance()->lance_index_job_report_requests_failed->increment(1);
        LOG(WARNING) << "failed to report lance index job " << report_kind << ": job_id=" << job_id
                     << " invocation_id=" << invocation_id
                     << ", error=" << client_status.to_string();
        try_time += 1;
        sleep(1);
    }
    // Terminal accounting for the gauges: the worker is provably reaped
    // before any report fires, so the invocation's slot is released here
    // regardless of the report outcome (what the BE could not deliver is
    // owned by the FE deadline/epoch sweep).
    _outstanding.fetch_sub(1);
}

void LanceIndexJobService::_report_result(const TLanceIndexJobReport& report) {
    if (_stopping.load()) {
        // Stopping-drop: the RPC is skipped by design, but the invocation's
        // gauge slot is still released so the queue/inflight feeders never
        // overcount past a shutdown drain.
        LOG(INFO) << "lance service stopping; dropping result report: job_id=" << report.job_id
                  << " invocation_id=" << report.invocation_id;
        _outstanding.fetch_sub(1);
        return;
    }
    _report_with_retry("result", report.job_id, report.invocation_id,
                       [&report](MasterServerClient* client, TStatus* status) {
                           return client->report_lance_index_job(report, status);
                       });
}

void LanceIndexJobService::_report_termination(const TLanceIndexJobTerminationReport& report) {
    if (_stopping.load()) {
        LOG(INFO) << "lance service stopping; dropping termination report: job_id="
                  << report.job_id << " invocation_id=" << report.invocation_id;
        _outstanding.fetch_sub(1);
        return;
    }
    _report_with_retry("termination", report.job_id, report.invocation_id,
                       [&report](MasterServerClient* client, TStatus* status) {
                           return client->report_lance_index_job_termination(report, status);
                       });
}

void LanceIndexJobService::_release_slot_on_silent_ending(int64_t job_id,
                                                          const std::string& invocation_id) {
    // The silent-ending counterpart of _report_with_retry's terminal
    // accounting: an unprovable termination emits NO report, so no report
    // callback would ever run for this invocation and the reserved slot would
    // inflate the gauges until BE restart (with max_inflight=1 the worker
    // would read permanently busy). Only the local gauge accounting changes
    // here; the FE-side slot is untouched and converges through the
    // deadline/epoch sweep, exactly like a report the BE failed to deliver.
    LOG(INFO) << "lance invocation ended without provable termination; releasing the local "
                 "gauge slot (the FE-side slot stays with the epoch sweep): job_id="
              << job_id << " invocation_id=" << invocation_id;
    _outstanding.fetch_sub(1);
}

} // namespace lance
} // namespace doris
