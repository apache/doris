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

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

#include <pthread.h>

#include "agent/heartbeat_server.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "lance/index/lance_index_worker.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris::lance {
namespace {

// Queue depth and in-flight execution gauges (N3), pull-fed from the service's
// own accounting via the REGISTER_HOOK_METRIC pattern (pr4a precedent).
DEFINE_GAUGE_METRIC_PROTOTYPE_3ARG(lance_index_mutation_queue_depth, MetricUnit::NOUNIT,
                                   "Lance index mutations waiting in the bounded receive queue");
DEFINE_GAUGE_METRIC_PROTOTYPE_3ARG(lance_index_mutation_inflight_workers, MetricUnit::NOUNIT,
                                   "Lance index worker executions currently in flight");

// Outcome counters (N3): the phase x code-band table of the D8 answer shapes.
// Fixed names, no labels - no invocation id, index name, or any user data ever
// rides a metric.
DEFINE_COUNTER_METRIC_PROTOTYPE_3ARG(
        lance_index_mutation_outcome_never_launched, MetricUnit::NOUNIT,
        "Lance index mutations refused before launch (BE-level never-launched)");
DEFINE_COUNTER_METRIC_PROTOTYPE_3ARG(
        lance_index_mutation_outcome_pre_invocation_rejected, MetricUnit::NOUNIT,
        "Lance index mutations rejected by the worker before the mutating native call");
DEFINE_COUNTER_METRIC_PROTOTYPE_3ARG(
        lance_index_mutation_outcome_native_ok, MetricUnit::NOUNIT,
        "Lance index mutations that committed (or proved a no-op) with native code 0");
DEFINE_COUNTER_METRIC_PROTOTYPE_3ARG(
        lance_index_mutation_outcome_native_error, MetricUnit::NOUNIT,
        "Lance index mutations that returned a non-zero native lance error code");
DEFINE_COUNTER_METRIC_PROTOTYPE_3ARG(
        lance_index_mutation_outcome_indeterminate, MetricUnit::NOUNIT,
        "Lance index mutations with no trusted result after launch (worker loss, ambiguity, "
        "duplicate delivery)");

// D4 payload limits, checked at receipt so an oversized request fails fast
// instead of riding the queue into the supervisor's frame bound. Error
// messages carry sizes only - never a storage-option key or value.
constexpr size_t MAX_STORAGE_OPTIONS = 64;
constexpr size_t MAX_STORAGE_OPTION_KEY_BYTES = 256;
constexpr size_t MAX_STORAGE_OPTION_VALUE_BYTES = 4096;

// The dequeue-rail monotonic runway cap: the supervisor's wall-clock ceiling
// (lance_index_worker_wallclock_limit_seconds) caps actual execution anyway,
// so a farther runway only needs to stay unambiguous under saturating adds.
constexpr int64_t MAX_MONO_RUNWAY_MS = 24LL * 60 * 60 * 1000;

const char* mutation_type_name(TLanceIndexMutationType::type type) {
    switch (type) {
    case TLanceIndexMutationType::CREATE:
        return "CREATE";
    case TLanceIndexMutationType::REPLACE:
        return "REPLACE";
    case TLanceIndexMutationType::DROP:
        return "DROP";
    }
    return "UNKNOWN";
}

} // namespace

LanceIndexWorkerService::LanceIndexWorkerService() = default;

LanceIndexWorkerService::~LanceIndexWorkerService() {
    stop();
}

Status LanceIndexWorkerService::start() {
    if (_started.load()) {
        return Status::OK();
    }
    if (_stopping.load()) {
        return Status::Cancelled("lance index worker service is stopping");
    }
    // Structural knobs are sampled once (pr4a max_inflight/queue_size
    // precedent): the executor threads ARE the two-state slots and the queue
    // capacity is fixed at construction.
    const int32_t concurrency = std::clamp<int32_t>(
            config::lance_index_mutation_worker_max_concurrency, 1, 64);
    const int32_t queue_capacity =
            std::clamp<int32_t>(config::lance_index_mutation_worker_max_queued, 1, 1024);
    _queue = std::make_unique<BlockingQueue<PendingPtr>>(
            static_cast<uint32_t>(queue_capacity));
    try {
        _executors.reserve(concurrency);
        for (int i = 0; i < concurrency; ++i) {
            _executors.emplace_back([this]() { _executor_loop(); });
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "lance index worker service could not spawn executor threads: "
                     << e.what();
        // Refuse permanently: a half-spawned pool must never serve. The
        // already-spawned executors drain their (empty) queue and exit.
        _stopping.store(true);
        _queue->shutdown();
        for (std::thread& executor : _executors) {
            if (executor.joinable()) {
                executor.join();
            }
        }
        _executors.clear();
        _queue.reset();
        return Status::InternalError(
                "lance index worker service could not spawn executor threads: {}", e.what());
    }

    // Metric registration spelled out (the REGISTER_HOOK_METRIC shorthand
    // requires DorisMetrics members; these metrics belong to this component).
    auto* entity = DorisMetrics::instance()->server_entity();
    _gauge_queue_depth = static_cast<UIntGauge*>(
            entity->register_metric<UIntGauge>(&METRIC_lance_index_mutation_queue_depth));
    entity->register_hook("lance_index_mutation_queue_depth", [this]() {
        _gauge_queue_depth->set_value(static_cast<uint64_t>(queue_depth()));
    });
    _gauge_inflight_workers = static_cast<UIntGauge*>(
            entity->register_metric<UIntGauge>(&METRIC_lance_index_mutation_inflight_workers));
    entity->register_hook("lance_index_mutation_inflight_workers", [this]() {
        _gauge_inflight_workers->set_value(static_cast<uint64_t>(inflight_workers()));
    });
    _metric_never_launched = static_cast<IntCounter*>(entity->register_metric<IntCounter>(
            &METRIC_lance_index_mutation_outcome_never_launched));
    _metric_pre_invocation_rejected = static_cast<IntCounter*>(
            entity->register_metric<IntCounter>(
                    &METRIC_lance_index_mutation_outcome_pre_invocation_rejected));
    _metric_native_ok = static_cast<IntCounter*>(
            entity->register_metric<IntCounter>(&METRIC_lance_index_mutation_outcome_native_ok));
    _metric_native_error = static_cast<IntCounter*>(
            entity->register_metric<IntCounter>(&METRIC_lance_index_mutation_outcome_native_error));
    _metric_indeterminate = static_cast<IntCounter*>(
            entity->register_metric<IntCounter>(
                    &METRIC_lance_index_mutation_outcome_indeterminate));
    _metrics_registered = true;

    // Startup isolation probe: a failure is NOT fatal for BE startup - the
    // supervisor logs the precise step, isolation stays unverified and every
    // invocation is rejected with a never-launched answer (no soft fallback).
    static_cast<void>(_supervisor.preflight());

    _started.store(true);
    LOG(INFO) << "lance index worker service started: executors=" << concurrency
              << " queue_capacity=" << queue_capacity
              << " isolation_verified=" << _supervisor.isolation_verified();
    return Status::OK();
}

void LanceIndexWorkerService::stop() {
    if (!_started.load() && _executors.empty() && _queue == nullptr) {
        _stopping.store(true);
        return;
    }
    if (_stopping.exchange(true)) {
        return;
    }
    // 1. Refuse every queued entry: the shutdown wakes the executors and each
    //    buffered entry is answered (never-launched) by the drain branch of
    //    the executor loop - a handler thread never waits past stop().
    if (_queue != nullptr) {
        _queue->shutdown();
    }
    // 2. Close the supervisor's launch gate (a late launch would escape the
    //    kill snapshot) and accelerate the in-flight kill ladders.
    _supervisor.stop();
    // 3. Join the executors: they converge on their own threads, bounded by
    //    their kill ladders (the in-flight execute() calls own their threads).
    for (std::thread& executor : _executors) {
        if (executor.joinable()) {
            executor.join();
        }
    }
    _executors.clear();
    if (_metrics_registered) {
        auto* entity = DorisMetrics::instance()->server_entity();
        // The pull hooks capture `this` and must come off before the service
        // object dies; the gauges they feed deregister with them (nothing but
        // a hook ever touches a gauge).
        entity->deregister_hook("lance_index_mutation_queue_depth");
        entity->deregister_hook("lance_index_mutation_inflight_workers");
        entity->deregister_metric(&METRIC_lance_index_mutation_queue_depth);
        entity->deregister_metric(&METRIC_lance_index_mutation_inflight_workers);
        _gauge_queue_depth = nullptr;
        _gauge_inflight_workers = nullptr;
        // The outcome counters are deliberately NOT deregistered and the
        // member pointers deliberately stay live: stop() joins the executors
        // but never the thrift handler threads, and an RPC racing the
        // shutdown still bumps a never-launched/indeterminate bucket through
        // _bump - deregister_metric deletes the object, which would turn that
        // benign race into a use-after-free. The entity owns the counters
        // for its (process) lifetime, the same lifetime every other
        // DorisMetrics counter has; a later start() re-registering the same
        // prototypes simply receives the same objects back.
        _metrics_registered = false;
    }
    LOG(INFO) << "lance index worker service stopped";
}

int64_t LanceIndexWorkerService::queue_depth() const {
    return _queue != nullptr ? static_cast<int64_t>(_queue->get_size()) : 0;
}

int64_t LanceIndexWorkerService::inflight_workers() const {
    return _inflight_executions.load();
}

int64_t LanceIndexWorkerService::_be_process_epoch() const {
    // The FE echoes the heartbeat-reported be_start_time, so the receipt check
    // must compare against exactly that number (heartbeat_server.cpp sets it
    // from the same formula). 0 = no heartbeat server constructed yet, which
    // no valid FE request can predate.
    if (const int64_t forced = _forced_be_epoch.load(); forced != 0) {
        return forced;
    }
    return HeartbeatServer::process_epoch();
}

Status LanceIndexWorkerService::_validate_request(const TLanceIndexMutationRequest& request) const {
    // Structural and identity checks only. The deep semantic layer (D11 local
    // dataset rejection, properties/contract replay, IF semantics) is the
    // isolated worker's pre-invocation band (phase 1), deliberately not
    // duplicated here: the receive rail proves the request is well-formed
    // enough to enqueue, the worker proves it is safe to execute.
    //
    // Required-field presence is enforced by the thrift decode itself (a
    // frame missing a required field never reaches this handler), so this
    // rail checks the required fields' VALUES plus the optional fields'
    // cardinality (__isset exists only for optional fields in the generated
    // code).
    if (request.invocation_id.empty()) {
        return Status::InvalidArgument("lance index mutation request has an empty invocation_id");
    }
    if (request.mutation_type != TLanceIndexMutationType::CREATE &&
        request.mutation_type != TLanceIndexMutationType::REPLACE &&
        request.mutation_type != TLanceIndexMutationType::DROP) {
        return Status::InvalidArgument("lance index mutation request carries an unknown type");
    }
    if (request.deadline_ms <= 0) {
        return Status::InvalidArgument(
                "lance index mutation request carries a non-positive deadline_ms");
    }
    // Version 0 is the storage-latest sentinel; the pinned open never uses it.
    if (request.admitted_dataset_version < 1) {
        return Status::InvalidArgument(
                "lance index mutation request carries a non-positive admitted_dataset_version");
    }
    if (request.dataset_uri.empty() || request.index_name.empty()) {
        return Status::InvalidArgument(
                "lance index mutation request carries an empty dataset locator or index name");
    }
    if (request.__isset.storage_options) {
        if (request.storage_options.size() > MAX_STORAGE_OPTIONS) {
            return Status::InvalidArgument(
                    "lance index mutation request carries too many storage options: {} (limit {})",
                    request.storage_options.size(), MAX_STORAGE_OPTIONS);
        }
        for (const auto& option : request.storage_options) {
            if (option.first.size() > MAX_STORAGE_OPTION_KEY_BYTES ||
                option.second.size() > MAX_STORAGE_OPTION_VALUE_BYTES) {
                return Status::InvalidArgument(
                        "lance index mutation request storage option exceeds the key/value size "
                        "limit (key <= {} bytes, value <= {} bytes)",
                        MAX_STORAGE_OPTION_KEY_BYTES, MAX_STORAGE_OPTION_VALUE_BYTES);
            }
        }
    }
    return Status::OK();
}

LanceIndexWorkerService::DedupDecision
LanceIndexWorkerService::_admit_invocation_id(const TLanceIndexMutationRequest& request) {
    // thrift :414-418, the BE half of the single-send contract: one
    // invocation_id is one execution. The id is retained until the request
    // deadline plus a bounded window (pr4a supervisor dedup logic) so a late
    // redelivery of a completed invocation can never re-execute external side
    // effects. The retention saturates: a pathological deadline near INT64_MAX
    // must not wrap into the past, and the horizon cap bounds even the
    // saturated value so no id is retained forever.
    const int64_t now = UnixMillis();
    const int64_t unbounded_erase_after =
            request.deadline_ms > std::numeric_limits<int64_t>::max() - kDuplicateRetentionMs
                    ? std::numeric_limits<int64_t>::max()
                    : request.deadline_ms + kDuplicateRetentionMs;
    const int64_t erase_after =
            std::min(unbounded_erase_after, now + kDuplicateRetentionHorizonMs);
    const int64_t forced_cap = _forced_dedup_max_entries.load();
    const size_t max_entries = forced_cap > 0 ? static_cast<size_t>(forced_cap) : kMaxDedupEntries;
    std::lock_guard<std::mutex> lock(_dedup_mutex);
    for (auto it = _dedup_erase_after_ms.begin(); it != _dedup_erase_after_ms.end();) {
        it = it->second <= now ? _dedup_erase_after_ms.erase(it) : std::next(it);
    }
    if (_dedup_erase_after_ms.count(request.invocation_id) != 0) {
        return DedupDecision::DUPLICATE;
    }
    if (_dedup_erase_after_ms.size() >= max_entries) {
        // The set is rate-driven otherwise; at the cap a fresh id is refused
        // as backpressure (the queue-full rail's twin), never executed and
        // never retained - a corrected retry of this id stays free.
        return DedupDecision::SATURATED;
    }
    _dedup_erase_after_ms.emplace(request.invocation_id, erase_after);
    return DedupDecision::ADMITTED;
}

void LanceIndexWorkerService::_forget_invocation_id(const std::string& invocation_id) {
    // A request rejected after the dedup admission (budget rail, full queue)
    // never executed, so its id must not block a legitimate later retry by a
    // corrected sender.
    std::lock_guard<std::mutex> lock(_dedup_mutex);
    _dedup_erase_after_ms.erase(invocation_id);
}

void LanceIndexWorkerService::_reject_never_launched(TLanceIndexMutationResult& result,
                                                     const TLanceIndexMutationRequest& request,
                                                     const Status& status) {
    status.to_thrift(&result.status);
    result.__set_invocation_id(request.invocation_id);
    result.__set_be_process_epoch(request.be_process_epoch);
    _count_outcome_never_launched();
}

void LanceIndexWorkerService::_reject_duplicate(TLanceIndexMutationResult& result,
                                                const TLanceIndexMutationRequest& request) {
    Status::OK().to_thrift(&result.status);
    result.__set_worker_phase(TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
    result.__set_message(
            "duplicate invocation_id refused: the single-send contract allows one delivery per "
            "invocation and this delivery was not executed; the first delivery's answer went to "
            "its own connection");
    result.__set_invocation_id(request.invocation_id);
    result.__set_be_process_epoch(request.be_process_epoch);
    _count_outcome_indeterminate();
}

void LanceIndexWorkerService::lance_index_mutate(TLanceIndexMutationResult& result,
                                                 const TLanceIndexMutationRequest& request) {
    // Rail 1: lifecycle. Not started = the BE answered before its own startup
    // finished (a defensive refusal, the thrift server starts after start()).
    if (!_started.load() || _stopping.load()) {
        LOG(WARNING) << "rejecting lance index mutation before the receive rail: the service is "
                        "not accepting invocations: invocation_id="
                     << request.invocation_id;
        _reject_never_launched(result, request,
                               Status::Cancelled(
                                       "lance index worker service is not accepting invocations "
                                       "(not started or stopping)"));
        return;
    }

    // Rail 2: per-field structural validation.
    Status validation = _validate_request(request);
    if (!validation.ok()) {
        LOG(WARNING) << "rejecting lance index mutation: request validation failed: "
                        "invocation_id="
                     << request.invocation_id << " mutation_type="
                     << static_cast<int>(request.mutation_type)
                     << " error=" << validation.to_string();
        _reject_never_launched(result, request, validation);
        return;
    }

    // Rail 3: epoch receipt check (N2). The FE mints field 2 from the
    // heartbeat-reported be_start_time of the process incarnation it selected;
    // a mismatch means the request was minted for another incarnation (e.g.
    // this BE restarted after the FE's observation) and must not execute.
    const int64_t be_epoch = _be_process_epoch();
    if (be_epoch == 0 || request.be_process_epoch != be_epoch) {
        LOG(WARNING) << "rejecting lance index mutation: be process epoch mismatch: "
                        "invocation_id="
                     << request.invocation_id << " mutation_type="
                     << mutation_type_name(request.mutation_type)
                     << " request_epoch=" << request.be_process_epoch
                     << " process_epoch=" << be_epoch;
        _reject_never_launched(
                result, request,
                Status::InvalidArgument(
                        "lance index mutation request carries a be_process_epoch that does not "
                        "match this backend process (stale incarnation or forged identity)"));
        return;
    }

    // Rail 4: dedup (the BE half of the single-send contract).
    const DedupDecision dedup = _admit_invocation_id(request);
    if (dedup == DedupDecision::DUPLICATE) {
        LOG(WARNING) << "lance index mutation redelivered (single-send contract violation, not "
                        "re-executing): invocation_id="
                     << request.invocation_id << " mutation_type="
                     << mutation_type_name(request.mutation_type);
        _reject_duplicate(result, request);
        return;
    }
    if (dedup == DedupDecision::SATURATED) {
        LOG(WARNING) << "rejecting lance index mutation: dedup guard saturated: invocation_id="
                     << request.invocation_id << " mutation_type="
                     << mutation_type_name(request.mutation_type);
        _reject_never_launched(
                result, request,
                Status::TooManyTasks(
                        "lance index mutation dedup guard is saturated (too many distinct "
                        "invocations retained); refusing before enqueue"));
        return;
    }
    auto forget_on_reject = [this, &request]() { _forget_invocation_id(request.invocation_id); };

    // Rail 5: D3 receive rail. remaining <= RESERVATION + MIN_RUN means even a
    // minimal worker run cannot complete inside the kill reservation, so the
    // invocation is refused at receipt instead of being launched into certain
    // expiry. Constructively consistent with the dequeue rail below (same four
    // constants) and with the supervisor's own wall-clock twin.
    const int64_t remaining_ms = request.deadline_ms - UnixMillis();
    if (remaining_ms <= kKillReservationMs + kMinWorkerRunMs) {
        LOG(WARNING) << "rejecting lance index mutation: deadline budget exhausted on arrival: "
                        "invocation_id="
                     << request.invocation_id << " mutation_type="
                     << mutation_type_name(request.mutation_type)
                     << " remaining_ms=" << remaining_ms;
        forget_on_reject();
        _reject_never_launched(result, request,
                               Status::Cancelled(
                                       "lance index mutation deadline budget is already exhausted "
                                       "on arrival (remaining budget does not cover the kill "
                                       "reservation and the minimum worker run)"));
        return;
    }

    // D3 local conversion, computed once and never reset: the kill deadline in
    // both clocks. The wall form is what execute() enforces; the monotonic
    // form is what the dequeue rail re-checks after the queue wait.
    auto pending = std::make_shared<PendingInvocation>();
    pending->request = request;
    pending->kill_deadline_ms = request.deadline_ms - kKillReservationMs;
    pending->kill_deadline_mono =
            MonotonicMillis() + std::min(remaining_ms - kKillReservationMs, MAX_MONO_RUNWAY_MS);

    // Rail 6: bounded queue. Full = a never-launched refusal (the FE's
    // admission bounded its own side; a full BE queue is backpressure, not a
    // reason to execute later past the budget).
    if (!_queue->try_put(pending)) {
        forget_on_reject();
        const bool stopping_now = _stopping.load();
        LOG(WARNING) << "rejecting lance index mutation at enqueue: invocation_id="
                     << request.invocation_id << " mutation_type="
                     << mutation_type_name(request.mutation_type) << " stopping=" << stopping_now;
        _reject_never_launched(
                result, request,
                stopping_now ? Status::Cancelled("lance index worker service is stopping; "
                                                  "refusing the queued invocation")
                             : Status::TooManyTasks(
                                       "lance index mutation receive queue is full (capacity {})",
                                       _queue->get_capacity()));
        return;
    }

    // The synchronous handoff: block until an executor answers. Every dequeued
    // entry is answered by construction (stop drains with rejections, the
    // budget rails reject, execute() always returns), and on the executed
    // path the answer is set only after the execution domain's termination
    // was confirmed (respond-after-reap, D10) - so this wait is bounded by the
    // request's own budget, never by an unwatched worker.
    try {
        result = pending->answer.get_future().get();
    } catch (const std::exception& e) {
        // Unreachable by construction (the local `pending` co-owns the
        // promise); kept so the handler can never throw past the thrift
        // boundary even if that invariant ever regresses.
        LOG(ERROR) << "lance index mutation answer channel failed: invocation_id="
                   << request.invocation_id << " error=" << e.what();
        _reject_never_launched(result, request,
                               Status::InternalError(
                                       "lance index mutation answer channel failed"));
    }
}

void LanceIndexWorkerService::_executor_loop() {
#if defined(__linux__)
    ::pthread_setname_np(::pthread_self(), "lance-idx-mut");
#endif
    for (;;) {
        PendingPtr pending;
        if (!_queue->blocking_get(&pending)) {
            return; // queue shut down and drained: stop() owns the lifecycle
        }
        // Exception guard (the handler's "Never throws" contract extends to
        // the executor side by thread, not by type): a bad_alloc in the
        // thrift copies/encodes or any other escape must never cross the
        // thread entry - that is std::terminate and takes the whole BE down.
        // The guarded work always answers, so the waiting handler is freed on
        // every path and the loop stays alive for the next entry.
        try {
            if (_stopping.load()) {
                // Drain-and-refuse (N4): a buffered entry must never start a
                // worker past the stop (a late launch would escape the kill
                // snapshot). It provably never executed, so the waiting
                // handler gets a never-launched answer, never an endless
                // wait.
                TLanceIndexMutationResult result;
                _reject_never_launched(
                        result, pending->request,
                        Status::Cancelled("lance index worker service is stopping; refusing a "
                                          "queued invocation"));
                _run_executor_answer(*pending, std::move(result));
                continue;
            }
            _execute_pending(pending);
        } catch (const std::exception& e) {
            _answer_after_executor_exception(pending, e.what());
        } catch (...) {
            _answer_after_executor_exception(pending, "an exception of an unknown type");
        }
    }
}

void LanceIndexWorkerService::_answer_after_executor_exception(const PendingPtr& pending,
                                                               const char* what) {
    LOG(ERROR) << "lance index mutation executor path failed: invocation_id="
               << pending->request.invocation_id << " error=" << what;
    TLanceIndexMutationResult result;
    Status::OK().to_thrift(&result.status);
    result.__set_worker_phase(TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
    result.__set_message(
            "lance index mutation executor failed before it could answer; the invocation's "
            "outcome is unknown to this backend");
    result.__set_invocation_id(pending->request.invocation_id);
    result.__set_be_process_epoch(pending->request.be_process_epoch);
    _count_outcome_indeterminate();
    try {
        _run_executor_answer(*pending, std::move(result));
    } catch (...) {
        // Only reachable if the answer contract itself regressed; never
        // rethrow past the thread entry.
        LOG(ERROR) << "lance index mutation answer channel failed while reporting an executor "
                      "exception: invocation_id="
                   << pending->request.invocation_id;
    }
}

void LanceIndexWorkerService::_execute_pending(const PendingPtr& pending) {
    const TLanceIndexMutationRequest& request = pending->request;

    // Dequeue rail (D3, the second of the double-reject pair): the queue wait
    // consumed budget, and an invocation whose remaining kill budget cannot
    // fund a minimal worker run is rejected instead of launched into certain
    // expiry. The supervisor re-checks the same invariant against the wall
    // clock inside execute() - three constructive-consistent rails.
    // The dedup entry is deliberately retained here, unlike the receive-rail
    // rejections that forget: the invocation provably never executed either
    // way, but this id has already entered the system, under the single-send
    // contract no legitimate redelivery exists, and keeping every mutation of
    // the dedup map on the receive rail (handler threads) is simpler to
    // reason about. The asymmetry only ever costs the bounded retention
    // window - the conservative direction.
    if (MonotonicMillis() + kMinWorkerRunMs > pending->kill_deadline_mono) {
        LOG(WARNING) << "rejecting lance index mutation at dequeue: deadline budget exhausted "
                        "after the queue wait: invocation_id="
                     << request.invocation_id << " mutation_type="
                     << mutation_type_name(request.mutation_type);
        TLanceIndexMutationResult result;
        _reject_never_launched(
                result, request,
                Status::Cancelled("lance index mutation deadline budget was exhausted while "
                                  "queued; refusing before launch"));
        _run_executor_answer(*pending, std::move(result));
        return;
    }

    // Fault-injection point (the executor exception guard): throws past the
    // execution path the way a bad_alloc in the dispatch build would - the
    // executor loop's guard must answer the waiting handler and keep the
    // thread alive. Deliberately after the dequeue rail and before any
    // reservation or supervisor work: nothing has launched.
    DBUG_EXECUTE_IF("LanceIndexWorkerService.executor_throw",
                    { throw std::runtime_error("injected executor exception"); });

    // Two-state slot, `reserved` state (D4): the invocation passed the dequeue
    // rail and owns this executor thread until its slot is released. A
    // reserved slot that fails before launch is released directly here; a
    // launched slot is released only after the supervisor confirms the
    // execution domain's termination (execute() returning).
    _reserved_slots.fetch_add(1);
    Defer reserved_guard([this]() { _reserved_slots.fetch_sub(1); });

    if (!_supervisor.isolation_verified()) {
        LOG(WARNING) << "rejecting lance index mutation at dequeue: worker isolation is not "
                        "verified: invocation_id="
                     << request.invocation_id << " mutation_type="
                     << mutation_type_name(request.mutation_type);
        TLanceIndexMutationResult result;
        _reject_never_launched(result, request,
                               Status::CgroupError(
                                       "lance index worker isolation is not verified (startup "
                                       "preflight failed or was skipped); rejecting invocation"));
        _run_executor_answer(*pending, std::move(result));
        return;
    }

    const TLanceIndexWorkerDispatch dispatch = _build_dispatch(request);

    // `launched` state: execute() owns the domain from here. Its non-OK return
    // is a provable never-exec'd; its OK return is the respond-after-reap
    // boundary - the classification below runs only after the kill ladder, the
    // bounded reap and the populated wait completed.
    WorkerExecution execution;
    Status status;
    _inflight_executions.fetch_add(1);
    Defer inflight_guard([this]() { _inflight_executions.fetch_sub(1); });
    status = _supervisor.execute(dispatch, pending->kill_deadline_ms, &execution);
    if (status.ok()) {
        _launched_total.fetch_add(1);
    }

    TLanceIndexMutationResult result;
    _fill_from_execution(result, request, status, execution);
    _run_executor_answer(*pending, std::move(result));
}

void LanceIndexWorkerService::_fill_from_execution(TLanceIndexMutationResult& result,
                                                   const TLanceIndexMutationRequest& request,
                                                   const Status& status,
                                                   const WorkerExecution& execution) {
    if (!status.ok()) {
        // BE-level never-launched (the supervisor refused before any fork
        // work): the wire's confirmed-failure path. The status message is the
        // supervisor's sanitized static category.
        status.to_thrift(&result.status);
        result.__set_invocation_id(request.invocation_id);
        result.__set_be_process_epoch(request.be_process_epoch);
        _count_outcome_never_launched();
        return;
    }

    Status::OK().to_thrift(&result.status);
    switch (execution.kind) {
    case WorkerExecution::Kind::TRUSTED_RESULT: {
        // Phase/code verbatim from the validated frame; the phase band was
        // already enforced by the supervisor's frame contract.
        const TLanceIndexWorkerResult& frame = *execution.result;
        const bool pre_invocation =
                frame.phase == TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED;
        result.__set_worker_phase(pre_invocation ? TLanceIndexWorkerPhase::PRE_INVOCATION_REJECTED
                                                 : TLanceIndexWorkerPhase::NATIVE_RETURN);
        result.__set_lance_error_code(frame.lance_error_code);
        if (!execution.message.empty()) {
            result.__set_message(execution.message);
        }
        if (pre_invocation && frame.__isset.observed_latest_version) {
            result.__set_observed_latest_version(frame.observed_latest_version);
        }
        if (pre_invocation) {
            _count_outcome_pre_invocation_rejected();
        } else if (frame.lance_error_code == 0) {
            _count_outcome_native_ok();
        } else {
            _count_outcome_native_error();
        }
        break;
    }
    case WorkerExecution::Kind::PROVEN_PRE_INVOCATION_REJECTION: {
        // The supervisor proved a pre-FFI violation WITH a reap proof: phase 1
        // with the resource-rejected code from the synthesized frame.
        result.__set_worker_phase(TLanceIndexWorkerPhase::PRE_INVOCATION_REJECTED);
        result.__set_lance_error_code(execution.result->lance_error_code);
        if (!execution.message.empty()) {
            result.__set_message(execution.message);
        }
        _count_outcome_pre_invocation_rejected();
        break;
    }
    case WorkerExecution::Kind::INDETERMINATE:
        // No trusted result exists: phase 3, no code - never a guess.
        result.__set_worker_phase(TLanceIndexWorkerPhase::INDETERMINATE_AFTER_LAUNCH);
        if (!execution.message.empty()) {
            result.__set_message(execution.message);
        }
        _count_outcome_indeterminate();
        break;
    }
    result.__set_invocation_id(request.invocation_id);
    result.__set_be_process_epoch(request.be_process_epoch);
}

TLanceIndexWorkerDispatch LanceIndexWorkerService::_build_dispatch(
        const TLanceIndexMutationRequest& request) const {
    // The explicit per-field mirror (never a thrift include): fields 1-15 copy
    // the semantic surface of the wire request, fields 16+ carry the facts the
    // worker cannot read anywhere else (it starts before any BE config).
    TLanceIndexWorkerDispatch dispatch;
    dispatch.invocation_id = request.invocation_id;
    dispatch.be_process_epoch = request.be_process_epoch;
    dispatch.deadline_ms = request.deadline_ms;
    switch (request.mutation_type) {
    case TLanceIndexMutationType::CREATE:
        dispatch.mutation_type = TLanceIndexWorkerMutationType::CREATE;
        break;
    case TLanceIndexMutationType::REPLACE:
        dispatch.mutation_type = TLanceIndexWorkerMutationType::REPLACE;
        break;
    case TLanceIndexMutationType::DROP:
        dispatch.mutation_type = TLanceIndexWorkerMutationType::DROP;
        break;
    }
    dispatch.index_name = request.index_name;
    dispatch.column_name = request.column_name;
    dispatch.index_type = request.index_type;
    if (request.__isset.properties_json) {
        dispatch.__set_properties_json(request.properties_json);
    }
    if (request.__isset.if_not_exists) {
        dispatch.__set_if_not_exists(request.if_not_exists);
    }
    if (request.__isset.if_exists) {
        dispatch.__set_if_exists(request.if_exists);
    }
    dispatch.dataset_uri = request.dataset_uri;
    dispatch.admitted_dataset_version = request.admitted_dataset_version;
    if (request.__isset.schema_contract_json) {
        dispatch.__set_schema_contract_json(request.schema_contract_json);
    }
    if (request.__isset.storage_options) {
        dispatch.__set_storage_options(request.storage_options);
    }
    if (request.__isset.index_display_name) {
        dispatch.__set_index_display_name(request.index_display_name);
    }
    // Field 16/17: the admitted bounds snapshot from the BE config mirrors.
    dispatch.__set_max_num_partitions(config::lance_index_max_num_partitions);
    dispatch.__set_max_num_sub_vectors(config::lance_index_max_num_sub_vectors);
    // Field 18: names-only, armed-then-dispatched snapshot of the fixed
    // worker-fault allowlist (D11). Only the names cross the exec boundary;
    // the supervisor applies worker_debug_points_env_value before anything
    // reaches the child environment. Unset when nothing is armed.
    std::vector<std::string> debug_points;
    for (const char* name : {WORKER_DEBUG_POINT_HANG, WORKER_DEBUG_POINT_SKIP_RESULT}) {
        if (DebugPoints::instance()->get_debug_point(name) != nullptr) {
            debug_points.emplace_back(name);
        }
    }
    if (!debug_points.empty()) {
        dispatch.__set_debug_points(std::move(debug_points));
    }
    // Field 19 (worker_deadline_ms) is stamped by the supervisor from the kill
    // deadline handed to execute().
    return dispatch;
}

void LanceIndexWorkerService::_run_executor_answer(PendingInvocation& pending,
                                                   TLanceIndexMutationResult result) {
    pending.answer.set_value(std::move(result));
}

void LanceIndexWorkerService::_bump(IntCounter* metric, std::atomic<int64_t>& counter) {
    counter.fetch_add(1);
    if (metric != nullptr) {
        metric->increment(1);
    }
}

void LanceIndexWorkerService::_count_outcome_never_launched() {
    _bump(_metric_never_launched, _never_launched_total);
}

void LanceIndexWorkerService::_count_outcome_pre_invocation_rejected() {
    _bump(_metric_pre_invocation_rejected, _pre_invocation_rejected_total);
}

void LanceIndexWorkerService::_count_outcome_native_ok() {
    _bump(_metric_native_ok, _native_ok_total);
}

void LanceIndexWorkerService::_count_outcome_native_error() {
    _bump(_metric_native_error, _native_error_total);
}

void LanceIndexWorkerService::_count_outcome_indeterminate() {
    _bump(_metric_indeterminate, _indeterminate_total);
}

} // namespace doris::lance
