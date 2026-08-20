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

#include <memory>

#include "common/exception.h"
#include "common/logging.h"
#include "exec/scan/scanner_context.h"
#include "exec/scan/scanner_scheduler.h"

namespace doris {
class ScannerDelegate;
class ScanTask;

Status TaskExecutorSimplifiedScanScheduler::schedule_scan_task(
        std::shared_ptr<ScannerContext> scanner_ctx, std::shared_ptr<ScanTask> current_scan_task,
        std::unique_lock<std::mutex>& transfer_lock) {
    std::unique_lock<std::shared_mutex> wl(_lock);
    return scanner_ctx->schedule_scan_task(current_scan_task, transfer_lock, wl);
}

Status ThreadPoolSimplifiedScanScheduler::schedule_scan_task(
        std::shared_ptr<ScannerContext> scanner_ctx, std::shared_ptr<ScanTask> current_scan_task,
        std::unique_lock<std::mutex>& transfer_lock) {
    // Unlike TaskExecutor, ThreadPool queues a Context runnable. It later admits one pending task
    // under transfer_lock. This bounds queue entries to one per Context even when many scanners
    // become runnable together.
    DORIS_CHECK(transfer_lock.owns_lock());
    if (current_scan_task != nullptr) {
        // The operator has consumed a non-EOS result, making this scanner eligible for another
        // scan attempt. Queue the scanner first; the Context runnable chooses it later.
        scanner_ctx->push_pending_scan_task(std::move(current_scan_task), transfer_lock);
    }
    if (scanner_ctx->is_context_queued(transfer_lock)) {
        // A queued runnable will see all pending scanners added before it obtains transfer_lock.
        // Submitting another runnable would only duplicate work and distort Context queue latency.
        return Status::OK();
    }

    // transfer_lock prevents another producer from submitting concurrently. The worker callback
    // also waits for this lock, so it cannot run between successful submission and marking queued.
    Status status;
    if (_is_stop) {
        status = Status::InternalError<false>("scanner pool {} is shutdown.", _sched_name);
    } else {
        status = _scan_thread_pool->submit_func([this, scanner_ctx] { _run_context(scanner_ctx); });
    }
    if (status.ok()) {
        // Start the Context wait interval only after submission succeeds. This excludes failed
        // submit_func() calls, which never waited for a worker and must not affect the profile.
        scanner_ctx->set_context_queued(true, transfer_lock);
    } else {
        // No worker can dequeue a rejected runnable. The Context remains unqueued, so a later
        // scheduling attempt can submit it again without clearing state or accounting queue time.
        LOG(WARNING) << fmt::format("Failed to submit scanner context {}, reason: {}",
                                    scanner_ctx->debug_string(), status.to_string());
    }
    return status;
}

void ThreadPoolSimplifiedScanScheduler::_run_context(std::shared_ptr<ScannerContext> scanner_ctx) {
    std::shared_ptr<ScanTask> scan_task;
    {
        std::unique_lock<std::mutex> transfer_lock(scanner_ctx->transfer_lock());
        // The worker has dequeued the Context. Clearing the marker also charges its queue latency:
        // the interval from successful submit_func() to worker start, not scanner execution time.
        scanner_ctx->set_context_queued(false, transfer_lock);

        auto task_execution_lock = scanner_ctx->task_exec_ctx();
        if (task_execution_lock == nullptr) {
            return;
        }

        // Admission checks completed results, active tasks, adaptive limits, and shared LIMIT while
        // holding transfer_lock. A null task means the Context is currently not allowed to run one.
        scan_task = scanner_ctx->try_get_next_scan_task(transfer_lock);
        if (scan_task == nullptr) {
            return;
        }

        // Queue the next Context runnable before executing this task. Example: with a concurrency
        // limit of two, the next worker may admit scanner B while this worker scans scanner A.
        // Releasing transfer_lock only after resubmission keeps the admission decision atomic.
        Status resubmit_status = schedule_scan_task(scanner_ctx, nullptr, transfer_lock);
        if (!resubmit_status.ok()) {
            LOG(WARNING) << fmt::format("Failed to resubmit scanner context {}, reason: {}",
                                        scanner_ctx->ctx_id, resubmit_status.to_string());
        }
    }
    // The scan runs without transfer_lock so the operator and other Context workers can continue
    // consuming results and admitting work. Completion reacquires the lock before publishing.
    execute_scan_task(scanner_ctx, scan_task);
}
} // namespace doris
