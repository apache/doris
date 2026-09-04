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
#include "runtime/thread_context.h"
#include "util/debug_points.h"

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
        // Submitting another runnable would only duplicate work.
        return Status::OK();
    }
    if (!scanner_ctx->can_admit_scan_task(transfer_lock)) {
        // No runnable is needed when the Context has no pending scanner or its concurrency slots
        // are occupied. A completion only wakes the operator; the operator's next consumption in
        // get_block_from_queue(), or the successor submit in _run_context(), retries scheduling.
        return Status::OK();
    }

    if (_is_stop) {
        Status failure = Status::InternalError<false>("scanner pool {} is shutdown.", _sched_name);
        scanner_ctx->set_context_failure(failure, transfer_lock);
        return failure;
    }

    // ThreadPool::submit_func() may return an error after retaining the runnable. Set the marker
    // before submission so either outcome is safe: a retained callback clears it, while a truly
    // rejected callback leaves a terminal Context that no longer needs rescheduling.
    scanner_ctx->set_context_queued(true, transfer_lock);
    Status status =
            _scan_thread_pool->submit_func([this, scanner_ctx] { _run_context(scanner_ctx); });
    if (status.ok()) {
        VLOG_DEBUG << "submit context runnable to scanner pool " << _sched_name << ", "
                   << scanner_ctx->debug_string();
        return Status::OK();
    }
    Status failure =
            Status::TooManyTasks("Failed to submit scanner context {} to scanner pool, reason: {}",
                                 scanner_ctx->ctx_id, status.msg());
    scanner_ctx->set_context_failure(failure, transfer_lock);
    return failure;
}

void ThreadPoolSimplifiedScanScheduler::_run_context(std::shared_ptr<ScannerContext> scanner_ctx) {
    std::shared_ptr<ScanTask> scan_task;
    Status admission_status = [&]() -> Status {
        std::unique_lock<std::mutex> transfer_lock(scanner_ctx->transfer_lock());
        scanner_ctx->set_context_queued(false, transfer_lock);

        auto task_execution_lock = scanner_ctx->task_exec_ctx();
        if (task_execution_lock == nullptr) {
            return Status::OK();
        }
#ifndef BE_TEST
        // Attach before admission: allocations below (for example the resubmitted
        // FunctionRunnable) must charge the query rather than the orphan tracker. Scoped to this
        // lambda so it detaches before execute_scan_task(), whose _scanner_scan() attaches again.
        SCOPED_ATTACH_TASK(scanner_ctx->state());
#endif
        Status status = [&]() -> Status {
            RETURN_IF_CATCH_EXCEPTION({
                // Admission checks completed results, active tasks, and adaptive limits while
                // holding transfer_lock.
                scan_task = scanner_ctx->try_get_next_scan_task(transfer_lock);
                if (scan_task != nullptr) {
                    DBUG_EXECUTE_IF("ThreadPoolSimplifiedScanScheduler._run_context.inject_failure",
                                    {
                                        throw Exception(ErrorCode::INTERNAL_ERROR,
                                                        "injected admission failure");
                                    });
                    // Queue the next Context runnable before executing this task. Holding
                    // transfer_lock keeps the admission decision atomic.
                    RETURN_IF_ERROR(schedule_scan_task(scanner_ctx, nullptr, transfer_lock));
                }
            });
            return Status::OK();
        }();
        if (!status.ok() && scan_task == nullptr) {
            scanner_ctx->set_context_failure(status, transfer_lock);
        }
        return status;
    }();
    if (!admission_status.ok()) [[unlikely]] {
        if (scan_task != nullptr) {
            // The scanner was admitted before the failure. Publish it to release the in-flight
            // slot and make the error observable by the operator.
            scan_task->set_status(admission_status);
            scanner_ctx->push_completed_scan_task(scan_task);
        }
        return;
    }
    if (scan_task == nullptr) {
        return;
    }
    // The scan runs without transfer_lock so the operator and other Context workers can continue
    // consuming results and admitting work. Completion reacquires the lock before publishing.
    execute_scan_task(scanner_ctx, scan_task);
}
} // namespace doris
