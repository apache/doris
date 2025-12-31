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

#include "scanner_scheduler.h"
#include "util/timed_lock.h"
#include "vec/exec/scan/scanner_context.h"
#include "vec/exec/scan/scan_node.h"

namespace doris::vectorized {
class ScannerDelegate;
class ScanTask;

Status TaskExecutorSimplifiedScanScheduler::schedule_scan_task(
        std::shared_ptr<ScannerContext> scanner_ctx, std::shared_ptr<ScanTask> current_scan_task,
        std::unique_lock<std::mutex>& transfer_lock) {
    // std::unique_lock<std::shared_mutex> wl(_lock);
    int64_t lock_wait_time_ns = 0;
    TimedLock<std::shared_mutex> timed_lock(_lock, &lock_wait_time_ns);
    // LOG(INFO) << "yy debug lock_wait_time_ns: " << lock_wait_time_ns;
    if (current_scan_task) {
        if (auto s = current_scan_task->scanner.lock()) {
            s->_scanner->update_sched_lock_timer(lock_wait_time_ns);
        }
    }
    std::unique_lock<std::shared_mutex> wl = timed_lock.transfer_to_unique_lock();
    return scanner_ctx->schedule_scan_task(current_scan_task, transfer_lock, wl);
}

Status ThreadPoolSimplifiedScanScheduler::schedule_scan_task(
        std::shared_ptr<ScannerContext> scanner_ctx, std::shared_ptr<ScanTask> current_scan_task,
        std::unique_lock<std::mutex>& transfer_lock) {
    // std::unique_lock<std::shared_mutex> wl(_lock);
    int64_t lock_wait_time_ns = 0;
    TimedLock<std::shared_mutex> timed_lock(_lock, &lock_wait_time_ns);
    // LOG(INFO) << "yy debug 2 lock_wait_time_ns: " << lock_wait_time_ns;
    if (current_scan_task) {
        if (auto s = current_scan_task->scanner.lock()) {
            s->_scanner->update_sched_lock_timer(lock_wait_time_ns);
        }
    }
    std::unique_lock<std::shared_mutex> wl = timed_lock.transfer_to_unique_lock();
    return scanner_ctx->schedule_scan_task(current_scan_task, transfer_lock, wl);
}
} // namespace doris::vectorized
