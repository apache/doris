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

#include "exec/scan/scanner_scheduler.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/scan/file_scanner.h"
#include "exec/scan/olap_scanner.h" // IWYU pragma: keep
#include "exec/scan/scan_node.h"
#include "exec/scan/scanner.h"
#include "exec/scan/scanner_context.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "storage/tablet/tablet.h"
#include "util/async_io.h" // IWYU pragma: keep
#include "util/cpu_info.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/threadpool.h"

namespace doris {

Status ScannerScheduler::submit(std::shared_ptr<ScannerContext> ctx,
                                std::shared_ptr<ScanTask> scan_task) {
    if (ctx->done()) {
        return Status::OK();
    }
    auto task_lock = ctx->task_exec_ctx();
    if (task_lock == nullptr) {
        LOG(INFO) << "could not lock task execution context, query " << ctx->debug_string()
                  << " maybe finished";
        return Status::OK();
    }
    std::shared_ptr<ScannerDelegate> scanner_delegate = scan_task->scanner.lock();
    if (scanner_delegate == nullptr) {
        return Status::OK();
    }

    scan_task->set_state(ScanTask::State::IN_FLIGHT);
    scanner_delegate->_scanner->start_queue_wait();
    TabletStorageType type = scanner_delegate->_scanner->get_storage_type();
    auto sumbit_task = [&]() {
        auto work_func = [scanner_ref = scan_task, ctx]() {
            auto status = [&] {
                RETURN_IF_CATCH_EXCEPTION(_scanner_scan(ctx, scanner_ref));
                return Status::OK();
            }();

            if (!status.ok()) {
                scanner_ref->set_status(status);
                ctx->push_back_scan_task(scanner_ref);
                return true;
            }
            return scanner_ref->is_eos();
        };
        SimplifiedScanTask simple_scan_task = {work_func, ctx, scan_task};
        return this->submit_scan_task(simple_scan_task);
    };

    Status submit_status = sumbit_task();
    if (!submit_status.ok()) {
        // User will see TooManyTasks error. It looks like a more reasonable error.
        Status scan_task_status = Status::TooManyTasks(
                "Failed to submit scanner to scanner pool reason:" +
                std::string(submit_status.msg()) + "|type:" + std::to_string(type));
        scan_task->set_status(scan_task_status);
        return scan_task_status;
    }

    return Status::OK();
}

void handle_reserve_memory_failure(RuntimeState* state, std::shared_ptr<ScannerContext> ctx,
                                   const Status& st, size_t reserve_size) {
    ctx->clear_free_blocks();
    auto* local_state = ctx->local_state();

    auto debug_msg = fmt::format(
            "Query: {} , scanner try to reserve: {}, operator name {}, "
            "operator "
            "id: {}, "
            "task id: "
            "{}, failed: {}",
            print_id(state->query_id()), PrettyPrinter::print_bytes(reserve_size),
            local_state->get_name(), local_state->parent()->node_id(), state->task_id(),
            st.to_string());
    // PROCESS_MEMORY_EXCEEDED error msg alread contains process_mem_log_str
    if (!st.is<ErrorCode::PROCESS_MEMORY_EXCEEDED>()) {
        debug_msg += fmt::format(", debug info: {}", GlobalMemoryArbitrator::process_mem_log_str());
    }
    VLOG_DEBUG << debug_msg;

    state->get_query_ctx()->set_low_memory_mode();
}

void ScannerScheduler::_scanner_scan(std::shared_ptr<ScannerContext> ctx,
                                     std::shared_ptr<ScanTask> scan_task) {
    auto task_lock = ctx->task_exec_ctx();
    if (task_lock == nullptr) {
        return;
    }
    SCOPED_ATTACH_TASK(ctx->state());

    ctx->update_peak_running_scanner(1);
    Defer defer([&] { ctx->update_peak_running_scanner(-1); });

    std::shared_ptr<ScannerDelegate> scanner_delegate = scan_task->scanner.lock();
    if (scanner_delegate == nullptr) {
        return;
    }

    ScannerSPtr& scanner = scanner_delegate->_scanner;
    // for cpu hard limit, thread name should not be reset
    if (ctx->_should_reset_thread_name) {
        Thread::set_self_name("_scanner_scan");
    }

#ifndef __APPLE__
    // The configuration item is used to lower the priority of the scanner thread,
    // typically employed to ensure CPU scheduling for write operations.
    if (config::scan_thread_nice_value != 0 && scanner->get_name() != FileScanner::NAME) {
        Thread::set_thread_nice_value();
    }
#endif

    // we set and get counter according below order, to make sure the counter is updated before get_block, and the time of get_block is recorded in the counter.
    // 1. update_wait_worker_timer to make sure the time of waiting for worker thread is recorded in the timer
    // 2. start_scan_cpu_timer to make sure the cpu timer include the time of open and get_block, which is the real cpu time of scanner
    // 3. update_scan_cpu_timer when defer, to make sure the cpu timer include the time of open and get_block, which is the real cpu time of scanner
    // 4. start_wait_worker_timer when defer, to make sure the time of waiting for worker thread is recorded in the timer

    MonotonicStopWatch max_run_time_watch;
    max_run_time_watch.start();
    scanner->resume();

    bool need_update_profile = true;
    auto update_scanner_profile = [&]() {
        if (need_update_profile) {
            scanner->pause();
            scanner->update_realtime_counters();
            need_update_profile = false;
        }
    };

    Status status = Status::OK();
    bool eos = false;

    ASSIGN_STATUS_IF_CATCH_EXCEPTION(
            RuntimeState* state = ctx->state(); DCHECK(nullptr != state);
            // scanner->open may alloc plenty amount of memory(read blocks of data),
            // so better to also check low memory and clear free blocks here.
            if (ctx->low_memory_mode()) { ctx->clear_free_blocks(); }

            if (!scanner->has_prepared()) {
                status = scanner->prepare();
                if (!status.ok()) {
                    eos = true;
                }
            }

            if (!eos && !scanner->is_open()) {
                status = scanner->open(state);
                if (!status.ok()) {
                    eos = true;
                }
                scanner->set_opened();
            }

            Status rf_status = scanner->try_append_late_arrival_runtime_filter();
            if (!rf_status.ok()) {
                LOG(WARNING) << "Failed to append late arrival runtime filter: "
                             << rf_status.to_string();
            }

            size_t raw_bytes_threshold = config::doris_scanner_row_bytes;
            if (ctx->low_memory_mode()) {
                ctx->clear_free_blocks();
                if (raw_bytes_threshold > ctx->low_memory_mode_scan_bytes_per_scanner()) {
                    raw_bytes_threshold = ctx->low_memory_mode_scan_bytes_per_scanner();
                }
            }

            bool first_read = true;
            int64_t limit = scanner->limit(); if (UNLIKELY(ctx->done())) {
                eos = true;
            } else if (ctx->remaining_limit() == 0) { eos = true; } else if (!eos) {
                do {
                    DEFER_RELEASE_RESERVED();
                    BlockUPtr free_block;
                    if (first_read) {
                        free_block = ctx->get_free_block(first_read);
                    } else {
                        if (state->get_query_ctx()
                                    ->resource_ctx()
                                    ->task_controller()
                                    ->is_enable_reserve_memory()) {
                            size_t block_avg_bytes = scanner->get_block_avg_bytes();
                            auto st = thread_context()->thread_mem_tracker_mgr->try_reserve(
                                    block_avg_bytes);
                            if (!st.ok()) {
                                handle_reserve_memory_failure(state, ctx, st, block_avg_bytes);
                                break;
                            }
                        }
                        free_block = ctx->get_free_block(first_read);
                    }
                    if (free_block == nullptr) {
                        break;
                    }
                    // We got a new created block or a reused block.
                    status = scanner->get_block_after_projects(state, free_block.get(), &eos);
                    first_read = false;
                    if (!status.ok()) {
                        LOG(WARNING) << "Scan thread read Scanner failed: " << status.to_string();
                        break;
                    }
                    // Check column type only after block is read successfully.
                    // Or it may cause a crash when the block is not normal.
                    _make_sure_virtual_col_is_materialized(scanner, free_block.get());

                    // Shared limit quota: acquire rows from the context's shared pool.
                    // Discard or truncate the block if quota is exhausted.
                    if (free_block->rows() > 0) {
                        int64_t block_rows = free_block->rows();
                        int64_t granted = ctx->acquire_limit_quota(block_rows);
                        if (granted == 0) {
                            // No quota remaining, discard this block and mark eos.
                            ctx->return_free_block(std::move(free_block));
                            eos = true;
                            break;
                        } else if (granted < block_rows) {
                            // Partial quota: truncate block to granted rows and mark eos.
                            free_block->set_num_rows(granted);
                            eos = true;
                        }
                    }
                    // Projection will truncate useless columns, makes block size change.
                    auto free_block_bytes = free_block->allocated_bytes();
                    ctx->reestimated_block_mem_bytes(cast_set<int64_t>(free_block_bytes));
                    DCHECK(scan_task->cached_block == nullptr);
                    ctx->inc_block_usage(free_block->allocated_bytes());
                    scan_task->cached_block = std::move(free_block);

                    // Per-scanner small-limit optimization: if limit is small (< batch_size),
                    // return immediately instead of accumulating to raw_bytes_threshold.
                    if (limit > 0 && limit < ctx->batch_size()) {
                        break;
                    }

                    if (scan_task->cached_block->rows() > 0) {
                        auto block_avg_bytes = (scan_task->cached_block->bytes() +
                                                scan_task->cached_block->rows() - 1) /
                                               scan_task->cached_block->rows() * ctx->batch_size();
                        scanner->update_block_avg_bytes(block_avg_bytes);
                    }
                    if (ctx->low_memory_mode()) {
                        ctx->clear_free_blocks();
                    }
                } while (false);
            }

                                              if (UNLIKELY(!status.ok())) {
                                                  scan_task->set_status(status);
                                                  eos = true;
                                              },
                                              status);

    if (UNLIKELY(!status.ok())) {
        scan_task->set_status(status);
        eos = true;
    }

    // Always update scanner profile to properly account for CPU time on the same
    // thread that started the CPU timer (CLOCK_THREAD_CPUTIME_ID is per-thread).
    update_scanner_profile();

    if (eos) {
        scanner->mark_to_need_to_close();
        scan_task->set_state(ScanTask::State::EOS);
    } else {
        scan_task->set_state(ScanTask::State::COMPLETED);
    }

    VLOG_DEBUG << fmt::format(
            "Scanner context {} has finished task, current scheduled task is "
            "{}, eos: {}, status: {}",
            ctx->ctx_id, ctx->num_scheduled_scanners(), eos, status.to_string());

    ctx->push_back_scan_task(scan_task);
}
int ScannerScheduler::default_local_scan_thread_num() {
    return config::doris_scanner_thread_pool_thread_num > 0
                   ? config::doris_scanner_thread_pool_thread_num
                   : std::max(48, CpuInfo::num_cores() * 2);
}
int ScannerScheduler::default_remote_scan_thread_num() {
    int num = config::doris_max_remote_scanner_thread_pool_thread_num > 0
                      ? config::doris_max_remote_scanner_thread_pool_thread_num
                      : std::max(512, CpuInfo::num_cores() * 10);
    return std::max(num, default_local_scan_thread_num());
}

int ScannerScheduler::get_remote_scan_thread_queue_size() {
    return config::doris_remote_scanner_thread_pool_queue_size;
}

int ScannerScheduler::default_min_active_scan_threads() {
    return config::min_active_scan_threads > 0
                   ? config::min_active_scan_threads
                   : config::min_active_scan_threads = CpuInfo::num_cores() * 2;
}

int ScannerScheduler::default_min_active_file_scan_threads() {
    return config::min_active_file_scan_threads > 0
                   ? config::min_active_file_scan_threads
                   : config::min_active_file_scan_threads = CpuInfo::num_cores() * 8;
}

void ScannerScheduler::_make_sure_virtual_col_is_materialized(
        const std::shared_ptr<Scanner>& scanner, Block* free_block) {
#ifndef NDEBUG
    // Currently, virtual column can only be used on olap table.
    std::shared_ptr<OlapScanner> olap_scanner = std::dynamic_pointer_cast<OlapScanner>(scanner);
    if (olap_scanner == nullptr) {
        return;
    }

    if (free_block->rows() == 0) {
        return;
    }

    size_t idx = 0;
    for (const auto& entry : *free_block) {
        // Virtual column must be materialized on the end of SegmentIterator's next batch method.
        const ColumnNothing* column_nothing =
                check_and_get_column<ColumnNothing>(entry.column.get());
        if (column_nothing == nullptr) {
            idx++;
            continue;
        }

        std::vector<std::string> vcid_to_idx;

        for (const auto& pair : olap_scanner->_vir_cid_to_idx_in_block) {
            vcid_to_idx.push_back(fmt::format("{}-{}", pair.first, pair.second));
        }

        std::string error_msg = fmt::format(
                "Column in idx {} is nothing, block columns {}, normal_columns "
                "{}, "
                "vir_cid_to_idx_in_block_msg {}",
                idx, free_block->columns(), olap_scanner->_return_columns.size(),
                fmt::format("_vir_cid_to_idx_in_block:[{}]", fmt::join(vcid_to_idx, ",")));
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, error_msg);
    }
#endif
}

Result<SharedListenableFuture<Void>> ScannerSplitRunner::process_for(std::chrono::nanoseconds) {
    _started = true;
    bool is_completed = _scan_func();
    if (is_completed) {
        _completion_future.set_value(Void {});
    }
    return SharedListenableFuture<Void>::create_ready(Void {});
}

bool ScannerSplitRunner::is_finished() {
    return _completion_future.is_done();
}

Status ScannerSplitRunner::finished_status() {
    return _completion_future.get_status();
}

bool ScannerSplitRunner::is_started() const {
    return _started.load();
}

bool ScannerSplitRunner::is_auto_reschedule() const {
    return false;
}

} // namespace doris
