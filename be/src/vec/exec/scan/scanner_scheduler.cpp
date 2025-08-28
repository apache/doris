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

#include "scanner_scheduler.h"

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
#include "file_scanner.h"
#include "olap/tablet.h"
#include "pipeline/pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "util/async_io.h" // IWYU pragma: keep
#include "util/cpu_info.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "vec/columns/column_nothing.h"
#include "vec/core/block.h"
#include "vec/exec/scan/olap_scanner.h" // IWYU pragma: keep
#include "vec/exec/scan/scan_node.h"
#include "vec/exec/scan/scanner.h"
#include "vec/exec/scan/scanner_context.h"

namespace doris::vectorized {

ScannerScheduler::ScannerScheduler() = default;

ScannerScheduler::~ScannerScheduler() = default;

void ScannerScheduler::stop() {
    if (!_is_init) {
        return;
    }

    _is_closed = true;

    LOG(INFO) << "ScannerScheduler stopped";
}

Status ScannerScheduler::init(ExecEnv* env) {
    _is_init = true;
    return Status::OK();
}

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

    scanner_delegate->_scanner->start_wait_worker_timer();
    TabletStorageType type = scanner_delegate->_scanner->get_storage_type();
    auto sumbit_task = [&]() {
        SimplifiedScanScheduler* scan_sched = ctx->get_scan_scheduler();
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
        return scan_sched->submit_scan_task(simple_scan_task);
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
    MonotonicStopWatch max_run_time_watch;
    max_run_time_watch.start();
    scanner->update_wait_worker_timer();
    scanner->start_scan_cpu_timer();
    Status status = Status::OK();
    bool eos = false;
    ASSIGN_STATUS_IF_CATCH_EXCEPTION(
            RuntimeState* state = ctx->state(); DCHECK(nullptr != state);
            // scanner->open may alloc plenty amount of memory(read blocks of data),
            // so better to also check low memory and clear free blocks here.
            if (ctx->low_memory_mode()) { ctx->clear_free_blocks(); }

            if (!scanner->is_init()) {
                status = scanner->init();
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

            size_t raw_bytes_read = 0;
            bool first_read = true; int64_t limit = scanner->limit();
            // If the first block is full, then it is true. Or the first block + second block > batch_size
            bool has_first_full_block = false;

            // During low memory mode, every scan task will return at most 2 block to reduce memory usage.
            while (!eos && raw_bytes_read < raw_bytes_threshold &&
                   (!ctx->low_memory_mode() || !has_first_full_block) &&
                   (!has_first_full_block || doris::thread_context()
                                                     ->thread_mem_tracker_mgr->limiter_mem_tracker()
                                                     ->check_limit(1))) {
                if (UNLIKELY(ctx->done())) {
                    eos = true;
                    break;
                }
                if (max_run_time_watch.elapsed_time() >
                    config::doris_scanner_max_run_time_ms * 1e6) {
                    break;
                }
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
                // Projection will truncate useless columns, makes block size change.
                auto free_block_bytes = free_block->allocated_bytes();
                raw_bytes_read += free_block_bytes;
                if (!scan_task->cached_blocks.empty() &&
                    scan_task->cached_blocks.back().first->rows() + free_block->rows() <=
                            ctx->batch_size()) {
                    size_t block_size = scan_task->cached_blocks.back().first->allocated_bytes();
                    vectorized::MutableBlock mutable_block(
                            scan_task->cached_blocks.back().first.get());
                    status = mutable_block.merge(*free_block);
                    if (!status.ok()) {
                        LOG(WARNING) << "Block merge failed: " << status.to_string();
                        break;
                    }
                    scan_task->cached_blocks.back().second = mutable_block.allocated_bytes();
                    scan_task->cached_blocks.back().first.get()->set_columns(
                            std::move(mutable_block.mutable_columns()));

                    // Return block succeed or not, this free_block is not used by this scan task any more.
                    // If block can be reused, its memory usage will be added back.
                    ctx->return_free_block(std::move(free_block));
                    ctx->inc_block_usage(scan_task->cached_blocks.back().first->allocated_bytes() -
                                         block_size);
                } else {
                    if (!scan_task->cached_blocks.empty()) {
                        has_first_full_block = true;
                    }
                    ctx->inc_block_usage(free_block->allocated_bytes());
                    scan_task->cached_blocks.emplace_back(std::move(free_block), free_block_bytes);
                }

                if (limit > 0 && limit < ctx->batch_size()) {
                    // If this scanner has limit, and less than batch size,
                    // return immediately and no need to wait raw_bytes_threshold.
                    // This can save time that each scanner may only return a small number of rows,
                    // but rows are enough from all scanners.
                    // If not break, the query like "select * from tbl where id=1 limit 10"
                    // may scan a lot data when the "id=1"'s filter ratio is high.
                    // If limit is larger than batch size, this rule is skipped,
                    // to avoid user specify a large limit and causing too much small blocks.
                    break;
                }

                if (scan_task->cached_blocks.back().first->rows() > 0) {
                    auto block_avg_bytes = (scan_task->cached_blocks.back().first->bytes() +
                                            scan_task->cached_blocks.back().first->rows() - 1) /
                                           scan_task->cached_blocks.back().first->rows() *
                                           ctx->batch_size();
                    scanner->update_block_avg_bytes(block_avg_bytes);
                }
                if (ctx->low_memory_mode()) {
                    ctx->clear_free_blocks();
                    if (raw_bytes_threshold > ctx->low_memory_mode_scan_bytes_per_scanner()) {
                        raw_bytes_threshold = ctx->low_memory_mode_scan_bytes_per_scanner();
                    }
                }
            } // end for while

            if (UNLIKELY(!status.ok())) {
                scan_task->set_status(status);
                eos = true;
            },
            status);

    if (UNLIKELY(!status.ok())) {
        scan_task->set_status(status);
        eos = true;
    }
    // WorkloadGroup Policy will check cputime realtime, so that should update the counter
    // as soon as possible, could not update it on close.
    scanner->update_scan_cpu_timer();
    scanner->update_realtime_counters();

    if (eos) {
        scanner->mark_to_need_to_close();
    }
    scan_task->set_eos(eos);

    VLOG_DEBUG << fmt::format(
            "Scanner context {} has finished task, cached_block {} current scheduled task is "
            "{}, eos: {}, status: {}",
            ctx->ctx_id, scan_task->cached_blocks.size(), ctx->num_scheduled_scanners(), eos,
            status.to_string());

    ctx->push_back_scan_task(scan_task);
}

int ScannerScheduler::get_remote_scan_thread_num() {
    static int remote_max_thread_num = []() {
        int num = config::doris_max_remote_scanner_thread_pool_thread_num != -1
                          ? config::doris_max_remote_scanner_thread_pool_thread_num
                          : std::max(512, CpuInfo::num_cores() * 10);
        return std::max(num, config::doris_scanner_thread_pool_thread_num);
    }();
    return remote_max_thread_num;
}

int ScannerScheduler::get_remote_scan_thread_queue_size() {
    return config::doris_remote_scanner_thread_pool_queue_size;
}

void ScannerScheduler::_make_sure_virtual_col_is_materialized(
        const std::shared_ptr<Scanner>& scanner, vectorized::Block* free_block) {
#ifndef NDEBUG
    // Currently, virtual column can only be used on olap table.
    std::shared_ptr<OlapScanner> olap_scanner = std::dynamic_pointer_cast<OlapScanner>(scanner);
    if (olap_scanner == nullptr) {
        return;
    }

    size_t idx = 0;
    for (const auto& entry : *free_block) {
        // Virtual column must be materialized on the end of SegmentIterator's next batch method.
        const vectorized::ColumnNothing* column_nothing =
                vectorized::check_and_get_column<vectorized::ColumnNothing>(entry.column.get());
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

} // namespace doris::vectorized
