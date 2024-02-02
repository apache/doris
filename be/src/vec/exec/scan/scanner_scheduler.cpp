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
#include <typeinfo>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "olap/tablet.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/async_io.h" // IWYU pragma: keep
#include "util/blocking_queue.hpp"
#include "util/cpu_info.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/work_thread_pool.hpp"
#include "vec/core/block.h"
#include "vec/exec/scan/new_olap_scanner.h" // IWYU pragma: keep
#include "vec/exec/scan/scanner_context.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/scan/vscanner.h"
#include "vfile_scanner.h"

namespace doris::vectorized {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(local_scan_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(local_scan_thread_pool_thread_num, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(remote_scan_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(remote_scan_thread_pool_thread_num, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(limited_scan_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(limited_scan_thread_pool_thread_num, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(group_local_scan_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(group_local_scan_thread_pool_thread_num, MetricUnit::NOUNIT);

ScannerScheduler::ScannerScheduler() = default;

ScannerScheduler::~ScannerScheduler() {
    if (!_is_init) {
        return;
    }

    _deregister_metrics();
}

void ScannerScheduler::stop() {
    if (!_is_init) {
        return;
    }

    _is_closed = true;

    _local_scan_thread_pool->shutdown();
    _remote_scan_thread_pool->shutdown();
    _limited_scan_thread_pool->shutdown();

    _local_scan_thread_pool->join();
    _remote_scan_thread_pool->join();
    _limited_scan_thread_pool->wait();

    LOG(INFO) << "ScannerScheduler stopped";
}

Status ScannerScheduler::init(ExecEnv* env) {
    // 1. local scan thread pool
    _local_scan_thread_pool = std::make_unique<PriorityThreadPool>(
            config::doris_scanner_thread_pool_thread_num,
            config::doris_scanner_thread_pool_queue_size, "local_scan");

    // 2. remote scan thread pool
    _remote_thread_pool_max_size = config::doris_max_remote_scanner_thread_pool_thread_num != -1
                                           ? config::doris_max_remote_scanner_thread_pool_thread_num
                                           : std::max(512, CpuInfo::num_cores() * 10);
    _remote_thread_pool_max_size =
            std::max(_remote_thread_pool_max_size, config::doris_scanner_thread_pool_thread_num);
    _remote_scan_thread_pool = std::make_unique<PriorityThreadPool>(
            _remote_thread_pool_max_size, config::doris_remote_scanner_thread_pool_queue_size,
            "RemoteScanThreadPool");

    // 3. limited scan thread pool
    static_cast<void>(ThreadPoolBuilder("LimitedScanThreadPool")
                              .set_min_threads(config::doris_scanner_thread_pool_thread_num)
                              .set_max_threads(config::doris_scanner_thread_pool_thread_num)
                              .set_max_queue_size(config::doris_scanner_thread_pool_queue_size)
                              .build(&_limited_scan_thread_pool));
    _register_metrics();
    _is_init = true;
    return Status::OK();
}

void ScannerScheduler::submit(std::shared_ptr<ScannerContext> ctx,
                              std::shared_ptr<RunningScanner> running_scanner) {
    running_scanner->last_submit_time = GetCurrentTimeNanos();
    if (ctx->done()) {
        running_scanner->eos = true;
        running_scanner->status = Status::EndOfFile("ScannerContext is done");
        ctx->append_block_to_queue(running_scanner);
        return;
    }
    auto task_lock = ctx->task_exec_ctx();
    if (task_lock == nullptr) {
        running_scanner->eos = true;
        running_scanner->status =
                Status::EndOfFile("could not lock task execution context, query maybe finished");
        ctx->append_block_to_queue(running_scanner);
        return;
    }

    // Submit scanners to thread pool
    // TODO(cmy): How to handle this "nice"?
    int nice = 1;
    if (ctx->thread_token != nullptr) {
        std::shared_ptr<ScannerDelegate> scanner_delegate = running_scanner->scanner.lock();
        if (scanner_delegate == nullptr) {
            running_scanner->eos = true;
            ctx->append_block_to_queue(running_scanner);
            return;
        }

        scanner_delegate->_scanner->start_wait_worker_timer();
        auto s = ctx->thread_token->submit_func([this, scanner_ref = running_scanner, ctx]() {
            this->_scanner_scan(ctx, scanner_ref);
        });
        if (!s.ok()) {
            running_scanner->status = s;
            ctx->append_block_to_queue(running_scanner);
            return;
        }
    } else {
        std::shared_ptr<ScannerDelegate> scanner_delegate = running_scanner->scanner.lock();
        if (scanner_delegate == nullptr) {
            running_scanner->eos = true;
            ctx->append_block_to_queue(running_scanner);
            return;
        }

        scanner_delegate->_scanner->start_wait_worker_timer();
        TabletStorageType type = scanner_delegate->_scanner->get_storage_type();
        bool ret = false;
        if (type == TabletStorageType::STORAGE_TYPE_LOCAL) {
            if (auto* scan_sche = ctx->get_simple_scan_scheduler()) {
                auto work_func = [this, scanner_ref = running_scanner, ctx]() {
                    this->_scanner_scan(ctx, scanner_ref);
                };
                SimplifiedScanTask simple_scan_task = {work_func, ctx};
                ret = scan_sche->get_scan_queue()->try_put(simple_scan_task);
            } else {
                PriorityThreadPool::Task task;
                task.work_function = [this, scanner_ref = running_scanner, ctx]() {
                    this->_scanner_scan(ctx, scanner_ref);
                };
                task.priority = nice;
                ret = _local_scan_thread_pool->offer(task);
            }
        } else {
            PriorityThreadPool::Task task;
            task.work_function = [this, scanner_ref = running_scanner, ctx]() {
                this->_scanner_scan(ctx, scanner_ref);
            };
            task.priority = nice;
            ret = _remote_scan_thread_pool->offer(task);
        }
        if (!ret) {
            running_scanner->status =
                    Status::InternalError("failed to submit scanner to scanner pool");
            ctx->append_block_to_queue(running_scanner);
            return;
        }
    }
}

std::unique_ptr<ThreadPoolToken> ScannerScheduler::new_limited_scan_pool_token(
        ThreadPool::ExecutionMode mode, int max_concurrency) {
    return _limited_scan_thread_pool->new_token(mode, max_concurrency);
}

void ScannerScheduler::_scanner_scan(std::shared_ptr<ScannerContext> ctx,
                                     std::shared_ptr<RunningScanner> running_scanner) {
    // record the time from scanner submission to actual execution in nanoseconds
    ctx->incr_ctx_scheduling_time(GetCurrentTimeNanos() - running_scanner->last_submit_time);
    auto task_lock = ctx->task_exec_ctx();
    if (task_lock == nullptr) {
        running_scanner->eos = true;
        ctx->append_block_to_queue(running_scanner);
        return;
    }

    std::shared_ptr<ScannerDelegate> scanner_delegate = running_scanner->scanner.lock();
    if (scanner_delegate == nullptr) {
        running_scanner->eos = true;
        ctx->append_block_to_queue(running_scanner);
        return;
    }

    VScannerSPtr& scanner = scanner_delegate->_scanner;
    SCOPED_ATTACH_TASK(scanner->runtime_state());
    // for cpu hard limit, thread name should not be reset
    if (ctx->_should_reset_thread_name) {
        Thread::set_self_name("_scanner_scan");
    }

#ifndef __APPLE__
    // The configuration item is used to lower the priority of the scanner thread,
    // typically employed to ensure CPU scheduling for write operations.
    if (config::scan_thread_nice_value != 0 && scanner->get_name() != VFileScanner::NAME) {
        Thread::set_thread_nice_value();
    }
#endif
    scanner->update_wait_worker_timer();
    scanner->start_scan_cpu_timer();
    Status status = Status::OK();
    bool eos = false;
    RuntimeState* state = ctx->state();
    DCHECK(nullptr != state);
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

    static_cast<void>(scanner->try_append_late_arrival_runtime_filter());

    bool first_read = true;
    int last_read_rows = ctx->batch_size();
    while (!eos) {
        if (UNLIKELY(ctx->done())) {
            eos = true;
            break;
        }
        BlockUPtr free_block = nullptr;
        if (first_read) {
            status = scanner->get_block_after_projects(state, running_scanner->current_block.get(),
                                                       &eos);
            first_read = false;
            if (running_scanner->current_block->rows() > 0) {
                last_read_rows = running_scanner->current_block->rows();
            }
        } else {
            free_block = ctx->get_free_block(last_read_rows);
            status = scanner->get_block_after_projects(state, free_block.get(), &eos);
            if (free_block->rows() > 0) {
                last_read_rows = free_block->rows();
            }
        }

        // The VFileScanner for external table may try to open not exist files,
        // Because FE file cache for external table may out of date.
        // So, NOT_FOUND for VFileScanner is not a fail case.
        // Will remove this after file reader refactor.
        if (!status.ok() && (scanner->get_name() != doris::vectorized::VFileScanner::NAME ||
                             (scanner->get_name() == doris::vectorized::VFileScanner::NAME &&
                              !status.is<ErrorCode::NOT_FOUND>()))) {
            LOG(WARNING) << "Scan thread read VScanner failed: " << status.to_string();
            break;
        } else if (status.is<ErrorCode::NOT_FOUND>()) {
            // The only case in this "if" branch is external table file delete and fe cache has not been updated yet.
            // Set status to OK.
            status = Status::OK();
            eos = true;
            break;
        }

        if (!first_read && free_block) {
            vectorized::MutableBlock mutable_block(running_scanner->current_block.get());
            static_cast<void>(mutable_block.merge(*free_block));
            running_scanner->current_block->set_columns(std::move(mutable_block.mutable_columns()));
            ctx->return_free_block(std::move(free_block));
        }
        if (running_scanner->current_block->rows() >= ctx->batch_size()) {
            break;
        }
    } // end for while

    if (UNLIKELY(!status.ok())) {
        running_scanner->status = status;
        eos = true;
    }

    scanner->update_scan_cpu_timer();
    if (eos) {
        scanner->mark_to_need_to_close();
    }
    running_scanner->eos = eos;
    ctx->append_block_to_queue(running_scanner);
}

void ScannerScheduler::_register_metrics() {
    REGISTER_HOOK_METRIC(local_scan_thread_pool_queue_size,
                         [this]() { return _local_scan_thread_pool->get_queue_size(); });
    REGISTER_HOOK_METRIC(local_scan_thread_pool_thread_num,
                         [this]() { return _local_scan_thread_pool->get_active_threads(); });
    REGISTER_HOOK_METRIC(remote_scan_thread_pool_queue_size,
                         [this]() { return _remote_scan_thread_pool->get_queue_size(); });
    REGISTER_HOOK_METRIC(remote_scan_thread_pool_thread_num,
                         [this]() { return _remote_scan_thread_pool->get_active_threads(); });
    REGISTER_HOOK_METRIC(limited_scan_thread_pool_queue_size,
                         [this]() { return _limited_scan_thread_pool->get_queue_size(); });
    REGISTER_HOOK_METRIC(limited_scan_thread_pool_thread_num,
                         [this]() { return _limited_scan_thread_pool->num_threads(); });
}

void ScannerScheduler::_deregister_metrics() {
    DEREGISTER_HOOK_METRIC(local_scan_thread_pool_queue_size);
    DEREGISTER_HOOK_METRIC(local_scan_thread_pool_thread_num);
    DEREGISTER_HOOK_METRIC(remote_scan_thread_pool_queue_size);
    DEREGISTER_HOOK_METRIC(remote_scan_thread_pool_thread_num);
    DEREGISTER_HOOK_METRIC(limited_scan_thread_pool_queue_size);
    DEREGISTER_HOOK_METRIC(limited_scan_thread_pool_thread_num);
    DEREGISTER_HOOK_METRIC(group_local_scan_thread_pool_queue_size);
    DEREGISTER_HOOK_METRIC(group_local_scan_thread_pool_thread_num);
}
} // namespace doris::vectorized
