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

    for (int i = 0; i < QUEUE_NUM; i++) {
        delete _pending_queues[i];
    }
    delete[] _pending_queues;
    _deregister_metrics();
}

void ScannerScheduler::stop() {
    if (!_is_init) {
        return;
    }

    for (int i = 0; i < QUEUE_NUM; i++) {
        _pending_queues[i]->shutdown();
    }

    _is_closed = true;

    _scheduler_pool->shutdown();
    _local_scan_thread_pool->shutdown();
    _remote_scan_thread_pool->shutdown();
    _limited_scan_thread_pool->shutdown();

    _scheduler_pool->wait();
    _local_scan_thread_pool->join();
    _remote_scan_thread_pool->join();
    _limited_scan_thread_pool->wait();

    LOG(INFO) << "ScannerScheduler stopped";
}

Status ScannerScheduler::init(ExecEnv* env) {
    // 1. scheduling thread pool and scheduling queues
    static_cast<void>(ThreadPoolBuilder("SchedulingThreadPool")
                              .set_min_threads(QUEUE_NUM)
                              .set_max_threads(QUEUE_NUM)
                              .build(&_scheduler_pool));

    _pending_queues = new BlockingQueue<std::shared_ptr<ScannerContext>>*[QUEUE_NUM];
    for (int i = 0; i < QUEUE_NUM; i++) {
        _pending_queues[i] = new BlockingQueue<std::shared_ptr<ScannerContext>>(INT32_MAX);
        static_cast<void>(_scheduler_pool->submit_func([this, i] { this->_schedule_thread(i); }));
    }

    // 2. local scan thread pool
    _local_scan_thread_pool = std::make_unique<PriorityThreadPool>(
            config::doris_scanner_thread_pool_thread_num,
            config::doris_scanner_thread_pool_queue_size, "local_scan");

    // 3. remote scan thread pool
    _remote_thread_pool_max_size = config::doris_max_remote_scanner_thread_pool_thread_num != -1
                                           ? config::doris_max_remote_scanner_thread_pool_thread_num
                                           : std::max(512, CpuInfo::num_cores() * 10);
    _remote_thread_pool_max_size =
            std::max(_remote_thread_pool_max_size, config::doris_scanner_thread_pool_thread_num);
    _remote_scan_thread_pool = std::make_unique<PriorityThreadPool>(
            _remote_thread_pool_max_size, config::doris_remote_scanner_thread_pool_queue_size,
            "RemoteScanThreadPool");

    // 4. limited scan thread pool
    static_cast<void>(ThreadPoolBuilder("LimitedScanThreadPool")
                              .set_min_threads(config::doris_scanner_thread_pool_thread_num)
                              .set_max_threads(config::doris_scanner_thread_pool_thread_num)
                              .set_max_queue_size(config::doris_scanner_thread_pool_queue_size)
                              .build(&_limited_scan_thread_pool));
    _register_metrics();
    _is_init = true;
    return Status::OK();
}

Status ScannerScheduler::submit(std::shared_ptr<ScannerContext> ctx) {
    if (ctx->done()) {
        return Status::EndOfFile("ScannerContext is done");
    }
    ctx->queue_idx = (_queue_idx++ % QUEUE_NUM);
    if (!_pending_queues[ctx->queue_idx]->blocking_put(ctx)) {
        return Status::InternalError("failed to submit scanner context to scheduler");
    }
    return Status::OK();
}

std::unique_ptr<ThreadPoolToken> ScannerScheduler::new_limited_scan_pool_token(
        ThreadPool::ExecutionMode mode, int max_concurrency) {
    return _limited_scan_thread_pool->new_token(mode, max_concurrency);
}

void ScannerScheduler::_schedule_thread(int queue_id) {
    BlockingQueue<std::shared_ptr<ScannerContext>>* queue = _pending_queues[queue_id];
    while (!_is_closed) {
        std::shared_ptr<ScannerContext> ctx;
        bool ok = queue->blocking_get(&ctx);
        if (!ok) {
            // maybe closed
            continue;
        }

        _schedule_scanners(ctx);
        // If ctx is done, no need to schedule it again.
        // But should notice that there may still scanners running in scanner pool.
    }
}

void ScannerScheduler::_schedule_scanners(std::shared_ptr<ScannerContext> ctx) {
    auto task_lock = ctx->get_task_execution_context().lock();
    if (task_lock == nullptr) {
        // LOG(WARNING) << "could not lock task execution context, query " << print_id(_query_id)
        //             << " maybe finished";
        return;
    }
    MonotonicStopWatch watch;
    watch.reset();
    watch.start();
    ctx->incr_num_ctx_scheduling(1);
    size_t size = 0;
    Defer defer {[&]() {
        ctx->incr_num_scanner_scheduling(size);
        ctx->dec_num_scheduling_ctx();
    }};

    if (ctx->done()) {
        return;
    }

    std::list<VScannerSPtr> this_run;
    ctx->get_next_batch_of_scanners(&this_run);
    size = this_run.size();
    if (!size) {
        // There will be 2 cases when this_run is empty:
        // 1. The blocks queue reaches limit.
        //      The consumer will continue scheduling the ctx.
        // 2. All scanners are running.
        //      There running scanner will schedule the ctx after they are finished.
        // So here we just return to stop scheduling ctx.
        return;
    }

    ctx->inc_num_running_scanners(this_run.size());

    // Submit scanners to thread pool
    // TODO(cmy): How to handle this "nice"?
    int nice = 1;
    auto iter = this_run.begin();
    if (ctx->thread_token != nullptr) {
        // TODO llj tg how to treat this?
        while (iter != this_run.end()) {
            (*iter)->start_wait_worker_timer();
            auto s = ctx->thread_token->submit_func(
                    [this, scanner = *iter, ctx] { this->_scanner_scan(this, ctx, scanner); });
            if (s.ok()) {
                this_run.erase(iter++);
            } else {
                ctx->set_status_on_error(s);
                break;
            }
        }
    } else {
        while (iter != this_run.end()) {
            (*iter)->start_wait_worker_timer();
            TabletStorageType type = (*iter)->get_storage_type();
            bool ret = false;
            if (type == TabletStorageType::STORAGE_TYPE_LOCAL) {
                if (auto* scan_sche = ctx->get_simple_scan_scheduler()) {
                    auto work_func = [this, scanner = *iter, ctx] {
                        this->_scanner_scan(this, ctx, scanner);
                    };
                    SimplifiedScanTask simple_scan_task = {work_func, ctx};
                    ret = scan_sche->get_scan_queue()->try_put(simple_scan_task);
                } else {
                    PriorityThreadPool::Task task;
                    task.work_function = [this, scanner = *iter, ctx] {
                        this->_scanner_scan(this, ctx, scanner);
                    };
                    task.priority = nice;
                    ret = _local_scan_thread_pool->offer(task);
                }
            } else {
                PriorityThreadPool::Task task;
                task.work_function = [this, scanner = *iter, ctx] {
                    this->_scanner_scan(this, ctx, scanner);
                };
                task.priority = nice;
                ret = _remote_scan_thread_pool->offer(task);
            }
            if (ret) {
                this_run.erase(iter++);
            } else {
                ctx->set_status_on_error(
                        Status::InternalError("failed to submit scanner to scanner pool"));
                break;
            }
        }
    }
    ctx->incr_ctx_scheduling_time(watch.elapsed_time());
}

void ScannerScheduler::_scanner_scan(ScannerScheduler* scheduler,
                                     std::shared_ptr<ScannerContext> ctx, VScannerSPtr scanner) {
    auto task_lock = ctx->get_task_execution_context().lock();
    if (task_lock == nullptr) {
        // LOG(WARNING) << "could not lock task execution context, query " << print_id(_query_id)
        //             << " maybe finished";
        return;
    }
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
            ctx->set_status_on_error(status);
            eos = true;
        }
    }
    if (!eos && !scanner->is_open()) {
        status = scanner->open(state);
        if (!status.ok()) {
            ctx->set_status_on_error(status);
            eos = true;
        }
        scanner->set_opened();
    }

    static_cast<void>(scanner->try_append_late_arrival_runtime_filter());

    // Because we use thread pool to scan data from storage. One scanner can't
    // use this thread too long, this can starve other query's scanner. So, we
    // need yield this thread when we do enough work. However, OlapStorage read
    // data in pre-aggregate mode, then we can't use storage returned data to
    // judge if we need to yield. So we record all raw data read in this round
    // scan, if this exceeds row number or bytes threshold, we yield this thread.
    std::vector<vectorized::BlockUPtr> blocks;
    int64_t raw_rows_read = scanner->get_rows_read();
    int64_t raw_rows_threshold = raw_rows_read + config::doris_scanner_row_num;
    int64_t raw_bytes_read = 0;
    int64_t raw_bytes_threshold = config::doris_scanner_row_bytes;
    int num_rows_in_block = 0;

    // Only set to true when ctx->done() return true.
    // Use this flag because we need distinguish eos from `should_stop`.
    // If eos is true, we still need to return blocks,
    // but is should_stop is true, no need to return blocks
    bool should_stop = false;
    // Has to wait at least one full block, or it will cause a lot of schedule task in priority
    // queue, it will affect query latency and query concurrency for example ssb 3.3.
    auto should_do_scan = [&, batch_size = state->batch_size(),
                           time = state->wait_full_block_schedule_times()]() {
        if (raw_bytes_read < raw_bytes_threshold && raw_rows_read < raw_rows_threshold) {
            return true;
        } else if (num_rows_in_block < batch_size) {
            return raw_bytes_read < raw_bytes_threshold * time &&
                   raw_rows_read < raw_rows_threshold * time;
        }
        return false;
    };

    while (!eos && should_do_scan()) {
        // TODO llj task group should should_yield?
        if (UNLIKELY(ctx->done())) {
            // No need to set status on error here.
            // Because done() maybe caused by "should_stop"
            should_stop = true;
            break;
        }

        BlockUPtr block = ctx->get_free_block();

        status = scanner->get_block_after_projects(state, block.get(), &eos);
        // The VFileScanner for external table may try to open not exist files,
        // Because FE file cache for external table may out of date.
        // So, NOT_FOUND for VFileScanner is not a fail case.
        // Will remove this after file reader refactor.
        if (!status.ok() && (scanner->get_name() != doris::vectorized::VFileScanner::NAME ||
                             (scanner->get_name() == doris::vectorized::VFileScanner::NAME &&
                              !status.is<ErrorCode::NOT_FOUND>()))) {
            LOG(WARNING) << "Scan thread read VScanner failed: " << status.to_string();
            break;
        }
        VLOG_ROW << "VScanNode input rows: " << block->rows() << ", eos: " << eos;
        if (status.is<ErrorCode::NOT_FOUND>()) {
            // The only case in this "if" branch is external table file delete and fe cache has not been updated yet.
            // Set status to OK.
            status = Status::OK();
            eos = true;
        }

        raw_bytes_read += block->allocated_bytes();
        num_rows_in_block += block->rows();
        if (UNLIKELY(block->rows() == 0)) {
            ctx->return_free_block(std::move(block));
        } else {
            if (!blocks.empty() && blocks.back()->rows() + block->rows() <= state->batch_size()) {
                vectorized::MutableBlock mutable_block(blocks.back().get());
                static_cast<void>(mutable_block.merge(*block));
                blocks.back().get()->set_columns(std::move(mutable_block.mutable_columns()));
                ctx->return_free_block(std::move(block));
            } else {
                blocks.push_back(std::move(block));
            }
        }
        raw_rows_read = scanner->get_rows_read();
    } // end for while

    // if we failed, check status.
    if (UNLIKELY(!status.ok())) {
        // _transfer_done = true;
        ctx->set_status_on_error(status);
        eos = true;
        blocks.clear();
    } else if (should_stop) {
        // No need to return blocks because of should_stop, just delete them
        blocks.clear();
    } else if (!blocks.empty()) {
        ctx->append_blocks_to_queue(blocks);
    }

    scanner->update_scan_cpu_timer();
    if (eos || should_stop) {
        scanner->mark_to_need_to_close();
    }
    ctx->push_back_scanner_and_reschedule(scanner);
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
