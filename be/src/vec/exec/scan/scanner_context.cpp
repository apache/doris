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

#include "scanner_context.h"

#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>
#include <zconf.h>

#include <cstdint>
#include <ctime>
#include <memory>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <tuple>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/tablet.h"
#include "pipeline/exec/scan_operator.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/exec/scan/scan_node.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris::vectorized {

using namespace std::chrono_literals;

ScannerContext::ScannerContext(
        RuntimeState* state, pipeline::ScanLocalStateBase* local_state,
        const TupleDescriptor* output_tuple_desc, const RowDescriptor* output_row_descriptor,
        const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners, int64_t limit_,
        std::shared_ptr<pipeline::Dependency> dependency, int parallism_of_scan_operator)
        : HasTaskExecutionCtx(state),
          _state(state),
          _local_state(local_state),
          _output_tuple_desc(output_row_descriptor
                                     ? output_row_descriptor->tuple_descriptors().front()
                                     : output_tuple_desc),
          _output_row_descriptor(output_row_descriptor),
          _batch_size(state->batch_size()),
          limit(limit_),
          _scanner_scheduler_global(state->exec_env()->scanner_scheduler()),
          _all_scanners(scanners.begin(), scanners.end()),
          _parallism_of_scan_operator(parallism_of_scan_operator),
          _min_scan_concurrency_of_scan_scheduler(_state->min_scan_concurrency_of_scan_scheduler()),
          _min_scan_concurrency(_state->min_scan_concurrency_of_scanner()) {
    DCHECK(_state != nullptr);
    DCHECK(_output_row_descriptor == nullptr ||
           _output_row_descriptor->tuple_descriptors().size() == 1);
    _query_id = _state->get_query_ctx()->query_id();
    _resource_ctx = _state->get_query_ctx()->resource_ctx();
    ctx_id = UniqueId::gen_uid().to_string();
    for (auto& scanner : _all_scanners) {
        _pending_scanners.push(scanner);
    };
    if (limit < 0) {
        limit = -1;
    }
    _dependency = dependency;
    DorisMetrics::instance()->scanner_ctx_cnt->increment(1);
}

// After init function call, should not access _parent
Status ScannerContext::init() {
#ifndef BE_TEST
    _scanner_profile = _local_state->_scanner_profile;
    _newly_create_free_blocks_num = _local_state->_newly_create_free_blocks_num;
    _scanner_memory_used_counter = _local_state->_memory_used_counter;

    // 3. get thread token
    if (!_state->get_query_ctx()) {
        return Status::InternalError("Query context of {} is not set",
                                     print_id(_state->query_id()));
    }

    if (_state->get_query_ctx()->get_scan_scheduler()) {
        _should_reset_thread_name = false;
    }

    auto scanner = _all_scanners.front().lock();
    DCHECK(scanner != nullptr);

    // TODO: Maybe need refactor.
    // A query could have remote scan task and local scan task at the same time.
    // So we need to compute the _scanner_scheduler in each scan operator instead of query context.
    if (scanner->_scanner->get_storage_type() == TabletStorageType::STORAGE_TYPE_LOCAL) {
        _scanner_scheduler = _state->get_query_ctx()->get_scan_scheduler();
    } else {
        _scanner_scheduler = _state->get_query_ctx()->get_remote_scan_scheduler();
    }
    if (auto* task_executor_scheduler =
                dynamic_cast<TaskExecutorSimplifiedScanScheduler*>(_scanner_scheduler)) {
        std::shared_ptr<TaskExecutor> task_executor = task_executor_scheduler->task_executor();
        vectorized::TaskId task_id(fmt::format("{}-{}", print_id(_state->query_id()), ctx_id));
        _task_handle = DORIS_TRY(task_executor->create_task(
                task_id, []() { return 0.0; },
                config::task_executor_initial_max_concurrency_per_task,
                std::chrono::milliseconds(100), std::nullopt));
    }
#endif
    // _max_bytes_in_queue controls the maximum memory that can be used by a single scan operator.
    // scan_queue_mem_limit on FE is 100MB by default, on backend we will make sure its actual value
    // is larger than 10MB.
    _max_bytes_in_queue = std::max(_state->scan_queue_mem_limit(), (int64_t)1024 * 1024 * 10);

    // Provide more memory for wide tables, increase proportionally by multiples of 300
    _max_bytes_in_queue *= _output_tuple_desc->slots().size() / 300 + 1;

    if (_min_scan_concurrency_of_scan_scheduler == 0) {
        // _scanner_scheduler->get_max_threads() is setted by workload group.
        _min_scan_concurrency_of_scan_scheduler = 2 * _scanner_scheduler->get_max_threads();
    }

    if (_all_scanners.empty()) {
        _is_finished = true;
        _set_scanner_done();
    }

    // The overall target of our system is to make full utilization of the resources.
    // At the same time, we dont want too many tasks are queued by scheduler, that is not necessary.
    // Each scan operator can submit _max_scan_concurrency scanner to scheduelr if scheduler has enough resource.
    // So that for a single query, we can make sure it could make full utilization of the resource.
    _max_scan_concurrency = _state->num_scanner_threads();
    if (_max_scan_concurrency == 0) {
        // Why this is safe:
        /*
            1. If num cpu cores is less than or equal to 24:
                _max_concurrency_of_scan_scheduler will be 96. _parallism_of_scan_operator will be 1 or C/2.
                so _max_scan_concurrency will be 96 or (96 * 2 / C).
                For a single scan node, most scanner it can submit will be 96 or (96 * 2 / C) * (C / 2) which is 96 too.
                So a single scan node could make full utilization of the resource without sumbiting all its tasks.
            2. If num cpu cores greater than 24:
                _max_concurrency_of_scan_scheduler will be 4 * C. _parallism_of_scan_operator will be 1 or C/2.
                so _max_scan_concurrency will be 4 * C or (4 * C * 2 / C).
                For a single scan node, most scanner it can submit will be 4 * C or (4 * C * 2 / C) * (C / 2) which is 4 * C too.

            So, in all situations, when there is only one scan node, it could make full utilization of the resource.
        */
        _max_scan_concurrency =
                _min_scan_concurrency_of_scan_scheduler / _parallism_of_scan_operator;
        _max_scan_concurrency = _max_scan_concurrency == 0 ? 1 : _max_scan_concurrency;
    }

    _max_scan_concurrency = std::min(_max_scan_concurrency, (int32_t)_pending_scanners.size());

    // when user not specify scan_thread_num, so we can try downgrade _max_thread_num.
    // becaue we found in a table with 5k columns, column reader may ocuppy too much memory.
    // you can refer https://github.com/apache/doris/issues/35340 for details.
    const int32_t max_column_reader_num = _state->max_column_reader_num();

    if (_max_scan_concurrency != 1 && max_column_reader_num > 0) {
        int32_t scan_column_num = _output_tuple_desc->slots().size();
        int32_t current_column_num = scan_column_num * _max_scan_concurrency;
        if (current_column_num > max_column_reader_num) {
            int32_t new_max_thread_num = max_column_reader_num / scan_column_num;
            new_max_thread_num = new_max_thread_num <= 0 ? 1 : new_max_thread_num;
            if (new_max_thread_num < _max_scan_concurrency) {
                int32_t origin_max_thread_num = _max_scan_concurrency;
                _max_scan_concurrency = new_max_thread_num;
                LOG(INFO) << "downgrade query:" << print_id(_state->query_id())
                          << " scan's max_thread_num from " << origin_max_thread_num << " to "
                          << _max_scan_concurrency << ",column num: " << scan_column_num
                          << ", max_column_reader_num: " << max_column_reader_num;
            }
        }
    }

    // For select * from table limit 10; should just use one thread.
    if (_local_state->should_run_serial()) {
        _max_scan_concurrency = 1;
        _min_scan_concurrency = 1;
    }

    // Avoid corner case.
    _min_scan_concurrency = std::min(_min_scan_concurrency, _max_scan_concurrency);

    COUNTER_SET(_local_state->_max_scan_concurrency, (int64_t)_max_scan_concurrency);
    COUNTER_SET(_local_state->_min_scan_concurrency, (int64_t)_min_scan_concurrency);

    std::unique_lock<std::mutex> l(_transfer_lock);
    RETURN_IF_ERROR(_scanner_scheduler->schedule_scan_task(shared_from_this(), nullptr, l));

    return Status::OK();
}

ScannerContext::~ScannerContext() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_resource_ctx->memory_context()->mem_tracker());
    _tasks_queue.clear();
    vectorized::BlockUPtr block;
    while (_free_blocks.try_dequeue(block)) {
        // do nothing
    }
    block.reset();
    DorisMetrics::instance()->scanner_ctx_cnt->increment(-1);
    if (_task_handle) {
        if (auto* task_executor_scheduler =
                    dynamic_cast<TaskExecutorSimplifiedScanScheduler*>(_scanner_scheduler)) {
            static_cast<void>(task_executor_scheduler->task_executor()->remove_task(_task_handle));
        }
        _task_handle = nullptr;
    }
}

vectorized::BlockUPtr ScannerContext::get_free_block(bool force) {
    vectorized::BlockUPtr block = nullptr;
    if (_free_blocks.try_dequeue(block)) {
        DCHECK(block->mem_reuse());
        _block_memory_usage -= block->allocated_bytes();
        _scanner_memory_used_counter->set(_block_memory_usage);
        // A free block is reused, so the memory usage should be decreased
        // The caller of get_free_block will increase the memory usage
    } else if (_block_memory_usage < _max_bytes_in_queue || force) {
        _newly_create_free_blocks_num->update(1);
        block = vectorized::Block::create_unique(_output_tuple_desc->slots(), 0,
                                                 true /*ignore invalid slots*/);
    }
    return block;
}

void ScannerContext::return_free_block(vectorized::BlockUPtr block) {
    // If under low memory mode, should not return the freeblock, it will occupy too much memory.
    if (!_local_state->low_memory_mode() && block->mem_reuse() &&
        _block_memory_usage < _max_bytes_in_queue) {
        size_t block_size_to_reuse = block->allocated_bytes();
        _block_memory_usage += block_size_to_reuse;
        _scanner_memory_used_counter->set(_block_memory_usage);
        block->clear_column_data();
        // Free blocks is used to improve memory efficiency. Failure during pushing back
        // free block will not incur any bad result so just ignore the return value.
        _free_blocks.enqueue(std::move(block));
    }
}

Status ScannerContext::submit_scan_task(std::shared_ptr<ScanTask> scan_task,
                                        std::unique_lock<std::mutex>& /*transfer_lock*/) {
    // increase _num_finished_scanners no matter the scan_task is submitted successfully or not.
    // since if submit failed, it will be added back by ScannerContext::push_back_scan_task
    // and _num_finished_scanners will be reduced.
    // if submit succeed, it will be also added back by ScannerContext::push_back_scan_task
    // see ScannerScheduler::_scanner_scan.
    _num_scheduled_scanners++;
    return _scanner_scheduler_global->submit(shared_from_this(), scan_task);
}

void ScannerContext::clear_free_blocks() {
    clear_blocks(_free_blocks);
}

void ScannerContext::push_back_scan_task(std::shared_ptr<ScanTask> scan_task) {
    if (scan_task->status_ok()) {
        for (const auto& [block, _] : scan_task->cached_blocks) {
            if (block->rows() > 0) {
                Status st = validate_block_schema(block.get());
                if (!st.ok()) {
                    scan_task->set_status(st);
                    break;
                }
            }
        }
    }

    std::lock_guard<std::mutex> l(_transfer_lock);
    if (!scan_task->status_ok()) {
        _process_status = scan_task->get_status();
    }
    _tasks_queue.push_back(scan_task);
    _num_scheduled_scanners--;

    _dependency->set_ready();
}

Status ScannerContext::get_block_from_queue(RuntimeState* state, vectorized::Block* block,
                                            bool* eos, int id) {
    if (state->is_cancelled()) {
        _set_scanner_done();
        return state->cancel_reason();
    }
    std::unique_lock l(_transfer_lock);

    if (!_process_status.ok()) {
        _set_scanner_done();
        return _process_status;
    }

    std::shared_ptr<ScanTask> scan_task = nullptr;

    if (!_tasks_queue.empty() && !done()) {
        // https://en.cppreference.com/w/cpp/container/list/front
        // The behavior is undefined if the list is empty.
        scan_task = _tasks_queue.front();
    }

    if (scan_task != nullptr) {
        // The abnormal status of scanner may come from the execution of the scanner itself,
        // or come from the scanner scheduler, such as TooManyTasks.
        if (!scan_task->status_ok()) {
            // TODO: If the scanner status is TooManyTasks, maybe we can retry the scanner after a while.
            _process_status = scan_task->get_status();
            _set_scanner_done();
            return _process_status;
        }

        // No need to worry about small block, block is merged together when they are appended to cached_blocks.
        if (!scan_task->cached_blocks.empty()) {
            auto [current_block, block_size] = std::move(scan_task->cached_blocks.front());
            scan_task->cached_blocks.pop_front();
            _block_memory_usage -= block_size;
            // consume current block
            block->swap(*current_block);
            return_free_block(std::move(current_block));
        }

        VLOG_DEBUG << fmt::format(
                "ScannerContext {} get block from queue, task_queue size {}, current scan "
                "task remaing cached_block size {}, eos {}, scheduled tasks {}",
                ctx_id, _tasks_queue.size(), scan_task->cached_blocks.size(), scan_task->is_eos(),
                _num_scheduled_scanners);

        if (scan_task->cached_blocks.empty()) {
            // All Cached blocks are consumed, pop this task from task_queue.
            if (!_tasks_queue.empty()) {
                _tasks_queue.pop_front();
            }

            if (scan_task->is_eos()) {
                // 1. if eos, record a finished scanner.
                _num_finished_scanners++;
                RETURN_IF_ERROR(
                        _scanner_scheduler->schedule_scan_task(shared_from_this(), nullptr, l));
            } else {
                RETURN_IF_ERROR(
                        _scanner_scheduler->schedule_scan_task(shared_from_this(), scan_task, l));
            }
        }
    }

    if (_num_finished_scanners == _all_scanners.size() && _tasks_queue.empty()) {
        _set_scanner_done();
        _is_finished = true;
    }

    *eos = done();

    if (_tasks_queue.empty()) {
        _dependency->block();
    }

    return Status::OK();
}

Status ScannerContext::validate_block_schema(Block* block) {
    size_t index = 0;
    for (auto& slot : _output_tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        auto& data = block->get_by_position(index++);
        if (data.column->is_nullable() != data.type->is_nullable()) {
            return Status::Error<ErrorCode::INVALID_SCHEMA>(
                    "column(name: {}) nullable({}) does not match type nullable({}), slot(id: "
                    "{}, "
                    "name:{})",
                    data.name, data.column->is_nullable(), data.type->is_nullable(), slot->id(),
                    slot->col_name());
        }

        if (data.column->is_nullable() != slot->is_nullable()) {
            return Status::Error<ErrorCode::INVALID_SCHEMA>(
                    "column(name: {}) nullable({}) does not match slot(id: {}, name: {}) "
                    "nullable({})",
                    data.name, data.column->is_nullable(), slot->id(), slot->col_name(),
                    slot->is_nullable());
        }
    }
    return Status::OK();
}

void ScannerContext::stop_scanners(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_transfer_lock);
    if (_should_stop) {
        return;
    }
    _should_stop = true;
    _set_scanner_done();
    for (const std::weak_ptr<ScannerDelegate>& scanner : _all_scanners) {
        if (std::shared_ptr<ScannerDelegate> sc = scanner.lock()) {
            sc->_scanner->try_stop();
        }
    }
    _tasks_queue.clear();
    if (_task_handle) {
        if (auto* task_executor_scheduler =
                    dynamic_cast<TaskExecutorSimplifiedScanScheduler*>(_scanner_scheduler)) {
            static_cast<void>(task_executor_scheduler->task_executor()->remove_task(_task_handle));
        }
        _task_handle = nullptr;
    }
    // TODO yiguolei, call mark close to scanners
    if (state->enable_profile()) {
        std::stringstream scanner_statistics;
        std::stringstream scanner_rows_read;
        std::stringstream scanner_wait_worker_time;
        std::stringstream scanner_projection;
        scanner_statistics << "[";
        scanner_rows_read << "[";
        scanner_wait_worker_time << "[";
        scanner_projection << "[";
        // Scanners can in 3 state
        //  state 1: in scanner context, not scheduled
        //  state 2: in scanner worker pool's queue, scheduled but not running
        //  state 3: scanner is running.
        for (auto& scanner_ref : _all_scanners) {
            auto scanner = scanner_ref.lock();
            if (scanner == nullptr) {
                continue;
            }
            // Add per scanner running time before close them
            scanner_statistics << PrettyPrinter::print(scanner->_scanner->get_time_cost_ns(),
                                                       TUnit::TIME_NS)
                               << ", ";
            scanner_projection << PrettyPrinter::print(scanner->_scanner->projection_time(),
                                                       TUnit::TIME_NS)
                               << ", ";
            scanner_rows_read << PrettyPrinter::print(scanner->_scanner->get_rows_read(),
                                                      TUnit::UNIT)
                              << ", ";
            scanner_wait_worker_time
                    << PrettyPrinter::print(scanner->_scanner->get_scanner_wait_worker_timer(),
                                            TUnit::TIME_NS)
                    << ", ";
            // since there are all scanners, some scanners is running, so that could not call scanner
            // close here.
        }
        scanner_statistics << "]";
        scanner_rows_read << "]";
        scanner_wait_worker_time << "]";
        scanner_projection << "]";
        _scanner_profile->add_info_string("PerScannerRunningTime", scanner_statistics.str());
        _scanner_profile->add_info_string("PerScannerRowsRead", scanner_rows_read.str());
        _scanner_profile->add_info_string("PerScannerWaitTime", scanner_wait_worker_time.str());
        _scanner_profile->add_info_string("PerScannerProjectionTime", scanner_projection.str());
    }
}

std::string ScannerContext::debug_string() {
    return fmt::format(
            "id: {}, total scanners: {}, pending tasks: {},"
            " _should_stop: {}, _is_finished: {}, free blocks: {},"
            " limit: {}, _num_running_scanners: {}, _max_thread_num: {},"
            " _max_bytes_in_queue: {}, query_id: {}",
            ctx_id, _all_scanners.size(), _tasks_queue.size(), _should_stop, _is_finished,
            _free_blocks.size_approx(), limit, _num_scheduled_scanners, _max_scan_concurrency,
            _max_bytes_in_queue, print_id(_query_id));
}

void ScannerContext::_set_scanner_done() {
    _dependency->set_always_ready();
}

void ScannerContext::update_peak_running_scanner(int num) {
    _local_state->_peak_running_scanner->add(num);
}

int32_t ScannerContext::_get_margin(std::unique_lock<std::mutex>& transfer_lock,
                                    std::unique_lock<std::shared_mutex>& scheduler_lock) {
    // margin_1 is used to ensure each scan operator could have at least _min_scan_concurrency scan tasks.
    int32_t margin_1 = _min_scan_concurrency - (_tasks_queue.size() + _num_scheduled_scanners);

    // margin_2 is used to ensure the scan scheduler could have at least _min_scan_concurrency_of_scan_scheduler scan tasks.
    int32_t margin_2 =
            _min_scan_concurrency_of_scan_scheduler -
            (_scanner_scheduler->get_active_threads() + _scanner_scheduler->get_queue_size());

    if (margin_1 <= 0 && margin_2 <= 0) {
        return 0;
    }

    int32_t margin = std::max(margin_1, margin_2);

    if (low_memory_mode()) {
        // In low memory mode, we will limit the number of running scanners to `low_memory_mode_scanners()`.
        // So that we will not submit too many scan tasks to scheduler.
        margin = std::min(low_memory_mode_scanners() - _num_scheduled_scanners, margin);
    }

    VLOG_DEBUG << fmt::format(
            "[{}|{}] schedule scan task, margin_1: {} = {} - ({} + {}), margin_2: {} = {} - "
            "({} + {}), margin: {}",
            print_id(_query_id), ctx_id, margin_1, _min_scan_concurrency, _tasks_queue.size(),
            _num_scheduled_scanners, margin_2, _min_scan_concurrency_of_scan_scheduler,
            _scanner_scheduler->get_active_threads(), _scanner_scheduler->get_queue_size(), margin);

    return margin;
}

// This function must be called with:
// 1. _transfer_lock held.
// 2. SimplifiedScanScheduler::_lock held.
Status ScannerContext::schedule_scan_task(std::shared_ptr<ScanTask> current_scan_task,
                                          std::unique_lock<std::mutex>& transfer_lock,
                                          std::unique_lock<std::shared_mutex>& scheduler_lock) {
    if (current_scan_task &&
        (!current_scan_task->cached_blocks.empty() || current_scan_task->is_eos())) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Scanner scheduler logical error.");
    }

    std::list<std::shared_ptr<ScanTask>> tasks_to_submit;

    int32_t margin = _get_margin(transfer_lock, scheduler_lock);

    // margin is less than zero. Means this scan operator could not submit any scan task for now.
    if (margin <= 0) {
        // Be careful with current scan task.
        // We need to add it back to task queue to make sure it could be resubmitted.
        if (current_scan_task) {
            // This usually happens when we should downgrade the concurrency.
            _pending_scanners.push(current_scan_task->scanner);
            VLOG_DEBUG << fmt::format(
                    "{} push back scanner to task queue, because diff <= 0, task_queue size "
                    "{}, _num_scheduled_scanners {}",
                    ctx_id, _tasks_queue.size(), _num_scheduled_scanners);
        }

#ifndef NDEBUG
        // This DCHECK is necessary.
        // We need to make sure each scan operator could have at least 1 scan tasks.
        // Or this scan operator will not be re-scheduled.
        if (!_pending_scanners.empty() && _num_scheduled_scanners == 0 && _tasks_queue.empty()) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Scanner scheduler logical error.");
        }
#endif

        return Status::OK();
    }

    bool first_pull = true;

    while (margin-- > 0) {
        std::shared_ptr<ScanTask> task_to_run;
        const int32_t current_concurrency =
                _tasks_queue.size() + _num_scheduled_scanners + tasks_to_submit.size();
        VLOG_DEBUG << fmt::format("{} currenct concurrency: {} = {} + {} + {}", ctx_id,
                                  current_concurrency, _tasks_queue.size(), _num_scheduled_scanners,
                                  tasks_to_submit.size());
        if (first_pull) {
            task_to_run = _pull_next_scan_task(current_scan_task, current_concurrency);
            if (task_to_run == nullptr) {
                // In two situations we will get nullptr.
                // 1. current_concurrency already reached _max_scan_concurrency.
                // 2. all scanners are finished.
                if (current_scan_task) {
                    DCHECK(current_scan_task->cached_blocks.empty());
                    DCHECK(!current_scan_task->is_eos());
                    if (!current_scan_task->cached_blocks.empty() || current_scan_task->is_eos()) {
                        // This should not happen.
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                               "Scanner scheduler logical error.");
                    }
                    // Current scan task is not eos, but we can not resubmit it.
                    // Add current_scan_task back to task queue, so that we have chance to resubmit it in the future.
                    _pending_scanners.push(current_scan_task->scanner);
                }
            }
            first_pull = false;
        } else {
            task_to_run = _pull_next_scan_task(nullptr, current_concurrency);
        }

        if (task_to_run) {
            tasks_to_submit.push_back(task_to_run);
        } else {
            break;
        }
    }

    if (tasks_to_submit.empty()) {
        return Status::OK();
    }

    VLOG_DEBUG << fmt::format("[{}:{}] submit {} scan tasks to scheduler, remaining scanner: {}",
                              print_id(_query_id), ctx_id, tasks_to_submit.size(),
                              _pending_scanners.size());

    for (auto& scan_task_iter : tasks_to_submit) {
        Status submit_status = submit_scan_task(scan_task_iter, transfer_lock);
        if (!submit_status.ok()) {
            _process_status = submit_status;
            _set_scanner_done();
            return _process_status;
        }
    }

    return Status::OK();
}

std::shared_ptr<ScanTask> ScannerContext::_pull_next_scan_task(
        std::shared_ptr<ScanTask> current_scan_task, int32_t current_concurrency) {
    if (current_concurrency >= _max_scan_concurrency) {
        VLOG_DEBUG << fmt::format(
                "ScannerContext {} current concurrency {} >= _max_scan_concurrency {}, skip "
                "pull",
                ctx_id, current_concurrency, _max_scan_concurrency);
        return nullptr;
    }

    if (current_scan_task != nullptr) {
        if (!current_scan_task->cached_blocks.empty() || current_scan_task->is_eos()) {
            // This should not happen.
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Scanner scheduler logical error.");
        }
        return current_scan_task;
    }

    if (!_pending_scanners.empty()) {
        std::weak_ptr<ScannerDelegate> next_scan_task;
        next_scan_task = _pending_scanners.top();
        _pending_scanners.pop();
        return std::make_shared<ScanTask>(next_scan_task);
    } else {
        return nullptr;
    }
}

bool ScannerContext::low_memory_mode() const {
    return _local_state->low_memory_mode();
}

} // namespace doris::vectorized
