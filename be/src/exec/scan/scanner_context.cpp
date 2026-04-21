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

#include "exec/scan/scanner_context.h"

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
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/operator/scan_operator.h"
#include "exec/scan/scan_node.h"
#include "exec/scan/scanner_scheduler.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/tablet/tablet.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {

using namespace std::chrono_literals;

// ==================== ScannerContext ====================
ScannerContext::ScannerContext(RuntimeState* state, ScanLocalStateBase* local_state,
                               const TupleDescriptor* output_tuple_desc,
                               const RowDescriptor* output_row_descriptor,
                               const std::list<std::shared_ptr<ScannerDelegate>>& scanners,
                               int64_t limit_, std::shared_ptr<Dependency> dependency,
                               std::atomic<int64_t>* shared_scan_limit,
                               std::shared_ptr<MemShareArbitrator> arb,
                               std::shared_ptr<MemLimiter> limiter, int ins_idx,
                               bool enable_adaptive_scan
#ifdef BE_TEST
                               ,
                               int num_parallel_instances
#endif
                               )
        : HasTaskExecutionCtx(state),
          _state(state),
          _local_state(local_state),
          _output_tuple_desc(output_row_descriptor
                                     ? output_row_descriptor->tuple_descriptors().front()
                                     : output_tuple_desc),
          _output_row_descriptor(output_row_descriptor),
          _batch_size(state->batch_size()),
          limit(limit_),
          _shared_scan_limit(shared_scan_limit),
          _all_scanners(scanners.begin(), scanners.end()),
#ifndef BE_TEST
          _scanner_scheduler(local_state->scan_scheduler(state)),
          _min_scan_concurrency_of_scan_scheduler(
                  _scanner_scheduler->get_min_active_scan_threads()),
          _max_scan_concurrency(std::min(local_state->max_scanners_concurrency(state),
                                         cast_set<int>(scanners.size()))),
#else
          _scanner_scheduler(state->get_query_ctx()->get_scan_scheduler()),
          _min_scan_concurrency_of_scan_scheduler(0),
          _max_scan_concurrency(num_parallel_instances),
#endif
          _min_scan_concurrency(local_state->min_scanners_concurrency(state)),
          _scanner_mem_limiter(limiter),
          _mem_share_arb(arb),
          _ins_idx(ins_idx),
          _enable_adaptive_scanners(enable_adaptive_scan) {
    DCHECK(_state != nullptr);
    DCHECK(_output_row_descriptor == nullptr ||
           _output_row_descriptor->tuple_descriptors().size() == 1);
    _query_id = _state->get_query_ctx()->query_id();
    _resource_ctx = _state->get_query_ctx()->resource_ctx();
    ctx_id = UniqueId::gen_uid().to_string();
    for (auto& scanner : _all_scanners) {
        _pending_tasks.push(std::make_shared<ScanTask>(scanner));
    }
    if (limit < 0) {
        limit = -1;
    }
    _dependency = dependency;
    // Initialize adaptive processor
    _adaptive_processor = ScannerAdaptiveProcessor::create_shared();
    DorisMetrics::instance()->scanner_ctx_cnt->increment(1);
}

int64_t ScannerContext::acquire_limit_quota(int64_t desired) {
    DCHECK(desired > 0);
    int64_t remaining = _shared_scan_limit->load(std::memory_order_acquire);
    while (true) {
        if (remaining < 0) {
            // No limit set, grant all desired rows.
            return desired;
        }
        if (remaining == 0) {
            return 0;
        }
        int64_t granted = std::min(desired, remaining);
        if (_shared_scan_limit->compare_exchange_weak(remaining, remaining - granted,
                                                      std::memory_order_acq_rel,
                                                      std::memory_order_acquire)) {
            return granted;
        }
        // CAS failed, `remaining` is updated to current value, retry.
    }
}

void ScannerContext::_adjust_scan_mem_limit(int64_t old_value, int64_t new_value) {
    if (!_enable_adaptive_scanners) {
        return;
    }

    int64_t new_scan_mem_limit = _mem_share_arb->update_mem_bytes(old_value, new_value);
    _scanner_mem_limiter->update_mem_limit(new_scan_mem_limit);
    _scanner_mem_limiter->update_arb_mem_bytes(new_value);

    VLOG_DEBUG << fmt::format(
            "adjust_scan_mem_limit. context = {}, new mem scan limit = {}, scanner mem bytes = {} "
            "-> {}",
            debug_string(), new_scan_mem_limit, old_value, new_value);
}

int ScannerContext::_available_pickup_scanner_count() {
    if (!_enable_adaptive_scanners) {
        return _max_scan_concurrency;
    }

    int min_scanners = std::max(1, _min_scan_concurrency);
    int max_scanners = _scanner_mem_limiter->available_scanner_count(_ins_idx);
    max_scanners = std::min(max_scanners, _max_scan_concurrency);
    min_scanners = std::min(min_scanners, max_scanners);
    if (_ins_idx == 0) {
        // Adjust memory limit via memory share arbitrator
        _adjust_scan_mem_limit(_scanner_mem_limiter->get_arb_scanner_mem_bytes(),
                               _scanner_mem_limiter->get_estimated_block_mem_bytes());
    }

    ScannerAdaptiveProcessor& P = *_adaptive_processor;
    int& scanners = P.expected_scanners;
    int64_t now = UnixMillis();
    // Avoid frequent adjustment - only adjust every 100ms
    if (now - P.adjust_scanners_last_timestamp <= config::doris_scanner_dynamic_interval_ms) {
        return scanners;
    }
    P.adjust_scanners_last_timestamp = now;
    auto old_scanners = P.expected_scanners;

    scanners = std::max(min_scanners, scanners);
    scanners = std::min(max_scanners, scanners);
    VLOG_DEBUG << fmt::format(
            "_available_pickup_scanner_count. context = {}, old_scanners = {}, scanners = {} "
            ", min_scanners: {}, max_scanners: {}",
            debug_string(), old_scanners, scanners, min_scanners, max_scanners);

    // TODO(gabriel): Scanners are scheduled adaptively based on the memory usage now.
    return scanners;
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

    if (auto* task_executor_scheduler =
                dynamic_cast<TaskExecutorSimplifiedScanScheduler*>(_scanner_scheduler)) {
        std::shared_ptr<TaskExecutor> task_executor = task_executor_scheduler->task_executor();
        TaskId task_id(fmt::format("{}-{}", print_id(_state->query_id()), ctx_id));
        _task_handle = DORIS_TRY(task_executor->create_task(
                task_id, []() { return 0.0; },
                config::task_executor_initial_max_concurrency_per_task > 0
                        ? config::task_executor_initial_max_concurrency_per_task
                        : std::max(48, CpuInfo::num_cores() * 2),
                std::chrono::milliseconds(100), std::nullopt));
    }
#endif
    // _max_bytes_in_queue controls the maximum memory that can be used by a single scan operator.
    // scan_queue_mem_limit on FE is 100MB by default, on backend we will make sure its actual value
    // is larger than 10MB.
    _max_bytes_in_queue = std::max(_state->scan_queue_mem_limit(), (int64_t)1024 * 1024 * 10);

    // Provide more memory for wide tables, increase proportionally by multiples of 300
    _max_bytes_in_queue *= _output_tuple_desc->slots().size() / 300 + 1;

    if (_all_scanners.empty()) {
        _is_finished = true;
        _set_scanner_done();
    }

    // Initialize memory limiter if memory-aware scheduling is enabled
    if (_enable_adaptive_scanners) {
        DCHECK(_scanner_mem_limiter && _mem_share_arb);
        int64_t c = _scanner_mem_limiter->update_open_tasks_count(1);
        // TODO(gabriel): set estimated block size
        _scanner_mem_limiter->reestimated_block_mem_bytes(DEFAULT_SCANNER_MEM_BYTES);
        _scanner_mem_limiter->update_arb_mem_bytes(DEFAULT_SCANNER_MEM_BYTES);
        if (c == 0) {
            // First scanner context to open, adjust scan memory limit
            _adjust_scan_mem_limit(DEFAULT_SCANNER_MEM_BYTES,
                                   _scanner_mem_limiter->get_arb_scanner_mem_bytes());
        }
    }

    // when user not specify scan_thread_num, so we can try downgrade _max_thread_num.
    // becaue we found in a table with 5k columns, column reader may ocuppy too much memory.
    // you can refer https://github.com/apache/doris/issues/35340 for details.
    const int32_t max_column_reader_num = _state->max_column_reader_num();

    if (_max_scan_concurrency != 1 && max_column_reader_num > 0) {
        int32_t scan_column_num = cast_set<int32_t>(_output_tuple_desc->slots().size());
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

    COUNTER_SET(_local_state->_max_scan_concurrency, (int64_t)_max_scan_concurrency);
    COUNTER_SET(_local_state->_min_scan_concurrency, (int64_t)_min_scan_concurrency);

    std::unique_lock<std::mutex> l(_transfer_lock);
    RETURN_IF_ERROR(_scanner_scheduler->schedule_scan_task(shared_from_this(), nullptr, l));

    return Status::OK();
}

ScannerContext::~ScannerContext() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_resource_ctx->memory_context()->mem_tracker());
    _completed_tasks.clear();
    BlockUPtr block;
    while (_free_blocks.try_dequeue(block)) {
        // do nothing
    }
    block.reset();
    DorisMetrics::instance()->scanner_ctx_cnt->increment(-1);

    // Cleanup memory limiter if last context closing
    if (_enable_adaptive_scanners) {
        if (_scanner_mem_limiter->update_open_tasks_count(-1) == 1) {
            // Last scanner context to close, reset scan memory limit
            _adjust_scan_mem_limit(_scanner_mem_limiter->get_arb_scanner_mem_bytes(), 0);
        }
    }

    if (_task_handle) {
        if (auto* task_executor_scheduler =
                    dynamic_cast<TaskExecutorSimplifiedScanScheduler*>(_scanner_scheduler)) {
            static_cast<void>(task_executor_scheduler->task_executor()->remove_task(_task_handle));
        }
        _task_handle = nullptr;
    }
}

BlockUPtr ScannerContext::get_free_block(bool force) {
    BlockUPtr block = nullptr;
    if (_free_blocks.try_dequeue(block)) {
        DCHECK(block->mem_reuse());
        _block_memory_usage -= block->allocated_bytes();
        _scanner_memory_used_counter->set(_block_memory_usage);
        // A free block is reused, so the memory usage should be decreased
        // The caller of get_free_block will increase the memory usage
    } else if (_block_memory_usage < _max_bytes_in_queue || force) {
        _newly_create_free_blocks_num->update(1);
        block = Block::create_unique(_output_tuple_desc->slots(), 0);
    }
    return block;
}

void ScannerContext::return_free_block(BlockUPtr block) {
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
    _in_flight_tasks_num++;
    return _scanner_scheduler->submit(shared_from_this(), scan_task);
}

void ScannerContext::clear_free_blocks() {
    clear_blocks(_free_blocks);
}

void ScannerContext::push_back_scan_task(std::shared_ptr<ScanTask> scan_task) {
    if (scan_task->status_ok()) {
        if (scan_task->cached_block && scan_task->cached_block->rows() > 0) {
            Status st = validate_block_schema(scan_task->cached_block.get());
            if (!st.ok()) {
                scan_task->set_status(st);
            }
        }
    }

    std::lock_guard<std::mutex> l(_transfer_lock);
    if (!scan_task->status_ok()) {
        _process_status = scan_task->get_status();
    }
    _completed_tasks.push_back(scan_task);
    _in_flight_tasks_num--;

    _dependency->set_ready();
}

Status ScannerContext::get_block_from_queue(RuntimeState* state, Block* block, bool* eos, int id) {
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

    if (!_completed_tasks.empty() && !done()) {
        // https://en.cppreference.com/w/cpp/container/list/front
        // The behavior is undefined if the list is empty.
        scan_task = _completed_tasks.front();
        _completed_tasks.pop_front();
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

        if (scan_task->cached_block) {
            // No need to worry about small block, block is merged together when they are appended to cached_blocks.
            auto current_block = std::move(scan_task->cached_block);
            auto block_size = current_block->allocated_bytes();
            scan_task->cached_block.reset();
            _block_memory_usage -= block_size;
            // consume current block
            block->swap(*current_block);
            return_free_block(std::move(current_block));
        }
        VLOG_DEBUG << fmt::format(
                "ScannerContext {} get block from queue, current scan "
                "task remaing cached_block size {}, eos {}, scheduled tasks {}",
                ctx_id, _completed_tasks.size(), scan_task->is_eos(), _in_flight_tasks_num);
        if (scan_task->is_eos()) {
            // 1. if eos, record a finished scanner.
            _num_finished_scanners++;
            RETURN_IF_ERROR(_scanner_scheduler->schedule_scan_task(shared_from_this(), nullptr, l));
        } else {
            scan_task->set_state(ScanTask::State::IN_FLIGHT);
            RETURN_IF_ERROR(
                    _scanner_scheduler->schedule_scan_task(shared_from_this(), scan_task, l));
        }
    }

    // Mark finished when either:
    // (1) all scanners completed normally, or
    // (2) shared limit exhausted and no scanners are still running.
    if (_completed_tasks.empty() &&
        (_num_finished_scanners == _all_scanners.size() ||
         (_shared_scan_limit->load(std::memory_order_acquire) == 0 && _in_flight_tasks_num == 0))) {
        _set_scanner_done();
        _is_finished = true;
    }

    *eos = done();

    if (_completed_tasks.empty()) {
        _dependency->block();
    }

    return Status::OK();
}

Status ScannerContext::validate_block_schema(Block* block) {
    size_t index = 0;
    for (auto& slot : _output_tuple_desc->slots()) {
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
    _completed_tasks.clear();
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
            "_query_id: {}, id: {}, total scanners: {}, pending tasks: {}, completed tasks: {},"
            " _should_stop: {}, _is_finished: {}, free blocks: {},"
            " limit: {}, _in_flight_tasks_num: {}, remaining_limit: {}, _num_running_scanners: {}, "
            "_max_thread_num: {},"
            " _max_bytes_in_queue: {}, _ins_idx: {}, _enable_adaptive_scanners: {}, "
            "_mem_share_arb: {}, _scanner_mem_limiter: {}",
            print_id(_query_id), ctx_id, _all_scanners.size(), _pending_tasks.size(),
            _completed_tasks.size(), _should_stop, _is_finished, _free_blocks.size_approx(), limit,
            _shared_scan_limit->load(std::memory_order_relaxed), _in_flight_tasks_num,
            _num_finished_scanners, _max_scan_concurrency, _max_bytes_in_queue, _ins_idx,
            _enable_adaptive_scanners,
            _enable_adaptive_scanners ? _mem_share_arb->debug_string() : "NULL",
            _enable_adaptive_scanners ? _scanner_mem_limiter->debug_string() : "NULL");
}

void ScannerContext::_set_scanner_done() {
    _dependency->set_always_ready();
}

void ScannerContext::update_peak_running_scanner(int num) {
#ifndef BE_TEST
    _local_state->_peak_running_scanner->add(num);
#endif
    if (_enable_adaptive_scanners) {
        _scanner_mem_limiter->update_running_tasks_count(num);
    }
}

void ScannerContext::reestimated_block_mem_bytes(int64_t num) {
    if (_enable_adaptive_scanners) {
        _scanner_mem_limiter->reestimated_block_mem_bytes(num);
    }
}

int32_t ScannerContext::_get_margin(std::unique_lock<std::mutex>& transfer_lock,
                                    std::unique_lock<std::shared_mutex>& scheduler_lock) {
    // Get effective max concurrency considering adaptive scheduling
    int32_t effective_max_concurrency = _available_pickup_scanner_count();
    DCHECK_LE(effective_max_concurrency, _max_scan_concurrency);

    // margin_1 is used to ensure each scan operator could have at least _min_scan_concurrency scan tasks.
    int32_t margin_1 = _min_scan_concurrency -
                       (cast_set<int32_t>(_completed_tasks.size()) + _in_flight_tasks_num);

    // margin_2 is used to ensure the scan scheduler could have at least _min_scan_concurrency_of_scan_scheduler scan tasks.
    int32_t margin_2 =
            _min_scan_concurrency_of_scan_scheduler -
            (_scanner_scheduler->get_active_threads() + _scanner_scheduler->get_queue_size());

    // margin_3 is used to respect adaptive max concurrency limit
    int32_t margin_3 =
            std::max(effective_max_concurrency -
                             (cast_set<int32_t>(_completed_tasks.size()) + _in_flight_tasks_num),
                     1);

    if (margin_1 <= 0 && margin_2 <= 0) {
        return 0;
    }

    int32_t margin = std::max(margin_1, margin_2);
    if (_enable_adaptive_scanners) {
        margin = std::min(margin, margin_3); // Cap by adaptive limit
    }

    if (low_memory_mode()) {
        // In low memory mode, we will limit the number of running scanners to `low_memory_mode_scanners()`.
        // So that we will not submit too many scan tasks to scheduler.
        margin = std::min(low_memory_mode_scanners() - _in_flight_tasks_num, margin);
    }

    VLOG_DEBUG << fmt::format(
            "[{}|{}] schedule scan task, margin_1: {} = {} - ({} + {}), margin_2: {} = {} - "
            "({} + {}), margin_3: {} = {} - ({} + {}), margin: {}, adaptive: {}",
            print_id(_query_id), ctx_id, margin_1, _min_scan_concurrency, _completed_tasks.size(),
            _in_flight_tasks_num, margin_2, _min_scan_concurrency_of_scan_scheduler,
            _scanner_scheduler->get_active_threads(), _scanner_scheduler->get_queue_size(),
            margin_3, effective_max_concurrency, _completed_tasks.size(), _in_flight_tasks_num,
            margin, _enable_adaptive_scanners);

    return margin;
}

// This function must be called with:
// 1. _transfer_lock held.
// 2. ScannerScheduler::_lock held.
Status ScannerContext::schedule_scan_task(std::shared_ptr<ScanTask> current_scan_task,
                                          std::unique_lock<std::mutex>& transfer_lock,
                                          std::unique_lock<std::shared_mutex>& scheduler_lock) {
    if (current_scan_task &&
        (current_scan_task->cached_block != nullptr || current_scan_task->is_eos())) {
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
            current_scan_task->set_state(ScanTask::State::PENDING);
            _pending_tasks.push(current_scan_task);
            VLOG_DEBUG << fmt::format(
                    "{} push back scanner to task queue, because diff <= 0, _completed_tasks size "
                    "{}, _in_flight_tasks_num {}",
                    ctx_id, _completed_tasks.size(), _in_flight_tasks_num);
        }

#ifndef NDEBUG
        // This DCHECK is necessary.
        // We need to make sure each scan operator could have at least 1 scan tasks.
        // Or this scan operator will not be re-scheduled.
        if (!_pending_tasks.empty() && _in_flight_tasks_num == 0 && _completed_tasks.empty()) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Scanner scheduler logical error.");
        }
#endif

        return Status::OK();
    }

    bool first_pull = true;

    while (margin-- > 0) {
        std::shared_ptr<ScanTask> task_to_run;
        const int32_t current_concurrency = cast_set<int32_t>(
                _completed_tasks.size() + _in_flight_tasks_num + tasks_to_submit.size());
        VLOG_DEBUG << fmt::format("{} currenct concurrency: {} = {} + {} + {}", ctx_id,
                                  current_concurrency, _completed_tasks.size(),
                                  _in_flight_tasks_num, tasks_to_submit.size());
        if (first_pull) {
            task_to_run = _pull_next_scan_task(current_scan_task, current_concurrency);
            if (task_to_run == nullptr) {
                // In two situations we will get nullptr.
                // 1. current_concurrency already reached _max_scan_concurrency.
                // 2. all scanners are finished.
                if (current_scan_task) {
                    DCHECK(current_scan_task->cached_block == nullptr);
                    DCHECK(!current_scan_task->is_eos());
                    if (current_scan_task->cached_block != nullptr || current_scan_task->is_eos()) {
                        // This should not happen.
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                               "Scanner scheduler logical error.");
                    }
                    // Current scan task is not scheduled, we need to add it back to task queue to make sure it could be resubmitted.
                    current_scan_task->set_state(ScanTask::State::PENDING);
                    _pending_tasks.push(current_scan_task);
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
                              _pending_tasks.size());

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
    int32_t effective_max_concurrency = _max_scan_concurrency;
    if (_enable_adaptive_scanners) {
        effective_max_concurrency = _adaptive_processor->expected_scanners > 0
                                            ? _adaptive_processor->expected_scanners
                                            : _max_scan_concurrency;
    }

    if (current_concurrency >= effective_max_concurrency) {
        VLOG_DEBUG << fmt::format(
                "ScannerContext {} current concurrency {} >= effective_max_concurrency {}, skip "
                "pull",
                ctx_id, current_concurrency, effective_max_concurrency);
        return nullptr;
    }

    if (current_scan_task != nullptr) {
        if (current_scan_task->cached_block != nullptr || current_scan_task->is_eos()) {
            // This should not happen.
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Scanner scheduler logical error.");
        }
        return current_scan_task;
    }

    if (!_pending_tasks.empty()) {
        // If shared limit quota is exhausted, do not submit new scanners from pending queue.
        int64_t remaining = _shared_scan_limit->load(std::memory_order_acquire);
        if (remaining == 0) {
            return nullptr;
        }
        std::shared_ptr<ScanTask> next_scan_task;
        next_scan_task = _pending_tasks.top();
        _pending_tasks.pop();
        return next_scan_task;
    } else {
        return nullptr;
    }
}

bool ScannerContext::low_memory_mode() const {
    return _local_state->low_memory_mode();
}
} // namespace doris
