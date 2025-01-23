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
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris::vectorized {

using namespace std::chrono_literals;

ScannerContext::ScannerContext(
        RuntimeState* state, pipeline::ScanLocalStateBase* local_state,
        const TupleDescriptor* output_tuple_desc, const RowDescriptor* output_row_descriptor,
        const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners, int64_t limit_,
        std::shared_ptr<pipeline::Dependency> dependency, int num_parallel_instances)
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
          _num_parallel_instances(num_parallel_instances),
          _min_concurrency_of_scan_scheduler(_state->min_scan_concurrency_of_scan_scheduler()),
          _min_concurrency(_state->min_scan_concurrency_of_scanner()) {
    DCHECK(_output_row_descriptor == nullptr ||
           _output_row_descriptor->tuple_descriptors().size() == 1);
    _query_id = _state->get_query_ctx()->query_id();
    ctx_id = UniqueId::gen_uid().to_string();
    for (auto& scanner : _all_scanners) {
        _pending_scanners.push(scanner);
    };
    if (limit < 0) {
        limit = -1;
    }
    _query_thread_context = {_query_id, _state->query_mem_tracker(),
                             _state->get_query_ctx()->workload_group()};
    _dependency = dependency;
    if (_min_concurrency_of_scan_scheduler == 0) {
        _min_concurrency_of_scan_scheduler = 2 * config::doris_scanner_thread_pool_thread_num;
    }
    DorisMetrics::instance()->scanner_ctx_cnt->increment(1);
}

// After init function call, should not access _parent
Status ScannerContext::init() {
    _scanner_profile = _local_state->_scanner_profile;
    _newly_create_free_blocks_num = _local_state->_newly_create_free_blocks_num;
    _scanner_memory_used_counter = _local_state->_memory_used_counter;

#ifndef BE_TEST
    // 3. get thread token
    if (!_state->get_query_ctx()) {
        return Status::InternalError("Query context of {} is not set",
                                     print_id(_state->query_id()));
    }

    thread_token = _state->get_query_ctx()->get_token();

    if (_state->get_query_ctx()->get_scan_scheduler()) {
        _should_reset_thread_name = false;
    }

#endif
    _local_state->_runtime_profile->add_info_string("UseSpecificThreadToken",
                                                    thread_token == nullptr ? "False" : "True");

    // _max_bytes_in_queue controls the maximum memory that can be used by a single scan instance.
    // scan_queue_mem_limit on FE is 100MB by default, on backend we will make sure its actual value
    // is larger than 10MB.
    _max_bytes_in_queue = std::max(_state->scan_queue_mem_limit(), (int64_t)1024 * 1024 * 10);

    // Provide more memory for wide tables, increase proportionally by multiples of 300
    _max_bytes_in_queue *= _output_tuple_desc->slots().size() / 300 + 1;

    // TODO: Where is the proper position to place this code?
    if (_all_scanners.empty()) {
        _is_finished = true;
        _set_scanner_done();
    }

    auto scanner = _all_scanners.front().lock();
    DCHECK(scanner != nullptr);
    // A query could have remote scan task and local scan task at the same time.
    // So we need to compute the _scanner_scheduler in each scan operator instead of query context.
    SimplifiedScanScheduler* simple_scan_scheduler = _state->get_query_ctx()->get_scan_scheduler();
    SimplifiedScanScheduler* remote_scan_task_scheduler =
            _state->get_query_ctx()->get_remote_scan_scheduler();
    if (scanner->_scanner->get_storage_type() == TabletStorageType::STORAGE_TYPE_LOCAL) {
        // scan_scheduler could be empty if query does not have a workload group.
        if (simple_scan_scheduler) {
            _scanner_scheduler = simple_scan_scheduler;
        } else {
            _scanner_scheduler = _scanner_scheduler_global->get_local_scan_thread_pool();
        }
    } else {
        // remote_scan_task_scheduler could be empty if query does not have a workload group.
        if (remote_scan_task_scheduler) {
            _scanner_scheduler = remote_scan_task_scheduler;
        } else {
            _scanner_scheduler = _scanner_scheduler_global->get_remote_scan_thread_pool();
        }
    }

    // The overall target of our system is to make full utilization of the resources.
    // At the same time, we dont want too many tasks are queued by scheduler, that is not necessary.
    _max_concurrency = _state->num_scanner_threads();
    if (_max_concurrency == 0) {
            _max_concurrency = _min_concurrency_of_scan_scheduler / _num_parallel_instances;
            // In some rare cases, user may set num_parallel_instances to 1 handly to make many query could be executed
            // in parallel. We need to make sure the _max_thread_num is smaller than previous value in this situation.
            _max_concurrency =
                    std::min(_max_concurrency, config::doris_scanner_thread_pool_thread_num);
        }
        _max_concurrency = _max_concurrency == 0 ? 1 : _max_concurrency;
    }

    _max_concurrency = std::min(_max_concurrency, (int32_t)_all_scanners.size());

    // when user not specify scan_thread_num, so we can try downgrade _max_thread_num.
    // becaue we found in a table with 5k columns, column reader may ocuppy too much memory.
    // you can refer https://github.com/apache/doris/issues/35340 for details.
    int32_t max_column_reader_num = _state->query_options().max_column_reader_num;

    if (_max_concurrency != 1 && max_column_reader_num > 0) {
        int32_t scan_column_num = _output_tuple_desc->slots().size();
        int32_t current_column_num = scan_column_num * _max_concurrency;
        if (current_column_num > max_column_reader_num) {
            int32_t new_max_thread_num = max_column_reader_num / scan_column_num;
            new_max_thread_num = new_max_thread_num <= 0 ? 1 : new_max_thread_num;
            if (new_max_thread_num < _max_concurrency) {
                int32_t origin_max_thread_num = _max_concurrency;
                _max_concurrency = new_max_thread_num;
                LOG(INFO) << "downgrade query:" << print_id(_state->query_id())
                          << " scan's max_thread_num from " << origin_max_thread_num << " to "
                          << _max_concurrency << ",column num: " << scan_column_num
                          << ", max_column_reader_num: " << max_column_reader_num;
            }
        }
    }

    // Each scan operator can submit _basic_margin scanner to scheduelr if scheduler has enough resource.
    // So that for a single query, we can make sure it could make full utilization of the resource.
    _basic_margin = _serial_scan_operator ? _max_concurrency
                                          : _min_concurrency_of_scan_scheduler /
                                                    (_state->query_parallel_instance_num());

    // Make sure the _basic_margin is not too large.
    _basic_margin = std::min(_basic_margin, _max_concurrency);

    // For select * from table limit 10; should just use one thread.
    if (_local_state->should_run_serial()) {
        _max_concurrency = 1;
        _min_concurrency = 1;
        _basic_margin = 1;
    }

    // Avoid corner case.
    _min_concurrency = std::min(_min_concurrency, _max_concurrency);

    COUNTER_SET(_local_state->_max_concurency, (int64_t)_max_concurrency);
    COUNTER_SET(_local_state->_min_concurency, (int64_t)_min_concurrency);

    std::unique_lock<std::mutex> l(_transfer_lock);
    RETURN_IF_ERROR(_scanner_scheduler->schedule_scan_task(shared_from_this(), nullptr, l));

    return Status::OK();
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
    if (block->mem_reuse() && _block_memory_usage < _max_bytes_in_queue) {
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

    {
        std::lock_guard<std::mutex> l(_transfer_lock);
        if (!scan_task->status_ok()) {
            _process_status = scan_task->get_status();
        }
        _tasks_queue.push_back(scan_task);
        _num_scheduled_scanners--;
    }

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

    if (!_tasks_queue.empty()) {
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
            }

            RETURN_IF_ERROR(
                    _scanner_scheduler->schedule_scan_task(shared_from_this(), scan_task, l));
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
        if (!slot->need_materialize()) {
            continue;
        }
        auto& data = block->get_by_position(index++);
        if (data.column->is_nullable() != data.type->is_nullable()) {
            return Status::Error<ErrorCode::INVALID_SCHEMA>(
                    "column(name: {}) nullable({}) does not match type nullable({}), slot(id: {}, "
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
            _free_blocks.size_approx(), limit, _num_scheduled_scanners, _max_concurrency,
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
    int32_t margin_1 = _min_concurrency - (_tasks_queue.size() + _num_scheduled_scanners);

    // margin_2 is used to ensure the scan scheduler could have at least _min_scan_concurrency_of_scan_scheduler scan tasks.
    int32_t margin_2 =
            _min_concurrency_of_scan_scheduler -
            (_scanner_scheduler->get_active_threads() + _scanner_scheduler->get_queue_size());

    if (margin_1 <= 0 && margin_2 <= 0) {
        return 0;
    }

    int32_t margin = std::max(margin_1, margin_2);
    margin = std::min(margin, _basic_margin);

    VLOG_DEBUG << fmt::format(
            "[{}|{}] schedule scan task, margin_1: {} = {} - ({} + {}), margin_2: {} = {} - "
            "({} + {}), margin: {}",
            print_id(_query_id), ctx_id, margin_1, _min_concurrency, _tasks_queue.size(),
            _num_scheduled_scanners, margin_2, _min_concurrency_of_scan_scheduler,
            _scanner_scheduler->get_active_threads(), _scanner_scheduler->get_queue_size(), margin);

    return margin;
}

// This function must be called with:
// 1. _transfer_lock held.
// 2. SimplifiedScanScheduler::_lock held.
Status ScannerContext::_schedule_scan_task(std::shared_ptr<ScanTask> current_scan_task,
                                           std::unique_lock<std::mutex>& transfer_lock,
                                           std::unique_lock<std::shared_mutex>& scheduler_lock) {
    std::list<std::shared_ptr<ScanTask>> tasks_to_submit;

    int32_t margin = _get_margin(transfer_lock, scheduler_lock);

    // margin is less than zero. Means this scan operator could not submit any scan task for now.
    if (margin <= 0) {
        // Be careful with current scan task.
        // We need to add it back to task queue to make sure it could be resubmitted.
        if (current_scan_task && current_scan_task->cached_blocks.empty() &&
            !current_scan_task->is_eos()) {
            // This usually happens when we should downgrade the concurrency.
            _pending_scanners.push(current_scan_task->scanner);
            VLOG_DEBUG << fmt::format(
                    "{} push back scanner to task queue, because diff <= 0, task_queue size "
                    "{}, _num_scheduled_scanners {}",
                    ctx_id, _tasks_queue.size(), _num_scheduled_scanners);
        }
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
                // 1. current_concurrency already reached _max_concurrency.
                // 2. all scanners are finished.
                if (current_scan_task && current_scan_task->cached_blocks.empty() &&
                    !current_scan_task->is_eos()) {
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
    if (current_concurrency >= _max_concurrency) {
        VLOG_DEBUG << fmt::format(
                "ScannerContext {} current concurrency {} >= _max_concurrency {}, skip pull",
                ctx_id, current_concurrency, _max_concurrency);
        return nullptr;
    }

    if (current_scan_task != nullptr) {
        if (current_scan_task->cached_blocks.empty() && !current_scan_task->is_eos()) {
            return current_scan_task;
        }
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

} // namespace doris::vectorized
