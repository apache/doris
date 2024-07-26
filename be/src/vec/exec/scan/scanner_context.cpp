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

#include <mutex>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "pipeline/exec/scan_operator.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris::vectorized {

using namespace std::chrono_literals;

ScannerContext::ScannerContext(
        RuntimeState* state, pipeline::ScanLocalStateBase* local_state,
        const TupleDescriptor* output_tuple_desc, const RowDescriptor* output_row_descriptor,
        const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners, int64_t limit_,
        int64_t max_bytes_in_blocks_queue, std::shared_ptr<pipeline::Dependency> dependency,
        const int num_parallel_instances)
        : HasTaskExecutionCtx(state),
          _state(state),
          _local_state(local_state),
          _output_tuple_desc(output_row_descriptor
                                     ? output_row_descriptor->tuple_descriptors().front()
                                     : output_tuple_desc),
          _output_row_descriptor(output_row_descriptor),
          _batch_size(state->batch_size()),
          limit(limit_),
          _max_bytes_in_queue(std::max(max_bytes_in_blocks_queue, (int64_t)1024) *
                              num_parallel_instances),
          _scanner_scheduler(state->exec_env()->scanner_scheduler()),
          _all_scanners(scanners.begin(), scanners.end()),
          _num_parallel_instances(num_parallel_instances) {
    DCHECK(_output_row_descriptor == nullptr ||
           _output_row_descriptor->tuple_descriptors().size() == 1);
    _query_id = _state->get_query_ctx()->query_id();
    ctx_id = UniqueId::gen_uid().to_string();
    // Provide more memory for wide tables, increase proportionally by multiples of 300
    _max_bytes_in_queue *= _output_tuple_desc->slots().size() / 300 + 1;
    if (scanners.empty()) {
        _is_finished = true;
        _set_scanner_done();
    }
    _scanners.enqueue_bulk(scanners.begin(), scanners.size());
    if (limit < 0) {
        limit = -1;
    }
    MAX_SCALE_UP_RATIO = _state->scanner_scale_up_ratio();
    _max_thread_num = _state->num_scanner_threads() > 0
                              ? _state->num_scanner_threads()
                              : config::doris_scanner_thread_pool_thread_num /
                                        (_local_state ? num_parallel_instances
                                                      : state->query_parallel_instance_num());
    _max_thread_num = _max_thread_num == 0 ? 1 : _max_thread_num;
    _max_thread_num = std::min(_max_thread_num, (int32_t)scanners.size());
    // 1. Calculate max concurrency
    // For select * from table limit 10; should just use one thread.
    if (_local_state && _local_state->should_run_serial()) {
        _max_thread_num = 1;
    }
    // when user not specify scan_thread_num, so we can try downgrade _max_thread_num.
    // becaue we found in a table with 5k columns, column reader may ocuppy too much memory.
    // you can refer https://github.com/apache/doris/issues/35340 for details.
    int32_t max_column_reader_num = state->query_options().max_column_reader_num;
    if (_max_thread_num != 1 && max_column_reader_num > 0) {
        int32_t scan_column_num = _output_tuple_desc->slots().size();
        int32_t current_column_num = scan_column_num * _max_thread_num;
        if (current_column_num > max_column_reader_num) {
            int32_t new_max_thread_num = max_column_reader_num / scan_column_num;
            new_max_thread_num = new_max_thread_num <= 0 ? 1 : new_max_thread_num;
            if (new_max_thread_num < _max_thread_num) {
                int32_t origin_max_thread_num = _max_thread_num;
                _max_thread_num = new_max_thread_num;
                LOG(INFO) << "downgrade query:" << print_id(state->query_id())
                          << " scan's max_thread_num from " << origin_max_thread_num << " to "
                          << _max_thread_num << ",column num: " << scan_column_num
                          << ", max_column_reader_num: " << max_column_reader_num;
            }
        }
    }

    _query_thread_context = {_query_id, _state->query_mem_tracker(),
                             _state->get_query_ctx()->workload_group()};
    _dependency = dependency;
}

// After init function call, should not access _parent
Status ScannerContext::init() {
    _scanner_profile = _local_state->_scanner_profile;
    _scanner_sched_counter = _local_state->_scanner_sched_counter;
    _newly_create_free_blocks_num = _local_state->_newly_create_free_blocks_num;
    _scanner_wait_batch_timer = _local_state->_scanner_wait_batch_timer;
    _free_blocks_memory_usage_mark = _local_state->_free_blocks_memory_usage;
    _scanner_ctx_sched_time = _local_state->_scanner_ctx_sched_time;
    _scale_up_scanners_counter = _local_state->_scale_up_scanners_counter;

#ifndef BE_TEST
    // 3. get thread token
    if (_state->get_query_ctx()) {
        thread_token = _state->get_query_ctx()->get_token();
        _simple_scan_scheduler = _state->get_query_ctx()->get_scan_scheduler();
        if (_simple_scan_scheduler) {
            _should_reset_thread_name = false;
        }
        _remote_scan_task_scheduler = _state->get_query_ctx()->get_remote_scan_scheduler();
    }
#endif

    COUNTER_SET(_local_state->_max_scanner_thread_num, (int64_t)_max_thread_num);
    _local_state->_runtime_profile->add_info_string("UseSpecificThreadToken",
                                                    thread_token == nullptr ? "False" : "True");

    // submit `_max_thread_num` running scanners to `ScannerScheduler`
    // When a running scanners is finished, it will submit one of the remaining scanners.
    for (int i = 0; i < _max_thread_num; ++i) {
        std::weak_ptr<ScannerDelegate> next_scanner;
        if (_scanners.try_dequeue(next_scanner)) {
            submit_scan_task(std::make_shared<ScanTask>(next_scanner));
            _num_running_scanners++;
        }
    }

    return Status::OK();
}

std::string ScannerContext::parent_name() {
    return _local_state->get_name();
}

vectorized::BlockUPtr ScannerContext::get_free_block(bool force) {
    vectorized::BlockUPtr block = nullptr;
    if (_free_blocks.try_dequeue(block)) {
        DCHECK(block->mem_reuse());
        _free_blocks_memory_usage -= block->allocated_bytes();
        _free_blocks_memory_usage_mark->set(_free_blocks_memory_usage);
    } else if (_free_blocks_memory_usage < _max_bytes_in_queue || force) {
        _newly_create_free_blocks_num->update(1);
        block = vectorized::Block::create_unique(_output_tuple_desc->slots(), 0,
                                                 true /*ignore invalid slots*/);
    }
    return block;
}

void ScannerContext::return_free_block(vectorized::BlockUPtr block) {
    if (block->mem_reuse() && _free_blocks_memory_usage < _max_bytes_in_queue) {
        _free_blocks_memory_usage += block->allocated_bytes();
        _free_blocks_memory_usage_mark->set(_free_blocks_memory_usage);
        block->clear_column_data();
        _free_blocks.enqueue(std::move(block));
    }
}

bool ScannerContext::empty_in_queue(int id) {
    std::lock_guard<std::mutex> l(_transfer_lock);
    return _blocks_queue.empty();
}

void ScannerContext::submit_scan_task(std::shared_ptr<ScanTask> scan_task) {
    _scanner_sched_counter->update(1);
    _num_scheduled_scanners++;
    _scanner_scheduler->submit(shared_from_this(), scan_task);
}

void ScannerContext::append_block_to_queue(std::shared_ptr<ScanTask> scan_task) {
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
    if (_last_scale_up_time == 0) {
        _last_scale_up_time = UnixMillis();
    }
    if (_blocks_queue.empty() && _last_fetch_time != 0) {
        // there's no block in queue before current block, so the consumer is waiting
        _total_wait_block_time += UnixMillis() - _last_fetch_time;
    }
    _num_scheduled_scanners--;
    _blocks_queue.emplace_back(scan_task);
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
    if (!_blocks_queue.empty() && !done()) {
        _last_fetch_time = UnixMillis();
        auto scan_task = _blocks_queue.front();
        DCHECK(scan_task);

        if (!scan_task->status_ok()) {
            _set_scanner_done();
            return scan_task->get_status();
        }
        if (!scan_task->cached_blocks.empty()) {
            auto [current_block, block_size] = std::move(scan_task->cached_blocks.front());
            scan_task->cached_blocks.pop_front();
            if (_estimated_block_size > block_size) {
                _estimated_block_size = block_size;
            }
            _free_blocks_memory_usage -= block_size;
            _free_blocks_memory_usage_mark->set(_free_blocks_memory_usage);
            // consume current block
            block->swap(*current_block);
            return_free_block(std::move(current_block));
        } else {
            _blocks_queue.pop_front();
            if (scan_task->is_eos()) { // current scanner is finished, and no more data to read
                _num_finished_scanners++;
                std::weak_ptr<ScannerDelegate> next_scanner;
                // submit one of the remaining scanners
                if (_scanners.try_dequeue(next_scanner)) {
                    submit_scan_task(std::make_shared<ScanTask>(next_scanner));
                } else {
                    // no more scanner to be scheduled
                    // `_free_blocks` serve all running scanners, maybe it's too large for the remaining scanners
                    int free_blocks_for_each = _free_blocks.size_approx() / _num_running_scanners;
                    _num_running_scanners--;
                    for (int i = 0; i < free_blocks_for_each; ++i) {
                        vectorized::BlockUPtr removed_block;
                        if (_free_blocks.try_dequeue(removed_block)) {
                            _free_blocks_memory_usage -= block->allocated_bytes();
                            _free_blocks_memory_usage_mark->set(_free_blocks_memory_usage);
                        }
                    }
                }
            } else {
                // resubmit current running scanner to read the next block
                submit_scan_task(scan_task);
            }
        }
        // scale up
        _try_to_scale_up();
    }

    if (_num_finished_scanners == _all_scanners.size() && _blocks_queue.empty()) {
        _set_scanner_done();
        _is_finished = true;
    }
    *eos = done();

    if (_blocks_queue.empty()) {
        _dependency->block();
    }
    return Status::OK();
}

void ScannerContext::_try_to_scale_up() {
    // Four criteria to determine whether to increase the parallelism of the scanners
    // 1. It ran for at least `SCALE_UP_DURATION` ms after last scale up
    // 2. Half(`WAIT_BLOCK_DURATION_RATIO`) of the duration is waiting to get blocks
    // 3. `_free_blocks_memory_usage` < `_max_bytes_in_queue`, remains enough memory to scale up
    // 4. At most scale up `MAX_SCALE_UP_RATIO` times to `_max_thread_num`
    if (MAX_SCALE_UP_RATIO > 0 && _scanners.size_approx() > 0 &&
        (_num_running_scanners < _max_thread_num * MAX_SCALE_UP_RATIO) &&
        (_last_fetch_time - _last_scale_up_time > SCALE_UP_DURATION) && // duration > 5000ms
        (_total_wait_block_time > (_last_fetch_time - _last_scale_up_time) *
                                          WAIT_BLOCK_DURATION_RATIO)) { // too large lock time
        double wait_ratio =
                (double)_total_wait_block_time / (_last_fetch_time - _last_scale_up_time);
        if (_last_wait_duration_ratio > 0 && wait_ratio > _last_wait_duration_ratio * 0.8) {
            // when _last_wait_duration_ratio > 0, it has scaled up before.
            // we need to determine if the scale-up is effective:
            // the wait duration ratio after last scaling up should less than 80% of `_last_wait_duration_ratio`
            return;
        }

        bool is_scale_up = false;
        // calculate the number of scanners that can be scheduled
        int num_add = int(std::min(_num_running_scanners * SCALE_UP_RATIO,
                                   _max_thread_num * MAX_SCALE_UP_RATIO - _num_running_scanners));
        if (_estimated_block_size > 0) {
            int most_add =
                    (_max_bytes_in_queue - _free_blocks_memory_usage) / _estimated_block_size;
            num_add = std::min(num_add, most_add);
        }
        for (int i = 0; i < num_add; ++i) {
            // get enough memory to launch one more scanner.
            std::weak_ptr<ScannerDelegate> scale_up_scanner;
            if (_scanners.try_dequeue(scale_up_scanner)) {
                submit_scan_task(std::make_shared<ScanTask>(scale_up_scanner));
                _num_running_scanners++;
                _scale_up_scanners_counter->update(1);
                is_scale_up = true;
            } else {
                break;
            }
        }

        if (is_scale_up) {
            _last_wait_duration_ratio = wait_ratio;
            _last_scale_up_time = UnixMillis();
            _total_wait_block_time = 0;
        }
    }
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

void ScannerContext::set_status_on_error(const Status& status) {
    std::lock_guard<std::mutex> l(_transfer_lock);
    _process_status = status;
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
    _blocks_queue.clear();
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
            "id: {}, total scanners: {}, blocks in queue: {},"
            " _should_stop: {}, _is_finished: {}, free blocks: {},"
            " limit: {}, _num_running_scanners: {}, _max_thread_num: {},"
            " _max_bytes_in_queue: {}, query_id: {}",
            ctx_id, _all_scanners.size(), _blocks_queue.size(), _should_stop, _is_finished,
            _free_blocks.size_approx(), limit, _num_scheduled_scanners, _max_thread_num,
            _max_bytes_in_queue, print_id(_query_id));
}

void ScannerContext::_set_scanner_done() {
    _dependency->set_always_ready();
}

} // namespace doris::vectorized
