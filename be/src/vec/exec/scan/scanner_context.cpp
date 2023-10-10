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

#include <bthread/bthread.h>
#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <mutex>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/pretty_printer.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {

ScannerContext::ScannerContext(doris::RuntimeState* state_, doris::vectorized::VScanNode* parent,
                               const doris::TupleDescriptor* output_tuple_desc,
                               const std::list<VScannerSPtr>& scanners_, int64_t limit_,
                               int64_t max_bytes_in_blocks_queue_, const int num_parallel_instances)
        : _state(state_),
          _parent(parent),
          _output_tuple_desc(output_tuple_desc),
          _process_status(Status::OK()),
          _batch_size(state_->batch_size()),
          limit(limit_),
          _max_bytes_in_queue(max_bytes_in_blocks_queue_),
          _scanner_scheduler(state_->exec_env()->scanner_scheduler()),
          _scanners(scanners_),
          _num_parallel_instances(num_parallel_instances) {
    ctx_id = UniqueId::gen_uid().to_string();
    if (_scanners.empty()) {
        _is_finished = true;
    }
    if (limit < 0) {
        limit = -1;
    }
}

// After init function call, should not access _parent
Status ScannerContext::init() {
    // 1. Calculate max concurrency
    // TODO: now the max thread num <= config::doris_scanner_thread_pool_thread_num / 4
    // should find a more reasonable value.
    _max_thread_num = config::doris_scanner_thread_pool_thread_num / 4;
    if (_parent->_shared_scan_opt) {
        DCHECK(_num_parallel_instances > 0);
        _max_thread_num *= _num_parallel_instances;
    }
    _max_thread_num = _max_thread_num == 0 ? 1 : _max_thread_num;
    DCHECK(_max_thread_num > 0);
    _max_thread_num = std::min(_max_thread_num, (int32_t)_scanners.size());
    // For select * from table limit 10; should just use one thread.
    if (_parent->should_run_serial()) {
        _max_thread_num = 1;
    }

    _scanner_profile = _parent->_scanner_profile;
    _scanner_sched_counter = _parent->_scanner_sched_counter;
    _scanner_ctx_sched_counter = _parent->_scanner_ctx_sched_counter;
    _scanner_ctx_sched_time = _parent->_scanner_ctx_sched_time;
    _free_blocks_memory_usage = _parent->_free_blocks_memory_usage;
    _newly_create_free_blocks_num = _parent->_newly_create_free_blocks_num;
    _queued_blocks_memory_usage = _parent->_queued_blocks_memory_usage;
    _scanner_wait_batch_timer = _parent->_scanner_wait_batch_timer;
    // 2. Calculate the number of free blocks that all scanners can use.
    // The calculation logic is as follows:
    //  1. Assuming that at most M rows can be scanned in one scan(config::doris_scanner_row_num),
    //     then figure out how many blocks are required for one scan(_block_per_scanner).
    //  2. The maximum number of concurrency * the blocks required for one scan,
    //     that is, the number of blocks that all scanners can use.
    auto doris_scanner_row_num =
            limit == -1 ? config::doris_scanner_row_num
                        : std::min(static_cast<int64_t>(config::doris_scanner_row_num), limit);
    int real_block_size =
            limit == -1 ? _batch_size : std::min(static_cast<int64_t>(_batch_size), limit);
    _block_per_scanner = (doris_scanner_row_num + (real_block_size - 1)) / real_block_size;
    _free_blocks_capacity = _max_thread_num * _block_per_scanner;

#ifndef BE_TEST
    // 3. get thread token
    thread_token = _state->get_query_ctx()->get_token();
#endif

    // 4. This ctx will be submitted to the scanner scheduler right after init.
    // So set _num_scheduling_ctx to 1 here.
    _num_scheduling_ctx = 1;

    _num_unfinished_scanners = _scanners.size();

    COUNTER_SET(_parent->_max_scanner_thread_num, (int64_t)_max_thread_num);
    _parent->_runtime_profile->add_info_string("UseSpecificThreadToken",
                                               thread_token == nullptr ? "False" : "True");

    return Status::OK();
}

vectorized::BlockUPtr ScannerContext::get_free_block(bool* has_free_block,
                                                     bool get_block_not_empty) {
    vectorized::BlockUPtr block;
    if (_free_blocks.try_dequeue(block)) {
        if (!get_block_not_empty || block->mem_reuse()) {
            _free_blocks_capacity--;
            _free_blocks_memory_usage->add(-block->allocated_bytes());
            return block;
        }
    }

    COUNTER_UPDATE(_newly_create_free_blocks_num, 1);
    return vectorized::Block::create_unique(_output_tuple_desc->slots(), _batch_size,
                                            true /*ignore invalid slots*/);
}

void ScannerContext::return_free_block(std::unique_ptr<vectorized::Block> block) {
    block->clear_column_data();
    _free_blocks_memory_usage->add(block->allocated_bytes());
    _free_blocks.enqueue(std::move(block));
    ++_free_blocks_capacity;
}

void ScannerContext::append_blocks_to_queue(std::vector<vectorized::BlockUPtr>& blocks) {
    std::lock_guard l(_transfer_lock);
    auto old_bytes_in_queue = _cur_bytes_in_queue;
    for (auto& b : blocks) {
        _cur_bytes_in_queue += b->allocated_bytes();
        _blocks_queue.push_back(std::move(b));
    }
    blocks.clear();
    _blocks_queue_added_cv.notify_one();
    _queued_blocks_memory_usage->add(_cur_bytes_in_queue - old_bytes_in_queue);
}

bool ScannerContext::empty_in_queue(int id) {
    std::unique_lock l(_transfer_lock);
    return _blocks_queue.empty();
}

Status ScannerContext::get_block_from_queue(RuntimeState* state, vectorized::BlockUPtr* block,
                                            bool* eos, int id, bool wait) {
    std::unique_lock l(_transfer_lock);
    // Normally, the scanner scheduler will schedule ctx.
    // But when the amount of data in the blocks queue exceeds the upper limit,
    // the scheduler will stop scheduling.
    // (if the scheduler continues to schedule, it will cause a lot of busy running).
    // At this point, consumers are required to trigger new scheduling to ensure that
    // data can be continuously fetched.
    if (has_enough_space_in_blocks_queue() && _num_running_scanners == 0) {
        auto state = _scanner_scheduler->submit(this);
        if (state.ok()) {
            _num_scheduling_ctx++;
        } else {
            set_status_on_error(state, false);
        }
    }
    // Wait for block from queue
    if (wait) {
        SCOPED_TIMER(_scanner_wait_batch_timer);
        while (!(!_blocks_queue.empty() || _is_finished || !status().ok() ||
                 state->is_cancelled())) {
            _blocks_queue_added_cv.wait(l);
        }
    }

    if (state->is_cancelled()) {
        set_status_on_error(Status::Cancelled("cancelled"), false);
    }

    if (!status().ok()) {
        return status();
    }

    if (!_blocks_queue.empty()) {
        *block = std::move(_blocks_queue.front());
        _blocks_queue.pop_front();

        RETURN_IF_ERROR(validate_block_schema((*block).get()));

        auto block_bytes = (*block)->allocated_bytes();
        _cur_bytes_in_queue -= block_bytes;
        _queued_blocks_memory_usage->add(-block_bytes);
        return Status::OK();
    } else {
        *eos = _is_finished;
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

bool ScannerContext::set_status_on_error(const Status& status, bool need_lock) {
    std::unique_lock l(_transfer_lock, std::defer_lock);
    if (need_lock) {
        l.lock();
    }
    if (this->status().ok()) {
        _process_status = status;
        _status_error = true;
        _blocks_queue_added_cv.notify_one();
        _should_stop = true;
        return true;
    }
    return false;
}

Status ScannerContext::_close_and_clear_scanners(VScanNode* node, RuntimeState* state) {
    std::unique_lock l(_scanners_lock);
    if (state->enable_profile()) {
        std::stringstream scanner_statistics;
        std::stringstream scanner_rows_read;
        scanner_statistics << "[";
        scanner_rows_read << "[";
        for (auto finished_scanner_time : _finished_scanner_runtime) {
            scanner_statistics << PrettyPrinter::print(finished_scanner_time, TUnit::TIME_NS)
                               << ", ";
        }
        for (auto finished_scanner_rows : _finished_scanner_rows_read) {
            scanner_rows_read << PrettyPrinter::print(finished_scanner_rows, TUnit::UNIT) << ", ";
        }
        // Only unfinished scanners here
        for (auto& scanner : _scanners) {
            // Scanners are in ObjPool in ScanNode,
            // so no need to delete them here.
            // Add per scanner running time before close them
            scanner_statistics << PrettyPrinter::print(scanner->get_time_cost_ns(), TUnit::TIME_NS)
                               << ", ";
            scanner_rows_read << PrettyPrinter::print(scanner->get_rows_read(), TUnit::UNIT)
                              << ", ";
        }
        scanner_statistics << "]";
        scanner_rows_read << "]";
        node->_scanner_profile->add_info_string("PerScannerRunningTime", scanner_statistics.str());
        node->_scanner_profile->add_info_string("PerScannerRowsRead", scanner_rows_read.str());
    }
    // Only unfinished scanners here
    for (auto& scanner : _scanners) {
        scanner->close(state);
        // Scanners are in ObjPool in ScanNode,
        // so no need to delete them here.
    }
    _scanners.clear();
    return Status::OK();
}

void ScannerContext::clear_and_join(VScanNode* node, RuntimeState* state) {
    std::unique_lock l(_transfer_lock);
    do {
        if (_num_running_scanners == 0 && _num_scheduling_ctx == 0) {
            break;
        } else {
            DCHECK(!state->enable_pipeline_exec());
            while (!(_num_running_scanners == 0 && _num_scheduling_ctx == 0)) {
                _ctx_finish_cv.wait(l);
            }
            break;
        }
    } while (false);

    for (const auto& tid : _btids) {
        bthread_join(tid, nullptr);
    }
    // Must wait all running scanners stop running.
    // So that we can make sure to close all scanners.
    _close_and_clear_scanners(node, state);

    _blocks_queue.clear();
}

bool ScannerContext::no_schedule() {
    std::unique_lock l(_transfer_lock);
    return _num_running_scanners == 0 && _num_scheduling_ctx == 0;
}

std::string ScannerContext::debug_string() {
    return fmt::format(
            "id: {}, sacnners: {}, blocks in queue: {},"
            " status: {}, _should_stop: {}, _is_finished: {}, free blocks: {},"
            " limit: {}, _num_running_scanners: {}, _num_scheduling_ctx: {}, _max_thread_num: {},"
            " _block_per_scanner: {}, _cur_bytes_in_queue: {}, MAX_BYTE_OF_QUEUE: {}",
            ctx_id, _scanners.size(), _blocks_queue.size(), status().ok(), _should_stop,
            _is_finished, _free_blocks.size_approx(), limit, _num_running_scanners,
            _num_scheduling_ctx, _max_thread_num, _block_per_scanner, _cur_bytes_in_queue,
            _max_bytes_in_queue);
}

void ScannerContext::reschedule_scanner_ctx() {
    std::lock_guard l(_transfer_lock);
    auto state = _scanner_scheduler->submit(this);
    //todo(wb) rethinking is it better to mark current scan_context failed when submit failed many times?
    if (state.ok()) {
        _num_scheduling_ctx++;
    } else {
        set_status_on_error(state, false);
    }
}

void ScannerContext::push_back_scanner_and_reschedule(VScannerSPtr scanner) {
    {
        std::unique_lock l(_scanners_lock);
        _scanners.push_front(scanner);
    }
    std::lock_guard l(_transfer_lock);
    if (has_enough_space_in_blocks_queue()) {
        auto state = _scanner_scheduler->submit(this);
        if (state.ok()) {
            _num_scheduling_ctx++;
        } else {
            set_status_on_error(state, false);
        }
    }

    // Notice that after calling "_scanners.push_front(scanner)", there may be other ctx in scheduler
    // to schedule that scanner right away, and in that schedule run, the scanner may be marked as closed
    // before we call the following if() block.
    // So we need "scanner->set_counted_down()" to avoid "_num_unfinished_scanners" being decreased twice by
    // same scanner.
    if (scanner->need_to_close() && scanner->set_counted_down() &&
        (--_num_unfinished_scanners) == 0) {
        _dispose_coloate_blocks_not_in_queue();
        _is_finished = true;
        _blocks_queue_added_cv.notify_one();
    }
    // In pipeline engine, doris will close scanners when `no_schedule`.
    _num_running_scanners--;
    _ctx_finish_cv.notify_one();
}

void ScannerContext::get_next_batch_of_scanners(std::list<VScannerSPtr>* current_run) {
    // 1. Calculate how many scanners should be scheduled at this run.
    int thread_slot_num = 0;
    {
        // If there are enough space in blocks queue,
        // the scanner number depends on the _free_blocks numbers
        thread_slot_num = cal_thread_slot_num_by_free_block_num();
    }

    // 2. get #thread_slot_num scanners from ctx->scanners
    // and put them into "this_run".
    {
        std::unique_lock l(_scanners_lock);
        for (int i = 0; i < thread_slot_num && !_scanners.empty();) {
            VScannerSPtr scanner = _scanners.front();
            _scanners.pop_front();
            if (scanner->need_to_close()) {
                _finished_scanner_runtime.push_back(scanner->get_time_cost_ns());
                _finished_scanner_rows_read.push_back(scanner->get_rows_read());
                scanner->close(_state);
            } else {
                current_run->push_back(scanner);
                i++;
            }
        }
    }
}

} // namespace doris::vectorized
