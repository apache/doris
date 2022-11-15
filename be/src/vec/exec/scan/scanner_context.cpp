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

#include "common/config.h"
#include "runtime/runtime_state.h"
#include "util/threadpool.h"
#include "vec/core/block.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {

Status ScannerContext::init() {
    _real_tuple_desc = _input_tuple_desc != nullptr ? _input_tuple_desc : _output_tuple_desc;
    // 1. Calculate max concurrency
    // TODO: now the max thread num <= config::doris_scanner_thread_pool_thread_num / 4
    // should find a more reasonable value.
    _max_thread_num =
            std::min(config::doris_scanner_thread_pool_thread_num / 4, (int32_t)_scanners.size());

    // 2. Calculate how many blocks need to be preallocated.
    // The calculation logic is as follows:
    //  1. Assuming that at most M rows can be scanned in one scan(config::doris_scanner_row_num),
    //     then figure out how many blocks are required for one scan(_block_per_scanner).
    //  2. The maximum number of concurrency * the blocks required for one scan,
    //     that is, the number of blocks that need to be pre-allocated
    auto doris_scanner_row_num =
            limit == -1 ? config::doris_scanner_row_num
                        : std::min(static_cast<int64_t>(config::doris_scanner_row_num), limit);
    int real_block_size = limit == -1 ? _state->batch_size()
                                      : std::min(static_cast<int64_t>(_state->batch_size()), limit);
    _block_per_scanner = (doris_scanner_row_num + (real_block_size - 1)) / real_block_size;
    auto pre_alloc_block_count = _max_thread_num * _block_per_scanner;

    // The free blocks is used for final output block of scanners.
    // So use _output_tuple_desc;
    for (int i = 0; i < pre_alloc_block_count; ++i) {
        auto block = new vectorized::Block(_output_tuple_desc->slots(), real_block_size);
        _free_blocks.emplace_back(block);
    }

#ifndef BE_TEST
    // 3. get thread token
    thread_token = _state->get_query_fragments_ctx()->get_token();
#endif

    // 4. This ctx will be submitted to the scanner scheduler right after init.
    // So set _num_scheduling_ctx to 1 here.
    _num_scheduling_ctx = 1;

    _num_unfinished_scanners = _scanners.size();

    COUNTER_SET(_parent->_pre_alloc_free_blocks_num, (int64_t)pre_alloc_block_count);
    COUNTER_SET(_parent->_max_scanner_thread_num, (int64_t)_max_thread_num);

    return Status::OK();
}

vectorized::Block* ScannerContext::get_free_block(bool* get_free_block) {
    {
        std::lock_guard<std::mutex> l(_free_blocks_lock);
        if (!_free_blocks.empty()) {
            auto block = _free_blocks.back();
            _free_blocks.pop_back();
            return block;
        }
    }
    *get_free_block = false;

    COUNTER_UPDATE(_parent->_newly_create_free_blocks_num, 1);
    return new vectorized::Block(_real_tuple_desc->slots(), _state->batch_size());
}

void ScannerContext::return_free_block(vectorized::Block* block) {
    block->clear_column_data();
    std::lock_guard<std::mutex> l(_free_blocks_lock);
    _free_blocks.emplace_back(block);
}

void ScannerContext::append_blocks_to_queue(const std::vector<vectorized::Block*>& blocks) {
    std::lock_guard<std::mutex> l(_transfer_lock);
    blocks_queue.insert(blocks_queue.end(), blocks.begin(), blocks.end());
    for (auto b : blocks) {
        _cur_bytes_in_queue += b->allocated_bytes();
    }
    _blocks_queue_added_cv.notify_one();
}

Status ScannerContext::get_block_from_queue(vectorized::Block** block, bool* eos) {
    std::unique_lock<std::mutex> l(_transfer_lock);
    // Normally, the scanner scheduler will schedule ctx.
    // But when the amount of data in the blocks queue exceeds the upper limit,
    // the scheduler will stop scheduling.
    // (if the scheduler continues to schedule, it will cause a lot of busy running).
    // At this point, consumers are required to trigger new scheduling to ensure that
    // data can be continuously fetched.
    if (_has_enough_space_in_blocks_queue() && _num_running_scanners == 0) {
        _num_scheduling_ctx++;
        _state->exec_env()->scanner_scheduler()->submit(this);
    }
    // Wait for block from queue
    {
        SCOPED_TIMER(_parent->_scanner_wait_batch_timer);
        _blocks_queue_added_cv.wait(l, [this]() {
            return !blocks_queue.empty() || _is_finished || !_process_status.ok() ||
                   _state->is_cancelled();
        });
    }

    if (_state->is_cancelled()) {
        _process_status = Status::Cancelled("cancelled");
    }

    if (!_process_status.ok()) {
        return _process_status;
    }

    if (!blocks_queue.empty()) {
        *block = blocks_queue.front();
        blocks_queue.pop_front();
        _cur_bytes_in_queue -= (*block)->allocated_bytes();
        return Status::OK();
    } else {
        *eos = _is_finished;
    }
    return Status::OK();
}

bool ScannerContext::set_status_on_error(const Status& status) {
    std::lock_guard<std::mutex> l(_transfer_lock);
    if (_process_status.ok()) {
        _process_status = status;
        _blocks_queue_added_cv.notify_one();
        return true;
    }
    return false;
}

Status ScannerContext::_close_and_clear_scanners() {
    std::unique_lock<std::mutex> l(_scanners_lock);
    for (auto scanner : _scanners) {
        scanner->close(_state);
        // Scanners are in ObjPool in ScanNode,
        // so no need to delete them here.
    }
    _scanners.clear();
    return Status::OK();
}

void ScannerContext::clear_and_join() {
    std::unique_lock<std::mutex> l(_transfer_lock);
    do {
        if (_num_running_scanners == 0 && _num_scheduling_ctx == 0) {
            break;
        } else {
            _ctx_finish_cv.wait(
                    l, [this] { return _num_running_scanners == 0 && _num_scheduling_ctx == 0; });
            break;
        }
    } while (false);

    // Must wait all running scanners stop running.
    // So that we can make sure to close all scanners.
    _close_and_clear_scanners();

    COUNTER_SET(_parent->_scanner_sched_counter, _num_scanner_scheduling);
    COUNTER_SET(_parent->_scanner_ctx_sched_counter, _num_ctx_scheduling);

    std::for_each(blocks_queue.begin(), blocks_queue.end(),
                  std::default_delete<vectorized::Block>());
    std::for_each(_free_blocks.begin(), _free_blocks.end(),
                  std::default_delete<vectorized::Block>());

    return;
}

std::string ScannerContext::debug_string() {
    return fmt::format(
            "id: {}, sacnners: {}, blocks in queue: {},"
            " status: {}, _should_stop: {}, _is_finished: {}, free blocks: {},"
            " limit: {}, _num_running_scanners: {}, _num_scheduling_ctx: {}, _max_thread_num: {},"
            " _block_per_scanner: {}, _cur_bytes_in_queue: {}, _max_bytes_in_queue: {}",
            ctx_id, _scanners.size(), blocks_queue.size(), _process_status.ok(), _should_stop,
            _is_finished, _free_blocks.size(), limit, _num_running_scanners, _num_scheduling_ctx,
            _max_thread_num, _block_per_scanner, _cur_bytes_in_queue, _max_bytes_in_queue);
}

void ScannerContext::push_back_scanner_and_reschedule(VScanner* scanner) {
    {
        std::unique_lock<std::mutex> l(_scanners_lock);
        _scanners.push_front(scanner);
    }

    std::lock_guard<std::mutex> l(_transfer_lock);
    _num_running_scanners--;
    _num_scheduling_ctx++;
    _state->exec_env()->scanner_scheduler()->submit(this);

    // Notice that after calling "_scanners.push_front(scanner)", there may be other ctx in scheduler
    // to schedule that scanner right away, and in that schedule run, the scanner may be marked as closed
    // before we call the following if() block.
    // So we need "scanner->set_counted_down()" to avoid "_num_unfinished_scanners" being decreased twice by
    // same scanner.
    if (scanner->need_to_close() && scanner->set_counted_down() &&
        (--_num_unfinished_scanners) == 0) {
        _is_finished = true;
        // ATTN: this 2 counters will be set at close() again, which is the final values.
        // But we set them here because the counter set at close() can not send to FE's profile.
        // So we set them here, and the counter value may be little less than final values.
        COUNTER_SET(_parent->_scanner_sched_counter, _num_scanner_scheduling);
        COUNTER_SET(_parent->_scanner_ctx_sched_counter, _num_ctx_scheduling);
        _blocks_queue_added_cv.notify_one();
    }
    _ctx_finish_cv.notify_one();
}

void ScannerContext::get_next_batch_of_scanners(std::list<VScanner*>* current_run) {
    // 1. Calculate how many scanners should be scheduled at this run.
    int thread_slot_num = 0;
    {
        std::unique_lock<std::mutex> l(_transfer_lock);
        if (_has_enough_space_in_blocks_queue()) {
            // If there are enough space in blocks queue,
            // the scanner number depends on the _free_blocks numbers
            std::lock_guard<std::mutex> l(_free_blocks_lock);
            thread_slot_num = _free_blocks.size() / _block_per_scanner;
            thread_slot_num += (_free_blocks.size() % _block_per_scanner != 0);
            thread_slot_num = std::min(thread_slot_num, _max_thread_num - _num_running_scanners);
            if (thread_slot_num <= 0) {
                thread_slot_num = 1;
            }
        } else {
            // The blocks queue reaches limit, just return to stop scheduling
            // There will be two cases:
            // 1. There are running scanners, these scanner will continue scheduler the ctx.
            // 2. No running scanners, the consumer(ScanNode.get_next()) will continue scheduling the ctx.
            // In both cases, we do not need to continue to schedule ctx here. So just return
            return;
        }
    }

    // 2. get #thread_slot_num scanners from ctx->scanners
    // and put them into "this_run".
    {
        std::unique_lock<std::mutex> l(_scanners_lock);
        for (int i = 0; i < thread_slot_num && !_scanners.empty();) {
            auto scanner = _scanners.front();
            _scanners.pop_front();
            if (scanner->need_to_close()) {
                scanner->close(_state);
            } else {
                current_run->push_back(scanner);
                i++;
            }
        }
    }
    return;
}

} // namespace doris::vectorized
