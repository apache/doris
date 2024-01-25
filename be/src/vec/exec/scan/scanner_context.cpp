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
#include "pipeline/exec/scan_operator.h"
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

using namespace std::chrono_literals;

ScannerContext::ScannerContext(RuntimeState* state, const TupleDescriptor* output_tuple_desc,
                               const RowDescriptor* output_row_descriptor,
                               const std::list<std::shared_ptr<ScannerDelegate>>& scanners,
                               int64_t limit_, int64_t max_bytes_in_blocks_queue,
                               const int num_parallel_instances,
                               pipeline::ScanLocalStateBase* local_state,
                               std::shared_ptr<pipeline::ScanDependency> dependency)
        : HasTaskExecutionCtx(state),
          _state(state),
          _parent(nullptr),
          _local_state(local_state),
          _output_tuple_desc(output_row_descriptor
                                     ? output_row_descriptor->tuple_descriptors().front()
                                     : output_tuple_desc),
          _output_row_descriptor(output_row_descriptor),
          _process_status(Status::OK()),
          _batch_size(state->batch_size()),
          limit(limit_),
          _max_bytes_in_queue(std::max(max_bytes_in_blocks_queue, (int64_t)1024) *
                              num_parallel_instances),
          _scanner_scheduler(state->exec_env()->scanner_scheduler()),
          _scanners(scanners.begin(), scanners.end()),
          _all_scanners(scanners.begin(), scanners.end()),
          _num_parallel_instances(num_parallel_instances),
          _dependency(dependency) {
    DCHECK(_output_row_descriptor == nullptr ||
           _output_row_descriptor->tuple_descriptors().size() == 1);
    _query_id = _state->get_query_ctx()->query_id();
    ctx_id = UniqueId::gen_uid().to_string();
    if (_scanners.empty()) {
        _is_finished = true;
        _set_scanner_done();
    }
    if (limit < 0) {
        limit = -1;
    }
    _max_thread_num = config::doris_scanner_thread_pool_thread_num / 4;
    _max_thread_num *= num_parallel_instances;
    _max_thread_num = _max_thread_num == 0 ? 1 : _max_thread_num;
    DCHECK(_max_thread_num > 0);
    _max_thread_num = std::min(_max_thread_num, (int32_t)_scanners.size());
    // 1. Calculate max concurrency
    // For select * from table limit 10; should just use one thread.
    if ((_parent && _parent->should_run_serial()) ||
        (_local_state && _local_state->should_run_serial())) {
        _max_thread_num = 1;
    }
}

ScannerContext::ScannerContext(doris::RuntimeState* state, doris::vectorized::VScanNode* parent,
                               const doris::TupleDescriptor* output_tuple_desc,
                               const RowDescriptor* output_row_descriptor,
                               const std::list<std::shared_ptr<ScannerDelegate>>& scanners,
                               int64_t limit_, int64_t max_bytes_in_blocks_queue,
                               const int num_parallel_instances,
                               pipeline::ScanLocalStateBase* local_state)
        : HasTaskExecutionCtx(state),
          _state(state),
          _parent(parent),
          _local_state(local_state),
          _output_tuple_desc(output_row_descriptor
                                     ? output_row_descriptor->tuple_descriptors().front()
                                     : output_tuple_desc),
          _output_row_descriptor(output_row_descriptor),
          _process_status(Status::OK()),
          _batch_size(state->batch_size()),
          limit(limit_),
          _max_bytes_in_queue(std::max(max_bytes_in_blocks_queue, (int64_t)1024) *
                              num_parallel_instances),
          _scanner_scheduler(state->exec_env()->scanner_scheduler()),
          _scanners(scanners.begin(), scanners.end()),
          _all_scanners(scanners.begin(), scanners.end()),
          _num_parallel_instances(num_parallel_instances) {
    DCHECK(_output_row_descriptor == nullptr ||
           _output_row_descriptor->tuple_descriptors().size() == 1);
    _query_id = _state->get_query_ctx()->query_id();
    ctx_id = UniqueId::gen_uid().to_string();
    if (_scanners.empty()) {
        _is_finished = true;
        _set_scanner_done();
    }
    if (limit < 0) {
        limit = -1;
    }
    _max_thread_num = config::doris_scanner_thread_pool_thread_num / 4;
    _max_thread_num *= num_parallel_instances;
    _max_thread_num = _max_thread_num == 0 ? 1 : _max_thread_num;
    DCHECK(_max_thread_num > 0);
    _max_thread_num = std::min(_max_thread_num, (int32_t)_scanners.size());
    // 1. Calculate max concurrency
    // For select * from table limit 10; should just use one thread.
    if ((_parent && _parent->should_run_serial()) ||
        (_local_state && _local_state->should_run_serial())) {
        _max_thread_num = 1;
    }
}

// After init function call, should not access _parent
Status ScannerContext::init() {
    if (_parent) {
        _scanner_profile = _parent->_scanner_profile;
        _scanner_sched_counter = _parent->_scanner_sched_counter;
        _scanner_ctx_sched_counter = _parent->_scanner_ctx_sched_counter;
        _scanner_ctx_sched_time = _parent->_scanner_ctx_sched_time;
        _free_blocks_memory_usage = _parent->_free_blocks_memory_usage;
        _newly_create_free_blocks_num = _parent->_newly_create_free_blocks_num;
        _queued_blocks_memory_usage = _parent->_queued_blocks_memory_usage;
        _scanner_wait_batch_timer = _parent->_scanner_wait_batch_timer;
    } else {
        _scanner_profile = _local_state->_scanner_profile;
        _scanner_sched_counter = _local_state->_scanner_sched_counter;
        _scanner_ctx_sched_counter = _local_state->_scanner_ctx_sched_counter;
        _scanner_ctx_sched_time = _local_state->_scanner_ctx_sched_time;
        _free_blocks_memory_usage = _local_state->_free_blocks_memory_usage;
        _newly_create_free_blocks_num = _local_state->_newly_create_free_blocks_num;
        _queued_blocks_memory_usage = _local_state->_queued_blocks_memory_usage;
        _scanner_wait_batch_timer = _local_state->_scanner_wait_batch_timer;
    }

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
    auto block = get_free_block();
    _estimated_block_bytes = std::max(block->allocated_bytes(), (size_t)16);
    return_free_block(std::move(block));

#ifndef BE_TEST
    // 3. get thread token
    if (_state->get_query_ctx()) {
        thread_token = _state->get_query_ctx()->get_token();
        _simple_scan_scheduler = _state->get_query_ctx()->get_scan_scheduler();
        if (_simple_scan_scheduler) {
            _should_reset_thread_name = false;
        }
    }
#endif

    _num_unfinished_scanners = _scanners.size();

    if (_parent) {
        COUNTER_SET(_parent->_max_scanner_thread_num, (int64_t)_max_thread_num);
        _parent->_runtime_profile->add_info_string("UseSpecificThreadToken",
                                                   thread_token == nullptr ? "False" : "True");
    } else {
        COUNTER_SET(_local_state->_max_scanner_thread_num, (int64_t)_max_thread_num);
        _local_state->_runtime_profile->add_info_string("UseSpecificThreadToken",
                                                        thread_token == nullptr ? "False" : "True");
    }

    return Status::OK();
}

std::string ScannerContext::parent_name() {
    return _parent ? _parent->get_name() : _local_state->get_name();
}

vectorized::BlockUPtr ScannerContext::get_free_block() {
    vectorized::BlockUPtr block;
    if (_free_blocks.try_dequeue(block)) {
        DCHECK(block->mem_reuse());
        _free_blocks_memory_usage->add(-block->allocated_bytes());
        _serving_blocks_num++;
        return block;
    }

    block = vectorized::Block::create_unique(_output_tuple_desc->slots(), _batch_size,
                                             true /*ignore invalid slots*/);

    COUNTER_UPDATE(_newly_create_free_blocks_num, 1);

    _serving_blocks_num++;
    return block;
}

void ScannerContext::return_free_block(std::unique_ptr<vectorized::Block> block) {
    _serving_blocks_num--;
    if (block->mem_reuse()) {
        // Only put blocks with schema to free blocks, because colocate blocks
        // need schema.
        _estimated_block_bytes = std::max(block->allocated_bytes(), (size_t)16);
        block->clear_column_data();
        _free_blocks_memory_usage->add(block->allocated_bytes());
        _free_blocks.enqueue(std::move(block));
    }
}

void ScannerContext::append_blocks_to_queue(std::vector<vectorized::BlockUPtr>& blocks) {
    std::lock_guard l(_transfer_lock);
    auto old_bytes_in_queue = _cur_bytes_in_queue;
    for (auto& b : blocks) {
        auto st = validate_block_schema(b.get());
        if (!st.ok()) {
            set_status_on_error(st, false);
        }
        _cur_bytes_in_queue += b->allocated_bytes();
        _blocks_queue.push_back(std::move(b));
    }
    blocks.clear();
    if (_dependency) {
        _dependency->set_ready();
    }
    _blocks_queue_added_cv.notify_one();
    _queued_blocks_memory_usage->add(_cur_bytes_in_queue - old_bytes_in_queue);
}

bool ScannerContext::empty_in_queue(int id) {
    std::unique_lock l(_transfer_lock);
    return _blocks_queue.empty();
}

Status ScannerContext::get_block_from_queue(RuntimeState* state, vectorized::BlockUPtr* block,
                                            bool* eos, int id, bool wait) {
    std::vector<vectorized::BlockUPtr> merge_blocks;
    {
        std::unique_lock l(_transfer_lock);
        // Normally, the scanner scheduler will schedule ctx.
        // But when the amount of data in the blocks queue exceeds the upper limit,
        // the scheduler will stop scheduling.
        // (if the scheduler continues to schedule, it will cause a lot of busy running).
        // At this point, consumers are required to trigger new scheduling to ensure that
        // data can be continuously fetched.
        bool to_be_schedule = should_be_scheduled();

        bool is_scheduled = false;
        if (!done() && to_be_schedule && _num_running_scanners == 0) {
            is_scheduled = true;
            auto submit_status = _scanner_scheduler->submit(shared_from_this());
            if (!submit_status.ok()) {
                set_status_on_error(submit_status, false);
            }
        }

        // Wait for block from queue
        if (wait) {
            // scanner batch wait time
            SCOPED_TIMER(_scanner_wait_batch_timer);
            while (!(!_blocks_queue.empty() || done() || !status().ok() || state->is_cancelled())) {
                if (!is_scheduled && _num_running_scanners == 0 && should_be_scheduled()) {
                    LOG(INFO) << debug_string();
                }
                _blocks_queue_added_cv.wait_for(l, 1s);
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
            auto block_bytes = (*block)->allocated_bytes();
            _cur_bytes_in_queue -= block_bytes;
            _queued_blocks_memory_usage->add(-block_bytes);

            auto rows = (*block)->rows();
            while (!_blocks_queue.empty()) {
                auto& add_block = _blocks_queue.front();
                const auto add_rows = (*add_block).rows();
                if (rows + add_rows < state->batch_size()) {
                    rows += add_rows;
                    block_bytes = (*add_block).allocated_bytes();
                    _cur_bytes_in_queue -= block_bytes;
                    _queued_blocks_memory_usage->add(-block_bytes);
                    merge_blocks.emplace_back(std::move(add_block));
                    _blocks_queue.pop_front();
                } else {
                    break;
                }
            }
        } else {
            *eos = done();
        }
    }

    if (!merge_blocks.empty()) {
        vectorized::MutableBlock m(block->get());
        for (auto& merge_block : merge_blocks) {
            static_cast<void>(m.merge(*merge_block));
            return_free_block(std::move(merge_block));
        }
        (*block)->set_columns(std::move(m.mutable_columns()));
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

void ScannerContext::inc_num_running_scanners(int32_t inc) {
    std::lock_guard l(_transfer_lock);
    _num_running_scanners += inc;
}

void ScannerContext::set_status_on_error(const Status& status, bool need_lock) {
    std::unique_lock l(_transfer_lock, std::defer_lock);
    if (need_lock) {
        l.lock();
    }
    if (this->status().ok()) {
        _process_status = status;
        _blocks_queue_added_cv.notify_one();
        _should_stop = true;
        _set_scanner_done();
        LOG(INFO) << "ctx is set status on error " << debug_string()
                  << ", call stack is: " << Status::InternalError<true>("catch error status");
    }
}

void ScannerContext::stop_scanners(RuntimeState* state) {
    std::unique_lock l(_transfer_lock);
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
        scanner_statistics << "[";
        scanner_rows_read << "[";
        scanner_wait_worker_time << "[";
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
        _scanner_profile->add_info_string("PerScannerRunningTime", scanner_statistics.str());
        _scanner_profile->add_info_string("PerScannerRowsRead", scanner_rows_read.str());
        _scanner_profile->add_info_string("PerScannerWaitTime", scanner_wait_worker_time.str());
    }

    _blocks_queue_added_cv.notify_one();
}

void ScannerContext::_set_scanner_done() {
    if (_dependency) {
        _dependency->set_scanner_done();
    }
}

std::string ScannerContext::debug_string() {
    return fmt::format(
            "id: {}, total scanners: {}, scanners: {}, blocks in queue: {},"
            " status: {}, _should_stop: {}, _is_finished: {}, free blocks: {},"
            " limit: {}, _num_running_scanners: {}, _max_thread_num: {},"
            " _block_per_scanner: {}, _cur_bytes_in_queue: {}, MAX_BYTE_OF_QUEUE: {}, "
            "num_ctx_scheduled: {}, serving_blocks_num: {}, allowed_blocks_num: {}, query_id: {}",
            ctx_id, _all_scanners.size(), _scanners.size(), _blocks_queue.size(),
            _process_status.to_string(), _should_stop, _is_finished, _free_blocks.size_approx(),
            limit, _num_running_scanners, _max_thread_num, _block_per_scanner, _cur_bytes_in_queue,
            _max_bytes_in_queue, num_ctx_scheduled(), _serving_blocks_num, allowed_blocks_num(),
            print_id(_query_id));
}

void ScannerContext::reschedule_scanner_ctx() {
    std::lock_guard l(_transfer_lock);
    if (done()) {
        return;
    }
    auto submit_status = _scanner_scheduler->submit(shared_from_this());
    //todo(wb) rethinking is it better to mark current scan_context failed when submit failed many times?
    if (!submit_status.ok()) {
        set_status_on_error(submit_status, false);
    }
}

void ScannerContext::push_back_scanner_and_reschedule(std::shared_ptr<ScannerDelegate> scanner) {
    std::lock_guard l(_transfer_lock);
    // Use a transfer lock to avoid the scanner be scheduled concurrently. For example, that after
    // calling "_scanners.push_front(scanner)", there may be other ctx in scheduler
    // to schedule that scanner right away, and in that schedule run, the scanner may be marked as closed
    // before we call the following if() block.
    {
        --_num_running_scanners;
        if (scanner->_scanner->need_to_close()) {
            --_num_unfinished_scanners;
            if (_num_unfinished_scanners == 0) {
                _dispose_coloate_blocks_not_in_queue();
                _is_finished = true;
                _set_scanner_done();
                _blocks_queue_added_cv.notify_one();
                return;
            }
        } else {
            _scanners.push_front(scanner);
        }
    }

    if (should_be_scheduled()) {
        auto submit_status = _scanner_scheduler->submit(shared_from_this());
        if (!submit_status.ok()) {
            set_status_on_error(submit_status, false);
        }
    }
}

// This method is called in scanner scheduler, and task context is hold
void ScannerContext::get_next_batch_of_scanners(
        std::list<std::weak_ptr<ScannerDelegate>>* current_run) {
    std::lock_guard l(_transfer_lock);
    // Update the sched counter for profile
    Defer defer {[&]() { _scanner_sched_counter->update(current_run->size()); }};
    // 1. Calculate how many scanners should be scheduled at this run.
    // If there are enough space in blocks queue,
    // the scanner number depends on the _free_blocks numbers
    int thread_slot_num = get_available_thread_slot_num();

    // 2. get #thread_slot_num scanners from ctx->scanners
    // and put them into "this_run".
    for (int i = 0; i < thread_slot_num && !_scanners.empty();) {
        std::weak_ptr<ScannerDelegate> scanner_ref = _scanners.front();
        std::shared_ptr<ScannerDelegate> scanner = scanner_ref.lock();
        _scanners.pop_front();
        if (scanner == nullptr) {
            continue;
        }
        if (scanner->_scanner->need_to_close()) {
            static_cast<void>(scanner->_scanner->close(_state));
        } else {
            current_run->push_back(scanner_ref);
            i++;
        }
    }
}

} // namespace doris::vectorized
