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

ScannerContext::ScannerContext(RuntimeState* state, const TupleDescriptor* output_tuple_desc,
                               const RowDescriptor* output_row_descriptor,
                               const std::list<std::shared_ptr<ScannerDelegate>>& scanners,
                               int64_t limit_, int64_t max_bytes_in_blocks_queue,
                               const int num_parallel_instances,
                               pipeline::ScanLocalStateBase* local_state)
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
    } else if (limit < _batch_size) {
        _batch_size = limit;
    }
    _max_thread_num =
            config::doris_scanner_thread_pool_thread_num / state->query_parallel_instance_num();
    _max_thread_num *= num_parallel_instances;
    _max_thread_num = _max_thread_num == 0 ? 1 : _max_thread_num;
    _max_thread_num = std::min(_max_thread_num, (int32_t)scanners.size());
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
        : ScannerContext(state, output_tuple_desc, output_row_descriptor, scanners, limit_,
                         max_bytes_in_blocks_queue, num_parallel_instances, local_state) {
    _parent = parent;
}

// After init function call, should not access _parent
Status ScannerContext::init() {
    if (_parent) {
        _scanner_profile = _parent->_scanner_profile;
        _scanner_wait_batch_timer = _parent->_scanner_wait_batch_timer;
        _free_blocks_memory_usage_mark = _parent->_free_blocks_memory_usage;
        _scanner_ctx_sched_time = _parent->_scanner_ctx_sched_time;
        _scale_up_scanners_counter = _parent->_scale_up_scanners_counter;
    } else {
        _scanner_profile = _local_state->_scanner_profile;
        _scanner_wait_batch_timer = _local_state->_scanner_wait_batch_timer;
        _free_blocks_memory_usage_mark = _local_state->_free_blocks_memory_usage;
        _scanner_ctx_sched_time = _local_state->_scanner_ctx_sched_time;
        _scale_up_scanners_counter = _local_state->_scale_up_scanners_counter;
    }

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

    if (_parent) {
        COUNTER_SET(_parent->_max_scanner_thread_num, (int64_t)_max_thread_num);
        _parent->_runtime_profile->add_info_string("UseSpecificThreadToken",
                                                   thread_token == nullptr ? "False" : "True");
    } else {
        COUNTER_SET(_local_state->_max_scanner_thread_num, (int64_t)_max_thread_num);
        _local_state->_runtime_profile->add_info_string("UseSpecificThreadToken",
                                                        thread_token == nullptr ? "False" : "True");
    }

    // submit `_max_thread_num` running scanners to `ScannerScheduler`
    // When a running scanners is finished, it will submit one of the remaining scanners.
    for (int i = 0; i < _max_thread_num; ++i) {
        std::weak_ptr<ScannerDelegate> next_scanner;
        if (_scanners.try_dequeue(next_scanner)) {
            vectorized::BlockUPtr block = get_free_block(_batch_size);
            submit_running_scanner(
                    std::make_shared<RunningScanner>(next_scanner, std::move(block)));
            _num_running_scanners++;
        }
    }

    return Status::OK();
}

std::string ScannerContext::parent_name() {
    return _parent ? _parent->get_name() : _local_state->get_name();
}

vectorized::BlockUPtr ScannerContext::get_free_block(int batch_size) {
    vectorized::BlockUPtr block;
    if (_free_blocks.try_dequeue(block)) {
        std::lock_guard<std::mutex> fl(_free_blocks_lock);
        DCHECK(block->mem_reuse());
        _free_blocks_memory_usage -= block->allocated_bytes();
        _free_blocks_memory_usage_mark->set(_free_blocks_memory_usage);
        return block;
    }

    return vectorized::Block::create_unique(_output_tuple_desc->slots(), batch_size,
                                            true /*ignore invalid slots*/);
}

void ScannerContext::return_free_block(vectorized::BlockUPtr block) {
    std::lock_guard<std::mutex> fl(_free_blocks_lock);
    if (block->mem_reuse() && _free_blocks_memory_usage < _max_bytes_in_queue) {
        block->clear_column_data();
        _free_blocks_memory_usage += block->allocated_bytes();
        _free_blocks_memory_usage_mark->set(_free_blocks_memory_usage);
        _free_blocks.enqueue(std::move(block));
    }
}

bool ScannerContext::empty_in_queue(int id) {
    std::lock_guard<std::mutex> l(_transfer_lock);
    return _blocks_queue.empty();
}

void ScannerContext::submit_running_scanner(std::shared_ptr<RunningScanner> running_scanner) {
    _num_scheduled_scanners++;
    _scanner_scheduler->submit(shared_from_this(), running_scanner);
}

void ScannerContext::append_block_to_queue(std::shared_ptr<RunningScanner> running_scanner) {
    Status st = validate_block_schema(running_scanner->current_block.get());
    if (!st.ok()) {
        running_scanner->status = st;
    }
    // set `eos` if `END_OF_FILE`, don't take `END_OF_FILE` as error
    if (running_scanner->status.is<ErrorCode::END_OF_FILE>()) {
        running_scanner->eos = true;
    }
    // We can only the known the block size after reading at least one block
    // Just take the size of first block as `_estimated_block_size`
    if (running_scanner->first_block) {
        std::lock_guard<std::mutex> fl(_free_blocks_lock);
        size_t block_size = running_scanner->current_block->allocated_bytes();
        _free_blocks_memory_usage += block_size;
        _free_blocks_memory_usage_mark->set(_free_blocks_memory_usage);
        running_scanner->first_block = false;
        if (block_size > _estimated_block_size) {
            _estimated_block_size = block_size;
        }
    }
    std::lock_guard<std::mutex> l(_transfer_lock);
    if (_last_scale_up_time == 0) {
        _last_scale_up_time = UnixMillis();
    }
    if (_blocks_queue.empty() && _last_fetch_time != 0) {
        // there's no block in queue before current block, so the consumer is waiting
        _total_wait_block_time += UnixMillis() - _last_fetch_time;
    }
    if (running_scanner->eos) {
        _num_finished_scanners++;
    }
    _num_scheduled_scanners--;
    _blocks_queue.emplace_back(running_scanner);
    _blocks_queue_added_cv.notify_one();
}

Status ScannerContext::get_block_from_queue(RuntimeState* state, vectorized::Block* block,
                                            bool* eos, int id, bool wait) {
    if (state->is_cancelled()) {
        return Status::Cancelled("query cancelled");
    }
    std::unique_lock l(_transfer_lock);
    // Wait for block from queue
    if (wait) {
        // scanner batch wait time
        SCOPED_TIMER(_scanner_wait_batch_timer);
        while (!done() && _blocks_queue.empty()) {
            if (_num_finished_scanners == _all_scanners.size() && _blocks_queue.empty()) {
                _is_finished = true;
                break;
            }
            _blocks_queue_added_cv.wait_for(l, 1s);
        }
    }
    std::shared_ptr<RunningScanner> scanner = nullptr;
    if (!_blocks_queue.empty()) {
        _last_fetch_time = UnixMillis();
        scanner = _blocks_queue.front();
        _blocks_queue.pop_front();
    }

    if (scanner) {
        if (!scanner->status_ok()) {
            _set_scanner_done();
            return scanner->status;
        }
        block->swap(*scanner->current_block);
        if (!scanner->current_block->mem_reuse()) {
            // it depends on the memory strategy of ScanNode/ScanOperator
            // we should double check `mem_reuse()` of `current_block` to make sure it can be reused
            scanner->current_block = get_free_block(_batch_size);
        }
        if (scanner->eos) { // current scanner is finished, and no more data to read
            std::weak_ptr<ScannerDelegate> next_scanner;
            // submit one of the remaining scanners
            if (_scanners.try_dequeue(next_scanner)) {
                // reuse current running scanner, just reset some states.
                scanner->reuse_scanner(next_scanner);
                submit_running_scanner(scanner);
            } else {
                // no more scanner to be scheduled
                // `_free_blocks` serve all running scanners, maybe it's too large for the remaining scanners
                int free_blocks_for_each = _free_blocks.size_approx() / _num_running_scanners;
                _num_running_scanners--;
                std::lock_guard<std::mutex> fl(_free_blocks_lock);
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
            submit_running_scanner(scanner);
        }
        // scale up
        _try_to_scale_up();
    }

    if (_num_finished_scanners == _all_scanners.size() && _blocks_queue.empty()) {
        _set_scanner_done();
        _is_finished = true;
    }
    *eos = done();
    return Status::OK();
}

void ScannerContext::_try_to_scale_up() {
    // Four criteria to determine whether to increase the parallelism of the scanners
    // 1. It ran for at least `SCALE_UP_DURATION` ms after last scale up
    // 2. Half(`WAIT_BLOCK_DURATION_RATIO`) of the duration is waiting to get blocks
    // 3. `_free_blocks_memory_usage` < `_max_bytes_in_queue`, remains enough memory to scale up
    // 4. At most scale up `MAX_SCALE_UP_RATIO` times to `_max_thread_num`
    if (_scanners.size_approx() > 0 &&
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

        std::lock_guard<std::mutex> fl(_free_blocks_lock);
        bool is_scale_up = false;
        // calculate the number of scanners that can be scheduled
        int num_add = std::min(_num_running_scanners * SCALE_UP_RATIO,
                               _max_thread_num * MAX_SCALE_UP_RATIO - _num_running_scanners);
        num_add = std::max(num_add, 1);
        for (int i = 0; i < num_add; ++i) {
            vectorized::BlockUPtr add_block = nullptr;
            // reuse block in `_free_blocks` firstly
            if (!_free_blocks.try_dequeue(add_block)) {
                if (_free_blocks_memory_usage < _max_bytes_in_queue) {
                    add_block = vectorized::Block::create_unique(_output_tuple_desc->slots(),
                                                                 _batch_size,
                                                                 true /*ignore invalid slots*/);
                }
            } else {
                // comes from `_free_blocks`, decrease first, then will be added back.
                _free_blocks_memory_usage -= add_block->allocated_bytes();
                _free_blocks_memory_usage_mark->set(_free_blocks_memory_usage);
            }
            if (add_block) {
                // get enough memory to launch one more scanner.
                std::weak_ptr<ScannerDelegate> add_scanner;
                if (_scanners.try_dequeue(add_scanner)) {
                    std::shared_ptr<RunningScanner> scale_up_scanner =
                            std::make_shared<RunningScanner>(add_scanner, std::move(add_block));
                    _free_blocks_memory_usage += _estimated_block_size;
                    _free_blocks_memory_usage_mark->set(_free_blocks_memory_usage);
                    // `first_block` is used to update `_free_blocks_memory_usage`,
                    // we have got the `_estimated_block_size`, no need for further updates
                    scale_up_scanner->first_block = false;
                    submit_running_scanner(scale_up_scanner);
                    _num_running_scanners++;
                    _scale_up_scanners_counter->update(1);
                    is_scale_up = true;
                } else {
                    break;
                }
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

void ScannerContext::stop_scanners(RuntimeState* state) {
    if (!done()) {
        _should_stop = true;
        _set_scanner_done();
        for (const std::weak_ptr<ScannerDelegate>& scanner : _all_scanners) {
            if (std::shared_ptr<ScannerDelegate> sc = scanner.lock()) {
                sc->_scanner->try_stop();
            }
        }
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

} // namespace doris::vectorized
