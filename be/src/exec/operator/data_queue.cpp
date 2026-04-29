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

#include "exec/operator/data_queue.h"

#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "common/thread_safety_annotations.h"
#include "core/block/block.h"
#include "exec/pipeline/dependency.h"

namespace doris {

Status SubQueue::try_pop(std::unique_ptr<Block>* output_block) {
    bool need_notify_sink_ready = false;
    {
        LockGuard l(queue_lock);
        if (!blocks.empty()) {
            *output_block = std::move(blocks.front());
            blocks.pop_front();
            bytes_in_queue -= (*output_block)->allocated_bytes();
            blocks_in_queue -= 1;
            need_notify_sink_ready = blocks.empty();
        }
    }
    // Notify outside of queue_lock to avoid nested locks.
    if (need_notify_sink_ready) {
        sink_dependency->set_ready();
    }
    return Status::OK();
}

Status SubQueue::try_push(std::unique_ptr<Block> block) {
    bool need_block_sink = false;
    {
        LockGuard l(queue_lock);
        if (is_finished) {
            return Status::EndOfFile("Already finish");
        }
        bytes_in_queue += block->allocated_bytes();
        blocks.emplace_back(std::move(block));
        blocks_in_queue += 1;
        need_block_sink = (static_cast<int64_t>(blocks.size()) > max_blocks_in_queue.load());
    }
    // Notify outside of queue_lock to avoid nested locks.
    if (need_block_sink) {
        sink_dependency->block();
    }
    return Status::OK();
}

bool SubQueue::mark_finished(std::atomic_uint32_t& unfinished_counter,
                              std::atomic_bool& all_finished) {
    LockGuard l(queue_lock);
    if (is_finished) {
        return false;
    }
    is_finished = true;
    if (unfinished_counter.fetch_sub(1) == 1) {
        all_finished = true;
    }
    return true;
}

void SubQueue::clear_blocks() {
    bool need_set_always_ready = false;
    {
        LockGuard l(queue_lock);
        if (!blocks.empty()) {
            blocks.clear();
            bytes_in_queue = 0;
            blocks_in_queue = 0;
            need_set_always_ready = true;
        }
    }
    // Notify outside of queue_lock to keep lock ordering simple.
    if (need_set_always_ready) {
        sink_dependency->set_always_ready();
    }
}

DataQueue::DataQueue(int child_count) : _sub_queues(child_count), _child_count(child_count) {
    for (auto& sub : _sub_queues) {
        sub = std::make_unique<SubQueue>();
    }
    _un_finished_counter = child_count;
}

bool DataQueue::has_more_data() const {
    return _cur_blocks_total_nums.load() > 0;
}

void DataQueue::set_source_dependency(
        std::shared_ptr<Dependency> source_dependency) NO_THREAD_SAFETY_ANALYSIS {
    _source_dependency = std::move(source_dependency);
}

void DataQueue::set_sink_dependency(Dependency* sink_dependency, int child_idx) {
    _sub_queues[child_idx]->sink_dependency = sink_dependency;
}

void DataQueue::set_max_blocks_in_sub_queue(int64_t max_blocks) {
    for (auto& sub : _sub_queues) {
        sub->max_blocks_in_queue = max_blocks;
    }
}

void DataQueue::set_low_memory_mode() {
    _is_low_memory_mode = true;
    for (auto& sub : _sub_queues) {
        sub->max_blocks_in_queue = 1;
    }
    clear_free_blocks();
}

std::unique_ptr<Block> DataQueue::get_free_block(int child_idx) {
    auto& sub = *_sub_queues[child_idx];
    {
        LockGuard l(sub.free_lock);
        if (!sub.free_blocks.empty()) {
            auto block = std::move(sub.free_blocks.front());
            sub.free_blocks.pop_front();
            return block;
        }
    }

    return Block::create_unique();
}

void DataQueue::push_free_block(std::unique_ptr<Block> block, int child_idx) {
    DCHECK(block->rows() == 0);

    if (!_is_low_memory_mode) {
        auto& sub = *_sub_queues[child_idx];
        LockGuard l(sub.free_lock);
        sub.free_blocks.emplace_back(std::move(block));
    }
}

void DataQueue::clear_free_blocks() {
    for (auto& sub : _sub_queues) {
        LockGuard l(sub->free_lock);
        std::deque<std::unique_ptr<Block>> tmp_queue;
        sub->free_blocks.swap(tmp_queue);
    }
}

void DataQueue::terminate() {
    for (int i = 0; i < _child_count; ++i) {
        set_finish(i);
        _sub_queues[i]->clear_blocks();
    }
    clear_free_blocks();
}

//check which queue have data, and save the idx in _flag_queue_idx,
//so next loop, will check the record idx + 1 first
//maybe it's useful with many queue, others maybe always 0
bool DataQueue::remaining_has_data() {
    int count = _child_count;
    while (--count >= 0) {
        _flag_queue_idx++;
        if (_flag_queue_idx == _child_count) {
            _flag_queue_idx = 0;
        }
        if (_sub_queues[_flag_queue_idx]->blocks_in_queue.load() > 0) {
            return true;
        }
    }
    return false;
}

//the _flag_queue_idx indicate which queue has data, and in check can_read
//will be set idx in remaining_has_data function
Status DataQueue::get_block_from_queue(std::unique_ptr<Block>* output_block, int* child_idx) {
    const int idx = _flag_queue_idx;
    auto& sub = *_sub_queues[idx];

    RETURN_IF_ERROR(sub.try_pop(output_block));
    if (*output_block) {
        if (child_idx) {
            *child_idx = idx;
        }
        auto old_total = _cur_blocks_total_nums.fetch_sub(1);
        if (old_total == 1) {
            set_source_block();
        }
    }
    return Status::OK();
}

Status DataQueue::push_block(std::unique_ptr<Block> block, int child_idx) {
    if (!block) {
        return Status::OK();
    }
    auto& sub = *_sub_queues[child_idx];
    RETURN_IF_ERROR(sub.try_push(std::move(block)));
    _cur_blocks_total_nums++;
    set_source_ready();
    return Status::OK();
}

void DataQueue::set_finish(int child_idx) {
    auto& sub = *_sub_queues[child_idx];
    if (!sub.mark_finished(_un_finished_counter, _is_all_finished)) {
        return;
    }
    set_source_ready();
}

bool DataQueue::is_all_finish() {
    return _is_all_finished;
}

void DataQueue::set_source_ready() {
    LockGuard lc(_source_lock);
    if (_source_dependency) {
        _source_dependency->set_ready();
    }
}

void DataQueue::set_source_block() {
    // Re-check under _source_lock to avoid blocking the source when a concurrent push
    // has already added new blocks (or all children have finished) since we observed
    // the counter drop to zero.
    LockGuard lc(_source_lock);
    if (_source_dependency && _cur_blocks_total_nums == 0 && !is_all_finish()) {
        _source_dependency->block();
    }
}

} // namespace doris
