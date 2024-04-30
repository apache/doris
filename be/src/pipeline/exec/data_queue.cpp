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

#include "data_queue.h"

#include <glog/logging.h>

#include <algorithm>
#include <mutex>
#include <utility>

#include "gutil/integral_types.h"
#include "pipeline/dependency.h"
#include "vec/core/block.h"

namespace doris {
namespace pipeline {

DataQueue::DataQueue(int child_count)
        : _queue_blocks_lock(child_count),
          _queue_blocks(child_count),
          _free_blocks_lock(child_count),
          _free_blocks(child_count),
          _child_count(child_count),
          _is_finished(child_count),
          _is_canceled(child_count),
          _cur_bytes_in_queue(child_count),
          _cur_blocks_nums_in_queue(child_count),
          _flag_queue_idx(0) {
    for (int i = 0; i < child_count; ++i) {
        _queue_blocks_lock[i].reset(new std::mutex());
        _free_blocks_lock[i].reset(new std::mutex());
        _is_finished[i] = false;
        _is_canceled[i] = false;
        _cur_bytes_in_queue[i] = 0;
        _cur_blocks_nums_in_queue[i] = 0;
    }
    _un_finished_counter = child_count;
    _sink_dependencies.resize(child_count, nullptr);
}

std::unique_ptr<vectorized::Block> DataQueue::get_free_block(int child_idx) {
    {
        std::lock_guard<std::mutex> l(*_free_blocks_lock[child_idx]);
        if (!_free_blocks[child_idx].empty()) {
            auto block = std::move(_free_blocks[child_idx].front());
            _free_blocks[child_idx].pop_front();
            return block;
        }
    }

    return vectorized::Block::create_unique();
}

void DataQueue::push_free_block(std::unique_ptr<vectorized::Block> block, int child_idx) {
    DCHECK(block->rows() == 0);
    std::lock_guard<std::mutex> l(*_free_blocks_lock[child_idx]);
    _free_blocks[child_idx].emplace_back(std::move(block));
}

//use sink to check can_write
bool DataQueue::has_enough_space_to_push() {
    DCHECK(_cur_bytes_in_queue.size() == 1);
    return _cur_bytes_in_queue[0].load() < MAX_BYTE_OF_QUEUE / 2;
}

//use source to check can_read
bool DataQueue::has_data_or_finished(int child_idx) {
    return remaining_has_data() || _is_finished[child_idx];
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
        if (_cur_blocks_nums_in_queue[_flag_queue_idx] > 0) {
            return true;
        }
    }
    return false;
}

//the _flag_queue_idx indicate which queue has data, and in check can_read
//will be set idx in remaining_has_data function
Status DataQueue::get_block_from_queue(std::unique_ptr<vectorized::Block>* output_block,
                                       int* child_idx) {
    if (_is_canceled[_flag_queue_idx]) {
        return Status::InternalError("Current queue of idx {} have beed canceled: ",
                                     _flag_queue_idx);
    }

    {
        std::lock_guard<std::mutex> l(*_queue_blocks_lock[_flag_queue_idx]);
        if (_cur_blocks_nums_in_queue[_flag_queue_idx] > 0) {
            *output_block = std::move(_queue_blocks[_flag_queue_idx].front());
            _queue_blocks[_flag_queue_idx].pop_front();
            if (child_idx) {
                *child_idx = _flag_queue_idx;
            }
            _cur_bytes_in_queue[_flag_queue_idx] -= (*output_block)->allocated_bytes();
            _cur_blocks_nums_in_queue[_flag_queue_idx] -= 1;
            if (_cur_blocks_nums_in_queue[_flag_queue_idx] == 0) {
                _sink_dependencies[_flag_queue_idx]->set_ready();
            }
            auto old_value = _cur_blocks_total_nums.fetch_sub(1);
            if (old_value == 1 && _source_dependency) {
                set_source_block();
            }
        } else {
            if (_is_finished[_flag_queue_idx]) {
                _data_exhausted = true;
            }
        }
    }
    return Status::OK();
}

void DataQueue::push_block(std::unique_ptr<vectorized::Block> block, int child_idx) {
    if (!block) {
        return;
    }
    {
        std::lock_guard<std::mutex> l(*_queue_blocks_lock[child_idx]);
        _cur_bytes_in_queue[child_idx] += block->allocated_bytes();
        _queue_blocks[child_idx].emplace_back(std::move(block));
        _cur_blocks_nums_in_queue[child_idx] += 1;

        if (_cur_blocks_nums_in_queue[child_idx] > _max_blocks_in_sub_queue) {
            _sink_dependencies[child_idx]->block();
        }
        _cur_blocks_total_nums++;
        if (_source_dependency) {
            set_source_ready();
        }
        //this only use to record the queue[0] for profile
        _max_bytes_in_queue = std::max(_max_bytes_in_queue, _cur_bytes_in_queue[0].load());
        _max_size_of_queue = std::max(_max_size_of_queue, (int64)_queue_blocks[0].size());
    }
}

void DataQueue::set_finish(int child_idx) {
    std::lock_guard<std::mutex> l(*_queue_blocks_lock[child_idx]);
    if (_is_finished[child_idx]) {
        return;
    }
    _is_finished[child_idx] = true;
    if (_un_finished_counter.fetch_sub(1) == 1) {
        _is_all_finished = true;
    }
    set_source_ready();
}

void DataQueue::set_canceled(int child_idx) {
    std::lock_guard<std::mutex> l(*_queue_blocks_lock[child_idx]);
    DCHECK(!_is_finished[child_idx]);
    _is_canceled[child_idx] = true;
    _is_finished[child_idx] = true;
    if (_un_finished_counter.fetch_sub(1) == 1) {
        _is_all_finished = true;
    }
    set_source_ready();
}

bool DataQueue::is_finish(int child_idx) {
    return _is_finished[child_idx];
}

bool DataQueue::is_all_finish() {
    return _is_all_finished;
}

void DataQueue::set_source_ready() {
    if (_source_dependency) {
        std::unique_lock lc(_source_lock);
        _source_dependency->set_ready();
    }
}

void DataQueue::set_source_block() {
    if (_cur_blocks_total_nums == 0 && !is_all_finish()) {
        std::unique_lock lc(_source_lock);
        // Performing the judgment twice, attempting to avoid blocking the source as much as possible.
        if (_cur_blocks_total_nums == 0 && !is_all_finish()) {
            _source_dependency->block();
        }
    }
}

} // namespace pipeline
} // namespace doris
