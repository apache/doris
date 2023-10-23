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
#include "pipeline/pipeline_x/dependency.h"
#include "vec/core/block.h"

namespace doris {
namespace pipeline {

DataQueue::DataQueue(int child_count, WriteDependency* dependency)
        : _queue_blocks_lock(child_count),
          _queue_blocks(child_count),
          _free_blocks_lock(child_count),
          _free_blocks(child_count),
          _child_count(child_count),
          _is_finished(child_count),
          _is_canceled(child_count),
          _cur_bytes_in_queue(child_count),
          _cur_blocks_nums_in_queue(child_count),
          _flag_queue_idx(0),
          _dependency(dependency) {
    for (int i = 0; i < child_count; ++i) {
        _queue_blocks_lock[i].reset(new std::mutex());
        _free_blocks_lock[i].reset(new std::mutex());
        _is_finished[i] = false;
        _is_canceled[i] = false;
        _cur_bytes_in_queue[i] = 0;
        _cur_blocks_nums_in_queue[i] = 0;
    }
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
bool DataQueue::has_enough_space_to_push(int child_idx) {
    return _cur_bytes_in_queue[child_idx].load() < MAX_BYTE_OF_QUEUE / 2;
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
            if (_dependency && _dependency->is_not_fake()) {
                if (!_is_finished[_flag_queue_idx]) {
                    _dependency->block_reading();
                }
                _dependency->set_ready_for_write();
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
        if (_dependency && _dependency->is_not_fake()) {
            _dependency->set_ready_for_read();
            _dependency->block_writing();
        }
        //this only use to record the queue[0] for profile
        _max_bytes_in_queue = std::max(_max_bytes_in_queue, _cur_bytes_in_queue[0].load());
        _max_size_of_queue = std::max(_max_size_of_queue, (int64)_queue_blocks[0].size());
    }
}

void DataQueue::set_finish(int child_idx) {
    std::lock_guard<std::mutex> l(*_queue_blocks_lock[child_idx]);
    _is_finished[child_idx] = true;
    if (_dependency && _dependency->is_not_fake()) {
        _dependency->set_ready_for_read();
    }
}

void DataQueue::set_canceled(int child_idx) {
    std::lock_guard<std::mutex> l(*_queue_blocks_lock[child_idx]);
    DCHECK(!_is_finished[child_idx]);
    _is_canceled[child_idx] = true;
    _is_finished[child_idx] = true;
    if (_dependency && _dependency->is_not_fake()) {
        _dependency->set_ready_for_read();
    }
}

bool DataQueue::is_finish(int child_idx) {
    return _is_finished[child_idx];
}

bool DataQueue::is_all_finish() {
    for (int i = 0; i < _child_count; ++i) {
        if (_is_finished[i] == false) {
            return false;
        }
    }
    return true;
}

} // namespace pipeline
} // namespace doris
