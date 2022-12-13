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

#include <mutex>

#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"

namespace doris {
namespace vectorized {
class Block;
}
namespace pipeline {

DataQueue::DataQueue(int child_count) {
    _child_count = child_count;
    _queue_blocks.resize(child_count);
    _queue_blocks_lock.resize(child_count);
    for (int i = 0; i < child_count; ++i) {
        _queue_blocks_lock[i].reset(new std::mutex());
    }
}

bool DataQueue::remaining_has_data() {
    int count = _child_count - 1;
    //check which queue have data, and save the idx in _flag_queue_idx,
    //so next loop, will check the record idx + 1 first
    while (count >= 0) {
        _flag_queue_idx = (_flag_queue_idx + 1) % _child_count;
        std::lock_guard<std::mutex> l(*_queue_blocks_lock[_flag_queue_idx]);
        if (!_queue_blocks[count].empty()) {
            _flag_queue_idx = count;
            return true;
        }
        count--;
    }
    return false;
}

std::unique_ptr<vectorized::Block> DataQueue::get_block_from_queue() {
    std::lock_guard<std::mutex> l(*_queue_blocks_lock[_flag_queue_idx]);
    auto block = std::move(_queue_blocks[_flag_queue_idx].front());
    _queue_blocks[_flag_queue_idx].pop_front();
    return block;
}

//TODO: Now it's can write to deque without any limit
//need to control the deque size and limit all block used bytes, and maybe add free blocks queue
void DataQueue::push_block(int child_idx, std::unique_ptr<vectorized::Block> block) {
    std::lock_guard<std::mutex> l(*_queue_blocks_lock[child_idx]);
    _queue_blocks[child_idx].push_back(std::move(block));
}

} // namespace pipeline
} // namespace doris