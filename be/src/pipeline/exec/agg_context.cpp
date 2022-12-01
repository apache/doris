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

#include "agg_context.h"

#include "runtime/descriptors.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris {
namespace pipeline {

std::unique_ptr<vectorized::Block> AggContext::get_free_block() {
    {
        std::lock_guard<std::mutex> l(_free_blocks_lock);
        if (!_free_blocks.empty()) {
            auto block = std::move(_free_blocks.back());
            _free_blocks.pop_back();
            return block;
        }
    }

    return std::make_unique<vectorized::Block>();
}

void AggContext::return_free_block(std::unique_ptr<vectorized::Block> block) {
    DCHECK(block->rows() == 0);
    std::lock_guard<std::mutex> l(_free_blocks_lock);
    _free_blocks.emplace_back(std::move(block));
}

bool AggContext::has_data_or_finished() {
    std::unique_lock<std::mutex> l(_transfer_lock);
    return !_blocks_queue.empty() || _is_finished;
}

Status AggContext::get_block(std::unique_ptr<vectorized::Block>* block) {
    std::unique_lock<std::mutex> l(_transfer_lock);
    if (_is_canceled) {
        return Status::InternalError("AggContext canceled");
    }
    if (!_blocks_queue.empty()) {
        auto [block_ptr, block_size] = std::move(_blocks_queue.front());
        *block = std::move(block_ptr);
        _blocks_queue.pop_front();
        _cur_bytes_in_queue -= block_size;
    } else {
        if (_is_finished) {
            _data_exhausted = true;
        }
    }
    return Status::OK();
}

bool AggContext::has_enough_space_to_push() {
    std::unique_lock<std::mutex> l(_transfer_lock);
    return _cur_bytes_in_queue < MAX_BYTE_OF_QUEUE / 2;
}

void AggContext::push_block(std::unique_ptr<vectorized::Block> block) {
    if (!block) {
        return;
    }
    auto block_size = block->allocated_bytes();
    std::unique_lock<std::mutex> l(_transfer_lock);
    _blocks_queue.emplace_back(std::move(block), block_size);
    _cur_bytes_in_queue += block_size;

    _max_bytes_in_queue = std::max(_max_bytes_in_queue, _cur_bytes_in_queue);
    _max_size_of_queue = std::max(_max_size_of_queue, (int64)_blocks_queue.size());
}

void AggContext::set_finish() {
    std::unique_lock<std::mutex> l(_transfer_lock);
    _is_finished = true;
}

void AggContext::set_canceled() {
    std::unique_lock<std::mutex> l(_transfer_lock);
    DCHECK(!_is_finished);
    _is_canceled = true;
    _is_finished = true;
}

bool AggContext::is_finish() {
    std::unique_lock<std::mutex> l(_transfer_lock);
    return _is_finished;
}

} // namespace pipeline
} // namespace doris