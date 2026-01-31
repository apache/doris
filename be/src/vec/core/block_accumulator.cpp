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

#include "block_accumulator.h"

#include <glog/logging.h>

#include "vec/core/block.h"

namespace doris::vectorized {

Status BlockAccumulator::push(Block&& block) {
    size_t input_rows = block.rows();
    for (size_t start = 0; start < input_rows;) {
        size_t remain_rows = input_rows - start;
        size_t need_rows = 0;
        if (!_tmp_block.is_empty_column()) {
            // If tmp_block is not empty, it means there was a previous block that didn't reach batch_size, need to continue adding data
            need_rows = std::min(_batch_size - _tmp_block.rows(), remain_rows);
            RETURN_IF_ERROR(_tmp_block.add_rows(&block, start, need_rows));
        } else {
            // If tmp_block is empty, can directly create a new tmp_block
            need_rows = std::min(_batch_size, remain_rows);
            _tmp_block = block.clone_empty();
            RETURN_IF_ERROR(_tmp_block.add_rows(&block, start, need_rows));
        }
        if (_tmp_block.rows() >= _batch_size) {
            // If tmp_block's row count reaches or exceeds batch_size, push it to the queue
            // But theoretically it should not exceed batch_size
            // Will be checked in push_tmp_block_to_queue()
            RETURN_IF_ERROR(push_tmp_block_to_queue());
        }
        start += need_rows;
    }
    return Status::OK();
}

Status BlockAccumulator::set_finish() {
    if (_tmp_block.rows() > 0) {
        RETURN_IF_ERROR(push_tmp_block_to_queue());
    }
    return Status::OK();
}

Status BlockAccumulator::push_tmp_block_to_queue() {
    if (_tmp_block.rows() > _batch_size) {
        return Status::InternalError(
                "tmp block rows exceed batch size , tmp block rows: {}, batch size: {}",
                _tmp_block.rows(), _batch_size);
    }
    Block block = _tmp_block.to_block();
    _queue.push_back(std::move(block));
    _tmp_block.clear();
    return Status::OK();
}

bool BlockAccumulator::pull(Block& block) {
    if (_queue.empty()) {
        return false;
    }
    block = std::move(_queue.front());
    _queue.pop_front();
    return true;
}

void BlockAccumulator::close() {
    _queue.clear();
    _tmp_block.clear();
}

Status PipelineBlockAccumulator::push(Block&& block) {
    if (_is_finished) {
        return Status::InternalError("cannot push data after finished");
    }

    if (!_in_block) {
        // If there is no in_block currently, directly use the new block as in_block
        _in_block = MutableBlock::create_unique(std::move(block));
    } else if (_in_block->rows() + block.rows() >= _batch_size) {
        // If current in_block plus the new block exceeds batch_size, first make current in_block as out_block
        // Then use the new block as the new in_block
        DCHECK_EQ(_out_block, nullptr);
        _out_block = Block::create_unique(_in_block->to_block());
        _in_block = MutableBlock::create_unique(std::move(block));
    } else {
        // Otherwise append the new block to the current in_block
        RETURN_IF_ERROR(_in_block->merge(block));
    }
    return Status::OK();
}

Status PipelineBlockAccumulator::push_with_selector(Block&& block,
                                                    const IColumn::Selector& selector) {
    if (_is_finished) {
        return Status::InternalError("cannot push data after finished");
    }

    if (!_in_block) {
        // If there is no in_block currently, directly use the new block (with selector) as in_block
        _in_block = MutableBlock::create_unique(block.clone_empty());
        RETURN_IF_ERROR(block.append_to_block_by_selector(_in_block.get(), selector));
    } else if (_in_block->rows() + block.rows() >= _batch_size) {
        // If current in_block plus the new block exceeds batch_size, first make current in_block as out_block
        // Then use the new block (with selector) as the new in_block
        DCHECK_EQ(_out_block, nullptr);
        _out_block = Block::create_unique(_in_block->to_block());
        _in_block = MutableBlock::create_unique(block.clone_empty());
        RETURN_IF_ERROR(block.append_to_block_by_selector(_in_block.get(), selector));
    } else {
        // Otherwise append the new block (with selector) to the current in_block
        RETURN_IF_ERROR(block.append_to_block_by_selector(_in_block.get(), selector));
    }
    return Status::OK();
}

BlockUPtr PipelineBlockAccumulator::pull() {
    if (_is_finished && _out_block == nullptr && _in_block != nullptr) {
        auto result = Block::create_unique(_in_block->to_block());
        _in_block = nullptr;
        return result;
    }
    auto result = std::move(_out_block);
    _out_block = nullptr;
    return result;
}

bool PipelineBlockAccumulator::has_output() {
    return _out_block != nullptr || (_is_finished && _in_block != nullptr);
}

bool PipelineBlockAccumulator::need_input() {
    return !_is_finished && _out_block == nullptr;
}

bool PipelineBlockAccumulator::is_finished() {
    return _is_finished && _out_block == nullptr && _in_block == nullptr;
}

void PipelineBlockAccumulator::set_finish() {
    _is_finished = true;
}

}; // namespace doris::vectorized