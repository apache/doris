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

#include "multi_cast_data_streamer.h"

#include "pipeline/dependency.h"
#include "pipeline/exec/multi_cast_data_stream_source.h"
#include "runtime/runtime_state.h"

namespace doris::pipeline {

MultiCastBlock::MultiCastBlock(vectorized::Block* block, int used_count, int un_finish_copy,
                               size_t mem_size)
        : _used_count(used_count), _un_finish_copy(un_finish_copy), _mem_size(mem_size) {
    _block = vectorized::Block::create_unique(block->get_columns_with_type_and_name());
    block->clear();
}

Status MultiCastDataStreamer::pull(int sender_idx, doris::vectorized::Block* block, bool* eos) {
    int* un_finish_copy = nullptr;
    int use_count = 0;
    {
        std::lock_guard l(_mutex);
        auto& pos_to_pull = _sender_pos_to_read[sender_idx];
        const auto end = _multi_cast_blocks.end();
        DCHECK(pos_to_pull != end);

        *block = *pos_to_pull->_block;

        _cumulative_mem_size -= pos_to_pull->_mem_size;

        pos_to_pull->_used_count--;
        use_count = pos_to_pull->_used_count;
        un_finish_copy = &pos_to_pull->_un_finish_copy;

        pos_to_pull++;

        if (pos_to_pull == end) {
            _block_reading(sender_idx);
        }

        *eos = _eos and pos_to_pull == end;
    }

    if (use_count == 0) {
        // will clear _multi_cast_blocks
        _wait_copy_block(block, *un_finish_copy);
    } else {
        _copy_block(block, *un_finish_copy);
    }

    return Status::OK();
}

void MultiCastDataStreamer::_copy_block(vectorized::Block* block, int& un_finish_copy) {
    const auto rows = block->rows();
    for (int i = 0; i < block->columns(); ++i) {
        block->get_by_position(i).column = block->get_by_position(i).column->clone_resized(rows);
    }

    std::unique_lock l(_mutex);
    un_finish_copy--;
    if (un_finish_copy == 0) {
        l.unlock();
        _cv.notify_one();
    }
}

void MultiCastDataStreamer::_wait_copy_block(vectorized::Block* block, int& un_finish_copy) {
    std::unique_lock l(_mutex);
    _cv.wait(l, [&]() { return un_finish_copy == 0; });
    _multi_cast_blocks.pop_front();
}

Status MultiCastDataStreamer::push(RuntimeState* state, doris::vectorized::Block* block, bool eos) {
    auto rows = block->rows();
    COUNTER_UPDATE(_process_rows, rows);

    const auto block_mem_size = block->allocated_bytes();
    _cumulative_mem_size += block_mem_size;
    COUNTER_SET(_peak_mem_usage, std::max(_cumulative_mem_size, _peak_mem_usage->value()));

    {
        std::lock_guard l(_mutex);
        _multi_cast_blocks.emplace_back(block, _cast_sender_count, _cast_sender_count - 1,
                                        block_mem_size);
        // last elem
        auto end = std::prev(_multi_cast_blocks.end());
        for (int i = 0; i < _sender_pos_to_read.size(); ++i) {
            if (_sender_pos_to_read[i] == _multi_cast_blocks.end()) {
                _sender_pos_to_read[i] = end;
                _set_ready_for_read(i);
            }
        }
        _eos = eos;
    }

    if (_eos) {
        for (auto* read_dep : _dependencies) {
            read_dep->set_always_ready();
        }
    }
    return Status::OK();
}

void MultiCastDataStreamer::_set_ready_for_read(int sender_idx) {
    if (_dependencies.empty()) {
        return;
    }
    auto* dep = _dependencies[sender_idx];
    DCHECK(dep);
    dep->set_ready();
}

void MultiCastDataStreamer::_block_reading(int sender_idx) {
    if (_dependencies.empty()) {
        return;
    }
    auto* dep = _dependencies[sender_idx];
    DCHECK(dep);
    dep->block();
}

} // namespace doris::pipeline