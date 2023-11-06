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

#include "pipeline/pipeline_x/dependency.h"
#include "runtime/runtime_state.h"

namespace doris::pipeline {

MultiCastBlock::MultiCastBlock(vectorized::Block* block, int used_count, size_t mem_size)
        : _used_count(used_count), _mem_size(mem_size) {
    _block = vectorized::Block::create_unique(block->get_columns_with_type_and_name());
    block->clear();
}

void MultiCastDataStreamer::pull(int sender_idx, doris::vectorized::Block* block, bool* eos) {
    std::lock_guard l(_mutex);
    auto& pos_to_pull = _sender_pos_to_read[sender_idx];
    if (pos_to_pull != _multi_cast_blocks.end()) {
        if (pos_to_pull->_used_count == 1) {
            DCHECK(pos_to_pull == _multi_cast_blocks.begin());
            pos_to_pull->_block->swap(*block);

            _cumulative_mem_size -= pos_to_pull->_mem_size;
            pos_to_pull++;
            _multi_cast_blocks.pop_front();
        } else {
            pos_to_pull->_used_count--;
            pos_to_pull->_block->create_same_struct_block(0)->swap(*block);
            (void)vectorized::MutableBlock(block).merge(*pos_to_pull->_block);
            pos_to_pull++;
        }
    }
    *eos = _eos and pos_to_pull == _multi_cast_blocks.end();
    if (pos_to_pull == _multi_cast_blocks.end()) {
        _block_reading(sender_idx);
    }
}

void MultiCastDataStreamer::close_sender(int sender_idx) {
    std::lock_guard l(_mutex);
    auto& pos_to_pull = _sender_pos_to_read[sender_idx];
    while (pos_to_pull != _multi_cast_blocks.end()) {
        if (pos_to_pull->_used_count == 1) {
            DCHECK(pos_to_pull == _multi_cast_blocks.begin());
            _cumulative_mem_size -= pos_to_pull->_mem_size;
            pos_to_pull++;
            _multi_cast_blocks.pop_front();
        } else {
            pos_to_pull->_used_count--;
            pos_to_pull++;
        }
    }
    _closed_sender_count++;
    _block_reading(sender_idx);
}

Status MultiCastDataStreamer::push(RuntimeState* state, doris::vectorized::Block* block, bool eos) {
    auto rows = block->rows();
    COUNTER_UPDATE(_process_rows, rows);

    auto block_mem_size = block->allocated_bytes();
    std::lock_guard l(_mutex);
    int need_process_count = _cast_sender_count - _closed_sender_count;
    if (need_process_count == 0) {
        return Status::EndOfFile("All data streamer is EOF");
    }
    // TODO: if the [queue back block rows + block->rows()] < batch_size, better
    // do merge block. but need check the need_process_count and used_count whether
    // equal
    _multi_cast_blocks.emplace_back(block, need_process_count, block_mem_size);
    _cumulative_mem_size += block_mem_size;
    COUNTER_SET(_peak_mem_usage, std::max(_cumulative_mem_size, _peak_mem_usage->value()));

    auto end = _multi_cast_blocks.end();
    end--;
    for (int i = 0; i < _sender_pos_to_read.size(); ++i) {
        if (_sender_pos_to_read[i] == _multi_cast_blocks.end()) {
            _sender_pos_to_read[i] = end;
            _set_ready_for_read(i);
        }
    }
    _eos = eos;
    return Status::OK();
}

void MultiCastDataStreamer::_set_ready_for_read(int sender_idx) {
    if (!_has_dependencys) {
        return;
    }
    auto* dep = _dependencys[sender_idx];
    DCHECK(dep);
    dep->set_ready_for_read();
}

void MultiCastDataStreamer::_set_ready_for_read() {
    if (!_has_dependencys) {
        return;
    }
    for (auto* dep : _dependencys) {
        DCHECK(dep);
        dep->set_ready_for_read();
    }
}

void MultiCastDataStreamer::_block_reading(int sender_idx) {
    if (!_has_dependencys) {
        return;
    }
    auto* dep = _dependencys[sender_idx];
    DCHECK(dep);
    dep->block_reading();
}

} // namespace doris::pipeline