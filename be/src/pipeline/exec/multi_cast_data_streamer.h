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

#pragma once

#include "vec/sink/vdata_stream_sender.h"

namespace doris::pipeline {

class MultiCastDependency;
struct MultiCastBlock {
    MultiCastBlock(vectorized::Block* block, int used_count, size_t mem_size);

    std::unique_ptr<vectorized::Block> _block;
    int _used_count;
    size_t _mem_size;
};

// TDOD: MultiCastDataStreamer same as the data queue, maybe rethink union and refactor the
// code
class MultiCastDataStreamer {
public:
    MultiCastDataStreamer(const RowDescriptor& row_desc, ObjectPool* pool, int cast_sender_count)
            : _row_desc(row_desc),
              _profile(pool->add(new RuntimeProfile("MultiCastDataStreamSink"))),
              _cast_sender_count(cast_sender_count) {
        _sender_pos_to_read.resize(cast_sender_count, _multi_cast_blocks.end());
        _dependencys.resize(cast_sender_count, nullptr);
        _peak_mem_usage = ADD_COUNTER(profile(), "PeakMemUsage", TUnit::BYTES);
        _process_rows = ADD_COUNTER(profile(), "ProcessRows", TUnit::UNIT);
    };

    ~MultiCastDataStreamer() = default;

    void pull(int sender_idx, vectorized::Block* block, bool* eos);

    void close_sender(int sender_idx);

    Status push(RuntimeState* state, vectorized::Block* block, bool eos);

    // use sink to check can_write, now always true after we support spill to disk
    bool can_write() { return true; }

    bool can_read(int sender_idx) {
        std::lock_guard l(_mutex);
        return _sender_pos_to_read[sender_idx] != _multi_cast_blocks.end() || _eos;
    }

    const RowDescriptor& row_desc() { return _row_desc; }

    RuntimeProfile* profile() { return _profile; }

    void set_eos() {
        std::lock_guard l(_mutex);
        _eos = true;
        _set_ready_for_read();
    }

    void set_dep_by_sender_idx(int sender_idx, MultiCastDependency* dep) {
        _dependencys[sender_idx] = dep;
        _has_dependencys = true;
        _block_reading(sender_idx);
    }

private:
    void _set_ready_for_read(int sender_idx);
    void _set_ready_for_read();
    void _block_reading(int sender_idx);

    const RowDescriptor& _row_desc;
    RuntimeProfile* _profile;
    std::list<MultiCastBlock> _multi_cast_blocks;
    std::vector<std::list<MultiCastBlock>::iterator> _sender_pos_to_read;
    std::mutex _mutex;
    bool _eos = false;
    int _cast_sender_count = 0;
    int _closed_sender_count = 0;
    int64_t _cumulative_mem_size = 0;

    RuntimeProfile::Counter* _process_rows;
    RuntimeProfile::Counter* _peak_mem_usage;

    std::vector<MultiCastDependency*> _dependencys;
    bool _has_dependencys = false;
};
} // namespace doris::pipeline