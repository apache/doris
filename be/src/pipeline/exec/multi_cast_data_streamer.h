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
#include "common/compile_check_begin.h"

class Dependency;
struct MultiCastBlock {
    MultiCastBlock(vectorized::Block* block, int need_copy, size_t mem_size);

    std::unique_ptr<vectorized::Block> _block;
    // Each block is copied during pull. If _un_finish_copy == 0,
    // it indicates that this block has been fully used and can be released.
    int _un_finish_copy;
    size_t _mem_size;
};

// TDOD: MultiCastDataStreamer same as the data queue, maybe rethink union and refactor the
// code
class MultiCastDataStreamer {
public:
    MultiCastDataStreamer(ObjectPool* pool, int cast_sender_count)
            : _profile(pool->add(new RuntimeProfile("MultiCastDataStreamSink"))),
              _cast_sender_count(cast_sender_count) {
        _sender_pos_to_read.resize(cast_sender_count, _multi_cast_blocks.end());
        _dependencies.resize(cast_sender_count, nullptr);
        _peak_mem_usage = ADD_COUNTER(profile(), "PeakMemUsage", TUnit::BYTES);
        _process_rows = ADD_COUNTER(profile(), "ProcessRows", TUnit::UNIT);
    };

    ~MultiCastDataStreamer() = default;

    Status pull(int sender_idx, vectorized::Block* block, bool* eos);

    Status push(RuntimeState* state, vectorized::Block* block, bool eos);

    RuntimeProfile* profile() { return _profile; }

    void set_dep_by_sender_idx(int sender_idx, Dependency* dep) {
        _dependencies[sender_idx] = dep;
        _block_reading(sender_idx);
    }

private:
    void _set_ready_for_read(int sender_idx);
    void _block_reading(int sender_idx);

    void _copy_block(vectorized::Block* block, int& un_finish_copy);
    RuntimeProfile* _profile = nullptr;
    const int _cast_sender_count = 0;

    Mutex _mutex;
    std::list<MultiCastBlock> _multi_cast_blocks GUARDED_BY(_mutex);
    std::vector<std::list<MultiCastBlock>::iterator> _sender_pos_to_read GUARDED_BY(_mutex);
    bool _eos GUARDED_BY(_mutex) = false;

    // use to record the profile
    int64_t _cumulative_mem_size = 0;
    RuntimeProfile::Counter* _process_rows = nullptr;
    RuntimeProfile::Counter* _peak_mem_usage = nullptr;

    std::vector<Dependency*> _dependencies;
};
#include "common/compile_check_end.h"
} // namespace doris::pipeline