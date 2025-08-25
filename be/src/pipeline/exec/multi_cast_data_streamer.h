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

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "pipeline/dependency.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/sink/vdata_stream_sender.h"
#include "vec/spill/spill_stream.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

class Dependency;
struct MultiCastSharedState;

struct MultiCastBlock {
    MultiCastBlock(vectorized::Block* block, int need_copy, size_t mem_size);

    std::unique_ptr<vectorized::Block> _block;
    // Each block is copied during pull. If _un_finish_copy == 0,
    // it indicates that this block has been fully used and can be released.
    int _un_finish_copy;
    size_t _mem_size;
};

struct SpillingReader {
    vectorized::SpillReaderUPtr reader;
    vectorized::SpillStreamSPtr stream;
    int64_t block_offset {0};
    bool all_data_read {false};
};

// TDOD: MultiCastDataStreamer same as the data queue, maybe rethink union and refactor the
// code
class MultiCastDataStreamer {
public:
    MultiCastDataStreamer(MultiCastSharedState* shared_state, ObjectPool* pool,
                          int cast_sender_count, int32_t node_id)
            : _shared_state(shared_state),
              _profile(pool->add(new RuntimeProfile("MultiCastDataStreamSink"))),
              _cached_blocks(cast_sender_count),
              _cast_sender_count(cast_sender_count),
              _node_id(node_id),
              _spill_readers(cast_sender_count),
              _source_operator_profiles(cast_sender_count) {
        _sender_pos_to_read.resize(cast_sender_count, _multi_cast_blocks.end());
        _dependencies.resize(cast_sender_count, nullptr);

        _spill_dependency = Dependency::create_shared(_node_id, _node_id,
                                                      "MultiCastDataStreamerDependency", true);

        for (int i = 0; i != cast_sender_count; ++i) {
            _spill_read_dependencies.emplace_back(Dependency::create_shared(
                    node_id, node_id, "MultiCastReadSpillDependency", true));
        }
        _peak_mem_usage = ADD_COUNTER(profile(), "PeakMemUsage", TUnit::BYTES);
        _process_rows = ADD_COUNTER(profile(), "ProcessRows", TUnit::UNIT);
    };

    ~MultiCastDataStreamer() = default;

    Status pull(RuntimeState* state, int sender_idx, vectorized::Block* block, bool* eos);

    Status push(RuntimeState* state, vectorized::Block* block, bool eos);

    RuntimeProfile* profile() { return _profile; }

    void set_dep_by_sender_idx(int sender_idx, Dependency* dep) {
        _dependencies[sender_idx] = dep;
        _block_reading(sender_idx);
    }

    void set_write_dependency(Dependency* dependency) { _write_dependency = dependency; }

    Dependency* get_spill_dependency() const { return _spill_dependency.get(); }

    Dependency* get_spill_read_dependency(int sender_idx) const {
        return _spill_read_dependencies[sender_idx].get();
    }

    void set_sink_profile(RuntimeProfile* profile) { _sink_operator_profile = profile; }

    void set_source_profile(int sender_idx, RuntimeProfile* profile) {
        _source_operator_profiles[sender_idx] = profile;
    }

    std::string debug_string();

private:
    void _set_ready_for_read(int sender_idx);
    void _block_reading(int sender_idx);

    Status _copy_block(RuntimeState* state, int32_t sender_idx, vectorized::Block* block,
                       MultiCastBlock& multi_cast_block);

    Status _submit_spill_task(RuntimeState* state, vectorized::SpillStreamSPtr spill_stream);

    Status _trigger_spill_if_need(RuntimeState* state, bool* triggered);
    MultiCastSharedState* _shared_state;

    RuntimeProfile* _profile = nullptr;
    std::list<MultiCastBlock> _multi_cast_blocks;
    std::vector<std::vector<vectorized::Block>> _cached_blocks;
    std::vector<std::list<MultiCastBlock>::iterator> _sender_pos_to_read;
    std::mutex _mutex;
    bool _eos = false;
    int _cast_sender_count = 0;
    int _node_id;
    std::atomic_int64_t _cumulative_mem_size = 0;
    std::atomic_int64_t _copying_count = 0;
    RuntimeProfile::Counter* _process_rows = nullptr;
    RuntimeProfile::Counter* _peak_mem_usage = nullptr;

    Dependency* _write_dependency;
    std::vector<Dependency*> _dependencies;
    std::shared_ptr<Dependency> _spill_dependency;

    vectorized::BlockUPtr _pending_block;

    std::vector<std::vector<std::shared_ptr<SpillingReader>>> _spill_readers;

    std::vector<std::shared_ptr<Dependency>> _spill_read_dependencies;

    RuntimeProfile* _sink_operator_profile;
    // operator_profile of each source operator
    std::vector<RuntimeProfile*> _source_operator_profiles;
};
#include "common/compile_check_end.h"
} // namespace doris::pipeline