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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {

class RuntimeProfile;
class RuntimeState;
class SpillFile;
class SpillFileWriter;

using SpillFileSPtr = std::shared_ptr<SpillFile>;
using SpillFileWriterSPtr = std::shared_ptr<SpillFileWriter>;

/// An immutable run of consecutive analytic partitions handed from the sink to the source. Rows
/// keep the input order. partition_ends[p] is the exclusive end row of partition p inside the batch
/// and peer_group_ends are batch-relative. Small partitions are batched together so that the source
/// replays ordinary sized Blocks even when every partition contains only a few rows.
struct AnalyticSpillBatch {
    int64_t rows = 0;
    std::vector<Block> blocks;
    SpillFileSPtr data_file;

    std::vector<int64_t> peer_group_ends;
    SpillFileSPtr peer_group_file;

    std::vector<int64_t> partition_ends;
    std::vector<WindowSpillStrategy> strategies;
    std::vector<WindowSpillPeerFunction> peer_functions;
    // Row p holds the result of partition p for PARTITION_REDUCE functions.
    std::vector<ColumnPtr> partition_results;
    // Element p holds the parameter of partition p for PARTITION_CARDINALITY functions.
    std::vector<std::vector<int64_t>> function_parameters;
    std::vector<DataTypePtr> result_types;
    std::vector<bool> change_to_nullable_flags;
};

/// Owns the input rows of one analytic batch: zero or more finished partitions followed by the
/// partition that is still receiving rows. Before the first revoke it retains ordinary Blocks.
/// Once spilled, the writer stays open until seal(); later small Blocks are coalesced up to the
/// spill buffer size before being written, and large Blocks are written directly.
class AnalyticSpillBatchStore {
public:
    AnalyticSpillBatchStore(RuntimeProfile* profile, int node_id, bool has_peer_groups);
    ~AnalyticSpillBatchStore();

    AnalyticSpillBatchStore(const AnalyticSpillBatchStore&) = delete;
    AnalyticSpillBatchStore& operator=(const AnalyticSpillBatchStore&) = delete;

    Status append_block(RuntimeState* state, Block block);
    Status append_peer_group_end(RuntimeState* state, int64_t group_end);
    Status spill(RuntimeState* state);
    Status seal(RuntimeState* state, AnalyticSpillBatch* batch);

    [[nodiscard]] int64_t rows() const { return _rows; }
    [[nodiscard]] size_t revocable_mem_size() const;
    [[nodiscard]] bool is_spilled() const { return _data_file != nullptr; }
    [[nodiscard]] bool has_spilled_peer_groups() const { return _peer_group_file != nullptr; }
    [[nodiscard]] int64_t peer_group_metadata_bytes() const {
        return _peer_group_count * sizeof(int64_t);
    }

private:
    Status _create_writer(RuntimeState* state, const char* label, SpillFileSPtr& file,
                          SpillFileWriterSPtr& writer);
    Status _write_data_block(RuntimeState* state, Block block);
    Status _flush_write_buffer(RuntimeState* state);
    Status _ensure_peer_group_writer(RuntimeState* state);
    Status _flush_peer_group_ends(RuntimeState* state);
    void _release_blocks();
    void _release_peer_group_buffer();

    RuntimeProfile* _profile = nullptr;
    int _node_id = -1;
    bool _has_peer_groups = false;
    int64_t _rows = 0;
    int64_t _peer_group_count = 0;
    size_t _blocks_memory_bytes = 0;

    std::vector<Block> _blocks;
    SpillFileSPtr _data_file;
    SpillFileWriterSPtr _data_writer;
    // Small Blocks appended after the first spill, written once they reach the spill buffer size.
    MutableBlock _write_buffer;

    std::vector<int64_t> _peer_group_ends;
    SpillFileSPtr _peer_group_file;
    SpillFileWriterSPtr _peer_group_writer;
};

} // namespace doris
