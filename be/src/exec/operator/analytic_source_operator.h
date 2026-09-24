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
#include "exec/operator/analytic_spill.h"
#include "exec/operator/operator.h"

namespace doris {
class RuntimeState;
class SpillFileReader;
using SpillFileReaderSPtr = std::shared_ptr<SpillFileReader>;

class AnalyticSourceOperatorX;
class AnalyticLocalState final : public PipelineXSpillLocalState<AnalyticSharedState> {
public:
    using Base = PipelineXSpillLocalState<AnalyticSharedState>;

    ENABLE_FACTORY_CREATOR(AnalyticLocalState);
    AnalyticLocalState(RuntimeState* state, OperatorXBase* parent);
    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

private:
    friend class AnalyticSourceOperatorX;
    Status _get_spill_block(RuntimeState* state, Block* block, bool* eos);
    Status _open_spill_partition(RuntimeState* state,
                                 std::shared_ptr<AnalyticSpillPartition> partition);
    Status _read_partition_block(RuntimeState* state, Block* block, bool* partition_eos);
    Status _next_peer_group_end(RuntimeState* state);
    Status _append_spill_results(RuntimeState* state, Block* block);
    void _create_spill_result_columns(size_t rows, std::vector<MutableColumnPtr>& results,
                                      std::vector<IColumn*>& computed_result_columns) const;
    void _append_non_peer_results(size_t rows, const std::vector<IColumn*>& computed_result_columns,
                                  std::vector<MutableColumnPtr>& results) const;
    void _append_ntile_result(size_t function_index, size_t rows, IColumn* result_column) const;
    Status _append_peer_group_results(RuntimeState* state, size_t rows,
                                      const std::vector<IColumn*>& computed_result_columns);
    double _current_peer_group_result(size_t function_index) const;
    void _insert_spill_result_columns(Block* block, std::vector<MutableColumnPtr>& results) const;
    void _finish_spill_partition();

    RuntimeProfile::Counter* _get_next_timer = nullptr;
    RuntimeProfile::Counter* _filtered_rows_counter = nullptr;
    RuntimeProfile::Counter* _partition_replay_timer = nullptr;

    std::shared_ptr<AnalyticSpillPartition> _current_partition;
    SpillFileReaderSPtr _partition_reader;
    SpillFileReaderSPtr _peer_group_reader;
    size_t _in_memory_block_index = 0;
    int64_t _partition_output_position = 0;
    int64_t _peer_group_start = 0;
    int64_t _peer_group_end = 0;
    Block _peer_group_block;
    size_t _peer_group_block_position = 0;
    size_t _in_memory_peer_group_index = 0;
    bool _peer_group_file_eos = false;
};

class AnalyticSourceOperatorX final : public OperatorX<AnalyticLocalState> {
public:
    AnalyticSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                            const DescriptorTbl& descs);

#ifdef BE_TEST
    AnalyticSourceOperatorX() = default;
#endif
    Status get_block_impl(RuntimeState* state, Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    Status prepare(RuntimeState* state) override;

private:
    friend class AnalyticLocalState;
};

} // namespace doris
