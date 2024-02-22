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

#include <stdint.h>

#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/join_probe_operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "util/simd/bits.h"
#include "vec/exec/join/vnested_loop_join_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

class NestLoopJoinProbeOperatorBuilder final
        : public OperatorBuilder<vectorized::VNestedLoopJoinNode> {
public:
    NestLoopJoinProbeOperatorBuilder(int32_t id, ExecNode* node);

    OperatorPtr build_operator() override;
};

class NestLoopJoinProbeOperator final : public StatefulOperator<vectorized::VNestedLoopJoinNode> {
public:
    NestLoopJoinProbeOperator(OperatorBuilderBase* operator_builder, ExecNode* node);

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState*) override { return Status::OK(); }

    Status close(RuntimeState* state) override;
};

class NestedLoopJoinProbeDependency final : public Dependency {
public:
    using SharedState = NestedLoopJoinSharedState;
    NestedLoopJoinProbeDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "NestedLoopJoinProbeDependency", query_ctx) {}
    ~NestedLoopJoinProbeDependency() override = default;
};

class NestedLoopJoinProbeOperatorX;
class NestedLoopJoinProbeLocalState final
        : public JoinProbeLocalState<NestedLoopJoinProbeDependency, NestedLoopJoinProbeLocalState> {
public:
    using Parent = NestedLoopJoinProbeOperatorX;
    ENABLE_FACTORY_CREATOR(NestedLoopJoinProbeLocalState);
    NestedLoopJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent);
    ~NestedLoopJoinProbeLocalState() = default;

    void add_tuple_is_null_column(vectorized::Block* block) override;
#define CLEAR_BLOCK                                                  \
    for (size_t i = 0; i < column_to_keep; ++i) {                    \
        block->get_by_position(i).column->assume_mutable()->clear(); \
    }
    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;
    template <typename JoinOpType, bool set_build_side_flag, bool set_probe_side_flag>
    Status generate_join_block_data(RuntimeState* state, JoinOpType& join_op_variants);

private:
    friend class NestedLoopJoinProbeOperatorX;
    void _update_additional_flags(vectorized::Block* block);
    template <bool BuildSide, bool IsSemi>
    void _finalize_current_phase(vectorized::Block& block, size_t batch_size);
    void _resize_fill_tuple_is_null_column(size_t new_size, int left_flag, int right_flag);
    void _reset_with_next_probe_row();
    void _append_left_data_with_null(vectorized::Block& block) const;
    void _process_left_child_block(vectorized::Block& block,
                                   const vectorized::Block& now_process_build_block) const;
    template <typename Filter, bool SetBuildSideFlag, bool SetProbeSideFlag>
    void _do_filtering_and_update_visited_flags_impl(vectorized::Block* block, int column_to_keep,
                                                     int build_block_idx, int processed_blocks_num,
                                                     bool materialize, Filter& filter) {
        if constexpr (SetBuildSideFlag) {
            for (size_t i = 0; i < processed_blocks_num; i++) {
                auto& build_side_flag =
                        assert_cast<vectorized::ColumnUInt8*>(
                                _shared_state->build_side_visited_flags[build_block_idx].get())
                                ->get_data();
                auto* __restrict build_side_flag_data = build_side_flag.data();
                auto cur_sz = build_side_flag.size();
                const size_t offset = _build_offset_stack.top();
                _build_offset_stack.pop();
                for (size_t j = 0; j < cur_sz; j++) {
                    build_side_flag_data[j] |= filter[offset + j];
                }
                build_block_idx = build_block_idx == 0 ? _shared_state->build_blocks.size() - 1
                                                       : build_block_idx - 1;
            }
        }
        if constexpr (SetProbeSideFlag) {
            int end = filter.size();
            for (int i = _left_block_pos == _child_block->rows() ? _left_block_pos - 1
                                                                 : _left_block_pos;
                 i >= _left_block_start_pos; i--) {
                int offset = 0;
                if (!_probe_offset_stack.empty()) {
                    offset = _probe_offset_stack.top();
                    _probe_offset_stack.pop();
                }
                if (!_cur_probe_row_visited_flags[i]) {
                    _cur_probe_row_visited_flags[i] =
                            simd::contain_byte<uint8>(filter.data() + offset, end - offset, 1) ? 1
                                                                                               : 0;
                }
                end = offset;
            }
        }
        if (materialize) {
            vectorized::Block::filter_block_internal(block, filter, column_to_keep);
        } else {
            CLEAR_BLOCK
        }
    }

    // need exception safety
    template <bool SetBuildSideFlag, bool SetProbeSideFlag, bool IgnoreNull>
    Status _do_filtering_and_update_visited_flags(vectorized::Block* block, bool materialize) {
        auto column_to_keep = block->columns();
        // If we need to set visited flags for build side,
        // 1. Execute conjuncts and get a column with bool type to do filtering.
        // 2. Use bool column to update build-side visited flags.
        // 3. Use bool column to do filtering.
        size_t build_block_idx = _current_build_pos == 0 ? _shared_state->build_blocks.size() - 1
                                                         : _current_build_pos - 1;
        size_t processed_blocks_num = _build_offset_stack.size();
        if (LIKELY(!_join_conjuncts.empty() && block->rows() > 0)) {
            vectorized::IColumn::Filter filter(block->rows(), 1);
            bool can_filter_all = false;
            RETURN_IF_ERROR(vectorized::VExprContext::execute_conjuncts(
                    _join_conjuncts, nullptr, IgnoreNull, block, &filter, &can_filter_all));

            if (can_filter_all) {
                CLEAR_BLOCK
                std::stack<uint16_t> empty1;
                _probe_offset_stack.swap(empty1);

                std::stack<uint16_t> empty2;
                _build_offset_stack.swap(empty2);
            } else {
                _do_filtering_and_update_visited_flags_impl<decltype(filter), SetBuildSideFlag,
                                                            SetProbeSideFlag>(
                        block, column_to_keep, build_block_idx, processed_blocks_num, materialize,
                        filter);
            }
        } else if (block->rows() > 0) {
            if constexpr (SetBuildSideFlag) {
                for (size_t i = 0; i < processed_blocks_num; i++) {
                    auto& build_side_flag =
                            assert_cast<vectorized::ColumnUInt8*>(
                                    _shared_state->build_side_visited_flags[build_block_idx].get())
                                    ->get_data();
                    auto* __restrict build_side_flag_data = build_side_flag.data();
                    auto cur_sz = build_side_flag.size();
                    _build_offset_stack.pop();
                    memset(reinterpret_cast<void*>(build_side_flag_data), 1, cur_sz);
                    build_block_idx = build_block_idx == 0 ? _shared_state->build_blocks.size() - 1
                                                           : build_block_idx - 1;
                }
            }
            if constexpr (SetProbeSideFlag) {
                std::stack<uint16_t> empty;
                _probe_offset_stack.swap(empty);
                std::fill(_cur_probe_row_visited_flags.begin(), _cur_probe_row_visited_flags.end(),
                          1);
            }
            if (!materialize) {
                CLEAR_BLOCK
            }
        }
        vectorized::Block::erase_useless_column(block, column_to_keep);
        return Status::OK();
    }

    bool _matched_rows_done;
    int _left_block_start_pos = 0;
    int _left_block_pos; // current scan pos in _left_block
    int _left_side_process_count = 0;
    bool _need_more_input_data = true;
    // Visited flags for current row in probe side.
    std::vector<vectorized::Int8> _cur_probe_row_visited_flags;
    size_t _current_build_pos = 0;
    vectorized::MutableColumns _dst_columns;
    std::stack<uint16_t> _build_offset_stack;
    std::stack<uint16_t> _probe_offset_stack;
    uint64_t _output_null_idx_build_side = 0;
    vectorized::VExprContextSPtrs _join_conjuncts;

    RuntimeProfile::Counter* _loop_join_timer = nullptr;
};

class NestedLoopJoinProbeOperatorX final
        : public JoinProbeOperatorX<NestedLoopJoinProbeLocalState> {
public:
    NestedLoopJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                 const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status push(RuntimeState* state, vectorized::Block* input_block,
                SourceState source_state) const override;
    Status pull(doris::RuntimeState* state, vectorized::Block* output_block,
                SourceState& source_state) const override;
    const RowDescriptor& intermediate_row_desc() const override {
        return _old_version_flag ? _row_descriptor : *_intermediate_row_desc;
    }

    DataDistribution required_data_distribution() const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        }
        return {ExchangeType::ADAPTIVE_PASSTHROUGH};
    }

    const RowDescriptor& row_desc() const override {
        return _old_version_flag
                       ? (_output_row_descriptor ? *_output_row_descriptor : _row_descriptor)
                       : *_output_row_desc;
    }

    bool need_more_input_data(RuntimeState* state) const override;

private:
    friend class NestedLoopJoinProbeLocalState;
    bool _is_output_left_side_only;
    vectorized::VExprContextSPtrs _join_conjuncts;
    size_t _num_probe_side_columns = 0;
    size_t _num_build_side_columns = 0;
    const bool _old_version_flag;
};

} // namespace pipeline
} // namespace doris
