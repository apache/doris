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

#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <iosfwd>
#include <memory>
#include <stack>
#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/exec/join/vjoin_node_base.h"

namespace doris {
class DescriptorTbl;
class ObjectPool;
class RowDescriptor;

namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <typename Parent>
struct RuntimeFilterBuild {
    RuntimeFilterBuild(Parent* parent) : _parent(parent) {}
    Status operator()(RuntimeState* state);

private:
    Parent* _parent = nullptr;
};

// Node for nested loop joins.
class VNestedLoopJoinNode final : public VJoinNodeBase {
public:
    VNestedLoopJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    Status prepare(RuntimeState* state) override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    Status alloc_resource(doris::RuntimeState* state) override;

    void release_resource(doris::RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;

    bool need_more_input_data() const;

    Status close(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    void debug_string(int indentation_level, std::stringstream* out) const override;

    const RowDescriptor& intermediate_row_desc() const override {
        return _old_version_flag ? _row_descriptor : *_intermediate_row_desc;
    }

    const RowDescriptor& row_desc() const override {
        return _old_version_flag
                       ? (_output_row_descriptor ? *_output_row_descriptor : _row_descriptor)
                       : *_output_row_desc;
    }

    std::shared_ptr<Block> get_left_block() { return _left_block; }

    std::vector<TRuntimeFilterDesc>& runtime_filter_descs() { return _runtime_filter_descs; }
    VExprContextSPtrs& filter_src_expr_ctxs() { return _filter_src_expr_ctxs; }
    RuntimeProfile::Counter* runtime_filter_compute_timer() {
        return _runtime_filter_compute_timer;
    }
    Blocks& build_blocks() { return _build_blocks; }
    RuntimeProfile::Counter* publish_runtime_filter_timer() {
        return _publish_runtime_filter_timer;
    }

private:
    template <typename JoinOpType, bool set_build_side_flag, bool set_probe_side_flag>
    Status _generate_join_block_data(RuntimeState* state, JoinOpType& join_op_variants) {
        constexpr bool ignore_null = JoinOpType::value == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;
        _left_block_start_pos = _left_block_pos;
        _left_side_process_count = 0;
        DCHECK(!_need_more_input_data || !_matched_rows_done);

        if (!_matched_rows_done && !_need_more_input_data) {
            // We should try to join rows if there still are some rows from probe side.
            while (_join_block.rows() < state->batch_size()) {
                while (_current_build_pos == _build_blocks.size() ||
                       _left_block_pos == _left_block->rows()) {
                    // if left block is empty(), do not need disprocess the left block rows
                    if (_left_block->rows() > _left_block_pos) {
                        _left_side_process_count++;
                    }

                    _reset_with_next_probe_row();
                    if (_left_block_pos < _left_block->rows()) {
                        if constexpr (set_probe_side_flag) {
                            _probe_offset_stack.push(_join_block.rows());
                        }
                    } else {
                        if (_left_side_eos) {
                            _matched_rows_done = true;
                        } else {
                            _need_more_input_data = true;
                        }
                        break;
                    }
                }

                // Do not have left row need to be disposed
                if (_matched_rows_done || _need_more_input_data) {
                    break;
                }

                const auto& now_process_build_block = _build_blocks[_current_build_pos++];
                if constexpr (set_build_side_flag) {
                    _build_offset_stack.push(_join_block.rows());
                }
                _process_left_child_block(_join_block, now_process_build_block);
            }

            if constexpr (set_probe_side_flag) {
                Status status;
                RETURN_IF_CATCH_EXCEPTION(
                        (status = _do_filtering_and_update_visited_flags<
                                 set_build_side_flag, set_probe_side_flag, ignore_null>(
                                 &_join_block, !_is_left_semi_anti)));
                _update_additional_flags(&_join_block);
                if (!status.ok()) {
                    return status;
                }
                // If this join operation is left outer join or full outer join, when
                // `_left_side_process_count`, means all rows from build
                // side have been joined with _left_side_process_count, we should output current
                // probe row with null from build side.
                if (_left_side_process_count) {
                    _finalize_current_phase<false, JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN>(
                            _join_block, state->batch_size());
                }
            }

            if (_left_side_process_count) {
                if (_is_mark_join && _build_blocks.empty()) {
                    DCHECK_EQ(JoinOpType::value, TJoinOp::CROSS_JOIN);
                    _append_left_data_with_null(_join_block);
                }
            }
        }

        if constexpr (!set_probe_side_flag) {
            Status status;
            RETURN_IF_CATCH_EXCEPTION(
                    (status = _do_filtering_and_update_visited_flags<
                             set_build_side_flag, set_probe_side_flag, ignore_null>(
                             &_join_block, !_is_right_semi_anti)));
            _update_additional_flags(&_join_block);
            if (!status.ok()) {
                return status;
            }
        }

        if constexpr (set_build_side_flag) {
            if (_matched_rows_done && _output_null_idx_build_side < _build_blocks.size()) {
                _finalize_current_phase<true, JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN>(
                        _join_block, state->batch_size());
            }
        }
        return Status::OK();
    }

    Status _materialize_build_side(RuntimeState* state) override;
    // Processes a block from the left child.
    //  dst_columns: left_child_row and now_process_build_block to construct a bundle column of new block
    //  now_process_build_block: right child block now to process
    void _process_left_child_block(Block& block, const Block& now_process_build_block) const;

    template <bool SetBuildSideFlag, bool SetProbeSideFlag, bool IgnoreNull>
    Status _do_filtering_and_update_visited_flags(Block* block, bool materialize);

    // TODO: replace it as template lambda after support C++20
    template <typename Filter, bool SetBuildSideFlag, bool SetProbeSideFlag>
    void _do_filtering_and_update_visited_flags_impl(Block* block, int column_to_keep,
                                                     int build_block_idx, int processed_blocks_num,
                                                     bool materialize, Filter& filter);

    template <bool BuildSide, bool IsSemi>
    void _finalize_current_phase(Block& block, size_t batch_size);

    void _reset_with_next_probe_row();

    void _release_mem();

    Status _fresh_left_block(RuntimeState* state);

    void _resize_fill_tuple_is_null_column(size_t new_size, int left_flag, int right_flag);

    // add tuple is null flag column to Block for filter conjunct and output expr
    void _update_additional_flags(Block* block);

    void _add_tuple_is_null_column(Block* block) override;

    // For mark join, if the relation from right side is empty, we should construct intermediate
    // block with data from left side and filled with null for right side
    void _append_left_data_with_null(Block& block) const;

    // List of build blocks, constructed in prepare()
    Blocks _build_blocks;
    // Visited flags for each row in build side.
    MutableColumns _build_side_visited_flags;
    // Visited flags for current row in probe side.
    std::vector<int8_t> _cur_probe_row_visited_flags;
    size_t _current_build_pos = 0;

    size_t _num_probe_side_columns = 0;
    size_t _num_build_side_columns = 0;

    uint64_t _build_rows = 0;
    uint64_t _total_mem_usage = 0;
    uint64_t _output_null_idx_build_side = 0;

    bool _matched_rows_done;

    // _left_block must be cleared before calling get_next().  The child node
    // does not initialize all tuple ptrs in the row, only the ones that it
    // is responsible for.
    std::shared_ptr<Block> _left_block;

    int _left_block_start_pos = 0;
    int _left_block_pos; // current scan pos in _left_block
    bool _left_side_eos; // if true, left child has no more rows to process
    int _left_side_process_count = 0;

    bool _old_version_flag;

    MutableColumns _dst_columns;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    VExprContextSPtrs _filter_src_expr_ctxs;
    bool _is_output_left_side_only = false;
    bool _need_more_input_data = true;
    std::stack<uint16_t> _build_offset_stack;
    std::stack<uint16_t> _probe_offset_stack;
    VExprContextSPtrs _join_conjuncts;
    RuntimeProfile::Counter* _loop_join_timer = nullptr;
    template <typename Parent>
    friend struct RuntimeFilterBuild;
};

} // namespace doris::vectorized
