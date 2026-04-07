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

#include "exec/operator/sorted_streaming_distinct_operator.h"

#include <memory>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"

namespace doris {
#include "common/compile_check_begin.h"

SortedStreamingDistinctLocalState::SortedStreamingDistinctLocalState(RuntimeState* state,
                                                                     OperatorXBase* parent)
        : PipelineXLocalState<FakeSharedState>(state, parent),
          _child_block(Block::create_unique()),
          _output_block(Block::create_unique()) {}

Status SortedStreamingDistinctLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_init_timer);
    _comparison_timer = ADD_TIMER(Base::custom_profile(), "ComparisonTime");
    _filter_timer = ADD_TIMER(Base::custom_profile(), "FilterTime");
    _input_rows_counter = ADD_COUNTER(Base::custom_profile(), "InputRows", TUnit::UNIT);
    return Status::OK();
}

Status SortedStreamingDistinctLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = Base::_parent->template cast<SortedStreamingDistinctOperatorX>();
    _group_by_expr_ctxs.resize(p._group_by_expr_ctxs.size());
    for (size_t i = 0; i < _group_by_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._group_by_expr_ctxs[i]->clone(state, _group_by_expr_ctxs[i]));
    }
    return Status::OK();
}

void SortedStreamingDistinctLocalState::_make_nullable_output_key(Block* block) {
    if (block->rows() != 0) {
        for (auto cid :
             Base::_parent->cast<SortedStreamingDistinctOperatorX>()._make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

Status SortedStreamingDistinctLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    _output_block->clear();
    _prev_key_columns.clear();
    return Base::close(state);
}

Status SortedStreamingDistinctLocalState::_evaluate_key_columns(
        const Block* block, std::vector<ColumnPtr>& key_columns) {
    const size_t key_size = _group_by_expr_ctxs.size();
    key_columns.resize(key_size);
    for (size_t i = 0; i < key_size; ++i) {
        RETURN_IF_ERROR(_group_by_expr_ctxs[i]->execute(block, key_columns[i]));
        key_columns[i] = key_columns[i]->convert_to_full_column_if_const();
    }
    return Status::OK();
}

namespace {
template <typename Predicate>
size_t get_range_end(size_t begin, size_t end, Predicate pred) {
    DCHECK(begin < end);

    constexpr size_t linear_probe_threshold = 16;
    size_t linear_probe_end = std::min(begin + linear_probe_threshold, end);

    for (size_t pos = begin; pos < linear_probe_end; ++pos) {
        if (!pred(begin, pos)) {
            return pos;
        }
    }

    // Switch to binary search for long runs
    size_t low = linear_probe_end;
    size_t high = end - 1;
    while (low <= high) {
        size_t mid = low + (high - low) / 2;
        if (pred(begin, mid)) {
            low = mid + 1;
        } else {
            high = mid - 1;
            end = mid;
        }
    }
    return end;
}
} // namespace

size_t SortedStreamingDistinctLocalState::_build_distinct_filter(
        const std::vector<ColumnPtr>& key_columns, size_t num_rows, IColumn::Filter& filter) {
    // Handle continuation from previous chunk
    size_t range_begin = _continue_with_prev_range(key_columns, num_rows, filter);
    size_t output_rows = 0;

    // Range-based loop: find range of equal keys, keep first row, skip rest
    size_t range_end = range_begin;
    while (range_end != num_rows) {
        range_end = get_range_end(range_begin, num_rows, [&](size_t key_pos, size_t row_pos) {
            return _is_key(key_columns, key_pos, row_pos);
        });

        // Keep only the first row in this range
        filter[range_begin] = 1;
        if (range_end > range_begin + 1) {
            std::fill(filter.data() + range_begin + 1, filter.data() + range_end,
                      static_cast<uint8_t>(0));
        }
        ++output_rows;

        range_begin = range_end;
    }

    // Save the last row's key for cross-chunk comparison
    _prev_key_columns.clear();
    for (size_t col = 0; col < key_columns.size(); ++col) {
        _prev_key_columns.push_back(key_columns[col]->cut(num_rows - 1, 1));
    }
    _has_prev_key = true;

    return output_rows;
}

bool SortedStreamingDistinctLocalState::_is_key(const std::vector<ColumnPtr>& key_columns,
                                                size_t key_pos, size_t row_pos) const {
    for (size_t i = 0; i < key_columns.size(); ++i) {
        if (key_columns[i]->compare_at(key_pos, row_pos, *key_columns[i], -1) != 0) {
            return false;
        }
    }
    return true;
}

bool SortedStreamingDistinctLocalState::_is_prev_chunk_key(
        const std::vector<ColumnPtr>& key_columns, size_t row_pos) const {
    if (!_has_prev_key) {
        return false;
    }
    DCHECK(!_prev_key_columns.empty());
    DCHECK(row_pos < key_columns[0]->size());
    for (size_t i = 0; i < _prev_key_columns.size(); ++i) {
        // compare_at(0, row_pos): 0 is the single saved row in _prev_key_columns[i]
        if (_prev_key_columns[i]->compare_at(0, row_pos, *key_columns[i], -1) != 0) {
            return false;
        }
    }
    return true;
}

size_t SortedStreamingDistinctLocalState::_continue_with_prev_range(
        const std::vector<ColumnPtr>& key_columns, size_t num_rows, IColumn::Filter& filter) {
    if (!_is_prev_chunk_key(key_columns, 0)) {
        return 0;
    }

    // Find how far the previous key extends into this chunk
    size_t range_end = get_range_end(0, num_rows, [&](size_t, size_t row_pos) {
        return _is_prev_chunk_key(key_columns, row_pos);
    });

    // All rows in [0, range_end) are duplicates of the previous chunk's last key — skip them
    std::fill(filter.data(), filter.data() + range_end, static_cast<uint8_t>(0));
    return range_end;
}

void SortedStreamingDistinctLocalState::_build_output_block(
        const std::vector<ColumnPtr>& key_columns, const IColumn::Filter& filter,
        size_t output_rows) {
    Block output_block;
    for (size_t i = 0; i < key_columns.size(); ++i) {
        auto filtered_column = key_columns[i]->filter(filter, output_rows);
        output_block.insert({std::move(filtered_column),
                             _group_by_expr_ctxs[i]->root()->data_type(),
                             _group_by_expr_ctxs[i]->root()->expr_name()});
    }
    _output_block->swap(output_block);
}

// ==================== OperatorX ====================

SortedStreamingDistinctOperatorX::SortedStreamingDistinctOperatorX(ObjectPool* pool,
                                                                   int operator_id,
                                                                   const TPlanNode& tnode,
                                                                   const DescriptorTbl& descs)
        : StatefulOperatorX<SortedStreamingDistinctLocalState>(pool, tnode, operator_id, descs),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_colocate(tnode.agg_node.__isset.is_colocate && tnode.agg_node.is_colocate) {
    if (tnode.agg_node.__isset.use_streaming_preaggregation) {
        _is_streaming_preagg = tnode.agg_node.use_streaming_preaggregation;
    }
}

Status SortedStreamingDistinctOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<SortedStreamingDistinctLocalState>::init(tnode, state));
    RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _group_by_expr_ctxs));
    _op_name = "SORTED_STREAMING_DISTINCT_OPERATOR";
    return Status::OK();
}

Status SortedStreamingDistinctOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<SortedStreamingDistinctLocalState>::prepare(state));
    RETURN_IF_ERROR(VExpr::prepare(_group_by_expr_ctxs, state, _child->row_desc()));
    RETURN_IF_ERROR(VExpr::open(_group_by_expr_ctxs, state));
    init_make_nullable(state);
    return Status::OK();
}

void SortedStreamingDistinctOperatorX::init_make_nullable(RuntimeState* state) {
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    for (size_t i = 0; i < _group_by_expr_ctxs.size(); ++i) {
        auto nullable_output = _output_tuple_desc->slots()[i]->is_nullable();
        auto nullable_input = _group_by_expr_ctxs[i]->root()->is_nullable();
        if (nullable_output != nullable_input) {
            DCHECK(nullable_output);
            _make_nullable_keys.emplace_back(i);
        }
    }
}

Status SortedStreamingDistinctOperatorX::push(RuntimeState* state, Block* in_block,
                                              bool eos) const {
    auto& local_state = get_local_state(state);
    if (in_block->rows() == 0) {
        return Status::OK();
    }

    const size_t num_rows = in_block->rows();
    COUNTER_UPDATE(local_state._input_rows_counter, num_rows);

    // Evaluate key expressions
    std::vector<ColumnPtr> key_columns;
    RETURN_IF_ERROR(local_state._evaluate_key_columns(in_block, key_columns));

    // Build filter: compare adjacent rows
    IColumn::Filter filter(num_rows, 0);
    size_t output_rows = 0;

    {
        SCOPED_TIMER(local_state._comparison_timer);
        output_rows = local_state._build_distinct_filter(key_columns, num_rows, filter);
    }

    // All rows are duplicates
    if (output_rows == 0) {
        return Status::OK();
    }

    {
        SCOPED_TIMER(local_state._filter_timer);
        local_state._build_output_block(key_columns, filter, output_rows);
    }

    // Check limit
    if (_limit != -1 &&
        (local_state._num_rows_returned + local_state._output_block->rows()) >= _limit) {
        auto limit_rows = _limit - local_state._num_rows_returned;
        local_state._output_block->set_num_rows(limit_rows);
        local_state._reach_limit = true;
    }

    return Status::OK();
}

Status SortedStreamingDistinctOperatorX::pull(RuntimeState* state, Block* block, bool* eos) const {
    auto& local_state = get_local_state(state);
    if (!local_state._output_block->empty()) {
        block->swap(*local_state._output_block);
        local_state._output_block->clear_column_data(block->columns());
    }

    local_state._make_nullable_output_key(block);
    if (!_is_streaming_preagg) {
        RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block));
    }
    local_state.add_num_rows_returned(block->rows());
    *eos = (local_state._child_eos && local_state._output_block->empty()) ||
           local_state._reach_limit;
    return Status::OK();
}

bool SortedStreamingDistinctOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._output_block->empty() &&
           !(local_state._child_eos || local_state._reach_limit);
}

} // namespace doris
