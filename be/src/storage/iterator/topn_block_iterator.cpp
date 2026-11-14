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

#include "storage/iterator/topn_block_iterator.h"

#include <glog/logging.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <utility>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "storage/rowset/rowset_reader_context.h"

namespace doris {
using namespace ErrorCode;

bool TopNBlockIterator::RowPosComparator::operator()(const size_t& lpos, const size_t& rpos) const {
    int ret = _candidates->compare_at(lpos, rpos, _sort_columns, *_candidates, -1);
    return _is_reverse ? ret > 0 : ret < 0;
}

TopNBlockIterator::Candidates::Candidates(Block empty_block,
                                          const std::vector<uint32_t>* sort_columns,
                                          bool is_reverse, size_t limit)
        : _sort_columns(sort_columns),
          _is_reverse(is_reverse),
          _limit(limit),
          _rows(MutableBlock::build_mutable_block(std::move(empty_block))),
          _sorted_row_pos(RowPosComparator(&_rows, sort_columns, is_reverse)) {
    for (auto column_idx : *_sort_columns) {
        DORIS_CHECK(column_idx < _rows.columns());
    }
}

size_t TopNBlockIterator::Candidates::_rows_to_copy(const Block& block) const {
    if (_sorted_row_pos.empty()) {
        return std::min(block.rows(), _limit);
    }
    // is_reverse == true  last_row_pos is the pos of smallest row
    // is_reverse == false last_row_pos is biggest row
    const size_t last_row_pos = *_sorted_row_pos.rbegin();
    size_t rows_to_copy = 0;
    for (size_t j = 0; j < block.rows(); j++) {
        // if there is not enough rows in sorted_row_pos, just copy new rows
        if (_sorted_row_pos.size() + rows_to_copy < _limit) {
            rows_to_copy++;
            continue;
        }
        int res = 0;
        for (auto k : *_sort_columns) {
            DORIS_CHECK(k < block.columns());
            DCHECK(block.get_by_position(k).type->equals(*_rows.get_datatype_by_position(k)));
            res = block.get_by_position(k).column->compare_at(j, last_row_pos,
                                                              *_rows.get_column_by_position(k), -1);
            if (res) {
                break;
            }
        }
        // only copy needed rows
        // is_reverse == true  > smallest is ok
        // is_reverse == false < biggest is ok
        if ((_is_reverse && res > 0) || (!_is_reverse && res < 0)) {
            rows_to_copy++;
        } else {
            break;
        }
    }
    return rows_to_copy;
}

Status TopNBlockIterator::Candidates::offer(const Block& block, size_t* kept) {
    const size_t rows_to_copy = _rows_to_copy(block);
    *kept = rows_to_copy;
    if (rows_to_copy == 0) {
        return Status::OK();
    }
    const size_t base = _rows.rows();
    RETURN_IF_ERROR(_rows.add_rows(&block, 0, rows_to_copy));
    for (size_t j = 0; j < rows_to_copy; j++) {
        _sorted_row_pos.insert(base + j);
    }
    _trim();
    return Status::OK();
}

void TopNBlockIterator::Candidates::_trim() {
    if (_sorted_row_pos.size() <= _limit) {
        return;
    }
    _sorted_row_pos.erase(std::next(_sorted_row_pos.begin(), cast_set<int64_t>(_limit)),
                          _sorted_row_pos.end());
    if (_rows.rows() <= _limit * 2) {
        return;
    }
    VLOG_DEBUG << "topn debug start  shrink mutable_block from " << _rows.rows() << " rows";
    Block tmp_block = _rows.to_block();
    _rows = MutableBlock::build_mutable_block(tmp_block.clone_empty());
    for (size_t pos : _sorted_row_pos) {
        _rows.add_row(&tmp_block, cast_set<int>(pos));
    }
    // The kept rows were appended in sort order: the positions 0..limit are sorted.
    _sorted_row_pos.clear();
    for (size_t j = 0; j < _limit; j++) {
        _sorted_row_pos.insert(j);
    }
    VLOG_DEBUG << "topn debug finish shrink mutable_block to " << _rows.rows() << " rows";
}

void TopNBlockIterator::Candidates::worst_value(size_t ordinal, Field* value) const {
    DCHECK(full());
    _rows.get_column_by_position(ordinal)->get(*_sorted_row_pos.rbegin(), *value);
}

TopNBlockIterator::TopNBlockIterator(std::vector<RowwiseIteratorUPtr>* rowset_iters,
                                     const StorageReadOptions* init_options,
                                     const RowsetReaderContext* context)
        : _rowset_iters(rowset_iters), _init_options(init_options), _context(context) {}

Status TopNBlockIterator::next_block(Block* block, bool* eof) {
    if (_done) {
        *eof = true;
        return Status::OK();
    }
    _done = true;
    RETURN_IF_ERROR(_collect(block));
    *eof = block->rows() == 0;
    return Status::OK();
}

Status TopNBlockIterator::_collect(Block* block) {
    DORIS_CHECK(block != nullptr);
    const std::vector<uint32_t>* sort_columns = &_context->merge.compare_ordinals;
    DORIS_CHECK(!sort_columns->empty());
    const size_t limit = _context->read_orderby_key_limit;
    DORIS_CHECK_GT(limit, 0);

    // `block` is reused below as the per-rowset read buffer. Keep TopN candidates in a
    // separate block with the same schema so stored row positions remain stable.
    Block clone_block = block->clone_empty();
    // Initialize virtual slot columns by schema (avoid runtime type checks).
    // virtual_column_exprs is keyed by read-schema ordinal == block position.
    for (const auto& [ordinal, expr_ctx] : _context->virtual_column_exprs) {
        DORIS_CHECK(ordinal < clone_block.columns());
        clone_block.get_by_position(ordinal).column =
                expr_ctx->root()->data_type()->create_column();
    }
    Candidates candidates(std::move(clone_block), sort_columns, _context->merge.reverse, limit);

    for (size_t i = 0; i < _rowset_iters->size(); i++) {
        RETURN_IF_ERROR(_collect_rowset(*(*_rowset_iters)[i], block, &candidates));
        VLOG_DEBUG << "topn debug rowset " << i << " limit=" << limit
                   << " candidates full=" << candidates.full();
    }
    *block = candidates.take();
    VLOG_DEBUG << "topn debug result limit=" << limit << " rows=" << block->rows();
    return Status::OK();
}

Status TopNBlockIterator::_collect_rowset(RowwiseIterator& rowset_iter, Block* block,
                                          Candidates* candidates) {
    RETURN_IF_ERROR(rowset_iter.init(*_init_options));
    const size_t limit = _context->read_orderby_key_limit;
    // read at most `limit` rows from this rowset
    size_t read_rows = 0;
    while (read_rows < limit) {
        block->clear_column_data();
        auto status = rowset_iter.next_batch(block);
        if (status.is<END_OF_FILE>()) {
            break;
        }
        RETURN_IF_ERROR(status);
        read_rows += block->rows();

        size_t kept = 0;
        RETURN_IF_ERROR(candidates->offer(*block, &kept));
        if (kept > 0 && candidates->full() && !_context->topn_filter_source_node_ids.empty()) {
            RETURN_IF_ERROR(_update_runtime_predicate(*candidates));
        }
    }
    return Status::OK();
}

// The n-th best row so far bounds the rows worth reading elsewhere: publish its leading sort
// column value to the TopN runtime predicate of every source node.
Status TopNBlockIterator::_update_runtime_predicate(const Candidates& candidates) {
    Field new_top;
    candidates.worst_value(_context->merge.compare_ordinals[0], &new_top);
    auto* query_ctx = _context->runtime_state->get_query_ctx();
    for (int id : _context->topn_filter_source_node_ids) {
        RETURN_IF_ERROR(query_ctx->get_runtime_predicate(id).update(new_top));
    }
    return Status::OK();
}

} // namespace doris
