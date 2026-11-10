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

#include "storage/iterator/topn_iterator.h"

#include <glog/logging.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <set>
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

TopNIterator::TopNIterator(std::vector<RowSetSplits> rs_splits, RowsetReaderContext* context,
                           ReadSchemaSPtr read_schema)
        : _rs_splits(std::move(rs_splits)),
          _context(context),
          _read_schema(std::move(read_schema)),
          _topn_limit(context->read_orderby_key_limit),
          _is_reverse(context->read_orderby_key_reverse) {
    DORIS_CHECK_GT(_topn_limit, 0);
}

// Moved unchanged from VCollectIterator::_topn_next; splitting it up is left to its own change.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status TopNIterator::next_batch(Block* block) {
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(_context->read_orderby_key_columns != nullptr);
    DORIS_CHECK(!_context->read_orderby_key_columns->empty());

    if (_topn_eof) {
        return Status::Error<END_OF_FILE>("");
    }

    // `block` is reused below as the per-rowset read buffer. Keep TopN candidates in a
    // separate mutable block with the same schema so stored row positions remain stable.
    auto clone_block = block->clone_empty();
    // Initialize virtual slot columns by schema (avoid runtime type checks).
    // virtual_column_exprs is keyed by read-schema ordinal == block position.
    for (const auto& [ordinal, expr_ctx] : _context->virtual_column_exprs) {
        DORIS_CHECK(ordinal < clone_block.columns());
        clone_block.get_by_position(ordinal).column =
                expr_ctx->root()->data_type()->create_column();
    }
    const size_t clone_block_columns = clone_block.columns();
    MutableBlock mutable_block = MutableBlock::build_mutable_block(std::move(clone_block));

    const std::vector<uint32_t>* sort_columns = _context->read_orderby_key_columns;
    for (auto column_idx : *sort_columns) {
        DORIS_CHECK(column_idx < clone_block_columns);
    }
    size_t first_sort_column_idx = (*sort_columns)[0];

    BlockRowPosComparator row_pos_comparator(&mutable_block, sort_columns,
                                             _context->read_orderby_key_reverse);
    std::multiset<size_t, BlockRowPosComparator, std::allocator<size_t>> sorted_row_pos(
            row_pos_comparator);

    if (_is_reverse) {
        std::ranges::reverse(_rs_splits);
    }

    for (size_t i = 0; i < _rs_splits.size(); i++) {
        const auto& rs_split = _rs_splits[i];
        // init will prune segment by _reader_context.conditions and _reader_context.runtime_conditions
        RETURN_IF_ERROR(rs_split.rs_reader->init(_context, rs_split));

        // read _topn_limit rows from this rs
        size_t read_rows = 0;
        bool eof = false;
        while (read_rows < _topn_limit && !eof) {
            block->clear_column_data();
            auto status = rs_split.rs_reader->next_batch(block);
            if (!status.ok()) {
                if (status.is<END_OF_FILE>()) {
                    eof = true;
                    if (block->rows() == 0) {
                        break;
                    }
                } else {
                    return status;
                }
            }

            // update read rows
            read_rows += block->rows();

            // insert block rows to mutable_block and adjust sorted_row_pos
            bool changed = false;

            size_t rows_to_copy = 0;
            if (sorted_row_pos.empty()) {
                rows_to_copy = std::min(block->rows(), _topn_limit);
            } else {
                // _is_reverse == true  last_row_pos is the pos of smallest row
                // _is_reverse == false last_row_pos is biggest row
                size_t last_row_pos = *sorted_row_pos.rbegin();

                // find the how many rows which is less than the last row in mutable_block
                for (size_t j = 0; j < block->rows(); j++) {
                    // if there is not enough rows in sorted_row_pos, just copy new rows
                    if (sorted_row_pos.size() + rows_to_copy < _topn_limit) {
                        rows_to_copy++;
                        continue;
                    }

                    DCHECK_GE(block->columns(), sort_columns->size());
                    DCHECK_GE(mutable_block.columns(), sort_columns->size());

                    int res = 0;
                    for (auto k : *sort_columns) {
                        DORIS_CHECK(k < block->columns());
                        DORIS_CHECK(k < mutable_block.columns());
                        DCHECK(block->get_by_position(k).type->equals(
                                *mutable_block.get_datatype_by_position(k)));
                        res = block->get_by_position(k).column->compare_at(
                                j, last_row_pos, *(mutable_block.get_column_by_position(k)), -1);
                        if (res) {
                            break;
                        }
                    }

                    // only copy needed rows
                    // _is_reverse == true  > smallest is ok
                    // _is_reverse == false < biggest is ok
                    if ((_is_reverse && res > 0) || (!_is_reverse && res < 0)) {
                        rows_to_copy++;
                    } else {
                        break;
                    }
                }
            }

            if (rows_to_copy > 0) {
                size_t base = mutable_block.rows();
                // append block to mutable_block
                RETURN_IF_ERROR(mutable_block.add_rows(block, 0, rows_to_copy));
                // insert appended rows pos in mutable_block to sorted_row_pos and sort it
                for (size_t j = 0; j < rows_to_copy; j++) {
                    sorted_row_pos.insert(base + j);
                    changed = true;
                }
            }

            // delete to keep _topn_limit row pos
            if (sorted_row_pos.size() > _topn_limit) {
                auto first = sorted_row_pos.begin();
                for (size_t j = 0; j < _topn_limit; j++) {
                    first++;
                }
                sorted_row_pos.erase(first, sorted_row_pos.end());

                // shrink mutable_block to save memory when rows > _topn_limit * 2
                if (mutable_block.rows() > _topn_limit * 2) {
                    VLOG_DEBUG << "topn debug start  shrink mutable_block from "
                               << mutable_block.rows() << " rows";
                    Block tmp_block = mutable_block.to_block();
                    clone_block = tmp_block.clone_empty();
                    mutable_block = MutableBlock::build_mutable_block(std::move(clone_block));
                    for (size_t row_pos : sorted_row_pos) {
                        mutable_block.add_row(&tmp_block, cast_set<int>(row_pos));
                    }

                    sorted_row_pos.clear();
                    for (size_t j = 0; j < _topn_limit; j++) {
                        sorted_row_pos.insert(j);
                    }
                    VLOG_DEBUG << "topn debug finish shrink mutable_block to "
                               << mutable_block.rows() << " rows";
                }
            }

            // update runtime_predicate
            if (!_context->topn_filter_source_node_ids.empty() && changed &&
                sorted_row_pos.size() >= _topn_limit) {
                // get field value from column
                size_t last_sorted_row = *sorted_row_pos.rbegin();
                auto* col_ptr = mutable_block.get_column_by_position(first_sort_column_idx).get();
                Field new_top;
                col_ptr->get(last_sorted_row, new_top);

                // update orderby_extrems in query global context
                auto* query_ctx = _context->runtime_state->get_query_ctx();
                for (int id : _context->topn_filter_source_node_ids) {
                    RETURN_IF_ERROR(query_ctx->get_runtime_predicate(id).update(new_top));
                }
            }
        } // end of while (read_rows < _topn_limit && !eof)
        VLOG_DEBUG << "topn debug rowset " << i << " read_rows=" << read_rows << " eof=" << eof
                   << " _topn_limit=" << _topn_limit
                   << " sorted_row_pos.size()=" << sorted_row_pos.size()
                   << " mutable_block.rows()=" << mutable_block.rows();
    } // end of for (auto rs_reader : _rs_readers)

    // copy result_block to block
    VLOG_DEBUG << "topn debug result _topn_limit=" << _topn_limit
               << " sorted_row_pos.size()=" << sorted_row_pos.size()
               << " mutable_block.rows()=" << mutable_block.rows();
    *block = mutable_block.to_block();
    _topn_eof = true;
    return block->rows() > 0 ? Status::OK() : Status::Error<END_OF_FILE>("");
}

bool TopNIterator::BlockRowPosComparator::operator()(const size_t& lpos, const size_t& rpos) const {
    int ret = _mutable_block->compare_at(lpos, rpos, _compare_columns, *_mutable_block, -1);
    return _is_reverse ? ret > 0 : ret < 0;
}

} // namespace doris
