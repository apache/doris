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

#include "storage/iterator/mask_merge_iterator.h"

#include <bvar/bvar.h>

#include <algorithm>
#include <list>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "core/column/column.h"
#include "storage/iterator/row_sources_buffer.h"
#include "storage/iterators.h"

namespace doris {
using namespace ErrorCode;

namespace {

bvar::Adder<int64_t> g_vertical_compaction_active_segment_contexts(
        "vertical_compaction_active_segment_contexts");

} // namespace

// --------------- VerticalMergeIteratorContext ------------- //
// One source of a value group: the rows of a key group source, read in order and handed out as
// the row sources name them. Takes ownership of the iterator.
class VerticalMergeIteratorContext {
public:
    VerticalMergeIteratorContext(RowwiseIteratorUPtr&& iter,
                                 VerticalCompactionContextStats* context_stats)
            : _iter(std::move(iter)), _context_stats(context_stats) {}

    VerticalMergeIteratorContext(const VerticalMergeIteratorContext&) = delete;
    VerticalMergeIteratorContext(VerticalMergeIteratorContext&&) = delete;
    VerticalMergeIteratorContext& operator=(const VerticalMergeIteratorContext&) = delete;
    VerticalMergeIteratorContext& operator=(VerticalMergeIteratorContext&&) = delete;

    ~VerticalMergeIteratorContext();
    Status block_reset(const std::shared_ptr<Block>& block);
    Status init(const StorageReadOptions& opts, CompactionSampleInfo* sample_info = nullptr);
    // Copy `count` rows from the current one on into `block`; the last of them becomes the
    // current row.
    Status copy_rows(Block* block, size_t count);

    Status advance();

    // Advance by n rows at once (batch optimization)
    // More efficient than calling advance() n times when n rows are in same block
    Status advance_by(size_t n);

    // Return if it has remaining data in this context.
    // Only when this function return true, current_row()
    // will return a valid row
    bool valid() const { return _valid; }

    size_t remain_rows() { return _block->rows() - _index_in_block; }

    bool is_first_row() const { return _is_first_row; }
    void set_is_first_row(bool is_first_row) { _is_first_row = is_first_row; }
    void set_cur_row_ref(IteratorRowRef* ref) {
        ref->block = _block;
        ref->row_pos = _index_in_block;
    }
    bool inited() const { return _inited; }

    size_t bytes() {
        if (_block) {
            return _block->allocated_bytes();
        } else {
            return 0;
        }
    }

    size_t rows() {
        if (_block) {
            return _block->rows();
        } else {
            return 0;
        }
    }

    // Get current row position in block (for batch optimization)
    uint32_t current_row_pos() const { return _index_in_block; }

    // Get block pointer (for batch optimization)
    Block* block() const { return _block.get(); }

    const std::shared_ptr<Block>& block_ptr() const { return _block; }

    // No later row source references this context. The returned IteratorRowRef/RowBatch keeps
    // its own shared_ptr<Block>, so the segment reader and context-owned blocks can be released.
    void release_resources();

private:
    // Load next block into _block
    Status _load_next_block();
    void _mark_active();
    void _mark_inactive();

    RowwiseIteratorUPtr _iter;

    bool _valid = false;
    bool _inited = false;
    int32_t _index_in_block = -1;
    size_t _block_row_max = 0;
    VerticalCompactionContextStats* _context_stats;

    // used to store data load from iterator->next_batch(Block*)
    std::shared_ptr<Block> _block;
    // used to store data still on block view
    std::list<std::shared_ptr<Block>> _block_list;
    // use to identify whether it's first block load from RowwiseIterator
    bool _is_first_row = true;
    bool _is_active_context_counted = false;
};

// ----------  vertical merge iterator context ----------//
VerticalMergeIteratorContext::~VerticalMergeIteratorContext() {
    release_resources();
}

void VerticalMergeIteratorContext::release_resources() {
    _valid = false;
    _iter.reset();
    _block.reset();
    // Unlike the physical EOF fallback, source exhaustion proves that this context will not
    // access these blocks again. External IteratorRowRef/RowBatch owners keep them alive through
    // shared_ptr, so dropping the context-owned references here is intentional.
    _block_list.clear();
    _mark_inactive();
}

void VerticalMergeIteratorContext::_mark_active() {
    DCHECK(!_is_active_context_counted);
    _is_active_context_counted = true;
    g_vertical_compaction_active_segment_contexts << 1;
    if (_context_stats != nullptr) {
        ++_context_stats->active_segment_contexts;
        _context_stats->active_segment_contexts_peak =
                std::max(_context_stats->active_segment_contexts_peak,
                         _context_stats->active_segment_contexts);
    }
}

void VerticalMergeIteratorContext::_mark_inactive() {
    if (!_is_active_context_counted) {
        return;
    }
    _is_active_context_counted = false;
    g_vertical_compaction_active_segment_contexts << -1;
    if (_context_stats != nullptr) {
        DCHECK_GT(_context_stats->active_segment_contexts, 0);
        --_context_stats->active_segment_contexts;
    }
}

Status VerticalMergeIteratorContext::block_reset(const std::shared_ptr<Block>& block) {
    if (!block->columns()) {
        const ReadSchema& schema = _iter->schema();
        *block = schema.create_read_block();
        auto columns_guard = block->mutate_columns_scoped();
        for (auto& column : columns_guard.mutable_columns()) {
            column->reserve(_block_row_max);
        }
    } else {
        block->clear_column_data();
    }
    return Status::OK();
}

Status VerticalMergeIteratorContext::copy_rows(Block* block, size_t count) {
    Block& src = *_block;
    Block& dst = *block;
    DCHECK(count > 0);

    auto start = _index_in_block;
    _index_in_block += count - 1;
    RETURN_IF_CATCH_EXCEPTION({
        for (size_t i = 0; i < src.columns(); ++i) {
            auto& s_col = src.get_by_position(i);
            auto& d_col = dst.get_by_position(i);

            ColumnPtr& s_cp = s_col.column;
            ColumnPtr& d_cp = d_col.column;

            d_cp->assert_mutable()->insert_range_from(*s_cp, start, count);
        }
    });
    return Status::OK();
}
Status VerticalMergeIteratorContext::init(const StorageReadOptions& opts,
                                          CompactionSampleInfo* sample_info) {
    if (LIKELY(_inited)) {
        return Status::OK();
    }
    _block_row_max = opts.block_row_max;
    RETURN_IF_ERROR(_iter->init(opts));
    RETURN_IF_ERROR(_load_next_block());
    if (sample_info != nullptr) {
        sample_info->bytes += bytes();
        sample_info->rows += rows();
    }
    if (valid()) {
        _mark_active();
        RETURN_IF_ERROR(advance());
    }
    _inited = true;
    return Status::OK();
}

Status VerticalMergeIteratorContext::advance() {
    // NOTE: we increase _index_in_block directly to valid one check
    do {
        _index_in_block++;
        if (LIKELY(_index_in_block < _block->rows())) {
            return Status::OK();
        }
        // current batch has no data, load next batch
        RETURN_IF_ERROR(_load_next_block());
    } while (_valid);
    return Status::OK();
}

Status VerticalMergeIteratorContext::advance_by(size_t n) {
    if (n == 0) {
        return Status::OK();
    }

    while (n > 0) {
        // Calculate how many rows we can advance within current block
        // _index_in_block points to current row, so remaining = total - current - 1
        int64_t remaining_in_block = static_cast<int64_t>(_block->rows()) - _index_in_block - 1;

        if (static_cast<int64_t>(n) <= remaining_in_block) {
            // All n rows are within current block
            _index_in_block += n;
            return Status::OK();
        }

        // Need to cross block boundary
        // Consume all remaining rows in current block (including advancing past current row)
        n -= remaining_in_block + 1;

        // Load next block
        RETURN_IF_ERROR(_load_next_block());
        if (!_valid) {
            if (n > 0) {
                return Status::InternalError("advance_by exceeded available data");
            }
            break;
        }
        // After _load_next_block(), _index_in_block is set to -1
        // Next iteration will correctly calculate remaining_in_block
    }
    return Status::OK();
}

Status VerticalMergeIteratorContext::_load_next_block() {
    do {
        if (_block != nullptr) {
            _block_list.push_back(_block);
            _block = nullptr;
        }
        for (auto it = _block_list.begin(); it != _block_list.end(); it++) {
            if (it->use_count() == 1) {
                RETURN_IF_ERROR(block_reset(*it));
                _block = *it;
                _block_list.erase(it);
                break;
            }
        }
        if (_block == nullptr) {
            _block = std::make_shared<Block>();
            RETURN_IF_ERROR(block_reset(_block));
        }
        Status st = _iter->next_batch(_block.get());
        if (!st.ok()) {
            _valid = false;
            if (st.is<END_OF_FILE>()) {
                // When reading to the end of the segment file, clearing the block did not release the memory.
                // Directly releasing the block to free up memory.
                _block.reset();
                // When reading through segment file for columns that are dictionary encoded,
                // the column iterator in the segment iterator will hold the dictionary.
                // Release the segment iterator to free up the dictionary.
                _iter.reset();
                if (_block_list.empty()) {
                    _mark_inactive();
                }
                return Status::OK();
            } else {
                return st;
            }
        }
    } while (_block->rows() == 0);
    _index_in_block = -1;
    _valid = true;
    return Status::OK();
}

//  ----------------  VerticalMaskMergeIterator  -------------  //
VerticalMaskMergeIterator::VerticalMaskMergeIterator(std::vector<RowwiseIteratorUPtr>&& iters,
                                                     RowSourcesBuffer* row_sources_buf,
                                                     VerticalCompactionContextStats* context_stats)
        : _origin_iters(std::move(iters)),
          _row_sources_buf(row_sources_buf),
          _context_stats(context_stats) {}

VerticalMaskMergeIterator::~VerticalMaskMergeIterator() = default;

Status VerticalMaskMergeIterator::check_all_iter_finished() {
    for (auto& iter : _origin_iter_ctx) {
        if (iter->inited()) {
            if (iter->valid()) {
                RETURN_IF_ERROR(iter->advance());
            }
            DCHECK(!iter->valid());
        }
    }
    return Status::OK();
}

void VerticalMaskMergeIterator::consume_row_sources(uint16_t order, size_t count) {
    _row_sources_buf->advance(count);
    if (_row_sources_buf->is_source_exhausted(order)) {
        _origin_iter_ctx[order]->release_resources();
    }
}

Status VerticalMaskMergeIterator::unique_key_next_row(IteratorRowRef* ref) {
    DCHECK(_row_sources_buf);
    auto st = _row_sources_buf->has_remaining();
    while (st.ok()) {
        auto row_source = _row_sources_buf->current();
        uint16_t order = row_source.get_source_num();
        auto& ctx = _origin_iter_ctx[order];
        RETURN_IF_ERROR(ctx->init(*_opts, _sample_info));
        DCHECK(ctx->valid());
        if (!ctx->valid()) {
            LOG(INFO) << "VerticalMergeIteratorContext not valid";
            return Status::InternalError("VerticalMergeIteratorContext not valid");
        }

        if (UNLIKELY(ctx->is_first_row()) && !row_source.agg_flag()) {
            // first row in block, don't call ctx->advance
            // Except first row, we call advance first and than get cur row
            ctx->set_cur_row_ref(ref);
            ctx->set_is_first_row(false);
            consume_row_sources(order);
            return Status::OK();
        }
        RETURN_IF_ERROR(ctx->advance());
        if (!row_source.agg_flag()) {
            ctx->set_cur_row_ref(ref);
            consume_row_sources(order);
            return Status::OK();
        }
        consume_row_sources(order);
        _filtered_rows++;
        st = _row_sources_buf->has_remaining();
    }

    if (st.is<END_OF_FILE>()) {
        RETURN_IF_ERROR(check_all_iter_finished());
    }
    return st;
}

Status VerticalMaskMergeIterator::unique_key_next_batch(std::vector<RowBatch>* batches,
                                                        size_t max_rows, size_t* actual_rows) {
    DCHECK(_row_sources_buf);
    batches->clear();
    *actual_rows = 0;

    std::shared_ptr<Block> current_block;
    uint32_t batch_start = 0;
    uint32_t batch_count = 0;

    while (*actual_rows < max_rows) {
        // Check if RowSourceBuffer has remaining data
        auto st = _row_sources_buf->has_remaining();
        if (!st.ok()) {
            if (st.is<END_OF_FILE>()) {
                break;
            }
            return st;
        }

        auto row_source = _row_sources_buf->current();
        uint16_t order = row_source.get_source_num();
        auto& ctx = _origin_iter_ctx[order];

        // Initialize context
        RETURN_IF_ERROR(ctx->init(*_opts, _sample_info));
        if (!ctx->valid()) {
            return Status::InternalError("VerticalMergeIteratorContext not valid");
        }

        // Handle is_first_row and position to current row
        // Match the logic of unique_key_next_row:
        // - Only skip advance when is_first_row=true AND agg_flag=false
        // - When is_first_row=true AND agg_flag=true, we must advance to skip this row
        bool is_first = ctx->is_first_row();
        if (is_first && !row_source.agg_flag()) {
            // First row in block with non-agg flag, don't advance
            ctx->set_is_first_row(false);
        } else {
            // All other cases: advance to position at current row
            // Keep is_first_row=true when skipping leading agg rows.
            RETURN_IF_ERROR(ctx->advance());
        }

        // If current row has agg_flag=true, skip it (single row)
        if (row_source.agg_flag()) {
            consume_row_sources(order);
            _filtered_rows++;
            continue;
        }

        // Current row is non-agg, count continuous same-source non-agg rows
        // Limit by: remaining capacity and current block's remaining rows
        size_t limit = std::min(max_rows - *actual_rows, ctx->remain_rows());
        size_t run_count = _row_sources_buf->same_source_continuous_non_agg_count(order, limit);

        // run_count >= 1 (current row is non-agg)
        uint32_t start_row = ctx->current_row_pos();

        // Save block pointer before advance_by (block won't change due to limit)
        auto block = ctx->block_ptr();

        // Advance remaining rows in this run (first row already positioned)
        if (run_count > 1) {
            RETURN_IF_ERROR(ctx->advance_by(run_count - 1));
        }

        consume_row_sources(order, run_count);

        // Try to merge into current batch or create new batch
        if (current_block == block && start_row == batch_start + batch_count) {
            // Continuous with previous batch, merge
            batch_count += static_cast<uint32_t>(run_count);
        } else {
            // Save previous batch if exists
            if (current_block != nullptr && batch_count > 0) {
                batches->emplace_back(current_block, batch_start, batch_count);
            }
            // Start new batch
            current_block = std::move(block);
            batch_start = start_row;
            batch_count = static_cast<uint32_t>(run_count);
        }

        *actual_rows += run_count;
    }

    // Save the last batch
    if (current_block != nullptr && batch_count > 0) {
        batches->emplace_back(current_block, batch_start, batch_count);
    }

    // Check if we've reached the end
    if (*actual_rows == 0) {
        auto st = _row_sources_buf->has_remaining();
        if (st.is<END_OF_FILE>()) {
            RETURN_IF_ERROR(check_all_iter_finished());
        }
    }

    return Status::OK();
}

Status VerticalMaskMergeIterator::next_batch(Block* block) {
    return _next_batch(block, nullptr);
}

Status VerticalMaskMergeIterator::next_batch(Block* block, std::vector<uint8_t>* same_key) {
    same_key->clear();
    return _next_batch(block, same_key);
}

Status VerticalMaskMergeIterator::_next_batch(Block* block, std::vector<uint8_t>* same_key) {
    DCHECK(_row_sources_buf);
    size_t rows = 0;
    auto st = _row_sources_buf->has_remaining();
    while (rows < _block_row_max && st.ok()) {
        uint16_t order = _row_sources_buf->current().get_source_num();
        DCHECK(order < _origin_iter_ctx.size());
        auto& ctx = _origin_iter_ctx[order];
        RETURN_IF_ERROR(ctx->init(*_opts, _sample_info));
        DCHECK(ctx->valid());
        if (!ctx->valid()) {
            LOG(INFO) << "VerticalMergeIteratorContext not valid";
            return Status::InternalError("VerticalMergeIteratorContext not valid");
        }

        // find max same source count in cur ctx
        size_t limit = std::min(ctx->remain_rows(), _block_row_max - rows);
        auto same_source_cnt = _row_sources_buf->same_source_count(order, limit);
        if (same_key != nullptr) {
            const uint64_t first = _row_sources_buf->buf_idx();
            for (uint64_t index = first; index < first + same_source_cnt; ++index) {
                const bool same = _row_sources_buf->get_agg_flag(index);
                same_key->push_back(same);
                _filtered_rows += same;
            }
        }
        _row_sources_buf->advance(same_source_cnt);
        // copy rows to block
        RETURN_IF_ERROR(ctx->copy_rows(block, same_source_cnt));
        RETURN_IF_ERROR(ctx->advance());
        rows += same_source_cnt;
        st = _row_sources_buf->has_remaining();
    }
    if (st.is<END_OF_FILE>()) {
        RETURN_IF_ERROR(check_all_iter_finished());
        return rows > 0 ? Status::OK() : st;
    }
    return st;
}

Status VerticalMaskMergeIterator::init(const StorageReadOptions& opts,
                                       CompactionSampleInfo* sample_info) {
    _opts.emplace(opts);
    if (_origin_iters.empty()) {
        return Status::OK();
    }
    _schema = &(*_origin_iters.begin())->schema();
    for (auto& iter : _origin_iters) {
        auto ctx = std::make_unique<VerticalMergeIteratorContext>(std::move(iter), _context_stats);
        _origin_iter_ctx.push_back(std::move(ctx));
    }
    _origin_iters.clear();

    _sample_info = sample_info;
    _block_row_max = opts.block_row_max;
    return Status::OK();
}

// interfaces to create vertical merge iterator
std::shared_ptr<RowwiseIterator> new_vertical_mask_merge_iterator(
        std::vector<RowwiseIteratorUPtr>&& inputs, RowSourcesBuffer* row_sources,
        VerticalCompactionContextStats* context_stats) {
    return std::make_shared<VerticalMaskMergeIterator>(std::move(inputs), row_sources,
                                                       context_stats);
}

} // namespace doris
