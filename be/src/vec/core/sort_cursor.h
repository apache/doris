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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/SortCursor.h
// and modified by Doris

#pragma once

#include <utility>

#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/sort_description.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
/** Cursor allows to compare rows in different blocks (and parts).
  * Cursor moves inside single block.
  * It is used in priority queue.
  */
struct MergeSortCursorImpl {
    ENABLE_FACTORY_CREATOR(MergeSortCursorImpl);
    std::shared_ptr<Block> block;
    ColumnRawPtrs sort_columns;
    ColumnRawPtrs columns;
    SortDescription desc;
    size_t sort_columns_size = 0;
    int pos = 0;
    int rows = 0;

    MergeSortCursorImpl() = default;
    virtual ~MergeSortCursorImpl() = default;

    MergeSortCursorImpl(std::shared_ptr<Block> block_, SortDescription desc_)
            : block(std::move(block_)), desc(std::move(desc_)), sort_columns_size(desc.size()) {
        reset();
    }

    MergeSortCursorImpl(SortDescription desc_)
            : block(Block::create_shared()),
              desc(std::move(desc_)),
              sort_columns_size(desc.size()) {}

    bool empty() const { return rows == 0; }

    void reverse(IColumn::Permutation& reverse_perm) {
        MutableColumns columns_reversed;
        reverse_perm.resize(rows - pos);
        for (int i = 0; i + pos < rows; ++i) {
            reverse_perm[i] = rows - i - 1;
        }
        for (auto& column : columns) {
            columns_reversed.push_back(column->permute(reverse_perm, rows - pos));
        }
        block->set_columns(std::move(columns_reversed));
        for (auto& column_desc : desc) {
            column_desc.direction *= -1;
        }
        reset();
    }

    /// Set the cursor to the beginning of the new block.
    void reset() {
        sort_columns.clear();
        columns.clear();

        auto tmp_columns = block->get_columns_and_convert();
        columns.reserve(tmp_columns.size());
        for (auto col : tmp_columns) {
            columns.push_back(col.get());
        }
        for (auto& column_desc : desc) {
            size_t column_number = !column_desc.column_name.empty()
                                           ? block->get_position_by_name(column_desc.column_name)
                                           : column_desc.column_number;
            sort_columns.push_back(columns[column_number]);
        }

        pos = 0;
        rows = (int)block->rows();
    }

    bool is_first() const { return pos == 0; }
    bool is_last(size_t size = 1) const { return pos + size >= rows; }
    void next(size_t size = 1) { pos += size; }
    size_t get_size() const { return rows; }

    virtual void process_next() {}
    virtual Block* block_ptr() { return nullptr; }
    virtual bool eof() const { return false; }

    Field get_top_value() const {
        Field field {PrimitiveType::TYPE_NULL};
        sort_columns[0]->get(pos, field);
        return field;
    }
};

using BlockSupplier = std::function<Status(Block*, bool* eos)>;

struct BlockSupplierSortCursorImpl : public MergeSortCursorImpl {
    ENABLE_FACTORY_CREATOR(BlockSupplierSortCursorImpl);
    BlockSupplierSortCursorImpl(BlockSupplier block_supplier,
                                const VExprContextSPtrs& ordering_expr,
                                const std::vector<bool>& is_asc_order,
                                const std::vector<bool>& nulls_first)
            : _ordering_expr(ordering_expr), _block_supplier(std::move(block_supplier)) {
        block = Block::create_shared();
        sort_columns_size = ordering_expr.size();

        desc.resize(ordering_expr.size());
        for (int i = 0; i < desc.size(); i++) {
            desc[i].direction = is_asc_order[i] ? 1 : -1;
            desc[i].nulls_direction = nulls_first[i] ? -desc[i].direction : desc[i].direction;
        }
        process_next();
    }

    BlockSupplierSortCursorImpl(BlockSupplier block_supplier, const SortDescription& desc_)
            : MergeSortCursorImpl(desc_), _block_supplier(std::move(block_supplier)) {
        process_next();
    }

    void process_next() override {
        if (_is_eof) {
            return;
        }
        block->clear();
        THROW_IF_ERROR(_block_supplier(block.get(), &_is_eof));
        DCHECK(!block->empty() or _is_eof);
        if (!block->empty()) {
            if (!_ordering_expr.empty()) {
                DCHECK_EQ(_ordering_expr.size(), desc.size());
                for (int i = 0; i < desc.size(); ++i) {
                    THROW_IF_ERROR(_ordering_expr[i]->execute(block.get(), &desc[i].column_number));
                }
            }
            MergeSortCursorImpl::reset();
        } else {
            pos = 0;
            rows = (int)block->rows();
        }
    }

    Block* block_ptr() override { return block.get(); }
    bool eof() const override { return is_last(0) && _is_eof; }

    VExprContextSPtrs _ordering_expr;
    BlockSupplier _block_supplier {};
    bool _is_eof = false;
};

/// For easy copying.
struct MergeSortCursor {
    ENABLE_FACTORY_CREATOR(MergeSortCursor);
    std::shared_ptr<MergeSortCursorImpl> impl;

    MergeSortCursor(std::shared_ptr<MergeSortCursorImpl> impl_) : impl(std::move(impl_)) {}
    MergeSortCursorImpl* operator->() const { return impl.get(); }

    /// The specified row of this cursor is greater than the specified row of another cursor.
    int8_t greater_at(const MergeSortCursor& rhs, size_t lhs_pos, size_t rhs_pos) const {
        for (size_t i = 0; i < impl->sort_columns_size; ++i) {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res = direction * impl->sort_columns[i]->compare_at(lhs_pos, rhs_pos,
                                                                    *(rhs.impl->sort_columns[i]),
                                                                    nulls_direction);
            if (res > 0) {
                return 1;
            }
            if (res < 0) {
                return -1;
            }
        }
        return 0;
    }

    bool greater_or_equals_with_offset(const MergeSortCursor& rhs, size_t lhs_offset,
                                       size_t rhs_offset) const {
        return greater_at(rhs, impl->pos + lhs_offset, rhs.impl->pos + rhs_offset) >= 0;
    }

    bool greater(const MergeSortCursor& rhs) const {
        return !impl->empty() && greater_at(rhs, impl->pos, rhs.impl->pos) > 0;
    }

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool operator<(const MergeSortCursor& rhs) const { return greater(rhs); }

    Field get_top_value() const { return impl->get_top_value(); }
};

/// For easy copying.
struct MergeSortBlockCursor {
    ENABLE_FACTORY_CREATOR(MergeSortBlockCursor);
    std::shared_ptr<MergeSortCursorImpl> impl = nullptr;

    MergeSortBlockCursor(std::shared_ptr<MergeSortCursorImpl> impl_) : impl(std::move(impl_)) {}
    MergeSortCursorImpl* operator->() const { return impl.get(); }

    /// The specified row of this cursor is greater than the specified row of another cursor.
    int8_t less_at(const MergeSortBlockCursor& rhs, int rows) const {
        for (size_t i = 0; i < impl->sort_columns_size; ++i) {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res = direction * impl->sort_columns[i]->compare_at(rows, rhs->rows - 1,
                                                                    *(rhs.impl->sort_columns[i]),
                                                                    nulls_direction);
            if (res < 0) {
                return 1;
            }
            if (res > 0) {
                return -1;
            }
        }
        return 0;
    }

    /// Checks that all rows in the current block of this cursor are less than or equal to all the rows of the current block of another cursor.
    bool totally_greater(const MergeSortBlockCursor& rhs) const {
        if (impl->rows == 0 || rhs.impl->rows == 0) {
            return false;
        }

        /// The last row of this cursor is no larger than the first row of the another cursor.
        return less_at(rhs, 0) == -1;
    }

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool operator<(const MergeSortBlockCursor& rhs) const {
        return less_at(rhs, impl->rows - 1) == 1;
    }
};

enum class SortingQueueStrategy : uint8_t { Default, Batch };

/// Allows to fetch data from multiple sort cursors in sorted order (merging sorted data streams).
template <typename Cursor, SortingQueueStrategy strategy>
class SortingQueueImpl {
public:
    SortingQueueImpl() = default;

    template <typename Cursors>
    explicit SortingQueueImpl(Cursors& cursors) {
        size_t size = cursors.size();
        _queue.reserve(size);

        for (size_t i = 0; i < size; ++i) {
            _queue.emplace_back(cursors[i]);
        }

        std::make_heap(_queue.begin(), _queue.end());

        if constexpr (strategy == SortingQueueStrategy::Batch) {
            if (!_queue.empty()) {
                update_batch_size();
            }
        }
    }

    bool is_valid() const { return !_queue.empty(); }

    Cursor& current()
        requires(strategy == SortingQueueStrategy::Default)
    {
        return &_queue.front();
    }

    std::pair<Cursor*, size_t> current()
        requires(strategy == SortingQueueStrategy::Batch)
    {
        return {&_queue.front(), batch_size};
    }

    size_t size() { return _queue.size(); }

    Cursor& next_child() { return _queue[next_child_index()]; }

    void ALWAYS_INLINE next()
        requires(strategy == SortingQueueStrategy::Default)
    {
        assert(is_valid());

        if (!_queue.front()->is_last()) {
            _queue.front()->next();
            update_top(true);
        } else {
            remove_top();
        }
    }

    void ALWAYS_INLINE next(size_t batch_size_value)
        requires(strategy == SortingQueueStrategy::Batch)
    {
        assert(is_valid());
        assert(batch_size_value <= batch_size);
        assert(batch_size_value > 0);

        batch_size -= batch_size_value;
        if (batch_size > 0) {
            _queue.front()->next(batch_size_value);
            return;
        }

        if (!_queue.front()->is_last(batch_size_value)) {
            _queue.front()->next(batch_size_value);
            update_top(false);
        } else {
            remove_top();
        }
    }

    void remove_top() {
        std::pop_heap(_queue.begin(), _queue.end());
        _queue.pop_back();
        next_child_idx = 0;

        if constexpr (strategy == SortingQueueStrategy::Batch) {
            if (_queue.empty()) {
                batch_size = 0;
            } else {
                update_batch_size();
            }
        }
    }

    void push(MergeSortCursor cursor) {
        _queue.emplace_back(std::move(cursor));
        std::push_heap(_queue.begin(), _queue.end());
        next_child_idx = 0;

        if constexpr (strategy == SortingQueueStrategy::Batch) {
            update_batch_size();
        }
    }

private:
    using Container = std::vector<Cursor>;
    Container _queue;

    /// Cache comparison between first and second child if the order in queue has not been changed.
    size_t next_child_idx = 0;
    size_t batch_size = 0;

    size_t ALWAYS_INLINE next_child_index() {
        if (next_child_idx == 0) {
            next_child_idx = 1;

            if (_queue.size() > 2 && _queue[1].greater(_queue[2])) {
                ++next_child_idx;
            }
        }

        return next_child_idx;
    }

    /// This is adapted version of the function __sift_down from libc++.
    /// Why cannot simply use std::priority_queue?
    /// - because it doesn't support updating the top element and requires pop and push instead.
    /// Also look at "Boost.Heap" library.
    void ALWAYS_INLINE update_top(bool check_in_order) {
        size_t size = _queue.size();
        if (size < 2) {
            return;
        }

        auto begin = _queue.begin();

        size_t child_idx = next_child_index();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        if (check_in_order && (*child_it).greater(*begin)) {
            if constexpr (strategy == SortingQueueStrategy::Batch) {
                update_batch_size();
            }
            return;
        }

        next_child_idx = 0;

        auto curr_it = begin;
        auto top(std::move(*begin));
        do {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = std::move(*child_it);
            curr_it = child_it;

            // recompute the child based off of the updated parent
            child_idx = 2 * child_idx + 1;

            if (child_idx >= size) {
                break;
            }

            child_it = begin + child_idx;

            if ((child_idx + 1) < size && (*child_it).greater(*(child_it + 1))) {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

            /// Check if we are in order.
        } while (!((*child_it).greater(top)));
        *curr_it = std::move(top);

        if constexpr (strategy == SortingQueueStrategy::Batch) {
            update_batch_size();
        }
    }

    /// Update batch size of elements that client can extract from current cursor
    void update_batch_size() {
        DCHECK(!_queue.empty());

        auto& begin_cursor = *_queue.begin();
        size_t min_cursor_size = begin_cursor->get_size();
        size_t min_cursor_pos = begin_cursor->pos;

        if (_queue.size() == 1) {
            batch_size = min_cursor_size - min_cursor_pos;
            return;
        }

        batch_size = 1;
        size_t child_idx = next_child_index();
        auto& next_child_cursor = *(_queue.begin() + child_idx);

        auto add_if_better = [&](size_t step) {
            if (min_cursor_pos + batch_size + step > min_cursor_size) {
                return false;
            }
            if (next_child_cursor.greater_or_equals_with_offset(begin_cursor, 0,
                                                                batch_size + step - 1)) {
                batch_size += step;
                return true;
            }
            return false;
        };

        if (!add_if_better(1)) {
            return;
        }

        size_t big_step = min_cursor_size - min_cursor_pos - batch_size;
        if (add_if_better(big_step)) {
            return;
        }

        while (add_if_better(1)) {
        }
    }
};
template <typename Cursor>
using SortingQueue = SortingQueueImpl<Cursor, SortingQueueStrategy::Default>;
template <typename Cursor>
using SortingQueueBatch = SortingQueueImpl<Cursor, SortingQueueStrategy::Batch>;
#include "common/compile_check_end.h"
} // namespace doris::vectorized
