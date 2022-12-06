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

#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/sort_description.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

struct HeapSortCursorBlockView {
public:
    Block block;
    ColumnRawPtrs sort_columns;
    SortDescription& desc;

    HeapSortCursorBlockView(Block&& cur_block, SortDescription& sort_desc)
            : block(cur_block), desc(sort_desc) {
        _reset();
    }

    void filter_block(IColumn::Filter& filter) {
        Block::filter_block_internal(&block, filter, block.columns());
        _reset();
    }

private:
    void _reset() {
        sort_columns.clear();
        auto columns = block.get_columns();
        for (size_t j = 0, size = desc.size(); j < size; ++j) {
            auto& column_desc = desc[j];
            size_t column_number = !column_desc.column_name.empty()
                                           ? block.get_position_by_name(column_desc.column_name)
                                           : column_desc.column_number;
            sort_columns.push_back(columns[column_number].get());
        }
    }
};

// Use `SharedHeapSortCursorBlockView` for `HeapSortCursorBlockView` instead of shared_ptr because there will be no
// concurrent operation for `HeapSortCursorBlockView` and we don't need the lock inside shared_ptr
class SharedHeapSortCursorBlockView {
public:
    SharedHeapSortCursorBlockView(HeapSortCursorBlockView&& reference)
            : _ref_count(0), _reference(std::move(reference)) {}
    SharedHeapSortCursorBlockView(const SharedHeapSortCursorBlockView&) = delete;
    void unref() noexcept {
        DCHECK_GT(_ref_count, 0);
        _ref_count--;
        if (_ref_count == 0) {
            delete this;
        }
    }
    void ref() noexcept { _ref_count++; }

    HeapSortCursorBlockView& value() { return _reference; }

    int ref_count() const { return _ref_count; }

private:
    ~SharedHeapSortCursorBlockView() noexcept = default;
    int _ref_count;
    HeapSortCursorBlockView _reference;
};

struct HeapSortCursorImpl {
public:
    HeapSortCursorImpl(int row_id, SharedHeapSortCursorBlockView* block_view)
            : _row_id(row_id), _block_view(block_view) {
        block_view->ref();
    }

    HeapSortCursorImpl(const HeapSortCursorImpl& other) {
        _row_id = other._row_id;
        _block_view = other._block_view;
        _block_view->ref();
    }

    HeapSortCursorImpl(HeapSortCursorImpl&& other) {
        _row_id = other._row_id;
        _block_view = other._block_view;
        other._block_view = nullptr;
    }

    HeapSortCursorImpl& operator=(HeapSortCursorImpl&& other) {
        std::swap(_row_id, other._row_id);
        std::swap(_block_view, other._block_view);
        return *this;
    }

    ~HeapSortCursorImpl() {
        if (_block_view) {
            _block_view->unref();
        }
    };

    const size_t row_id() const { return _row_id; }

    const ColumnRawPtrs& sort_columns() const { return _block_view->value().sort_columns; }

    const Block* block() const { return &_block_view->value().block; }

    const SortDescription& sort_desc() const { return _block_view->value().desc; }

    bool operator<(const HeapSortCursorImpl& rhs) const {
        for (size_t i = 0; i < sort_desc().size(); ++i) {
            int direction = sort_desc()[i].direction;
            int nulls_direction = sort_desc()[i].nulls_direction;
            int res = direction * sort_columns()[i]->compare_at(row_id(), rhs.row_id(),
                                                                *(rhs.sort_columns()[i]),
                                                                nulls_direction);
            // ASC: direction == 1. If bigger, res > 0. So we return true.
            if (res < 0) {
                return true;
            }
            if (res > 0) {
                return false;
            }
        }
        return false;
    }

private:
    size_t _row_id;
    SharedHeapSortCursorBlockView* _block_view;
};

/** Cursor allows to compare rows in different blocks (and parts).
  * Cursor moves inside single block.
  * It is used in priority queue.
  */
struct MergeSortCursorImpl {
    ColumnRawPtrs all_columns;
    ColumnRawPtrs sort_columns;
    SortDescription desc;
    size_t sort_columns_size = 0;
    size_t pos = 0;
    size_t rows = 0;

    MergeSortCursorImpl() = default;
    virtual ~MergeSortCursorImpl() = default;

    MergeSortCursorImpl(const Block& block, const SortDescription& desc_)
            : desc(desc_), sort_columns_size(desc.size()) {
        reset(block);
    }

    MergeSortCursorImpl(const Columns& columns, const SortDescription& desc_)
            : desc(desc_), sort_columns_size(desc.size()) {
        for (auto& column_desc : desc) {
            if (!column_desc.column_name.empty()) {
                LOG(FATAL)
                        << "SortDesctiption should contain column position if MergeSortCursor was "
                           "used without header.";
            }
        }
        reset(columns, {});
    }

    bool empty() const { return rows == 0; }

    /// Set the cursor to the beginning of the new block.
    void reset(const Block& block) { reset(block.get_columns(), block); }

    /// Set the cursor to the beginning of the new block.
    void reset(const Columns& columns, const Block& block) {
        all_columns.clear();
        sort_columns.clear();

        size_t num_columns = columns.size();

        for (size_t j = 0; j < num_columns; ++j) {
            all_columns.push_back(columns[j].get());
        }

        for (size_t j = 0, size = desc.size(); j < size; ++j) {
            auto& column_desc = desc[j];
            size_t column_number = !column_desc.column_name.empty()
                                           ? block.get_position_by_name(column_desc.column_name)
                                           : column_desc.column_number;
            sort_columns.push_back(columns[column_number].get());
        }

        pos = 0;
        rows = all_columns[0]->size();
    }

    bool isFirst() const { return pos == 0; }
    bool isLast() const { return pos + 1 >= rows; }
    void next() { ++pos; }

    virtual bool has_next_block() { return false; }
    virtual Block* block_ptr() { return nullptr; }
};

using BlockSupplier = std::function<Status(Block**)>;

struct ReceiveQueueSortCursorImpl : public MergeSortCursorImpl {
    ReceiveQueueSortCursorImpl(const BlockSupplier& block_supplier,
                               const std::vector<VExprContext*>& ordering_expr,
                               const std::vector<bool>& is_asc_order,
                               const std::vector<bool>& nulls_first)
            : _ordering_expr(ordering_expr), _block_supplier(block_supplier) {
        sort_columns_size = ordering_expr.size();

        desc.resize(ordering_expr.size());
        for (int i = 0; i < desc.size(); i++) {
            desc[i].direction = is_asc_order[i] ? 1 : -1;
            desc[i].nulls_direction = nulls_first[i] ? -desc[i].direction : desc[i].direction;
        }
        _is_eof = !has_next_block();
    }

    bool has_next_block() override {
        auto status = _block_supplier(&_block_ptr);
        if (status.ok() && _block_ptr != nullptr) {
            for (int i = 0; i < desc.size(); ++i) {
                _ordering_expr[i]->execute(_block_ptr, &desc[i].column_number);
            }
            MergeSortCursorImpl::reset(*_block_ptr);
            return true;
        }
        _block_ptr = nullptr;
        return false;
    }

    Block* block_ptr() override { return _block_ptr; }

    size_t columns_num() const { return all_columns.size(); }

    Block create_empty_blocks() const {
        size_t num_columns = columns_num();
        MutableColumns columns(num_columns);
        for (size_t i = 0; i < num_columns; ++i) {
            columns[i] = all_columns[i]->clone_empty();
        }
        return _block_ptr->clone_with_columns(std::move(columns));
    }

    const std::vector<VExprContext*>& _ordering_expr;
    Block* _block_ptr = nullptr;
    BlockSupplier _block_supplier {};
    bool _is_eof = false;
};

/// For easy copying.
struct MergeSortCursor {
    MergeSortCursorImpl* impl;

    MergeSortCursor(MergeSortCursorImpl* impl_) : impl(impl_) {}
    MergeSortCursorImpl* operator->() const { return impl; }

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

    /// Checks that all rows in the current block of this cursor are less than or equal to all the rows of the current block of another cursor.
    bool totally_less(const MergeSortCursor& rhs) const {
        if (impl->rows == 0 || rhs.impl->rows == 0) {
            return false;
        }

        /// The last row of this cursor is no larger than the first row of the another cursor.
        return greater_at(rhs, impl->rows - 1, 0) == -1;
    }

    bool greater(const MergeSortCursor& rhs) const {
        return !impl->empty() && greater_at(rhs, impl->pos, rhs.impl->pos) > 0;
    }

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool operator<(const MergeSortCursor& rhs) const { return greater(rhs); }
};

/// For easy copying.
struct MergeSortBlockCursor {
    MergeSortCursorImpl* impl;

    MergeSortBlockCursor(MergeSortCursorImpl* impl_) : impl(impl_) {}
    MergeSortCursorImpl* operator->() const { return impl; }

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

} // namespace doris::vectorized
