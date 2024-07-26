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

    // need exception safety
    void filter_block(IColumn::Filter& filter) {
        Block::filter_block_internal(&block, filter, block.columns());
        _reset();
    }

private:
    void _reset() {
        sort_columns.clear();
        auto columns = block.get_columns_and_convert();
        for (size_t j = 0, size = desc.size(); j < size; ++j) {
            auto& column_desc = desc[j];
            size_t column_number = !column_desc.column_name.empty()
                                           ? block.get_position_by_name(column_desc.column_name)
                                           : column_desc.column_number;
            sort_columns.push_back(columns[column_number].get());
        }
    }
};

using HeapSortCursorBlockSPtr = std::shared_ptr<HeapSortCursorBlockView>;

struct HeapSortCursorImpl {
public:
    HeapSortCursorImpl(int row_id, HeapSortCursorBlockSPtr block_view)
            : _row_id(row_id), _block_view(block_view) {}

    HeapSortCursorImpl(const HeapSortCursorImpl& other) {
        _row_id = other._row_id;
        _block_view = other._block_view;
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

    ~HeapSortCursorImpl() = default;

    size_t row_id() const { return _row_id; }

    const ColumnRawPtrs& sort_columns() const { return _block_view->sort_columns; }

    const Block* block() const { return &_block_view->block; }

    const SortDescription& sort_desc() const { return _block_view->desc; }

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
    HeapSortCursorBlockSPtr _block_view;
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

    MergeSortCursorImpl(Block& block, const SortDescription& desc_)
            : desc(desc_), sort_columns_size(desc.size()) {
        reset(block);
    }

    MergeSortCursorImpl(const SortDescription& desc_)
            : desc(desc_), sort_columns_size(desc.size()) {}
    bool empty() const { return rows == 0; }

    /// Set the cursor to the beginning of the new block.
    void reset(Block& block) {
        all_columns.clear();
        sort_columns.clear();

        auto columns = block.get_columns_and_convert();
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

    bool is_first() const { return pos == 0; }
    bool is_last() const { return pos + 1 >= rows; }
    void next() { ++pos; }

    virtual bool has_next_block() { return false; }
    virtual Block* block_ptr() { return nullptr; }
};

using BlockSupplier = std::function<Status(Block*, bool* eos)>;

struct BlockSupplierSortCursorImpl : public MergeSortCursorImpl {
    BlockSupplierSortCursorImpl(const BlockSupplier& block_supplier,
                                const VExprContextSPtrs& ordering_expr,
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

    BlockSupplierSortCursorImpl(const BlockSupplier& block_supplier, const SortDescription& desc_)
            : MergeSortCursorImpl(desc_), _block_supplier(block_supplier) {
        _is_eof = !has_next_block();
    }

    bool has_next_block() override {
        if (_is_eof) {
            return false;
        }
        _block.clear();
        Status status;
        do {
            status = _block_supplier(&_block, &_is_eof);
        } while (_block.empty() && !_is_eof && status.ok());
        // If status not ok, upper callers could not detect whether it is eof or error.
        // So that fatal here, and should throw exception in the future.
        if (status.ok() && !_block.empty()) {
            if (_ordering_expr.size() > 0) {
                for (int i = 0; status.ok() && i < desc.size(); ++i) {
                    // TODO yiguolei: throw exception if status not ok in the future
                    status = _ordering_expr[i]->execute(&_block, &desc[i].column_number);
                }
            }
            MergeSortCursorImpl::reset(_block);
            return status.ok();
        } else if (!status.ok()) {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, status.msg());
        }
        return false;
    }

    Block* block_ptr() override {
        if (_is_eof) {
            return nullptr;
        }
        return &_block;
    }

    size_t columns_num() const { return all_columns.size(); }

    Block create_empty_blocks() const {
        size_t num_columns = columns_num();
        MutableColumns columns(num_columns);
        for (size_t i = 0; i < num_columns; ++i) {
            columns[i] = all_columns[i]->clone_empty();
        }
        return _block.clone_with_columns(std::move(columns));
    }

    VExprContextSPtrs _ordering_expr;
    Block _block;
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
    MergeSortCursorImpl* impl = nullptr;

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
