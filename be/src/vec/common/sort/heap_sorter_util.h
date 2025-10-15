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

#include "vec/common/sort/sorter.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

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
        auto columns = block.get_columns_and_convert();
        for (auto& column_desc : desc) {
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
    HeapSortCursorImpl(size_t row_id, HeapSortCursorBlockSPtr block_view)
            : _row_id(row_id), _block_view(std::move(block_view)) {}

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

class SortingHeap {
    ENABLE_FACTORY_CREATOR(SortingHeap);

public:
    const HeapSortCursorImpl& top() { return _queue.top(); }

    size_t size() { return _queue.size(); }

    bool empty() { return _queue.empty(); }

    void pop() { _queue.pop(); }

    void replace_top(HeapSortCursorImpl&& top) {
        _queue.pop();
        _queue.push(std::move(top));
    }

    void push(HeapSortCursorImpl&& cursor) { _queue.push(std::move(cursor)); }

    void replace_top_if_less(HeapSortCursorImpl&& val) {
        if (val < top()) {
            replace_top(std::move(val));
        }
    }

private:
    std::priority_queue<HeapSortCursorImpl> _queue;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized