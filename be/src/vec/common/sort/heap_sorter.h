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
#include <queue>

#include "vec/common/sort/sorter.h"

namespace doris::vectorized {

class SortingHeap {
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

class HeapSorter final : public Sorter {
public:
    HeapSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset, ObjectPool* pool,
               std::vector<bool>& is_asc_order, std::vector<bool>& nulls_first,
               const RowDescriptor& row_desc);

    ~HeapSorter() override = default;

    void init_profile(RuntimeProfile* runtime_profile) override {
        _topn_filter_timer = ADD_TIMER(runtime_profile, "TopNFilterTime");
        _topn_filter_rows_counter = ADD_COUNTER(runtime_profile, "TopNFilterRows", TUnit::UNIT);
        _materialize_timer = ADD_TIMER(runtime_profile, "MaterializeTime");
    }

    Status append_block(Block* block) override;

    Status prepare_for_read() override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    size_t data_size() const override;

    Field get_top_value() override;

    static constexpr size_t HEAP_SORT_THRESHOLD = 1024;

private:
    void _do_filter(HeapSortCursorBlockView& block_view, size_t num_rows);

    Status _prepare_sort_descs(Block* block);

    size_t _data_size;
    size_t _heap_size;
    std::unique_ptr<SortingHeap> _heap;
    Block _return_block;
    int64_t _topn_filter_rows;
    bool _init_sort_descs;

    RuntimeProfile::Counter* _topn_filter_timer = nullptr;
    RuntimeProfile::Counter* _topn_filter_rows_counter = nullptr;
    RuntimeProfile::Counter* _materialize_timer = nullptr;
};

} // namespace doris::vectorized
