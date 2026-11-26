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

#include <cstddef>
#include <cstdint>
#include <queue>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "storage/iterator/block_batch_limit.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

enum class HeapSortOrder : uint8_t {
    ASC,  // small to large (0 -> N)
    DESC, // large to small (N -> 0)
};

struct HeapSortCol {
    uint32_t col_idx;
    HeapSortOrder order;
};

// Sort order of VHeapMergeIterator. Full-key merges (with an optional secondary column when keys
// are equal) and explicit sort-column merges are mutually exclusive, enforced by the factory
// methods below and compiled into a flat `HeapSortCol` list once `num_key_columns` is known.
class HeapMergeOrder {
public:
    // Default: compare all leading key columns ascending, newer child (larger index) first.
    HeapMergeOrder() = default;

    static HeapMergeOrder by_all_keys(HeapSortOrder child_iterator_order = HeapSortOrder::DESC);

    static HeapMergeOrder by_all_keys_and_same_key_col(
            int32_t same_key_sort_col_idx, HeapSortOrder same_key_sort_order,
            HeapSortOrder child_iterator_order = HeapSortOrder::DESC);

    static HeapMergeOrder by_sort_cols(std::vector<uint32_t> sort_col_idxs,
                                       HeapSortOrder sort_order,
                                       HeapSortOrder child_iterator_order = HeapSortOrder::DESC);

    bool uses_all_keys() const { return _sort_col_idxs.empty(); }
    HeapSortOrder child_iterator_order() const { return _child_iterator_order; }
    std::vector<HeapSortCol> resolve_sort_cols(size_t num_key_columns) const;

private:
    int32_t _same_key_sort_col_idx = -1;
    HeapSortOrder _same_key_sort_order = HeapSortOrder::DESC;
    HeapSortOrder _child_iterator_order = HeapSortOrder::DESC;
    std::vector<uint32_t> _sort_col_idxs;
    HeapSortOrder _key_sort_order = HeapSortOrder::ASC;
};

// Merges key-ordered children into one ordered stream. An output Block may end inside the rows of
// a key, and a child's Block that sorts wholly before the other children is passed on whole
// instead of copied. It does not init() the children.
class VHeapMergeIterator final : public RowwiseIterator {
public:
    VHeapMergeIterator(std::vector<RowwiseIteratorUPtr> children, ReadSchemaSPtr read_schema,
                       HeapMergeOrder order = {});

    ~VHeapMergeIterator() override = default;

    // Reads the first Block of every child. `opts` supplies the row limit and the byte budget
    // of an output Block, and the runtime state.
    Status init(const StorageReadOptions& opts) override;

    // Fill `block`, which must be empty, with at least one row and return OK, or return EndOfFile
    // once every row has been read. `block` may get the columns of a child's Block.
    Status next_batch(Block* block) override;

    const ReadSchema& schema() const override { return *_read_schema; }

    void update_profile(RuntimeProfile* profile) override;

private:
    // One input of the merge and the Block it is positioned in.
    struct Child {
        RowwiseIteratorUPtr iter;
        // The current Block, refilled in place.
        Block block;
        // The current row of `block`.
        size_t pos = 0;
    };

    // Read the next Block of `child` that has rows; `*eof` once the child has no more rows.
    Status _read_next_block(Child& child, bool* eof);
    // Push child `child` back into the heap. When its Block is used up, read its next Block
    // first, and release the child instead once it has no more rows.
    Status _push_back_to_heap(uint32_t child);

    // Whether row `row` of child `child` sorts before the current row of child `other`; both are
    // indexes into `_children`.
    bool _less_than_current_row(uint32_t child, size_t row, uint32_t other) const;
    // How many rows of child `child`, which is out of the heap, from its current row on sort
    // before the current row of the next child, `limit` at most. The heap must not be empty.
    size_t _rows_before_next_child(uint32_t child, size_t limit) const;

    // The order of `_heap`. std::priority_queue keeps its largest element on top, so `lhs` is
    // "less" than `rhs` when the current row of `rhs` sorts first.
    struct HeapOrder {
        const VHeapMergeIterator* merge;
        bool operator()(uint32_t lhs, uint32_t rhs) const {
            return merge->_less_than_current_row(rhs, merge->_children[rhs].pos, lhs);
        }
    };

    // The row limit of the next output Block, see the class comment.
    size_t _output_row_limit() const;
    // Copy `rows` rows of `child`, from its current row on, to the output `columns`.
    static void _copy_rows(MutableColumns& columns, const Child& child, size_t rows);
    // Give the empty output `columns` the rows of `child`'s Block without copying them: they swap
    // columns, and the child reads its next Block into the empty ones.
    static void _move_block_to_output(MutableColumns& columns, Child& child);

    std::vector<Child> _children;
    const ReadSchemaSPtr _read_schema;
    const std::vector<HeapSortCol> _sort_cols;
    const HeapSortOrder _child_iterator_order;

    // From init():
    BlockBatchLimit _batch_limit;
    RuntimeState* _runtime_state = nullptr;
    // Where the time of reading the children's Blocks goes (block_fetch_ns).
    OlapReaderStatistics* _stats = nullptr;

    // The children that still have rows; the one whose current row goes first is on top.
    std::priority_queue<uint32_t, std::vector<uint32_t>, HeapOrder> _heap {HeapOrder {this}};
};

} // namespace doris
