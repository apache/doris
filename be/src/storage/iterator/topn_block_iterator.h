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
#include <set>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterator/tablet_block_iterator.h"
#include "storage/iterators.h"

namespace doris {

class Field;
struct RowsetReaderContext;

// ORDER BY key LIMIT n pushed down to the storage layer, for the table models read without a
// merge (DUP_KEYS, merge-on-write UNIQUE_KEYS): every rowset is read in key order for at most
// n rows, and the best n rows over all rowsets are the result, handed out as one Block.
//
// The rowsets are read one after another; after each one, the current n-th best row feeds the
// TopN runtime predicate (topn_filter_source_node_ids), so the scans of the other tablets
// can skip rows that cannot make it into the result.
class TopNBlockIterator final : public TabletBlockIterator {
public:
    // `rowset_iters` yields the rows of every rowset in key order (descending when the merge
    // decision says reverse), in the order the rowsets are read; each is initialized with
    // `init_options` when it is reached. `context` holds the sort columns (the compare
    // ordinals of its merge decision), the limit and the runtime predicate ids.
    TopNBlockIterator(std::vector<RowwiseIteratorUPtr>* rowset_iters,
                      const StorageReadOptions* init_options, const RowsetReaderContext* context);

    Status init() override { return Status::OK(); }

    // The first call reads every rowset and fills `block` with the result rows; the second
    // call reports eof.
    Status next_block(Block* block, bool* eof) override;

private:
    // Orders the row positions of the candidate Block by the sort columns.
    class RowPosComparator {
    public:
        RowPosComparator(const MutableBlock* candidates, const std::vector<uint32_t>* sort_columns,
                         bool is_reverse)
                : _candidates(candidates), _sort_columns(sort_columns), _is_reverse(is_reverse) {}

        bool operator()(const size_t& lpos, const size_t& rpos) const;

    private:
        const MutableBlock* _candidates;
        const std::vector<uint32_t>* _sort_columns;
        const bool _is_reverse;
    };

    // The best `limit` rows seen so far: a Block of the read schema and the positions of
    // those rows in it, ordered by the sort columns with the worst one last.
    class Candidates {
    public:
        Candidates(Block empty_block, const std::vector<uint32_t>* sort_columns, bool is_reverse,
                   size_t limit);

        // Take in the rows of `block`, which is sorted like the candidates: the leading rows
        // that fill up the candidates or beat the worst one. `*kept` is how many were taken.
        Status offer(const Block& block, size_t* kept);

        bool full() const { return _sorted_row_pos.size() >= _limit; }

        // The value of column `ordinal` in the worst candidate; only when full().
        void worst_value(size_t ordinal, Field* value) const;

        Block take() { return _rows.to_block(); }

    private:
        size_t _rows_to_copy(const Block& block) const;
        // Drop the candidates beyond `_limit`, and rebuild the Block from the kept rows once
        // it holds more dropped rows than candidates.
        void _trim();

        const std::vector<uint32_t>* _sort_columns;
        const bool _is_reverse;
        const size_t _limit;
        MutableBlock _rows;
        std::multiset<size_t, RowPosComparator> _sorted_row_pos;
    };

    Status _collect(Block* block);
    // Read at most `limit` rows of `rowset_iter` into `block`, offering each batch to the
    // candidates.
    Status _collect_rowset(RowwiseIterator& rowset_iter, Block* block, Candidates* candidates);
    Status _update_runtime_predicate(const Candidates& candidates);

    std::vector<RowwiseIteratorUPtr>* _rowset_iters;
    const StorageReadOptions* _init_options;
    const RowsetReaderContext* _context;
    bool _done = false;
};

} // namespace doris
