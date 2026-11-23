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
#include <functional>
#include <map>
#include <set>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "storage/iterator/block_iterator.h"
#include "storage/iterators.h"

namespace doris {

// Returns the first `limit` rows by the sort columns, for a query that pushed ORDER BY a key prefix
// LIMIT down to storage. The rowsets are read one after another, each as streams of rows that come
// sorted, and at most `limit` rows of a stream, as its later rows cannot sort before those. It
// keeps the rows that sort before the last of the first `limit` rows so far, and tightens the TopN
// filter of the query to that last row. For example, with `limit` 2 and the streams 1 4 7 and
// 2 3 9, it keeps 1 4 and tightens the filter to 4, then keeps 1 2 and tightens it to 2.
class TopNBlockIterator final : public BlockIterator {
public:
    // Returns the rows of one rowset as streams that each come sorted by the sort columns, such as
    // one per segment when its segments overlap on keys; they need no merge, as the rows kept are
    // the first ones across all streams. It is called when the read gets to the rowset, so that its
    // segments are set up with the filter the streams before tightened.
    using RowsetOpener = std::function<Status(std::vector<RowwiseIteratorUPtr>* streams)>;
    // Takes the first sort column of the last of the first `limit` rows each time that row changes.
    using FilterUpdater = std::function<Status(const Field& value)>;

    // `rowsets` come in the order to read them. `sort_columns` are positions in the read Blocks,
    // compared in turn, ascending with NULLs first, or descending with NULLs last. A virtual column
    // of the read Blocks has a column of its type in `virtual_column_types`, by position.
    // `update_filter` may be empty.
    TopNBlockIterator(std::vector<RowsetOpener> rowsets, size_t limit,
                      std::vector<uint32_t> sort_columns, bool descending,
                      std::map<uint32_t, DataTypePtr> virtual_column_types,
                      FilterUpdater update_filter);

    // The first call reads every rowset into `block`, an empty read Block, and then fills it with
    // the first `limit` rows, sorted, or all rows if there are fewer. `*eof` when there are none,
    // and on every later call.
    Status next_block(Block* block, bool* eof) override;

private:
    // Read the rows of one stream into `block`, and keep the ones among the first `limit` so far.
    Status _read_stream(RowwiseIterator& stream, Block* block);
    // Copy the rows of `block`, which are sorted, that are among the first `limit` so far.
    Status _keep_top_rows(const Block& block);
    // How many rows from the start of `block`, which are sorted, are among the first `limit` so
    // far.
    size_t _count_top_rows(const Block& block) const;
    // Whether row `row` of `block` sorts before row `top_row` of `_rows`.
    bool _sorts_before(const Block& block, size_t row, size_t top_row) const;
    // Drop the rows of `_rows` that are not in `_top_rows`; `_rows` then holds them in order.
    void _remove_dropped_rows();

    // The order of `_top_rows`, by the sort columns of the rows of `_rows`.
    struct RowOrder {
        const TopNBlockIterator* topn;
        bool operator()(size_t lhs, size_t rhs) const;
    };

    const std::vector<RowsetOpener> _rowsets;
    const size_t _limit;
    const std::vector<uint32_t> _sort_columns;
    const bool _descending;
    const std::map<uint32_t, DataTypePtr> _virtual_column_types;
    const FilterUpdater _update_filter;

    // The rows copied from the rowsets: the ones in `_top_rows`, and the ones dropped from it
    // since the last _remove_dropped_rows().
    MutableBlock _rows;
    // The positions in `_rows` of the first `limit` rows so far, in sort order.
    std::multiset<size_t, RowOrder> _top_rows {RowOrder {this}};
    bool _done = false;
};

} // namespace doris
