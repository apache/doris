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
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterators.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/schema.h"

namespace doris {

struct RowsetReaderContext;

// ORDER BY key LIMIT n pushed down to the storage layer, for the reads without a merge
// (DUP_KEYS, merge-on-write UNIQUE_KEYS): every rowset is read in key order for at most n rows,
// and the best n rows over all rowsets come out as one Block. SegmentIterator has already
// applied all pushed filters and its per-segment row budget; this only keeps the best local rows
// across rowsets. Whenever the n-th best row changes, its leading sort column value feeds the
// TopN runtime predicate (topn_filter_source_node_ids), so the scans of the other tablets can
// skip rows that cannot make it into the result.
class TopNIterator final : public RowwiseIterator {
public:
    // `context` holds the sort columns, the limit and the runtime predicate ids. Every rowset
    // reader of `rs_splits` is initialized with it when the read reaches its rowset.
    TopNIterator(std::vector<RowSetSplits> rs_splits, RowsetReaderContext* context,
                 ReadSchemaSPtr read_schema);

    // The first call reads every rowset and fills `block` with the result rows; the next one
    // returns EndOfFile.
    Status next_batch(Block* block) override;

    const ReadSchema& schema() const override { return *_read_schema; }

private:
    class BlockRowPosComparator {
    public:
        BlockRowPosComparator(MutableBlock* mutable_block,
                              const std::vector<uint32_t>* compare_columns, bool is_reverse)
                : _mutable_block(mutable_block),
                  _compare_columns(compare_columns),
                  _is_reverse(is_reverse) {}

        bool operator()(const size_t& lpos, const size_t& rpos) const;

    private:
        const MutableBlock* _mutable_block = nullptr;
        const std::vector<uint32_t>* _compare_columns;
        // reverse the compare order
        const bool _is_reverse = false;
    };

    std::vector<RowSetSplits> _rs_splits;
    RowsetReaderContext* const _context;
    const ReadSchemaSPtr _read_schema;
    const size_t _topn_limit;
    // reverse the compare order
    const bool _is_reverse;
    bool _topn_eof = false;
};

} // namespace doris
