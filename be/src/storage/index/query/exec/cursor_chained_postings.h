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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/query/exec/chained_conjunction.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::index_query {

// The first index at or after `from` whose value `before` rejects, found by galloping from
// `from`; `before` accepts a prefix of the ascending values.
template <typename Before>
size_t gallop_past(std::span<const uint32_t> values, size_t from, Before before) {
    size_t low = from;
    size_t probe = from;
    size_t step = 1;
    while (probe < values.size() && before(values[probe])) {
        low = probe + 1;
        probe = low + step;
        step <<= 1;
    }
    const auto high = values.begin() + std::min(probe, values.size());
    return std::partition_point(values.begin() + low, high, before) - values.begin();
}

// Calls `visit(block)` for every block the cursor lists from its position, in order.
template <typename Visit>
Status for_each_block(PostingsCursor& cursor, Visit&& visit) {
    PostingsBlock block;
    bool eof = false;
    while (true) {
        RETURN_IF_ERROR(cursor.next_block(&block, &eof));
        if (eof) {
            return Status::OK();
        }
        RETURN_IF_ERROR(visit(block));
    }
}

// Calls `visit(block, slice)` for every block of the cursor that can hold one of the
// ascending `candidates`, with the candidates between its first and last document. Only those
// blocks are decoded.
template <typename Visit>
Status for_each_candidate_block(PostingsCursor& cursor, std::span<const uint32_t> candidates,
                                Visit&& visit) {
    PostingsBlock block;
    bool eof = false;
    size_t next = 0;
    while (next < candidates.size()) {
        RETURN_IF_ERROR(cursor.seek_block(candidates[next], &block, &eof));
        if (eof) {
            return Status::OK();
        }
        const uint32_t first = block.doc_at(0);
        const uint32_t last = block.doc_at(block.size() - 1);
        const size_t begin =
                gallop_past(candidates, next, [first](uint32_t doc) { return doc < first; });
        next = gallop_past(candidates, begin, [last](uint32_t doc) { return doc <= last; });
        if (next > begin) {
            RETURN_IF_ERROR(visit(block, candidates.subspan(begin, next - begin)));
        }
    }
    return Status::OK();
}

// Appends the ordinals in `docs` of documents also in `candidates`. Both lists are strictly
// ascending, and candidates lie between the first and last document of the block.
void intersect_block_ordinals(std::span<const uint32_t> docs, std::span<const uint32_t> candidates,
                              std::vector<uint32_t>* ordinals);

// Ordinals retained from partial intersections, grouped by their postings blocks.
struct SelectedPostings {
    struct Block {
        uint32_t last_doc;
        size_t end;
    };
    std::vector<uint32_t> ordinals;
    std::vector<Block> blocks;
};

// A term of the chained conjunction read through its postings cursor: the start prefetches
// the candidates' blocks and the listing walks only those.
class CursorChainedPostings final : public ChainedPostings {
public:
    explicit CursorChainedPostings(PostingsCursor& cursor, SelectedPostings* selected = nullptr)
            : _cursor(cursor), _selected(selected) {}

    uint64_t doc_freq() const override { return _cursor.doc_freq(); }
    Status start(const std::vector<uint32_t>* candidates) override;
    Status collect(std::vector<uint32_t>* out) override;

private:
    PostingsCursor& _cursor;
    const std::vector<uint32_t>* _candidates = nullptr;
    // Partial sparse intersections retain their ordinals for a later position read.
    SelectedPostings* _selected = nullptr;
};

// The rows every one of `cursors` holds, among `candidates` when given, listed as a chain that
// narrows the cheapest cursor's rows by the others.
Status chain_cursors(std::span<const std::unique_ptr<PostingsCursor>> cursors,
                     const std::vector<uint32_t>* candidates, std::vector<uint32_t>* rows);

} // namespace doris::index_query
