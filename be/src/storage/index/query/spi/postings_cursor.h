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

#include <cstdint>
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/query/spi/position_cursor.h"

namespace doris::index_query {

// Views remain valid until the next operation that changes the cursor's block.
struct PostingsBlock {
    std::span<const uint32_t> docs;
    std::span<const uint32_t> freqs;
    std::span<const uint32_t> norms;
    uint32_t range_begin = 0;
    uint64_t range_end = 0;
    bool dense = false;

    uint64_t size() const { return dense ? range_end - range_begin : docs.size(); }
    uint32_t doc_at(uint64_t ordinal) const {
        return dense ? static_cast<uint32_t>(range_begin + ordinal) : docs[ordinal];
    }
    uint32_t freq_at(uint64_t ordinal) const { return freqs.empty() ? 1 : freqs[ordinal]; }
    uint32_t norm_at(uint64_t ordinal) const { return norms.empty() ? 1 : norms[ordinal]; }

    PostingsBlock suffix(uint64_t ordinal) const {
        auto result = *this;
        if (dense) {
            result.range_begin = static_cast<uint32_t>(range_begin + ordinal);
        } else {
            result.docs = docs.subspan(ordinal);
        }
        if (!freqs.empty()) {
            result.freqs = freqs.subspan(ordinal);
        }
        if (!norms.empty()) {
            result.norms = norms.subspan(ordinal);
        }
        return result;
    }
};

// The positions of chosen documents of a block. The i-th holds flat[offsets[k], offsets[k + 1]),
// where k is i, or its ordinal in the block when `by_ordinal`: an adapter hands over the positions
// of the whole block that way rather than copying the chosen ones out.
struct BlockPositions {
    std::span<const uint32_t> flat;
    std::span<const uint32_t> offsets;
    bool by_ordinal = false;

    // The positions of the i-th of the chosen `ordinals`.
    std::span<const uint32_t> of(size_t i, std::span<const uint32_t> ordinals) const {
        const size_t k = by_ordinal ? ordinals[i] : i;
        return flat.subspan(offsets[k], offsets[k + 1] - offsets[k]);
    }
};

// The buffers an adapter copying positions fills; a caller keeps them across blocks.
struct PositionsBuffer {
    std::vector<uint32_t> flat;
    std::vector<uint32_t> offsets;
};

struct BlockBound {
    uint32_t last_doc = 0;
    int32_t max_freq = -1;
    int32_t max_norm = -1;
    bool last_doc_known = false;
};

class PostingsCursor {
public:
    virtual ~PostingsCursor() = default;
    virtual uint32_t doc_freq() const = 0;
    // Successful reads return a nonempty block, or eof with an empty block.
    virtual Status next_block(PostingsBlock* block, bool* eof) = 0;
    // May return a block beginning before target; blocks remain forward-only.
    virtual Status seek_block(uint32_t target, PostingsBlock* block, bool* eof) = 0;
    // Moves only the skip cursor. moved invalidates any previously returned block.
    virtual Status shallow_seek(uint32_t target, bool* moved) = 0;
    virtual BlockBound current_block_bound() const = 0;
    // A hint that only the ascending `candidates` (every document when null) will be asked
    // for, with their positions when `positions`: an adapter may read what they need in one
    // round. Ignored by default.
    virtual Status prefetch(const std::vector<uint32_t>* candidates, bool positions) {
        (void)candidates;
        (void)positions;
        return Status::OK();
    }
    // Restarts at the first block with the same block boundaries and document order.
    // Only an adapter that keeps its bytes supports it.
    virtual Status rewind() { return Status::NotSupported("this posting type cannot rewind"); }
    // The mean work of decoding one document's positions, in positions, over the blocks whose
    // positions were read; 0 when the adapter cannot tell.
    virtual Status positions_per_doc(uint64_t* out) {
        *out = 0;
        return Status::OK();
    }
    // The current block's documents at the strictly ascending `ordinals` are opened through
    // open_positions in that order and each finished: an adapter may decode each document's
    // positions as they are read instead of the block's, and check what it skipped once the last
    // is finished. Ignored by default.
    virtual Status stream_positions(std::span<const uint32_t> ordinals) {
        (void)ordinals;
        return Status::OK();
    }
    // Ordinals advance within the decoded block; each document is opened at most once.
    virtual Status open_positions(uint32_t ordinal, PositionCursor** out) {
        *out = nullptr;
        return Status::NotSupported("This posting type does not support positions");
    }
    // Opens the document and reads its first chunk in one call, which an adapter may fuse.
    // On success, a null cursor means all positions were returned and no finishing remains.
    virtual Status open_position_stream(uint32_t ordinal, std::span<uint32_t> first_chunk,
                                        size_t* count, PositionCursor** out) {
        *count = 0;
        RETURN_IF_ERROR(open_positions(ordinal, out));
        return (*out)->next_positions(first_chunk, count);
    }
    // The positions of the document at `ordinal`, each plus `offset`, appended to `output`:
    // the open and the drain as one call, which an adapter may fuse.
    virtual Status append_positions(uint32_t ordinal, uint32_t offset,
                                    std::vector<uint32_t>& output) {
        PositionCursor* positions = nullptr;
        RETURN_IF_ERROR(open_positions(ordinal, &positions));
        return positions->append_remaining_positions(offset, output);
    }
    // The positions of the current block's documents at the strictly ascending `ordinals`,
    // valid until the next block or call. An adapter may decode only those documents; by
    // default each is read through append_positions into `buffer`.
    virtual Status block_positions(std::span<const uint32_t> ordinals, PositionsBuffer* buffer,
                                   BlockPositions* out) {
        buffer->flat.clear();
        buffer->offsets.assign(1, 0);
        for (const uint32_t ordinal : ordinals) {
            RETURN_IF_ERROR(append_positions(ordinal, 0, buffer->flat));
            buffer->offsets.push_back(static_cast<uint32_t>(buffer->flat.size()));
        }
        *out = {.flat = buffer->flat, .offsets = buffer->offsets};
        return Status::OK();
    }
};

} // namespace doris::index_query
