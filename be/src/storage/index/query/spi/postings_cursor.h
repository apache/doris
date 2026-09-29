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
    // A hint that only the ascending `candidates` will be asked for, with their positions when
    // `positions`: an adapter may read what they need in one round. Ignored by default.
    virtual Status prefetch(const std::vector<uint32_t>& candidates, bool positions) {
        (void)candidates;
        (void)positions;
        return Status::OK();
    }
    // Ordinals advance within the decoded block; each document is opened at most once.
    virtual Status open_positions(uint32_t ordinal, PositionCursor** out) {
        *out = nullptr;
        return Status::NotSupported("This posting type does not support positions");
    }
};

} // namespace doris::index_query
