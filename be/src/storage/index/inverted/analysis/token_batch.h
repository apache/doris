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
#include <limits>
#include <span>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "core/arena.h"
#include "storage/index/inverted/analysis/scratch.h"

namespace doris::segment_v2::inverted_index {

// One token: a view of its term, and how many positions it is after the token before it.
struct TokenRef {
    const char* data = nullptr;
    uint32_t size = 0;
    int32_t position_increment = 0; // >= 0

    std::string_view term() const { return {data, size}; }
};

// The tokens a stream hands over in one TextTokenStream::next_batch() call. A term is a view: of
// the value being analyzed, of memory the stream keeps, or of the batch's own term storage, which
// push_copy() and allocate_term() fill. Term storage is an arena, so growing it never moves a term
// handed out before, and clear() drops all of them at once.
//
// The consumer owns the batch and keeps it from value to value. Between two values it calls
// release_oversized(): the stream cannot, and one huge value must not pin its memory until the
// segment ends.
class TokenBatch {
public:
    // Drops the tokens and the terms in term storage. The token array and the arena's largest
    // block stay, so the next batch of a steady stream allocates nothing.
    void clear() {
        _tokens.clear();
        _terms.clear();
    }

    size_t size() const { return _tokens.size(); }
    std::span<TokenRef> tokens() { return _tokens; }
    std::span<const TokenRef> tokens() const { return _tokens; }

    // Appends a token whose term stays where it is; whoever owns the term keeps it valid while the
    // batch holds the token.
    void push(std::string_view term, int32_t position_increment) {
        DCHECK_GE(position_increment, 0);
        DCHECK_LE(term.size(), std::numeric_limits<uint32_t>::max());
        _tokens.push_back({.data = term.data(),
                           .size = static_cast<uint32_t>(term.size()),
                           .position_increment = position_increment});
    }

    // Appends a token with a copy of `term` in term storage.
    void push_copy(std::string_view term, int32_t position_increment) {
        char* copy = allocate_term(term.size());
        term.copy(copy, term.size());
        push({copy, term.size()}, position_increment);
    }

    // `size` bytes of term storage, for a term built in place. Valid until the next clear().
    char* allocate_term(size_t size) { return _terms.alloc(size); }

    // The bytes the batch keeps across clear(), for the writer's memory reporter.
    size_t capacity_bytes() const { return _tokens.capacity() * sizeof(TokenRef) + _terms.size(); }

    // Between two values, on an empty batch: when the batch keeps more than the high water mark,
    // which only a value with a very large batch or very long terms leaves behind, it releases all
    // of it. An ordinary batch keeps a 4 KiB token array and a 4 KiB arena block, and keeps them.
    void release_oversized() {
        DCHECK_EQ(_tokens.size(), 0);
        if (capacity_bytes() > ANALYZER_SCRATCH_HIGH_WATER_BYTES) {
            std::vector<TokenRef>().swap(_tokens);
            _terms.clear(/*delete_head=*/true);
        }
    }

private:
    std::vector<TokenRef> _tokens;
    Arena _terms;
};

} // namespace doris::segment_v2::inverted_index
