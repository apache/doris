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

// `<CLucene.h>` (rather than the narrower `<CLucene/analysis/AnalysisHeader.h>`)
// is required so the CL_NS_DEF / TCHAR / CLUCENE_INLINE_EXPORT macros set up
// in `_SharedHeader.h` are defined before the analysis header types appear.
// Including only `AnalysisHeader.h` works in TUs that already pulled
// `<CLucene.h>` transitively, but breaks self-contained tests.
#include <CLucene.h> // IWYU pragma: keep

#include <cstdint>
#include <string_view>

#include "storage/index/inverted/spimi/posting_buffer.h"

namespace doris::segment_v2::inverted_index::spimi {

// CLucene `TokenStream` wrapper that observes every token an upstream
// stream produces and taps a copy into a `SpimiPostingBuffer`. Used by
// `InvertedIndexColumnWriter` to capture the EXACT token sequence
// CLucene's `addDocument` consumes — re-tokenising after the fact
// produced subtly different tokens (different position-increment
// handling, filter pipeline state, ...), so the tee approach is what
// gets the SPIMI shadow segment byte-equal to CLucene's primary segment.
//
// Lifetime: the tee does NOT own the upstream stream (the analyser
// manages it via reusableTokenStream) and does NOT own the buffer (the
// integration layer owns it). Both pointers must outlive the tee.
class TeeTokenStream final : public lucene::analysis::TokenStream {
public:
    TeeTokenStream() = default;
    ~TeeTokenStream() override = default;

    TeeTokenStream(const TeeTokenStream&) = delete;
    TeeTokenStream& operator=(const TeeTokenStream&) = delete;

    // Per-doc configuration. Call before attaching to a Field. `upstream`
    // is the reusable stream the analyser handed back for this value;
    // `buffer` is the SPIMI accumulator (non-owning); `doc_id` is the row
    // id this value belongs to.
    //
    // Configure() is the *only* per-doc position reset. `reset()`
    // intentionally does NOT touch `_pos` — CLucene's reusable-stream
    // protocol calls `reset()` between filter passes within one doc, and
    // zeroing `_pos` there would let two overlapping passes record the
    // same position twice (breaking phrase recall for CJK + n-gram
    // chains in particular). See SPIMI_DESIGN.md § 9 for the open
    // question on resetting analyzer chains.
    void Configure(lucene::analysis::TokenStream* upstream, SpimiPostingBuffer* buffer,
                   uint32_t doc_id) {
        _upstream = upstream;
        _buffer = buffer;
        _doc_id = doc_id;
        _pos = -1;
        _first_token = true;
    }

    lucene::analysis::Token* next(lucene::analysis::Token* token) override {
        if (_upstream == nullptr) {
            return nullptr;
        }
        lucene::analysis::Token* t = _upstream->next(token);
        if (t != nullptr && _buffer != nullptr) {
            _pos += t->getPositionIncrement();
            // H-sec-5 — `_pos` starts at -1 so the first token with the
            // canonical increment of 1 lands at position 0. Some analyzers
            // can emit a synonym overlay as the very first token with
            // increment = 0, which would leave `_pos` at -1; the unsigned
            // cast below would then record position 0xFFFFFFFF and the
            // next genuine token's prox delta would wrap negative on the
            // wire. Clamp only on the first token, matching CLucene's
            // `DocumentsWriter` which normalises the first position to 0.
            // After the first token the invariant `_pos >= 0` is
            // maintained by `getPositionIncrement()` always returning
            // non-negative.
            if (_first_token) {
                if (_pos < 0) {
                    _pos = 0;
                }
                _first_token = false;
            }
            const char* term_buf = t->template termBuffer<char>();
            const size_t term_len = t->template termLength<char>();
            if (term_len > 0 && term_buf != nullptr) {
                _buffer->Append(std::string_view(term_buf, term_len), _doc_id,
                                static_cast<uint32_t>(_pos));
            }
        }
        return t;
    }

    void close() override {
        if (_upstream != nullptr) {
            _upstream->close();
        }
    }

    void reset() override {
        // Intentionally NOT zeroing `_pos` here — see the comment on
        // Configure() above. Per-doc resets must go through Configure().
        if (_upstream != nullptr) {
            _upstream->reset();
        }
    }

private:
    lucene::analysis::TokenStream* _upstream = nullptr;
    SpimiPostingBuffer* _buffer = nullptr;
    uint32_t _doc_id = 0;
    int32_t _pos = -1;
    bool _first_token = true;
};

} // namespace doris::segment_v2::inverted_index::spimi
