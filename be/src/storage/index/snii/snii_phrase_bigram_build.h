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
#include <functional>
#include <string_view>
#include <vector>

// Phrase-bigram emission for the SNII index writer, extracted here as a
// dependency-free (no Doris/CLucene types) template so it can be unit tested in
// isolation. The writer feeds it the row's indexable analyzer tokens and turns
// every emitted pair into a hidden `make_phrase_bigram_term` posting.
namespace doris::segment_v2 {

// One analyzer token positioned within a row, for phrase-bigram emission. `term`
// is a NON-owning view into the caller's token storage (the row's TermInfo
// vector); it is valid only for the duration of the emit call.
struct PhrasePositionedTerm {
    std::string_view term;
    uint32_t position = 0;
};

// G05 id-keyed variant: the token is carried as the SPIMI unigram TERM-ID the
// writer captured from add_token_returning_id when it interned the token, so
// the emitted pair feeds SpimiTermBuffer::add_bigram_token(left_id, right_id,
// ...) -- no term bytes flow through the per-pair hot path at all. Unigram ids
// are stable for the buffer's lifetime (only hidden bigram terms are ever
// evicted/recycled), so holding them across the row is safe. The member is
// deliberately named `term` too: emit_adjacent_phrase_bigrams below is generic
// over the element type and forwards `.term` / `.position` unchanged.
struct PhrasePositionedTermId {
    uint32_t term = 0;
    uint32_t position = 0;
};

// Emits every adjacent phrase-bigram pair (left@p, right@p+1) drawn from `terms`.
//
// Contract: `terms` is expected to be ordered by ascending position. Analyzer
// output already satisfies this -- token position is monotonic non-decreasing
// (analyzer.cpp advances `position += getPositionIncrement()` with increment >=
// 0) and the per-array `position_base` is a uniform constant offset -- so the
// guard below is a COLD PATH that only fires when the invariant is violated
// (e.g. a hand-shuffled unit-test input). The return value reports whether the
// guard sorted: the writer asserts `!did_sort` to document the invariant, while
// an out-of-order caller still gets the correct pair set (the guard sorts first).
//
// The pre-refactor sort used a secondary term key; it is intentionally DROPPED.
// Only the SET of emitted (left, right, position) triples is defined -- the
// order of `emit` calls within a position group is unspecified. That is exactly
// what the downstream SpimiTermBuffer needs: it dedups per term and re-sorts on
// finish, so emission order never reaches the on-disk posting bytes.
//
// Generic over the positioned-term element `PT` (PhrasePositionedTerm or
// PhrasePositionedTermId -- anything with `.term` and a uint32_t `.position`).
// `emit` has signature void(decltype(PT::term) left, decltype(PT::term) right,
// uint32_t position); `position` is the left token's position.
template <class PT, class Emit>
bool emit_adjacent_phrase_bigrams(std::vector<PT>& terms, Emit&& emit) {
    bool did_sort = false;
    if (!std::ranges::is_sorted(terms, {}, &PT::position)) {
        std::ranges::sort(terms, {}, &PT::position);
        did_sort = true;
    }

    size_t left_begin = 0;
    while (left_begin < terms.size()) {
        size_t left_end = left_begin + 1;
        while (left_end < terms.size() && terms[left_end].position == terms[left_begin].position) {
            ++left_end;
        }

        size_t right_begin = left_end;
        while (right_begin < terms.size() &&
               terms[right_begin].position <= terms[left_begin].position) {
            ++right_begin;
        }
        if (right_begin == terms.size() ||
            terms[right_begin].position != terms[left_begin].position + 1) {
            left_begin = left_end;
            continue;
        }
        size_t right_end = right_begin + 1;
        while (right_end < terms.size() &&
               terms[right_end].position == terms[right_begin].position) {
            ++right_end;
        }

        for (size_t l = left_begin; l < left_end; ++l) {
            for (size_t r = right_begin; r < right_end; ++r) {
                emit(terms[l].term, terms[r].term, terms[l].position);
            }
        }
        left_begin = left_end;
    }
    return did_sort;
}

} // namespace doris::segment_v2
