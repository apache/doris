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
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "storage/index/query/phrase/exact_phrase_matcher.h"
#include "storage/index/query/phrase/exact_phrase_stream_matcher.h"
#include "storage/index/query/phrase/position_span.h"
#include "storage/index/query/phrase/position_stream.h"
#include "storage/index/query/phrase/sloppy_phrase_matcher.h"

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris::index_query {

// How a phrase query matches on every index format. The tokens may be `slop` moves apart, kept in
// order when `ordered`, and with `candidates` only those documents are verified and can match.
struct PhraseQueryOptions {
    uint32_t slop = 0;
    bool ordered = false;
    const roaring::Roaring* candidates = nullptr;
    // Set once the phrase reaches `candidates`: every slot holds a term, so its rows depend on
    // them. A phrase that stops before is empty for the whole segment.
    bool* candidate_rows_consumed = nullptr;
};

// Decides whether one document holds a phrase and how often, for every index format. Clause i
// reads the positions of distinct term clause_terms[i] and sits offsets[i] - offsets[0] after the
// phrase start; a term the phrase repeats is read once per document.
class PhraseVerifier {
public:
    // The clause costs choose the adjacent pair an exact phrase of three or more clauses checks
    // before it reads the other clauses.
    PhraseVerifier(std::vector<size_t> clause_terms, std::span<const uint32_t> offsets,
                   std::span<const uint64_t> clause_costs, uint32_t slop, bool ordered)
            : clause_terms_(std::move(clause_terms)) {
        DORIS_CHECK(!clause_terms_.empty());
        DORIS_CHECK_EQ(offsets.size(), clause_terms_.size());
        for (uint32_t offset : offsets) {
            DORIS_CHECK_GE(offset, offsets.front());
            offsets_.push_back(offset - offsets.front());
        }
        const size_t term_count = *std::ranges::max_element(clause_terms_) + 1;
        can_stream_ = slop == 0 && clause_terms_.size() > 1 && term_count == clause_terms_.size() &&
                      std::ranges::adjacent_find(offsets_, std::greater_equal {}) == offsets_.end();
        if (slop > 0 || clause_terms_.size() > 2) {
            state_ = std::make_unique<MatcherState>();
            state_->term_positions.resize(term_count);
            state_->loaded_epoch.resize(term_count);
            if (slop > 0) {
                state_->clause_spans.resize(clause_terms_.size());
                state_->sloppy.emplace(clause_terms_, offsets_, slop, ordered);
            } else {
                DORIS_CHECK_EQ(clause_costs.size(), clause_terms_.size());
                state_->exact.emplace(offsets_, select_phrase_verification_pair(
                                                        clause_costs.size(), [&](size_t clause) {
                                                            return clause_costs[clause];
                                                        }));
            }
        }
    }

    // The exact matcher views offsets_, so a copy would view the original's offsets.
    PhraseVerifier(const PhraseVerifier&) = delete;
    PhraseVerifier& operator=(const PhraseVerifier&) = delete;
    PhraseVerifier(PhraseVerifier&&) = default;

    bool can_stream() const { return can_stream_; }

    Status verify_stream(std::span<PositionStream> streams, bool* matched) const {
        DCHECK(can_stream_);
        return match_exact_phrase_positions(streams, std::span(clause_terms_), std::span(offsets_),
                                            matched);
    }

    // `load(term, span)` reads distinct term `term`'s positions in the document and keeps them
    // valid until verify() returns. A frequency of zero means the document does not match.
    // Inlined, it runs inside each caller's per-document loop.
    template <typename Load>
    ALWAYS_INLINE Status verify(Load load, bool collect_frequency, float* frequency) {
        if (state_ == nullptr) {
            PhrasePositionSpan left;
            RETURN_IF_ERROR(load(clause_terms_[0], &left));
            if (clause_terms_.size() == 1) {
                const auto count = static_cast<uint32_t>(left.second - left.first);
                *frequency = static_cast<float>(collect_frequency ? count : std::min(count, 1U));
                return Status::OK();
            }
            PhrasePositionSpan right = left;
            if (clause_terms_[0] != clause_terms_[1]) {
                RETURN_IF_ERROR(load(clause_terms_[1], &right));
            }
            *frequency = static_cast<float>(
                    collect_frequency ? count_two_term_phrase(left, right, offsets_[1])
                                      : contains_two_term_phrase(left, right, offsets_[1]));
            return Status::OK();
        }
        return state_->verify(clause_terms_, load, collect_frequency, frequency);
    }

private:
    struct MatcherState {
        template <typename Load>
        Status verify(std::span<const size_t> clauses, Load load, bool collect_frequency,
                      float* frequency) {
            // Each document gets a new epoch, so the loaded marks need no clearing per document.
            if (++epoch == 0) {
                std::ranges::fill(loaded_epoch, 0);
                epoch = 1;
            }
            const auto clause_positions = [&](size_t clause, PhrasePositionSpan* span) -> Status {
                const size_t term = clauses[clause];
                if (loaded_epoch[term] != epoch) {
                    PhrasePositionSpan loaded;
                    RETURN_IF_ERROR(load(term, &loaded));
                    term_positions[term] = loaded;
                    loaded_epoch[term] = epoch;
                }
                *span = term_positions[term];
                return Status::OK();
            };
            *frequency = 0.0F;
            if (sloppy.has_value()) {
                for (size_t clause = 0; clause < clause_spans.size(); ++clause) {
                    RETURN_IF_ERROR(clause_positions(clause, &clause_spans[clause]));
                }
                *frequency = sloppy->match(clause_spans, collect_frequency);
                return Status::OK();
            }
            if (exact.has_value()) {
                uint32_t count = 0;
                RETURN_IF_ERROR(exact->match(clause_positions, collect_frequency, &count));
                *frequency = static_cast<float>(count);
                return Status::OK();
            }
            return Status::OK();
        }

        std::vector<PhrasePositionSpan> term_positions;
        std::vector<uint32_t> loaded_epoch;
        uint32_t epoch = 0;
        std::vector<PhrasePositionSpan> clause_spans;
        std::optional<SloppyPhraseMatcher> sloppy;
        std::optional<ExactPhraseMatcher> exact;
    };

    std::vector<size_t> clause_terms_;
    std::vector<uint32_t> offsets_;
    std::unique_ptr<MatcherState> state_;
    bool can_stream_ = false;
};

} // namespace doris::index_query
