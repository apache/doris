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
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "storage/index/query/phrase/exact_phrase_matcher.h"
#include "storage/index/query/phrase/position_span.h"
#include "storage/index/query/phrase/sloppy_phrase_matcher.h"

namespace doris::index_query {

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
        term_begins_.resize(term_count);
        term_ends_.resize(term_count);
        loaded_epoch_.resize(term_count);
        if (slop > 0) {
            clause_spans_.resize(clause_terms_.size());
            sloppy_.emplace(clause_terms_, offsets_, slop, ordered);
        } else if (clause_terms_.size() > 2) {
            DORIS_CHECK_EQ(clause_costs.size(), clause_terms_.size());
            exact_.emplace(offsets_,
                           select_phrase_verification_pair(clause_costs.size(), [&](size_t clause) {
                               return clause_costs[clause];
                           }));
        }
    }

    // The exact matcher views offsets_, so a copy would view the original's offsets.
    PhraseVerifier(const PhraseVerifier&) = delete;
    PhraseVerifier& operator=(const PhraseVerifier&) = delete;
    PhraseVerifier(PhraseVerifier&&) = default;

    // `load(term, span)` reads distinct term `term`'s positions in the document and keeps them
    // valid until verify() returns. A frequency of zero means the document does not match.
    // Inlined, it runs inside each caller's per-document loop.
    template <typename Load>
    ALWAYS_INLINE Status verify(Load load, bool collect_frequency, float* frequency) {
        // Each document gets a new epoch, so the loaded marks need no clearing per document.
        if (++epoch_ == 0) {
            std::ranges::fill(loaded_epoch_, 0);
            epoch_ = 1;
        }
        const auto clause_positions = [&](size_t clause, PhrasePositionSpan* span) -> Status {
            const size_t term = clause_terms_[clause];
            if (loaded_epoch_[term] != epoch_) {
                PhrasePositionSpan loaded;
                RETURN_IF_ERROR(load(term, &loaded));
                term_begins_[term] = loaded.first;
                term_ends_[term] = loaded.second;
                loaded_epoch_[term] = epoch_;
            }
            *span = {term_begins_[term], term_ends_[term]};
            return Status::OK();
        };
        *frequency = 0.0F;
        if (sloppy_.has_value()) {
            for (size_t clause = 0; clause < clause_spans_.size(); ++clause) {
                RETURN_IF_ERROR(clause_positions(clause, &clause_spans_[clause]));
            }
            *frequency = sloppy_->match(clause_spans_, collect_frequency);
            return Status::OK();
        }
        if (exact_.has_value()) {
            uint32_t count = 0;
            RETURN_IF_ERROR(exact_->match(clause_positions, collect_frequency, &count));
            *frequency = static_cast<float>(count);
            return Status::OK();
        }
        PhrasePositionSpan left;
        RETURN_IF_ERROR(clause_positions(0, &left));
        if (clause_terms_.size() == 1) {
            const auto count = static_cast<uint32_t>(left.second - left.first);
            *frequency = static_cast<float>(collect_frequency ? count : std::min(count, 1U));
            return Status::OK();
        }
        PhrasePositionSpan right;
        RETURN_IF_ERROR(clause_positions(1, &right));
        *frequency = static_cast<float>(
                collect_frequency ? count_two_term_phrase(left, right, offsets_[1])
                                  : contains_two_term_phrase(left, right, offsets_[1]));
        return Status::OK();
    }

private:
    std::vector<size_t> clause_terms_;
    std::vector<uint32_t> offsets_;
    // Begins and ends live apart so that a loader's two pointer reads stay two 8-byte loads: one
    // 16-byte read right after the loader appended positions would wait for that store.
    std::vector<const uint32_t*> term_begins_;
    std::vector<const uint32_t*> term_ends_;
    std::vector<uint32_t> loaded_epoch_;
    uint32_t epoch_ = 0;
    std::vector<PhrasePositionSpan> clause_spans_;
    std::optional<SloppyPhraseMatcher> sloppy_;
    std::optional<ExactPhraseMatcher> exact_;
};

} // namespace doris::index_query
