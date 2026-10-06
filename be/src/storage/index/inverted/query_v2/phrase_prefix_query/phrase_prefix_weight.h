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

#include "storage/index/inverted/query_v2/phrase_query/phrase_scorer.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_weight.h"
#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/inverted/query_v2/union_postings.h"
#include "storage/index/inverted/query_v2/weight.h"
#include "storage/index/query/term_pattern.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// A phrase whose last term matches the terms that start with it. With `suffix` its first term
// matches the terms that end with it.
class PhrasePrefixWeight final : public SlotPhraseWeight {
public:
    PhrasePrefixWeight(std::wstring field, std::vector<std::pair<size_t, std::string>> phrase_terms,
                       std::pair<size_t, std::string> prefix,
                       index_query::ScoringContextPtr<float> similarity, bool enable_scoring,
                       int32_t max_expansions, index_query::PhraseQueryOptions options, bool suffix,
                       bool nullable)
            : SlotPhraseWeight(std::move(field), options, std::move(similarity), enable_scoring,
                               nullable),
              _phrase_terms(std::move(phrase_terms)),
              _prefix(std::move(prefix)),
              _max_expansions(max_expansions),
              _suffix(suffix) {}
    ~PhrasePrefixWeight() override = default;

private:
    std::vector<PhraseSlot> _slots() const override {
        std::vector<PhraseSlot> slots;
        for (const auto& [offset, term] : _phrase_terms) {
            PhraseSlot& slot = slots.emplace_back(
                    PhraseSlot {.offset = static_cast<uint32_t>(offset), .terms = {term}});
            if (_suffix && offset == 0) {
                slot.expand = index_query::TermPatternKind::kSuffix;
                slot.max_expansions = _max_expansions;
            }
        }
        slots.push_back({.offset = static_cast<uint32_t>(_prefix.first),
                         .terms = {_prefix.second},
                         .expand = index_query::TermPatternKind::kPrefix,
                         .max_expansions = _max_expansions});
        return slots;
    }

    ScorerPtr _streamed_scorer(index_query::IndexSource& source, uint32_t num_docs) override {
        std::vector<std::pair<size_t, PostingsPtr>> all_postings;
        for (const auto& [offset, term] : _phrase_terms) {
            PostingsPtr posting =
                    _suffix && offset == 0
                            ? _expanded_postings(source, index_query::TermPatternKind::kSuffix,
                                                 term)
                            : open_postings(source, term, /*positions=*/true, _enable_scoring,
                                            _similarity);
            if (!posting) {
                return std::make_shared<EmptyScorer>();
            }
            all_postings.emplace_back(offset, std::move(posting));
        }
        PostingsPtr tail =
                _expanded_postings(source, index_query::TermPatternKind::kPrefix, _prefix.second);
        if (!tail) {
            return std::make_shared<EmptyScorer>();
        }
        all_postings.emplace_back(_prefix.first, std::move(tail));
        return PhraseScorer<PostingsPtr>::create(all_postings, _similarity, _options, num_docs);
    }

    // The terms `text` matches as a `kind` pattern, in dictionary order.
    std::vector<std::string> _expand(index_query::IndexSource& source,
                                     index_query::TermPatternKind kind,
                                     const std::string& text) const {
        index_query::TermPattern pattern;
        THROW_IF_ERROR(index_query::TermPattern::create(kind, text, &pattern));
        std::vector<std::string> terms;
        THROW_IF_ERROR(source.expand_terms(pattern, _max_expansions, &terms));
        return terms;
    }

    // The union of the positions of the terms `text` expands to as a `kind` pattern, or nullptr
    // when it expands to none.
    PostingsPtr _expanded_postings(index_query::IndexSource& source,
                                   index_query::TermPatternKind kind, const std::string& text) {
        std::vector<SegmentPostingsPtr> postings;
        for (const auto& term : _expand(source, kind, text)) {
            auto posting =
                    open_postings(source, term, /*positions=*/true, _enable_scoring, _similarity);
            if (posting) {
                postings.emplace_back(std::move(posting));
            }
        }
        if (postings.empty()) {
            return nullptr;
        }
        return make_union_postings(std::move(postings));
    }

    std::vector<std::pair<size_t, std::string>> _phrase_terms;
    std::pair<size_t, std::string> _prefix;
    int32_t _max_expansions = 50;
    bool _suffix = false;
};

} // namespace doris::segment_v2::inverted_index::query_v2
