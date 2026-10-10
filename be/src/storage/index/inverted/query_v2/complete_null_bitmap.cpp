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

#include "storage/index/inverted/query_v2/complete_null_bitmap.h"

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <span>
#include <utility>

#include "core/custom_allocator.h"
#include "storage/index/collection_similarity.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_scorer.h"
#include "storage/index/inverted/query_v2/scored_rows_scorer.h"
#include "storage/index/inverted/query_v2/term_query/term_scorer.h"
#include "storage/index/query/boolean/truth_set.h"
#include "storage/index/query/exec/collect_postings.h"
#include "storage/index/query/roaring_docid_sink.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {

template <typename Next, typename Visitor>
roaring::Roaring consume_true_rows(uint32_t doc, Next&& next, Visitor&& visit) {
    roaring::Roaring result;
    std::array<uint32_t, 256> batch {};
    size_t count = 0;
    while (doc != TERMINATED) {
        batch[count++] = doc;
        visit(doc);
        if (count == batch.size()) {
            result.addMany(count, batch.data());
            count = 0;
        }
        doc = next();
    }
    if (count != 0) {
        result.addMany(count, batch.data());
    }
    return result;
}

template <typename ScorerType, typename Visitor>
roaring::Roaring scan_candidate_rows(ScorerType& scorer, uint32_t doc,
                                     const roaring::Roaring& candidates, Visitor&& visit) {
    const uint32_t last = candidates.maximum();
    if (doc < candidates.minimum()) {
        doc = scorer.seek(candidates.minimum());
    }
    const auto find_match = [&]() {
        while (doc != TERMINATED && doc <= last && !candidates.contains(doc)) {
            doc = scorer.advance();
        }
        return doc <= last ? doc : TERMINATED;
    };
    const auto first = find_match();
    return consume_true_rows(
            first,
            [&]() {
                if (doc == last) {
                    return TERMINATED;
                }
                doc = scorer.advance();
                return find_match();
            },
            std::forward<Visitor>(visit));
}

template <typename ScorerType, typename Visitor>
roaring::Roaring seek_candidate_rows(ScorerType& scorer, uint32_t doc,
                                     const roaring::Roaring& candidates, Visitor&& visit) {
    auto candidate = candidates.begin();
    const auto find_match = [&]() {
        while (candidate.i.has_value && doc != TERMINATED) {
            if (doc < *candidate) {
                doc = scorer.seek(*candidate);
            }
            if (doc == *candidate) {
                return doc;
            }
            candidate.equalorlarger(doc);
        }
        return TERMINATED;
    };
    const auto first = find_match();
    return consume_true_rows(
            first,
            [&]() {
                ++candidate;
                return find_match();
            },
            std::forward<Visitor>(visit));
}

template <typename ScorerType, typename Visitor>
roaring::Roaring consume_typed_true_rows(ScorerType& scorer, uint32_t doc,
                                         const roaring::Roaring* candidates, Visitor&& visit) {
    const auto visit_doc = [&](uint32_t row) { visit(row, [&]() { return scorer.score(); }); };
    if (candidates == nullptr) {
        return consume_true_rows(
                doc, [&]() { return scorer.advance(); }, visit_doc);
    }
    if (candidates->isEmpty()) {
        return {};
    }
    // Sequential reads avoid repeated cursor searches when candidates are dense.
    constexpr uint64_t seek_cost_multiplier = 4;
    if (candidates->cardinality() * seek_cost_multiplier >= scorer.cost()) {
        return scan_candidate_rows(scorer, doc, *candidates, visit_doc);
    }
    return seek_candidate_rows(scorer, doc, *candidates, visit_doc);
}

template <bool VisitRows, typename Visitor>
roaring::Roaring consume_true_rows(Scorer& scorer, uint32_t doc, const roaring::Roaring* candidates,
                                   Visitor&& visit) {
    if (auto* term = dynamic_cast<TermScorer*>(&scorer); term != nullptr) {
        roaring::Roaring result;
        index_query::RoaringDocIdSink sink(result);
        THROW_IF_ERROR(index_query::collect_postings<VisitRows>(
                term->postings_doc_set(), candidates, sink,
                [&](uint32_t row, uint32_t frequency, uint32_t norm) {
                    visit(row, [&]() { return term->score_for_posting(frequency, norm); });
                }));
        return result;
    }
    if (auto* phrase = dynamic_cast<PhraseScorer<SegmentPostingsPtr>*>(&scorer);
        phrase != nullptr) {
        return consume_typed_true_rows(*phrase, doc, candidates, std::forward<Visitor>(visit));
    }
    return consume_typed_true_rows(scorer, doc, candidates, std::forward<Visitor>(visit));
}

struct ScoredRow {
    uint32_t doc;
    float score;
};

class MaterializedScorer final : public Scorer {
public:
    MaterializedScorer(const ScorerPtr& source, bool enable_scoring,
                       const NullBitmapResolver* resolver, const roaring::Roaring* candidates)
            : _iterator(_true_rows.begin()) {
        if (const auto* nulls = source->get_null_bitmap(resolver); nulls != nullptr) {
            _null_rows = candidates == nullptr ? *nulls : *nulls & *candidates;
        }
        std::optional<roaring::Roaring> true_candidates;
        if (candidates != nullptr && !_null_rows.isEmpty()) {
            true_candidates = *candidates - _null_rows;
            candidates = &*true_candidates;
        }
        if (enable_scoring) {
            _true_rows = consume_true_rows<true>(
                    *source, source->doc(), candidates, [&](uint32_t doc, auto&& score) {
                        _scores.push_back({.doc = doc, .score = score()});
                    });
        } else {
            _true_rows = consume_true_rows<false>(*source, source->doc(), candidates,
                                                  [](uint32_t, auto&&) {});
        }
        _iterator = _true_rows.begin();
    }

    uint32_t doc() const override {
        if (!_scores.empty()) {
            return _ordinal < _scores.size() ? _scores[_ordinal].doc : TERMINATED;
        }
        return _iterator.i.has_value ? *_iterator : TERMINATED;
    }
    uint32_t size_hint() const override { return static_cast<uint32_t>(_true_rows.cardinality()); }

    uint32_t advance() override {
        if (!_scores.empty()) {
            if (_ordinal < _scores.size()) {
                ++_ordinal;
            }
        } else if (_iterator.i.has_value) {
            ++_iterator;
        }
        return doc();
    }

    uint32_t seek(uint32_t target) override {
        const uint32_t current = doc();
        if (target <= current) {
            return current;
        }
        if (!_scores.empty()) {
            // Unique sorted document IDs bound the skipped row count by the document gap.
            const size_t limit = std::min(_scores.size(), _ordinal + (target - current));
            const auto next = std::lower_bound(
                    _scores.begin() + _ordinal, _scores.begin() + limit, target,
                    [](const ScoredRow& row, uint32_t value) { return row.doc < value; });
            _ordinal = next - _scores.begin();
        } else {
            _iterator.equalorlarger(target);
        }
        return doc();
    }

    float score() override { return _scores.empty() ? 0.0F : _scores[_ordinal].score; }
    bool has_null_bitmap(const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return !_null_rows.isEmpty();
    }
    const roaring::Roaring* get_null_bitmap(
            const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return &_null_rows;
    }
    const roaring::Roaring* get_true_bitmap() const override { return &_true_rows; }

private:
    roaring::Roaring _true_rows;
    roaring::Roaring _null_rows;
    roaring::Roaring::const_iterator _iterator;
    DorisVector<ScoredRow> _scores;
    size_t _ordinal = 0;
};

// Equal UNKNOWN sets stay UNKNOWN under AND and OR.
std::optional<roaring::Roaring> shared_null_bitmap(std::span<const ScorerPtr> children,
                                                   const NullBitmapResolver* resolver) {
    const roaring::Roaring* shared = nullptr;
    bool first = true;
    for (const auto& child : children) {
        const auto* nulls = child == nullptr ? nullptr : child->get_null_bitmap(resolver);
        if (nulls != nullptr && nulls->isEmpty()) {
            nulls = nullptr;
        }
        if (first) {
            shared = nulls;
            first = false;
        } else if (shared != nulls &&
                   (shared == nullptr || nulls == nullptr || *shared != *nulls)) {
            return std::nullopt;
        }
    }
    return shared == nullptr ? roaring::Roaring() : *shared;
}

} // namespace

index_query::TruthSet collect_truth_set(const ScorerPtr& scorer, const NullBitmapResolver* resolver,
                                        const roaring::Roaring* candidates) {
    index_query::TruthSet result;
    if (scorer == nullptr) {
        return result;
    }
    // Candidates dense enough to scan cost less to intersect once than to test row by row.
    constexpr uint64_t dense_candidate_factor = 4;
    if (candidates != nullptr &&
        candidates->cardinality() * dense_candidate_factor >= scorer->cost()) {
        result = collect_truth_set(scorer, resolver);
        result.true_rows &= *candidates;
        result.null_rows &= *candidates;
        return result;
    }
    if (const auto* nulls = scorer->get_null_bitmap(resolver); nulls != nullptr) {
        result.null_rows = candidates == nullptr ? *nulls : *nulls & *candidates;
    }
    if (const auto* truths = scorer->get_true_bitmap(); truths != nullptr) {
        result.true_rows = candidates == nullptr ? *truths : *truths & *candidates;
    } else {
        uint32_t doc = scorer->doc();
        if (doc == TERMINATED) {
            doc = scorer->advance();
        }
        std::optional<roaring::Roaring> true_candidates;
        if (candidates != nullptr && !result.null_rows.isEmpty()) {
            true_candidates = *candidates - result.null_rows;
            candidates = &*true_candidates;
        }
        result.true_rows =
                consume_true_rows<false>(*scorer, doc, candidates, [](uint32_t, auto&&) {});
    }
    return result;
}

void collect_true_rows(const ScorerPtr& scorer, roaring::Roaring* rows) {
    if (const auto* truths = scorer->get_true_bitmap(); truths != nullptr) {
        *rows |= *truths;
        return;
    }
    auto collected =
            consume_true_rows<false>(*scorer, scorer->doc(), nullptr, [](uint32_t, auto&&) {});
    if (rows->isEmpty()) {
        rows->swap(collected);
    } else {
        *rows |= collected;
    }
}

roaring::Roaring collect_scored_rows(const ScorerPtr& scorer, uint32_t doc_base,
                                     CollectionSimilarity& similarity) {
    if (const auto* listed = dynamic_cast<const ScoredRowsScorer*>(scorer.get());
        listed != nullptr) {
        const std::span<const uint32_t> rows = listed->rows();
        const std::span<const float> scores = listed->scores();
        for (size_t i = 0; i < rows.size(); ++i) {
            similarity.collect(rows[i] + doc_base, scores[i]);
        }
        roaring::Roaring result;
        result.addMany(rows.size(), rows.data());
        return result;
    }
    return consume_true_rows<true>(
            *scorer, scorer->doc(), nullptr,
            [&](uint32_t row, auto&& score) { similarity.collect(row + doc_base, score()); });
}

ScorerPtr materialize_scorer(ScorerPtr source, bool enable_scoring,
                             const NullBitmapResolver* resolver,
                             const roaring::Roaring* candidates) {
    if (source->get_true_bitmap() != nullptr) {
        return source;
    }
    return std::make_shared<MaterializedScorer>(source, enable_scoring, resolver, candidates);
}

roaring::Roaring complete_null_bitmap(std::vector<ScorerPtr>& children, bool intersection,
                                      bool enable_scoring, const NullBitmapResolver* resolver) {
    if (auto shared = shared_null_bitmap(children, resolver); shared.has_value()) {
        return std::move(*shared);
    }
    const bool nullable = std::ranges::any_of(children, [resolver](const ScorerPtr& child) {
        return child != nullptr && child->has_null_bitmap(resolver);
    });
    if (!nullable) {
        return {};
    }

    index_query::TruthSet result;
    bool first = true;
    for (auto& child : children) {
        index_query::TruthSet value;
        if (child != nullptr) {
            child = materialize_scorer(std::move(child), enable_scoring, resolver);
            value = collect_truth_set(child, resolver);
        }
        if (first) {
            result = std::move(value);
            first = false;
        } else if (intersection) {
            result.intersect_with(value);
        } else {
            result.union_with(value);
        }
    }
    return std::move(result.null_rows);
}

} // namespace doris::segment_v2::inverted_index::query_v2
