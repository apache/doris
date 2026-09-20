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
#include <utility>

#include "core/custom_allocator.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query_v2/intersection.h"
#include "storage/index/inverted/query_v2/term_query/term_scorer.h"
#include "storage/index/query/boolean/truth_set.h"
#include "storage/index/query/exec/bitmap_conjunction.h"
#include "storage/index/query/exec/collect_postings.h"
#include "storage/index/query/exec/nullable_conjunction.h"
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

    MaterializedScorer(index_query::TruthSet truth, DorisVector<ScoredRow> scores)
            : _true_rows(std::move(truth.true_rows)),
              _null_rows(std::move(truth.null_rows)),
              _iterator(_true_rows.begin()),
              _scores(std::move(scores)) {}

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

template <typename Source>
class ScorerDocSet {
public:
    explicit ScorerDocSet(Source& source) : _source(&source), _doc(normalize(source.doc())) {}
    uint64_t doc() const { return _doc; }
    uint64_t cost() const { return _source->cost(); }
    uint64_t seek(uint32_t target) { return _doc = normalize(_source->seek(target)); }

private:
    static uint64_t normalize(uint32_t doc) {
        return doc == TERMINATED ? index_query::kDocIdEnd : doc;
    }
    Source* _source;
    uint64_t _doc;
};

ScorerPtr collect_term_conjunction(const std::vector<ScorerPtr>& sources, bool enable_scoring,
                                   uint32_t row_count, const NullBitmapResolver* resolver) {
    DorisVector<index_query::ConjunctionPostings> inputs;
    inputs.reserve(sources.size());
    for (const auto& source : sources) {
        auto& term = static_cast<TermScorer&>(*source);
        inputs.push_back(
                {.docs = &term.postings_doc_set(), .null_rows = term.get_null_bitmap(resolver)});
    }
    index_query::BitmapConjunctionResult result;
    const std::span<const index_query::ConjunctionPostings> postings(inputs);
    if (enable_scoring) {
        THROW_IF_ERROR(index_query::collect_bitmap_conjunction<true>(postings, row_count, &result));
    } else {
        THROW_IF_ERROR(
                index_query::collect_bitmap_conjunction<false>(postings, row_count, &result));
    }
    DorisVector<ScoredRow> scores;
    if (enable_scoring) {
        DorisVector<size_t> ordinals(sources.size(), 0);
        for (const uint32_t doc : result.truth.true_rows) {
            float score = 0.0F;
            for (size_t slot = 0; slot < sources.size(); ++slot) {
                const auto& rows = result.statistics[slot];
                auto& ordinal = ordinals[slot];
                while (ordinal < rows.size() && rows[ordinal].doc < doc) {
                    ++ordinal;
                }
                DCHECK_LT(ordinal, rows.size());
                const auto& row = rows[ordinal];
                DCHECK_EQ(row.doc, doc);
                score += static_cast<TermScorer&>(*sources[slot])
                                 .score_for_posting(row.frequency, row.norm);
            }
            scores.push_back({.doc = doc, .score = score});
        }
    }
    return std::make_shared<MaterializedScorer>(std::move(result.truth), std::move(scores));
}

struct RequiredSource {
    ScorerPtr docs;
    const roaring::Roaring* null_rows;
};

DorisVector<RequiredSource> group_required_sources(const std::vector<ScorerPtr>& sources,
                                                   uint32_t row_count,
                                                   const NullBitmapResolver* resolver) {
    struct Group {
        std::vector<ScorerPtr> children;
        const roaring::Roaring* null_rows;
    };
    DorisVector<Group> groups;
    for (const auto& source : sources) {
        const auto* nulls = source->get_null_bitmap(resolver);
        if (nulls != nullptr && nulls->isEmpty()) {
            nulls = nullptr;
        }
        auto group = std::ranges::find_if(groups, [nulls](const auto& item) {
            return item.null_rows == nulls ||
                   (item.null_rows != nullptr && nulls != nullptr && *item.null_rows == *nulls);
        });
        if (group == groups.end()) {
            groups.push_back({.children = {source}, .null_rows = nulls});
        } else {
            group->children.push_back(source);
        }
    }
    DorisVector<RequiredSource> result;
    result.reserve(groups.size());
    for (auto& group : groups) {
        result.push_back({.docs = make_intersect_scorers(std::move(group.children), row_count),
                          .null_rows = group.null_rows});
    }
    return result;
}

template <typename Source, typename DocSource>
ScorerPtr collect_required_scorers(const std::vector<ScorerPtr>& sources,
                                   std::span<const RequiredSource> groups, bool enable_scoring,
                                   uint32_t row_count) {
    DorisVector<index_query::NullableDocSet<ScorerDocSet<DocSource>>> inputs;
    inputs.reserve(groups.size());
    for (const auto& group : groups) {
        auto& typed = static_cast<DocSource&>(*group.docs);
        inputs.emplace_back(ScorerDocSet(typed), group.null_rows);
    }
    std::ranges::sort(inputs, [](const auto& a, const auto& b) { return a.cost() < b.cost(); });
    index_query::TruthSet truth;
    index_query::RoaringDocIdSink true_sink(truth.true_rows);
    index_query::RoaringDocIdSink null_sink(truth.null_rows);
    DorisVector<ScoredRow> scores;
    THROW_IF_ERROR(index_query::collect_nullable_conjunction(
            std::span(inputs), row_count, true_sink, null_sink, [&](uint32_t doc) {
                if (enable_scoring) {
                    float score = 0.0F;
                    for (const auto& source : sources) {
                        score += static_cast<Source&>(*source).score();
                    }
                    scores.push_back({.doc = doc, .score = score});
                }
            }));
    return std::make_shared<MaterializedScorer>(std::move(truth), std::move(scores));
}

class CompleteTruthScorer final : public Scorer {
public:
    CompleteTruthScorer(ScorerPtr source, index_query::TruthSet truth)
            : _source(std::move(source)), _truth(std::move(truth)) {}

    uint32_t doc() const override { return _source->doc(); }
    uint32_t size_hint() const override { return _source->size_hint(); }
    uint32_t advance() override { return _source->advance(); }
    uint32_t seek(uint32_t target) override { return _source->seek(target); }
    float score() override { return _source->score(); }

    bool has_null_bitmap(const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return !_truth.null_rows.isEmpty();
    }
    const roaring::Roaring* get_null_bitmap(
            const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return &_truth.null_rows;
    }
    const roaring::Roaring* get_true_bitmap() const override { return &_truth.true_rows; }

private:
    ScorerPtr _source;
    index_query::TruthSet _truth;
};

class CompleteNullScorer final : public Scorer {
public:
    CompleteNullScorer(ScorerPtr source, roaring::Roaring null_rows)
            : _source(std::move(source)), _null_rows(std::move(null_rows)) {}

    uint32_t doc() const override { return _source->doc(); }
    uint32_t size_hint() const override { return _source->size_hint(); }
    uint64_t cost() const override { return _source->cost(); }
    uint32_t norm() const override { return _source->norm(); }
    uint32_t advance() override { return _source->advance(); }
    uint32_t seek(uint32_t target) override { return _source->seek(target); }
    float score() override { return _source->score(); }
    bool has_null_bitmap(const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return !_null_rows.isEmpty();
    }
    const roaring::Roaring* get_null_bitmap(
            const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return &_null_rows;
    }
    const roaring::Roaring* get_true_bitmap() const override { return _source->get_true_bitmap(); }

private:
    ScorerPtr _source;
    roaring::Roaring _null_rows;
};

class TruthSetScorer final : public Scorer {
public:
    TruthSetScorer(index_query::TruthSet truth, std::vector<ScorerPtr> score_sources)
            : _truth(std::move(truth)),
              _iterator(_truth.true_rows.begin()),
              _score_sources(std::move(score_sources)) {}

    uint32_t doc() const override { return _iterator.i.has_value ? *_iterator : TERMINATED; }
    uint32_t size_hint() const override {
        return static_cast<uint32_t>(_truth.true_rows.cardinality());
    }

    uint32_t advance() override {
        if (_iterator.i.has_value) {
            ++_iterator;
        }
        _score_cache.reset();
        return doc();
    }

    uint32_t seek(uint32_t target) override {
        if (_iterator.i.has_value && target > *_iterator) {
            _iterator.equalorlarger(target);
        }
        _score_cache.reset();
        return doc();
    }

    float score() override {
        if (_score_cache.has_value()) {
            return *_score_cache;
        }
        float sum = 0.0F;
        const uint32_t current = doc();
        for (const auto& source : _score_sources) {
            uint32_t source_doc = source->doc();
            while (source_doc < current) {
                source_doc = source->advance();
            }
            if (source_doc == current) {
                sum += source->score();
            }
        }
        _score_cache = sum;
        return sum;
    }

    bool has_null_bitmap(const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return !_truth.null_rows.isEmpty();
    }
    const roaring::Roaring* get_null_bitmap(
            const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return &_truth.null_rows;
    }
    const roaring::Roaring* get_true_bitmap() const override { return &_truth.true_rows; }

private:
    index_query::TruthSet _truth;
    roaring::Roaring::const_iterator _iterator;
    std::vector<ScorerPtr> _score_sources;
    std::optional<float> _score_cache;
};

} // namespace

index_query::TruthSet collect_truth_set(const ScorerPtr& scorer, const NullBitmapResolver* resolver,
                                        const roaring::Roaring* candidates) {
    index_query::TruthSet result;
    if (scorer == nullptr) {
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

ScorerPtr materialize_scorer(ScorerPtr source, bool enable_scoring,
                             const NullBitmapResolver* resolver,
                             const roaring::Roaring* candidates) {
    if (source->get_true_bitmap() != nullptr) {
        return source;
    }
    return std::make_shared<MaterializedScorer>(source, enable_scoring, resolver, candidates);
}

ScorerPtr make_nullable_conjunction(const std::vector<ScorerPtr>& sources, bool enable_scoring,
                                    uint32_t row_count, const NullBitmapResolver* resolver) {
    if (std::ranges::all_of(sources, [](const auto& source) {
            return dynamic_cast<TermScorer*>(source.get()) != nullptr;
        })) {
        return collect_term_conjunction(sources, enable_scoring, row_count, resolver);
    }
    const auto groups = group_required_sources(sources, row_count, resolver);
    if (std::ranges::all_of(groups,
                            [](const auto& group) { return group.docs->doc() == TERMINATED; })) {
        index_query::TruthSet result;
        result.null_rows.addRange(0, row_count);
        for (const auto& group : groups) {
            if (group.null_rows == nullptr) {
                result.null_rows = {};
                break;
            }
            result.null_rows &= *group.null_rows;
        }
        return make_truth_set_scorer(std::move(result), {}, false);
    }
    return collect_required_scorers<Scorer, Scorer>(sources, groups, enable_scoring, row_count);
}

ScorerPtr make_complete_truth_scorer(ScorerPtr scorer, index_query::TruthSet truth) {
    return std::make_shared<CompleteTruthScorer>(std::move(scorer), std::move(truth));
}

ScorerPtr make_complete_null_scorer(ScorerPtr scorer, roaring::Roaring null_rows) {
    if (null_rows.isEmpty()) {
        return scorer;
    }
    return std::make_shared<CompleteNullScorer>(std::move(scorer), std::move(null_rows));
}

std::optional<roaring::Roaring> shared_null_bitmap(
        std::initializer_list<std::span<const ScorerPtr>> groups,
        const NullBitmapResolver* resolver) {
    const roaring::Roaring* shared = nullptr;
    bool first = true;
    for (const auto& group : groups) {
        for (const auto& child : group) {
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
    }
    return shared == nullptr ? roaring::Roaring() : *shared;
}

ScorerPtr make_truth_set_scorer(index_query::TruthSet truth, std::vector<ScorerPtr> score_sources,
                                bool enable_scoring) {
    if (enable_scoring) {
        return std::make_shared<TruthSetScorer>(std::move(truth), std::move(score_sources));
    }
    auto true_rows = std::make_shared<roaring::Roaring>(std::move(truth.true_rows));
    auto null_rows = std::make_shared<roaring::Roaring>(std::move(truth.null_rows));
    return std::make_shared<BitSetScorer>(std::move(true_rows), std::move(null_rows));
}

roaring::Roaring complete_null_bitmap(std::vector<ScorerPtr>& children, bool intersection,
                                      bool enable_scoring, const NullBitmapResolver* resolver) {
    if (auto shared = shared_null_bitmap({children}, resolver); shared.has_value()) {
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
