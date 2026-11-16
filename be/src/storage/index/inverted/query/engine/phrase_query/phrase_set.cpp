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

#include "storage/index/inverted/query/engine/phrase_query/phrase_set.h"

#include <algorithm>
#include <span>
#include <string_view>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "storage/index/inverted/common/string_helper.h"
#include "storage/index/inverted/query/engine/null_bitmap_fetcher.h"
#include "storage/index/inverted/query/engine/nullable_scorer.h"
#include "storage/index/inverted/query/engine/phrase_query/phrase_weight.h"
#include "storage/index/inverted/query/engine/score_combiner.h"
#include "storage/index/inverted/query/engine/union/buffered_union.h"
#include "storage/index/inverted/query/exec/roaring_docid_sink.h"

namespace doris::segment_v2::inverted_index::query_v2 {

std::shared_ptr<PhraseSetEngine> PhraseSetEngine::open(index_query::IndexSource& source,
                                                       const std::vector<PhraseSetPhrase>& phrases,
                                                       const roaring::Roaring* within) {
    std::shared_ptr<PhraseSetEngine> engine(new PhraseSetEngine());

    // The distinct words, and the engine word of each clause.
    std::vector<std::string_view> words;
    std::vector<std::vector<size_t>> clause_words(phrases.size());
    for (size_t i = 0; i < phrases.size(); ++i) {
        for (const TermInfo& clause : phrases[i].clauses) {
            const std::string& term = clause.get_single_term();
            auto it = std::ranges::find(words, std::string_view(term));
            if (it == words.end()) {
                it = words.insert(words.end(), term);
            }
            clause_words[i].push_back(static_cast<size_t>(it - words.begin()));
        }
    }

    // The documents of each word, read without positions. A word the index lacks lists none.
    // Asked about a few rows only, a word much more common than them seeks to each row.
    constexpr uint64_t seek_factor = 8;
    std::vector<roaring::Roaring> docs(words.size());
    for (size_t w = 0; w < words.size(); ++w) {
        if (within != nullptr) {
            std::unique_ptr<index_query::PostingsCursor> cursor;
            THROW_IF_ERROR(
                    source.open_term(words[w], /*positions=*/false, /*scoring=*/false, &cursor));
            if (cursor == nullptr) {
                continue;
            }
            auto postings = make_segment_postings(std::move(cursor), /*scoring=*/false, nullptr);
            if (within->cardinality() * seek_factor < postings->size_hint()) {
                std::vector<uint32_t> held;
                for (const uint32_t row : *within) {
                    uint32_t doc = postings->doc();
                    if (doc < row) {
                        doc = postings->seek(row);
                    }
                    if (doc == TERMINATED) {
                        break;
                    }
                    if (doc == row) {
                        held.push_back(row);
                    }
                }
                docs[w].addMany(held.size(), held.data());
                continue;
            }
        }
        const std::string term(words[w]);
        index_query::RoaringDocIdSink sink(docs[w]);
        THROW_IF_ERROR(source.collect_terms(std::span(&term, 1), sink));
        if (within != nullptr) {
            docs[w] &= *within;
        }
    }

    // The rows each phrase may match: the rows that hold all its words. As its phrase query, a
    // phrase with a word no row holds matches nothing in the segment.
    engine->_phrases.resize(phrases.size());
    std::vector<bool> used(words.size(), false);
    for (size_t i = 0; i < phrases.size(); ++i) {
        if (std::ranges::any_of(clause_words[i], [&](size_t w) { return docs[w].isEmpty(); })) {
            continue;
        }
        PhraseSetEngine::Phrase& phrase = engine->_phrases[i];
        for (size_t w : clause_words[i]) {
            used[w] = true;
        }
        std::vector<size_t> by_size = clause_words[i];
        std::ranges::sort(by_size, {}, [&](size_t w) { return docs[w].cardinality(); });
        phrase.rows = docs[by_size.front()];
        for (size_t k = 1; k < by_size.size() && !phrase.rows.isEmpty(); ++k) {
            if (by_size[k] != by_size[k - 1]) {
                phrase.rows &= docs[by_size[k]];
            }
        }
        engine->_candidates |= phrase.rows;
    }
    // The document lists are not needed past here.
    docs.clear();
    docs.shrink_to_fit();

    // Each word some phrase may match on is opened once, with positions.
    engine->_words.resize(words.size());
    for (size_t w = 0; w < words.size(); ++w) {
        if (!used[w]) {
            continue;
        }
        std::unique_ptr<index_query::PostingsCursor> cursor;
        THROW_IF_ERROR(source.open_term(words[w], /*positions=*/true, /*scoring=*/false, &cursor));
        DORIS_CHECK(cursor != nullptr);
        engine->_words[w].postings =
                make_segment_postings(std::move(cursor), /*scoring=*/false, nullptr);
    }

    // Each phrase checks positions as its phrase query would, its clauses reading the engine's
    // words.
    for (size_t i = 0; i < phrases.size(); ++i) {
        PhraseSetEngine::Phrase& phrase = engine->_phrases[i];
        if (phrase.rows.isEmpty()) {
            continue;
        }
        std::vector<size_t> clause_terms;
        std::vector<uint32_t> offsets;
        std::vector<uint64_t> costs;
        for (size_t c = 0; c < clause_words[i].size(); ++c) {
            const size_t w = clause_words[i][c];
            const auto local = std::ranges::find(phrase.words, w);
            clause_terms.push_back(static_cast<size_t>(local - phrase.words.begin()));
            if (local == phrase.words.end()) {
                phrase.words.push_back(w);
            }
            offsets.push_back(static_cast<uint32_t>(phrases[i].clauses[c].position));
            costs.push_back(engine->_words[w].postings->cost());
        }
        phrase.verifier.emplace(std::move(clause_terms), offsets, costs, phrases[i].slop,
                                phrases[i].ordered);
    }
    return engine;
}

PhraseSetEngine::~PhraseSetEngine() = default;

bool PhraseSetEngine::holds(Phrase& phrase, uint32_t row) {
    if (!phrase.next.has_value()) {
        phrase.next.emplace(phrase.rows.begin());
        phrase.next_row = *phrase.next == phrase.rows.end() ? TERMINATED : **phrase.next;
    }
    // The rows asked about are close together, so stepping is cheaper than a seek, which looks
    // up the maximum of the container it lands in.
    for (int step = 0; step < 8 && phrase.next_row < row; ++step) {
        ++*phrase.next;
        phrase.next_row = *phrase.next == phrase.rows.end() ? TERMINATED : **phrase.next;
    }
    if (phrase.next_row < row) {
        phrase.next->equalorlarger(row);
        phrase.next_row = *phrase.next == phrase.rows.end() ? TERMINATED : **phrase.next;
    }
    return phrase.next_row == row;
}

Status PhraseSetEngine::load(size_t word, uint32_t row, index_query::PhrasePositionSpan* span) {
    Word& w = _words[word];
    if (w.loaded_row != row) {
        if (w.postings->doc() < row) {
            w.postings->seek(row);
        }
        DORIS_CHECK_EQ(w.postings->doc(), row);
        w.positions.clear();
        w.postings->append_positions_with_offset(0, w.positions);
        w.loaded_row = row;
    }
    *span = {w.positions.data(), w.positions.data() + w.positions.size()};
    return Status::OK();
}

bool PhraseSetEngine::verify(Phrase& phrase, uint32_t row) {
    float frequency = 0.0F;
    THROW_IF_ERROR(phrase.verifier->verify(
            [this, &phrase, row](size_t term, index_query::PhrasePositionSpan* span) {
                return load(phrase.words[term], row, span);
            },
            /*count=*/false, &frequency));
    return frequency > 0.0F;
}

uint32_t PhraseSetEngine::next_match(uint32_t target) {
    auto it = _candidates.begin();
    it.equalorlarger(target);
    for (; it != _candidates.end(); ++it) {
        const uint32_t row = *it;
        for (Phrase& phrase : _phrases) {
            if (holds(phrase, row) && verify(phrase, row)) {
                return row;
            }
        }
    }
    return TERMINATED;
}

void PhraseSetEngine::match_rows(const roaring::Roaring* rows, roaring::Roaring* out) {
    std::optional<roaring::Roaring> narrowed;
    const roaring::Roaring* candidates = &_candidates;
    if (rows != nullptr) {
        narrowed = *rows & _candidates;
        candidates = &*narrowed;
    }
    std::vector<uint32_t> matched;
    for (const uint32_t row : *candidates) {
        for (Phrase& phrase : _phrases) {
            if (holds(phrase, row) && verify(phrase, row)) {
                matched.push_back(row);
                break;
            }
        }
    }
    out->addMany(matched.size(), matched.data());
}

namespace {

// The OR of a set's phrases over one engine.
class PhraseSetScorer final : public Scorer {
public:
    explicit PhraseSetScorer(PhraseSetEnginePtr engine)
            : _engine(std::move(engine)), _doc(_engine->next_match(0)) {}

    uint32_t advance() override {
        if (_doc != TERMINATED) {
            _doc = _engine->next_match(_doc + 1);
        }
        return _doc;
    }
    uint32_t seek(uint32_t target) override {
        if (target > _doc) {
            _doc = _engine->next_match(target);
        }
        return _doc;
    }
    uint32_t doc() const override { return _doc; }
    uint32_t size_hint() const override {
        return static_cast<uint32_t>(_engine->candidates().cardinality());
    }
    // Checking positions costs like a phrase does.
    uint64_t cost() const override { return static_cast<uint64_t>(size_hint()) * 10 * 2; }
    uint32_t norm() const override { return 1; }
    float score() override { return 1.0F; }

private:
    PhraseSetEnginePtr _engine;
    uint32_t _doc = TERMINATED;
};

index_query::IndexSourcePtr required_source(index_query::IndexSourcePtr source,
                                            const std::wstring& field) {
    if (!source) {
        throw Exception(ErrorCode::NOT_FOUND, "Reader not found for field '{}'",
                        StringHelper::to_string(field));
    }
    return source;
}

// The phrase weight of one phrase of a set, which a source batching its reads runs instead.
WeightPtr phrase_weight(const PhraseSetGroup& group, size_t phrase) {
    const PhraseSetPhrase& spec = group.phrases()[phrase];
    return std::make_shared<PhraseWeight>(
            group.field(), spec.clauses,
            index_query::PhraseQueryOptions {.slop = spec.slop, .ordered = spec.ordered},
            /*similarity=*/nullptr, /*enable_scoring=*/false, /*nullable=*/true);
}

class PhraseSetWeight final : public Weight {
public:
    explicit PhraseSetWeight(PhraseSetGroupPtr group) : _group(std::move(group)) {}

    // A row the field is NULL in is UNKNOWN, as for each phrase on its own.
    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override {
        auto source =
                required_source(lookup_source(_group->field(), ctx, binding_key), _group->field());
        ScorerPtr scorer;
        if (source->batches_reads()) {
            std::vector<ScorerPtr> scorers;
            for (size_t i = 0; i < _group->phrases().size(); ++i) {
                scorers.push_back(phrase_weight(*_group, i)->scorer(ctx, binding_key));
            }
            scorer = make_buffered_union(scorers, std::make_shared<DoNothingCombiner>());
        } else {
            scorer = std::make_shared<PhraseSetScorer>(
                    PhraseSetEngine::open(*source, _group->phrases()));
        }
        return make_nullable_scorer(scorer,
                                    logical_field_or_fallback(ctx, binding_key, _group->field()),
                                    ctx.null_resolver);
    }

    bool lists_rows(const QueryExecutionContext& ctx,
                    const std::string& binding_key) const override {
        return lookup_source(_group->field(), ctx, binding_key) != nullptr;
    }

    index_query::TruthSet listed_rows(const QueryExecutionContext& ctx,
                                      const std::string& binding_key,
                                      const roaring::Roaring* candidates) override {
        auto source =
                required_source(lookup_source(_group->field(), ctx, binding_key), _group->field());
        index_query::TruthSet result;
        if (source->batches_reads()) {
            for (size_t i = 0; i < _group->phrases().size(); ++i) {
                result.union_with(
                        phrase_weight(*_group, i)->listed_rows(ctx, binding_key, candidates));
            }
            return result;
        }
        auto engine = PhraseSetEngine::open(*source, _group->phrases(), candidates);
        engine->match_rows(candidates, &result.true_rows);
        auto nulls = FieldNullBitmapFetcher::fetch(
                ctx.null_resolver, logical_field_or_fallback(ctx, binding_key, _group->field()));
        if (nulls != nullptr) {
            result.null_rows = candidates == nullptr ? *nulls : *nulls & *candidates;
            result.null_rows -= result.true_rows;
        }
        return result;
    }

private:
    PhraseSetGroupPtr _group;
};

} // namespace

WeightPtr PhraseSetQuery::weight(bool enable_scoring) {
    DORIS_CHECK(!enable_scoring);
    return std::make_shared<PhraseSetWeight>(_group);
}

} // namespace doris::segment_v2::inverted_index::query_v2
