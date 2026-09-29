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

#include "storage/index/inverted/query_v2/phrase_query/phrase_weight.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query_v2/complete_null_bitmap.h"
#include "storage/index/inverted/query_v2/const_score_query/const_score_scorer.h"
#include "storage/index/inverted/query_v2/nullable_scorer.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_scorer.h"
#include "storage/index/inverted/query_v2/segment_postings.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/index/query/exec/cursor_chained_postings.h"
#include "storage/index/query/phrase/exact_phrase_matcher.h"
#include "storage/index/query/phrase/phrase_verifier.h"
#include "storage/index/query/phrase/position_span.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::segment_v2::inverted_index::query_v2 {

PhraseWeight::PhraseWeight(std::wstring field, std::vector<TermInfo> term_infos,
                           index_query::PhraseQueryOptions options,
                           index_query::ScoringContextPtr<float> similarity, bool enable_scoring,
                           bool nullable)
        : _field(std::move(field)),
          _term_infos(std::move(term_infos)),
          _options(options),
          _similarity(std::move(similarity)),
          _enable_scoring(enable_scoring),
          _nullable(nullable) {}

index_query::IndexSourcePtr PhraseWeight::_source(const QueryExecutionContext& ctx,
                                                  const std::string& binding_key) const {
    auto source = lookup_source(_field, ctx, binding_key);
    if (!source) {
        throw Exception(ErrorCode::NOT_FOUND, "Reader not found for field '{}'",
                        StringHelper::to_string(_field));
    }
    return source;
}

ScorerPtr PhraseWeight::scorer(const QueryExecutionContext& ctx, const std::string& binding_key) {
    auto source = _source(ctx, binding_key);
    ScorerPtr scorer = _lists(*source) ? _listed_scorer(*source, _options.candidates)
                                       : _streamed_scorer(*source, ctx.segment_num_rows);
    if (_nullable) {
        auto logical_field = logical_field_or_fallback(ctx, binding_key, _field);
        return make_nullable_scorer(scorer, logical_field, ctx.null_resolver);
    }
    return scorer;
}

bool PhraseWeight::lists_rows(const QueryExecutionContext& ctx,
                              const std::string& binding_key) const {
    auto source = lookup_source(_field, ctx, binding_key);
    return source != nullptr && _lists(*source);
}

index_query::TruthSet PhraseWeight::listed_rows(const QueryExecutionContext& ctx,
                                                const std::string& binding_key,
                                                const roaring::Roaring* candidates) {
    auto source = _source(ctx, binding_key);
    DORIS_CHECK(_lists(*source));
    // The scan's candidates narrow the conjunction's for the rows listed; the UNKNOWN rows
    // are the field's among the conjunction's candidates, as the streamed phrase reports them.
    const roaring::Roaring* listed = candidates;
    std::optional<roaring::Roaring> narrowed;
    if (_options.candidates != nullptr) {
        if (candidates == nullptr) {
            listed = _options.candidates;
        } else {
            narrowed = *candidates & *_options.candidates;
            listed = &*narrowed;
        }
    }
    index_query::TruthSet result;
    collect_true_rows(_listed_scorer(*source, listed), &result.true_rows);
    if (_nullable) {
        auto nulls = FieldNullBitmapFetcher::fetch(
                ctx.null_resolver, logical_field_or_fallback(ctx, binding_key, _field));
        if (nulls != nullptr) {
            result.null_rows = candidates == nullptr ? *nulls : *nulls & *candidates;
        }
    }
    return result;
}

ScorerPtr PhraseWeight::_streamed_scorer(index_query::IndexSource& source, uint32_t num_docs) {
    std::vector<std::pair<size_t, SegmentPostingsPtr>> term_postings_list;
    for (const auto& term_info : _term_infos) {
        size_t offset = term_info.position;
        auto posting = open_postings(source, term_info.get_single_term(),
                                     /*positions=*/true, _enable_scoring, _similarity);
        if (posting) {
            term_postings_list.emplace_back(offset, std::move(posting));
        } else {
            return std::make_shared<EmptyScorer>();
        }
    }
    return PhraseScorer<SegmentPostingsPtr>::create(term_postings_list, _similarity, _options,
                                                    num_docs);
}

namespace {

// The rows holding every term, among `candidates` when given, listed as a chain.
Status chain_rows(std::span<const std::unique_ptr<index_query::PostingsCursor>> cursors,
                  const roaring::Roaring* candidates, std::vector<uint32_t>* rows) {
    std::vector<uint32_t> initial;
    const std::vector<uint32_t>* initial_candidates = nullptr;
    if (candidates != nullptr) {
        initial.resize(candidates->cardinality());
        candidates->toUint32Array(initial.data());
        initial_candidates = &initial;
    }
    std::vector<index_query::CursorChainedPostings> chained;
    chained.reserve(cursors.size());
    std::vector<index_query::ChainedPostings*> chain;
    for (const auto& cursor : cursors) {
        chained.emplace_back(*cursor);
        chain.push_back(&chained.back());
    }
    return index_query::chained_conjunction(chain, initial_candidates, rows);
}

// One term walked over the listed rows in ascending order. Entering a block reads the
// positions of every listed row it holds in one call; they stay viewed until the walk leaves
// the block.
class TermWalk {
public:
    TermWalk(index_query::PostingsCursor& cursor, std::span<const uint32_t> rows)
            : _cursor(cursor), _rows(rows) {}

    Status positions_of(size_t row, index_query::PhrasePositionSpan* span) {
        if (row >= _end) {
            RETURN_IF_ERROR(_enter_block(row));
        }
        const size_t chosen = row - _begin;
        *span = {_positions.flat.data() + _positions.offsets[chosen],
                 _positions.flat.data() + _positions.offsets[chosen + 1]};
        return Status::OK();
    }

private:
    // The chain kept only rows the term holds, so the listed rows in the block are some of its
    // documents, and all of them when they are as many.
    Status _enter_block(size_t row) {
        bool eof = false;
        RETURN_IF_ERROR(_cursor.seek_block(_rows[row], &_block, &eof));
        DORIS_CHECK(!eof);
        const uint32_t last = _block.doc_at(_block.size() - 1);
        _begin = row;
        _end = index_query::gallop_past(_rows, row, [last](uint32_t doc) { return doc <= last; });
        const size_t count = _end - _begin;
        if (count == _block.size()) {
            for (auto ordinal = static_cast<uint32_t>(_every.size()); ordinal < count; ++ordinal) {
                _every.push_back(ordinal);
            }
            return _cursor.block_positions(std::span(_every).first(count), &_buffer, &_positions);
        }
        _ordinals.clear();
        size_t ordinal = 0;
        for (size_t listed = _begin; listed < _end; ++listed) {
            const uint32_t doc = _rows[listed];
            if (_block.dense) {
                _ordinals.push_back(doc - _block.range_begin);
                continue;
            }
            while (_block.docs[ordinal] < doc) {
                ++ordinal;
            }
            DCHECK_EQ(_block.docs[ordinal], doc);
            _ordinals.push_back(static_cast<uint32_t>(ordinal++));
        }
        return _cursor.block_positions(_ordinals, &_buffer, &_positions);
    }

    index_query::PostingsCursor& _cursor;
    std::span<const uint32_t> _rows;
    index_query::PostingsBlock _block;
    // 0, 1, 2, ...: the ordinals of a block whose every document is listed.
    std::vector<uint32_t> _every;
    std::vector<uint32_t> _ordinals;
    index_query::PositionsBuffer _buffer;
    index_query::BlockPositions _positions;
    size_t _begin = 0;
    size_t _end = 0;
};

// The phrase's shape: which distinct term each clause reads and where it sits.
struct PhraseClauses {
    std::vector<size_t> terms;
    std::vector<uint32_t> offsets;
    std::vector<uint64_t> costs;
};

// The rows the phrase matches, each verified over the positions its terms hold there. Two
// distinct terms of an exact phrase check directly; every other shape goes through the shared
// verifier, which may skip a term on a row it already rejected.
Status verify_rows(std::span<const std::unique_ptr<index_query::PostingsCursor>> cursors,
                   std::span<const uint32_t> rows, const PhraseClauses& clauses,
                   const index_query::PhraseQueryOptions& options, std::vector<uint32_t>* matched) {
    std::vector<TermWalk> walks;
    walks.reserve(cursors.size());
    for (const auto& cursor : cursors) {
        RETURN_IF_ERROR(cursor->rewind());
        walks.emplace_back(*cursor, rows);
    }
    if (options.slop == 0 && clauses.terms.size() == 2 && clauses.terms[0] == 0 &&
        clauses.terms[1] == 1) {
        const uint32_t delta = clauses.offsets[1] - clauses.offsets[0];
        for (size_t row = 0; row < rows.size(); ++row) {
            index_query::PhrasePositionSpan left;
            index_query::PhrasePositionSpan right;
            RETURN_IF_ERROR(walks[0].positions_of(row, &left));
            RETURN_IF_ERROR(walks[1].positions_of(row, &right));
            if (index_query::contains_two_term_phrase(left, right, delta)) {
                matched->push_back(rows[row]);
            }
        }
        return Status::OK();
    }
    index_query::PhraseVerifier verifier(clauses.terms, clauses.offsets, clauses.costs,
                                         options.slop, options.ordered);
    for (size_t row = 0; row < rows.size(); ++row) {
        const auto load = [&walks, row](size_t term, index_query::PhrasePositionSpan* span) {
            return walks[term].positions_of(row, span);
        };
        float frequency = 0.0F;
        RETURN_IF_ERROR(verifier.verify(load, /*collect_frequency=*/false, &frequency));
        if (frequency > 0.0F) {
            matched->push_back(rows[row]);
        }
    }
    return Status::OK();
}

} // namespace

ScorerPtr PhraseWeight::_listed_scorer(index_query::IndexSource& source,
                                       const roaring::Roaring* candidates) {
    // Every distinct term opens once; the clauses repeating it share its positions.
    std::vector<std::string> terms;
    PhraseClauses clauses;
    for (const auto& term_info : _term_infos) {
        const std::string& term = term_info.get_single_term();
        const auto found = std::ranges::find(terms, term);
        clauses.terms.push_back(static_cast<size_t>(found - terms.begin()));
        clauses.offsets.push_back(static_cast<uint32_t>(term_info.position));
        if (found == terms.end()) {
            terms.push_back(term);
        }
    }
    std::vector<std::unique_ptr<index_query::PostingsCursor>> cursors;
    THROW_IF_ERROR(source.open_terms(terms, /*positions=*/true, /*scoring=*/false, &cursors));
    if (std::ranges::any_of(cursors, [](const auto& cursor) { return cursor == nullptr; })) {
        return std::make_shared<EmptyScorer>();
    }
    for (const size_t term : clauses.terms) {
        clauses.costs.push_back(cursors[term]->doc_freq());
    }
    std::vector<uint32_t> rows;
    THROW_IF_ERROR(chain_rows(cursors, candidates, &rows));
    if (rows.empty()) {
        return std::make_shared<EmptyScorer>();
    }
    // The rows' positions, read in one round, then every row verified as the terms are walked.
    for (const auto& cursor : cursors) {
        THROW_IF_ERROR(cursor->prefetch(&rows, /*positions=*/true));
    }
    THROW_IF_ERROR(source.fetch_pending());
    std::vector<uint32_t> matched;
    THROW_IF_ERROR(verify_rows(cursors, rows, clauses, _options, &matched));
    auto matched_rows = std::make_shared<roaring::Roaring>();
    matched_rows->addMany(matched.size(), matched.data());
    return std::make_shared<ConstScoreScorer<BitSetScorerPtr>>(
            std::make_shared<BitSetScorer>(std::move(matched_rows)));
}

} // namespace doris::segment_v2::inverted_index::query_v2
