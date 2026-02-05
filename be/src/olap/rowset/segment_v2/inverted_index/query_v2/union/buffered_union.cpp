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

#include "olap/rowset/segment_v2/inverted_index/query_v2/union/buffered_union.h"

#include <algorithm>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

static constexpr size_t HORIZON_NUM_TINYBITSETS = 64;
static constexpr uint32_t HORIZON = static_cast<uint32_t>(64) * HORIZON_NUM_TINYBITSETS;

template <typename ScoreCombinerPtrU>
ScorerPtr make_buffered_union(const std::vector<ScorerPtr>& scorers,
                              ScoreCombinerPtrU score_combiner) {
    std::vector<ScorerPtr> non_empty_scorers;
    for (const auto& docset : scorers) {
        if (docset && docset->doc() != TERMINATED) {
            non_empty_scorers.push_back(docset);
        }
    }
    auto bitsets = std::vector<TinySet>(HORIZON_NUM_TINYBITSETS);
    auto scores = std::vector<ScoreCombinerPtrU>(HORIZON);
    std::ranges::generate(scores, [&score_combiner]() { return score_combiner->clone(); });

    std::vector<TermScorerPtr> term_scorers;
    term_scorers.reserve(non_empty_scorers.size());
    bool all_term_scorers = true;
    for (const auto& scorer : non_empty_scorers) {
        if (auto term_scorer = std::dynamic_pointer_cast<TermScorer>(scorer)) {
            term_scorers.push_back(term_scorer);
        } else {
            all_term_scorers = false;
            break;
        }
    }

    if (all_term_scorers && !term_scorers.empty()) {
        auto union_scorer = std::make_shared<BufferedUnion<TermScorerPtr, ScoreCombinerPtrU>>(
                std::move(term_scorers), bitsets, scores, HORIZON_NUM_TINYBITSETS, 0, 0);

        if (union_scorer->refill()) {
            union_scorer->advance();
        } else {
            union_scorer->_doc = TERMINATED;
        }

        return union_scorer;
    }

    auto union_scorer = std::make_shared<BufferedUnion<ScorerPtr, ScoreCombinerPtrU>>(
            std::move(non_empty_scorers), std::move(bitsets), std::move(scores),
            HORIZON_NUM_TINYBITSETS, 0, 0);

    if (union_scorer->refill()) {
        union_scorer->advance();
    } else {
        union_scorer->_doc = TERMINATED;
    }

    return union_scorer;
}

template <typename T, typename Predicate>
void unordered_drain_filter(std::vector<T>& v, Predicate predicate) {
    size_t i = 0;
    while (i < v.size()) {
        if (predicate(v[i])) {
            v[i] = std::move(v.back());
            v.pop_back();
        } else {
            i++;
        }
    }
}

template <typename ScorerT, typename ScoreCombinerPtrT, typename ScorerPtrT>
inline bool refill_scorer_predicate(ScorerT& scorer, std::vector<TinySet>& bitsets,
                                    std::vector<ScoreCombinerPtrT>& scores, uint32_t min_doc,
                                    uint32_t horizon, const ScorerPtrT& scorer_ptr) {
    while (true) {
        uint32_t doc = scorer.doc();
        if (doc >= horizon) {
            return false;
        }
        uint32_t delta = doc - min_doc;
        bitsets[static_cast<size_t>(delta / 64)].insert_mut(delta % 64);
        if constexpr (!std::is_same_v<ScoreCombinerPtrT, DoNothingCombinerPtr>) {
            scores[static_cast<size_t>(delta)]->update(scorer_ptr);
        }
        if (scorer.advance() == TERMINATED) {
            return true;
        }
    }
}

template <typename ScorerPtrT, typename ScoreCombinerPtrT>
BufferedUnion<ScorerPtrT, ScoreCombinerPtrT>::BufferedUnion(std::vector<ScorerPtrT> scorers,
                                                            std::vector<TinySet> bitsets,
                                                            std::vector<ScoreCombinerPtrT> scores,
                                                            size_t cursor, uint32_t offset,
                                                            uint32_t doc)
        : _scorers(std::move(scorers)),
          _bitsets(std::move(bitsets)),
          _scores(std::move(scores)),
          _cursor(cursor),
          _offset(offset),
          _doc(doc) {}

template <typename ScorerPtrT, typename ScoreCombinerPtrT>
bool BufferedUnion<ScorerPtrT, ScoreCombinerPtrT>::refill() {
    if (_scorers.empty()) {
        return false;
    }
    uint32_t min_doc = TERMINATED;
    for (const auto& ds : _scorers) {
        min_doc = std::min(min_doc, ds->doc());
    }
    if (min_doc == TERMINATED) {
        return false;
    }
    _offset = min_doc;
    _cursor = 0;
    _doc = min_doc;
    refill(_scorers, _bitsets, _scores, min_doc);
    return true;
}

template <typename ScorerPtrT, typename ScoreCombinerPtrT>
void BufferedUnion<ScorerPtrT, ScoreCombinerPtrT>::refill(std::vector<ScorerPtrT>& scorers,
                                                          std::vector<TinySet>& bitsets,
                                                          std::vector<ScoreCombinerPtrT>& scores,
                                                          uint32_t min_doc) {
    uint32_t horizon = min_doc + HORIZON;
    unordered_drain_filter(scorers, [&](const ScorerPtrT& scorer_ptr) -> bool {
        return refill_scorer_predicate(*scorer_ptr, bitsets, scores, min_doc, horizon, scorer_ptr);
    });
}

template <typename ScorerPtrT, typename ScoreCombinerPtrT>
bool BufferedUnion<ScorerPtrT, ScoreCombinerPtrT>::advance_buffered() {
    while (_cursor < HORIZON_NUM_TINYBITSETS) {
        auto& bitset = _bitsets[_cursor];
        if (!bitset.is_empty()) {
            uint32_t val = bitset.pop_lowest_unchecked();
            uint32_t delta = val + (static_cast<uint32_t>(_cursor) * 64);
            _doc = _offset + delta;
            if constexpr (!std::is_same_v<ScoreCombinerPtrT, DoNothingCombinerPtr>) {
                auto& score_combiner = _scores[static_cast<size_t>(delta)];
                _score = score_combiner->score();
                score_combiner->clear();
            }
            return true;
        }
        _cursor++;
    }
    return false;
}

template <typename ScorerPtrT, typename ScoreCombinerPtrT>
uint32_t BufferedUnion<ScorerPtrT, ScoreCombinerPtrT>::advance() {
    if (advance_buffered()) {
        return _doc;
    }
    if (!refill()) {
        _doc = TERMINATED;
        return TERMINATED;
    }
    if (!advance_buffered()) {
        return TERMINATED;
    }
    return _doc;
}

template <typename ScorerPtrT, typename ScoreCombinerPtrT>
uint32_t BufferedUnion<ScorerPtrT, ScoreCombinerPtrT>::seek(uint32_t target) {
    if (_doc >= target) {
        return _doc;
    }
    uint32_t gap = target - _offset;
    if (gap < HORIZON) {
        size_t new_cursor = static_cast<size_t>(gap) / 64;
        for (size_t i = _cursor; i < new_cursor; ++i) {
            _bitsets[i].clear();
        }
        for (size_t i = _cursor * 64; i < new_cursor * 64; ++i) {
            _scores[i]->clear();
        }
        _cursor = new_cursor;
        uint32_t current_doc = _doc;
        while (current_doc < target) {
            current_doc = advance();
        }
        return current_doc;
    } else {
        for (auto& tinyset : _bitsets) {
            tinyset.clear();
        }
        for (auto& score_combiner : _scores) {
            score_combiner->clear();
        }
        unordered_drain_filter(_scorers, [target](auto& docset) {
            if (docset->doc() < target) {
                docset->seek(target);
            }
            return docset->doc() == TERMINATED;
        });
        if (!refill()) {
            _doc = TERMINATED;
            return TERMINATED;
        }
        return advance();
    }
}

template <typename ScorerPtrT, typename ScoreCombinerPtrT>
uint32_t BufferedUnion<ScorerPtrT, ScoreCombinerPtrT>::doc() const {
    return _doc;
}

template <typename ScorerPtrT, typename ScoreCombinerPtrT>
uint32_t BufferedUnion<ScorerPtrT, ScoreCombinerPtrT>::size_hint() const {
    uint32_t max_hint = 0;
    for (const auto& docset : _scorers) {
        max_hint = std::max(max_hint, docset->size_hint());
    }
    return max_hint;
}

template <typename ScorerPtrT, typename ScoreCombinerPtrT>
float BufferedUnion<ScorerPtrT, ScoreCombinerPtrT>::score() {
    return _score;
}

template ScorerPtr make_buffered_union(const std::vector<ScorerPtr>& scorers,
                                       SumCombinerPtr score_combiner);
template ScorerPtr make_buffered_union(const std::vector<ScorerPtr>& scorers,
                                       DoNothingCombinerPtr score_combiner);

} // namespace doris::segment_v2::inverted_index::query_v2