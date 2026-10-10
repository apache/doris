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

#include "storage/index/inverted/query_v2/buffered_union_scorer.h"

#include <algorithm>
#include <type_traits>

#include "storage/index/inverted/query_v2/complete_null_bitmap.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScoreCombinerPtrT>
UnionScorer<ScoreCombinerPtrT>::UnionScorer(std::vector<ScorerPtr> scorers,
                                            ScoreCombinerPtrT score_combiner,
                                            uint32_t /*segment_num_rows*/,
                                            const NullBitmapResolver* resolver)
        : _scorers(std::move(scorers)), _score_combiner(std::move(score_combiner)) {
    _null_bitmap = complete_null_bitmap(
            _scorers, false, !std::is_same_v<ScoreCombinerPtrT, DoNothingCombinerPtr>, resolver);

    for (size_t idx = 0; idx < _scorers.size(); ++idx) {
        auto& scorer = _scorers[idx];
        if (scorer && scorer->doc() != TERMINATED) {
            _heap.push(HeapEntry {scorer->doc(), idx});
        }
    }

    if (!_prepare_next()) {
        _doc = TERMINATED;
    }
}

template <typename ScoreCombinerPtrT>
uint32_t UnionScorer<ScoreCombinerPtrT>::advance() {
    if (_doc == TERMINATED) {
        return TERMINATED;
    }
    if (!_prepare_next()) {
        _doc = TERMINATED;
        return TERMINATED;
    }
    return _doc;
}

template <typename ScoreCombinerPtrT>
uint32_t UnionScorer<ScoreCombinerPtrT>::seek(uint32_t target) {
    if (_doc == TERMINATED || target <= _doc) {
        return _doc;
    }
    while (_doc < target && _doc != TERMINATED) {
        advance();
    }
    return _doc;
}

template <typename ScoreCombinerPtrT>
uint32_t UnionScorer<ScoreCombinerPtrT>::size_hint() const {
    uint32_t hint = 0;
    for (const auto& scorer : _scorers) {
        hint = std::max(hint, scorer ? scorer->size_hint() : 0U);
    }
    return hint;
}

template <typename ScoreCombinerPtrT>
bool UnionScorer<ScoreCombinerPtrT>::_prepare_next() {
    if (_heap.empty()) {
        return false;
    }

    auto combiner = _score_combiner->clone();
    uint32_t current_doc = TERMINATED;
    std::vector<size_t> consumed_indices;

    while (!_heap.empty()) {
        auto entry = _heap.top();
        if (current_doc == TERMINATED) {
            current_doc = entry.doc;
        }
        if (entry.doc != current_doc) {
            break;
        }
        _heap.pop();
        consumed_indices.push_back(entry.index);
    }

    if (current_doc == TERMINATED) {
        return false;
    }

    for (auto idx : consumed_indices) {
        auto& child = _scorers[idx];
        combiner->update(child);
        if (child->advance() != TERMINATED) {
            _heap.push(HeapEntry {child->doc(), idx});
        }
    }

    _doc = current_doc;
    _current_score = combiner->score();
    return true;
}

template <typename ScoreCombinerPtrT>
ScorerPtr buffered_union_scorer_build(std::vector<ScorerPtr> scorers,
                                      ScoreCombinerPtrT score_combiner, uint32_t segment_num_rows,
                                      const NullBitmapResolver* resolver) {
    return std::make_shared<UnionScorer<ScoreCombinerPtrT>>(
            std::move(scorers), std::move(score_combiner), segment_num_rows, resolver);
}

template ScorerPtr buffered_union_scorer_build(std::vector<ScorerPtr> scorers,
                                               SumCombinerPtr score_combiner,
                                               uint32_t segment_num_rows,
                                               const NullBitmapResolver* resolver);
template ScorerPtr buffered_union_scorer_build(std::vector<ScorerPtr> scorers,
                                               DoNothingCombinerPtr score_combiner,
                                               uint32_t segment_num_rows,
                                               const NullBitmapResolver* resolver);

} // namespace doris::segment_v2::inverted_index::query_v2
