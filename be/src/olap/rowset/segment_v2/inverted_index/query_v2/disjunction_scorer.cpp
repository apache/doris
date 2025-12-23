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

#include "olap/rowset/segment_v2/inverted_index/query_v2/disjunction_scorer.h"

#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScoreCombinerPtrT>
DisjunctionScorer<ScoreCombinerPtrT>::DisjunctionScorer(std::vector<ScorerPtr> scorers,
                                                        ScoreCombinerPtrT score_combiner,
                                                        size_t minimum_matches_required)
        : _minimum_matches_required(minimum_matches_required),
          _score_combiner(std::move(score_combiner)) {
    DCHECK(minimum_matches_required > 1) << "union scorer works better if just one match required";

    for (auto& scorer : scorers) {
        if (scorer && scorer->doc() != TERMINATED) {
            _size_hint = std::max(_size_hint, scorer->size_hint());
            _heap.emplace(std::move(scorer));
        }
    }

    if (_minimum_matches_required > _heap.size()) {
        return;
    }

    do_advance();
}

template <typename ScoreCombinerPtrT>
void DisjunctionScorer<ScoreCombinerPtrT>::do_advance() {
    size_t current_num_matches = 0;
    while (!_heap.empty()) {
        ScorerWrapper candidate = std::move(const_cast<ScorerWrapper&>(_heap.top()));
        _heap.pop();

        uint32_t next = candidate.current_doc;
        if (next == TERMINATED) {
            continue;
        }

        if (_current_doc != next) {
            if (current_num_matches >= _minimum_matches_required) {
                _heap.push(std::move(candidate));
                _current_score = _score_combiner->score();
                return;
            }
            current_num_matches = 0;
            _current_doc = next;
            _score_combiner->clear();
        }

        current_num_matches++;
        _score_combiner->update(candidate.scorer);

        candidate.current_doc = candidate.scorer->advance();
        _heap.push(std::move(candidate));
    }

    if (current_num_matches < _minimum_matches_required) {
        _current_doc = TERMINATED;
    }
    _current_score = _score_combiner->score();
}

template <typename ScoreCombinerPtrT>
uint32_t DisjunctionScorer<ScoreCombinerPtrT>::advance() {
    if (_current_doc == TERMINATED) {
        return TERMINATED;
    }
    do_advance();
    return _current_doc;
}

template <typename ScoreCombinerPtrT>
uint32_t DisjunctionScorer<ScoreCombinerPtrT>::seek(uint32_t target) {
    if (_current_doc == TERMINATED) {
        return TERMINATED;
    }
    while (_current_doc < target && _current_doc != TERMINATED) {
        do_advance();
    }
    return _current_doc;
}

template <typename ScoreCombinerPtrT>
uint32_t DisjunctionScorer<ScoreCombinerPtrT>::size_hint() const {
    return _size_hint;
}

template <typename ScoreCombinerPtrT>
ScorerPtr make_disjunction(std::vector<ScorerPtr> scorers, ScoreCombinerPtrT score_combiner,
                           size_t minimum_matches_required) {
    if (scorers.empty()) {
        return std::make_shared<EmptyScorer>();
    }
    if (minimum_matches_required > scorers.size()) {
        return std::make_shared<EmptyScorer>();
    }
    return std::make_shared<DisjunctionScorer<ScoreCombinerPtrT>>(
            std::move(scorers), std::move(score_combiner), minimum_matches_required);
}

template class DisjunctionScorer<SumCombinerPtr>;
template class DisjunctionScorer<DoNothingCombinerPtr>;

template ScorerPtr make_disjunction(std::vector<ScorerPtr> scorers, SumCombinerPtr score_combiner,
                                    size_t minimum_matches_required);
template ScorerPtr make_disjunction(std::vector<ScorerPtr> scorers,
                                    DoNothingCombinerPtr score_combiner,
                                    size_t minimum_matches_required);

} // namespace doris::segment_v2::inverted_index::query_v2