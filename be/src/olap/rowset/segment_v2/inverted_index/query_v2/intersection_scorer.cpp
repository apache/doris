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

#include "olap/rowset/segment_v2/inverted_index/query_v2/intersection_scorer.h"

#include <algorithm>
#include <cassert>

#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

uint32_t go_to_first_doc(const std::vector<ScorerPtr>& docsets) {
    assert(!docsets.empty());

    uint32_t candidate = docsets[0]->doc();
    for (size_t i = 1; i < docsets.size(); ++i) {
        candidate = std::max(candidate, docsets[i]->doc());
    }

    while (true) {
        bool need_continue = false;

        for (const auto& docset : docsets) {
            uint32_t seek_doc = docset->seek(candidate);
            if (seek_doc > candidate) {
                candidate = docset->doc();
                need_continue = true;
                break;
            }
        }

        if (!need_continue) {
            return candidate;
        }
    }
}

ScorerPtr intersection_scorer_build(std::vector<ScorerPtr>& scorers) {
    if (scorers.empty()) {
        return std::make_shared<EmptyScorer>();
    } else if (scorers.size() == 1) {
        return scorers[0];
    }
    std::ranges::sort(scorers, [](const ScorerPtr& a, const ScorerPtr& b) {
        return a->size_hint() < b->size_hint();
    });
    uint32_t doc = go_to_first_doc(scorers);
    if (doc == TERMINATED) {
        return std::make_shared<EmptyScorer>();
    }
    auto left = scorers[0];
    auto right = scorers[1];
    std::vector<ScorerPtr> others(scorers.begin() + 2, scorers.end());

    auto try_create_term_intersection = [&]<typename TPtr>() -> ScorerPtr {
        if (auto l = std::dynamic_pointer_cast<typename TPtr::element_type>(left)) {
            if (auto r = std::dynamic_pointer_cast<typename TPtr::element_type>(right)) {
                return std::make_shared<IntersectionScorer<TPtr>>(l, r, others);
            }
        }
        return nullptr;
    };

    if (auto result = try_create_term_intersection.template operator()<TS_Base>()) {
        return result;
    }
    if (auto result = try_create_term_intersection.template operator()<TS_NoScore>()) {
        return result;
    }
    if (auto result = try_create_term_intersection.template operator()<TS_Empty>()) {
        return result;
    }

    return std::make_shared<IntersectionScorer<ScorerPtr>>(std::move(left), std::move(right),
                                                           std::move(others));
}

template <typename PivotScorerPtr>
IntersectionScorer<PivotScorerPtr>::IntersectionScorer(PivotScorerPtr left, PivotScorerPtr right,
                                                       std::vector<ScorerPtr> others)
        : _left(std::move(left)), _right(std::move(right)), _others(std::move(others)) {}

template <typename PivotScorerPtr>
uint32_t IntersectionScorer<PivotScorerPtr>::advance() {
    uint32_t candidate = _left->advance();

    while (true) {
        while (true) {
            uint32_t right_doc = _right->seek(candidate);
            candidate = _left->seek(right_doc);
            if (candidate == right_doc) {
                break;
            }
        }

        bool need_continue = false;
        for (const auto& docset : _others) {
            uint32_t seek_doc = docset->seek(candidate);
            if (seek_doc > candidate) {
                candidate = _left->seek(seek_doc);
                need_continue = true;
                break;
            }
        }

        if (!need_continue) {
            return candidate;
        }
    }
}

template <typename PivotScorerPtr>
uint32_t IntersectionScorer<PivotScorerPtr>::seek(uint32_t target) {
    _left->seek(target);
    std::vector<ScorerPtr> docsets;
    docsets.push_back(_left);
    docsets.push_back(_right);
    for (auto& docset : _others) {
        docsets.push_back(docset);
    }
    return go_to_first_doc(docsets);
}

template <typename PivotScorerPtr>
uint32_t IntersectionScorer<PivotScorerPtr>::doc() const {
    return _left->doc();
}

template <typename PivotScorerPtr>
uint32_t IntersectionScorer<PivotScorerPtr>::size_hint() const {
    return _left->size_hint();
}

template <typename PivotScorerPtr>
float IntersectionScorer<PivotScorerPtr>::score() {
    float total = _left->score() + _right->score();
    for (auto& scorer : _others) {
        total += scorer->score();
    }
    return total;
}

} // namespace doris::segment_v2::inverted_index::query_v2