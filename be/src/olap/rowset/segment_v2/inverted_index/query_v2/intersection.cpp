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

#include "olap/rowset/segment_v2/inverted_index/query_v2/intersection.h"

#include "common/status.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/postings_with_offset.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/size_hint.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename T, typename = void>
struct is_scorer_ptr : std::false_type {};

template <typename T>
struct is_scorer_ptr<T, std::void_t<typename T::element_type>>
        : std::is_base_of<Scorer, typename T::element_type> {};

template <typename T>
inline constexpr bool is_scorer_ptr_v = is_scorer_ptr<T>::value;

template <typename TDocSetPtr>
uint32_t go_to_first_doc(const std::vector<TDocSetPtr>& docsets) {
    if (docsets.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "At least 1 docset is required for intersection");
    }

    uint32_t candidate = docsets.front()->doc();
    for (size_t i = 1; i < docsets.size(); ++i) {
        candidate = std::max(candidate, docsets[i]->doc());
    }

outer:
    while (true) {
        for (const auto& docset : docsets) {
            if (docset->doc() < candidate) {
                uint32_t seek_doc = docset->seek(candidate);
                if (seek_doc > candidate) {
                    candidate = docset->doc();
                    goto outer;
                }
            }
        }
        return candidate;
    }
}

ScorerPtr make_intersect_scorers(std::vector<ScorerPtr> scorers, uint32_t num_docs) {
    if (scorers.empty()) {
        return std::make_shared<EmptyScorer>();
    }
    if (scorers.size() == 1) {
        return std::move(scorers[0]);
    }
    std::ranges::sort(scorers,
                      [](const ScorerPtr& a, const ScorerPtr& b) { return a->cost() < b->cost(); });
    uint32_t doc = go_to_first_doc(scorers);
    if (doc == TERMINATED) {
        return std::make_shared<EmptyScorer>();
    }

    auto left = scorers[0];
    auto right = scorers[1];
    std::vector<ScorerPtr> others(std::make_move_iterator(scorers.begin() + 2),
                                  std::make_move_iterator(scorers.end()));

    auto left_term = std::dynamic_pointer_cast<TermScorer>(left);
    auto right_term = std::dynamic_pointer_cast<TermScorer>(right);
    if (left_term && right_term) {
        return std::make_shared<Intersection<TermScorerPtr, ScorerPtr>>(
                std::move(left_term), std::move(right_term), std::move(others), num_docs);
    }
    return std::make_shared<Intersection<ScorerPtr, ScorerPtr>>(std::move(left), std::move(right),
                                                                std::move(others), num_docs);
}

template <typename TDocSet, typename TOtherDocSet>
template <typename T>
std::enable_if_t<std::is_same_v<TDocSet, T>, IntersectionPtr<TDocSet, TDocSet>>
Intersection<TDocSet, TOtherDocSet>::create(std::vector<TDocSet>& docsets, uint32_t num_docs) {
    size_t num_docsets = docsets.size();
    if (num_docsets < 2) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "At least 2 docsets are required for intersection");
    }

    std::sort(docsets.begin(), docsets.end(),
              [](const TDocSet& a, const TDocSet& b) { return a->cost() < b->cost(); });
    go_to_first_doc(docsets);

    TDocSet left = std::move(docsets[0]);
    TDocSet right = std::move(docsets[1]);
    docsets.erase(docsets.begin(), docsets.begin() + 2);
    return std::make_shared<Intersection<TDocSet, TDocSet>>(std::move(left), std::move(right),
                                                            std::move(docsets), num_docs);
}

template <typename TDocSet, typename TOtherDocSet>
Intersection<TDocSet, TOtherDocSet>::Intersection(TDocSet left, TDocSet right,
                                                  std::vector<TOtherDocSet> others,
                                                  uint32_t num_docs)
        : _left(std::move(left)),
          _right(std::move(right)),
          _others(std::move(others)),
          _num_docs(num_docs) {}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::advance() {
    return intersect_from(_left->advance());
}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::seek(uint32_t target) {
    _left->seek(target);
    uint32_t candidate = std::max(_left->doc(), _right->doc());
    for (const auto& docset : _others) {
        candidate = std::max(candidate, docset->doc());
    }
    return intersect_from(candidate);
}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::doc() const {
    return _left->doc();
}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::size_hint() const {
    std::vector<uint32_t> sizes;
    sizes.reserve(2 + _others.size());
    sizes.push_back(_left->size_hint());
    sizes.push_back(_right->size_hint());
    for (const auto& docset : _others) {
        sizes.push_back(docset->size_hint());
    }
    return estimate_intersection(sizes, _num_docs);
}

template <typename TDocSet, typename TOtherDocSet>
uint64_t Intersection<TDocSet, TOtherDocSet>::cost() const {
    return _left->cost();
}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::norm() const {
    return _left->norm();
}

template <typename TDocSet, typename TOtherDocSet>
float Intersection<TDocSet, TOtherDocSet>::score() {
    if constexpr (is_scorer_ptr_v<TDocSet>) {
        return _left->score() + _right->score() +
               std::accumulate(_others.begin(), _others.end(), 0.0F,
                               [](float sum, const auto& scorer) { return sum + scorer->score(); });
    } else {
        return 0.0F;
    }
}

template <typename TDocSet, typename TOtherDocSet>
template <typename T>
std::enable_if_t<std::is_same_v<TDocSet, T>, TDocSet&>
Intersection<TDocSet, TOtherDocSet>::docset_mut_specialized(size_t ord) {
    switch (ord) {
    case 0:
        return _left;
    case 1:
        return _right;
    default:
        return _others[ord - 2];
    }
}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::intersect_from(uint32_t candidate) {
left_right_intersection:
    while (true) {
        uint32_t right_doc = _right->seek(candidate);
        if (right_doc != candidate) {
            candidate = _left->seek(right_doc);
            if (candidate != right_doc) {
                continue;
            }
        }
        break;
    }

    for (const auto& docset : _others) {
        if (docset->doc() < candidate) {
            uint32_t seek_doc = docset->seek(candidate);
            if (seek_doc > candidate) {
                candidate = _left->seek(seek_doc);
                goto left_right_intersection;
            }
        }
    }

    return candidate;
}

#define INSTANTIATE_INTERSECTION(T)                                             \
    template class Intersection<T, T>;                                          \
    template std::enable_if_t<std::is_same_v<T, T>, IntersectionPtr<T, T>>      \
    Intersection<T, T>::create<T>(std::vector<T> & docsets, uint32_t num_docs); \
    template std::enable_if_t<std::is_same_v<T, T>, T&>                         \
    Intersection<T, T>::docset_mut_specialized<T>(size_t ord);

INSTANTIATE_INTERSECTION(std::shared_ptr<PostingsWithOffset<PostingsPtr>>)
INSTANTIATE_INTERSECTION(std::shared_ptr<PostingsWithOffset<SegmentPostingsPtr>>)
INSTANTIATE_INTERSECTION(MockDocSetPtr)

#undef INSTANTIATE_INTERSECTION

} // namespace doris::segment_v2::inverted_index::query_v2