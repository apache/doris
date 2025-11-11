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

#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/postings_with_offset.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TDocSet, typename TOtherDocSet>
template <typename T>
std::enable_if_t<std::is_same_v<TDocSet, T>, IntersectionPtr<TDocSet, TDocSet>>
Intersection<TDocSet, TOtherDocSet>::create(std::vector<TDocSet>& docsets) {
    size_t num_docsets = docsets.size();
    if (num_docsets < 2) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "At least 2 docsets are required for intersection");
    }

    std::sort(docsets.begin(), docsets.end(),
              [](const TDocSet& a, const TDocSet& b) { return a->size_hint() < b->size_hint(); });
    go_to_first_doc(docsets);

    TDocSet left = std::move(docsets[0]);
    TDocSet right = std::move(docsets[1]);
    docsets.erase(docsets.begin(), docsets.begin() + 2);
    return std::make_shared<Intersection<TDocSet, TDocSet>>(std::move(left), std::move(right),
                                                            std::move(docsets));
}

template <typename TDocSet, typename TOtherDocSet>
Intersection<TDocSet, TOtherDocSet>::Intersection(TDocSet left, TDocSet right,
                                                  std::vector<TOtherDocSet> others)
        : _left(std::move(left)), _right(std::move(right)), _others(std::move(others)) {}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::advance() {
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

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::seek(uint32_t target) {
    _left->seek(target);
    std::vector<TDocSet> docsets;
    docsets.push_back(_left);
    docsets.push_back(_right);
    for (auto& docset : _others) {
        docsets.push_back(docset);
    }
    return go_to_first_doc(docsets);
}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::doc() const {
    return _left->doc();
}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::size_hint() const {
    return _left->size_hint();
}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::norm() const {
    return _left->norm();
}

template <typename TDocSet, typename TOtherDocSet>
uint32_t Intersection<TDocSet, TOtherDocSet>::go_to_first_doc(const std::vector<TDocSet>& docsets) {
    if (docsets.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "At least 1 docset is required for intersection");
    }

    uint32_t candidate = docsets.front()->doc();
    for (size_t i = 1; i < docsets.size(); ++i) {
        candidate = std::max(candidate, docsets[i]->seek(candidate));
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

#define INSTANTIATE_INTERSECTION(T)                                        \
    template class Intersection<T, T>;                                     \
    template std::enable_if_t<std::is_same_v<T, T>, IntersectionPtr<T, T>> \
            Intersection<T, T>::create<T>(std::vector<T> & docsets);       \
    template std::enable_if_t<std::is_same_v<T, T>, T&>                    \
    Intersection<T, T>::docset_mut_specialized<T>(size_t ord);

INSTANTIATE_INTERSECTION(std::shared_ptr<PostingsWithOffset<PostingsPtr>>)
INSTANTIATE_INTERSECTION(std::shared_ptr<PostingsWithOffset<PositionPostingsPtr>>)
INSTANTIATE_INTERSECTION(MockDocSetPtr)

#undef INSTANTIATE_INTERSECTION

} // namespace doris::segment_v2::inverted_index::query_v2