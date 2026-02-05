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

#include "olap/rowset/segment_v2/inverted_index/query_v2/exclude_scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TDocSet, typename TDocSetExclude>
Exclude<TDocSet, TDocSetExclude>::Exclude(TDocSet underlying_docset,
                                          TDocSetExclude excluding_docset)
        : _underlying_docset(std::move(underlying_docset)),
          _excluding_docset(std::move(excluding_docset)) {
    while (_underlying_docset->doc() != TERMINATED) {
        uint32_t target = _underlying_docset->doc();
        if (!is_within(_excluding_docset, target)) {
            break;
        }
        _underlying_docset->advance();
    }
}

template <typename TDocSet, typename TDocSetExclude>
uint32_t Exclude<TDocSet, TDocSetExclude>::advance() {
    while (true) {
        uint32_t candidate = _underlying_docset->advance();
        if (candidate == TERMINATED) {
            return TERMINATED;
        }
        if (!is_within(_excluding_docset, candidate)) {
            return candidate;
        }
    }
}

template <typename TDocSet, typename TDocSetExclude>
uint32_t Exclude<TDocSet, TDocSetExclude>::seek(uint32_t target) {
    uint32_t candidate = _underlying_docset->seek(target);
    if (candidate == TERMINATED) {
        return TERMINATED;
    }
    if (!is_within(_excluding_docset, candidate)) {
        return candidate;
    }
    return advance();
}

template <typename TDocSet, typename TDocSetExclude>
uint32_t Exclude<TDocSet, TDocSetExclude>::doc() const {
    return _underlying_docset->doc();
}

template <typename TDocSet, typename TDocSetExclude>
uint32_t Exclude<TDocSet, TDocSetExclude>::size_hint() const {
    return _underlying_docset->size_hint();
}

template <typename TDocSet, typename TDocSetExclude>
float Exclude<TDocSet, TDocSetExclude>::score() {
    if constexpr (std::is_base_of_v<Scorer, typename TDocSet::element_type>) {
        return _underlying_docset->score();
    }
    return 0.0F;
}

ScorerPtr make_exclude(ScorerPtr underlying, ScorerPtr excluding) {
    return std::make_shared<Exclude<ScorerPtr, ScorerPtr>>(std::move(underlying),
                                                           std::move(excluding));
}

template class Exclude<ScorerPtr, ScorerPtr>;

} // namespace doris::segment_v2::inverted_index::query_v2