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

#include "storage/index/inverted/query_v2/intersection_scorer.h"

#include <algorithm>
#include <limits>

#include "storage/index/inverted/query_v2/complete_null_bitmap.h"

namespace doris::segment_v2::inverted_index::query_v2 {

ScorerPtr intersection_scorer_build(std::vector<ScorerPtr> scorers, bool enable_scoring,
                                    const NullBitmapResolver* resolver) {
    return std::make_shared<AndScorer>(std::move(scorers), enable_scoring, resolver);
}

AndScorer::AndScorer(std::vector<ScorerPtr> scorers, bool enable_scoring,
                     const NullBitmapResolver* resolver)
        : _scorers(std::move(scorers)), _enable_scoring(enable_scoring) {
    _null_bitmap = complete_null_bitmap(_scorers, true, enable_scoring, resolver);
    if (_scorers.empty()) {
        _doc = TERMINATED;
        return;
    }

    uint32_t initial_candidate = 0;
    for (const auto& scorer : _scorers) {
        if (!scorer || scorer->doc() == TERMINATED) {
            _doc = TERMINATED;
            return;
        }
        initial_candidate = std::max(initial_candidate, scorer->doc());
    }

    if (!_advance_to(initial_candidate)) {
        _doc = TERMINATED;
    }
}

uint32_t AndScorer::advance() {
    if (_doc == TERMINATED) {
        return TERMINATED;
    }
    if (_scorers.empty() || _scorers.front()->advance() == TERMINATED) {
        _doc = TERMINATED;
        return TERMINATED;
    }
    uint32_t target = _scorers.front()->doc();
    if (!_advance_to(target)) {
        _doc = TERMINATED;
        return TERMINATED;
    }
    return _doc;
}

uint32_t AndScorer::seek(uint32_t target) {
    if (_doc == TERMINATED || target <= _doc) {
        return _doc;
    }
    if (_scorers.empty()) {
        _doc = TERMINATED;
        return TERMINATED;
    }
    if (_scorers.front()->seek(target) == TERMINATED) {
        _doc = TERMINATED;
        return TERMINATED;
    }
    uint32_t real_target = _scorers.front()->doc();
    if (!_advance_to(real_target)) {
        _doc = TERMINATED;
        return TERMINATED;
    }
    return _doc;
}

uint32_t AndScorer::size_hint() const {
    uint32_t hint = std::numeric_limits<uint32_t>::max();
    for (const auto& scorer : _scorers) {
        hint = std::min(hint, scorer ? scorer->size_hint() : 0U);
    }
    return hint == std::numeric_limits<uint32_t>::max() ? 0 : hint;
}

bool AndScorer::_advance_to(uint32_t target) {
    uint32_t candidate = target;

    while (candidate != TERMINATED) {
        bool all_match = true;
        uint32_t next_candidate = std::numeric_limits<uint32_t>::max();

        for (auto& scorer : _scorers) {
            uint32_t doc = scorer->doc();
            if (doc < candidate) {
                doc = scorer->seek(candidate);
            }
            if (doc == TERMINATED) {
                _doc = TERMINATED;
                return false;
            }
            if (doc > candidate) {
                next_candidate = std::min(next_candidate, doc);
                all_match = false;
            }
        }

        if (all_match) {
            _doc = candidate;
            _current_score = 0.0F;
            if (_enable_scoring) {
                for (const auto& scorer : _scorers) {
                    _current_score += scorer->score();
                }
            }
            return true;
        }

        if (next_candidate == std::numeric_limits<uint32_t>::max()) {
            break;
        }
        candidate = next_candidate;
    }

    _doc = TERMINATED;
    return false;
}

AndNotScorer::AndNotScorer(ScorerPtr include, std::vector<ScorerPtr> excludes,
                           const NullBitmapResolver* resolver)
        : _include(std::move(include)) {
    if (_include && _include->has_null_bitmap(resolver)) {
        const auto* null_bitmap = _include->get_null_bitmap(resolver);
        if (null_bitmap != nullptr) {
            _null_bitmap |= *null_bitmap;
        }
    }

    for (auto& scorer : excludes) {
        if (!scorer) {
            continue;
        }
        while (scorer->doc() != TERMINATED) {
            _exclude_true.add(scorer->doc());
            scorer->advance();
        }
        if (scorer->has_null_bitmap(resolver)) {
            const auto* null_bitmap = scorer->get_null_bitmap(resolver);
            if (null_bitmap != nullptr) {
                _exclude_null |= *null_bitmap;
            }
        }
    }

    _exclude_null -= _exclude_true;
    _null_bitmap -= _exclude_true;
    if (_include != nullptr && !_exclude_null.isEmpty()) {
        _include = materialize_scorer(std::move(_include), true, resolver);
        _null_bitmap |= *_include->get_true_bitmap() & _exclude_null;
    }

    if (_include == nullptr || _include->doc() == TERMINATED) {
        _doc = TERMINATED;
        return;
    }

    if (!_advance_to(_include->doc())) {
        _doc = TERMINATED;
    }
}

uint32_t AndNotScorer::advance() {
    if (_doc == TERMINATED || !_include) {
        return TERMINATED;
    }
    if (_include->advance() == TERMINATED) {
        _doc = TERMINATED;
        return TERMINATED;
    }
    if (_advance_to(_include->doc())) {
        return _doc;
    }
    _doc = TERMINATED;
    return TERMINATED;
}

uint32_t AndNotScorer::seek(uint32_t target) {
    if (_doc == TERMINATED || !_include || target <= _doc) {
        return _doc;
    }
    if (_include->seek(target) == TERMINATED) {
        _doc = TERMINATED;
        return TERMINATED;
    }
    if (_advance_to(_include->doc())) {
        return _doc;
    }
    _doc = TERMINATED;
    return TERMINATED;
}

uint32_t AndNotScorer::size_hint() const {
    return _include ? _include->size_hint() : 0U;
}

bool AndNotScorer::_advance_to(uint32_t target) {
    if (!_include) {
        return false;
    }

    uint32_t current = target;
    while (current != TERMINATED) {
        uint32_t doc = _include->doc();
        if (doc < current) {
            doc = _include->seek(current);
        }
        if (doc == TERMINATED) {
            return false;
        }

        bool in_exclude_true = _exclude_true.contains(doc);
        bool in_exclude_null = _exclude_null.contains(doc);

        if (in_exclude_true) {
            current = doc + 1;
            continue;
        }

        if (in_exclude_null) {
            current = doc + 1;
            continue;
        }

        _doc = doc;
        _current_score = _include->score();
        return true;
    }

    return false;
}

} // namespace doris::segment_v2::inverted_index::query_v2
