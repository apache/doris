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
#include <limits>

namespace doris::segment_v2::inverted_index::query_v2 {

ScorerPtr intersection_scorer_build(std::vector<ScorerPtr> scorers, bool enable_scoring,
                                    const NullBitmapResolver* resolver) {
    return std::make_shared<AndScorer>(std::move(scorers), enable_scoring, resolver);
}

AndScorer::AndScorer(std::vector<ScorerPtr> scorers, bool enable_scoring,
                     const NullBitmapResolver* resolver)
        : _scorers(std::move(scorers)), _enable_scoring(enable_scoring), _resolver(resolver) {
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

bool AndScorer::has_null_bitmap(const NullBitmapResolver* resolver) {
    if (resolver != nullptr) {
        _resolver = resolver;
    }
    if (!_null_sources_checked) {
        _null_sources_checked = true;
        if (_resolver != nullptr) {
            for (const auto& scorer : _scorers) {
                if (scorer && scorer->has_null_bitmap(_resolver)) {
                    _has_null_sources = true;
                    break;
                }
            }
        }
    }
    return _has_null_sources;
}

const roaring::Roaring* AndScorer::get_null_bitmap(const NullBitmapResolver* resolver) {
    _ensure_null_bitmap(resolver);
    return _null_bitmap.isEmpty() ? nullptr : &_null_bitmap;
}

bool AndScorer::_advance_to(uint32_t target) {
    uint32_t candidate = target;

    while (candidate != TERMINATED) {
        bool all_match = true;
        bool has_null = false;
        bool has_false = false;
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
                const roaring::Roaring* null_bitmap = nullptr;
                if (_resolver != nullptr) {
                    if (scorer->has_null_bitmap(_resolver)) {
                        null_bitmap = scorer->get_null_bitmap(_resolver);
                    }
                }
                if (null_bitmap != nullptr && null_bitmap->contains(candidate)) {
                    has_null = true;
                } else {
                    has_false = true;
                }
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
            _true_bitmap.add(_doc);
            if (_possible_null.contains(_doc)) {
                _possible_null.remove(_doc);
            }
            return true;
        }

        if (!has_false && has_null) {
            _possible_null.add(candidate);
        } else if (has_false) {
            _false_bitmap.add(candidate);
        }

        if (next_candidate == std::numeric_limits<uint32_t>::max()) {
            break;
        }
        candidate = next_candidate;
    }

    _doc = TERMINATED;
    return false;
}

void AndScorer::_ensure_null_bitmap(const NullBitmapResolver* resolver) {
    if (resolver != nullptr) {
        _resolver = resolver;
    }
    if (!_null_sources_checked) {
        _null_sources_checked = true;
        if (_resolver != nullptr) {
            for (const auto& scorer : _scorers) {
                if (scorer && scorer->has_null_bitmap(_resolver)) {
                    _has_null_sources = true;
                    break;
                }
            }
        }
    }
    if (!_has_null_sources || _null_ready) {
        return;
    }
    _null_bitmap = _possible_null;
    _null_bitmap -= _false_bitmap;
    _null_bitmap -= _true_bitmap;
    _null_ready = true;
}

AndNotScorer::AndNotScorer(ScorerPtr include, std::vector<ScorerPtr> excludes,
                           const NullBitmapResolver* resolver)
        : _include(std::move(include)), _resolver(resolver) {
    if (_include && _resolver != nullptr && _include->has_null_bitmap(_resolver)) {
        const auto* null_bitmap = _include->get_null_bitmap(_resolver);
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
        if (_resolver != nullptr && scorer->has_null_bitmap(_resolver)) {
            const auto* null_bitmap = scorer->get_null_bitmap(_resolver);
            if (null_bitmap != nullptr) {
                _exclude_null |= *null_bitmap;
            }
        }
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
            _null_bitmap.add(doc);
            current = doc + 1;
            continue;
        }

        if (_null_bitmap.contains(doc)) {
            _null_bitmap.remove(doc);
        }
        _doc = doc;
        _current_score = _include->score();
        _true_bitmap.add(_doc);
        return true;
    }

    return false;
}

} // namespace doris::segment_v2::inverted_index::query_v2
