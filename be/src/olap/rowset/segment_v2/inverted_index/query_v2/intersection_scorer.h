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

#pragma once

#include <roaring/roaring.hh>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

ScorerPtr intersection_scorer_build(std::vector<ScorerPtr> scorers, bool enable_scoring,
                                    const NullBitmapResolver* resolver);

class AndScorer final : public Scorer {
public:
    AndScorer(std::vector<ScorerPtr> scorers, bool enable_scoring,
              const NullBitmapResolver* resolver);
    ~AndScorer() override = default;

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override { return _doc; }
    uint32_t size_hint() const override;
    float score() override { return _current_score; }

    bool has_null_bitmap(const NullBitmapResolver* resolver = nullptr) override;
    const roaring::Roaring* get_null_bitmap(const NullBitmapResolver* resolver = nullptr) override;

private:
    bool _advance_to(uint32_t target);
    void _ensure_null_bitmap(const NullBitmapResolver* resolver);

    std::vector<ScorerPtr> _scorers;
    bool _enable_scoring = false;
    const NullBitmapResolver* _resolver = nullptr;

    uint32_t _doc = TERMINATED;
    float _current_score = 0.0F;

    roaring::Roaring _true_bitmap;
    roaring::Roaring _possible_null;
    roaring::Roaring _false_bitmap;

    bool _has_null_sources = false;
    bool _null_sources_checked = false;
    bool _null_ready = false;
    roaring::Roaring _null_bitmap;
};

class AndNotScorer final : public Scorer {
public:
    AndNotScorer(ScorerPtr include, std::vector<ScorerPtr> excludes,
                 const NullBitmapResolver* resolver);
    ~AndNotScorer() override = default;

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override { return _doc; }
    uint32_t size_hint() const override;
    float score() override { return _current_score; }

    bool has_null_bitmap(const NullBitmapResolver* resolver = nullptr) override {
        if (resolver != nullptr) {
            _resolver = resolver;
        }
        return !_null_bitmap.isEmpty();
    }
    const roaring::Roaring* get_null_bitmap(
            const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return _null_bitmap.isEmpty() ? nullptr : &_null_bitmap;
    }

private:
    bool _advance_to(uint32_t target);

    ScorerPtr _include;
    roaring::Roaring _exclude_true;
    roaring::Roaring _exclude_null;
    roaring::Roaring _null_bitmap;
    roaring::Roaring _true_bitmap;

    uint32_t _doc = TERMINATED;
    float _current_score = 0.0F;
    const NullBitmapResolver* _resolver = nullptr;
};

} // namespace doris::segment_v2::inverted_index::query_v2
