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

#include <functional>
#include <queue>
#include <roaring/roaring.hh>
#include <utility>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScoreCombinerPtrT>
ScorerPtr buffered_union_scorer_build(std::vector<ScorerPtr> scorers,
                                      ScoreCombinerPtrT score_combiner, uint32_t segment_num_rows,
                                      const NullBitmapResolver* resolver);

template <typename ScoreCombinerPtrT>
class UnionScorer final : public Scorer {
public:
    UnionScorer(std::vector<ScorerPtr> scorers, ScoreCombinerPtrT score_combiner,
                uint32_t segment_num_rows, const NullBitmapResolver* resolver);
    ~UnionScorer() override = default;

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override { return _doc; }
    uint32_t size_hint() const override;
    float score() override { return _current_score; }

    bool has_null_bitmap(const NullBitmapResolver* resolver = nullptr) override;
    const roaring::Roaring* get_null_bitmap(const NullBitmapResolver* resolver = nullptr) override;

private:
    struct HeapEntry {
        uint32_t doc;
        size_t index;
        bool operator>(const HeapEntry& other) const { return doc > other.doc; }
    };

    bool _prepare_next();
    void _ensure_null_bitmap(const NullBitmapResolver* resolver);
    void _collect_child_nulls();

    std::vector<ScorerPtr> _scorers;
    ScoreCombinerPtrT _score_combiner;
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>> _heap;
    uint32_t _doc = TERMINATED;
    float _current_score = 0.0F;
    uint32_t _segment_num_rows = 0;

    roaring::Roaring _true_bitmap;
    roaring::Roaring _candidate_null;
    const NullBitmapResolver* _resolver = nullptr;
    bool _has_null_sources = false;
    bool _null_sources_checked = false;
    bool _null_ready = false;
    roaring::Roaring _null_bitmap;
    bool _nulls_collected = false;
};

} // namespace doris::segment_v2::inverted_index::query_v2
