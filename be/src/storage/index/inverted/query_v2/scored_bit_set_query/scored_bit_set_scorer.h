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

#include <memory>
#include <utility>

#include "roaring/roaring.hh"
#include "storage/compaction/collection_similarity.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

using ScoredBitSetMap = doris::ScoreMap;
using ScoredBitSetMapPtr = std::shared_ptr<const ScoredBitSetMap>;

// A doc set whose relevance scores were computed before the query tree was assembled.
//
// The SNII native reader answers a whole clause inside its own query() call and produces the
// per-document BM25 values as a side effect there, long before this scorer exists. It therefore
// cannot participate in the incremental term/norm scoring the CLucene-backed scorers do; the
// values are simply looked up per document as the collector walks the doc set.
//
// Iteration is delegated to BitSetScorer so the two stay identical by construction: only score()
// differs, and duplicating the roaring iteration would be the thing most likely to drift.
class ScoredBitSetScorer final : public Scorer {
public:
    ScoredBitSetScorer(std::shared_ptr<roaring::Roaring> bitmap,
                       std::shared_ptr<roaring::Roaring> null_bitmap, ScoredBitSetMapPtr scores)
            : _doc_set(std::move(bitmap), std::move(null_bitmap)), _scores(std::move(scores)) {
        DCHECK(_scores != nullptr);
    }
    ~ScoredBitSetScorer() override = default;

    uint32_t advance() override { return _doc_set.advance(); }

    uint32_t seek(uint32_t target) override { return _doc_set.seek(target); }

    uint32_t doc() const override { return _doc_set.doc(); }

    uint32_t size_hint() const override { return _doc_set.size_hint(); }

    float score() override {
        const uint32_t current = _doc_set.doc();
        if (current == TERMINATED) {
            return 0.0F;
        }
        auto it = _scores->find(current);
        // A matched document without a score means the producer scored only part of the doc set.
        // Zero matches how CollectionSimilarity itself reports an unscored row, so an unexpected
        // gap degrades to "ranks last" rather than to a fabricated constant.
        DCHECK(it != _scores->end());
        return it != _scores->end() ? it->second : 0.0F;
    }

    bool has_null_bitmap(const NullBitmapResolver* resolver = nullptr) override {
        return _doc_set.has_null_bitmap(resolver);
    }

    const roaring::Roaring* get_null_bitmap(const NullBitmapResolver* resolver = nullptr) override {
        return _doc_set.get_null_bitmap(resolver);
    }

private:
    BitSetScorer _doc_set;
    ScoredBitSetMapPtr _scores;
};
using ScoredBitSetScorerPtr = std::shared_ptr<ScoredBitSetScorer>;

} // namespace doris::segment_v2::inverted_index::query_v2
