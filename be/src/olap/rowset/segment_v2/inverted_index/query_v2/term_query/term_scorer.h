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

#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/similarity.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename SegmentPostingsPtr>
class TermScorer final : public Scorer {
public:
    TermScorer(SegmentPostingsPtr segment_postings, SimilarityPtr similarity)
            : _segment_postings(std::move(segment_postings)), _similarity(std::move(similarity)) {}

    uint32_t advance() override { return _segment_postings->advance(); }
    uint32_t seek(uint32_t target) override { return _segment_postings->seek(target); }
    uint32_t doc() const override { return _segment_postings->doc(); }
    uint32_t size_hint() const override { return _segment_postings->size_hint(); }

    float score() override {
        auto freq = _segment_postings->freq();
        auto norm = _segment_postings->norm();
        return _similarity->score(static_cast<float>(freq), norm);
    }

private:
    SegmentPostingsPtr _segment_postings;
    SimilarityPtr _similarity;
};

using TS_Base = std::shared_ptr<TermScorer<std::shared_ptr<SegmentPostings<TermDocsPtr>>>>;
using TS_NoScore = std::shared_ptr<TermScorer<std::shared_ptr<NoScoreSegmentPosting<TermDocsPtr>>>>;
using TS_Empty = std::shared_ptr<TermScorer<std::shared_ptr<EmptySegmentPosting<TermDocsPtr>>>>;

} // namespace doris::segment_v2::inverted_index::query_v2