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

#include <limits>

#include "storage/index/inverted/query_v2/doc_set.h"
#include "storage/index/inverted/spi/clucene_postings_cursor.h"
#include "storage/index/query/exec/block_doc_set.h"
#include "storage/index/query/spi/scoring_context.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class Postings : public DocSet {
public:
    Postings() = default;
    ~Postings() override = default;

    virtual void positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) {
        output.clear();
        append_positions_with_offset(offset, output);
    }

    virtual void append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) = 0;
};

using PostingsPtr = std::shared_ptr<Postings>;

class SegmentPostings final : public Postings {
public:
    explicit SegmentPostings(TermDocsPtr iter, bool enable_scoring,
                             index_query::ScoringContextPtr<float> similarity)
            : _similarity(std::move(similarity)),
              _cursor(std::move(iter)),
              _docs(_cursor),
              _enable_scoring(enable_scoring) {}

    explicit SegmentPostings(TermPositionsPtr iter, bool enable_scoring,
                             index_query::ScoringContextPtr<float> similarity)
            : _similarity(std::move(similarity)),
              _cursor(std::move(iter)),
              _docs(_cursor),
              _enable_scoring(enable_scoring) {}

    uint32_t advance() override { return _docs.advance() ? _docs.doc() : TERMINATED; }
    uint32_t seek(uint32_t target) override {
        return _docs.seek(target) ? _docs.doc() : TERMINATED;
    }
    uint32_t doc() const override { return _docs.exhausted() ? TERMINATED : _docs.doc(); }
    uint32_t size_hint() const override { return _docs.size_hint(); }
    uint32_t freq() const override { return _enable_scoring ? _docs.freq() : 1; }
    uint32_t norm() const override { return _enable_scoring ? _docs.norm() : 1; }

    void append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) override {
        THROW_IF_ERROR(_cursor.append_positions_with_offset(static_cast<uint32_t>(_docs.ordinal()),
                                                            offset, output));
    }

    index_query::BlockDocSet& doc_set() { return _docs; }

    bool scoring_enabled() const { return _enable_scoring; }
    int64_t block_id() const { return static_cast<int64_t>(_docs.generation()); }
    void seek_block(uint32_t target) { _docs.shallow_seek(target); }

    uint32_t last_doc_in_block() const {
        const auto bound = _cursor.current_block_bound();
        return bound.last_doc_known ? bound.last_doc : TERMINATED;
    }

    float block_max_score() {
        if (!_enable_scoring || !_similarity) {
            return std::numeric_limits<float>::max();
        }
        if (_scored_generation == _docs.generation()) {
            return _block_max_score_cache;
        }
        const auto bound = _cursor.current_block_bound();
        _block_max_score_cache =
                bound.max_freq >= 0 && bound.max_norm >= 0
                        ? _similarity->score(static_cast<float>(bound.max_freq), bound.max_norm)
                        : _similarity->max_score();
        _scored_generation = _docs.generation();
        return _block_max_score_cache;
    }

    float max_score() const {
        return _enable_scoring && _similarity ? _similarity->max_score()
                                              : std::numeric_limits<float>::max();
    }

    int32_t max_block_freq() const { return _cursor.current_block_bound().max_freq; }
    int32_t max_block_norm() const { return _cursor.current_block_bound().max_norm; }

private:
    index_query::ScoringContextPtr<float> _similarity;
    ClucenePostingsCursor _cursor;
    index_query::BlockDocSet _docs;
    bool _enable_scoring;
    float _block_max_score_cache = 0.0F;
    uint64_t _scored_generation = 0;
};
using SegmentPostingsPtr = std::shared_ptr<SegmentPostings>;

inline SegmentPostingsPtr make_segment_postings(TermDocsPtr iter, bool enable_scoring,
                                                index_query::ScoringContextPtr<float> similarity) {
    return std::make_shared<SegmentPostings>(std::move(iter), enable_scoring, similarity);
}

inline SegmentPostingsPtr make_segment_postings(TermPositionsPtr iter, bool enable_scoring,
                                                index_query::ScoringContextPtr<float> similarity) {
    return std::make_shared<SegmentPostings>(std::move(iter), enable_scoring, similarity);
}

} // namespace doris::segment_v2::inverted_index::query_v2