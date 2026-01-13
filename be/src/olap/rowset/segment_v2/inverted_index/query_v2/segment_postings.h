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
#include <variant>

#include "CLucene/index/DocRange.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/similarity.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"

namespace doris::segment_v2::inverted_index::query_v2 {

using doris::segment_v2::Similarity;
using doris::segment_v2::SimilarityPtr;

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

class SegmentPostings : public Postings {
public:
    using IterVariant = std::variant<std::monostate, TermDocsPtr, TermPositionsPtr>;

    explicit SegmentPostings(TermDocsPtr iter, bool enable_scoring, SimilarityPtr similarity)
            : _iter(std::move(iter)),
              _enable_scoring(enable_scoring),
              _similarity(std::move(similarity)) {
        if (auto* p = std::get_if<TermDocsPtr>(&_iter)) {
            _raw_iter = p->get();
        }
        _init_doc();
    }

    explicit SegmentPostings(TermPositionsPtr iter, bool enable_scoring, SimilarityPtr similarity)
            : _iter(std::move(iter)),
              _enable_scoring(enable_scoring),
              _has_positions(true),
              _similarity(std::move(similarity)) {
        if (auto* p = std::get_if<TermPositionsPtr>(&_iter)) {
            _raw_iter = p->get();
        }
        _init_doc();
    }

    uint32_t advance() override {
        if (_block.doc_many && _cursor < _block.doc_many_size_) {
            return _doc = (*_block.doc_many)[_cursor++];
        }
        if (!_refill()) {
            return _doc = TERMINATED;
        }
        return _doc = (*_block.doc_many)[_cursor++];
    }

    uint32_t seek(uint32_t target) override {
        if (target <= _doc) {
            return _doc;
        }

        if (_block.doc_many) {
            while (_cursor < _block.doc_many_size_) {
                uint32_t curr = (*_block.doc_many)[_cursor++];
                if (curr >= target) {
                    return _doc = curr;
                }
            }
        }

        _raw_iter->skipToBlock(target);

        while (_refill()) {
            while (_cursor < _block.doc_many_size_) {
                uint32_t curr = (*_block.doc_many)[_cursor++];
                if (curr >= target) {
                    return _doc = curr;
                }
            }
        }

        return _doc = TERMINATED;
    }

    uint32_t doc() const override { return _doc; }

    uint32_t size_hint() const override { return _raw_iter ? _raw_iter->docFreq() : 0; }

    uint32_t freq() const override {
        if (!_enable_scoring || !_block.freq_many || _cursor == 0) {
            return 1;
        }
        return (*_block.freq_many)[_cursor - 1];
    }

    uint32_t norm() const override {
        if (!_enable_scoring || !_block.norm_many || _cursor == 0) {
            return 1;
        }
        return (*_block.norm_many)[_cursor - 1];
    }

    void append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) override {
        if (!_has_positions) {
            throw Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "This posting type does not support position information");
        }
        if (!_block.freq_many) {
            throw Exception(doris::ErrorCode::INTERNAL_ERROR,
                            "Position information requested but freq data is missing");
        }

        auto* term_pos_ptr = std::get_if<TermPositionsPtr>(&_iter);
        if (!term_pos_ptr || !*term_pos_ptr) {
            throw Exception(doris::ErrorCode::INTERNAL_ERROR,
                            "Position information requested but TermPositions iterator is missing");
        }

        uint32_t current_doc_idx = _cursor - 1;
        if (current_doc_idx > _prox_cursor) {
            int32_t skip_count = 0;
            for (uint32_t i = _prox_cursor; i < current_doc_idx; ++i) {
                skip_count += (*_block.freq_many)[i];
            }
            if (skip_count > 0) {
                (*term_pos_ptr)->addLazySkipProxCount(skip_count);
            }
        }

        uint32_t freq = (*_block.freq_many)[current_doc_idx];
        int32_t position = 0;
        for (uint32_t i = 0; i < freq; ++i) {
            position += (*term_pos_ptr)->nextDeltaPosition();
            output.push_back(position + offset);
        }

        _prox_cursor = current_doc_idx + 1;
    }

    bool scoring_enabled() const { return _enable_scoring; }

    int64_t block_id() const { return _block_id; }

    void seek_block(uint32_t target_doc) {
        if (target_doc <= _doc) {
            return;
        }
        if (_raw_iter->skipToBlock(target_doc)) {
            _block_max_score_cache = -1.0F;
            _cursor = 0;
            _block.doc_many_size_ = 0;
        }
    }

    uint32_t last_doc_in_block() const {
        int32_t last_doc = _raw_iter->getLastDocInBlock();
        if (last_doc == -1 || last_doc == 0x7FFFFFFFL) {
            return TERMINATED;
        }
        return static_cast<uint32_t>(last_doc);
    }

    float block_max_score() {
        if (!_enable_scoring || !_similarity) {
            return std::numeric_limits<float>::max();
        }
        if (_block_max_score_cache >= 0.0F) {
            return _block_max_score_cache;
        }
        int32_t max_block_freq = _raw_iter->getMaxBlockFreq();
        int32_t max_block_norm = _raw_iter->getMaxBlockNorm();
        if (max_block_freq >= 0 && max_block_norm >= 0) {
            _block_max_score_cache = _similarity->score(static_cast<float>(max_block_freq),
                                                        static_cast<int64_t>(max_block_norm));
            return _block_max_score_cache;
        }
        return _similarity->max_score();
    }

    float max_score() const { return _similarity->max_score(); }

    int32_t max_block_freq() const { return _raw_iter->getMaxBlockFreq(); }
    int32_t max_block_norm() const { return _raw_iter->getMaxBlockNorm(); }

private:
    bool _refill() {
        if (!_raw_iter->readBlock(&_block)) {
            return false;
        }
        _cursor = 0;
        _prox_cursor = 0;
        _block_max_score_cache = -1.0F;
        _block_id++;
        return _block.doc_many_size_ > 0;
    }

    void _init_doc() {
        if (!_raw_iter) {
            throw Exception(doris::ErrorCode::INVALID_ARGUMENT,
                            "SegmentPostings requires a valid iterator");
        }
        if (_refill()) {
            _doc = (*_block.doc_many)[_cursor++];
        } else {
            _doc = TERMINATED;
        }
    }

    IterVariant _iter;
    lucene::index::TermDocs* _raw_iter = nullptr;
    uint32_t _doc = TERMINATED;
    bool _enable_scoring = false;
    bool _has_positions = false;

    DocRange _block;
    uint32_t _cursor = 0;
    uint32_t _prox_cursor = 0;
    mutable float _block_max_score_cache = -1.0F;
    mutable int64_t _block_id = 0;
    SimilarityPtr _similarity;
};
using SegmentPostingsPtr = std::shared_ptr<SegmentPostings>;

inline SegmentPostingsPtr make_segment_postings(TermDocsPtr iter, bool enable_scoring,
                                                SimilarityPtr similarity) {
    return std::make_shared<SegmentPostings>(std::move(iter), enable_scoring, similarity);
}

inline SegmentPostingsPtr make_segment_postings(TermPositionsPtr iter, bool enable_scoring,
                                                SimilarityPtr similarity) {
    return std::make_shared<SegmentPostings>(std::move(iter), enable_scoring, similarity);
}

} // namespace doris::segment_v2::inverted_index::query_v2