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

#include <variant>

#include "CLucene/index/DocRange.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"

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
    using IterVariant = std::variant<std::monostate, TermDocsPtr, TermPositionsPtr>;

    explicit SegmentPostings(TermDocsPtr iter, bool enable_scoring = false)
            : _iter(std::move(iter)), _enable_scoring(enable_scoring) {
        if (auto* p = std::get_if<TermDocsPtr>(&_iter)) {
            _raw_iter = p->get();
        }
        _init_doc();
    }

    explicit SegmentPostings(TermPositionsPtr iter, bool enable_scoring = false)
            : _iter(std::move(iter)), _enable_scoring(enable_scoring), _has_positions(true) {
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

private:
    bool _refill() {
        _block.need_positions = _has_positions;
        if (!_raw_iter->readRange(&_block)) {
            return false;
        }
        _cursor = 0;
        _prox_cursor = 0;
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
};

using SegmentPostingsPtr = std::shared_ptr<SegmentPostings>;

inline SegmentPostingsPtr make_segment_postings(TermDocsPtr iter, bool enable_scoring = false) {
    return std::make_shared<SegmentPostings>(std::move(iter), enable_scoring);
}

inline SegmentPostingsPtr make_segment_postings(TermPositionsPtr iter,
                                                bool enable_scoring = false) {
    return std::make_shared<SegmentPostings>(std::move(iter), enable_scoring);
}

} // namespace doris::segment_v2::inverted_index::query_v2