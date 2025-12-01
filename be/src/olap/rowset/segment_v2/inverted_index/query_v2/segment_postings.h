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

    SegmentPostings() = default;

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
        if (!_raw_iter) {
            return _doc = TERMINATED;
        }
        if (_raw_iter->next()) {
            return _doc = _raw_iter->doc();
        }
        return _doc = TERMINATED;
    }

    uint32_t seek(uint32_t target) override {
        if (!_raw_iter) {
            return TERMINATED;
        }
        if (target <= _doc) {
            return _doc;
        }
        if (_raw_iter->skipTo(target)) {
            return _doc = _raw_iter->doc();
        }
        return _doc = TERMINATED;
    }

    uint32_t doc() const override { return _doc; }

    uint32_t size_hint() const override { return _raw_iter ? _raw_iter->docFreq() : 0; }

    uint32_t freq() const override {
        if (!_enable_scoring) {
            return 1;
        }
        return _raw_iter ? _raw_iter->freq() : 1;
    }

    uint32_t norm() const override {
        if (!_enable_scoring) {
            return 1;
        }
        return _raw_iter ? _raw_iter->norm() : 1;
    }

    void append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) override {
        if (!_has_positions) {
            throw Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "This posting type does not support position information");
        }
        auto* pos_iter = std::get_if<TermPositionsPtr>(&_iter);
        if (!pos_iter || !*pos_iter) {
            return;
        }
        auto f = freq();
        size_t prev_len = output.size();
        output.resize(prev_len + f);
        for (uint32_t i = 0; i < f; ++i) {
            output[prev_len + i] = offset + static_cast<uint32_t>((*pos_iter)->nextPosition());
        }
    }

    bool is_empty() const { return !_raw_iter; }
    bool has_positions() const { return _has_positions; }
    bool scoring_enabled() const { return _enable_scoring; }

private:
    void _init_doc() {
        if (_raw_iter && _raw_iter->next()) {
            int32_t d = _raw_iter->doc();
            _doc = d >= INT_MAX ? TERMINATED : static_cast<uint32_t>(d);
        }
    }

    IterVariant _iter;
    lucene::index::TermDocs* _raw_iter = nullptr;
    uint32_t _doc = TERMINATED;
    bool _enable_scoring = false;
    bool _has_positions = false;
};

using SegmentPostingsPtr = std::shared_ptr<SegmentPostings>;

inline SegmentPostingsPtr make_segment_postings() {
    return std::make_shared<SegmentPostings>();
}

inline SegmentPostingsPtr make_segment_postings(TermDocsPtr iter, bool enable_scoring = false) {
    return std::make_shared<SegmentPostings>(std::move(iter), enable_scoring);
}

inline SegmentPostingsPtr make_segment_postings(TermPositionsPtr iter,
                                                bool enable_scoring = false) {
    return std::make_shared<SegmentPostings>(std::move(iter), enable_scoring);
}

} // namespace doris::segment_v2::inverted_index::query_v2