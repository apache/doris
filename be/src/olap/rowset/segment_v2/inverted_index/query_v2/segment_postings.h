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

template <typename TCLuceneIter>
class SegmentPostingsBase : public Postings {
public:
    SegmentPostingsBase() = default;
    SegmentPostingsBase(TCLuceneIter iter) : _iter(std::move(iter)) {
        if (_iter->next()) {
            int32_t d = _iter->doc();
            _doc = d >= INT_MAX ? TERMINATED : d;
        } else {
            _doc = TERMINATED;
        }
    }

    uint32_t advance() override {
        if (_iter->next()) {
            return _doc = _iter->doc();
        }
        return _doc = TERMINATED;
    }

    uint32_t seek(uint32_t target) override {
        if (target <= _doc) {
            return _doc;
        }
        if (_iter->skipTo(target)) {
            return _doc = _iter->doc();
        }
        return _doc = TERMINATED;
    }

    uint32_t doc() const override { return _doc; }
    uint32_t size_hint() const override { return _iter->docFreq(); }
    uint32_t freq() const override { return _iter->freq(); }
    uint32_t norm() const override { return _iter->norm(); }

    void append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) override {
        throw Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "This posting type does not support position information");
    }

protected:
    TCLuceneIter _iter;

private:
    uint32_t _doc = TERMINATED;
};
template <typename TCLuceneIter>
using SegmentPostingsBasePtr = std::shared_ptr<SegmentPostingsBase<TCLuceneIter>>;

template <typename TCLuceneIter>
class SegmentPostings final : public SegmentPostingsBase<TCLuceneIter> {
public:
    SegmentPostings(TCLuceneIter iter) : SegmentPostingsBase<TCLuceneIter>(std::move(iter)) {}
};
using TermPostingsPtr = std::shared_ptr<SegmentPostings<TermDocsPtr>>;
using PositionPostingsPtr = std::shared_ptr<SegmentPostings<TermPositionsPtr>>;

template <>
class SegmentPostings<TermPositionsPtr> final : public SegmentPostingsBase<TermPositionsPtr> {
public:
    SegmentPostings(TermPositionsPtr iter)
            : SegmentPostingsBase<TermPositionsPtr>(std::move(iter)) {}

    void append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) override {
        auto freq = this->freq();
        size_t prev_len = output.size();
        output.resize(prev_len + freq);
        for (int32_t i = 0; i < freq; ++i) {
            auto pos = this->_iter->nextPosition();
            output[prev_len + i] = offset + static_cast<uint32_t>(pos);
        }
    }
};

template <typename TCLuceneIter>
class NoScoreSegmentPosting final : public SegmentPostingsBase<TCLuceneIter> {
public:
    NoScoreSegmentPosting(TCLuceneIter iter) : SegmentPostingsBase<TCLuceneIter>(std::move(iter)) {}

    uint32_t freq() const override { return 1; }
    uint32_t norm() const override { return 1; }
};

template <typename TCLuceneIter>
class EmptySegmentPosting final : public SegmentPostingsBase<TCLuceneIter> {
public:
    EmptySegmentPosting() = default;

    uint32_t advance() override { return TERMINATED; }
    uint32_t seek(uint32_t) override { return TERMINATED; }
    uint32_t doc() const override { return TERMINATED; }
    uint32_t size_hint() const override { return 0; }

    uint32_t freq() const override { return 1; }
    uint32_t norm() const override { return 1; }
};

} // namespace doris::segment_v2::inverted_index::query_v2