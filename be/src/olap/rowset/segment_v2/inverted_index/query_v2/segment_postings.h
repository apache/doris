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

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TermIterator>
class SegmentPostingsBase : public DocSet {
public:
    SegmentPostingsBase() = default;
    SegmentPostingsBase(TermIterator iter) : _iter(std::move(iter)) {
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

    virtual int32_t freq() const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "freq() method not implemented in base SegmentPostingsBase class");
    }

    virtual int32_t norm() const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "norm() method not implemented in base SegmentPostingsBase class");
    }

protected:
    TermIterator _iter;

private:
    uint32_t _doc = TERMINATED;
};

template <typename TermIterator>
class SegmentPostings final : public SegmentPostingsBase<TermIterator> {
public:
    SegmentPostings(TermIterator iter) : SegmentPostingsBase<TermIterator>(std::move(iter)) {}

    int32_t freq() const override { return this->_iter->freq(); }
    int32_t norm() const override { return this->_iter->norm(); }
};

template <typename TermIterator>
class NoScoreSegmentPosting final : public SegmentPostingsBase<TermIterator> {
public:
    NoScoreSegmentPosting(TermIterator iter) : SegmentPostingsBase<TermIterator>(std::move(iter)) {}

    int32_t freq() const override { return 1; }
    int32_t norm() const override { return 1; }
};

template <typename TermIterator>
class EmptySegmentPosting final : public SegmentPostingsBase<TermIterator> {
public:
    EmptySegmentPosting() = default;

    uint32_t advance() override { return TERMINATED; }
    uint32_t seek(uint32_t) override { return TERMINATED; }
    uint32_t doc() const override { return TERMINATED; }
    uint32_t size_hint() const override { return 0; }

    int32_t freq() const override { return 1; }
    int32_t norm() const override { return 1; }
};

} // namespace doris::segment_v2::inverted_index::query_v2