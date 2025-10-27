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

#include <climits>
#include <cstdint>

#include "common/exception.h"

namespace doris::segment_v2::inverted_index::query_v2 {

static constexpr uint32_t TERMINATED = static_cast<uint32_t>(INT_MAX);

class DocSet {
public:
    DocSet() = default;
    virtual ~DocSet() = default;

    virtual uint32_t advance() {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "advance() method not implemented in base DocSet class");
    }

    virtual uint32_t seek(uint32_t target) {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "seek() method not implemented in base DocSet class");
    }

    virtual uint32_t doc() const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "doc() method not implemented in base DocSet class");
    }

    virtual uint32_t size_hint() const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "size_hint() method not implemented in base DocSet class");
    }

    virtual uint32_t freq() const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "freq() method not implemented in base DocSet class");
    }

    virtual uint32_t norm() const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "norm() method not implemented in base DocSet class");
    }
};

class MockDocSet : public DocSet {
public:
    MockDocSet(std::vector<uint32_t> docs, uint32_t size_hint_val = 0, uint32_t norm_val = 1)
            : _docs(std::move(docs)), _size_hint_val(size_hint_val), _norm_val(norm_val) {
        if (_docs.empty()) {
            _current_doc = TERMINATED;
        } else {
            std::ranges::sort(_docs.begin(), _docs.end());
            _current_doc = _docs[0];
        }
        if (_size_hint_val == 0) {
            _size_hint_val = static_cast<uint32_t>(_docs.size());
        }
    }

    uint32_t advance() override {
        if (_docs.empty() || _index >= _docs.size()) {
            _current_doc = TERMINATED;
            return TERMINATED;
        }
        ++_index;
        if (_index >= _docs.size()) {
            _current_doc = TERMINATED;
            return TERMINATED;
        }
        _current_doc = _docs[_index];
        return _current_doc;
    }

    uint32_t seek(uint32_t target) override {
        if (_docs.empty() || _index >= _docs.size()) {
            _current_doc = TERMINATED;
            return TERMINATED;
        }
        if (_current_doc >= target) {
            return _current_doc;
        }
        auto it = std::lower_bound(_docs.begin() + _index, _docs.end(), target);
        if (it == _docs.end()) {
            _index = _docs.size();
            _current_doc = TERMINATED;
            return TERMINATED;
        }
        _index = static_cast<size_t>(it - _docs.begin());
        _current_doc = *it;
        return _current_doc;
    }

    uint32_t doc() const override { return _current_doc; }

    uint32_t size_hint() const override { return _size_hint_val; }

    uint32_t norm() const override { return _norm_val; }

private:
    std::vector<uint32_t> _docs;
    size_t _index = 0;
    uint32_t _current_doc = TERMINATED;
    uint32_t _size_hint_val = 0;
    uint32_t _norm_val = 1;
};

using MockDocSetPtr = std::shared_ptr<MockDocSet>;

} // namespace doris::segment_v2::inverted_index::query_v2