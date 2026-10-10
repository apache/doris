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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>
#include <span>
#include <utility>
#include <vector>

#include "common/check.h"
#include "storage/index/inverted/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// The rows a listing scored, ascending, each with its score; `nulls` are the rows the field
// leaves UNKNOWN, null when there are none.
class ScoredRowsScorer final : public Scorer {
public:
    ScoredRowsScorer(std::vector<uint32_t> rows, std::vector<float> scores,
                     std::shared_ptr<roaring::Roaring> nulls)
            : _rows(std::move(rows)), _scores(std::move(scores)), _nulls(std::move(nulls)) {
        DORIS_CHECK_EQ(_rows.size(), _scores.size());
    }
    ~ScoredRowsScorer() override = default;

    uint32_t advance() override {
        if (_index < _rows.size()) {
            ++_index;
        }
        return doc();
    }

    uint32_t seek(uint32_t target) override {
        if (_index < _rows.size() && _rows[_index] < target) {
            _index = static_cast<size_t>(
                    std::lower_bound(_rows.begin() + static_cast<std::ptrdiff_t>(_index),
                                     _rows.end(), target) -
                    _rows.begin());
        }
        return doc();
    }

    uint32_t doc() const override { return _index < _rows.size() ? _rows[_index] : TERMINATED; }

    uint32_t size_hint() const override { return static_cast<uint32_t>(_rows.size()); }

    float score() override { return _index < _rows.size() ? _scores[_index] : 0.0F; }

    // The rows from the current one on, and their scores.
    std::span<const uint32_t> rows() const { return std::span(_rows).subspan(_index); }
    std::span<const float> scores() const { return std::span(_scores).subspan(_index); }

    bool has_null_bitmap(const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return _nulls != nullptr && !_nulls->isEmpty();
    }

    const roaring::Roaring* get_null_bitmap(
            const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return _nulls ? _nulls.get() : nullptr;
    }

private:
    std::vector<uint32_t> _rows;
    std::vector<float> _scores;
    std::shared_ptr<roaring::Roaring> _nulls;
    size_t _index = 0;
};

} // namespace doris::segment_v2::inverted_index::query_v2
