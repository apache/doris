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
#include <limits>
#include <ranges>
#include <string>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

struct ScoredDoc {
    ScoredDoc() = default;
    ScoredDoc(uint32_t doc, float s) : doc_id(doc), score(s) {}

    uint32_t doc_id = 0;
    float score = 0.0F;
};

struct ScoredDocByScoreDesc {
    bool operator()(const ScoredDoc& a, const ScoredDoc& b) const {
        return a.score > b.score || (a.score == b.score && a.doc_id < b.doc_id);
    }
};

class TopKCollector {
public:
    static constexpr size_t kMaxK = 10000;

    explicit TopKCollector(size_t k) : _k(std::clamp(k, size_t(1), kMaxK)) {
        _buffer.reserve(_k * 2);
    }

    float collect(uint32_t doc_id, float score) {
        if (score < _threshold) {
            return _threshold;
        }
        _buffer.emplace_back(doc_id, score);
        if (_buffer.size() == _buffer.capacity()) {
            _truncate();
        } else if (_buffer.size() == _k) {
            _update_threshold_at_capacity();
        }
        return _threshold;
    }

    float threshold() const { return _threshold; }
    size_t size() const { return std::min(_buffer.size(), _k); }

    [[nodiscard]] std::vector<ScoredDoc> into_sorted_vec() {
        if (_buffer.size() > _k) {
            _truncate();
        }
        std::ranges::sort(_buffer, ScoredDocByScoreDesc {});
        return std::move(_buffer);
    }

private:
    void _truncate() {
        std::ranges::nth_element(_buffer, _buffer.begin() + _k, ScoredDocByScoreDesc {});
        _buffer.resize(_k);
        _update_threshold_at_capacity();
    }

    void _update_threshold_at_capacity() {
        auto it = std::ranges::min_element(_buffer, ScoredDocByScoreDesc {});
        _threshold = it->score;
    }

    size_t _k;
    float _threshold = -std::numeric_limits<float>::infinity();
    std::vector<ScoredDoc> _buffer;
};

std::vector<ScoredDoc> collect_multi_segment_top_k(const WeightPtr& weight,
                                                   const QueryExecutionContext& context,
                                                   const std::string& binding_key, size_t k,
                                                   bool use_wand = true);

} // namespace doris::segment_v2::inverted_index::query_v2
