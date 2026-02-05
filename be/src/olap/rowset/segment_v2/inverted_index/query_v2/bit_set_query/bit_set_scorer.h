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
#include <limits>
#include <memory>

#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "roaring/roaring.hh"

namespace doris::segment_v2::inverted_index::query_v2 {

class BitSetScorer final : public Scorer {
public:
    BitSetScorer(std::shared_ptr<roaring::Roaring> bitmap,
                 std::shared_ptr<roaring::Roaring> null_bitmap = nullptr)
            : _bit_set(std::move(bitmap)),
              _null_bitmap(std::move(null_bitmap)),
              _it(_bit_set->begin()) {
        _doc = (_it.i.has_value) ? *_it : TERMINATED;
    }
    ~BitSetScorer() override = default;

    uint32_t advance() override {
        if (_doc == TERMINATED) {
            return TERMINATED;
        }
        ++_it;
        if (!_it.i.has_value) {
            _doc = TERMINATED;
            return TERMINATED;
        }
        _doc = *_it;
        return _doc;
    }

    uint32_t seek(uint32_t target) override {
        if (_doc == TERMINATED) {
            return TERMINATED;
        }
        if (target <= _doc) {
            return _doc;
        }
        _it.equalorlarger(target);
        if (!_it.i.has_value) {
            _doc = TERMINATED;
            return TERMINATED;
        }
        _doc = *_it;
        return _doc;
    }

    uint32_t doc() const override { return _doc; }

    uint32_t size_hint() const override {
        uint64_t card = _bit_set->cardinality();
        return static_cast<uint32_t>(
                std::min<uint64_t>(card, std::numeric_limits<uint32_t>::max()));
    }

    float score() override { return 1.0F; }

    bool has_null_bitmap(const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return _null_bitmap != nullptr && !_null_bitmap->isEmpty();
    }

    const roaring::Roaring* get_null_bitmap(
            const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return _null_bitmap ? _null_bitmap.get() : nullptr;
    }

private:
    std::shared_ptr<roaring::Roaring> _bit_set;
    std::shared_ptr<roaring::Roaring> _null_bitmap;
    roaring::Roaring::const_iterator _it;
    uint32_t _doc = TERMINATED;
};
using BitSetScorerPtr = std::shared_ptr<BitSetScorer>;

} // namespace doris::segment_v2::inverted_index::query_v2
