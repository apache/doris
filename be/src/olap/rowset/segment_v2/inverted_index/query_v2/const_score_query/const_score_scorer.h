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

#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScorerPtrT>
class ConstScoreScorer : public Scorer {
public:
    ConstScoreScorer(ScorerPtrT scorer) : _scorer(std::move(scorer)) {}
    ~ConstScoreScorer() override = default;

    uint32_t advance() override { return _scorer->advance(); }
    uint32_t seek(uint32_t target) override { return _scorer->seek(target); }
    uint32_t doc() const override { return _scorer->doc(); }
    uint32_t size_hint() const override { return _scorer->size_hint(); }

    float score() override { return _score; }

    bool has_null_bitmap(const NullBitmapResolver* resolver = nullptr) override {
        return _scorer && _scorer->has_null_bitmap(resolver);
    }

    const roaring::Roaring* get_null_bitmap(const NullBitmapResolver* resolver = nullptr) override {
        return _scorer ? _scorer->get_null_bitmap(resolver) : nullptr;
    }

private:
    ScorerPtrT _scorer;

    float _score = 1.0F;
};

} // namespace doris::segment_v2::inverted_index::query_v2
