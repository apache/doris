// be/src/olap/rowset/segment_v2/inverted_index/query_v2/nullable_scorer.h

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

#include <glog/logging.h>

#include <memory>

#include "common/exception.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/null_bitmap_fetcher.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "roaring/roaring.hh"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename ScorerPtrT = ScorerPtr>
class NullableScorer : public Scorer {
public:
    NullableScorer(ScorerPtrT inner_scorer, std::shared_ptr<roaring::Roaring> null_bitmap)
            : _inner_scorer(std::move(inner_scorer)), _null_bitmap(std::move(null_bitmap)) {}
    ~NullableScorer() override = default;

    uint32_t advance() override { return _inner_scorer->advance(); }
    uint32_t seek(uint32_t target) override { return _inner_scorer->seek(target); }
    uint32_t doc() const override { return _inner_scorer->doc(); }
    uint32_t size_hint() const override { return _inner_scorer->size_hint(); }
    float score() override { return _inner_scorer->score(); }

    bool has_null_bitmap(const NullBitmapResolver* /*resolver*/ = nullptr) override { return true; }

    const roaring::Roaring* get_null_bitmap(
            const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return _null_bitmap.get();
    }

private:
    ScorerPtrT _inner_scorer;
    std::shared_ptr<roaring::Roaring> _null_bitmap;
};
using NullableScorerPtr = std::shared_ptr<NullableScorer<>>;

template <typename ScorerPtrT = ScorerPtr>
inline ScorerPtr make_nullable_scorer(ScorerPtrT inner_scorer, const std::string& logical_field,
                                      const NullBitmapResolver* resolver) {
    if (!inner_scorer) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "make_nullable_scorer: inner_scorer must not be null");
    }

    auto null_bitmap = FieldNullBitmapFetcher::fetch(resolver, logical_field, inner_scorer.get());

    if (!null_bitmap || null_bitmap->isEmpty()) {
        return inner_scorer;
    }

    return std::make_shared<NullableScorer<ScorerPtrT>>(std::move(inner_scorer),
                                                        std::move(null_bitmap));
}

} // namespace doris::segment_v2::inverted_index::query_v2