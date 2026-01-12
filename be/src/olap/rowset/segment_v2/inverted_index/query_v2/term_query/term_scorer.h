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

#include <optional>
#include <roaring/roaring.hh>

#include "olap/rowset/segment_v2/inverted_index/query_v2/null_bitmap_fetcher.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/similarity.h"
#include "olap/rowset/segment_v2/inverted_index_iterator.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class TermScorer final : public Scorer {
public:
    TermScorer(SegmentPostingsPtr segment_postings, SimilarityPtr similarity,
               std::string logical_field = {})
            : _segment_postings(std::move(segment_postings)),
              _similarity(std::move(similarity)),
              _logical_field(std::move(logical_field)) {}

    uint32_t advance() override { return _segment_postings->advance(); }
    uint32_t seek(uint32_t target) override { return _segment_postings->seek(target); }
    uint32_t doc() const override { return _segment_postings->doc(); }
    uint32_t size_hint() const override { return _segment_postings->size_hint(); }
    uint32_t freq() const override { return _segment_postings->freq(); }
    uint32_t norm() const override { return _segment_postings->norm(); }

    float score() override { return _similarity->score(freq(), norm()); }

    bool has_null_bitmap(const NullBitmapResolver* resolver = nullptr) override {
        _ensure_null_bitmap(resolver);
        return _null_bitmap.has_value() && !_null_bitmap->isEmpty();
    }

    const roaring::Roaring* get_null_bitmap(const NullBitmapResolver* resolver = nullptr) override {
        _ensure_null_bitmap(resolver);
        return _null_bitmap ? &(*_null_bitmap) : nullptr;
    }

private:
    void _ensure_null_bitmap(const NullBitmapResolver* resolver) {
        if (_null_bitmap_checked) {
            return;
        }

        if (resolver == nullptr || _logical_field.empty()) {
            LOG(WARNING) << "TermScorer: Null bitmap resolver or logical field is empty";
            return;
        }

        _null_bitmap_checked = true;

        auto bitmap = FieldNullBitmapFetcher::fetch(resolver, _logical_field, this);
        if (bitmap != nullptr) {
            _null_bitmap = *bitmap;
        }
    }

    SegmentPostingsPtr _segment_postings;
    SimilarityPtr _similarity;
    std::string _logical_field;
    bool _null_bitmap_checked = false;
    std::optional<roaring::Roaring> _null_bitmap;
};

using TermScorerPtr = std::shared_ptr<TermScorer>;

} // namespace doris::segment_v2::inverted_index::query_v2
