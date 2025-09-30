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

#include <memory>

#include "olap/rowset/segment_v2/inverted_index/query_v2/bitmap_query/bitmap_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"
#include "roaring/roaring.hh"

namespace doris::segment_v2::inverted_index::query_v2 {

class BitmapWeight final : public Weight {
public:
    BitmapWeight(std::shared_ptr<roaring::Roaring> bitmap,
                 std::shared_ptr<roaring::Roaring> null_bitmap = nullptr)
            : _bitmap(std::move(bitmap)), _null_bitmap(std::move(null_bitmap)) {}
    ~BitmapWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& /*context*/) override {
        if (_bitmap == nullptr || _bitmap->isEmpty()) {
            return std::make_shared<EmptyScorer>();
        }
        return std::make_shared<BitmapScorer>(_bitmap, _null_bitmap);
    }

private:
    std::shared_ptr<roaring::Roaring> _bitmap;
    std::shared_ptr<roaring::Roaring> _null_bitmap;
};

using BitmapWeightPtr = std::shared_ptr<BitmapWeight>;

} // namespace doris::segment_v2::inverted_index::query_v2
