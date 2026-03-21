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

#include "storage/index/inverted/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TDocSetExclude>
inline bool is_within(TDocSetExclude& docset, uint32_t doc) {
    return docset->doc() <= doc && docset->seek(doc) == doc;
}

template <typename TDocSet, typename TDocSetExclude>
class Exclude final : public Scorer {
public:
    Exclude(TDocSet underlying_docset, TDocSetExclude excluding_docset,
            roaring::Roaring exclude_null = {}, const NullBitmapResolver* resolver = nullptr);
    ~Exclude() override = default;

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override;
    uint32_t size_hint() const override;
    float score() override;

    bool has_null_bitmap(const NullBitmapResolver* resolver = nullptr) override;
    const roaring::Roaring* get_null_bitmap(const NullBitmapResolver* resolver = nullptr) override;

private:
    TDocSet _underlying_docset;
    TDocSetExclude _excluding_docset;
    roaring::Roaring _exclude_null;
    roaring::Roaring _null_bitmap;
};

using ExcludeScorerPtr = std::shared_ptr<Exclude<ScorerPtr, ScorerPtr>>;

ScorerPtr make_exclude(ScorerPtr underlying, ScorerPtr excluding,
                       roaring::Roaring exclude_null = {},
                       const NullBitmapResolver* resolver = nullptr);

} // namespace doris::segment_v2::inverted_index::query_v2