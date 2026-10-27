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

#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <utility>

#include "storage/index/inverted/query_v2/query.h"
#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/inverted/query_v2/weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// SEARCH treats physical NULL postings as nonmatches before Boolean composition.
class NullMaskedScorer final : public Scorer {
public:
    NullMaskedScorer(ScorerPtr inner, std::shared_ptr<roaring::Roaring> nulls)
            : _inner(std::move(inner)), _nulls(std::move(nulls)) {
        _doc = _skip_nulls(_inner->doc());
    }
    uint32_t advance() override { return _doc = _skip_nulls(_inner->advance()); }
    uint32_t seek(uint32_t target) override { return _doc = _skip_nulls(_inner->seek(target)); }
    uint32_t doc() const override { return _doc; }
    uint32_t size_hint() const override { return _inner->size_hint(); }
    float score() override { return _inner->score(); }

private:
    uint32_t _skip_nulls(uint32_t doc) {
        while (doc != TERMINATED && _nulls->contains(doc)) {
            doc = _inner->advance();
        }
        return doc;
    }
    ScorerPtr _inner;
    std::shared_ptr<roaring::Roaring> _nulls;
    uint32_t _doc = TERMINATED;
};

class NullMaskedWeight final : public Weight {
public:
    using Weight::for_each_pruning;
    NullMaskedWeight(WeightPtr inner, std::shared_ptr<roaring::Roaring> nulls)
            : _inner(std::move(inner)), _nulls(std::move(nulls)) {}
    ScorerPtr scorer(const QueryExecutionContext& context,
                     const std::string& binding_key) override {
        auto inner = _inner->scorer(context, binding_key);
        if (inner == nullptr) {
            return nullptr;
        }
        return std::make_shared<NullMaskedScorer>(std::move(inner), _nulls);
    }
    void for_each_pruning(const QueryExecutionContext& context, const std::string& binding_key,
                          float threshold, PruningCallback callback) override {
        auto sc = scorer(context, binding_key);
        if (sc != nullptr) {
            for_each_pruning_scorer(sc, threshold, std::move(callback));
        }
    }

private:
    WeightPtr _inner;
    std::shared_ptr<roaring::Roaring> _nulls;
};

class NullMaskedQuery final : public Query {
public:
    NullMaskedQuery(QueryPtr inner, std::shared_ptr<roaring::Roaring> nulls)
            : _inner(std::move(inner)), _nulls(std::move(nulls)) {}
    WeightPtr weight(bool enable_scoring) override {
        return std::make_shared<NullMaskedWeight>(_inner->weight(enable_scoring), _nulls);
    }

private:
    QueryPtr _inner;
    std::shared_ptr<roaring::Roaring> _nulls;
};

} // namespace doris::segment_v2::inverted_index::query_v2
