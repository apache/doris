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
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <utility>

#include "storage/index/inverted/query/engine/query.h"
#include "storage/index/inverted/query/engine/scorer.h"
#include "storage/index/inverted/query/engine/weight.h"
#include "storage/index/inverted/query/exec/truth_set.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// The rows `inner` matches outside `nulls`. A row in `nulls` is UNKNOWN even where `inner` lists
// it, as an array column's index does for the values a NULL array still holds.
// SEARCH sets nulls_are_false so those rows are nonmatches before Boolean composition.
class NullMaskedScorer final : public Scorer {
public:
    NullMaskedScorer(ScorerPtr inner, std::shared_ptr<roaring::Roaring> nulls,
                     bool nulls_are_false = false)
            : _inner(std::move(inner)),
              _nulls(std::move(nulls)),
              _nulls_are_false(nulls_are_false) {
        if (const auto* truths = _inner->get_true_bitmap(); truths != nullptr) {
            _true_rows = *truths - *_nulls;
        }
        _doc = _skip_nulls(_inner->doc());
    }

    uint32_t advance() override {
        _doc = _skip_nulls(_inner->advance());
        return _doc;
    }
    uint32_t seek(uint32_t target) override {
        _doc = _skip_nulls(_inner->seek(target));
        return _doc;
    }
    uint32_t doc() const override { return _doc; }
    uint32_t size_hint() const override { return _inner->size_hint(); }
    uint64_t cost() const override { return _inner->cost(); }
    uint32_t freq() const override { return _inner->freq(); }
    uint32_t norm() const override { return _inner->norm(); }
    float score() override { return _inner->score(); }

    const roaring::Roaring* get_true_bitmap() const override {
        return _true_rows.has_value() ? &*_true_rows : nullptr;
    }

    bool has_null_bitmap(const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return !_nulls_are_false;
    }

    // The inner rows' own UNKNOWN rows stay UNKNOWN too.
    const roaring::Roaring* get_null_bitmap(const NullBitmapResolver* resolver = nullptr) override {
        if (_nulls_are_false) {
            return nullptr;
        }
        if (!_null_rows.has_value()) {
            const auto* inner_nulls =
                    _inner->has_null_bitmap(resolver) ? _inner->get_null_bitmap(resolver) : nullptr;
            _null_rows = inner_nulls == nullptr ? *_nulls : *_nulls | *inner_nulls;
        }
        return &*_null_rows;
    }

private:
    uint32_t _skip_nulls(uint32_t doc) {
        while (doc != TERMINATED && _nulls->contains(doc)) {
            doc = _inner->advance();
        }
        return doc;
    }

    ScorerPtr _inner;
    std::shared_ptr<roaring::Roaring> _nulls;
    std::optional<roaring::Roaring> _true_rows;
    std::optional<roaring::Roaring> _null_rows;
    bool _nulls_are_false;
    uint32_t _doc = TERMINATED;
};

class NullMaskedWeight final : public Weight {
public:
    NullMaskedWeight(WeightPtr inner, std::shared_ptr<roaring::Roaring> nulls,
                     bool nulls_are_false = false)
            : _inner(std::move(inner)),
              _nulls(std::move(nulls)),
              _nulls_are_false(nulls_are_false) {}

    bool is_two_valued() const override { return _nulls_are_false; }

    ScorerPtr scorer(const QueryExecutionContext& context,
                     const std::string& binding_key) override {
        auto inner = _inner->scorer(context, binding_key);
        if (inner == nullptr) {
            return nullptr;
        }
        return std::make_shared<NullMaskedScorer>(std::move(inner), _nulls, _nulls_are_false);
    }

    bool lists_rows(const QueryExecutionContext& context,
                    const std::string& binding_key) const override {
        return _inner->lists_rows(context, binding_key);
    }

    index_query::TruthSet listed_rows(const QueryExecutionContext& context,
                                      const std::string& binding_key,
                                      const roaring::Roaring* candidates) override {
        auto rows = _inner->listed_rows(context, binding_key, candidates);
        rows.null_rows |= candidates == nullptr ? *_nulls : *_nulls & *candidates;
        rows.true_rows -= rows.null_rows;
        if (_nulls_are_false) {
            rows.null_rows = roaring::Roaring();
        }
        return rows;
    }

private:
    WeightPtr _inner;
    std::shared_ptr<roaring::Roaring> _nulls;
    bool _nulls_are_false;
};

// A leaf whose rows in `nulls` are UNKNOWN (SQL) or FALSE (SEARCH).
class NullMaskedQuery final : public Query {
public:
    NullMaskedQuery(QueryPtr inner, std::shared_ptr<roaring::Roaring> nulls,
                    bool nulls_are_false = false)
            : _inner(std::move(inner)),
              _nulls(std::move(nulls)),
              _nulls_are_false(nulls_are_false) {}

    WeightPtr weight(bool enable_scoring) override {
        return std::make_shared<NullMaskedWeight>(_inner->weight(enable_scoring), _nulls,
                                                  _nulls_are_false);
    }

private:
    QueryPtr _inner;
    std::shared_ptr<roaring::Roaring> _nulls;
    bool _nulls_are_false;
};

} // namespace doris::segment_v2::inverted_index::query_v2
