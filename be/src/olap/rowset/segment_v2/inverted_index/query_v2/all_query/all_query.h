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
#include <memory>
#include <string>

#include "olap/rowset/segment_v2/inverted_index/query_v2/nullable_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class AllScorer;
class AllWeight;
class AllQuery;

using AllScorerPtr = std::shared_ptr<AllScorer>;
using AllWeightPtr = std::shared_ptr<AllWeight>;
using AllQueryPtr = std::shared_ptr<AllQuery>;

/// Scorer that matches all documents [0, max_doc).
/// Mirrors Lucene's MatchAllDocsQuery scorer with ConstantScoreWeight:
/// returns a constant score of 1.0 when scoring is enabled, 0.0 otherwise.
class AllScorer : public Scorer {
public:
    AllScorer(uint32_t max_doc, bool enable_scoring)
            : _max_doc(max_doc), _score(enable_scoring ? 1.0F : 0.0F) {
        _doc = (_max_doc == 0) ? TERMINATED : 0;
    }

    ~AllScorer() override = default;

    uint32_t doc() const override { return _doc; }

    uint32_t advance() override {
        if (_doc == TERMINATED) {
            return TERMINATED;
        }
        if (_doc + 1 >= _max_doc) {
            _doc = TERMINATED;
            return TERMINATED;
        }
        ++_doc;
        return _doc;
    }

    uint32_t seek(uint32_t target) override {
        if (_doc == TERMINATED) {
            return TERMINATED;
        }
        if (target >= _max_doc) {
            _doc = TERMINATED;
            return TERMINATED;
        }
        _doc = std::max(_doc, target);
        return _doc;
    }

    float score() override { return _score; }

    uint32_t size_hint() const override { return _max_doc; }

private:
    uint32_t _max_doc = 0;
    uint32_t _doc = TERMINATED;
    float _score;
};

/// Weight for AllQuery. Analogous to Lucene's ConstantScoreWeight used by MatchAllDocsQuery.
class AllWeight : public Weight {
public:
    explicit AllWeight(bool enable_scoring) : _enable_scoring(enable_scoring) {}

    AllWeight(std::wstring field, bool nullable, bool enable_scoring)
            : _field(std::move(field)), _nullable(nullable), _enable_scoring(enable_scoring) {}

    ~AllWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& context) override {
        auto inner = std::make_shared<AllScorer>(context.segment_num_rows, _enable_scoring);
        if (_nullable && context.null_resolver != nullptr) {
            std::string logical = logical_field_or_fallback(context, "", _field);
            return make_nullable_scorer(std::move(inner), logical, context.null_resolver);
        }
        return inner;
    }

private:
    std::wstring _field;
    bool _nullable = false;
    bool _enable_scoring = false;
};

/// Query that matches all documents, analogous to Lucene's MatchAllDocsQuery.
/// Uses constant scoring (score = 1.0) like Lucene's ConstantScoreWeight.
class AllQuery : public Query {
public:
    AllQuery() = default;
    AllQuery(std::wstring field, bool nullable) : _field(std::move(field)), _nullable(nullable) {}

    ~AllQuery() override = default;

    WeightPtr weight(bool enable_scoring) override {
        if (!_field.empty()) {
            return std::make_shared<AllWeight>(_field, _nullable, enable_scoring);
        }
        return std::make_shared<AllWeight>(enable_scoring);
    }

private:
    std::wstring _field;
    bool _nullable = false;
};

} // namespace doris::segment_v2::inverted_index::query_v2
