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

class AllScorer : public Scorer {
public:
    explicit AllScorer(uint32_t max_doc) : _max_doc(max_doc) {
        if (_max_doc == 0) {
            _doc = TERMINATED;
        } else {
            _doc = 0;
        }
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

    float score() override { return 1.0F; }

    uint32_t size_hint() const override { return _max_doc; }

private:
    uint32_t _max_doc = 0;
    uint32_t _doc = TERMINATED;
};

class AllWeight : public Weight {
public:
    explicit AllWeight(uint32_t max_doc) : _max_doc(max_doc) {}

    ~AllWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& context) override {
        return std::make_shared<AllScorer>(_max_doc);
    }

private:
    uint32_t _max_doc = 0;
};

class AllQuery : public Query {
public:
    explicit AllQuery(uint32_t max_doc) : _max_doc(max_doc) {}

    ~AllQuery() override = default;

    WeightPtr weight(bool /*enable_scoring*/) override {
        return std::make_shared<AllWeight>(_max_doc);
    }

private:
    uint32_t _max_doc = 0;
};

} // namespace doris::segment_v2::inverted_index::query_v2
