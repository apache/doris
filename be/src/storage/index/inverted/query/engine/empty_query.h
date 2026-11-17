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
#include <string>
#include <utility>

#include "storage/index/inverted/query/engine/nullable_scorer.h"
#include "storage/index/inverted/query/engine/query.h"
#include "storage/index/inverted/query/engine/weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// Matches no row of `field`; the rows where the field is NULL stay UNKNOWN.
class EmptyWeight final : public Weight {
public:
    explicit EmptyWeight(std::wstring field) : _field(std::move(field)) {}

    ScorerPtr scorer(const QueryExecutionContext& context,
                     const std::string& binding_key) override {
        return make_nullable_scorer(std::make_shared<EmptyScorer>(),
                                    logical_field_or_fallback(context, binding_key, _field),
                                    context.null_resolver);
    }

private:
    std::wstring _field;
};

// A leaf the analyzer turned into no term.
class EmptyQuery final : public Query {
public:
    explicit EmptyQuery(std::wstring field) : _field(std::move(field)) {}

    WeightPtr weight(bool /*enable_scoring*/) override {
        return std::make_shared<EmptyWeight>(_field);
    }

private:
    std::wstring _field;
};

} // namespace doris::segment_v2::inverted_index::query_v2
