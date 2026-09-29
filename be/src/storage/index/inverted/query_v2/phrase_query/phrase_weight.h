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
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "storage/index/inverted/query/query_info.h"
#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/inverted/query_v2/weight.h"
#include "storage/index/query/boolean/truth_set.h"
#include "storage/index/query/phrase/phrase_verifier.h"
#include "storage/index/query/spi/index_source.h"
#include "storage/index/query/spi/scoring_context.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// A phrase of single terms. It runs one document at a time over the terms' postings, except
// unscored on a source batching its reads: there it lists the rows holding every term as a
// chain, reads their positions in one round and verifies the phrase row by row on what it
// read, so a conjunction can hand it the rows its other clauses kept.
class PhraseWeight : public Weight {
public:
    PhraseWeight(std::wstring field, std::vector<TermInfo> term_infos,
                 index_query::PhraseQueryOptions options,
                 index_query::ScoringContextPtr<float> similarity, bool enable_scoring,
                 bool nullable);
    ~PhraseWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override;
    bool lists_rows(const QueryExecutionContext& ctx,
                    const std::string& binding_key) const override;
    index_query::TruthSet listed_rows(const QueryExecutionContext& ctx,
                                      const std::string& binding_key,
                                      const roaring::Roaring* candidates) override;

private:
    index_query::IndexSourcePtr _source(const QueryExecutionContext& ctx,
                                        const std::string& binding_key) const;
    bool _lists(const index_query::IndexSource& source) const {
        return !_enable_scoring && source.batches_reads();
    }
    ScorerPtr _streamed_scorer(index_query::IndexSource& source, uint32_t num_docs);
    ScorerPtr _listed_scorer(index_query::IndexSource& source, const roaring::Roaring* candidates);

    std::wstring _field;
    std::vector<TermInfo> _term_infos;
    index_query::PhraseQueryOptions _options;
    index_query::ScoringContextPtr<float> _similarity;
    bool _enable_scoring = false;
    bool _nullable = true;
};

} // namespace doris::segment_v2::inverted_index::query_v2
