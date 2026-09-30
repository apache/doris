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
#include <optional>
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
#include "storage/index/query/term_pattern.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// The terms a phrase clause at `offset` matches: `terms`, or with `expand` the first
// `max_expansions` terms (every one when not positive) that its one text matches as that
// pattern, in dictionary order.
struct PhraseSlot {
    uint32_t offset = 0;
    std::vector<std::string> terms;
    std::optional<index_query::TermPatternKind> expand = std::nullopt;
    int32_t max_expansions = 0;
};

// A phrase whose every clause matches one of the terms of its slot. On a source that reads
// terms one at a time it runs one document at a time over the slots' postings. On a source
// batching its reads it lists the rows holding a term of every slot as a chain, reads their
// positions in one round and verifies the phrase row by row on what it read, so an unscored
// conjunction can hand it the rows its other clauses kept; scored, it scores each verified row
// on the phrase's frequency there and the source's norm.
class SlotPhraseWeight : public Weight {
public:
    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override;
    bool lists_rows(const QueryExecutionContext& ctx,
                    const std::string& binding_key) const override;
    index_query::TruthSet listed_rows(const QueryExecutionContext& ctx,
                                      const std::string& binding_key,
                                      const roaring::Roaring* candidates) override;

protected:
    SlotPhraseWeight(std::wstring field, index_query::PhraseQueryOptions options,
                     index_query::ScoringContextPtr<float> similarity, bool enable_scoring,
                     bool nullable);

    // The phrase's slots, in clause order.
    virtual std::vector<PhraseSlot> _slots() const = 0;
    // The phrase run one document at a time over the slots' postings.
    virtual ScorerPtr _streamed_scorer(index_query::IndexSource& source, uint32_t num_docs) = 0;

    std::wstring _field;
    index_query::PhraseQueryOptions _options;
    index_query::ScoringContextPtr<float> _similarity;
    bool _enable_scoring = false;
    bool _nullable = true;

private:
    index_query::IndexSourcePtr _source(const QueryExecutionContext& ctx,
                                        const std::string& binding_key) const;
    static bool _lists(const index_query::IndexSource& source) { return source.batches_reads(); }
    ScorerPtr _listed_scorer(index_query::IndexSource& source, const roaring::Roaring* candidates);
};

// A phrase of single terms.
class PhraseWeight final : public SlotPhraseWeight {
public:
    PhraseWeight(std::wstring field, std::vector<TermInfo> term_infos,
                 index_query::PhraseQueryOptions options,
                 index_query::ScoringContextPtr<float> similarity, bool enable_scoring,
                 bool nullable);

private:
    std::vector<PhraseSlot> _slots() const override;
    ScorerPtr _streamed_scorer(index_query::IndexSource& source, uint32_t num_docs) override;

    std::vector<TermInfo> _term_infos;
};

} // namespace doris::segment_v2::inverted_index::query_v2
