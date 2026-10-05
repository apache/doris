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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>
#include <span>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/query/boolean/truth_set.h"
#include "storage/index/query/spi/index_source.h"
#include "storage/index/query/spi/postings_cursor.h"
#include "storage/index/query/spi/scoring_context.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// The term clauses of one boolean that read one source batching its reads, or of an unscored
// conjunction on any source. They are listed together instead of one document at a time: the
// conjunction as a chain narrowing the cheapest term's rows block by block, the disjunction as
// one round of reads a wave of terms at a time. Scored, the conjunction scores its listed rows
// on the positions its terms read a wave at a time and the source's norms, and the disjunction
// merges the terms' scores from the frequencies and norms read with their postings.
class ListedTerms {
public:
    // `nulls` are the rows the terms' field leaves UNKNOWN, null when there are none.
    ListedTerms(index_query::IndexSourcePtr source, std::shared_ptr<roaring::Roaring> nulls);

    const index_query::IndexSourcePtr& source() const { return _source; }
    // Adds a clause in ascending clause order, scored by `similarity` when the boolean scores.
    void add(size_t clause, std::string term,
             index_query::ScoringContextPtr<float> similarity = nullptr);
    // Whether the boolean's clause `clause` is listed here.
    bool holds(size_t clause) const;

    // Opens a conjunction's terms together, for its chain and its costs, and none when the
    // source surely lacks one of them; a disjunction opens its terms as it reads them.
    void open(bool conjunctive, bool scoring = false);
    // Whether the dictionary lacks a term, so the conjunction is FALSE everywhere.
    bool has_absent_term() const;
    // The fewest documents any term holds: what the conjunction's first term lists.
    uint64_t cheapest_doc_freq() const;
    // The rows holding every term among `candidates` (every row when null), with the field's
    // UNKNOWN rows; FALSE everywhere when a term is absent.
    index_query::TruthSet conjunction(const std::vector<uint32_t>* candidates);
    // The rows holding any term, with the field's UNKNOWN rows; FALSE everywhere when every
    // term is absent.
    index_query::TruthSet disjunction();
    // The rows holding every term, each with the sum of the terms' scores there; empty when a
    // term is absent.
    ScorerPtr scored_conjunction();
    // The rows holding any term, each with the sum of the scores of the terms holding it.
    ScorerPtr scored_disjunction();

private:
    index_query::IndexSourcePtr _source;
    std::shared_ptr<roaring::Roaring> _nulls;
    std::vector<size_t> _clauses;
    std::vector<std::string> _terms;
    std::vector<index_query::ScoringContextPtr<float>> _similarities;
    std::vector<std::unique_ptr<index_query::PostingsCursor>> _cursors;
};

} // namespace doris::segment_v2::inverted_index::query_v2
