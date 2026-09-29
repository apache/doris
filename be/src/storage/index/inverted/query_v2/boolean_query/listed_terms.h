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
#include <string>
#include <vector>

#include "storage/index/query/boolean/truth_set.h"
#include "storage/index/query/spi/index_source.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// The term clauses of one unscored boolean that read one source batching its reads. They are
// listed together instead of one document at a time: the conjunction as a chain narrowing the
// cheapest term's rows, the disjunction as one round of reads over every term.
class ListedTerms {
public:
    // `nulls` are the rows the terms' field leaves UNKNOWN, null when there are none.
    ListedTerms(index_query::IndexSourcePtr source, std::shared_ptr<roaring::Roaring> nulls);

    const index_query::IndexSourcePtr& source() const { return _source; }
    // Adds clause `clause` of the boolean, which asks for `term`.
    void add(size_t clause, std::string term);
    // Whether the boolean's clause `clause` is listed here.
    bool holds(size_t clause) const;

    // Opens every term together; the listings and the costs below need it.
    void open();
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

private:
    index_query::IndexSourcePtr _source;
    std::shared_ptr<roaring::Roaring> _nulls;
    std::vector<size_t> _clauses;
    std::vector<std::string> _terms;
    std::vector<std::unique_ptr<index_query::PostingsCursor>> _cursors;
};

} // namespace doris::segment_v2::inverted_index::query_v2
