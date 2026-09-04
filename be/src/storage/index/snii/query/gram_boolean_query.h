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
#include <string_view>

#include "common/status.h"
#include "storage/index/inverted/gram/gram_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"

// gram_boolean_query -- evaluates a gram::GramQuery boolean query tree against the gram-family
// dictionary/postings of one SNII segment, producing a docid bitmap. The index may only narrow
// the candidate set: a missing gram, an unsupported query shape or a failed lookup can only make
// the layer above degrade to "no acceleration" and must never change the query result, which is
// why every code path here returns a Status instead of asserting that a gram must exist.
namespace doris::snii::query {

// The posting data source gram_boolean_query() consumes. In production it is
// LogicalIndexPostingSource below, adapting a LogicalIndexReader; tests inject a map-based fake
// so that the AND/OR/ALL/NONE evaluation logic can be covered without building real index files.
class GramPostingSource {
public:
    virtual ~GramPostingSource() = default;
    // Look up the document frequency (df) of one gram. found=false means the gram is not in the
    // dictionary, in which case every AND node containing it evaluates to NONE (the empty set)
    // and no posting list is read at all.
    virtual Status df(std::string_view gram, bool* found, uint64_t* df) = 0;
    // Decode the complete docid set of one gram (without positions or term frequencies) into
    // out. Only called after df() has confirmed that the gram exists.
    virtual Status postings(std::string_view gram, roaring::Roaring* out) = 0;
};

// The production GramPostingSource implementation on top of LogicalIndexReader: df() is just an
// ordinary dictionary lookup, and postings() additionally decodes the docid-only posting.
class LogicalIndexPostingSource final : public GramPostingSource {
public:
    explicit LogicalIndexPostingSource(const reader::LogicalIndexReader& idx) : _idx(idx) {}
    Status df(std::string_view gram, bool* found, uint64_t* df) override;
    Status postings(std::string_view gram, roaring::Roaring* out) override;

private:
    const reader::LogicalIndexReader& _idx;
};

// Evaluate q against src: ALL -> [0, num_docs); NONE -> the empty set; AND first looks up the df
// of every gram leaf it holds directly (a single missing leaf short-circuits the whole node to
// empty without reading any posting), then intersects the remaining leaves in ascending df order
// and returns early as soon as the intersection is empty, and finally intersects with each
// sub-query the same way (an AND with neither leaves nor sub-queries degenerates to ALL); OR
// unions the postings of every leaf with the result of every sub-query. The recursion depth is
// bounded by the tree GramQuery::parse produces (which is capped at construction time).
Status gram_boolean_query(GramPostingSource& src, const segment_v2::gram::GramQuery& q,
                          uint32_t num_docs, roaring::Roaring* out);

} // namespace doris::snii::query
