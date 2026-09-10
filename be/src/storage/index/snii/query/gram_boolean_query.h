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
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/gram/gram_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"

// gram_boolean_query -- evaluates a gram::GramQuery boolean query tree against the gram-family
// dictionary/postings of one SNII segment, producing a docid bitmap. The index may only narrow
// the candidate set: a missing gram, an unsupported query shape or a failed lookup can only make
// the layer above degrade to "no acceleration" and must never change the query result, which is
// why every code path here returns a Status instead of asserting that a gram must exist.
namespace doris::snii::query {

// One gram's dictionary resolution, aligned position by position with the gram vector handed to
// GramPostingSource::dfs(). found=false means the gram is absent from this segment's dictionary.
// How far above the segment's own "commonness line" a node has to sit before pruning stops
// paying. The line itself comes from the segment (the high-df digest's ceiling); this is the
// dimensionless factor applied to it, and unlike a basis-point ratio it does not have to be
// retuned per dataset -- it scales with whatever that segment's distribution turns out to be.
//
// 3 reproduces the behaviour the previously configured 15 bp had on the measured corpora: on
// an untruncated digest the ceiling sits at one part in kHighDfDigestDivisor (5 bp), and
// three times that is the same line the sweep found admitted no regression.
inline constexpr uint64_t kCandidateBudgetCeilingMultiple = 3;

struct GramDf {
    bool found = false;
    uint64_t df = 0;
    // stop-gram: the gram is in the dictionary with a real df, but its posting list was
    // dropped at write time, so it constrains nothing -- it matches every document. It is
    // the opposite of !found, which matches none, and the two must never be conflated: one
    // widens a node, the other empties it.
    bool matches_all = false;
};

// The posting data source gram_boolean_query() consumes. Every entry point takes the WHOLE gram
// list of one query node rather than a single gram: on object storage each dictionary lookup and
// each posting read is an independent remote request, so a per-gram interface turns a k-gram node
// into O(k) serial round trips whose latency dominates the query. In production this is
// LogicalIndexPostingSource below, adapting a LogicalIndexReader; tests inject a map-based fake so
// that the AND/OR/ALL/NONE evaluation logic can be covered without building real index files.
class GramPostingSource {
public:
    virtual ~GramPostingSource() = default;

    // Look up the document frequency (df) of every gram in one batch. out is aligned with grams;
    // out[i].found=false means that gram is not in the dictionary, in which case every AND node
    // containing it evaluates to NONE (the empty set) and no posting list is read at all.
    // out[i].matches_all means the opposite: the gram IS in the dictionary but its posting list
    // was dropped at write time (stop-gram), so it matches every document and constrains nothing.
    virtual Status dfs(const std::vector<std::string>& grams, std::vector<GramDf>* out) = 0;

    // Decode the intersection of the docid sets of all grams (without positions or term
    // frequencies) into out. Only called after dfs() has confirmed that every gram exists, and
    // never with a gram dfs() reported as matches_all -- the evaluator drops those first, since
    // they have no posting list to intersect and would not narrow the result if they had one.
    virtual Status and_postings(const std::vector<std::string>& grams, roaring::Roaring* out) = 0;

    // Decode the union of the docid sets of all grams into out. Grams absent from the dictionary
    // simply contribute nothing to the union. Never called when any gram is matches_all: such a
    // union is the whole segment, which the evaluator answers from the df batch alone.
    virtual Status or_postings(const std::vector<std::string>& grams, roaring::Roaring* out) = 0;

    // ---- The no-IO arm of the cost gate ----
    //
    // dfs() above costs one remote dictionary round trip per gram -- 40 seconds on a segment
    // whose cache holds nothing, and the gate frequently spends it only to conclude the node
    // is not worth pruning. These two calls let it reach that conclusion first, for free.

    // The candidate count above which this segment is not worth pruning, derived from the
    // segment's own term distribution. 0 means the source cannot supply one and the caller
    // falls back to the configured ratio.
    virtual uint64_t candidate_budget() const { return 0; }

    // Exact df for the grams the source can answer without IO; known[i] == 0 for the rest,
    // whose df is then only bounded from above (by the budget's own basis) and so might
    // still be rare enough to be worth reading. out and known are sized to grams.
    //
    // The asymmetry is the point. An AND is estimated from ALL its dfs, so one unknown gram
    // sinks the arm: the digest holds only common terms, so an unknown gram is a rare one --
    // exactly the gram most likely to carry the node. An OR is the mirror, because a union is
    // at least as large as any member: one known gram over budget settles it.
    virtual void known_dfs(const std::vector<std::string>& grams, std::vector<uint64_t>* out,
                           std::vector<uint8_t>* known) const {
        out->assign(grams.size(), 0);
        known->assign(grams.size(), 0);
    }
};

// The production GramPostingSource on top of LogicalIndexReader. All three entry points share one
// request-scoped dictionary memo: dfs() resolves a batch through LogicalIndexReader::lookup_batch
// (bounded waves of concurrent DICT block reads instead of one blocking read per gram) and keeps
// the resolved DictEntry, so the and_postings()/or_postings() call that follows reads postings
// straight from those entries and never looks the same gram up a second time. Posting reads then
// go through the same batched plan/fetch pipeline boolean_and()/boolean_or() use, so a node costs
// a constant number of round trips instead of one per gram. The memo lives exactly as long as
// this object, which production creates per index-query call.
class LogicalIndexPostingSource final : public GramPostingSource {
public:
    explicit LogicalIndexPostingSource(const reader::LogicalIndexReader& idx) : _idx(idx) {}
    Status dfs(const std::vector<std::string>& grams, std::vector<GramDf>* out) override;
    Status and_postings(const std::vector<std::string>& grams, roaring::Roaring* out) override;
    Status or_postings(const std::vector<std::string>& grams, roaring::Roaring* out) override;
    uint64_t candidate_budget() const override;
    void known_dfs(const std::vector<std::string>& grams, std::vector<uint64_t>* out,
                   std::vector<uint8_t>* known) const override;

private:
    // Resolves, in one batch, every gram of the list that the memo does not hold yet.
    Status _resolve(const std::vector<std::string>& grams);

    const reader::LogicalIndexReader& _idx;
    std::unordered_map<std::string, reader::LogicalIndexReader::BatchLookupResult> _resolved;
};

// Evaluate q against src: ALL -> [0, num_docs); NONE -> the empty set; AND looks up the df of all
// of its gram leaves in one batch (a single missing leaf short-circuits the whole node to empty
// without reading any posting), intersects the leaves through one batched conjunction, then
// intersects with each sub-query and returns early as soon as the intersection is empty (an AND
// with neither leaves nor sub-queries degenerates to ALL); OR unions the postings of all its gram
// leaves through one batched union, then unions the result of every sub-query. The recursion
// depth is bounded by the query tree produced by RegexGramCompiler's depth-limited analysis.
//
// COST GATE (config::gram_index_max_candidate_ratio_bp, in basis points of the segment; 0
// disables it; applied only to segments of at least config::gram_index_candidate_ratio_min_rows
// rows). When the candidate set
// is a large fraction of a segment big enough for that to matter, the index IO is paid in full
// for almost no pruning, so a node whose estimated candidate count already exceeds the ratio
// gives up before reading any posting -- an AND is estimated from all its dfs together, an OR
// from its largest, which is an exact lower bound on a union -- and the final result gives up if
// it ends up above the ratio anyway.
// "Giving up" is a cost decision and NEVER a semantic one: it yields the whole [0, num_docs)
// range, so the caller's `row_bitmap &= result` is a no-op and not a single row can be pruned
// away. Widening a candidate set is always safe (the expression above re-verifies every candidate
// row); narrowing it is not, which is why no path here may ever produce fewer docids than the
// true candidate set.
// What the gate decided, for the profile. Every defect this gate has had presented the same way
// from outside -- the index was consulted and eliminated nothing -- and a profile could only show
// it indirectly, as a candidate count equal to the segment with zero rows filtered. That is an
// inference a reader has to already suspect to make; this is the decision itself.
struct GramGateStats {
    // Nodes the cost gate widened to the whole segment rather than read. Counts gate decisions
    // only: an AND with no grams, or an OR holding a stop-gram, is ALL for reasons of meaning
    // rather than cost and is not counted here.
    uint32_t nodes_given_up = 0;
};

Status gram_boolean_query(GramPostingSource& src, const segment_v2::gram::GramQuery& q,
                          uint32_t num_docs, roaring::Roaring* out,
                          GramGateStats* gate_stats = nullptr);

} // namespace doris::snii::query
