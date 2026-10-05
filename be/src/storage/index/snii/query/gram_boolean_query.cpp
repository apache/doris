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

#include "storage/index/snii/query/gram_boolean_query.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "storage/index/snii/format/bsbf.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_union.h"

namespace doris::snii::query {

// R8/R24 (unity build): file-level helper implementations live in this named namespace private
// to the file rather than in a bare anonymous one, so they cannot clash with symbols of other
// files under a unity build.
namespace gram_boolean_query_detail {

// Behaves exactly like RoaringDocIdSink in the anonymous namespace of snii_index_reader.cpp (a
// non-empty batch goes through addMany, a non-empty range through addRange(first,
// last_exclusive), and dedups()==true lets a multi-gram OR/AND stream postings straight into the
// same bitmap); that class cannot be reused across translation units, hence this copy.
class RoaringSink final : public DocIdSink {
public:
    explicit RoaringSink(roaring::Roaring* bitmap) : _bitmap(bitmap) {}

    Status append_sorted(std::span<const uint32_t> docids) override {
        if (!docids.empty()) {
            _bitmap->addMany(docids.size(), docids.data());
        }
        return Status::OK();
    }

    Status append_range(uint32_t first, uint64_t last_exclusive) override {
        if (last_exclusive > first) {
            _bitmap->addRange(first, last_exclusive);
        }
        return Status::OK();
    }

    bool dedups() const override { return true; }

private:
    roaring::Roaring* _bitmap;
};

// The cost gate of gram_boolean_query(): true when a candidate set of `candidates` rows is too
// large a fraction of the segment for the index IO to pay for itself. It is a COST predicate
// only -- the caller answers true by widening the node to the whole docid space, never by
// dropping rows. config::gram_index_max_candidate_ratio_bp == 0 disables the gate; a value of
// 10000 or more can never fire because a candidate set never exceeds the segment.
// True when `candidates` is too large for the index to pay for itself on this segment.
//
// `budget` comes from the segment's own term distribution (GramPostingSource::
// candidate_budget) and is preferred when available: it is the only form of this predicate
// that does not need retuning when the dataset changes. The basis-point ratio below is the
// fallback for segments written before the digest existed, and stays available as an
// override for anyone who needs to pin the old behaviour exactly.
bool exceeds_candidate_budget(uint64_t candidates, uint32_t num_docs, uint64_t budget) {
    if (budget > 0) {
        return candidates > budget;
    }
    const int32_t bp = config::gram_index_max_candidate_ratio_bp;
    if (bp <= 0 || num_docs == 0) {
        return false;
    }
    // Row floor. Below it the segment's whole gram index is a few KB and one or two requests:
    // giving up saves nothing measurable, and costs the pruning the index really does deliver at
    // that size. The ratio only starts to mean something once the skipped index IO can outweigh
    // the rows it stops eliminating.
    const int32_t min_rows = config::gram_index_candidate_ratio_min_rows;
    if (min_rows > 0 && num_docs < static_cast<uint32_t>(min_rows)) {
        return false;
    }
    return candidates * 10000 > static_cast<uint64_t>(num_docs) * static_cast<uint64_t>(bp);
}

// How many documents an AND over gram postings with these dfs is likely to keep.
//
// The gate used min(df), which is the exact upper bound: an intersection can never hold more
// docs than its rarest member. For grams cut out of a single ASCII literal that bound is also
// close to the truth, because those grams overlap almost perfectly -- they were produced by
// sliding over the same bytes, so a row holding one very likely holds the rest, and the
// intersection really is about as big as the rarest of them.
//
// Nothing guarantees that overlap, and one column shape breaks it completely. A CJK column
// indexes one term per code point, so the grams of a four-character literal are four ordinary
// characters that share little beyond the rows actually spelling it. Measured on weibo, a
// literal whose rarest character was still a common one had five matching rows and was
// abandoned by the gate, having eliminated none: min(df) said the node was a fifth of the
// segment when it was five documents.
//
// The opposite extreme, multiplying the selectivities as if the grams were independent,
// understates a correlated node by just as much and would disable the gate on the ASCII
// patterns it was built for. This takes the standard middle course: sort by df, take the
// rarest gram as the base, and let every further gram contribute its selectivity under an
// exponentially decaying exponent -- the second counts most, the third half as much, and so
// on. The result is never above min(df) and never below the independence product, so it
// degrades to the old behaviour exactly where the old behaviour was right, and it needs no
// statistics beyond the dfs the caller has already read.
//
// This is an estimate and not a bound, which the gate can afford: giving up widens a node to
// the whole segment and proceeding reads postings, so being wrong in either direction costs
// IO and never a row.
uint64_t estimate_and_candidates(std::vector<uint64_t> dfs, uint32_t num_docs) {
    if (dfs.empty() || num_docs == 0) {
        return 0;
    }
    std::ranges::sort(dfs);
    if (dfs.front() == 0) {
        return 0;
    }
    double estimate = static_cast<double>(dfs.front());
    double exponent = 1.0;
    for (size_t i = 1; i < dfs.size(); ++i) {
        exponent *= 0.5;
        if (dfs[i] == 0) {
            return 0;
        }
        const double selectivity =
                std::min(1.0, static_cast<double>(dfs[i]) / static_cast<double>(num_docs));
        estimate *= std::pow(selectivity, exponent);
    }
    // Round up so a node that survives at all is never estimated at zero documents, and clamp
    // to the exact upper bound: floating point must not let the estimate drift above min(df).
    const double rounded = std::ceil(estimate);
    return std::min(static_cast<uint64_t>(rounded), dfs.front());
}

Status eval(GramPostingSource& src, const segment_v2::gram::GramQuery& q, uint32_t num_docs,
            roaring::Roaring* out, GramGateStats* gate_stats);

// Records one node the gate widened rather than read. Only cost decisions pass through here:
// widening because a node means ALL (an AND with no grams, an OR holding a stop-gram) is not a
// gate decision and must stay uncounted, or the counter stops meaning "the index gave up".
void note_gave_up(GramGateStats* gate_stats) {
    if (gate_stats != nullptr) {
        ++gate_stats->nodes_given_up;
    }
}

// AND. The order here is deliberate and each step earns its place:
//
//  1. one batched dictionary lookup for the gram leaves. A gram missing from the segment makes
//     the whole node definitively empty, which is where most of this index's value comes from,
//     and finding that out costs one dictionary round and no postings.
//  2. the sub-queries, evaluated before the leaves are read. An AND is commutative, so this
//     changes no answer, and it means the sub-query's cardinality -- an exact upper bound on
//     the whole node -- is in hand when the gate judges the leaves. Judging the leaves alone
//     is how `(A|B)C` was abandoned: the grams of C were common, and the alternation that held
//     the node to a handful of rows was never looked at.
//  3. the cost gate, on the smaller of the leaf estimate and what the sub-queries already
//     narrowed to.
//  4. the leaf postings, through one batched conjunction.
//
// An AND with neither gram leaves nor sub-queries counts as ALL.
Status eval_and(GramPostingSource& src, const segment_v2::gram::GramQuery& q, uint32_t num_docs,
                roaring::Roaring* out, GramGateStats* gate_stats) {
    bool has_acc = false;
    roaring::Roaring acc;
    std::vector<std::string> constraining_grams;
    std::vector<GramDf> constraining_dfs;
    const uint64_t budget = src.candidate_budget();

    if (!q.grams.empty()) {
        // Cheapest arm of the gate, and the only one that costs nothing. The estimate needs
        // every gram's df, so it takes an exactly-known df for EVERY gram: the digest holds
        // only the segment's common terms, and a gram it cannot bound is one rarer than the
        // digest's floor -- exactly the gram most likely to carry the node. Estimating
        // without it would systematically overstate the node and give up on the patterns the
        // index exists for. When they are all known no dictionary block is read at all, which
        // on a cold segment is the difference between 40 seconds and none.
        //
        // It is skipped entirely when the node has sub-queries: nothing here can bound those,
        // and giving up on the leaves while a sub-query holds the node to a few rows is the
        // exact mistake this function is arranged to avoid. Such a node pays one dictionary
        // round instead.
        if (budget > 0 && q.subs.empty()) {
            std::vector<uint64_t> known_df;
            std::vector<uint8_t> known;
            src.known_dfs(q.grams, &known_df, &known);
            bool all_known = true;
            for (size_t i = 0; i < q.grams.size(); ++i) {
                if (known[i] == 0) {
                    all_known = false;
                    break;
                }
            }
            if (all_known && exceeds_candidate_budget(estimate_and_candidates(known_df, num_docs),
                                                      num_docs, budget)) {
                note_gave_up(gate_stats);
                out->addRange(0, num_docs);
                return Status::OK();
            }
        }
        std::vector<GramDf> dfs;
        RETURN_IF_ERROR(src.dfs(q.grams, &dfs));
        for (const GramDf& gram_df : dfs) {
            if (!gram_df.found) {
                // The index only produces a superset of candidates: a missing gram makes the
                // whole AND branch definitively empty, so return right away without reading any
                // posting -- and without evaluating a sub-query that could not change it.
                return Status::OK();
            }
        }
        // A stop-gram matches every document, so it narrows nothing and has no posting to
        // read. Dropping it from the conjunction leaves the node's meaning unchanged
        // (X AND ALL == X) and keeps it away from the planner, which refuses such a term
        // outright. When they are all stop-grams the gram leaves constrain nothing at all:
        // the node becomes its sub-queries, or ALL if it has none, which is exactly what the
        // tail below already does.
        constraining_grams.reserve(q.grams.size());
        constraining_dfs.reserve(dfs.size());
        for (size_t i = 0; i < q.grams.size(); ++i) {
            if (!dfs[i].matches_all) {
                constraining_grams.push_back(q.grams[i]);
                constraining_dfs.push_back(dfs[i]);
            }
        }
    }

    for (const auto& sub : q.subs) {
        roaring::Roaring cur;
        RETURN_IF_ERROR(eval(src, sub, num_docs, &cur, gate_stats));
        if (!has_acc) {
            acc = std::move(cur);
            has_acc = true;
        } else {
            acc &= cur;
        }
        if (acc.isEmpty()) {
            return Status::OK();
        }
    }

    if (!constraining_grams.empty()) {
        // Cost gate, before any posting read. Giving up here rather than after and_postings()
        // is the entire point: a node that turns out too large has already paid for every
        // posting it read by then, and measurements on remote object storage show that cost is
        // not recovered -- the surviving candidates are too scattered for a single page of the
        // column to be skipped, so the column is read in full anyway and the index reads are
        // pure overhead, up to a 1.4x regression against not using the index at all.
        //
        // The node is bounded by two things: what its own leaves are likely to intersect to,
        // and what the sub-queries have already narrowed it to. The second is not an estimate
        // at all -- those rows have been counted -- so it wins whenever it is smaller.
        uint64_t estimate = estimate_and_candidates(
                [&] {
                    std::vector<uint64_t> values;
                    values.reserve(constraining_dfs.size());
                    for (const GramDf& gram_df : constraining_dfs) {
                        values.push_back(gram_df.df);
                    }
                    return values;
                }(),
                num_docs);
        if (has_acc) {
            estimate = std::min(estimate, acc.cardinality());
        }
        if (exceeds_candidate_budget(estimate, num_docs, budget)) {
            note_gave_up(gate_stats);
            out->addRange(0, num_docs);
            return Status::OK();
        }
        roaring::Roaring leaves;
        RETURN_IF_ERROR(src.and_postings(constraining_grams, &leaves));
        if (!has_acc) {
            acc = std::move(leaves);
            has_acc = true;
        } else {
            acc &= leaves;
        }
        if (acc.isEmpty()) {
            return Status::OK();
        }
    }

    if (!has_acc) {
        // Neither gram leaves nor sub-queries: the AND degenerates to ALL.
        out->addRange(0, num_docs);
        return Status::OK();
    }
    *out |= acc;
    return Status::OK();
}

// OR: union the postings of all gram leaves held directly through one batched union, then union
// the evaluation result of every sub-query.
Status eval_or(GramPostingSource& src, const segment_v2::gram::GramQuery& q, uint32_t num_docs,
               roaring::Roaring* out, GramGateStats* gate_stats) {
    if (!q.grams.empty()) {
        const uint64_t budget = src.candidate_budget();
        // The mirror of the AND's no-IO arm, and a cheaper test: a union is at least as large
        // as any one member, so a SINGLE gram known to be over budget settles the node. No
        // need for the others to be known at all.
        if (budget > 0) {
            std::vector<uint64_t> known_df;
            std::vector<uint8_t> known;
            src.known_dfs(q.grams, &known_df, &known);
            for (size_t i = 0; i < q.grams.size(); ++i) {
                if (known[i] != 0 && exceeds_candidate_budget(known_df[i], num_docs, budget)) {
                    note_gave_up(gate_stats);
                    out->addRange(0, num_docs);
                    return Status::OK();
                }
            }
        }
        std::vector<GramDf> dfs;
        RETURN_IF_ERROR(src.dfs(q.grams, &dfs));
        // A union is at least as large as its largest member, so max(df) over the gram leaves is
        // a lower bound on this node's candidate count -- enough to fire the cost gate before a
        // single posting (or any sub-query) is read.
        uint64_t max_df = 0;
        for (const GramDf& gram_df : dfs) {
            if (gram_df.matches_all) {
                // One member that matches every document makes the whole union everything,
                // whatever the others hold and whatever the sub-queries add. Nothing left to
                // read.
                out->addRange(0, num_docs);
                return Status::OK();
            }
            if (gram_df.found) {
                max_df = std::max(max_df, gram_df.df);
            }
        }
        if (exceeds_candidate_budget(max_df, num_docs, budget)) {
            note_gave_up(gate_stats);
            out->addRange(0, num_docs);
            return Status::OK();
        }
        RETURN_IF_ERROR(src.or_postings(q.grams, out));
    }
    for (const auto& sub : q.subs) {
        roaring::Roaring cur;
        RETURN_IF_ERROR(eval(src, sub, num_docs, &cur, gate_stats));
        *out |= cur;
    }
    return Status::OK();
}

Status eval(GramPostingSource& src, const segment_v2::gram::GramQuery& q, uint32_t num_docs,
            roaring::Roaring* out, GramGateStats* gate_stats) {
    using Op = segment_v2::gram::GramQuery::Op;
    switch (q.op) {
    case Op::ALL:
        out->addRange(0, num_docs);
        return Status::OK();
    case Op::NONE:
        return Status::OK();
    case Op::AND:
        return eval_and(src, q, num_docs, out, gate_stats);
    case Op::OR:
        return eval_or(src, q, num_docs, out, gate_stats);
    }
    return Status::OK();
}

} // namespace gram_boolean_query_detail

Status LogicalIndexPostingSource::_resolve(const std::vector<std::string>& grams) {
    std::vector<std::string> missing;
    missing.reserve(grams.size());
    for (const std::string& gram : grams) {
        if (!_resolved.contains(gram)) {
            missing.push_back(gram);
        }
    }
    if (missing.empty()) {
        return Status::OK();
    }
    // lookup_batch() requires a sorted, duplicate-free batch and returns results aligned with it;
    // the memo below maps them back to the caller's order.
    std::sort(missing.begin(), missing.end());
    missing.erase(std::unique(missing.begin(), missing.end()), missing.end());
    std::vector<reader::LogicalIndexReader::BatchLookupResult> results;
    RETURN_IF_ERROR(_idx.lookup_batch(missing, &results));
    for (size_t i = 0; i < missing.size(); ++i) {
        _resolved.emplace(std::move(missing[i]), std::move(results[i]));
    }
    return Status::OK();
}

// The gate's budget, taken from this segment's own term distribution instead of a ratio
// configured for some other dataset.
//
// The digest holds the segment's highest-df terms and reports the ceiling below which every
// term it omitted sits. That ceiling is the segment's own answer to "how common is common
// here", which is exactly the question a fixed basis-point ratio has to guess at -- and it
// guesses badly, because it applies one proportion to a 500k-row table and a 34M-row one
// whose page counts differ seventyfold.
//
// The multiplier says how far above that line a node has to be before pruning stops paying.
// It is dimensionless and stable across datasets, which is what a configured ratio is not:
// on a flat vocabulary the digest fills to its cap, the ceiling rises, and the budget rises
// with it -- the segment widens its own gate exactly when its grams are least selective.
// Small segments move the other way: their ceiling falls toward zero, the gate tightens, and
// the index stops being read for a table a scan handles trivially. That is the behaviour the
// configured row floor used to approximate.
uint64_t LogicalIndexPostingSource::candidate_budget() const {
    const auto& digest = _idx.high_df_terms();
    if (digest.empty() && digest.df_ceiling == 0) {
        // Written before the digest existed: no basis, caller falls back to the config.
        return 0;
    }
    return static_cast<uint64_t>(digest.df_ceiling) * kCandidateBudgetCeilingMultiple;
}

void LogicalIndexPostingSource::known_dfs(const std::vector<std::string>& grams,
                                          std::vector<uint64_t>* out,
                                          std::vector<uint8_t>* known) const {
    out->assign(grams.size(), 0);
    known->assign(grams.size(), 0);
    const auto& digest = _idx.high_df_terms();
    if (digest.empty()) {
        return;
    }
    for (size_t i = 0; i < grams.size(); ++i) {
        bool exact = false;
        const uint32_t df = digest.df_upper_bound(format::bsbf_hash(grams[i]), &exact);
        if (exact) {
            (*out)[i] = df;
            (*known)[i] = 1;
        }
    }
}

Status LogicalIndexPostingSource::dfs(const std::vector<std::string>& grams,
                                      std::vector<GramDf>* out) {
    out->assign(grams.size(), GramDf {});
    if (grams.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_resolve(grams));
    for (size_t i = 0; i < grams.size(); ++i) {
        const auto it = _resolved.find(grams[i]);
        if (it == _resolved.end() || !it->second.found) {
            continue;
        }
        (*out)[i].found = true;
        (*out)[i].df = it->second.entry.df;
        (*out)[i].matches_all = it->second.entry.posting_dropped;
    }
    return Status::OK();
}

Status LogicalIndexPostingSource::and_postings(const std::vector<std::string>& grams,
                                               roaring::Roaring* out) {
    if (grams.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_resolve(grams));
    std::vector<internal::ResolvedQueryTerm> resolved;
    resolved.reserve(grams.size());
    for (const std::string& gram : grams) {
        const auto it = _resolved.find(gram);
        if (it == _resolved.end() || !it->second.found) {
            // A gram missing from the dictionary makes the intersection definitively empty.
            return Status::OK();
        }
        resolved.push_back({it->second.entry, it->second.frq_base, it->second.prx_base});
    }
    // The same plan/fetch/leapfrog pipeline boolean_and() runs, minus the per-term dictionary
    // resolution it would repeat: one batched prelude round, then a docid-only conjunction that
    // drives on the rarest gram and skips whole windows of the denser ones.
    io::BatchRangeFetcher round1(_idx.reader());
    std::vector<internal::TermPlan> plans;
    RETURN_IF_ERROR(internal::plan_resolved_terms(_idx, std::move(resolved), &round1, &plans,
                                                  /*need_positions=*/false));
    if (round1.pending() > 0) {
        RETURN_IF_ERROR(round1.fetch());
    }
    RETURN_IF_ERROR(internal::open_preludes(round1, &plans, /*need_positions=*/false));
    std::vector<uint32_t> docids;
    RETURN_IF_ERROR(internal::build_docid_only_conjunction(_idx, round1, plans, &docids));
    if (!docids.empty()) {
        out->addMany(docids.size(), docids.data());
    }
    return Status::OK();
}

Status LogicalIndexPostingSource::or_postings(const std::vector<std::string>& grams,
                                              roaring::Roaring* out) {
    if (grams.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_resolve(grams));
    std::vector<internal::ResolvedDocidPosting> postings;
    postings.reserve(grams.size());
    for (const std::string& gram : grams) {
        const auto it = _resolved.find(gram);
        if (it == _resolved.end() || !it->second.found) {
            continue;
        }
        postings.push_back({it->second.entry, it->second.frq_base, it->second.prx_base});
    }
    if (postings.empty()) {
        return Status::OK();
    }
    // Same union pipeline boolean_or() runs: windowed postings share one prelude round and one
    // docid round, and each posting is streamed straight into the (deduplicating) bitmap.
    gram_boolean_query_detail::RoaringSink sink(out);
    return internal::emit_docid_union(_idx, postings, &sink);
}

Status gram_boolean_query(GramPostingSource& src, const segment_v2::gram::GramQuery& q,
                          uint32_t num_docs, roaring::Roaring* out, GramGateStats* gate_stats) {
    RETURN_IF_ERROR(gram_boolean_query_detail::eval(src, q, num_docs, out, gate_stats));
    // Second arm of the cost gate, for the shapes whose size no df bound could predict (a
    // multi-gram AND above all). It cannot give the IO back, but it avoids an equally pointless
    // bitmap intersection above and makes "this index pruned nothing" visible instead of hiding
    // it behind a candidate set that happens to be nearly the whole segment. Widening to the full
    // range only ever adds rows.
    const uint64_t candidates = out->cardinality();
    if (gram_boolean_query_detail::exceeds_candidate_budget(candidates, num_docs,
                                                            src.candidate_budget())) {
        // Only a result that still holds something back is a give-up here. A node that already
        // abandoned itself above arrives as the whole segment and would otherwise be counted
        // twice, which turns the counter from "nodes the gate widened" into a number no reader
        // can interpret.
        if (candidates < num_docs) {
            gram_boolean_query_detail::note_gave_up(gate_stats);
        }
        out->addRange(0, num_docs);
    }
    return Status::OK();
}

} // namespace doris::snii::query
