#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "snii/query/bm25_scorer.h"
#include "snii/reader/logical_index_reader.h"
#include "snii/stats/snii_stats_provider.h"

// scoring_query -- top-K BM25 scored retrieval over one logical index for one or
// more query terms. Two entry points produce IDENTICAL rankings:
//   - scoring_query_exhaustive(): scores every candidate document (the baseline
//     correctness oracle).
//   - scoring_query_wand(): a block-max / WAND-style optimization that uses the
//     per-window max_freq / max_norm columns from the frq_prelude to bound each
//     window's best possible score and SKIP windows that cannot enter the
//     current top-K. A window without block-max stats (slim/inline entries or a
//     missing prelude) is never pruned, so the result still equals the
//     exhaustive ranking.
//
// Results are sorted by score descending; ties are broken by ascending docid so
// the ordering is deterministic and the two paths compare equal.
namespace snii::query {

// One scored hit.
struct ScoredDoc {
    uint32_t docid = 0;
    double score = 0.0;
};

// Exhaustive baseline: score every doc that contains any query term, return the
// top-k by score. params controls k1/b. Unknown terms are skipped.
doris::Status scoring_query_exhaustive(const snii::reader::LogicalIndexReader& idx,
                                const snii::stats::SniiStatsProvider& stats,
                                const std::vector<std::string>& terms, uint32_t k,
                                const Bm25Params& params, std::vector<ScoredDoc>* out);

// WAND-style block-max pruning. MUST return the same top-k as the exhaustive
// path. Windows whose block-max upper bound cannot beat the current k-th score
// are skipped; windows lacking block-max stats are scored fully.
doris::Status scoring_query_wand(const snii::reader::LogicalIndexReader& idx,
                          const snii::stats::SniiStatsProvider& stats,
                          const std::vector<std::string>& terms, uint32_t k,
                          const Bm25Params& params, std::vector<ScoredDoc>* out);

// SELECTIVE-FETCH block-max WAND (design spec section 5, "Phase C"). Same WAND /
// theta / >= tie machinery as scoring_query_wand, but it DEFERS the .frq window
// fetch: for each windowed term it first reads ONLY the frq_prelude (block-max
// columns), then fetches a term's .frq window lazily and at most once -- and ONLY
// when the running block-max bound proves a doc in that window can still reach the
// top-K (bound >= theta). A window the bound rules out is never fetched. The
// result (top-K docids AND scores, INCLUDING ties) is byte-identical to
// scoring_query_exhaustive / scoring_query_wand; only the bytes read differ.
// Slim/inline terms (no prelude) are fetched fully, exactly as today.
doris::Status scoring_query_wand_selective(const snii::reader::LogicalIndexReader& idx,
                                    const snii::stats::SniiStatsProvider& stats,
                                    const std::vector<std::string>& terms, uint32_t k,
                                    const Bm25Params& params, std::vector<ScoredDoc>* out);

} // namespace snii::query
