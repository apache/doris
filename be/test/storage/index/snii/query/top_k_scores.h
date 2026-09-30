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
#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/query_v2/boolean_query/operator.h"
#include "storage/index/inverted/query_v2/boolean_query/operator_boolean_weight.h"
#include "storage/index/inverted/query_v2/score_combiner.h"
#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/inverted/query_v2/term_query/term_weight.h"
#include "storage/index/inverted/query_v2/weight.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_index_source.h"
#include "storage/index/snii/stats/snii_stats_provider.h"

namespace doris::snii::snii_test {

// BM25's parameters, which the engine fixes; a reference ranking takes them as inputs.
struct Bm25Params {
    double k1 = 1.2;
    double b = 0.75;
};

struct ScoredDoc {
    uint32_t docid = 0;
    double score = 0.0;
};

// The `k` best documents of the disjunction of `terms`, scored through the shared engine over
// the index's source with the segment's own statistics (idf from each term's document
// frequency, the average length from the stats block), ties in ascending docid order.
inline Status top_k_scores(const reader::LogicalIndexReader& idx,
                           const stats::SniiStatsProvider& stats,
                           const std::vector<std::string>& terms, uint32_t k,
                           std::vector<ScoredDoc>* out) {
    namespace query_v2 = segment_v2::inverted_index::query_v2;
    out->clear();
    if (k == 0) {
        return Status::OK();
    }
    auto source = std::make_shared<reader::SniiIndexSource>(idx);
    const std::wstring field = L"body";
    std::vector<query_v2::WeightPtr> weights;
    std::vector<std::string> keys;
    for (const std::string& term : terms) {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        RETURN_IF_ERROR(idx.lookup(term, &found, &entry, &frq_base, &prx_base));
        if (!found) {
            continue;
        }
        const double n = static_cast<double>(stats.indexed_doc_count());
        const double df = static_cast<double>(entry.df);
        const auto idf = static_cast<float>(std::log(1.0 + (n - df + 0.5) / (df + 0.5)));
        weights.push_back(std::make_shared<query_v2::TermWeight>(
                field, term,
                std::make_shared<segment_v2::BM25Similarity>(idf,
                                                             static_cast<float>(stats.avgdl())),
                /*enable_scoring=*/true));
        keys.emplace_back();
    }
    std::vector<ScoredDoc> scored;
    if (!weights.empty()) {
        query_v2::OperatorBooleanWeight<query_v2::SumCombinerPtr> weight(
                query_v2::OperatorType::OP_OR, std::move(weights), std::move(keys),
                std::make_shared<query_v2::SumCombiner>());
        query_v2::QueryExecutionContext context;
        context.segment_num_rows = source->doc_count();
        context.field_sources.emplace(field, source);
        auto scorer = weight.scorer(context);
        for (uint32_t doc = scorer->doc(); doc != query_v2::TERMINATED; doc = scorer->advance()) {
            scored.push_back({.docid = doc, .score = scorer->score()});
        }
    }
    std::ranges::sort(scored, [](const ScoredDoc& a, const ScoredDoc& b) {
        return a.score != b.score ? a.score > b.score : a.docid < b.docid;
    });
    if (scored.size() > k) {
        scored.resize(k);
    }
    *out = std::move(scored);
    return Status::OK();
}

} // namespace doris::snii::snii_test
