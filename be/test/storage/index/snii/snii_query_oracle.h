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

// The SNII query entries the tests read indexes back with, answered by the shared engine over
// an SniiIndexSource: each entry plans the logical leaf MATCH would and runs it on `run_leaf`;
// the frequency entries score a phrase weight with a context whose score is the frequency.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <roaring/roaring.hh>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/query_v2/expand_query/expand_weight.h"
#include "storage/index/inverted/query_v2/phrase_prefix_query/phrase_prefix_weight.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_weight.h"
#include "storage/index/inverted/query_v2/weight.h"
#include "storage/index/query/docid_sink.h"
#include "storage/index/query/logical/node.h"
#include "storage/index/query/phrase/phrase_verifier.h"
#include "storage/index/query/spi/scoring_context.h"
#include "storage/index/query/term_pattern.h"
#include "storage/index/snii/format/prx_decode_stats.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/io_metrics.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_index_source.h"
#include "storage/olap_common.h"

namespace doris::snii::query {

// What one entry cost: its wall time, the reader's I/O metrics before and after when the reader
// meters them, and the PRX frames the source decoded.
struct QueryProfile {
    uint64_t elapsed_ns = 0;
    bool has_io_metrics = false;
    io::IoMetrics io_before;
    io::IoMetrics io_after;
    io::IoMetrics io_delta;
    format::PrxDecodeStats prx_decode_stats;
};

class QueryProfileScope {
public:
    QueryProfileScope(io::FileReader* reader, QueryProfile* profile)
            : _reader(reader), _profile(profile) {
        if (_profile == nullptr) {
            return;
        }
        _start = std::chrono::steady_clock::now();
        *_profile = QueryProfile {};
        if (_reader == nullptr) {
            return;
        }
        const io::IoMetrics* metrics = _reader->io_metrics();
        if (metrics == nullptr) {
            return;
        }
        _profile->has_io_metrics = true;
        _profile->io_before = *metrics;
    }
    ~QueryProfileScope() { finish(); }
    QueryProfileScope(const QueryProfileScope&) = delete;
    QueryProfileScope& operator=(const QueryProfileScope&) = delete;

    void finish() {
        if (_profile == nullptr || _finished) {
            return;
        }
        _finished = true;
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now() - _start)
                                     .count();
        _profile->elapsed_ns = std::max<uint64_t>(1, static_cast<uint64_t>(elapsed));
        if (!_profile->has_io_metrics || _reader == nullptr) {
            return;
        }
        const io::IoMetrics* metrics = _reader->io_metrics();
        if (metrics == nullptr) {
            _profile->has_io_metrics = false;
            return;
        }
        _profile->io_after = *metrics;
        _profile->io_delta = io::delta(_profile->io_after, _profile->io_before);
    }

private:
    io::FileReader* _reader = nullptr;
    QueryProfile* _profile = nullptr;
    std::chrono::steady_clock::time_point _start;
    bool _finished = false;
};

struct PhraseMatch {
    uint32_t docid = 0;
    float frequency = 0.0F;

    bool operator==(const PhraseMatch&) const = default;
};

using index_query::PhraseQueryOptions;

struct PhrasePrefixQueryOptions {
    int32_t max_expansions = 0;
    // Restricts the result the way PhraseQueryOptions::candidates does.
    const roaring::Roaring* candidates = nullptr;
};

namespace oracle {

namespace logical = index_query::logical;
namespace query_v2 = segment_v2::inverted_index::query_v2;

inline const std::wstring kField = L"body";
inline const logical::FieldRef kFieldRef {.name = "body", .binding = "body"};

// A scoring context whose score is the frequency itself.
class FrequencyScoring final : public index_query::ScoringContext<float> {
public:
    float score(float frequency, int64_t /*encoded_norm*/) override { return frequency; }
    float max_score() override { return std::numeric_limits<float>::max(); }
    void bind_norms(std::span<const float> /*lengths*/) override {}
};

inline std::vector<segment_v2::TermInfo> slots(const std::vector<std::string>& terms) {
    std::vector<segment_v2::TermInfo> slots;
    slots.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        slots.push_back({.term = terms[i], .position = static_cast<int32_t>(i)});
    }
    return slots;
}

// One entry's run: a session capping expansions at `max_expansions` (0 for none), the
// statistics the leaf runner times, and the profile filled from the reader's metrics and the
// source's PRX decodes.
class Run {
public:
    Run(const reader::LogicalIndexReader& idx, int32_t max_expansions, QueryProfile* profile)
            : _profile(profile),
              _scope(idx.reader(), profile),
              _source(std::make_shared<reader::SniiIndexSource>(idx, &_prx_stats)) {
        TQueryOptions options;
        options.inverted_index_max_expansions = max_expansions;
        _state.set_query_options(options);
        _context->stats = &_stats;
        _context->runtime_state = &_state;
    }
    ~Run() { finish(); }
    Run(const Run&) = delete;
    Run& operator=(const Run&) = delete;

    const segment_v2::IndexQueryContextPtr& context() const { return _context; }

    // The rows the unscored leaf matches, restricted to `candidates` when given.
    Status leaf(const logical::Node& leaf, const roaring::Roaring* candidates,
                std::vector<uint32_t>* docids) {
        auto result = std::make_shared<roaring::Roaring>();
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(segment_v2::run_leaf(_context, kField, leaf, candidates,
                                                                /*scoring=*/false, _source,
                                                                _source->doc_count(), result));
        docids->assign(result->begin(), result->end());
        finish();
        return Status::OK();
    }

    // The rows the weight matches, each with its score.
    Status scored(const query_v2::WeightPtr& weight, std::vector<PhraseMatch>* matches) {
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION([&]() -> Status {
            query_v2::QueryExecutionContext execution;
            execution.segment_num_rows = _source->doc_count();
            execution.field_sources.emplace(kField, _source);
            auto scorer = weight->scorer(execution);
            for (uint32_t doc = scorer->doc(); doc != query_v2::TERMINATED;
                 doc = scorer->advance()) {
                matches->push_back({.docid = doc, .frequency = scorer->score()});
            }
            return Status::OK();
        }());
        finish();
        return Status::OK();
    }

private:
    void finish() {
        _scope.finish();
        if (_profile != nullptr) {
            _profile->prx_decode_stats = _prx_stats;
        }
    }

    QueryProfile* _profile;
    QueryProfileScope _scope;
    format::PrxDecodeStats _prx_stats;
    std::shared_ptr<reader::SniiIndexSource> _source;
    OlapReaderStatistics _stats;
    RuntimeState _state;
    segment_v2::IndexQueryContextPtr _context = std::make_shared<segment_v2::IndexQueryContext>();
};

inline Status into_sink(const std::vector<uint32_t>& docids, index_query::DocIdSink* sink) {
    if (sink == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("snii query: null sink");
    }
    return sink->append_sorted(docids);
}

inline Status require_output(const void* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("snii query: null output");
    }
    return Status::OK();
}

inline Status expand(const reader::LogicalIndexReader& idx, logical::ExpandKind kind,
                     std::string_view pattern, std::vector<uint32_t>* docids, QueryProfile* profile,
                     int32_t max_expansions) {
    RETURN_IF_ERROR(require_output(docids));
    Run run(idx, max_expansions, profile);
    return run.leaf(logical::Node {.value = logical::Expand {.field = kFieldRef,
                                                             .kind = kind,
                                                             .pattern = std::string(pattern)}},
                    nullptr, docids);
}

inline Status term_set(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                       bool require_all, std::vector<uint32_t>* docids, QueryProfile* profile) {
    RETURN_IF_ERROR(require_output(docids));
    docids->clear();
    if (terms.empty()) {
        return Status::OK();
    }
    Run run(idx, 0, profile);
    return run.leaf(logical::Node {.value = logical::TermSet {.field = kFieldRef,
                                                              .terms = terms,
                                                              .require_all = require_all}},
                    nullptr, docids);
}

inline Status phrase(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                     std::vector<uint32_t>* docids, QueryProfile* profile,
                     const PhraseQueryOptions& options, bool prefix, bool suffix,
                     int32_t max_expansions) {
    RETURN_IF_ERROR(require_output(docids));
    docids->clear();
    if (terms.empty()) {
        return Status::OK();
    }
    Run run(idx, max_expansions, profile);
    // A phrase of one term is that term, as the engine lowers it.
    if (!prefix && !suffix && terms.size() == 1) {
        return run.leaf(
                logical::Node {.value = logical::Term {.field = kFieldRef, .term = terms.front()}},
                options.candidates, docids);
    }
    // One item of an edge phrase matches every term that contains it.
    if (prefix && suffix && terms.size() == 1) {
        return run.leaf(
                logical::Node {.value = logical::Expand {.field = kFieldRef,
                                                         .kind = logical::ExpandKind::kContains,
                                                         .pattern = terms.front()}},
                options.candidates, docids);
    }
    return run.leaf(
            logical::Node {.value = logical::Phrase {.field = kFieldRef,
                                                     .slots = slots(terms),
                                                     .slop = static_cast<int32_t>(options.slop),
                                                     .ordered = options.ordered,
                                                     .prefix = prefix,
                                                     .suffix = suffix}},
            options.candidates, docids);
}

} // namespace oracle

inline Status term_query(const reader::LogicalIndexReader& idx, std::string_view term,
                         std::vector<uint32_t>* docids, QueryProfile* profile = nullptr) {
    RETURN_IF_ERROR(oracle::require_output(docids));
    oracle::Run run(idx, 0, profile);
    return run.leaf(
            oracle::logical::Node {.value = oracle::logical::Term {.field = oracle::kFieldRef,
                                                                   .term = std::string(term)}},
            nullptr, docids);
}

inline Status term_query(const reader::LogicalIndexReader& idx, std::string_view term,
                         index_query::DocIdSink* sink) {
    std::vector<uint32_t> docids;
    RETURN_IF_ERROR(term_query(idx, term, &docids));
    return oracle::into_sink(docids, sink);
}

inline Status boolean_or(const reader::LogicalIndexReader& idx,
                         const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                         QueryProfile* profile = nullptr) {
    return oracle::term_set(idx, terms, /*require_all=*/false, docids, profile);
}

inline Status boolean_or(const reader::LogicalIndexReader& idx,
                         const std::vector<std::string>& terms, index_query::DocIdSink* sink) {
    std::vector<uint32_t> docids;
    RETURN_IF_ERROR(boolean_or(idx, terms, &docids));
    return oracle::into_sink(docids, sink);
}

inline Status boolean_and(const reader::LogicalIndexReader& idx,
                          const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                          QueryProfile* profile = nullptr) {
    return oracle::term_set(idx, terms, /*require_all=*/true, docids, profile);
}

inline Status prefix_query(const reader::LogicalIndexReader& idx, std::string_view prefix,
                           std::vector<uint32_t>* docids, int32_t max_expansions = 0) {
    return oracle::expand(idx, oracle::logical::ExpandKind::kPrefix, prefix, docids, nullptr,
                          max_expansions);
}

inline Status prefix_query(const reader::LogicalIndexReader& idx, std::string_view prefix,
                           std::vector<uint32_t>* docids, QueryProfile* profile,
                           int32_t max_expansions = 0) {
    return oracle::expand(idx, oracle::logical::ExpandKind::kPrefix, prefix, docids, profile,
                          max_expansions);
}

inline Status regexp_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                           std::vector<uint32_t>* docids, int32_t max_expansions = 0) {
    return oracle::expand(idx, oracle::logical::ExpandKind::kRegexp, pattern, docids, nullptr,
                          max_expansions);
}

inline Status regexp_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                           std::vector<uint32_t>* docids, QueryProfile* profile,
                           int32_t max_expansions = 0) {
    return oracle::expand(idx, oracle::logical::ExpandKind::kRegexp, pattern, docids, profile,
                          max_expansions);
}

inline Status regexp_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                           index_query::DocIdSink* sink, int32_t max_expansions = 0) {
    std::vector<uint32_t> docids;
    RETURN_IF_ERROR(regexp_query(idx, pattern, &docids, max_expansions));
    return oracle::into_sink(docids, sink);
}

inline Status prefix_query(const reader::LogicalIndexReader& idx, std::string_view prefix,
                           index_query::DocIdSink* sink, int32_t max_expansions = 0) {
    std::vector<uint32_t> docids;
    RETURN_IF_ERROR(prefix_query(idx, prefix, &docids, max_expansions));
    return oracle::into_sink(docids, sink);
}

inline Status wildcard_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                             std::vector<uint32_t>* docids, int32_t max_expansions = 0) {
    return oracle::expand(idx, oracle::logical::ExpandKind::kWildcard, pattern, docids, nullptr,
                          max_expansions);
}

inline Status wildcard_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                             std::vector<uint32_t>* docids, QueryProfile* profile,
                             int32_t max_expansions = 0) {
    return oracle::expand(idx, oracle::logical::ExpandKind::kWildcard, pattern, docids, profile,
                          max_expansions);
}

inline Status wildcard_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                             index_query::DocIdSink* sink, int32_t max_expansions = 0) {
    std::vector<uint32_t> docids;
    RETURN_IF_ERROR(wildcard_query(idx, pattern, &docids, max_expansions));
    return oracle::into_sink(docids, sink);
}

inline Status phrase_query(const reader::LogicalIndexReader& idx,
                           const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                           QueryProfile* profile = nullptr,
                           const PhraseQueryOptions& options = {}) {
    return oracle::phrase(idx, terms, docids, profile, options, /*prefix=*/false,
                          /*suffix=*/false, 0);
}

inline Status phrase_query_with_frequencies(const reader::LogicalIndexReader& idx,
                                            const std::vector<std::string>& terms,
                                            std::vector<PhraseMatch>* matches,
                                            QueryProfile* profile = nullptr,
                                            const PhraseQueryOptions& options = {}) {
    RETURN_IF_ERROR(oracle::require_output(matches));
    matches->clear();
    if (terms.empty()) {
        return Status::OK();
    }
    oracle::Run run(idx, 0, profile);
    auto weight = std::make_shared<oracle::query_v2::PhraseWeight>(
            oracle::kField, oracle::slots(terms), options,
            std::make_shared<oracle::FrequencyScoring>(), /*enable_scoring=*/true,
            /*nullable=*/true);
    return run.scored(weight, matches);
}

inline Status phrase_prefix_query(const reader::LogicalIndexReader& idx,
                                  const std::vector<std::string>& terms,
                                  std::vector<uint32_t>* docids, QueryProfile* profile,
                                  const PhrasePrefixQueryOptions& options) {
    return oracle::phrase(idx, terms, docids, profile,
                          PhraseQueryOptions {.candidates = options.candidates},
                          /*prefix=*/true, /*suffix=*/false, options.max_expansions);
}

inline Status phrase_prefix_query(const reader::LogicalIndexReader& idx,
                                  const std::vector<std::string>& terms,
                                  std::vector<uint32_t>* docids, int32_t max_expansions = 0) {
    return phrase_prefix_query(idx, terms, docids, nullptr,
                               PhrasePrefixQueryOptions {.max_expansions = max_expansions});
}

inline Status phrase_prefix_query(const reader::LogicalIndexReader& idx,
                                  const std::vector<std::string>& terms,
                                  std::vector<uint32_t>* docids, QueryProfile* profile,
                                  int32_t max_expansions = 0) {
    return phrase_prefix_query(idx, terms, docids, profile,
                               PhrasePrefixQueryOptions {.max_expansions = max_expansions});
}

inline Status phrase_edge_query(const reader::LogicalIndexReader& idx,
                                const std::vector<std::string>& terms,
                                std::vector<uint32_t>* docids, QueryProfile* profile,
                                const PhrasePrefixQueryOptions& options) {
    return oracle::phrase(idx, terms, docids, profile,
                          PhraseQueryOptions {.candidates = options.candidates},
                          /*prefix=*/true, /*suffix=*/true, options.max_expansions);
}

inline Status phrase_prefix_query_with_frequencies(const reader::LogicalIndexReader& idx,
                                                   const std::vector<std::string>& terms,
                                                   std::vector<PhraseMatch>* matches,
                                                   QueryProfile* profile,
                                                   const PhrasePrefixQueryOptions& options) {
    RETURN_IF_ERROR(oracle::require_output(matches));
    matches->clear();
    if (terms.empty()) {
        return Status::OK();
    }
    oracle::Run run(idx, options.max_expansions, profile);
    oracle::query_v2::WeightPtr weight;
    if (terms.size() == 1) {
        // Only the prefix: the terms under it, each occurrence counting one.
        weight = std::make_shared<oracle::query_v2::ExpandWeight>(
                run.context(), oracle::kField, index_query::TermPatternKind::kPrefix,
                terms.front());
    } else {
        std::vector<std::pair<size_t, std::string>> phrase_terms;
        for (size_t i = 0; i + 1 < terms.size(); ++i) {
            phrase_terms.emplace_back(i, terms[i]);
        }
        weight = std::make_shared<oracle::query_v2::PhrasePrefixWeight>(
                oracle::kField, std::move(phrase_terms),
                std::make_pair(terms.size() - 1, terms.back()),
                std::make_shared<oracle::FrequencyScoring>(), /*enable_scoring=*/true,
                options.max_expansions, PhraseQueryOptions {.candidates = options.candidates},
                /*suffix=*/false,
                /*nullable=*/true);
    }
    return run.scored(weight, matches);
}

inline Status phrase_prefix_query_with_frequencies(const reader::LogicalIndexReader& idx,
                                                   const std::vector<std::string>& terms,
                                                   std::vector<PhraseMatch>* matches,
                                                   QueryProfile* profile = nullptr,
                                                   int32_t max_expansions = 0) {
    return phrase_prefix_query_with_frequencies(
            idx, terms, matches, profile,
            PhrasePrefixQueryOptions {.max_expansions = max_expansions});
}

} // namespace doris::snii::query
