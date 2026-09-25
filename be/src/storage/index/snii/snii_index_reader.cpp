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

#include "storage/index/snii/snii_index_reader.h"

#include <CLucene.h>
#include <CLucene/util/stringUtil.h>
#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/common/single_flight.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/query/docid_sink.h"
#include "storage/index/query/logical/search_lowering.h"
#include "storage/index/query/roaring_docid_sink.h"
#include "storage/index/query/term_pattern.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/count_query.h"
#include "storage/index/snii/query/internal/plain_term_routing.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/regexp_query.h"
#include "storage/index/snii/query/scoring_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/query/wildcard_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/index/snii/snii_prx_profile.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
#include "util/defer_op.h"
#include "util/time.h"

#ifdef BE_TEST
namespace doris::snii::testing {
namespace {

std::atomic<uint64_t> prx_execution_profile_scope_constructions {0};
std::atomic<uint64_t> prx_execution_profile_scope_flushes {0};

} // namespace

void record_prx_execution_profile_scope_construction() {
    prx_execution_profile_scope_constructions.fetch_add(1, std::memory_order_relaxed);
}

void record_prx_execution_profile_scope_flush() {
    prx_execution_profile_scope_flushes.fetch_add(1, std::memory_order_relaxed);
}

void reset_prx_execution_profile_scope_counters() {
    prx_execution_profile_scope_constructions.store(0, std::memory_order_relaxed);
    prx_execution_profile_scope_flushes.store(0, std::memory_order_relaxed);
}

uint64_t prx_execution_profile_scope_construction_count() {
    return prx_execution_profile_scope_constructions.load(std::memory_order_relaxed);
}

uint64_t prx_execution_profile_scope_flush_count() {
    return prx_execution_profile_scope_flushes.load(std::memory_order_relaxed);
}

} // namespace doris::snii::testing
#endif

namespace doris::segment_v2 {

namespace {

struct SniiQueryExecutionResult {
    std::shared_ptr<roaring::Roaring> bitmap;
    std::vector<::doris::snii::query::PhraseMatch> phrase_matches;
};

std::vector<std::string> to_terms(const InvertedIndexQueryInfo& query_info) {
    std::vector<std::string> terms;
    terms.reserve(query_info.term_infos.size());
    for (const auto& term_info : query_info.term_infos) {
        DCHECK(term_info.is_single_term());
        terms.push_back(term_info.get_single_term());
    }
    return terms;
}

InvertedIndexQueryType expand_query_type(index_query::logical::ExpandKind kind) {
    switch (kind) {
    case index_query::logical::ExpandKind::kPrefix:
        // A one-term phrase prefix runs as a prefix of that term.
        return InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
    case index_query::logical::ExpandKind::kRegexp:
        return InvertedIndexQueryType::MATCH_REGEXP_QUERY;
    case index_query::logical::ExpandKind::kContains:
        // A one-term edge phrase runs as the terms that contain it.
        return InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY;
    case index_query::logical::ExpandKind::kWildcard:
    default:
        return InvertedIndexQueryType::WILDCARD_QUERY;
    }
}

bool uses_plain_term_frequency_scoring(InvertedIndexQueryType query_type,
                                       const InvertedIndexQueryInfo& query_info) {
    return query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
           query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
           (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY &&
            query_info.term_infos.size() == 1);
}

bool uses_phrase_frequency_scoring(InvertedIndexQueryType query_type,
                                   const InvertedIndexQueryInfo& query_info) {
    return query_info.term_infos.size() > 1 &&
           (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY);
}

Status score_plain_term_candidates(const IndexQueryContextPtr& context,
                                   std::string_view column_name,
                                   const InvertedIndexQueryInfo& query_info,
                                   const ::doris::snii::reader::LogicalIndexReader& logical_reader,
                                   const ::doris::snii::stats::SniiStatsProvider& segment_stats,
                                   const roaring::Roaring& final_candidates) {
    DORIS_CHECK(context->collection_statistics != nullptr);
    DORIS_CHECK(context->collection_similarity != nullptr);

    const std::wstring field_name = StringUtil::string_to_wstring(std::string(column_name));
    const double collection_avgdl =
            context->collection_statistics->get_or_calculate_avg_dl(field_name);
    std::vector<::doris::snii::query::CollectionScoringTerm> scoring_terms;
    scoring_terms.reserve(query_info.term_infos.size());
    for (const auto& term_info : query_info.term_infos) {
        DORIS_CHECK(term_info.is_single_term());
        const std::string& logical_term = term_info.get_single_term();
        RETURN_IF_ERROR(::doris::snii::query::internal::check_term_outside_internal_namespace(
                logical_term));
        const double idf = context->collection_statistics->get_or_calculate_idf(
                field_name, StringUtil::string_to_wstring(logical_term));
        scoring_terms.push_back({.physical_term = logical_term, .idf = idf});
    }
    DORIS_CHECK(final_candidates.isEmpty() || !scoring_terms.empty());

    std::vector<::doris::snii::query::ScoredDoc> scored_docs;
    RETURN_IF_ERROR(::doris::snii::query::scoring_query_candidates(
            logical_reader, segment_stats, scoring_terms, final_candidates, collection_avgdl,
            ::doris::snii::query::Bm25Params {}, &scored_docs));
    for (const auto& scored_doc : scored_docs) {
        context->collection_similarity->collect(scored_doc.docid,
                                                static_cast<float>(scored_doc.score));
    }
    return Status::OK();
}

Status score_phrase_matches(const IndexQueryContextPtr& context, std::string_view column_name,
                            InvertedIndexQueryType query_type,
                            const InvertedIndexQueryInfo& query_info,
                            const ::doris::snii::reader::LogicalIndexReader& logical_reader,
                            const ::doris::snii::stats::SniiStatsProvider& segment_stats,
                            const roaring::Roaring& final_candidates,
                            const std::vector<::doris::snii::query::PhraseMatch>& matches) {
    DORIS_CHECK(context->collection_statistics != nullptr);
    DORIS_CHECK(context->collection_similarity != nullptr);
    DORIS_CHECK(uses_phrase_frequency_scoring(query_type, query_info));
    DORIS_CHECK_EQ(final_candidates.cardinality(), matches.size());

    const std::wstring field_name = StringUtil::string_to_wstring(std::string(column_name));
    const double collection_avgdl =
            context->collection_statistics->get_or_calculate_avg_dl(field_name);
    const size_t idf_term_count = query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY
                                          ? query_info.term_infos.size() - 1
                                          : query_info.term_infos.size();
    double idf_sum = 0.0;
    for (size_t i = 0; i < idf_term_count; ++i) {
        const auto& term_info = query_info.term_infos[i];
        DORIS_CHECK(term_info.is_single_term());
        idf_sum += context->collection_statistics->get_or_calculate_idf(
                field_name, StringUtil::string_to_wstring(term_info.get_single_term()));
    }

    const auto scorer = ::doris::snii::query::ScorerContext::from_idf(idf_sum);
    std::vector<::doris::snii::query::ScoredDoc> scored_docs;
    scored_docs.reserve(matches.size());
    for (const auto& match : matches) {
        DCHECK(final_candidates.contains(match.docid));
        DCHECK_NE(match.frequency, 0);
        uint8_t norm = 0;
        RETURN_IF_ERROR(segment_stats.encoded_norm(match.docid, &norm));
        scored_docs.push_back({.docid = match.docid,
                               .score = scorer.score(match.frequency, norm, collection_avgdl,
                                                     ::doris::snii::query::Bm25Params {})});
    }
    for (const auto& scored_doc : scored_docs) {
        context->collection_similarity->collect(scored_doc.docid,
                                                static_cast<float>(scored_doc.score));
    }
    return Status::OK();
}

// Multi-term phrases verify positions per document, so only they gain from restricting the
// docid intersection to the scan candidates; every other query computes the full segment.
bool consumes_candidates(InvertedIndexQueryType query_type, size_t term_count) {
    return term_count > 1 && (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY ||
                              query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY ||
                              query_type == InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY);
}

std::shared_ptr<roaring::Roaring> docids_to_bitmap(const std::vector<uint32_t>& docids) {
    auto result = std::make_shared<roaring::Roaring>();
    if (!docids.empty()) {
        result->addMany(docids.size(), docids.data());
    }
    result->runOptimize();
    return result;
}

// Runs `compute` under single-flight keyed by `key`: concurrent identical queries collapse to a
// single execution and the followers reuse the leader's bitmap. `compute(out)` fills *out and
// returns its Status; on overall success *result receives the bitmap. See SingleFlight for why
// this matters under a cold cache with parallel scanners hitting the same segment.
template <typename Compute>
Status run_query_single_flight(
        ::doris::segment_v2::inverted_index::SingleFlight<
                std::pair<Status, std::shared_ptr<roaring::Roaring>>>& flight,
        const std::string& key, std::shared_ptr<roaring::Roaring>* result,
#ifdef BE_TEST
        SniiIndexReader::SingleFlightFollowerJoinedObserver follower_joined_observer,
        void* follower_joined_opaque,
        SniiIndexReader::SingleFlightLeaderBeforeComputeObserver leader_before_compute_observer,
        void* leader_before_compute_opaque,
#endif
        Compute&& compute) {
    auto follower = flight.join_or_lead(key);
    if (follower.has_value()) {
#ifdef BE_TEST
        if (follower_joined_observer != nullptr) {
            follower_joined_observer(follower_joined_opaque);
        }
#endif
        auto [leader_status, leader_bitmap] = follower->get();
        if (leader_status.ok() && leader_bitmap != nullptr) {
            *result = std::move(leader_bitmap);
            return Status::OK();
        }
        // Leader failed; fall through and compute independently (rare error path).
    }
    const bool is_leader = !follower.has_value();
#ifdef BE_TEST
    if (is_leader && leader_before_compute_observer != nullptr) {
        leader_before_compute_observer(leader_before_compute_opaque);
    }
#endif

    Status status = Status::OK();
    std::shared_ptr<roaring::Roaring> bitmap;
    {
        // Publish to any waiting followers on every exit path (including errors).
        DEFER(if (is_leader) { flight.publish(key, std::make_pair(status, bitmap)); });
        status = compute(&bitmap);
    }
    RETURN_IF_ERROR(status);
    *result = std::move(bitmap);
    return Status::OK();
}

// Keep every query type's dispatch to the SNII executors in one switch.
// NOLINTNEXTLINE(readability-function-size)
Status execute_snii_query(const ::doris::snii::reader::LogicalIndexReader& logical_reader,
                          InvertedIndexQueryType query_type,
                          const InvertedIndexQueryInfo& query_info, std::string_view search_str,
                          const std::vector<std::string>& terms, int32_t max_expansions,
                          bool collect_phrase_frequency, SniiQueryExecutionResult* result,
                          ::doris::snii::query::QueryProfile* profile,
                          const roaring::Roaring* candidates) {
    result->bitmap = std::make_shared<roaring::Roaring>();
    result->phrase_matches.clear();
    DORIS_CHECK(!collect_phrase_frequency || uses_phrase_frequency_scoring(query_type, query_info));
    DORIS_CHECK(candidates == nullptr || consumes_candidates(query_type, terms.size()));
    index_query::RoaringDocIdSink sink(*result->bitmap);
    std::vector<uint32_t> docids;
    bool emitted_to_sink = false;
    Status status;
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY:
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
        status = terms.size() == 1
                         ? ::doris::snii::query::term_query(logical_reader, terms.front(), &sink)
                         : ::doris::snii::query::boolean_or(logical_reader, terms, &sink);
        emitted_to_sink = true;
        break;
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
        if (terms.size() == 1) {
            status = ::doris::snii::query::term_query(logical_reader, terms.front(), &sink);
            emitted_to_sink = true;
        } else {
            status = ::doris::snii::query::boolean_and(logical_reader, terms, &docids);
        }
        break;
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
        if (terms.size() == 1) {
            status = ::doris::snii::query::term_query(logical_reader, terms.front(), &sink);
            emitted_to_sink = true;
        } else {
            status = collect_phrase_frequency
                             ? ::doris::snii::query::phrase_query_with_frequencies(
                                       logical_reader, terms, &result->phrase_matches, profile,
                                       {.slop = static_cast<uint32_t>(query_info.slop),
                                        .ordered = query_info.ordered,
                                        .candidates = candidates})
                             : ::doris::snii::query::phrase_query(
                                       logical_reader, terms, &docids, profile,
                                       {.slop = static_cast<uint32_t>(query_info.slop),
                                        .ordered = query_info.ordered,
                                        .candidates = candidates});
        }
        break;
    case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
        if (terms.size() == 1) {
            status = ::doris::snii::query::prefix_query(logical_reader, terms.front(), &sink,
                                                        max_expansions);
            emitted_to_sink = true;
        } else {
            status =
                    collect_phrase_frequency
                            ? ::doris::snii::query::phrase_prefix_query_with_frequencies(
                                      logical_reader, terms, &result->phrase_matches, profile,
                                      {.max_expansions = max_expansions, .candidates = candidates})
                            : ::doris::snii::query::phrase_prefix_query(
                                      logical_reader, terms, &docids, profile,
                                      {.max_expansions = max_expansions, .candidates = candidates});
        }
        break;
    case InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY:
        status = ::doris::snii::query::phrase_edge_query(
                logical_reader, terms, &docids, profile,
                {.max_expansions = max_expansions, .candidates = candidates});
        break;
    case InvertedIndexQueryType::MATCH_REGEXP_QUERY:
        status = ::doris::snii::query::regexp_query(logical_reader, search_str, &sink,
                                                    max_expansions);
        emitted_to_sink = true;
        break;
    case InvertedIndexQueryType::WILDCARD_QUERY:
        status = ::doris::snii::query::wildcard_query(logical_reader, search_str, &sink,
                                                      max_expansions);
        emitted_to_sink = true;
        break;
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII unsupported inverted index query type {}", query_type_to_string(query_type));
    }
    RETURN_IF_ERROR(status);
    if (collect_phrase_frequency) {
        for (const auto& match : result->phrase_matches) {
            result->bitmap->add(match.docid);
        }
        result->bitmap->runOptimize();
    } else if (emitted_to_sink) {
        result->bitmap->runOptimize();
    } else {
        result->bitmap = docids_to_bitmap(docids);
    }
    return Status::OK();
}

} // namespace

Status plan_native_query(index_query::logical::Node&& leaf, NativeQuery* out) {
    namespace logical = index_query::logical;
    DCHECK(out->query_info.term_infos.empty());
    std::vector<TermInfo>& term_infos = out->query_info.term_infos;
    if (auto* term = std::get_if<logical::Term>(&leaf.value)) {
        out->query_type = InvertedIndexQueryType::EQUAL_QUERY;
        term_infos.emplace_back(std::move(term->term));
    } else if (auto* set = std::get_if<logical::TermSet>(&leaf.value)) {
        // The executors know all-of and any-of; the SEARCH compile step counts a
        // threshold above them.
        DORIS_CHECK(set->min_should_match == 0);
        out->query_type = set->require_all ? InvertedIndexQueryType::MATCH_ALL_QUERY
                                           : InvertedIndexQueryType::MATCH_ANY_QUERY;
        term_infos.reserve(set->terms.size());
        for (auto& value : set->terms) {
            term_infos.emplace_back(std::move(value));
        }
    } else if (auto* phrase = std::get_if<logical::Phrase>(&leaf.value)) {
        out->query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY;
        if (phrase->suffix) {
            out->query_type = InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY;
        } else if (phrase->prefix) {
            out->query_type = InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
        }
        term_infos = std::move(phrase->slots);
        out->query_info.slop = phrase->slop;
        out->query_info.ordered = phrase->ordered;
    } else if (auto* expand = std::get_if<logical::Expand>(&leaf.value)) {
        out->query_type = expand_query_type(expand->kind);
        term_infos.emplace_back(std::move(expand->pattern));
        out->scored = false;
    } else {
        return Status::InternalError("leaf kind {} cannot run on SNII", leaf.value.index());
    }
    return Status::OK();
}

Status SniiIndexReader::new_iterator(std::unique_ptr<IndexIterator>* iterator) {
    if (*iterator == nullptr) {
        *iterator = InvertedIndexIterator::create_unique();
    }
    dynamic_cast<InvertedIndexIterator*>(iterator->get())
            ->add_reader(_reader_type,
                         dynamic_pointer_cast<InvertedIndexReader>(shared_from_this()));
    return Status::OK();
}

Status SniiIndexReader::_get_logical_reader(
        const IndexQueryContextPtr& context, InvertedIndexCacheHandle* searcher_cache_handle,
        std::unique_ptr<::doris::snii::reader::LogicalIndexReader>* uncached_reader,
        const ::doris::snii::reader::LogicalIndexReader** logical_reader) {
    DCHECK(searcher_cache_handle != nullptr);
    DCHECK(uncached_reader != nullptr);
    DCHECK(logical_reader != nullptr);

    const bool enable_searcher_cache =
            context->runtime_state != nullptr &&
            context->runtime_state->query_options().enable_inverted_index_searcher_cache;
    const auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);

    bool cache_hit = false;
    if (enable_searcher_cache) {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_lookup_timer);
        cache_hit = InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                                   searcher_cache_handle);
    }

    if (cache_hit) {
        context->stats->inverted_index_searcher_cache_hit++;
        *logical_reader = searcher_cache_handle->get_snii_logical_reader();
        if (*logical_reader == nullptr) {
            return Status::InternalError("SNII searcher cache entry has no logical reader");
        }
        return Status::OK();
    }

    SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_open_timer);
    context->stats->inverted_index_searcher_cache_miss++;
#ifdef BE_TEST
    if (_searcher_open_observer != nullptr) {
        _searcher_open_observer(_searcher_open_opaque);
    }
#endif
    RETURN_IF_ERROR(
            _index_file_reader->init(config::inverted_index_read_buffer_size, context->io_ctx));
    auto opened_reader =
            DORIS_TRY(_index_file_reader->open_snii_index(&_index_meta, context->io_ctx));

    if (!enable_searcher_cache) {
        *logical_reader = opened_reader.get();
        *uncached_reader = std::move(opened_reader);
        return Status::OK();
    }

    const size_t reader_size = std::max<size_t>(opened_reader->memory_usage(), 1);
    auto* cache_value = new InvertedIndexSearcherCache::CacheValue(
            std::move(opened_reader), reader_size, UnixMillis(), _index_file_reader);
    InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                   searcher_cache_handle);
    *logical_reader = searcher_cache_handle->get_snii_logical_reader();
    if (*logical_reader == nullptr) {
        return Status::InternalError("SNII searcher cache insert produced empty logical reader");
    }
    return Status::OK();
}

Status SniiIndexReader::query(const IndexQueryContextPtr& context, const std::string& column_name,
                              const Field& query_value, InvertedIndexQueryType query_type,
                              std::shared_ptr<roaring::Roaring>& bit_map,
                              const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    return _query(context, column_name, query_value, query_type, bit_map, nullptr, analyzer_ctx);
}

Status SniiIndexReader::query_with_null_bitmap(
        const IndexQueryContextPtr& context, const std::string& column_name,
        const Field& query_value, InvertedIndexQueryType query_type,
        std::shared_ptr<roaring::Roaring>& bit_map,
        InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
        const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    DORIS_CHECK(null_bitmap_cache_handle != nullptr);
    return _query(context, column_name, query_value, query_type, bit_map, null_bitmap_cache_handle,
                  analyzer_ctx);
}

Status SniiIndexReader::query_analyzed(const IndexQueryContextPtr& context,
                                       const std::string& column_name,
                                       InvertedIndexQueryType query_type,
                                       const InvertedIndexQueryInfo& query_info,
                                       std::shared_ptr<roaring::Roaring>& bit_map,
                                       InvertedIndexQueryCacheHandle* null_bitmap_cache_handle) {
    if (query_info.term_infos.empty()) {
        return Status::InvalidArgument("analyzed SNII query has no terms");
    }
    size_t longest_value_bytes = 0;
    for (const auto& term_info : query_info.term_infos) {
        if (!term_info.is_single_term()) {
            return Status::NotSupported("SNII does not run a multi-term slot in an analyzed query");
        }
        longest_value_bytes = std::max(longest_value_bytes, term_info.get_single_term().size());
    }
    const int32_t max_expansions = index_query::max_expansions(*context);
    const InvertedIndexAnalyzedQuerySemantic semantic {.term_infos = &query_info.term_infos,
                                                       .query_type = query_type,
                                                       .slop = query_info.slop,
                                                       .ordered = query_info.ordered,
                                                       .max_expansions = max_expansions};
    SniiQueryRequest request {
            .query_type = query_type,
            .cache_key = {.index_path = _index_file_reader->get_index_file_cache_key(&_index_meta),
                          .column_name = column_name,
                          .query_type = query_type,
                          .value = semantic.encode()},
            .max_expansions = max_expansions,
            .longest_value_bytes = longest_value_bytes,
            // A WILDCARD or REGEXP query carries its pattern as the one term.
            .search_str = query_info.term_infos.front().get_single_term(),
            .plan = [&query_info](InvertedIndexQueryType, std::string_view, NativeQuery* out) {
                out->query_info = query_info;
                return Status::OK();
            }};
    return _execute(context, column_name, request, bit_map, null_bitmap_cache_handle);
}

Status SniiIndexReader::_query(const IndexQueryContextPtr& context, const std::string& column_name,
                               const Field& query_value, InvertedIndexQueryType query_type,
                               std::shared_ptr<roaring::Roaring>& bit_map,
                               InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
                               const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    if (query_type == InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY &&
        _reader_type != InvertedIndexReaderType::FULLTEXT) {
        // A keyword index drops values longer than ignore_above, which a contains match may
        // still need, so rows answer MATCH_PHRASE_EDGE there.
        return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                "SNII keyword index does not run MATCH_PHRASE_EDGE; evaluating by function");
    }
    const std::string search_str = query_value.get<PrimitiveType::TYPE_STRING>();
    const int32_t max_expansions = index_query::max_expansions(*context);
    // Result cache keys contain only (index file, column, query type, raw query bytes). Analysis
    // is determined by index properties and policies, which are immutable once referenced, so
    // sharing can be decided before opening the segment.
    const InvertedIndexRawQuerySemantic raw_semantic {.raw_query_bytes = search_str,
                                                      .query_type = query_type,
                                                      .max_expansions = max_expansions};
    const std::map<std::string, std::string>& properties = _index_meta.properties();
    // Two captures keep the callback inside std::function's inline storage.
    const index_query::logical::AnalyzeValue analyze =
            [analyzer_ctx, &properties](std::string_view value, std::vector<TermInfo>* tokens) {
                return inverted_index::InvertedIndexAnalyzer::analyze(value, analyzer_ctx,
                                                                      properties, tokens);
            };
    SniiQueryRequest request {
            .query_type = query_type,
            .cache_key = {.index_path = _index_file_reader->get_index_file_cache_key(&_index_meta),
                          .column_name = column_name,
                          .query_type = query_type,
                          .value = raw_semantic.encode()},
            .max_expansions = max_expansions,
            .longest_value_bytes = search_str.size(),
            .search_str = search_str,
            // Analysis runs only for a query the cache does not answer. Two captures keep the
            // plan inside std::function's inline storage too.
            .plan = [&context, &analyze](InvertedIndexQueryType type, std::string_view value,
                                         NativeQuery* out) {
                SCOPED_RAW_TIMER(&context->stats->inverted_index_analyzer_timer);
                index_query::logical::Node node;
                RETURN_IF_ERROR(index_query::logical::lower_match(type, value, analyze, &node));
                // A value without tokens runs with no terms.
                return node.as<index_query::logical::Empty>() != nullptr
                               ? Status::OK()
                               : plan_native_query(std::move(node), out);
            }};
    return _execute(context, column_name, request, bit_map, null_bitmap_cache_handle);
}

// Keep the cache, count-only, candidate and single-flight decisions in one linear path.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status SniiIndexReader::_execute(const IndexQueryContextPtr& context,
                                 const std::string& column_name, const SniiQueryRequest& request,
                                 std::shared_ptr<roaring::Roaring>& bit_map,
                                 InvertedIndexQueryCacheHandle* null_bitmap_cache_handle) {
    const InvertedIndexQueryType query_type = request.query_type;
    const std::string_view search_str = request.search_str;
    const int32_t max_expansions = request.max_expansions;
    const bool track_requested_null_time = null_bitmap_cache_handle != nullptr;
    const int64_t query_ns_before =
            track_requested_null_time ? context->stats->inverted_index_query_timer : 0;
    int64_t requested_null_ns = 0;
    DEFER({
        if (!track_requested_null_time) {
            return;
        }
        const int64_t inclusive_query_ns =
                context->stats->inverted_index_query_timer - query_ns_before;
        DORIS_CHECK_GE(inclusive_query_ns, 0);
        const int64_t exclusive_query_ns =
                inclusive_query_ns > requested_null_ns ? inclusive_query_ns - requested_null_ns : 0;
        context->stats->inverted_index_query_timer = query_ns_before + exclusive_query_ns;
    });
    SCOPED_RAW_TIMER(&context->stats->inverted_index_query_timer);
    // Fresh per-search reply: only the query about to run decides whether its result is
    // candidate-restricted.
    context->candidate_rows_consumed = false;
    const auto finish_query =
            [&](const ::doris::snii::reader::LogicalIndexReader* reader) -> Status {
        if (null_bitmap_cache_handle == nullptr) {
            return Status::OK();
        }
        const int64_t null_ns_before = context->stats->inverted_index_query_null_bitmap_timer;
        Status status = _read_null_bitmap(context, null_bitmap_cache_handle, reader);
        const int64_t null_ns_after = context->stats->inverted_index_query_null_bitmap_timer;
        DORIS_CHECK_GE(null_ns_after, null_ns_before);
        requested_null_ns += null_ns_after - null_ns_before;
        return status;
    };

    if (int ignore_above =
                std::stoi(get_parser_ignore_above_value_from_properties(_index_meta.properties()));
        _reader_type == InvertedIndexReaderType::STRING_TYPE &&
        std::cmp_greater(request.longest_value_bytes, ignore_above)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                "query value is too long, evaluate skipped.");
    }

    // Scoring queries depend on collection statistics and use neither the result cache nor
    // single-flight coalescing.
    const bool actual_similarity =
            context->collection_similarity &&
            IndexReaderHelper::is_need_similarity_score(query_type, &_index_meta);
    const bool allow_result_cache = !actual_similarity;
    const InvertedIndexQueryCache::CacheKey& cache_key = request.cache_key;
    std::string single_flight_key = cache_key.encode();
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    if (handle_query_cache(context, cache, cache_key, &cache_handler, bit_map,
                           allow_result_cache)) {
        return finish_query(nullptr);
    }

    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(context->io_ctx);
    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::reader::LogicalIndexReader> uncached_reader;
    const ::doris::snii::reader::LogicalIndexReader* logical_reader = nullptr;
    RETURN_IF_ERROR(_get_logical_reader(context, &searcher_cache_handle, &uncached_reader,
                                        &logical_reader));

    NativeQuery planned {.query_type = query_type, .query_info = {}};
    RETURN_IF_ERROR(request.plan(query_type, search_str, &planned));
    // The request's type keys the cache and decides scoring; the planned type runs.
    const InvertedIndexQueryType execution_type = planned.query_type;
    const InvertedIndexQueryInfo& execution_query_info = planned.query_info;
    if (execution_query_info.term_infos.empty()) {
        auto msg = fmt::format("token parser result is empty for SNII query '{}'", search_str);
        if (is_match_query(query_type)) {
            LOG(WARNING) << msg;
            bit_map = std::make_shared<roaring::Roaring>();
            insert_query_cache(context, cache, cache_key, bit_map, &cache_handler,
                               allow_result_cache);
            return finish_query(logical_reader);
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(msg);
    }
    if (actual_similarity && execution_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY &&
        execution_query_info.term_infos.size() == 1) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII scoring does not support a single-token phrase-prefix query");
    }
    std::vector<std::string> terms = to_terms(execution_query_info);

    // G02 count-only fast path: the SegmentIterator asserted (via the context
    // flag) that only the match COUNT of this predicate matters, so eligible
    // shapes are answered from dict-entry df without decoding postings. Placed
    // AFTER the query-cache lookup (a cached row-accurate bitmap is free and
    // counts correctly) and BEFORE single-flight; the fabricated [0, df) bitmap
    // is returned early and NEVER inserted into the query cache or published to
    // single-flight followers -- both are keyed identically to row-accurate
    // queries and must only ever serve real row ids.
    if (context->count_on_index_fastpath) {
        bool count_handled = false;
        std::shared_ptr<roaring::Roaring> count_bitmap;
        RETURN_IF_ERROR(_try_count_only_fastpath(context, execution_type, execution_query_info,
                                                 terms, &count_handled, &count_bitmap,
                                                 logical_reader));
        if (count_handled) {
            bit_map = std::move(count_bitmap);
            RETURN_IF_ERROR(finish_query(logical_reader));
            // G03 reply: tell the SegmentIterator the bitmap is count-shaped
            // (cardinality exact, row ids fabricated) so it may short-circuit
            // row emission. Deliberately NOT set on the cache-hit return above
            // or on the decode path below -- those bitmaps are row-accurate
            // and keep today's emission.
            context->count_on_index_fastpath_hit = true;
            return Status::OK();
        }
    }

    // A multi-term phrase restricted to the scan candidates produces a partial bitmap. It stays
    // out of the result cache and single-flight, which both serve the full-segment query.
    const bool consume_candidates =
            context->candidate_rows != nullptr && consumes_candidates(execution_type, terms.size());
    context->candidate_rows_consumed = consume_candidates;
    const SniiQueryBitmapRequest bitmap_request {
            .query_type = execution_type,
            .query_info = execution_query_info,
            .search_str = search_str,
            .max_expansions = max_expansions,
            .logical_reader = logical_reader,
            .candidates = consume_candidates ? context->candidate_rows : nullptr};

    // Under a cold cache, parallel scanners _lazy_init the same segment concurrently and each
    // would otherwise miss the searcher/query caches and redundantly open + decode this segment's
    // index. Collapse identical concurrent queries into one shared execution (see SingleFlight).
    static ::doris::segment_v2::inverted_index::SingleFlight<
            std::pair<Status, std::shared_ptr<roaring::Roaring>>>
            query_single_flight;
    std::shared_ptr<roaring::Roaring> result_bitmap;
    std::vector<::doris::snii::query::PhraseMatch> phrase_matches;
    auto* phrase_matches_out =
            actual_similarity && uses_phrase_frequency_scoring(execution_type, execution_query_info)
                    ? &phrase_matches
                    : nullptr;
    Status single_flight_status;
    if (!allow_result_cache || consume_candidates) {
        single_flight_status = _compute_query_bitmap(context, bitmap_request, &terms,
                                                     &result_bitmap, phrase_matches_out);
    } else {
        DORIS_CHECK(phrase_matches_out == nullptr);
        single_flight_status = run_query_single_flight(
                query_single_flight, single_flight_key, &result_bitmap,
#ifdef BE_TEST
                _single_flight_follower_joined_observer, _single_flight_follower_joined_opaque,
                _single_flight_leader_before_compute_observer,
                _single_flight_leader_before_compute_opaque,
#endif
                [&](std::shared_ptr<roaring::Roaring>* out) {
                    auto status =
                            _compute_query_bitmap(context, bitmap_request, &terms, out, nullptr);
                    if (status.ok()) {
                        insert_query_cache(context, cache, cache_key, *out, &cache_handler,
                                           allow_result_cache);
                    }
                    return status;
                });
    }
    RETURN_IF_ERROR(single_flight_status);
    DORIS_CHECK(result_bitmap != nullptr);
    if (actual_similarity && !result_bitmap->isEmpty()) {
        ::doris::snii::stats::SniiStatsProvider segment_stats;
        RETURN_IF_ERROR(
                ::doris::snii::stats::SniiStatsProvider::open(logical_reader, &segment_stats));
        if (phrase_matches_out != nullptr) {
            RETURN_IF_ERROR(score_phrase_matches(context, column_name, execution_type,
                                                 execution_query_info, *logical_reader,
                                                 segment_stats, *result_bitmap, phrase_matches));
        } else if (uses_plain_term_frequency_scoring(execution_type, execution_query_info)) {
            RETURN_IF_ERROR(score_plain_term_candidates(context, column_name, execution_query_info,
                                                        *logical_reader, segment_stats,
                                                        *result_bitmap));
        }
    }
    bit_map = result_bitmap;
    return finish_query(logical_reader);
}

Status SniiIndexReader::_compute_query_bitmap(
        const IndexQueryContextPtr& context, const SniiQueryBitmapRequest& request,
        std::vector<std::string>* preanalyzed_terms, std::shared_ptr<roaring::Roaring>* out,
        std::vector<::doris::snii::query::PhraseMatch>* phrase_matches) {
    // Bound once so the body below reads the same as before the request object was introduced;
    // renaming 71 uses would have buried the actual change.
    const InvertedIndexQueryType query_type = request.query_type;
    const InvertedIndexQueryInfo& request_query_info = request.query_info;
    const std::string_view search_str = request.search_str;
    const int32_t max_expansions = request.max_expansions;
    const ::doris::snii::reader::LogicalIndexReader* logical_reader = request.logical_reader;

    DORIS_CHECK(preanalyzed_terms != nullptr);
    DORIS_CHECK(logical_reader != nullptr);
    DORIS_CHECK(request_query_info.term_infos.size() == preanalyzed_terms->size());
    if (phrase_matches != nullptr) {
        phrase_matches->clear();
    }
    InvertedIndexQueryInfo query_info = request_query_info;
    std::vector<std::string> routed_terms = *preanalyzed_terms;
    auto* terms = &routed_terms;
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY:
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY: {
        RETURN_IF_ERROR(
                ::doris::snii::query::internal::check_query_terms_outside_internal_namespace(
                        query_info));
        if (terms->empty() && (query_type == InvertedIndexQueryType::EQUAL_QUERY ||
                               query_type == InvertedIndexQueryType::MATCH_ANY_QUERY)) {
            *out = std::make_shared<roaring::Roaring>();
            return Status::OK();
        }
        break;
    }
    default:
        break;
    }
    SniiQueryExecutionResult query_result;
    const bool phrase_can_decode_prx = query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY;
    const bool needs_prx_profile =
            terms->size() > 1 && (phrase_can_decode_prx ||
                                  query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY ||
                                  query_type == InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY);
    if (needs_prx_profile) {
        ::doris::snii::SniiPrxExecutionProfileScope execution_profile(*context->stats);
        const Status execution_status =
                execute_snii_query(*logical_reader, query_type, query_info, search_str, *terms,
                                   max_expansions, phrase_matches != nullptr, &query_result,
                                   execution_profile.profile(), request.candidates);
        RETURN_IF_ERROR(execution_status);
    } else {
        RETURN_IF_ERROR(execute_snii_query(*logical_reader, query_type, query_info, search_str,
                                           *terms, max_expansions, phrase_matches != nullptr,
                                           &query_result, nullptr, request.candidates));
    }
    *out = std::move(query_result.bitmap);
    if (phrase_matches != nullptr) {
        *phrase_matches = std::move(query_result.phrase_matches);
    }
    return Status::OK();
}

#ifdef BE_TEST
Status SniiIndexReader::_compute_query_bitmap(const IndexQueryContextPtr& context,
                                              InvertedIndexQueryType query_type,
                                              const InvertedIndexQueryInfo& query_info,
                                              std::string_view search_str,
                                              std::vector<std::string>* terms,
                                              int32_t max_expansions,
                                              std::shared_ptr<roaring::Roaring>* out) {
    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(context->io_ctx);
    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::reader::LogicalIndexReader> uncached_reader;
    const ::doris::snii::reader::LogicalIndexReader* logical_reader = nullptr;
    RETURN_IF_ERROR(_get_logical_reader(context, &searcher_cache_handle, &uncached_reader,
                                        &logical_reader));
    return _compute_query_bitmap(context,
                                 {.query_type = query_type,
                                  .query_info = query_info,
                                  .search_str = search_str,
                                  .max_expansions = max_expansions,
                                  .logical_reader = logical_reader},
                                 terms, out, nullptr);
}
#endif

// Keep the complete count-only eligibility and null-safe fabrication contract in one linear path.
// NOLINTNEXTLINE(readability-function-size)
Status SniiIndexReader::_try_count_only_fastpath(
        const IndexQueryContextPtr& context, InvertedIndexQueryType query_type,
        const InvertedIndexQueryInfo& query_info, const std::vector<std::string>& terms,
        bool* handled, std::shared_ptr<roaring::Roaring>* out,
        const ::doris::snii::reader::LogicalIndexReader* preopened_reader) {
    *handled = false;
    // Shape guard: only exact-term query types. Prefix/regexp/wildcard/
    // phrase-prefix expand the term set, so no single dict entry carries the
    // count; range types never reach SNII anyway.
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY:
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
        break;
    default:
        return Status::OK();
    }
    if (terms.size() != 1) {
        // Multi-term MATCH_ANY (OR) / MATCH_ALL (AND) counts are not derivable
        // from per-term dfs (overlap unknown), and phrases require positional
        // verification, so execute the normal query path.
        return Status::OK();
    }

    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(context->io_ctx);
    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::reader::LogicalIndexReader> uncached_reader;
    const ::doris::snii::reader::LogicalIndexReader* logical_reader = preopened_reader;
    if (logical_reader == nullptr) {
        RETURN_IF_ERROR(_get_logical_reader(context, &searcher_cache_handle, &uncached_reader,
                                            &logical_reader));
    }

    // ARRAY columns: df is NOT null-free, so nothing below may fabricate from it.
    // ArrayColumnWriter::append_nullable hands add_array_values() every row of the
    // batch -- the offsets come from the nested ColumnArray, and
    // OlapColumnDataConvertorArray::convert_to_olap reads them without ever
    // consulting the outer null map -- and the add_array_nulls() that follows only
    // RECORDS the null row ids, it never retracts the tokens already emitted for
    // them. A nullable array whose nested payload survives under the null map does
    // occur: PreparedFunctionImpl::default_implementation_for_nulls documents that
    // nested columns keep "arbitrary values in rows corresponding to NULL value",
    // and need_replace_null_data_to_default() is false by default, so e.g.
    // array_concat(arr, nullable_arr) writes arr's tokens on a NULL row. That row
    // then sits in a posting and is counted by df. The decode path stays correct --
    // mask_out_null subtracts it -- but the fabrication below deliberately places
    // its ids OFF the null rows, so the same subtraction removes nothing and the
    // count comes out too high. CLucene writes arrays identically
    // (InvertedIndexColumnWriter::add_array_nulls only touches _null_bitmap), so
    // this cannot be repaired from the reader side; decline whenever this segment
    // has a null bitmap at all. Scalars are unaffected: ScalarColumnWriter::
    // append_nullable splits the batch into runs and sends null runs to
    // append_nulls(), which emits no tokens.
    if (_column_is_array && logical_reader->section_refs().null_bitmap.length > 0) {
        return Status::OK();
    }

    DORIS_CHECK(query_info.term_infos.size() == 1);
    const std::string& term = query_info.term_infos.front().get_single_term();
    RETURN_IF_ERROR(::doris::snii::query::internal::check_term_outside_internal_namespace(term));
    uint64_t count = 0;
    RETURN_IF_ERROR(::doris::snii::query::count_only_term_df(*logical_reader, term, &count));
    // df bounds the fabricated bitmap, so it has to be inside a document domain
    // that is itself real. Two steps, because they fail differently.
    const auto& stats = logical_reader->stats();
    if (count > stats.doc_count || count > stats.indexed_doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "SNII count fast path: term df {} exceeds document domain (doc count {}, "
                "indexed doc count {})",
                count, stats.doc_count, stats.indexed_doc_count);
    }
    // Both limits above are CRC-valid fields of the SAME image as df, so an image
    // whose stats were inflated together with df clears them: on a real 10-row
    // segment, df = doc_count = indexed_doc_count = 100 fabricates 100 ids, and
    // SegmentIterator -- which seeds _row_bitmap with [0, num_rows) and intersects
    // -- silently reports 10. The segment's own row count is the one bound the
    // image cannot move. One-sided on purpose: an index covering FEWER rows than
    // the segment still fabricates ids inside [0, num_rows), so only an oversized
    // domain is corruption. This mirrors the equality SniiSegmentReader::
    // load_inherited_index already demands of a rewrite.
    if (stats.doc_count > _rows_of_segment) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "SNII count fast path: index doc count {} exceeds the segment row count {}",
                stats.doc_count, _rows_of_segment);
    }

    // Null handling. df is the exact match count REGARDLESS of nulls: the writer
    // adds no tokens for a null doc (scalar add_nulls; ARRAY columns cannot reach
    // this point on a segment with nulls, see the guard above), so postings -- and
    // therefore df -- never include null rows, exactly matching MATCH's "null never
    // matches" semantics. The
    // fabricated bitmap however flows through FunctionMatchBase ->
    // InvertedIndexResultBitmap::mask_out_null, which subtracts the segment's
    // REAL null bitmap from it; a dense [0, df) range colliding with null row
    // ids would be shrunk below df. So on a segment WITH a null bitmap, load
    // it (query-cache backed, the same read the normal MATCH path performs)
    // and fabricate df ids DISJOINT from it, making that subtraction a
    // provable no-op. Segments without a null section (the writer omits it
    // when no row is null) keep the trivial [0, df) range.
    auto result = std::make_shared<roaring::Roaring>();
    if (count > 0 && logical_reader->section_refs().null_bitmap.length > 0) {
        InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        RETURN_IF_ERROR(_read_null_bitmap(context, &null_bitmap_cache_handle, logical_reader));
        std::shared_ptr<roaring::Roaring> nulls = null_bitmap_cache_handle.get_bitmap();
        // Fall through on a missing bitmap behind the cache handle or a
        // fabrication failure (df + null count breaching the docid domain):
        // both mean a corrupt index, and the row-accurate decode -- which
        // intersects real ids -- must own the answer rather than a blind
        // fabrication. The count_fastpath_hits test seam already counted the
        // dict lookup above; production correctness is unaffected.
        if (nulls == nullptr) {
            return Status::OK();
        }
        if (!nulls->isEmpty()) {
            if (!::doris::snii::query::fabricate_null_disjoint_count_bitmap(count, *nulls,
                                                                            result.get())
                         .ok()) {
                return Status::OK();
            }
        } else {
            result->addRange(0, count);
        }
    } else if (count > 0) {
        result->addRange(0, count);
    }
    *out = std::move(result);
    *handled = true;
    return Status::OK();
}

Status SniiIndexReader::try_query(const IndexQueryContextPtr& /*context*/,
                                  const std::string& /*column_name*/, const Field& /*query_value*/,
                                  InvertedIndexQueryType /*query_type*/, size_t* /*count*/) {
    return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>("SNII does not support try_query");
}

Status SniiIndexReader::read_null_bitmap(const IndexQueryContextPtr& context,
                                         InvertedIndexQueryCacheHandle* cache_handle,
                                         lucene::store::Directory* /*dir*/) {
    return _read_null_bitmap(context, cache_handle, nullptr);
}

Status SniiIndexReader::_read_null_bitmap(
        const IndexQueryContextPtr& context, InvertedIndexQueryCacheHandle* cache_handle,
        const ::doris::snii::reader::LogicalIndexReader* preopened_reader) {
    SCOPED_RAW_TIMER(&context->stats->inverted_index_query_null_bitmap_timer);
    auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
    InvertedIndexQueryCache::CacheKey cache_key {
            index_file_key, "", InvertedIndexQueryType::UNKNOWN_QUERY, "null_bitmap"};
    auto* cache = InvertedIndexQueryCache::instance();
    if (cache->lookup(cache_key, cache_handle)) {
        return Status::OK();
    }

    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(context->io_ctx);
    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::reader::LogicalIndexReader> uncached_reader;
    const ::doris::snii::reader::LogicalIndexReader* logical_reader = preopened_reader;
    if (logical_reader == nullptr) {
        RETURN_IF_ERROR(_get_logical_reader(context, &searcher_cache_handle, &uncached_reader,
                                            &logical_reader));
    }
    auto null_bitmap = std::make_shared<roaring::Roaring>();
    const auto& ref = logical_reader->section_refs().null_bitmap;
    if (ref.length > 0) {
        std::vector<uint8_t> bytes;
        RETURN_IF_ERROR(logical_reader->reader()->read_at(ref.offset, ref.length, &bytes));
        ::doris::snii::format::NullBitmapReader reader;
        RETURN_IF_ERROR(::doris::snii::format::NullBitmapReader::open(::doris::snii::Slice(bytes),
                                                                      &reader));
        reader.copy_to(null_bitmap.get());
        null_bitmap->runOptimize();
    }
    cache->insert(cache_key, null_bitmap, cache_handle);
    return Status::OK();
}

} // namespace doris::segment_v2
