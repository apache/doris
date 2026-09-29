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
#include "storage/index/snii/reader/snii_index_source.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/index/snii/snii_prx_profile.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
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
    out->query_type = logical::leaf_query_type(leaf);
    if (auto* term = std::get_if<logical::Term>(&leaf.value)) {
        term_infos.emplace_back(std::move(term->term));
    } else if (auto* set = std::get_if<logical::TermSet>(&leaf.value)) {
        // The executors know all-of and any-of; the SEARCH compile step counts a
        // threshold above them.
        DORIS_CHECK(set->min_should_match == 0);
        term_infos.reserve(set->terms.size());
        for (auto& value : set->terms) {
            term_infos.emplace_back(std::move(value));
        }
    } else if (auto* phrase = std::get_if<logical::Phrase>(&leaf.value)) {
        term_infos = std::move(phrase->slots);
        out->query_info.slop = phrase->slop;
        out->query_info.ordered = phrase->ordered;
    } else if (auto* expand = std::get_if<logical::Expand>(&leaf.value)) {
        term_infos.emplace_back(std::move(expand->pattern));
    } else {
        return Status::InternalError("leaf kind {} cannot run on SNII", leaf.value.index());
    }
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

namespace {

struct SniiOpenedIndex : OpenedIndex {
    explicit SniiOpenedIndex(const io::IOContext* io_ctx) : io_scope(io_ctx) {}

    snii_doris::DorisSniiFileReader::ScopedIOContext io_scope;
    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::reader::LogicalIndexReader> uncached_reader;
    const ::doris::snii::reader::LogicalIndexReader* reader = nullptr;
};

} // namespace

Status SniiIndexReader::_open_index(const IndexQueryContextPtr& context,
                                    std::unique_ptr<OpenedIndex>* out) {
    auto opened = std::make_unique<SniiOpenedIndex>(context->io_ctx);
    RETURN_IF_ERROR(_get_logical_reader(context, &opened->searcher_cache_handle,
                                        &opened->uncached_reader, &opened->reader));
    *out = std::move(opened);
    return Status::OK();
}

index_query::IndexSourcePtr SniiIndexReader::_bind_source(const IndexQueryContextPtr& /*context*/,
                                                          const std::wstring& /*field*/,
                                                          OpenedIndex& index) {
    return std::make_shared<::doris::snii::reader::SniiIndexSource>(
            *static_cast<SniiOpenedIndex&>(index).reader);
}

Status SniiIndexReader::_term_document_frequency(const std::string& /*column_name*/,
                                                 OpenedIndex& index, const std::string& term,
                                                 uint64_t* df, uint64_t* document_count) {
    const auto& reader = *static_cast<SniiOpenedIndex&>(index).reader;
    RETURN_IF_ERROR(::doris::snii::query::internal::check_term_outside_internal_namespace(term));
    RETURN_IF_ERROR(::doris::snii::query::count_only_term_df(reader, term, df));
    // The image's document count and its count of documents holding terms both bound df.
    const auto& stats = reader.stats();
    *document_count = std::min(stats.doc_count, stats.indexed_doc_count);
    return Status::OK();
}

Status SniiIndexReader::_run_leaf(const IndexQueryContextPtr& context,
                                  const std::string& column_name, OpenedIndex& index,
                                  const index_query::logical::Node& leaf,
                                  const roaring::Roaring* candidates, bool scoring,
                                  std::shared_ptr<roaring::Roaring>* out) {
    const auto* logical_reader = static_cast<SniiOpenedIndex&>(index).reader;
    NativeQuery planned;
    RETURN_IF_ERROR(plan_native_query(index_query::logical::Node(leaf), &planned));
    const InvertedIndexQueryType query_type = planned.query_type;
    const InvertedIndexQueryInfo& query_info = planned.query_info;
    for (const auto& term_info : query_info.term_infos) {
        if (!term_info.is_single_term()) {
            return Status::NotSupported("SNII does not run a multi-term slot");
        }
    }
    if (scoring && query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY &&
        query_info.term_infos.size() == 1) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII scoring does not support a single-token phrase-prefix query");
    }
    std::vector<std::string> terms = to_terms(query_info);
    DORIS_CHECK(!terms.empty());
    const SniiQueryBitmapRequest bitmap_request {
            .query_type = query_type,
            .query_info = query_info,
            // A WILDCARD or REGEXP query carries its pattern as the one term.
            .search_str = terms.front(),
            .max_expansions = index_query::max_expansions(*context),
            .logical_reader = logical_reader,
            .candidates = candidates};
    std::vector<::doris::snii::query::PhraseMatch> phrase_matches;
    auto* phrase_matches_out = scoring && uses_phrase_frequency_scoring(query_type, query_info)
                                       ? &phrase_matches
                                       : nullptr;
    std::shared_ptr<roaring::Roaring> result;
    RETURN_IF_ERROR(
            _compute_query_bitmap(context, bitmap_request, &terms, &result, phrase_matches_out));
    if (scoring && !result->isEmpty()) {
        ::doris::snii::stats::SniiStatsProvider segment_stats;
        RETURN_IF_ERROR(
                ::doris::snii::stats::SniiStatsProvider::open(logical_reader, &segment_stats));
        if (phrase_matches_out != nullptr) {
            RETURN_IF_ERROR(score_phrase_matches(context, column_name, query_type, query_info,
                                                 *logical_reader, segment_stats, *result,
                                                 phrase_matches));
        } else if (uses_plain_term_frequency_scoring(query_type, query_info)) {
            RETURN_IF_ERROR(score_plain_term_candidates(context, column_name, query_info,
                                                        *logical_reader, segment_stats, *result));
        }
    }
    *out = std::move(result);
    return Status::OK();
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
#ifdef BE_TEST
Status SniiIndexReader::_try_count_only_fastpath(
        const IndexQueryContextPtr& context, InvertedIndexQueryType /*query_type*/,
        const InvertedIndexQueryInfo& /*query_info*/, const std::vector<std::string>& terms,
        bool* handled, std::shared_ptr<roaring::Roaring>* out,
        const ::doris::snii::reader::LogicalIndexReader* preopened_reader) {
    *handled = false;
    if (terms.size() != 1) {
        return Status::OK();
    }
    std::unique_ptr<OpenedIndex> index;
    if (preopened_reader != nullptr) {
        auto opened = std::make_unique<SniiOpenedIndex>(context->io_ctx);
        opened->reader = preopened_reader;
        index = std::move(opened);
    } else {
        RETURN_IF_ERROR(_open_index(context, &index));
    }
    return _count_from_df(context, "", *index, terms.front(), handled, out);
}
#endif

Status SniiIndexReader::_read_null_bitmap(const IndexQueryContextPtr& context,
                                          InvertedIndexQueryCacheHandle* cache_handle,
                                          OpenedIndex* index) {
    return _read_snii_null_bitmap(
            context, cache_handle,
            index == nullptr ? nullptr : static_cast<SniiOpenedIndex*>(index)->reader);
}

Status SniiIndexReader::read_null_bitmap(const IndexQueryContextPtr& context,
                                         InvertedIndexQueryCacheHandle* cache_handle,
                                         lucene::store::Directory* /*dir*/) {
    return _read_snii_null_bitmap(context, cache_handle, nullptr);
}

Status SniiIndexReader::_read_snii_null_bitmap(
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
