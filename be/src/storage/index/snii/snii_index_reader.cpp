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
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/snii/common/single_flight.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/count_query.h"
#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/regexp_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/query/wildcard_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris::segment_v2 {

namespace {

class RoaringDocIdSink final : public ::doris::snii::query::DocIdSink {
public:
    explicit RoaringDocIdSink(roaring::Roaring* bitmap) : _bitmap(bitmap) {
        DCHECK(_bitmap != nullptr);
    }

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

    // Roaring addMany/addRange deduplicate and order natively, so multi-term OR
    // can stream each posting straight into the bitmap (no per-term vector + merge).
    bool dedups() const override { return true; }

private:
    roaring::Roaring* _bitmap;
};

struct SniiQueryExecutionResult {
    std::shared_ptr<roaring::Roaring> bitmap;
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

void parse_phrase_slop(std::string* query, InvertedIndexQueryInfo* query_info) {
    DCHECK(query != nullptr);
    DCHECK(query_info != nullptr);
    const auto is_digits = [](std::string_view str) {
        return std::all_of(str.begin(), str.end(), [](unsigned char c) { return std::isdigit(c); });
    };

    const size_t last_space_pos = query->find_last_of(' ');
    if (last_space_pos == std::string::npos) {
        return;
    }
    const size_t tilde_pos = last_space_pos + 1;
    if (tilde_pos >= query->size() - 1 || (*query)[tilde_pos] != '~') {
        return;
    }

    const size_t slop_pos = tilde_pos + 1;
    std::string_view slop_str(query->data() + slop_pos, query->size() - slop_pos);
    if (slop_str.empty()) {
        return;
    }

    bool ordered = false;
    if (slop_str.size() == 1) {
        if (!std::isdigit(static_cast<unsigned char>(slop_str[0]))) {
            return;
        }
    } else if (slop_str.back() == '+') {
        ordered = true;
        slop_str.remove_suffix(1);
    }

    if (!is_digits(slop_str)) {
        return;
    }
    auto result = std::from_chars(slop_str.begin(), slop_str.end(), query_info->slop);
    if (result.ec != std::errc()) {
        return;
    }
    query_info->ordered = ordered;
    *query = query->substr(0, last_space_pos);
}

std::string build_snii_query_cache_value(const InvertedIndexQueryInfo& query_info) {
    std::string cache_value;
    for (const auto& term_info : query_info.term_infos) {
        DCHECK(term_info.is_single_term());
        const auto& term = term_info.get_single_term();
        cache_value.append(std::to_string(term.size()));
        cache_value.push_back(':');
        cache_value.append(term);
        cache_value.push_back('@');
        cache_value.append(std::to_string(term_info.position));
        cache_value.push_back(';');
    }
    return cache_value;
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
        ::doris::snii::SingleFlight<std::pair<Status, std::shared_ptr<roaring::Roaring>>>& flight,
        const std::string& key, std::shared_ptr<roaring::Roaring>* result, Compute&& compute) {
    auto follower = flight.join_or_lead(key);
    if (follower.has_value()) {
        auto [leader_status, leader_bitmap] = follower->get();
        if (leader_status.ok() && leader_bitmap != nullptr) {
            *result = std::move(leader_bitmap);
            return Status::OK();
        }
        // Leader failed; fall through and compute independently (rare error path).
    }
    const bool is_leader = !follower.has_value();

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

Status execute_snii_query(const ::doris::snii::reader::LogicalIndexReader& logical_reader,
                          InvertedIndexQueryType query_type,
                          const InvertedIndexQueryInfo& query_info, std::string_view search_str,
                          const std::vector<std::string>& terms, int32_t max_expansions,
                          SniiQueryExecutionResult* result) {
    result->bitmap = std::make_shared<roaring::Roaring>();
    RoaringDocIdSink sink(result->bitmap.get());
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
        if (query_info.slop != 0) {
            return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "SNII does not support sloppy phrase query yet");
        }
        if (terms.size() == 1) {
            status = ::doris::snii::query::term_query(logical_reader, terms.front(), &sink);
            emitted_to_sink = true;
        } else {
            status = ::doris::snii::query::phrase_query(logical_reader, terms, &docids);
        }
        break;
    case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
        if (terms.size() == 1) {
            status = ::doris::snii::query::prefix_query(logical_reader, terms.front(), &sink,
                                                        max_expansions);
            emitted_to_sink = true;
        } else {
            status = ::doris::snii::query::phrase_prefix_query(logical_reader, terms, &docids,
                                                               max_expansions);
        }
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
    case InvertedIndexQueryType::LESS_THAN_QUERY:
    case InvertedIndexQueryType::LESS_EQUAL_QUERY:
    case InvertedIndexQueryType::GREATER_THAN_QUERY:
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY:
    case InvertedIndexQueryType::RANGE_QUERY:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII inverted index storage format does not support BKD/range query");
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII unsupported inverted index query type {}", query_type_to_string(query_type));
    }
    RETURN_IF_ERROR(status);
    if (emitted_to_sink) {
        result->bitmap->runOptimize();
    } else {
        result->bitmap = docids_to_bitmap(docids);
    }
    return Status::OK();
}

} // namespace

Status SniiIndexReader::new_iterator(std::unique_ptr<IndexIterator>* iterator) {
    if (*iterator == nullptr) {
        *iterator = InvertedIndexIterator::create_unique();
    }
    dynamic_cast<InvertedIndexIterator*>(iterator->get())
            ->add_reader(_reader_type,
                         dynamic_pointer_cast<InvertedIndexReader>(shared_from_this()));
    return Status::OK();
}

Status SniiIndexReader::_parse_query_terms(const IndexQueryContextPtr& context,
                                           std::string search_str,
                                           InvertedIndexQueryType query_type,
                                           const InvertedIndexAnalyzerCtx* analyzer_ctx,
                                           InvertedIndexQueryInfo* query_info) {
    DCHECK(query_info != nullptr);
    if (query_type == InvertedIndexQueryType::MATCH_REGEXP_QUERY ||
        query_type == InvertedIndexQueryType::WILDCARD_QUERY) {
        query_info->term_infos.emplace_back(search_str, 0);
        return Status::OK();
    }
    if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY ||
        query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY) {
        // parse_phrase_slop MUST run first: it strips a trailing ~N slop suffix off
        // search_str and records it in query_info before tokenization.
        parse_phrase_slop(&search_str, query_info);
        SCOPED_RAW_TIMER(&context->stats->inverted_index_analyzer_timer);
        try {
            // Mirror the non-phrase branch below: reuse the per-query-expr
            // analyzer_ctx (built once and shared across segments) instead of
            // rebuilding a CLucene analyzer per segment from properties. Falls back
            // to the properties path when analyzer_ctx is absent (internal callers).
            if (analyzer_ctx != nullptr && !analyzer_ctx->should_tokenize()) {
                query_info->term_infos.emplace_back(search_str);
            } else if (analyzer_ctx != nullptr && analyzer_ctx->analyzer != nullptr) {
                auto reader = inverted_index::InvertedIndexAnalyzer::create_reader(
                        analyzer_ctx->char_filter_map);
                reader->init(search_str.data(), static_cast<int32_t>(search_str.size()), true);
                query_info->term_infos = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                        reader, analyzer_ctx->analyzer.get());
            } else {
                query_info->term_infos = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                        search_str, _index_meta.properties());
            }
        } catch (const CLuceneError& e) {
            return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "SNII analyze query failed: {}", e.what());
        } catch (const Exception& e) {
            return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "SNII analyze query failed: {}", e.what());
        }
        return Status::OK();
    }

    SCOPED_RAW_TIMER(&context->stats->inverted_index_analyzer_timer);
    try {
        if (analyzer_ctx != nullptr && !analyzer_ctx->should_tokenize()) {
            query_info->term_infos.emplace_back(search_str);
        } else if (analyzer_ctx != nullptr && analyzer_ctx->analyzer != nullptr) {
            auto reader = inverted_index::InvertedIndexAnalyzer::create_reader(
                    analyzer_ctx->char_filter_map);
            reader->init(search_str.data(), static_cast<int32_t>(search_str.size()), true);
            query_info->term_infos = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                    reader, analyzer_ctx->analyzer.get());
        } else {
            query_info->term_infos = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                    search_str, _index_meta.properties());
        }
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "SNII analyze query failed: {}", e.what());
    } catch (const Exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "SNII analyze query failed: {}", e.what());
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
    SCOPED_RAW_TIMER(&context->stats->inverted_index_query_timer);
    std::string search_str = query_value.get<PrimitiveType::TYPE_STRING>();

    if (int ignore_above =
                std::stoi(get_parser_ignore_above_value_from_properties(_index_meta.properties()));
        _reader_type == InvertedIndexReaderType::STRING_TYPE && search_str.size() > ignore_above) {
        return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                "query value is too long, evaluate skipped.");
    }

    InvertedIndexQueryInfo query_info;
    RETURN_IF_ERROR(_parse_query_terms(context, search_str, query_type, analyzer_ctx, &query_info));
    if (query_info.term_infos.empty()) {
        auto msg = fmt::format("token parser result is empty for SNII query '{}'", search_str);
        if (is_match_query(query_type)) {
            LOG(WARNING) << msg;
            bit_map = std::make_shared<roaring::Roaring>();
            return Status::OK();
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(msg);
    }

    auto terms = to_terms(query_info);
    const int32_t max_expansions =
            context->runtime_state == nullptr
                    ? 50
                    : context->runtime_state->query_options().inverted_index_max_expansions;
    std::string cache_value = build_snii_query_cache_value(query_info);
    if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY) {
        cache_value += " " + std::to_string(query_info.slop);
        cache_value += " " + std::to_string(query_info.ordered);
    } else if (query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY ||
               query_type == InvertedIndexQueryType::MATCH_REGEXP_QUERY ||
               query_type == InvertedIndexQueryType::WILDCARD_QUERY) {
        cache_value += " " + std::to_string(max_expansions);
    }
    auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
    // Single-flight key: identifies this exact (segment file, column, query type, terms) so
    // concurrent identical queries share one execution. Built before cache_value is moved
    // into cache_key below; mirrors the query-cache key components.
    std::string single_flight_key = index_file_key;
    single_flight_key.push_back('\x01');
    single_flight_key.append(column_name);
    single_flight_key.push_back('\x01');
    single_flight_key.append(std::to_string(static_cast<int>(query_type)));
    single_flight_key.push_back('\x01');
    single_flight_key.append(cache_value);

    InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                 std::move(cache_value)};
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    if (handle_query_cache(context, cache, cache_key, &cache_handler, bit_map)) {
        return Status::OK();
    }

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
        RETURN_IF_ERROR(_try_count_only_fastpath(context, query_type, query_info, terms,
                                                 &count_handled, &count_bitmap));
        if (count_handled) {
            bit_map = std::move(count_bitmap);
            return Status::OK();
        }
    }

    // Under a cold cache, parallel scanners _lazy_init the same segment concurrently and each
    // would otherwise miss the searcher/query caches and redundantly open + decode this segment's
    // index. Collapse identical concurrent queries into one shared execution (see SingleFlight).
    static ::doris::snii::SingleFlight<std::pair<Status, std::shared_ptr<roaring::Roaring>>>
            query_single_flight;
    std::shared_ptr<roaring::Roaring> result_bitmap;
    RETURN_IF_ERROR(run_query_single_flight(query_single_flight, single_flight_key, &result_bitmap,
                                            [&](std::shared_ptr<roaring::Roaring>* out) {
                                                return _compute_query_bitmap(
                                                        context, query_type, query_info, search_str,
                                                        terms, max_expansions, out);
                                            }));
    bit_map = result_bitmap;
    cache->insert(cache_key, bit_map, &cache_handler);
    return Status::OK();
}

Status SniiIndexReader::_compute_query_bitmap(const IndexQueryContextPtr& context,
                                              InvertedIndexQueryType query_type,
                                              const InvertedIndexQueryInfo& query_info,
                                              std::string_view search_str,
                                              const std::vector<std::string>& terms,
                                              int32_t max_expansions,
                                              std::shared_ptr<roaring::Roaring>* out) {
    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(context->io_ctx);
    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::reader::LogicalIndexReader> uncached_reader;
    const ::doris::snii::reader::LogicalIndexReader* logical_reader = nullptr;
    RETURN_IF_ERROR(_get_logical_reader(context, &searcher_cache_handle, &uncached_reader,
                                        &logical_reader));
    SniiQueryExecutionResult query_result;
    RETURN_IF_ERROR(execute_snii_query(*logical_reader, query_type, query_info, search_str, terms,
                                       max_expansions, &query_result));
    *out = std::move(query_result.bitmap);
    return Status::OK();
}

Status SniiIndexReader::_try_count_only_fastpath(const IndexQueryContextPtr& context,
                                                 InvertedIndexQueryType query_type,
                                                 const InvertedIndexQueryInfo& query_info,
                                                 const std::vector<std::string>& terms,
                                                 bool* handled,
                                                 std::shared_ptr<roaring::Roaring>* out) {
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
    // The normal path rejects sloppy phrases with a BYPASS (downgrade to raw
    // evaluation); the count path must fall through so the downgrade happens.
    if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY && query_info.slop != 0) {
        return Status::OK();
    }
    const bool single_term = terms.size() == 1;
    const bool two_term_phrase =
            terms.size() == 2 && query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY;
    if (!single_term && !two_term_phrase) {
        // Multi-term MATCH_ANY (OR) / MATCH_ALL (AND) counts are not derivable
        // from per-term dfs (overlap unknown) -> normal decode path.
        return Status::OK();
    }

    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(context->io_ctx);
    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::reader::LogicalIndexReader> uncached_reader;
    const ::doris::snii::reader::LogicalIndexReader* logical_reader = nullptr;
    RETURN_IF_ERROR(_get_logical_reader(context, &searcher_cache_handle, &uncached_reader,
                                        &logical_reader));

    // Null guard. The fabricated bitmap is a dense [0, df) id range; the MATCH
    // machinery unconditionally subtracts the segment's null bitmap from the
    // result (FunctionMatchBase -> mask_out_null). Real postings never contain
    // null rows so that subtraction is a no-op on the true bitmap, but it WOULD
    // remove fabricated ids that happen to collide with null rows. Only proceed
    // when the segment column has no null bitmap at all.
    if (logical_reader->section_refs().null_bitmap.length > 0) {
        return Status::OK();
    }

    uint64_t count = 0;
    if (single_term) {
        RETURN_IF_ERROR(
                ::doris::snii::query::count_only_term_df(*logical_reader, terms.front(), &count));
    } else {
        bool bigram_handled = false;
        RETURN_IF_ERROR(::doris::snii::query::count_only_two_term_phrase_bigram_df(
                *logical_reader, terms[0], terms[1], &bigram_handled, &count));
        if (!bigram_handled) {
            // Pruned or absent bigram (or positionless index): the generic
            // phrase path owns those semantics.
            return Status::OK();
        }
    }

    auto result = std::make_shared<roaring::Roaring>();
    if (count > 0) {
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
    const ::doris::snii::reader::LogicalIndexReader* logical_reader = nullptr;
    RETURN_IF_ERROR(_get_logical_reader(context, &searcher_cache_handle, &uncached_reader,
                                        &logical_reader));
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
