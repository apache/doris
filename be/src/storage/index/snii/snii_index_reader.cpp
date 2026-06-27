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
#include "snii/format/null_bitmap.h"
#include "snii/query/boolean_query.h"
#include "snii/query/phrase_query.h"
#include "snii/query/prefix_query.h"
#include "snii/query/regexp_query.h"
#include "snii/query/term_query.h"
#include "snii/query/wildcard_query.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/snii/snii_doris_adapter.h"

namespace doris::segment_v2 {

namespace {

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
        parse_phrase_slop(&search_str, query_info);
        SCOPED_RAW_TIMER(&context->stats->inverted_index_analyzer_timer);
        try {
            query_info->term_infos = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                    search_str, _index_meta.properties());
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

void SniiIndexReader::_docids_to_bitmap(const std::vector<uint32_t>& docids,
                                        std::shared_ptr<roaring::Roaring>* bit_map) {
    auto result = std::make_shared<roaring::Roaring>();
    if (!docids.empty()) {
        result->addMany(docids.size(), docids.data());
    }
    result->runOptimize();
    *bit_map = std::move(result);
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
    InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                 std::move(cache_value)};
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    if (handle_query_cache(context, cache, cache_key, &cache_handler, bit_map)) {
        return Status::OK();
    }

    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(context->io_ctx);
    RETURN_IF_ERROR(
            _index_file_reader->init(config::inverted_index_read_buffer_size, context->io_ctx));
    auto logical_reader = DORIS_TRY(_index_file_reader->open_snii_index(&_index_meta));

    std::vector<uint32_t> docids;
    snii::Status status;
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY:
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
        status = terms.size() == 1
                         ? snii::query::term_query(*logical_reader, terms.front(), &docids)
                         : snii::query::boolean_or(*logical_reader, terms, &docids);
        break;
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
        status = snii::query::boolean_and(*logical_reader, terms, &docids);
        break;
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
        if (query_info.slop != 0) {
            return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "SNII does not support sloppy phrase query yet");
        }
        status = terms.size() == 1
                         ? snii::query::term_query(*logical_reader, terms.front(), &docids)
                         : snii::query::phrase_query(*logical_reader, terms, &docids);
        break;
    case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
        status = snii::query::phrase_prefix_query(*logical_reader, terms, &docids, max_expansions);
        break;
    case InvertedIndexQueryType::MATCH_REGEXP_QUERY:
        status = snii::query::regexp_query(*logical_reader, search_str, &docids, max_expansions);
        break;
    case InvertedIndexQueryType::WILDCARD_QUERY:
        status = snii::query::wildcard_query(*logical_reader, search_str, &docids, max_expansions);
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
    RETURN_IF_ERROR(snii_doris::to_doris_status(status));
    _docids_to_bitmap(docids, &bit_map);
    cache->insert(cache_key, bit_map, &cache_handler);
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
    RETURN_IF_ERROR(
            _index_file_reader->init(config::inverted_index_read_buffer_size, context->io_ctx));
    auto logical_reader = DORIS_TRY(_index_file_reader->open_snii_index(&_index_meta));
    auto null_bitmap = std::make_shared<roaring::Roaring>();
    const auto& ref = logical_reader->section_refs().null_bitmap;
    if (ref.length > 0) {
        std::vector<uint8_t> bytes;
        RETURN_IF_ERROR(snii_doris::to_doris_status(
                logical_reader->reader()->read_at(ref.offset, ref.length, &bytes)));
        snii::format::NullBitmapReader reader;
        RETURN_IF_ERROR(snii_doris::to_doris_status(
                snii::format::NullBitmapReader::open(snii::Slice(bytes), &reader)));
        reader.copy_to(null_bitmap.get());
        null_bitmap->runOptimize();
    }
    cache->insert(cache_key, null_bitmap, cache_handle);
    return Status::OK();
}

} // namespace doris::segment_v2
