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

#include "inverted_index_iterator.h"

#include <memory>

#include "common/cast_set.h"
#include "common/logging.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/utils.h"

namespace doris::segment_v2 {

InvertedIndexIterator::InvertedIndexIterator() = default;

std::string InvertedIndexIterator::ensure_normalized_key(const std::string& analyzer_key) {
    // Simple normalization: lowercase, empty stays empty.
    // Empty means "user did not specify" (auto-select mode).
    // Non-empty means "user specified this analyzer" (exact match mode).
    return normalize_analyzer_key(analyzer_key);
}

void InvertedIndexIterator::add_reader(InvertedIndexReaderType type,
                                       const InvertedIndexReaderPtr& reader) {
    // build_analyzer_key_from_properties already returns a normalized key,
    // no need for additional normalization.
    std::string analyzer_key = build_analyzer_key_from_properties(reader->get_index_properties());

    VLOG_DEBUG << "InvertedIndexIterator add_reader: type=" << static_cast<int>(type)
               << ", analyzer_key=" << analyzer_key;

    const size_t entry_index = _reader_entries.size();
    _reader_entries.push_back(
            ReaderEntry {.type = type, .analyzer_key = std::move(analyzer_key), .reader = reader});

    // Update index for O(1) lookup
    _key_to_entries[_reader_entries.back().analyzer_key].push_back(entry_index);
}

Status InvertedIndexIterator::read_from_index(const IndexParam& param) {
    const auto* i_param_ptr = std::get_if<InvertedIndexParam*>(&param);
    if (i_param_ptr == nullptr) {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>(
                "param does not hold InvertedIndexParam*");
    }
    auto* i_param = *i_param_ptr;
    if (i_param == nullptr) {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>("i_param is null");
    }
    DBUG_EXECUTE_IF("return_inverted_index_bypass", {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>("inverted index bypass");
    });

    // analyzer_name from analyzer_ctx: what user specified in USING ANALYZER clause.
    // Empty means "user did not specify" (BE auto-selects index).
    // Non-empty means "user specified this analyzer" (BE exact matches).
    const std::string& analyzer_name =
            (i_param->analyzer_ctx != nullptr) ? i_param->analyzer_ctx->analyzer_name : "";
    auto reader =
            DORIS_TRY(select_best_reader(i_param->column_type, i_param->query_type, analyzer_name));
    if (UNLIKELY(reader == nullptr)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "inverted index reader is null");
    }
    auto* runtime_state = _context->runtime_state;
    if (!i_param->skip_try && reader->type() == InvertedIndexReaderType::BKD) {
        if (runtime_state != nullptr &&
            runtime_state->query_options().inverted_index_skip_threshold > 0 &&
            runtime_state->query_options().inverted_index_skip_threshold < 100) {
            auto query_bkd_limit_percent =
                    runtime_state->query_options().inverted_index_skip_threshold;
            size_t hit_count = 0;
            RETURN_IF_ERROR(try_read_from_inverted_index(reader, i_param->column_name,
                                                         i_param->query_value, i_param->query_type,
                                                         &hit_count));
            if (hit_count > i_param->num_rows * query_bkd_limit_percent / 100) {
                return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                        "hit count: {}, bkd inverted reached limit {}% , segment num "
                        "rows:{}", // add blackspace after % to avoid log4j format bug
                        hit_count, query_bkd_limit_percent, i_param->num_rows);
            }
        }
    }

    // Note: analyzer_ctx is now passed via i_param->analyzer_ctx
    auto execute_query = [&]() {
        return reader->query(_context, i_param->column_name, i_param->query_value,
                             i_param->query_type, i_param->roaring, i_param->analyzer_ctx);
    };

    if (runtime_state != nullptr && runtime_state->query_options().enable_profile) {
        InvertedIndexQueryStatistics query_stats;
        {
            SCOPED_RAW_TIMER(&query_stats.exec_time);
            RETURN_IF_ERROR(execute_query());
        }
        query_stats.column_name = i_param->column_name;
        query_stats.hit_rows = i_param->roaring->cardinality();
        _context->stats->inverted_index_stats.stats.emplace_back(query_stats);
    } else {
        RETURN_IF_ERROR(execute_query());
    }

    return Status::OK();
}

Status InvertedIndexIterator::read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle) {
    // For null bitmap, use any available reader (empty = auto-select)
    auto reader = DORIS_TRY(select_best_reader(""));
    return reader->read_null_bitmap(_context, cache_handle, nullptr);
}

Result<bool> InvertedIndexIterator::has_null() {
    // For has_null check, use any available reader (empty = auto-select)
    auto reader = DORIS_TRY(select_best_reader(""));
    return reader->has_null();
}

Status InvertedIndexIterator::try_read_from_inverted_index(const InvertedIndexReaderPtr& reader,
                                                           const std::string& column_name,
                                                           const void* query_value,
                                                           InvertedIndexQueryType query_type,
                                                           size_t* count) {
    // NOTE: only bkd index support try read now.
    if (query_type == InvertedIndexQueryType::GREATER_EQUAL_QUERY ||
        query_type == InvertedIndexQueryType::GREATER_THAN_QUERY ||
        query_type == InvertedIndexQueryType::LESS_EQUAL_QUERY ||
        query_type == InvertedIndexQueryType::LESS_THAN_QUERY ||
        query_type == InvertedIndexQueryType::EQUAL_QUERY) {
        RETURN_IF_ERROR(reader->try_query(_context, column_name, query_value, query_type, count));
    }
    return Status::OK();
}

Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_for_text(
        const AnalyzerMatchResult& match, InvertedIndexQueryType query_type,
        const std::string& analyzer_key) {
    // Bypass: explicit analyzer specified but not found
    if (match.empty() && AnalyzerKeyMatcher::is_explicit(analyzer_key)) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "No inverted index reader found for analyzer '{}'. "
                "The index for this analyzer may not be built yet.",
                analyzer_key));
    }

    if (match.empty()) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(
                "No available inverted index readers for text column."));
    }

    // MATCH queries prefer FULLTEXT
    if (is_match_query(query_type)) {
        for (const auto* entry : match.candidates) {
            if (entry->type == InvertedIndexReaderType::FULLTEXT) {
                return entry->reader;
            }
        }
    }

    // EQUAL queries prefer STRING_TYPE for exact match
    if (is_equal_query(query_type)) {
        for (const auto* entry : match.candidates) {
            if (entry->type == InvertedIndexReaderType::STRING_TYPE) {
                return entry->reader;
            }
        }
    }

    // Default: return first candidate
    return match.candidates.front()->reader;
}

Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_for_numeric(
        const AnalyzerMatchResult& match, InvertedIndexQueryType query_type) {
    if (match.empty()) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(
                "No available inverted index readers for numeric column."));
    }

    // RANGE queries prefer BKD
    if (is_range_query(query_type)) {
        for (const auto* entry : match.candidates) {
            if (entry->type == InvertedIndexReaderType::BKD) {
                return entry->reader;
            }
        }
    }

    // Fallback priority: BKD > STRING_TYPE > others
    for (const auto* entry : match.candidates) {
        if (entry->type == InvertedIndexReaderType::BKD) {
            return entry->reader;
        }
    }
    for (const auto* entry : match.candidates) {
        if (entry->type == InvertedIndexReaderType::STRING_TYPE) {
            return entry->reader;
        }
    }

    // Last resort: first available
    return match.candidates.front()->reader;
}

Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_best_reader(
        const vectorized::DataTypePtr& column_type, InvertedIndexQueryType query_type,
        const std::string& analyzer_key) {
    if (_reader_entries.empty()) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(
                "No available inverted index readers. Check if index is properly initialized."));
    }

    // Normalize once at entry point
    const std::string normalized_key = ensure_normalized_key(analyzer_key);

    // Single reader optimization
    if (_reader_entries.size() == 1) {
        const auto& entry = _reader_entries.front();
        if (AnalyzerKeyMatcher::is_explicit(normalized_key) &&
            entry.analyzer_key != normalized_key) {
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "No inverted index reader found for analyzer '{}'. "
                    "Available analyzer: '{}'.",
                    normalized_key, entry.analyzer_key));
        }
        return entry.reader;
    }

    // Match analyzer key using AnalyzerKeyMatcher
    auto match = AnalyzerKeyMatcher::match(normalized_key, _reader_entries, _key_to_entries);

    // Dispatch by column type
    const auto field_type = column_type->get_storage_field_type();

    if (is_string_type(field_type)) {
        return select_for_text(match, query_type, normalized_key);
    }

    if (is_numeric_type(field_type)) {
        return select_for_numeric(match, query_type);
    }

    // Default: return first candidate or error
    if (match.empty()) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(
                "No available inverted index readers for column type."));
    }
    return match.candidates.front()->reader;
}

Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_best_reader(
        const std::string& analyzer_key) {
    if (_reader_entries.empty()) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(
                "No available inverted index readers. Check if index is properly initialized."));
    }

    const std::string normalized_key = ensure_normalized_key(analyzer_key);

    // Single reader optimization
    if (_reader_entries.size() == 1) {
        const auto& entry = _reader_entries.front();
        if (AnalyzerKeyMatcher::is_explicit(normalized_key) &&
            entry.analyzer_key != normalized_key) {
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "No inverted index reader found for analyzer '{}'. "
                    "Available analyzer: '{}'.",
                    normalized_key, entry.analyzer_key));
        }
        return entry.reader;
    }

    // Match and return first candidate
    auto match = AnalyzerKeyMatcher::match(normalized_key, _reader_entries, _key_to_entries);

    if (match.empty()) {
        if (AnalyzerKeyMatcher::is_explicit(normalized_key)) {
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "No inverted index reader found for analyzer '{}'.", normalized_key));
        }
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(
                "No available inverted index readers."));
    }

    return match.candidates.front()->reader;
}

IndexReaderPtr InvertedIndexIterator::get_reader(IndexReaderType type) const {
    const auto* inverted_type = std::get_if<InvertedIndexReaderType>(&type);
    if (inverted_type == nullptr) {
        return nullptr;
    }
    for (const auto& entry : _reader_entries) {
        if (entry.type == *inverted_type) {
            return entry.reader;
        }
    }
    return nullptr;
}

} // namespace doris::segment_v2
