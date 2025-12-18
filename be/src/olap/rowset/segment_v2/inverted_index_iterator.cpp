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

namespace doris::segment_v2 {

InvertedIndexIterator::InvertedIndexIterator() = default;

std::string InvertedIndexIterator::ensure_normalized_key(const std::string& analyzer_key) {
    if (analyzer_key.empty()) {
        return INVERTED_INDEX_DEFAULT_ANALYZER_KEY;
    }
    auto normalized = normalize_analyzer_key(analyzer_key);
    return normalized.empty() ? INVERTED_INDEX_DEFAULT_ANALYZER_KEY : normalized;
}

void InvertedIndexIterator::add_reader(InvertedIndexReaderType type,
                                       const InvertedIndexReaderPtr& reader) {
    std::string analyzer_key = ensure_normalized_key(
            build_analyzer_key_from_properties(reader->get_index_properties()));

    VLOG_DEBUG << "InvertedIndexIterator add_reader: type=" << static_cast<int>(type)
               << ", analyzer_key=" << analyzer_key;

    const size_t entry_index = _reader_entries.size();
    _reader_entries.push_back(ReaderEntry {type, std::move(analyzer_key), reader});

    // Update index for O(1) lookup
    _key_to_entries[_reader_entries.back().analyzer_key].push_back(entry_index);
}

Status InvertedIndexIterator::read_from_index(const IndexParam& param) {
    auto* i_param = std::get<InvertedIndexParam*>(param);
    if (i_param == nullptr) {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>("i_param is null");
    }
    DBUG_EXECUTE_IF("return_inverted_index_bypass", {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>("inverted index bypass");
    });

    // analyzer_key from param is expected to be pre-normalized; select_best_reader
    // will normalize again if needed, but this avoids redundant work when already normalized.
    const std::string& analyzer_name = (i_param->analyzer_ctx != nullptr)
                                               ? i_param->analyzer_ctx->analyzer_name
                                               : INVERTED_INDEX_DEFAULT_ANALYZER_KEY;
    auto reader =
            DORIS_TRY(select_best_reader(i_param->column_type, i_param->query_type, analyzer_name));
    if (UNLIKELY(reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
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
    auto reader = DORIS_TRY(select_best_reader(INVERTED_INDEX_DEFAULT_ANALYZER_KEY));
    return reader->read_null_bitmap(_context, cache_handle, nullptr);
}

Result<bool> InvertedIndexIterator::has_null() {
    auto reader = DORIS_TRY(select_best_reader(INVERTED_INDEX_DEFAULT_ANALYZER_KEY));
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

InvertedIndexIterator::CandidateResult InvertedIndexIterator::find_reader_candidates(
        const std::string& normalized_key) const {
    CandidateResult result;

    VLOG_DEBUG << "Finding reader candidates for normalized_key=" << normalized_key;

    // Helper lambda to populate candidates from index lookup result
    auto populate_from_indices = [this, &result](const std::vector<size_t>& indices) {
        result.candidates.reserve(indices.size());
        for (size_t idx : indices) {
            result.candidates.push_back(&_reader_entries[idx]);
        }
    };

    // Step 1: Try exact match using index (O(1) lookup)
    if (auto it = _key_to_entries.find(normalized_key); it != _key_to_entries.end()) {
        populate_from_indices(it->second);
        result.used_fallback = false;
        return result;
    }

    // Step 2: Fallback to default analyzer key (O(1) lookup)
    if (normalized_key != INVERTED_INDEX_DEFAULT_ANALYZER_KEY) {
        if (auto it = _key_to_entries.find(INVERTED_INDEX_DEFAULT_ANALYZER_KEY);
            it != _key_to_entries.end()) {
            populate_from_indices(it->second);
            result.used_fallback = true;
            LOG(WARNING) << "Analyzer key '" << normalized_key
                         << "' not found, falling back to default analyzer";
            return result;
        }
    }

    // Step 3: Fallback to all readers
    result.candidates.reserve(_reader_entries.size());
    for (const auto& entry : _reader_entries) {
        result.candidates.push_back(&entry);
    }
    if (!result.candidates.empty()) {
        result.used_fallback = true;
        LOG(WARNING) << "No matching analyzer found for '" << normalized_key
                     << "', using first available reader";
    }
    return result;
}

Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_best_reader(
        const vectorized::DataTypePtr& column_type, InvertedIndexQueryType query_type,
        const std::string& analyzer_key) {
    if (_reader_entries.empty()) {
        return ResultError(Status::RuntimeError(
                "No available inverted index readers. Check if index is properly initialized."));
    }

    // Normalize once at entry point, then use normalized key throughout
    const std::string normalized_key = ensure_normalized_key(analyzer_key);
    auto [candidates, used_fallback] = find_reader_candidates(normalized_key);
    if (candidates.empty()) {
        return ResultError(Status::RuntimeError(
                "No available inverted index readers. Check if index is properly initialized."));
    }

    // Select best reader based on column type and query type
    const auto field_type = column_type->get_storage_field_type();
    const bool is_string = is_string_type(field_type);

    if (is_string) {
        if (is_match_query(query_type)) {
            for (const auto* entry : candidates) {
                if (entry->type == InvertedIndexReaderType::FULLTEXT) {
                    return entry->reader;
                }
            }
        } else if (is_equal_query(query_type)) {
            for (const auto* entry : candidates) {
                if (entry->type == InvertedIndexReaderType::STRING_TYPE) {
                    return entry->reader;
                }
            }
        }
    }

    // Default: return first candidate
    return candidates.front()->reader;
}

Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_best_reader(
        const std::string& analyzer_key) {
    if (_reader_entries.empty()) {
        return ResultError(Status::RuntimeError(
                "No available inverted index readers. Check if index is properly initialized."));
    }

    // Normalize once at entry point
    const std::string normalized_key = ensure_normalized_key(analyzer_key);
    auto [candidates, used_fallback] = find_reader_candidates(normalized_key);
    if (candidates.empty()) {
        return ResultError(Status::RuntimeError(
                "No available inverted index readers. Check if index is properly initialized."));
    }

    return candidates.front()->reader;
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
