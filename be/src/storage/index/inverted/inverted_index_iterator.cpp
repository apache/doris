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

#include "storage/index/inverted/inverted_index_iterator.h"

#include <memory>

#include "common/cast_set.h"
#include "common/logging.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/utils.h"

namespace doris::segment_v2 {

InvertedIndexIterator::InvertedIndexIterator() = default;

std::string InvertedIndexIterator::ensure_normalized_key(const std::string& analyzer_key) {
    return normalize_analyzer_key(analyzer_key);
}

void InvertedIndexIterator::add_reader(InvertedIndexReaderType type,
                                       const InvertedIndexReaderPtr& reader) {
    // build_analyzer_key_from_properties already returns a normalized key,
    // no need for additional normalization.
    std::string analyzer_key = build_analyzer_key_from_properties(reader->get_index_properties());

    VLOG_DEBUG << "InvertedIndexIterator add_reader: type=" << static_cast<int>(type)
               << ", analyzer_key=" << analyzer_key;

    auto status = add_inverted_index_selection_candidate(
            InvertedIndexSelectionCandidate {.index_id = cast_set<int64_t>(reader->get_index_id()),
                                             .reader_type = type,
                                             .analyzer_key = std::move(analyzer_key)},
            &_selection_candidates, &_key_to_entries);
    DORIS_CHECK(status.ok()) << status;
    _readers.push_back(reader);
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

    // The execution context carries reader selection separately from analyzer execution.
    const std::string& analyzer_key =
            (i_param->analyzer_ctx != nullptr) ? i_param->analyzer_ctx->analyzer_key : "";
    auto reader =
            DORIS_TRY(select_best_reader(i_param->column_type, i_param->query_type, analyzer_key));
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
        if (i_param->null_bitmap_cache_handle != nullptr) {
            return reader->query_with_null_bitmap(
                    _context, i_param->column_name, i_param->query_value, i_param->query_type,
                    i_param->roaring, i_param->null_bitmap_cache_handle, i_param->analyzer_ctx);
        }
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
    auto reader = DORIS_TRY(select_any_reader());
    return reader->read_null_bitmap(_context, cache_handle, nullptr);
}

Result<bool> InvertedIndexIterator::has_null() {
    auto reader = DORIS_TRY(select_any_reader());
    return reader->has_null();
}

Status InvertedIndexIterator::try_read_from_inverted_index(const InvertedIndexReaderPtr& reader,
                                                           const std::string& column_name,
                                                           const Field& query_value,
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

Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_best_reader(
        const DataTypePtr& column_type, InvertedIndexQueryType query_type,
        const std::string& analyzer_key) {
    const std::string normalized_key = ensure_normalized_key(analyzer_key);
    const auto field_type = get_inverted_index_leaf_field_type(column_type);
    auto selection = select_best_inverted_index_candidate(_selection_candidates, _key_to_entries,
                                                          field_type, query_type, normalized_key);
    if (!selection.has_value()) {
        return ResultError(std::move(selection.error()));
    }
    const size_t selected = *selection;
    DORIS_CHECK(selected < _readers.size());
    return _readers[selected];
}

Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_any_reader() {
    auto selection = select_best_inverted_index_candidate(
            _selection_candidates, _key_to_entries, FieldType::OLAP_FIELD_TYPE_UNKNOWN,
            InvertedIndexQueryType::UNKNOWN_QUERY, "");
    if (!selection.has_value()) {
        return ResultError(std::move(selection.error()));
    }
    const size_t selected = *selection;
    DORIS_CHECK(selected < _readers.size());
    return _readers[selected];
}

Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_best_reader(
        const std::string& analyzer_key) {
    if (analyzer_key.empty()) {
        return select_any_reader();
    }
    const std::string normalized_key = ensure_normalized_key(analyzer_key);
    auto selection = select_best_inverted_index_candidate(
            _selection_candidates, _key_to_entries, FieldType::OLAP_FIELD_TYPE_UNKNOWN,
            InvertedIndexQueryType::UNKNOWN_QUERY, normalized_key);
    if (!selection.has_value()) {
        return ResultError(std::move(selection.error()));
    }
    const size_t selected = *selection;
    DORIS_CHECK(selected < _readers.size());
    return _readers[selected];
}

IndexReaderPtr InvertedIndexIterator::get_reader(IndexReaderType type) const {
    const auto* inverted_type = std::get_if<InvertedIndexReaderType>(&type);
    if (inverted_type == nullptr) {
        return nullptr;
    }
    for (size_t i = 0; i < _selection_candidates.size(); ++i) {
        if (_selection_candidates[i].reader_type == *inverted_type) {
            DORIS_CHECK(i < _readers.size());
            return _readers[i];
        }
    }
    return nullptr;
}

} // namespace doris::segment_v2
