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

#include "format_v2/table/lance_reader.h"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <lance/lance.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "exec/common/endian.h"
#include "format_v2/lance/lance_reader_helper.h"
#include "format_v2/lance/lance_runtime_filter_helper.h"
#include "format_v2/lance/lance_session_manager.h"
#include "runtime/file_scan_profile.h"
#include "runtime/runtime_state.h"

namespace doris::format::lance {

LanceTableReader::~LanceTableReader() {
    static_cast<void>(close());
}

Status LanceTableReader::fetch_schema(const TFileRangeDesc& range,
                                      const TFileScanRangeParams& scan_params,
                                      std::vector<std::string>* column_names,
                                      std::vector<DataTypePtr>* column_types) const {
    if (column_names == nullptr || column_types == nullptr) {
        return Status::InvalidArgument("Lance schema output must not be null");
    }
    const auto& params = range.table_format_params.lance_params;
    std::vector<std::string> storage_options;
    RETURN_IF_ERROR(build_lance_storage_options(&scan_params, &storage_options));
    std::vector<const char*> storage_option_ptrs;
    storage_option_ptrs.reserve(storage_options.size() + 1);
    for (const auto& option : storage_options) {
        storage_option_ptrs.emplace_back(option.c_str());
    }
    storage_option_ptrs.emplace_back(nullptr);

    std::unique_ptr<LanceDataset, LanceDatasetDeleter> dataset(
            lance_dataset_open(params.dataset_uri.c_str(),
                               storage_options.empty() ? nullptr : storage_option_ptrs.data(),
                               static_cast<uint64_t>(params.version)));
    if (dataset == nullptr) {
        return lance_error("open Lance dataset for schema");
    }

    std::shared_ptr<arrow::Schema> schema;
    RETURN_IF_ERROR(import_lance_dataset_schema(dataset.get(), &schema));
    return convert_arrow_schema_to_doris(schema, column_names, column_types);
}

Status LanceTableReader::init(TableReadOptions&& options) {
    RETURN_IF_ERROR(TableReader::init(std::move(options)));
    DORIS_CHECK(_runtime_state != nullptr);
    DORIS_CHECK(_scanner_profile != nullptr);
    DORIS_CHECK(_scan_params != nullptr);
    RETURN_IF_ERROR(_resolve_search_kind());

    const auto& lance_scan_params = _scan_params->lance_scan_params;
    ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, LANCE_READER_PROFILE,
                               file_scan_profile::TABLE_READER, 1);
    _dataset_open_time = ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LanceDatasetOpenTime",
                                                    LANCE_READER_PROFILE, 1);
    _arrow_to_doris_block_time = ADD_CHILD_TIMER_WITH_LEVEL(
            _scanner_profile, "LanceArrowToDorisBlockTime", LANCE_READER_PROFILE, 1);
    _data_cache_bytes_read_from_cache =
            ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceDataCacheBytesReadFromCache",
                                         TUnit::BYTES, LANCE_READER_PROFILE, 1);
    _data_cache_bytes_read_from_remote =
            ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceDataCacheBytesReadFromRemote",
                                         TUnit::BYTES, LANCE_READER_PROFILE, 1);
    if (_search_kind != SearchKind::NORMAL) {
        RETURN_IF_ERROR(_validate_external_search_request());
        const auto& request = lance_scan_params.external_search_request;
        int64_t top_k;
        int64_t offset;
        if (_search_kind == SearchKind::VECTOR) {
            const auto& vector = request.search_query.vector_search;
            top_k = vector.top_k;
            offset = vector.offset;
            _scanner_profile->add_info_string("LanceSearchType", "VECTOR");
        } else {
            DORIS_CHECK(_search_kind == SearchKind::FULL_TEXT);
            const auto& full_text = request.search_query.full_text_search;
            top_k = full_text.top_k;
            offset = full_text.offset;
            _scanner_profile->add_info_string("LanceSearchType", "FULL_TEXT");
            _scanner_profile->add_info_string(
                    "LanceFtsCoverageMode",
                    full_text.coverage_mode == TFtsCoverageMode::STRICT ? "STRICT" : "INDEX_ONLY");
            if (full_text.query_type == TFtsQueryType::MATCH) {
                _scanner_profile->add_info_string("LanceFtsQueryType", "MATCH");
                _scanner_profile->add_info_string(
                        "LanceFtsMatchOperator",
                        full_text.match_operator == TFtsMatchOperator::AND ? "AND" : "OR");
                _scanner_profile->add_info_string("LanceFtsMaxFuzzyDistance",
                                                  std::to_string(full_text.max_fuzzy_distance));
            } else {
                _scanner_profile->add_info_string("LanceFtsQueryType", "PHRASE");
                _scanner_profile->add_info_string("LanceFtsPhraseSlop",
                                                  std::to_string(full_text.phrase_slop));
            }
        }
        _scanner_profile->add_info_string("LanceTopK", std::to_string(top_k));
        _scanner_profile->add_info_string("LanceOffset", std::to_string(offset));
        _scanner_profile->add_info_string("LanceTopKPlusOffset", std::to_string(top_k + offset));
    }
    if (_scan_params->__isset.lance_scan_params &&
        lance_scan_params.__isset.lance_substrait_filter) {
        _scanner_profile->add_info_string("LancePushdownFormat", "SUBSTRAIT");
        _scanner_profile->add_info_string(
                "LanceSubstraitFilterBytes",
                std::to_string(lance_scan_params.lance_substrait_filter.size()));
    }

    return _record_batch_converter.init(_runtime_state, _projected_columns, _search_kind);
}

Status LanceTableReader::prepare_split(const SplitReadOptions& options) {
    _close_scanner();
    _eof = false;
    _runtime_filter_cache = options.cache;

    RETURN_IF_ERROR(TableReader::prepare_split(options));
    if (current_split_pruned()) {
        return Status::OK();
    }
    // COUNT(*)/COUNT(1) with no filter is served from Lance metadata. The base class already set
    // _remaining_table_level_count from the split's table_level_row_count, so skip opening any
    // dataset scanner; get_block() synthesizes the counted rows.
    if (_is_table_level_count_active()) {
        return Status::OK();
    }
    if (_record_batch_converter.requires_global_rowid() && !_global_rowid_context.has_value()) {
        return Status::InvalidArgument(
                "Lance global row id requested without global row id context");
    }

    RETURN_IF_ERROR(_ensure_dataset_open(options.current_range));
    RETURN_IF_ERROR(_open_scanner(options.current_range));
    return Status::OK();
}

Status LanceTableReader::get_block(Block* block, bool* eos) {
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(eos != nullptr);
    DORIS_CHECK(block->columns() == _projected_columns.size());
    *eos = false;

    if (_eof) {
        *eos = true;
        return Status::OK();
    }
    // Metadata COUNT(*) split: no scanner is opened. Emit synthetic rows for the upper COUNT
    // operator directly from the row count the base class parsed out of the split.
    if (_is_table_level_count_active()) {
        return _read_table_level_count(block, eos);
    }
    if (_scanner == nullptr) {
        return Status::InternalError("Lance scanner is not initialized for the current split");
    }

    const auto target_rows = std::max<size_t>(1, _scanner_batch_size);
    while (true) {
        block->clear_column_data(_projected_columns.size());
        size_t raw_rows = 0;
        while (raw_rows < target_rows) {
            if (_io_ctx != nullptr && _io_ctx->should_stop) {
                _eof = true;
                _close_scanner();
                *eos = true;
                return Status::OK();
            }

            LanceBatch* raw_batch = nullptr;
            int32_t scan_status = 0;
            {
                SCOPED_TIMER(_scanner_read_time);
                scan_status = lance_scanner_next(_scanner, &raw_batch);
            }
            if (scan_status == 1) {
                _eof = true;
                _close_scanner();
                break;
            }
            if (scan_status != 0 || raw_batch == nullptr) {
                return lance_error("read next Lance batch");
            }

            std::unique_ptr<LanceBatch, LanceBatchDeleter> batch(raw_batch);
            size_t rows = 0;
            {
                SCOPED_TIMER(_arrow_to_doris_block_time);
                RETURN_IF_ERROR(_fill_block_from_lance_batch(batch.get(), block, &rows));
            }
            _record_scan_rows(rows);
            raw_rows += rows;
        }

        if (raw_rows == 0) {
            DORIS_CHECK(_eof);
            *eos = true;
            return Status::OK();
        }

        if (block->rows() > 0) {
            // Preserve a non-empty final block. The next get_block() observes `_eof` and reports
            // split EOF, matching the generic reader contract.
            return Status::OK();
        }
        if (_eof) {
            *eos = true;
            return Status::OK();
        }
    }
}

Status LanceTableReader::read_by_row_ids(const TFileRangeDesc& range,
                                         const std::vector<uint64_t>& row_ids, Block* block) {
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(block->columns() == _projected_columns.size());
    if (row_ids.empty()) {
        return Status::OK();
    }
    if (_row_id_take_read_time == nullptr) {
        _row_id_take_read_time = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "LanceRowIdTakeReadTime", LANCE_READER_PROFILE, 1);
    }
    if (_row_id_fetch_total_time == nullptr) {
        _row_id_fetch_total_time = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "LanceRowIdFetchTotalTime", LANCE_READER_PROFILE, 1);
    }
    SCOPED_TIMER(_row_id_fetch_total_time);

    // Phase-two row fetch does not execute FTS, so a reader created only for take_rows must not
    // collect query-specific global statistics.
    RETURN_IF_ERROR(_ensure_dataset_open(range, false));
    std::vector<const char*> columns;
    columns.reserve(_projected_columns.size() + 1);
    for (const auto& column : _projected_columns) {
        columns.emplace_back(column.name.c_str());
    }
    columns.emplace_back(nullptr);

    ArrowArrayStream stream {};
    int32_t take_rows_status = 0;
    {
        SCOPED_TIMER(_row_id_take_read_time);
        take_rows_status = lance_dataset_take_rows(_dataset, row_ids.data(), row_ids.size(),
                                                   columns.data(), &stream);
    }
    if (take_rows_status != 0) {
        if (stream.release != nullptr) {
            stream.release(&stream);
        }
        return lance_error("take Lance rows by row id");
    }
    auto imported_reader = arrow::ImportRecordBatchReader(&stream);
    if (!imported_reader.ok()) {
        if (stream.release != nullptr) {
            stream.release(&stream);
        }
        return Status::InternalError("import Lance take-rows stream failed: {}",
                                     imported_reader.status().message());
    }

    size_t fetched_rows = 0;
    auto batch_reader = std::move(imported_reader).ValueUnsafe();
    while (true) {
        std::shared_ptr<arrow::RecordBatch> record_batch;
        arrow::Status read_status;
        {
            // Lance may materialize take_rows lazily while its Arrow stream is consumed.
            SCOPED_TIMER(_row_id_take_read_time);
            read_status = batch_reader->ReadNext(&record_batch);
        }
        if (!read_status.ok()) {
            return Status::InternalError("read Lance take-rows batch failed: {}",
                                         read_status.message());
        }
        if (record_batch == nullptr) {
            break;
        }
        size_t rows = 0;
        {
            SCOPED_TIMER(_arrow_to_doris_block_time);
            RETURN_IF_ERROR(_record_batch_converter.convert_record_batch_to_block(
                    record_batch, block, _global_rowid_context, &rows));
        }
        fetched_rows += rows;
    }
    if (fetched_rows != row_ids.size()) {
        return Status::InternalError("Lance row-id fetch returned {} rows for {} requested row ids",
                                     fetched_rows, row_ids.size());
    }
    return Status::OK();
}

Status LanceTableReader::abort_split() {
    _close_scanner();
    _eof = true;
    return TableReader::abort_split();
}

Status LanceTableReader::close() {
    _close_scanner();
    _close_dataset();
    _opened_dataset_key.reset();
    _eof = true;
    return TableReader::close();
}

Status LanceTableReader::_resolve_search_kind() {
    DORIS_CHECK(_scan_params != nullptr);
    _search_kind = SearchKind::NORMAL;
    if (!_scan_params->__isset.lance_scan_params) {
        return Status::OK();
    }
    const auto& lance_scan_params = _scan_params->lance_scan_params;
    if (!lance_scan_params.__isset.external_search_request) {
        return Status::OK();
    }
    const auto& request = lance_scan_params.external_search_request;
    if (!request.__isset.search_query) {
        return Status::InvalidArgument("external search request requires search_query");
    }
    const bool has_vector = request.search_query.__isset.vector_search;
    const bool has_full_text = request.search_query.__isset.full_text_search;
    if (has_vector == has_full_text) {
        return Status::InvalidArgument("external search query must set exactly one search kind");
    }
    _search_kind = has_vector ? SearchKind::VECTOR : SearchKind::FULL_TEXT;
    return Status::OK();
}

Status LanceTableReader::_validate_external_search_request() const {
    // FE validates requests produced by the search TVFs, but this reader consumes a deserialized
    // Thrift boundary. Recheck structural invariants and values used for allocation, pointer
    // arithmetic, C-string calls, and narrowing conversions before accessing them below.
    DORIS_CHECK(_scan_params != nullptr);
    DORIS_CHECK(_scan_params->__isset.lance_scan_params);
    const auto& lance_scan_params = _scan_params->lance_scan_params;
    DORIS_CHECK(lance_scan_params.__isset.external_search_request);
    if (lance_scan_params.__isset.lance_substrait_filter) {
        return Status::InvalidArgument(
                "Lance external search cannot combine its pre-search filter with "
                "lance_substrait_filter");
    }

    const auto& request = lance_scan_params.external_search_request;
    if (request.schema_version != 1) {
        return Status::NotSupported("unsupported external search schema version: {}",
                                    request.schema_version);
    }
    DORIS_CHECK(request.__isset.search_query);
    DORIS_CHECK(_search_kind != SearchKind::NORMAL);
    constexpr auto UINT32_MAX_VALUE = static_cast<int64_t>(std::numeric_limits<uint32_t>::max());
    if (_search_kind == SearchKind::VECTOR) {
        const auto& vector = request.search_query.vector_search;
        if (!vector.__isset.column || vector.column.empty() ||
            vector.column.find('\0') != std::string::npos) {
            return Status::InvalidArgument("Lance vector search requires a non-empty column");
        }
        if (!vector.__isset.query_vector) {
            return Status::InvalidArgument("Lance vector search requires a query vector");
        }
        const auto& query_vector = vector.query_vector;
        if (!query_vector.__isset.element_type || !query_vector.__isset.dimension ||
            !query_vector.__isset.values) {
            return Status::InvalidArgument(
                    "Lance query vector requires element_type, dimension, and values");
        }
        if (query_vector.dimension <= 0) {
            return Status::InvalidArgument("Lance query vector dimension must be positive: {}",
                                           query_vector.dimension);
        }
        const auto element_width = lance_vector_element_width(query_vector.element_type);
        if (element_width == 0) {
            return Status::NotSupported("unsupported Lance query vector element type: {}",
                                        static_cast<int>(query_vector.element_type));
        }
        const bool multi_vector = query_vector.__isset.num_vectors;
        // The optional count distinguishes a query matrix, including a one-row matrix.
        if (multi_vector && query_vector.num_vectors <= 0) {
            return Status::InvalidArgument(
                    "Lance multi-vector queries require positive num_vectors");
        }
        if (multi_vector && (query_vector.element_type == TVectorElementType::UINT8 ||
                             query_vector.element_type == TVectorElementType::INT8 ||
                             (vector.__isset.metric && vector.metric == TVectorMetric::HAMMING))) {
            return Status::NotSupported(
                    "Lance multi-vector search requires floating-point vectors and l2, cosine, or "
                    "dot");
        }
        const auto dimension = static_cast<size_t>(query_vector.dimension);
        const auto count = multi_vector ? static_cast<size_t>(query_vector.num_vectors) : 1;
        if (dimension > std::numeric_limits<size_t>::max() / count / element_width ||
            query_vector.values.size() != dimension * count * element_width) {
            return Status::InvalidArgument(
                    "Lance query vector byte size {} does not match {} vectors of dimension {} and "
                    "element width {}",
                    query_vector.values.size(), count, dimension, element_width);
        }
        if (!vector.__isset.top_k || vector.top_k <= 0) {
            return Status::InvalidArgument("Lance vector search top_k must be positive");
        }
        if (!vector.__isset.offset || vector.offset < 0) {
            return Status::InvalidArgument("Lance vector search offset must be non-negative");
        }
        if (vector.offset > UINT32_MAX_VALUE || vector.top_k > UINT32_MAX_VALUE - vector.offset) {
            return Status::InvalidArgument(
                    "Lance vector search top_k + offset exceeds uint32 range");
        }
        // Match FE/C API limits before constructing the per-subvector ANN plan branches.
        constexpr int MAX_QUERY_VECTORS = 128;
        constexpr int64_t MAX_QUERY_VECTOR_CANDIDATES = 100000;
        const auto refine_factor =
                request.__isset.vector_search_options &&
                                request.vector_search_options.__isset.refine_factor
                        ? request.vector_search_options.refine_factor
                        : 1;
        if (multi_vector &&
            (query_vector.num_vectors > MAX_QUERY_VECTORS || refine_factor <= 0 ||
             vector.top_k + vector.offset > MAX_QUERY_VECTOR_CANDIDATES / refine_factor ||
             query_vector.num_vectors >
                     MAX_QUERY_VECTOR_CANDIDATES / (vector.top_k + vector.offset))) {
            return Status::InvalidArgument(
                    "multi-vector query exceeds 128 subvectors or 100000 subvector-candidates");
        }
    } else {
        DORIS_CHECK(_search_kind == SearchKind::FULL_TEXT);
        const auto& full_text = request.search_query.full_text_search;
        if (!full_text.__isset.column || full_text.column.empty() ||
            full_text.column.find('\0') != std::string::npos) {
            return Status::InvalidArgument("Lance full-text search requires a non-empty column");
        }
        if (!full_text.__isset.query || full_text.query.empty() ||
            full_text.query.find('\0') != std::string::npos) {
            return Status::InvalidArgument("Lance full-text search requires a non-empty query");
        }
        if (!full_text.__isset.top_k || full_text.top_k <= 0) {
            return Status::InvalidArgument("Lance full-text search top_k must be positive");
        }
        if (!full_text.__isset.offset || full_text.offset < 0) {
            return Status::InvalidArgument("Lance full-text search offset must be non-negative");
        }
        if (full_text.offset > UINT32_MAX_VALUE ||
            full_text.top_k > UINT32_MAX_VALUE - full_text.offset) {
            return Status::InvalidArgument(
                    "Lance full-text search top_k + offset exceeds uint32 range");
        }
        if (!full_text.__isset.coverage_mode ||
            (full_text.coverage_mode != TFtsCoverageMode::STRICT &&
             full_text.coverage_mode != TFtsCoverageMode::INDEX_ONLY)) {
            return Status::InvalidArgument(
                    "Lance full-text search requires STRICT or INDEX_ONLY coverage_mode");
        }
        if (full_text.__isset.global_statistics && full_text.global_statistics.empty()) {
            return Status::InvalidArgument(
                    "Lance full-text search global_statistics must not be empty when set");
        }
        if (!full_text.__isset.query_type || (full_text.query_type != TFtsQueryType::MATCH &&
                                              full_text.query_type != TFtsQueryType::PHRASE)) {
            return Status::InvalidArgument(
                    "Lance full-text search requires MATCH or PHRASE query_type");
        }
        if (full_text.query_type == TFtsQueryType::MATCH) {
            if (!full_text.__isset.match_operator ||
                (full_text.match_operator != TFtsMatchOperator::OR &&
                 full_text.match_operator != TFtsMatchOperator::AND)) {
                return Status::InvalidArgument(
                        "Lance MATCH query requires OR or AND match_operator");
            }
            if (!full_text.__isset.max_fuzzy_distance || full_text.max_fuzzy_distance < 0) {
                return Status::InvalidArgument(
                        "Lance MATCH query max_fuzzy_distance must be non-negative");
            }
            if (full_text.max_fuzzy_distance != 0) {
                return Status::NotSupported(
                        "Lance prepared FTS does not yet support max_fuzzy_distance={}",
                        full_text.max_fuzzy_distance);
            }
            if (full_text.__isset.phrase_slop) {
                return Status::InvalidArgument("Lance MATCH query cannot set phrase_slop");
            }
        } else {
            if (!full_text.__isset.phrase_slop || full_text.phrase_slop < 0) {
                return Status::InvalidArgument(
                        "Lance PHRASE query phrase_slop must be non-negative");
            }
            if (full_text.__isset.match_operator || full_text.__isset.max_fuzzy_distance) {
                return Status::InvalidArgument(
                        "Lance PHRASE query cannot set MATCH-only parameters");
            }
        }
        if (request.__isset.vector_search_options) {
            return Status::InvalidArgument(
                    "Lance full-text search cannot set vector_search_options");
        }
    }

    if (request.__isset.search_filter) {
        const auto& filter = request.search_filter;
        if (!filter.__isset.format || !filter.__isset.payload || filter.payload.empty()) {
            return Status::InvalidArgument(
                    "external search filter requires format and non-empty payload");
        }
        if (filter.format != TSearchFilterFormat::SQL) {
            return Status::NotSupported("unsupported external search filter format: {}",
                                        static_cast<int>(filter.format));
        }
        if (filter.payload.find('\0') != std::string::npos) {
            return Status::InvalidArgument("Lance SQL search filter contains an embedded NUL byte");
        }
    }

    if (_search_kind == SearchKind::VECTOR && request.__isset.vector_search_options) {
        const auto& options = request.vector_search_options;
        if (options.__isset.nprobes && options.nprobes <= 0) {
            return Status::InvalidArgument("Lance nprobes must be positive");
        }
        if (options.__isset.refine_factor && options.refine_factor <= 0) {
            return Status::InvalidArgument("Lance refine_factor must be positive");
        }
        if (options.__isset.ef && options.ef <= 0) {
            return Status::InvalidArgument("Lance ef must be positive");
        }
    }
    return Status::OK();
}

Status LanceTableReader::_ensure_dataset_open(const TFileRangeDesc& range,
                                              bool prepare_fts_context) {
    DatasetKey key;
    RETURN_IF_ERROR(_dataset_key(range, &key));
    if (_dataset == nullptr) {
        RETURN_IF_ERROR(_open_dataset(key));
        _opened_dataset_key = key;
    } else if (!_opened_dataset_key.has_value() || *_opened_dataset_key != key) {
        return Status::InvalidArgument(
                "Lance reader cannot mix dataset snapshots or storage options");
    }
    if (_search_kind == SearchKind::FULL_TEXT && prepare_fts_context &&
        _fts_query_context == nullptr) {
        RETURN_IF_ERROR(_prepare_fts_query_context());
    }
    return Status::OK();
}

Status LanceTableReader::_open_dataset(const DatasetKey& key) {
    std::vector<const char*> storage_option_ptrs;
    storage_option_ptrs.reserve(key.storage_options.size() + 1);
    for (const auto& option : key.storage_options) {
        storage_option_ptrs.emplace_back(option.c_str());
    }
    storage_option_ptrs.emplace_back(nullptr);

    std::unique_ptr<LanceDataset, LanceDatasetDeleter> dataset;
    {
        SCOPED_TIMER(_dataset_open_time);
        LanceDataset* raw_dataset = nullptr;
        RETURN_IF_ERROR(LanceSessionManager::instance().open_dataset(
                key.uri.c_str(), key.storage_options.empty() ? nullptr : storage_option_ptrs.data(),
                static_cast<uint64_t>(key.version), &raw_dataset));
        dataset.reset(raw_dataset);
    }
    _dataset = dataset.release();
    return Status::OK();
}

Status LanceTableReader::_prepare_fts_query_context() {
    DORIS_CHECK(_dataset != nullptr);
    DORIS_CHECK(_fts_query_context == nullptr);
    DORIS_CHECK(_scan_params != nullptr);
    const auto& full_text =
            _scan_params->lance_scan_params.external_search_request.search_query.full_text_search;
    if (full_text.__isset.global_statistics) {
        return Status::NotSupported(
                "Lance FE-provided FTS global statistics require a lance-c consumer API");
    }
    const auto coverage_mode = full_text.coverage_mode == TFtsCoverageMode::STRICT
                                       ? LANCE_FTS_COVERAGE_STRICT
                                       : LANCE_FTS_COVERAGE_INDEX_ONLY;
    // Keep statistics preparation at the reader/scanner lifetime today. A future FE-provided
    // opaque statistics payload should enter through this boundary and create the same context,
    // leaving segment-scoped scanner execution unchanged.
    if (full_text.query_type == TFtsQueryType::MATCH) {
        const auto match_operator = full_text.match_operator == TFtsMatchOperator::AND
                                            ? LANCE_FTS_MATCH_OPERATOR_AND
                                            : LANCE_FTS_MATCH_OPERATOR_OR;
        _fts_query_context = lance_dataset_prepare_fts_match_query(
                _dataset, full_text.column.c_str(), full_text.query.c_str(), match_operator,
                static_cast<uint32_t>(full_text.max_fuzzy_distance), coverage_mode);
    } else {
        DORIS_CHECK(full_text.query_type == TFtsQueryType::PHRASE);
        _fts_query_context = lance_dataset_prepare_fts_phrase_query(
                _dataset, full_text.column.c_str(), full_text.query.c_str(), full_text.phrase_slop,
                coverage_mode);
    }
    if (_fts_query_context == nullptr) {
        return lance_error("prepare Lance FTS query context");
    }
    return Status::OK();
}

void LanceTableReader::_init_scanner_profile() {
    if (_scanner_configure_time != nullptr) {
        return;
    }

    _scanner_configure_time = ADD_CHILD_TIMER_WITH_LEVEL(
            _scanner_profile, "LanceScannerConfigureTime", LANCE_READER_PROFILE, 1);
    _runtime_filter_sql_time = ADD_CHILD_TIMER_WITH_LEVEL(
            _scanner_profile, "LanceRuntimeFilterSqlTime", LANCE_READER_PROFILE, 1);
    _scanner_read_time = ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LanceScannerReadTime",
                                                    LANCE_READER_PROFILE, 1);
    _execution_iops = ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceExecutionIOOps",
                                                   TUnit::UNIT, LANCE_READER_PROFILE, 1);
    _execution_requests = ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceExecutionIORequests",
                                                       TUnit::UNIT, LANCE_READER_PROFILE, 1);
    _execution_bytes_read = ADD_CHILD_COUNTER_WITH_LEVEL(
            _scanner_profile, "LanceExecutionIOBytesRead", TUnit::BYTES, LANCE_READER_PROFILE, 1);
    _index_partition_cache_miss_loads =
            ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceIndexPartitionCacheMissLoads",
                                         TUnit::UNIT, LANCE_READER_PROFILE, 1);
    _index_comparisons = ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceIndexComparisons",
                                                      TUnit::UNIT, LANCE_READER_PROFILE, 1);

    // Prefilter counters isolate row-id materialization. The generic scan counts below come
    // from Lance's FilteredRead execution node and are scan inputs, not ANN result counts.
    _lance_count_metrics = {
            {"prefilter_loads",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LancePrefilterLoads", TUnit::UNIT,
                                          LANCE_READER_PROFILE, 1)},
            {"prefilter_input_rows",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LancePrefilterInputRows", TUnit::UNIT,
                                          LANCE_READER_PROFILE, 1)},
            {"prefilter_input_batches",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LancePrefilterInputBatches",
                                          TUnit::UNIT, LANCE_READER_PROFILE, 1)},
            {"prefilter_row_ids",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LancePrefilterRowIds", TUnit::UNIT,
                                          LANCE_READER_PROFILE, 1)},
            {"fragments_scanned",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceFragmentsScanned", TUnit::UNIT,
                                          LANCE_READER_PROFILE, 1)},
            {"ranges_scanned",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceRowOffsetRangesScanned",
                                          TUnit::UNIT, LANCE_READER_PROFILE, 1)},
            {"rows_scanned", ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceRowsScanned",
                                                          TUnit::UNIT, LANCE_READER_PROFILE, 1)},
            {"partitions_ranked",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceIVFPartitionsRanked", TUnit::UNIT,
                                          LANCE_READER_PROFILE, 1)},
            {"partitions_searched",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceIVFPartitionsSearched",
                                          TUnit::UNIT, LANCE_READER_PROFILE, 1)},
            {"deltas_searched",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceVectorIndexSegmentsSearched",
                                          TUnit::UNIT, LANCE_READER_PROFILE, 1)},
            {"scalar_segments_requested",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceScalarIndexSegmentsRequested",
                                          TUnit::UNIT, LANCE_READER_PROFILE, 1)},
            {"scalar_segments_searched",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceScalarIndexSegmentsSearched",
                                          TUnit::UNIT, LANCE_READER_PROFILE, 1)},
            {"scalar_segment_fallbacks",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceScalarIndexSegmentFallbacks",
                                          TUnit::UNIT, LANCE_READER_PROFILE, 1)},
            {"scalar_segment_candidate_rows",
             ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LanceScalarIndexCandidateRows",
                                          TUnit::UNIT, LANCE_READER_PROFILE, 1)},
    };
    _lance_time_metrics = {
            // These are wall times in the ANN row-id loader. LoadTime includes input polling
            // and set construction; it must not be added to its component timers.
            {"prefilter_load_time",
             ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LancePrefilterLoadTime",
                                        LANCE_READER_PROFILE, 1)},
            {"prefilter_input_time",
             ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LancePrefilterInputTime",
                                        LANCE_READER_PROFILE, 1)},
            {"prefilter_build_time",
             ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LancePrefilterBuildTime",
                                        LANCE_READER_PROFILE, 1)},

            // This is wait time reported by the same Lance scan execution node described above,
            // rather than Doris scanner scheduling wait time.
            {"task_wait_time", ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LanceTaskWaitTime",
                                                          LANCE_READER_PROFILE, 1)},
            {"find_partitions_elapsed",
             ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LanceIVFPartitionRankingTime",
                                        LANCE_READER_PROFILE, 1)},
            {"scalar_segment_prepare_time",
             ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LanceScalarIndexSegmentPrepareTime",
                                        LANCE_READER_PROFILE, 1)},
            {"scalar_segment_search_time",
             ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LanceScalarIndexSegmentSearchTime",
                                        LANCE_READER_PROFILE, 1)},
    };
    if (_search_kind != SearchKind::NORMAL) {
        _planned_index_segment_count =
                ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LancePlannedIndexSegmentCount",
                                             TUnit::UNIT, LANCE_READER_PROFILE, 1);
        _planned_indexed_fragment_count =
                ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "LancePlannedIndexedFragmentCount",
                                             TUnit::UNIT, LANCE_READER_PROFILE, 1);
        _planned_flat_search_fragment_count = ADD_CHILD_COUNTER_WITH_LEVEL(
                _scanner_profile, "LancePlannedFlatSearchFragmentCount", TUnit::UNIT,
                LANCE_READER_PROFILE, 1);
    }
}

Status LanceTableReader::_open_scanner(const TFileRangeDesc& range) {
    _init_scanner_profile();
    SCOPED_TIMER(_scanner_configure_time);
    std::vector<const char*> columns;
    columns.reserve(_projected_columns.size() + 1);
    for (size_t idx = 0; idx < _projected_columns.size(); ++idx) {
        if (_record_batch_converter.is_global_rowid_output(idx)) {
            continue;
        }
        const auto& column = _projected_columns[idx];
        columns.emplace_back(column.name.c_str());
    }
    if (_search_kind != SearchKind::NORMAL && columns.empty()) {
        // Keep an explicit empty user projection from becoming `nullptr`, which means all dataset
        // columns to lance-c. Search execution already returns its generated result column.
        columns.emplace_back(_search_kind == SearchKind::VECTOR ? LANCE_DISTANCE_COLUMN.data()
                                                                : LANCE_SCORE_COLUMN.data());
    }
    columns.emplace_back(nullptr);

    const auto& lance_scan_params = _scan_params->lance_scan_params;
    std::string sql_filter;
    std::shared_ptr<const LanceRuntimeFilterSql> runtime_filter_sql;
    if (_search_kind == SearchKind::NORMAL) {
        if (has_lance_runtime_filters(_conjuncts)) {
            if (_dataset_schema == nullptr) {
                RETURN_IF_ERROR(import_lance_dataset_schema(_dataset, &_dataset_schema));
            }
            SCOPED_TIMER(_runtime_filter_sql_time);
            runtime_filter_sql = get_or_create_lance_runtime_filter_sql(
                    _conjuncts, *_dataset_schema, _runtime_filter_cache);
        }
    } else {
        const auto& request = lance_scan_params.external_search_request;
        if (request.__isset.search_filter &&
            request.search_filter.format == TSearchFilterFormat::SQL) {
            sql_filter = request.search_filter.payload;
        }
    }
    LanceScanner* scanner = lance_scanner_new(_dataset, columns.data(),
                                              sql_filter.empty() ? nullptr : sql_filter.c_str());
    if (scanner == nullptr) {
        return lance_error("create Lance scanner");
    }
    std::unique_ptr<LanceScanner, LanceScannerDeleter> scanner_guard(scanner);
    const auto collect_scan_statistics = [](void* callback_ctx,
                                            const LanceScanStatistics* statistics) {
        LanceTableReader::_collect_scan_statistics(callback_ctx, statistics);
    };
    if (lance_scanner_set_statistics_callback(scanner, collect_scan_statistics, this) != 0) {
        return lance_error("set Lance scanner statistics callback");
    }

    if (_record_batch_converter.requires_global_rowid() &&
        lance_scanner_with_row_id(scanner, true) != 0) {
        return lance_error("enable Lance row id output");
    }

    if (lance_scan_params.__isset.lance_substrait_filter &&
        lance_scanner_set_substrait_filter(
                scanner,
                reinterpret_cast<const uint8_t*>(lance_scan_params.lance_substrait_filter.data()),
                lance_scan_params.lance_substrait_filter.size()) != 0) {
        return lance_error("set Lance Substrait filter");
    }
    if (runtime_filter_sql != nullptr) {
        if (!runtime_filter_sql->expression.empty()) {
            if (lance_scanner_additional_sql_filter(scanner,
                                                    runtime_filter_sql->expression.c_str()) != 0) {
                return lance_error("set Lance additional SQL filter");
            }
        }
        record_lance_runtime_filter_pushdown(_scanner_profile, *runtime_filter_sql);
    }

    const auto batch_size = _batch_size > 0 ? _batch_size : _runtime_state->batch_size();
    if (lance_scanner_set_batch_size(scanner, static_cast<int64_t>(batch_size)) != 0) {
        return lance_error("set Lance scanner batch size");
    }
    RETURN_IF_ERROR(_configure_scan_options(scanner));

    const auto& lance_params = range.table_format_params.lance_params;
    switch (_search_kind) {
    case SearchKind::NORMAL:
        RETURN_IF_ERROR(_configure_normal_scan(scanner, lance_params));
        break;
    case SearchKind::VECTOR:
        RETURN_IF_ERROR(_configure_vector_search(scanner, lance_params));
        break;
    case SearchKind::FULL_TEXT:
        RETURN_IF_ERROR(_configure_full_text_search(scanner, lance_params));
        break;
    }
    _scanner = scanner_guard.release();
    _scanner_batch_size = batch_size;
    return Status::OK();
}

Status LanceTableReader::_configure_scan_options(LanceScanner* scanner) const {
    DORIS_CHECK(scanner != nullptr);
    // Doris runs multiple scanners concurrently. Limit each scanner's read-ahead;
    // the I/O budget does not cap its total memory usage.
    const auto io_buffer_size = static_cast<uint64_t>(config::lance_io_buffer_size_bytes);
    const auto batch_readahead = static_cast<size_t>(config::lance_batch_readahead);
    const auto fragment_readahead = static_cast<size_t>(config::lance_fragment_readahead);
    constexpr bool scan_in_order = false;

    if (lance_scanner_set_io_buffer_size(scanner, io_buffer_size) != 0) {
        return lance_error("set Lance scanner I/O buffer size");
    }
    if (lance_scanner_set_batch_readahead(scanner, batch_readahead) != 0) {
        return lance_error("set Lance scanner batch readahead");
    }
    if (lance_scanner_set_fragment_readahead(scanner, fragment_readahead) != 0) {
        return lance_error("set Lance scanner fragment readahead");
    }
    // Storage order is not required; query ordering is enforced by Sort/TopN operators.
    if (lance_scanner_set_scan_in_order(scanner, scan_in_order) != 0) {
        return lance_error("set Lance scanner scan order");
    }

    return Status::OK();
}

Status LanceTableReader::_configure_normal_scan(LanceScanner* scanner,
                                                const TLanceFileDesc& lance_params) const {
    DORIS_CHECK(scanner != nullptr);
    std::vector<uint64_t> fragment_ids;
    RETURN_IF_ERROR(parse_fragment_ids(lance_params, &fragment_ids));
    if (!fragment_ids.empty() &&
        lance_scanner_set_fragment_ids(scanner, fragment_ids.data(), fragment_ids.size()) != 0) {
        return lance_error("set Lance scanner fragment ids");
    }
    std::vector<uint8_t> segment_uuids;
    size_t segment_count = 0;
    RETURN_IF_ERROR(parse_index_segment_uuids(lance_params, &segment_uuids, &segment_count));
    if (segment_count > 1) {
        return Status::InvalidArgument("normal Lance scan accepts only one scalar index segment");
    }
    if (segment_count == 1) {
        if (fragment_ids.empty() || !lance_params.__isset.version || lance_params.version <= 0) {
            return Status::InvalidArgument(
                    "Lance scalar index segment requires a fixed version and nonempty fragment "
                    "ids");
        }
        if (lance_params.__isset.use_scalar_index && !lance_params.use_scalar_index) {
            return Status::InvalidArgument(
                    "Lance scalar index segment cannot be combined with use_scalar_index=false");
        }
        if (lance_scanner_set_scalar_index_segment(scanner, segment_uuids.data()) != 0) {
            return lance_error("set Lance scanner scalar index segment");
        }
    } else if (lance_params.__isset.use_scalar_index &&
               lance_scanner_set_use_scalar_index(scanner, lance_params.use_scalar_index) != 0) {
        return lance_error("set Lance scanner scalar index usage");
    }
    // FE sets this only when every predicate has been pushed into Lance.
    if (lance_params.__isset.limit && lance_params.limit > 0 &&
        lance_scanner_set_limit(scanner, lance_params.limit) != 0) {
        return lance_error("set Lance scanner limit");
    }
    return Status::OK();
}

Status LanceTableReader::_configure_vector_search(LanceScanner* scanner,
                                                  const TLanceFileDesc& lance_params) const {
    DORIS_CHECK(scanner != nullptr);
    DORIS_CHECK(_scan_params != nullptr);
    DORIS_CHECK(_scan_params->__isset.lance_scan_params);
    std::vector<uint64_t> fragment_ids;
    RETURN_IF_ERROR(parse_fragment_ids(lance_params, &fragment_ids));
    if (!fragment_ids.empty() &&
        lance_scanner_set_fragment_ids(scanner, fragment_ids.data(), fragment_ids.size()) != 0) {
        return lance_error("set Lance vector scanner fragment ids");
    }
    std::vector<uint8_t> segment_uuids;
    size_t segment_count = 0;
    RETURN_IF_ERROR(parse_index_segment_uuids(lance_params, &segment_uuids, &segment_count));
    if (segment_count > 0 &&
        lance_scanner_set_index_segments(scanner, segment_uuids.data(), segment_count) != 0) {
        return lance_error("set Lance vector scanner index segments");
    }
    // Fragment-scoped nearest queries require prefiltering before installing the query. The same
    // path applies the TVF search filter, when present.
    if (lance_scanner_set_prefilter(scanner, true) != 0) {
        return lance_error("enable Lance vector prefilter");
    }
    const auto& lance_scan_params = _scan_params->lance_scan_params;
    DORIS_CHECK(lance_scan_params.__isset.external_search_request);
    const auto& request = lance_scan_params.external_search_request;
    const auto& vector = request.search_query.vector_search;
    const auto& query = vector.query_vector;
    const auto dimension = static_cast<size_t>(query.dimension);
    const auto count = query.__isset.num_vectors ? static_cast<size_t>(query.num_vectors) : 1;
    const auto num_elements = dimension * count;
    const auto* bytes = query.values.data();
    const auto candidate_k = static_cast<uint32_t>(vector.top_k + vector.offset);

    const auto set_nearest = [&](const void* values, LanceDataType type) -> Status {
        const int result =
                query.__isset.num_vectors
                        ? lance_scanner_nearest_multivector(scanner, vector.column.c_str(), values,
                                                            dimension, count, type, candidate_k)
                        : lance_scanner_nearest(scanner, vector.column.c_str(), values, dimension,
                                                type, candidate_k);
        if (result != 0) {
            return lance_error("set Lance nearest query");
        }
        return Status::OK();
    };

    switch (query.element_type) {
    case TVectorElementType::FLOAT16: {
        std::vector<uint16_t> values(num_elements);
        for (size_t i = 0; i < num_elements; ++i) {
            values[i] = LittleEndian::Load16(bytes + i * sizeof(uint16_t));
        }
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_FLOAT16));
        break;
    }
    case TVectorElementType::FLOAT32: {
        std::vector<float> values(num_elements);
        for (size_t i = 0; i < num_elements; ++i) {
            const auto bits = LittleEndian::Load32(bytes + i * sizeof(uint32_t));
            values[i] = std::bit_cast<float>(bits);
        }
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_FLOAT32));
        break;
    }
    case TVectorElementType::FLOAT64: {
        std::vector<double> values(num_elements);
        for (size_t i = 0; i < num_elements; ++i) {
            const auto bits = LittleEndian::Load64(bytes + i * sizeof(uint64_t));
            values[i] = std::bit_cast<double>(bits);
        }
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_FLOAT64));
        break;
    }
    case TVectorElementType::UINT8: {
        std::vector<uint8_t> values(num_elements);
        std::memcpy(values.data(), bytes, num_elements);
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_UINT8));
        break;
    }
    case TVectorElementType::INT8: {
        std::vector<int8_t> values(num_elements);
        std::memcpy(values.data(), bytes, num_elements);
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_INT8));
        break;
    }
    default:
        return Status::NotSupported("unsupported Lance query vector element type: {}",
                                    static_cast<int>(query.element_type));
    }

    {
        // FE plans an omitted metric as L2; never let indexed splits choose another default.
        const auto requested_metric =
                !vector.__isset.metric || vector.metric == TVectorMetric::DEFAULT
                        ? TVectorMetric::L2
                        : vector.metric;
        LanceMetricType metric;
        switch (requested_metric) {
        case TVectorMetric::L2:
            metric = LANCE_METRIC_L2;
            break;
        case TVectorMetric::COSINE:
            metric = LANCE_METRIC_COSINE;
            break;
        case TVectorMetric::DOT_PRODUCT:
            metric = LANCE_METRIC_DOT;
            break;
        case TVectorMetric::HAMMING:
            metric = LANCE_METRIC_HAMMING;
            break;
        default:
            return Status::NotSupported("unsupported Lance vector metric: {}",
                                        static_cast<int>(vector.metric));
        }
        if (lance_scanner_set_metric(scanner, metric) != 0) {
            return lance_error("set Lance vector metric");
        }
    }

    // Exact refinement validates stored elements and gives indexed and unindexed candidates
    // the same row-level score before either path truncates its results.
    if (query.__isset.num_vectors && lance_scanner_set_refine_factor(scanner, 1) != 0) {
        return lance_error("enable Lance multi-vector refinement");
    }
    if (request.__isset.vector_search_options) {
        const auto& options = request.vector_search_options;
        if (options.__isset.nprobes &&
            lance_scanner_set_nprobes(scanner, static_cast<uint32_t>(options.nprobes)) != 0) {
            return lance_error("set Lance vector nprobes");
        }
        if (options.__isset.refine_factor &&
            lance_scanner_set_refine_factor(scanner,
                                            static_cast<uint32_t>(options.refine_factor)) != 0) {
            return lance_error("set Lance vector refine factor");
        }
        if (options.__isset.ef &&
            lance_scanner_set_ef(scanner, static_cast<uint32_t>(options.ef)) != 0) {
            return lance_error("set Lance vector ef");
        }
        if (options.__isset.use_index &&
            lance_scanner_set_use_index(scanner, options.use_index) != 0) {
            return lance_error("set Lance vector use_index");
        }
    }
    if (lance_scanner_set_offset(scanner, vector.offset) != 0) {
        return lance_error("set Lance vector offset");
    }
    if (lance_scanner_set_limit(scanner, vector.top_k) != 0) {
        return lance_error("set Lance vector result limit");
    }
    const auto fragment_count = static_cast<int64_t>(fragment_ids.size());
    if (segment_count > 0) {
        COUNTER_UPDATE(_planned_index_segment_count, static_cast<int64_t>(segment_count));
        COUNTER_UPDATE(_planned_indexed_fragment_count, fragment_count);
    } else {
        COUNTER_UPDATE(_planned_flat_search_fragment_count, fragment_count);
    }
    return Status::OK();
}

Status LanceTableReader::_configure_full_text_search(LanceScanner* scanner,
                                                     const TLanceFileDesc& lance_params) const {
    DORIS_CHECK(scanner != nullptr);
    DORIS_CHECK(_fts_query_context != nullptr);
    DORIS_CHECK(_scan_params != nullptr);
    // FTS fragment IDs describe the selected segment's coverage for planning and profiling. They
    // are not installed as a generic fragment filter because lance-c rejects combining one with a
    // prepared FTS context; the segment UUID is the execution boundary.
    std::vector<uint64_t> fragment_ids;
    RETURN_IF_ERROR(parse_fragment_ids(lance_params, &fragment_ids));
    std::vector<uint8_t> segment_uuids;
    size_t segment_count = 0;
    RETURN_IF_ERROR(parse_index_segment_uuids(lance_params, &segment_uuids, &segment_count));
    if (segment_count == 0) {
        return Status::InvalidArgument(
                "Lance full-text search split requires at least one FTS index segment UUID");
    }
    const auto& full_text =
            _scan_params->lance_scan_params.external_search_request.search_query.full_text_search;
    if (lance_scanner_set_fts_query_context(scanner, _fts_query_context) != 0) {
        return lance_error("attach Lance FTS query context");
    }
    if (lance_scanner_set_fts_index_segments(scanner, segment_uuids.data(), segment_count) != 0) {
        return lance_error("set Lance FTS scanner index segments");
    }
    if (lance_scanner_set_limit(scanner, full_text.top_k) != 0) {
        return lance_error("set Lance FTS scanner candidate limit");
    }
    COUNTER_UPDATE(_planned_index_segment_count, static_cast<int64_t>(segment_count));
    COUNTER_UPDATE(_planned_indexed_fragment_count, static_cast<int64_t>(fragment_ids.size()));
    return Status::OK();
}

void LanceTableReader::_collect_scan_statistics(void* callback_ctx, const void* opaque_statistics) {
    const auto* statistics = static_cast<const LanceScanStatistics*>(opaque_statistics);
    if (callback_ctx == nullptr || statistics == nullptr) {
        LOG(WARNING) << "Lance scan statistics callback received a null argument";
        return;
    }

    auto* reader = static_cast<LanceTableReader*>(callback_ctx);
    const auto update_counter = [](RuntimeProfile::Counter* counter, uint64_t value,
                                   std::string_view metric_name) {
        if (counter == nullptr) {
            return;
        }
        if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            LOG(WARNING) << "Ignoring Lance scan metric '" << metric_name << "' with value "
                         << value << " because it exceeds INT64_MAX";
            return;
        }
        COUNTER_UPDATE(counter, static_cast<int64_t>(value));
    };

    update_counter(reader->_execution_iops, statistics->iops, "iops");
    update_counter(reader->_execution_requests, statistics->requests, "requests");
    update_counter(reader->_execution_bytes_read, statistics->bytes_read, "bytes_read");
    update_counter(reader->_index_partition_cache_miss_loads, statistics->index_partitions_loaded,
                   "index_partitions_loaded");
    update_counter(reader->_index_comparisons, statistics->index_comparisons, "index_comparisons");

    if (statistics->metrics_len != 0 && statistics->metrics == nullptr) {
        LOG(WARNING) << "Ignoring malformed Lance scan statistics: metrics is NULL while "
                     << "metrics_len is " << statistics->metrics_len;
        return;
    }
    for (size_t index = 0; index < statistics->metrics_len; ++index) {
        const auto& metric = statistics->metrics[index];
        if (metric.name_len != 0 && metric.name == nullptr) {
            LOG(WARNING) << "Ignoring malformed Lance scan metric at index " << index
                         << ": name is NULL while name_len is " << metric.name_len;
            continue;
        }
        const std::string_view name(metric.name == nullptr ? "" : metric.name, metric.name_len);
        RuntimeProfile::Counter* counter = nullptr;
        switch (metric.kind) {
        case LANCE_SCAN_METRIC_COUNT: {
            const auto found = reader->_lance_count_metrics.find(name);
            if (found != reader->_lance_count_metrics.end()) {
                counter = found->second;
            }
            break;
        }
        case LANCE_SCAN_METRIC_TIME_NANOSECONDS: {
            const auto found = reader->_lance_time_metrics.find(name);
            if (found != reader->_lance_time_metrics.end()) {
                counter = found->second;
            } else if (name == "search_time") {
                // Scalar-index metrics exist only when Lance includes the corresponding
                // execution node in this scan plan.
                counter = ADD_CHILD_TIMER_WITH_LEVEL(reader->_scanner_profile,
                                                     "LanceScalarIndexQueryTime",
                                                     LANCE_READER_PROFILE, 1);
            } else if (name == "serialization_time") {
                counter = ADD_CHILD_TIMER_WITH_LEVEL(reader->_scanner_profile,
                                                     "LanceScalarIndexResultSerializationTime",
                                                     LANCE_READER_PROFILE, 1);
            }
            break;
        }
        default:
            break;
        }
        if (counter != nullptr) {
            update_counter(counter, metric.value, name);
        }
    }
}

void LanceTableReader::_close_scanner() {
    if (_scanner != nullptr) {
        lance_scanner_close(_scanner);
        _scanner = nullptr;
    }
    _scanner_batch_size = 0;
}

void LanceTableReader::_close_dataset() {
    if (_fts_query_context != nullptr) {
        lance_fts_query_context_close(_fts_query_context);
        _fts_query_context = nullptr;
    }
    if (_dataset != nullptr) {
        _collect_data_cache_statistics();
        lance_dataset_close(_dataset);
        _dataset = nullptr;
    }
    _dataset_schema.reset();
    _record_batch_converter.reset_schema();
}

void LanceTableReader::_collect_data_cache_statistics() {
    if (_dataset == nullptr || (_data_cache_bytes_read_from_cache == nullptr &&
                                _data_cache_bytes_read_from_remote == nullptr)) {
        return;
    }

    LanceDataCacheStatistics statistics {};
    if (lance_dataset_get_data_cache_statistics(_dataset, &statistics) != 0) {
        const auto status = lance_error("get Lance data cache statistics");
        LOG(WARNING) << "Failed to collect Lance data cache statistics: " << status.to_string();
        return;
    }

    const auto set_counter = [](RuntimeProfile::Counter* counter, uint64_t value,
                                std::string_view metric_name) {
        if (counter == nullptr) {
            return;
        }
        if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            LOG(WARNING) << "Ignoring Lance data cache metric '" << metric_name << "' with value "
                         << value << " because it exceeds INT64_MAX";
            return;
        }
        COUNTER_SET(counter, static_cast<int64_t>(value));
    };
    set_counter(_data_cache_bytes_read_from_cache, statistics.bytes_read_from_cache,
                "bytes_read_from_cache");
    set_counter(_data_cache_bytes_read_from_remote, statistics.bytes_read_from_remote,
                "bytes_read_from_remote");
}

Status LanceTableReader::_fill_block_from_lance_batch(LanceBatch* batch, Block* block,
                                                      size_t* rows) {
    DORIS_CHECK(batch != nullptr);
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(rows != nullptr);
    ArrowArray array {};
    ArrowSchema schema {};
    if (lance_batch_to_arrow(batch, &array, &schema) != 0) {
        return lance_error("export Lance batch to Arrow");
    }
    auto result = arrow::ImportRecordBatch(&array, &schema);
    if (!result.ok()) {
        if (array.release != nullptr) {
            array.release(&array);
        }
        if (schema.release != nullptr) {
            schema.release(&schema);
        }
        return Status::InternalError("import Lance Arrow batch failed: {}",
                                     result.status().message());
    }

    return _record_batch_converter.convert_record_batch_to_block(
            std::move(result).ValueUnsafe(), block, _global_rowid_context, rows);
}

Status LanceTableReader::_dataset_key(const TFileRangeDesc& range, DatasetKey* key) const {
    const auto& params = range.table_format_params.lance_params;
    key->uri = params.dataset_uri;
    key->version = params.version;
    return build_lance_storage_options(_scan_params, &key->storage_options);
}

} // namespace doris::format::lance
