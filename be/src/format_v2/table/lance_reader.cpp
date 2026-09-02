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
#include <unordered_set>

#include "common/consts.h"
#include "common/logging.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "exec/common/endian.h"
#include "format_v2/lance/lance_reader_helper.h"
#include "format_v2/lance/lance_runtime_filter_helper.h"
#include "runtime/file_scan_profile.h"
#include "storage/utils.h"

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

    ArrowSchema arrow_schema {};
    if (lance_dataset_schema(dataset.get(), &arrow_schema) != 0) {
        return lance_error("get Lance dataset schema");
    }
    auto imported_schema = arrow::ImportSchema(&arrow_schema);
    if (!imported_schema.ok()) {
        if (arrow_schema.release != nullptr) {
            arrow_schema.release(&arrow_schema);
        }
        return Status::InternalError("import Lance Arrow schema failed: {}",
                                     imported_schema.status().message());
    }

    return convert_arrow_schema_to_doris(std::move(imported_schema).ValueUnsafe(), column_names,
                                         column_types);
}

Status LanceTableReader::init(TableReadOptions&& options) {
    RETURN_IF_ERROR(TableReader::init(std::move(options)));
    DORIS_CHECK(_runtime_state != nullptr);
    DORIS_CHECK(_scanner_profile != nullptr);
    DORIS_CHECK(_scan_params != nullptr);
    RETURN_IF_ERROR(_resolve_search_kind());

    _ctz = _runtime_state->timezone_obj();
    const auto& lance_scan_params = _scan_params->lance_scan_params;
    ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, LANCE_READER_PROFILE,
                               file_scan_profile::TABLE_READER, 1);
    _dataset_open_time = ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LanceDatasetOpenTime",
                                                    LANCE_READER_PROFILE, 1);
    _scanner_configure_time = ADD_CHILD_TIMER_WITH_LEVEL(
            _scanner_profile, "LanceScannerConfigureTime", LANCE_READER_PROFILE, 1);
    _scanner_read_time = ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LanceScannerReadTime",
                                                    LANCE_READER_PROFILE, 1);
    _arrow_to_doris_block_time = ADD_CHILD_TIMER_WITH_LEVEL(
            _scanner_profile, "LanceArrowToDorisBlockTime", LANCE_READER_PROFILE, 1);
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
    // These scan counts are emitted by Lance's FilteredRead execution node. For vector searches
    // with an explicit fragment set, they normally describe the fragments, ranges, and rows read
    // while applying the row-id prefilter. They are scan input counts, not ANN result counts.
    _lance_count_metrics = {
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
    };
    _lance_time_metrics = {
            // This is wait time reported by the same Lance scan execution node described above,
            // rather than Doris scanner scheduling wait time.
            {"task_wait_time", ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LanceTaskWaitTime",
                                                          LANCE_READER_PROFILE, 1)},
            {"find_partitions_elapsed",
             ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "LanceIVFPartitionRankingTime",
                                        LANCE_READER_PROFILE, 1)},
    };
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
        }
        _scanner_profile->add_info_string("LanceTopK", std::to_string(top_k));
        _scanner_profile->add_info_string("LanceOffset", std::to_string(offset));
        _scanner_profile->add_info_string("LanceTopKPlusOffset", std::to_string(top_k + offset));
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
    if (_scan_params->__isset.lance_scan_params &&
        lance_scan_params.__isset.lance_substrait_filter) {
        _scanner_profile->add_info_string("LancePushdownFormat", "SUBSTRAIT");
        _scanner_profile->add_info_string(
                "LanceSubstraitFilterBytes",
                std::to_string(lance_scan_params.lance_substrait_filter.size()));
    }

    _output_name_to_idx.clear();
    _output_name_to_idx.reserve(_projected_columns.size());
    _global_rowid_output_idx.reset();
    for (size_t idx = 0; idx < _projected_columns.size(); ++idx) {
        const auto& column = _projected_columns[idx];
        if (column.type == nullptr) {
            return Status::InvalidArgument("Lance projected column '{}' has no type", column.name);
        }
        if (column.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            if (_search_kind == SearchKind::NORMAL) {
                return Status::NotSupported(
                        "Lance global row id is currently supported only for external search");
            }
            if (_global_rowid_output_idx.has_value()) {
                return Status::InvalidArgument("duplicate Lance global row id projected column: {}",
                                               column.name);
            }
            if (remove_nullable(column.type)->get_primitive_type() != TYPE_STRING) {
                return Status::InvalidArgument(
                        "Lance global row id column '{}' must have Doris STRING type, but was {}",
                        column.name, column.type->get_name());
            }
            _global_rowid_output_idx = idx;
            continue;
        }
        if (!_output_name_to_idx.emplace(column.name, idx).second) {
            return Status::InvalidArgument("duplicate Lance projected column: {}", column.name);
        }
        if (_search_kind == SearchKind::VECTOR && column.name == LANCE_DISTANCE_COLUMN) {
            const auto distance_type = remove_nullable(column.type);
            if (distance_type->get_primitive_type() != TYPE_FLOAT) {
                return Status::InvalidArgument(
                        "Lance vector search column '{}' must have Doris FLOAT type, but was {}",
                        LANCE_DISTANCE_COLUMN, column.type->get_name());
            }
        } else if (_search_kind == SearchKind::FULL_TEXT && column.name == LANCE_SCORE_COLUMN) {
            const auto score_type = remove_nullable(column.type);
            if (score_type->get_primitive_type() != TYPE_FLOAT) {
                return Status::InvalidArgument(
                        "Lance full-text search column '{}' must have Doris FLOAT type, but was {}",
                        LANCE_SCORE_COLUMN, column.type->get_name());
            }
        }
    }
    return Status::OK();
}

Status LanceTableReader::prepare_split(const SplitReadOptions& options) {
    _close_scanner();
    _eof = false;
    _runtime_filter_cache = options.cache;

    RETURN_IF_ERROR(TableReader::prepare_split(options));
    // Lance does not currently provide metadata aggregate pushdown. Do not let a generic
    // table-level count supplied by a future planner bypass fragment reads.
    _remaining_table_level_count = -1;
    if (current_split_pruned()) {
        return Status::OK();
    }
    if (_global_rowid_output_idx.has_value() && !_global_rowid_context.has_value()) {
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
            RETURN_IF_ERROR(_fill_block_from_record_batch(record_batch, block, &rows));
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
        const auto dimension = static_cast<size_t>(query_vector.dimension);
        if (dimension > std::numeric_limits<size_t>::max() / element_width ||
            query_vector.values.size() != dimension * element_width) {
            return Status::InvalidArgument(
                    "Lance query vector byte size {} does not match dimension {} and element width "
                    "{}",
                    query_vector.values.size(), dimension, element_width);
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

    {
        SCOPED_TIMER(_dataset_open_time);
        _dataset = lance_dataset_open(
                key.uri.c_str(), key.storage_options.empty() ? nullptr : storage_option_ptrs.data(),
                static_cast<uint64_t>(key.version));
    }
    if (_dataset == nullptr) {
        return lance_error("open Lance dataset");
    }
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
    _fts_query_context = lance_dataset_prepare_fts_query(_dataset, full_text.column.c_str(),
                                                         full_text.query.c_str(), 0, coverage_mode);
    if (_fts_query_context == nullptr) {
        return lance_error("prepare Lance FTS query context");
    }
    return Status::OK();
}

Status LanceTableReader::_open_scanner(const TFileRangeDesc& range) {
    SCOPED_TIMER(_scanner_configure_time);
    std::vector<const char*> columns;
    columns.reserve(_projected_columns.size() + 1);
    for (size_t idx = 0; idx < _projected_columns.size(); ++idx) {
        if (_global_rowid_output_idx == idx) {
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
        runtime_filter_sql =
                get_or_create_lance_runtime_filter_sql(_conjuncts, _runtime_filter_cache);
    } else {
        const auto& request = lance_scan_params.external_search_request;
        if (request.__isset.search_filter &&
            request.search_filter.format == TSearchFilterFormat::SQL) {
            sql_filter = request.search_filter.payload;
        }
    }
    LanceScanner* scanner =
            lance_scanner_new(_dataset, columns.size() == 1 ? nullptr : columns.data(),
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

    if (_global_rowid_output_idx.has_value() && lance_scanner_with_row_id(scanner, true) != 0) {
        return lance_error("enable Lance row id output");
    }

    if (lance_scan_params.__isset.lance_substrait_filter &&
        lance_scanner_set_substrait_filter(
                scanner,
                reinterpret_cast<const uint8_t*>(lance_scan_params.lance_substrait_filter.data()),
                lance_scan_params.lance_substrait_filter.size()) != 0) {
        return lance_error("set Lance Substrait filter");
    }
    if (runtime_filter_sql != nullptr && !runtime_filter_sql->expression.empty()) {
        if (lance_scanner_additional_sql_filter(scanner, runtime_filter_sql->expression.c_str()) !=
            0) {
            return lance_error("set Lance additional SQL filter");
        }
        record_lance_runtime_filter_pushdown(_scanner_profile, *runtime_filter_sql);
    }

    const auto batch_size = _batch_size > 0 ? _batch_size : _runtime_state->batch_size();
    if (lance_scanner_set_batch_size(scanner, static_cast<int64_t>(batch_size)) != 0) {
        return lance_error("set Lance scanner batch size");
    }

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

Status LanceTableReader::_configure_normal_scan(LanceScanner* scanner,
                                                const TLanceFileDesc& lance_params) const {
    DORIS_CHECK(scanner != nullptr);
    std::vector<uint64_t> fragment_ids;
    RETURN_IF_ERROR(parse_fragment_ids(lance_params, &fragment_ids));
    if (!fragment_ids.empty() &&
        lance_scanner_set_fragment_ids(scanner, fragment_ids.data(), fragment_ids.size()) != 0) {
        return lance_error("set Lance scanner fragment ids");
    }
    if (lance_params.__isset.index_segment_uuids && !lance_params.index_segment_uuids.empty()) {
        return Status::InvalidArgument("normal Lance scan cannot contain index segment UUIDs");
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
    const auto* bytes = query.values.data();
    const auto candidate_k = static_cast<uint32_t>(vector.top_k + vector.offset);

    const auto set_nearest = [&](const void* values, LanceDataType type) -> Status {
        if (lance_scanner_nearest(scanner, vector.column.c_str(), values, dimension, type,
                                  candidate_k) != 0) {
            return lance_error("set Lance nearest query");
        }
        return Status::OK();
    };

    switch (query.element_type) {
    case TVectorElementType::FLOAT16: {
        std::vector<uint16_t> values(dimension);
        for (size_t i = 0; i < dimension; ++i) {
            values[i] = LittleEndian::Load16(bytes + i * sizeof(uint16_t));
        }
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_FLOAT16));
        break;
    }
    case TVectorElementType::FLOAT32: {
        std::vector<float> values(dimension);
        for (size_t i = 0; i < dimension; ++i) {
            const auto bits = LittleEndian::Load32(bytes + i * sizeof(uint32_t));
            values[i] = std::bit_cast<float>(bits);
        }
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_FLOAT32));
        break;
    }
    case TVectorElementType::FLOAT64: {
        std::vector<double> values(dimension);
        for (size_t i = 0; i < dimension; ++i) {
            const auto bits = LittleEndian::Load64(bytes + i * sizeof(uint64_t));
            values[i] = std::bit_cast<double>(bits);
        }
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_FLOAT64));
        break;
    }
    case TVectorElementType::UINT8: {
        std::vector<uint8_t> values(dimension);
        std::memcpy(values.data(), bytes, dimension);
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_UINT8));
        break;
    }
    case TVectorElementType::INT8: {
        std::vector<int8_t> values(dimension);
        std::memcpy(values.data(), bytes, dimension);
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_INT8));
        break;
    }
    default:
        return Status::NotSupported("unsupported Lance query vector element type: {}",
                                    static_cast<int>(query.element_type));
    }

    if (vector.__isset.metric && vector.metric != TVectorMetric::DEFAULT) {
        LanceMetricType metric;
        switch (vector.metric) {
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
    if (lance_scanner_set_limit(scanner, full_text.top_k + full_text.offset) != 0) {
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
        lance_dataset_close(_dataset);
        _dataset = nullptr;
    }
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

    return _fill_block_from_record_batch(std::move(result).ValueUnsafe(), block, rows);
}

Status LanceTableReader::_append_global_row_ids(const std::shared_ptr<arrow::Array>& row_ids,
                                                MutableColumnPtr& output_column) const {
    DORIS_CHECK(row_ids != nullptr);
    DORIS_CHECK(_global_rowid_context.has_value());
    if (row_ids->type_id() != arrow::Type::UINT64) {
        return Status::InternalError("Lance row id column must be Arrow UINT64, but was {}",
                                     row_ids->type()->ToString());
    }

    ColumnString* data_column = nullptr;
    ColumnUInt8::Container* null_map = nullptr;
    if (auto* nullable = check_and_get_column<ColumnNullable>(*output_column)) {
        data_column = check_and_get_column<ColumnString>(nullable->get_nested_column());
        null_map = &nullable->get_null_map_data();
    } else {
        data_column = check_and_get_column<ColumnString>(*output_column);
    }
    if (data_column == nullptr) {
        return Status::InternalError("Lance global row id output column must be STRING");
    }

    const auto typed_row_ids = std::static_pointer_cast<arrow::UInt64Array>(row_ids);
    if (typed_row_ids->null_count() != 0) {
        return Status::InternalError("Lance returned null row id");
    }
    const auto row_count = static_cast<size_t>(typed_row_ids->length());
    if (null_map != nullptr) {
        null_map->resize_fill(null_map->size() + row_count, 0);
    }
    const auto& context = *_global_rowid_context;
    for (size_t row = 0; row < row_count; ++row) {
        const GlobalRowLoacationV2 location(ROW_VERSION::LANCE_DATASET_ROW_ID, context.backend_id,
                                            context.file_id, typed_row_ids->Value(row));
        data_column->insert_data(reinterpret_cast<const char*>(&location), sizeof(location));
    }
    return Status::OK();
}

Status LanceTableReader::_fill_block_from_record_batch(
        const std::shared_ptr<arrow::RecordBatch>& record_batch, Block* block, size_t* rows) {
    DORIS_CHECK(record_batch != nullptr);
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(rows != nullptr);
    const auto row_count = static_cast<size_t>(record_batch->num_rows());
    std::unordered_set<std::string> materialized_columns;
    materialized_columns.reserve(record_batch->num_columns());
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (int arrow_idx = 0; arrow_idx < record_batch->num_columns(); ++arrow_idx) {
        const auto& field = record_batch->schema()->field(arrow_idx);
        if (field->name() == LANCE_ROW_ID_COLUMN && _global_rowid_output_idx.has_value()) {
            const auto output_idx = *_global_rowid_output_idx;
            const auto& output_name = _projected_columns[output_idx].name;
            if (!materialized_columns.emplace(output_name).second) {
                return Status::InternalError("Lance returned duplicate column '{}'",
                                             LANCE_ROW_ID_COLUMN);
            }
            RETURN_IF_ERROR(
                    _append_global_row_ids(record_batch->column(arrow_idx), columns[output_idx]));
            continue;
        }
        const auto output_it = _output_name_to_idx.find(field->name());
        if (output_it == _output_name_to_idx.end()) {
            if ((_search_kind == SearchKind::VECTOR && field->name() == LANCE_DISTANCE_COLUMN) ||
                (_search_kind == SearchKind::FULL_TEXT && field->name() == LANCE_SCORE_COLUMN)) {
                // Lance auto-projects the generated search result column. It is valid for Doris
                // slot pruning to omit that optional result column.
                continue;
            }
            return Status::InternalError("Lance returned unknown column '{}'", field->name());
        }
        if (!materialized_columns.emplace(field->name()).second) {
            return Status::InternalError("Lance returned duplicate column '{}'", field->name());
        }
        const auto output_idx = output_it->second;
        try {
            RETURN_IF_ERROR(columns_guard.get_datatype_by_position(output_idx)
                                    ->get_serde()
                                    ->read_column_from_arrow(*columns[output_idx],
                                                             record_batch->column(arrow_idx).get(),
                                                             0, row_count, _ctz));
        } catch (const Exception& e) {
            return Status::InternalError("convert Lance Arrow column '{}' failed: {}",
                                         field->name(), e.what());
        }
    }
    for (const auto& column : _projected_columns) {
        if (!materialized_columns.contains(column.name)) {
            return Status::InternalError("Lance did not return requested column '{}'", column.name);
        }
    }
    *rows = row_count;
    return Status::OK();
}

Status LanceTableReader::_dataset_key(const TFileRangeDesc& range, DatasetKey* key) const {
    const auto& params = range.table_format_params.lance_params;
    key->uri = params.dataset_uri;
    key->version = params.version;
    return build_lance_storage_options(_scan_params, &key->storage_options);
}

} // namespace doris::format::lance
