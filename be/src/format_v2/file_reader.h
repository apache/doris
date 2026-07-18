// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "exprs/vexpr_fwd.h"
#include "format_v2/column_data.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/file_factory.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace doris {
class Block;
struct ConditionCacheContext;

namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::format {

class TableColumnMapper;
struct TableColumnMapperOptions;

enum class FileFormat {
    PARQUET,
    ORC,
    CSV,
    JSON,
    TEXT,
    JNI,
    NATIVE,
    ARROW,
};

struct FileScanRequest {
    virtual ~FileScanRequest() = default;

    std::string debug_string() const;

    // Columns that must be read before row-level filtering. They are materialized eagerly because
    // conjuncts/delete_conjuncts need them to decide the selected rows.
    std::vector<LocalColumnIndex> predicate_columns;
    // Columns read after row-level filtering. Predicate columns are also available for output and
    // should not be duplicated here.
    std::vector<LocalColumnIndex> non_predicate_columns;
    // file-local column id -> file-local output block position.
    std::map<LocalColumnId, LocalIndex> local_positions;
    // Row-level filters converted to file-local expressions from table-level predicates.
    VExprContextSPtrs conjuncts;
    // Delete predicates converted to file-local expressions. A TRUE result means that the row is
    // deleted, so readers must invert each result when building their keep filter.
    VExprContextSPtrs delete_conjuncts;
};

// Helper for constructing the scan-column layout in FileScanRequest.
// FileScanRequest keeps predicate and non-predicate columns separate because columnar readers such
// as Parquet can read predicate columns first, filter rows, and then lazily read the remaining
// projected columns. The two lists still share one file-local output block, whose positions are
// stored in local_positions. This builder centralizes the mechanical rules for that shared layout:
// - each root file column gets one stable block position;
// - predicate columns dominate non-predicate columns because they are already returned in the file
//   block and can be reused for final materialization;
// - repeated nested projections for the same root are merged instead of duplicated.
// TableColumnMapper should still own table-to-file semantic resolution. This helper only owns the
// FileScanRequest layout contract after a file-local projection has been produced.
class FileScanRequestBuilder {
public:
    explicit FileScanRequestBuilder(FileScanRequest* request) : _request(request) {
        DORIS_CHECK(_request != nullptr);
    }

    Status add_predicate_column(LocalColumnIndex projection) {
        return _add_column(std::move(projection), &_request->predicate_columns,
                           /*is_predicate_column=*/true);
    }

    Status add_non_predicate_column(LocalColumnIndex projection) {
        return _add_column(std::move(projection), &_request->non_predicate_columns,
                           /*is_predicate_column=*/false);
    }

    Status add_predicate_column(LocalColumnId column_id) {
        return add_predicate_column(LocalColumnIndex::top_level(column_id));
    }

    Status add_non_predicate_column(LocalColumnId column_id) {
        return add_non_predicate_column(LocalColumnIndex::top_level(column_id));
    }

private:
    static LocalIndex _next_block_position(const FileScanRequest& request) {
        size_t next_position = 0;
        for (const auto& [_, block_position] : request.local_positions) {
            next_position = std::max(next_position, block_position.value() + 1);
        }
        return LocalIndex(next_position);
    }

    static void _sort_projection_children_by_file_id(LocalColumnIndex* projection) {
        DORIS_CHECK(projection != nullptr);
        if (projection->project_all_children) {
            return;
        }
        for (auto& child : projection->children) {
            _sort_projection_children_by_file_id(&child);
        }
        std::ranges::sort(projection->children,
                          [](const LocalColumnIndex& lhs, const LocalColumnIndex& rhs) {
                              return lhs.local_id() < rhs.local_id();
                          });
    }

    Status _add_column(LocalColumnIndex projection, std::vector<LocalColumnIndex>* scan_columns,
                       bool is_predicate_column) {
        DORIS_CHECK(scan_columns != nullptr);
        const auto file_column_id = projection.column_id();
        DORIS_CHECK(file_column_id != LocalColumnId::invalid());
        if (!is_predicate_column &&
            std::ranges::find_if(_request->predicate_columns, [&](const LocalColumnIndex& p) {
                return p.column_id() == file_column_id;
            }) != _request->predicate_columns.end()) {
            return Status::OK();
        }
        if (!_request->local_positions.contains(file_column_id)) {
            _request->local_positions.emplace(file_column_id, _next_block_position(*_request));
        }

        _sort_projection_children_by_file_id(&projection);
        auto existing_projection_it = std::ranges::find_if(
                *scan_columns,
                [&](const LocalColumnIndex& p) { return p.column_id() == file_column_id; });
        if (existing_projection_it == scan_columns->end()) {
            scan_columns->push_back(std::move(projection));
        } else {
            RETURN_IF_ERROR(merge_local_column_index(&*existing_projection_it, projection));
            _sort_projection_children_by_file_id(&*existing_projection_it);
        }

        if (is_predicate_column) {
            auto it = std::ranges::find_if(
                    _request->non_predicate_columns,
                    [&](const LocalColumnIndex& p) { return p.column_id() == file_column_id; });
            if (it != _request->non_predicate_columns.end()) {
                _request->non_predicate_columns.erase(it);
            }
        }
        return Status::OK();
    }

    FileScanRequest* _request = nullptr;
};

struct FileAggregateRequest {
    struct Column {
        // File-local projection for the aggregate column. For nested MIN/MAX, this points to the
        // single primitive leaf that can be represented by file statistics. For COUNT(col), this
        // points to the top-level column whose NULL-ness should be counted.
        LocalColumnIndex projection;
    };

    TPushAggOp::type agg_type = TPushAggOp::type::NONE;
    // Empty for COUNT(*)/row-count pushdown. Non-empty for COUNT(col), where the file reader must
    // return the number of non-NULL rows for the requested column instead of total rows.
    std::vector<Column> columns;
};

struct FileAggregateResult {
    struct Column {
        // Mirrors FileAggregateRequest::Column::projection so TableReader can put the returned
        // aggregate value back into the matching projected nested shape.
        LocalColumnIndex projection;
        bool has_min = false;
        bool has_max = false;
        Field min_value;
        Field max_value;
    };

    int64_t count = 0;
    std::vector<Column> columns;
};

/**
 *                                +-----> get_schema() -----------------+
 * FileReader() -----> init() ----|                                      -----> close()
 *                                +-----> open() -----> get_block() ----+
 */
class FileReader {
public:
    struct ReaderStatistics {
        int32_t filtered_row_groups = 0;
        int32_t filtered_row_groups_by_min_max = 0;
        int32_t filtered_row_groups_by_bloom_filter = 0;
        int32_t read_row_groups = 0;
        int64_t filtered_group_rows = 0;
        int64_t filtered_page_rows = 0;
        int64_t lazy_read_filtered_rows = 0;
        int64_t read_rows = 0;
        int64_t filtered_bytes = 0;
        int64_t column_read_time = 0;
        int64_t parse_meta_time = 0;
        int64_t parse_footer_time = 0;
        int64_t file_footer_read_calls = 0;
        int64_t file_footer_hit_cache = 0;
        int64_t file_reader_create_time = 0;
        int64_t open_file_num = 0;
        int64_t row_group_filter_time = 0;
        int64_t page_index_filter_time = 0;
        int64_t read_page_index_time = 0;
        int64_t parse_page_index_time = 0;
        int64_t predicate_filter_time = 0;
        int64_t dict_filter_rewrite_time = 0;
        int64_t bloom_filter_read_time = 0;
    };

    FileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
               std::unique_ptr<io::FileDescription>& file_description,
               std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile)
            : _system_properties(system_properties),
              _file_description(std::move(file_description)),
              _io_ctx(io_ctx),
              _profile(profile) {}
    virtual ~FileReader() = default;

    // Initialize file reader and parse file metadata.
    virtual Status init(RuntimeState* state);

    // Set the maximum row count for the next physical read batch. Readers that do not batch by
    // rows may ignore it.
    virtual void set_batch_size(size_t batch_size) { (void)batch_size; }

    // Get semantic file-local schema from file metadata. The file schema is determined by file
    // format and file content, and does not contain table/global schema semantics. A file reader may
    // expose raw file identifiers, such as Parquet field_id, through ColumnDefinition::identifier,
    // but it must not interpret table-format semantics such as Iceberg name mapping,
    // default/generated columns, or partition columns. File-format physical wrappers should be
    // normalized away before exposing this schema; for example, Parquet MAP is exposed as key/value
    // children rather than key_value/entry.
    // Doris plans external-table scan types as nullable, including all nested children of complex
    // types. This protects Doris from illegal or inconsistent values produced by external systems.
    // Therefore every ColumnDefinition::type returned here must be nullable. Complex types must
    // also expose nullable child types recursively, even if the physical file marks those fields as
    // required.
    // This method can only be called after init() successfully, but does not require open() to be
    // called.
    virtual Status get_schema(std::vector<ColumnDefinition>* file_schema) const = 0;

    // Create the mapper that matches this reader's scan-request capabilities. TableReader still
    // owns table-format semantics such as BY_NAME/BY_FIELD_ID/BY_INDEX, partition values and
    // default expressions; the FileReader only chooses whether file-local requests support columnar
    // lazy materialization/pruning or must materialize one flat list of required columns.
    virtual std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const;

    // Open the file reader with file-local scan request. The file reader should initialize its internal state according to the request, but does not need to interpret table/global schema semantics. For example, all schema change, filter localization, default/generated/partition columns should be handled in table reader layer. This method can only be called after init() successfully.
    virtual Status open(std::shared_ptr<FileScanRequest> request) {
        _request = std::move(request);
        return Status::OK();
    }

    virtual Status get_block(Block* file_block, size_t* rows, bool* eof) {
        if (rows != nullptr) {
            *rows = 0;
        }
        if (eof != nullptr) {
            *eof = true;
        }
        _eof = true;
        return Status::OK();
    }

    virtual Status get_aggregate_result(const FileAggregateRequest& request,
                                        FileAggregateResult* result) {
        return Status::NotSupported("FileReader does not support aggregate pushdown");
    }

    // Condition cache is managed by TableReader and consumed by physical file readers.
    // On cache HIT, readers may skip granules whose cached bit is false before doing column IO.
    // On cache MISS, readers mark a granule true when row-level predicates keep at least one row
    // in that granule. Readers that cannot map batch rows to stable file-global row ids should
    // keep the default no-op implementation.
    virtual void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) {}

    // Total rows covered by this physical reader. TableReader uses it to pre-size the miss bitmap.
    // Readers should return 0 if the metadata is unavailable or the row coordinate is unstable.
    virtual int64_t get_total_rows() const { return 0; }

    virtual Status close() {
        _file_reader.reset();
        _tracing_file_reader.reset();
        _io_ctx.reset();
        _eof = true;
        return Status::OK();
    }

protected:
    virtual void _init_profile() {}
    void _record_scan_rows(int64_t rows) {
        DORIS_CHECK(rows >= 0);
        _reader_statistics.read_rows += rows;
        if (_io_ctx != nullptr && _io_ctx->file_reader_stats != nullptr) {
            _io_ctx->file_reader_stats->read_rows += cast_set<size_t>(rows);
        }
    }

    io::FileReaderSPtr _file_reader;
    // _tracing_file_reader wraps _file_reader.
    // _file_reader is original file reader.
    // _tracing_file_reader is tracing file reader with io context.
    // If io_ctx is null, _tracing_file_reader will be the same as file_reader.
    io::FileReaderSPtr _tracing_file_reader = nullptr;
    std::shared_ptr<FileScanRequest> _request;
    bool _eof = true;
    ReaderStatistics _reader_statistics;
    std::shared_ptr<io::FileSystemProperties> _system_properties;
    std::unique_ptr<io::FileDescription> _file_description;
    std::shared_ptr<io::IOContext> _io_ctx;
    RuntimeProfile* _profile = nullptr;
};

} // namespace doris::format
