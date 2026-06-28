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

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

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
class ColumnPredicate;
struct ConditionCacheContext;

namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::format {

class TableColumnMapper;
struct TableColumnMapperOptions;

// Struct-only nested predicate target used by file-layer pruning.
//
// This intentionally models only a STRUCT field chain. LIST/MAP/repeated predicates need explicit
// quantified semantics, so they must not be encoded here.
struct FileStructPredicateTarget {
    int32_t file_local_id = -1;
    std::string file_child_name;
    std::unique_ptr<FileStructPredicateTarget> child;

    FileStructPredicateTarget() = default;
    FileStructPredicateTarget(int32_t local_id, std::string child_name,
                              std::unique_ptr<FileStructPredicateTarget> nested_child = nullptr)
            : file_local_id(local_id),
              file_child_name(std::move(child_name)),
              child(std::move(nested_child)) {}
    FileStructPredicateTarget(const FileStructPredicateTarget& other);
    FileStructPredicateTarget& operator=(const FileStructPredicateTarget& other);
    FileStructPredicateTarget(FileStructPredicateTarget&& other) noexcept = default;
    FileStructPredicateTarget& operator=(FileStructPredicateTarget&& other) noexcept = default;
};

struct FileNestedPredicateTarget {
    LocalColumnId file_column_id = LocalColumnId::invalid();
    // Null means the predicate targets the top-level primitive column itself.
    std::unique_ptr<FileStructPredicateTarget> struct_target;

    FileNestedPredicateTarget() = default;
    explicit FileNestedPredicateTarget(LocalColumnId column_id) : file_column_id(column_id) {}
    FileNestedPredicateTarget(LocalColumnId column_id,
                              std::unique_ptr<FileStructPredicateTarget> target)
            : file_column_id(column_id), struct_target(std::move(target)) {}
    FileNestedPredicateTarget(const FileNestedPredicateTarget& other);
    FileNestedPredicateTarget& operator=(const FileNestedPredicateTarget& other);
    FileNestedPredicateTarget(FileNestedPredicateTarget&& other) noexcept = default;
    FileNestedPredicateTarget& operator=(FileNestedPredicateTarget&& other) noexcept = default;

    bool is_valid() const { return file_column_id.is_valid(); }
};

// File-local single-column predicates for file-layer pruning, such as min/max, page index,
// dictionary and bloom filter.
//
// Predicates must all belong to target.file_column_id. target.struct_target points to the nested
// primitive leaf under that root; null means the top-level column itself is the primitive leaf.
// These predicates are pruning hints only and are not row-level conjuncts.
struct FileColumnPredicateFilter {
    FileNestedPredicateTarget target;
    // Compatibility fields for call sites and tests that still construct pruning filters directly.
    // New mapper code should fill target; file readers consume target first and only fall back to
    // these fields while the API migration is in progress.
    LocalColumnId file_column_id = LocalColumnId::invalid();
    std::vector<int32_t> file_child_id_path;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;

    LocalColumnId effective_file_column_id() const;
    std::vector<int32_t> effective_file_child_id_path() const;
    bool same_target_as(const FileColumnPredicateFilter& other) const;
    std::string debug_string() const;
};

enum class FileFormat {
    PARQUET,
    ORC,
    CSV,
    JSON,
    TEXT,
    JNI,
    NATIVE,
};

// 通用文件层 scan 请求。
// 该结构描述所有文件格式都可以共享的 file-local 读取输入。这里不出现 table/global
// schema。所有 schema change、filter localization、default/generated/partition
// 列都应在 table 层完成。
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
    // Delete predicates converted to file-local expressions.
    VExprContextSPtrs delete_conjuncts;
    // Single-column predicates used only for file-layer pruning, such as statistics, page index,
    // dictionary and bloom filter. They must not be used for batch row-level filtering.
    std::vector<FileColumnPredicateFilter> column_predicate_filters;
    // Fallback expressions evaluated by file readers after decoding file-local columns. These are
    // used when a mapping requires reader-local expression semantics that cannot be represented as
    // a plain localized conjunct.
    std::vector<std::pair<LocalColumnId, VExprContextSPtr>> reader_expression_map;
};

// Helper for constructing the scan-column layout in FileScanRequest.
//
// FileScanRequest keeps predicate and non-predicate columns separate because columnar readers such
// as Parquet can read predicate columns first, filter rows, and then lazily read the remaining
// projected columns. The two lists still share one file-local output block, whose positions are
// stored in local_positions. This builder centralizes the mechanical rules for that shared layout:
// - each root file column gets one stable block position;
// - predicate columns dominate non-predicate columns because they are already returned in the file
//   block and can be reused for final materialization;
// - repeated nested projections for the same root are merged instead of duplicated.
//
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
        // single primitive leaf that can be represented by file statistics.
        LocalColumnIndex projection;
    };

    TPushAggOp::type agg_type = TPushAggOp::type::NONE;
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

// 文件物理读取层通用接口。
// 该接口只描述 file-local schema、file-local scan request 和 file-local block。
// TableReader/IcebergTableReader 可以通过它组合不同文件格式 reader。
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

    // Get semantic file-local schema from file metadata. The file schema is determined by file
    // format and file content, and does not contain table/global schema semantics. A file reader may
    // expose raw file identifiers, such as Parquet field_id, through ColumnDefinition::identifier,
    // but it must not interpret table-format semantics such as Iceberg name mapping,
    // default/generated columns, or partition columns. File-format physical wrappers should be
    // normalized away before exposing this schema; for example, Parquet MAP is exposed as key/value
    // children rather than key_value/entry.
    //
    // Doris plans external-table scan types as nullable, including all nested children of complex
    // types. This protects Doris from illegal or inconsistent values produced by external systems.
    // Therefore every ColumnDefinition::type returned here must be nullable. Complex types must
    // also expose nullable child types recursively, even if the physical file marks those fields as
    // required.
    //
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

    // 读取下一批 file-local block。
    // 该方法只能在 open(FileScanRequest) 成功后调用。
    // file_block 的列顺序和类型必须遵守 FileScanRequest，而不是 table/global schema。
    // rows 返回当前批次输出行数；eof 表示当前文件 reader 是否读完；多文件切换由
    // TableReader 负责。
    virtual Status get_block(Block* file_block, size_t* rows, bool* eof) {
        // stub 默认立即 EOF。
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

    // 关闭当前物理文件 reader 并释放文件层状态。
    // 该方法不处理 table-level delete/finalize 状态，后者由 TableReader 子类管理。
    virtual Status close() {
        _file_reader.reset();
        _tracing_file_reader.reset();
        _io_ctx.reset();
        _eof = true;
        return Status::OK();
    }

protected:
    virtual void _init_profile() {}

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
