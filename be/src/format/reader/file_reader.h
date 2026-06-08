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
#include "format/reader/column_data.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/file_factory.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace doris {
class Block;
class ColumnPredicate;

namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::reader {

// File-local single-column predicates for file-layer pruning, such as min/max, page index,
// dictionary and bloom filter.
//
// Predicates must all belong to file_column_id. file_child_id_path points to the nested primitive
// leaf under that root; empty means the top-level column itself is the primitive leaf. These
// predicates are pruning hints only and are not row-level conjuncts.
struct FileColumnPredicateFilter {
    LocalColumnId file_column_id = LocalColumnId::invalid();
    // Reader-local child id path under file_column_id. Empty means top-level scalar.
    // Each id is interpreted by the concrete FileReader inside the current parent node. For
    // Parquet this is the child ordinal under that parent, not the optional Parquet field_id.
    std::vector<int32_t> file_child_id_path;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;

    std::string debug_string() const;
};

enum class FileFormat {
    PARQUET,
    ORC,
    CSV,
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

    // Get file-local schema from file metadata. The file schema is determined by file format and
    // file content, and does not contain table/global schema semantics. A file reader may expose
    // raw file identifiers, such as Parquet field_id, through ColumnDefinition::identifier, but it
    // must not interpret table-format semantics such as Iceberg name mapping, default/generated
    // columns, or partition columns. This method can only be called after init() successfully, but
    // does not require open() to be called.
    virtual Status get_schema(std::vector<ColumnDefinition>* file_schema) const = 0;

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

} // namespace doris::reader
