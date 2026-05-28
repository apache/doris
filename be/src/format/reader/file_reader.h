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
#include "exprs/vexpr_fwd.h"
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

using ColumnId = int32_t;
using FileRowPosition = int64_t;

enum ColumnType {
    DATA_COLUMN = 0, // normal data column
    ROW_NUMBER = 1,  // row number in a file
    FILE_NAME = 2,   // file name
};

// 文件本地 schema 字段。
// 这是 FileReader 暴露给 table 层的 file-local schema 视图，不携带 table/global
// schema 语义。Iceberg field id、name mapping、default/generated/partition 列都不在
// FileReader 内部解释。
struct SchemaField {
    int32_t id = -1;
    std::string name;
    DataTypePtr type;
    std::vector<SchemaField> children;
    std::vector<int32_t> file_path;
    std::vector<int32_t> field_id_path;
    std::vector<std::string> name_path;
    ColumnType column_type = ColumnType::DATA_COLUMN;
};

// File-local nested projection. The top-level scan column is still represented
// by FileScanRequest::predicate_columns/non_predicate_columns; this tree only
// describes which child paths are needed inside a complex top-level field.
struct FieldProjection {
    ColumnId file_column_id = -1;
    std::vector<int32_t> file_path;
    bool project_all_children = true;
    std::vector<FieldProjection> children;
};

// File-local expression filter. It may reference multiple predicate_columns, so FileReader should
// evaluate it after all referenced predicate columns have been materialized in the file-local block.
struct FileExpressionFilter {
    VExprContextSPtr conjunct;
    // DeletePredicate
    VExprContextSPtr delete_conjunct;
    std::vector<ColumnId> file_column_ids;
};

// File-local single-column predicates for file-layer pruning, such as min/max, page index,
// dictionary and bloom filter. Predicates must all belong to file_column_id.
struct FileColumnPredicateFilter {
    ColumnId file_column_id = -1;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
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

    std::vector<ColumnId> predicate_columns;
    std::vector<ColumnId> non_predicate_columns;
    std::map<ColumnId, size_t> column_positions; // file_column_id -> file-local block position
    std::map<ColumnId, FieldProjection> complex_projections;
    std::vector<FileExpressionFilter> expression_filters;
    std::vector<FileColumnPredicateFilter> column_predicate_filters;
    // fallback path if filters cannot be localized to file-local predicates. The expression can reference projected_file_columns and partition columns.
    std::vector<std::pair<ColumnId, VExprContextSPtr>> reader_expression_map;
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

    // Get file-local schema from file metadata. The file schema is determined by file format and file content, and does not contain table/global schema semantics. For example, Iceberg field id, name mapping, default/generated/partition columns are not interpreted in file reader. This method can only be called after init() successfully, but does not require open() to be called.
    virtual Status get_schema(std::vector<SchemaField>* file_schema) const = 0;

    // Open the file reader with file-local scan request. The file reader should initialize its internal state according to the request, but does not need to interpret table/global schema semantics. For example, all schema change, filter localization, default/generated/partition columns should be handled in table reader layer. This method can only be called after init() successfully.
    virtual Status open(std::unique_ptr<FileScanRequest>& request) {
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

    // File-local physical row positions for the last returned batch after file-layer filtering.
    // Table formats such as Iceberg may combine these positions with table-format metadata.
    virtual const std::vector<FileRowPosition>& current_batch_row_positions() const {
        static const std::vector<FileRowPosition> empty_row_positions;
        return empty_row_positions;
    }

    // 关闭当前物理文件 reader 并释放文件层状态。
    // 该方法不处理 table-level delete/finalize 状态，后者由 TableReader 子类管理。
    virtual Status close() {
        _file_reader.reset();
        _tracing_file_reader.reset();
        _io_ctx.reset();
        _request.reset();
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
    std::unique_ptr<FileScanRequest> _request;
    bool _eof = true;
    ReaderStatistics _reader_statistics;
    std::shared_ptr<io::FileSystemProperties> _system_properties;
    std::unique_ptr<io::FileDescription> _file_description;
    std::shared_ptr<io::IOContext> _io_ctx;
    RuntimeProfile* _profile = nullptr;
};

} // namespace doris::reader
