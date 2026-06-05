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
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "exprs/vexpr_fwd.h"
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

// File-local top-level column id.
//
// Scope:
// - Only valid inside one physical file schema returned by FileReader::get_schema().
// - For Parquet, this is the top-level field ordinal in the new reader schema.
// - The synthetic row-position column also uses this type, with a reserved negative id.
//
// Do not use this for table/global column unique ids, block positions, nested child ids, or
// slot ids. Nested child ids are carried by LocalColumnIndex::index below.
class LocalColumnId {
public:
    constexpr LocalColumnId() = default;
    explicit constexpr LocalColumnId(int32_t id) : _id(id) {}

    static constexpr LocalColumnId invalid() { return LocalColumnId(); }

    constexpr int32_t value() const { return _id; }
    constexpr bool is_valid() const { return _id >= 0; }

    constexpr bool operator==(const LocalColumnId& other) const { return _id == other._id; }
    constexpr bool operator!=(const LocalColumnId& other) const { return !(*this == other); }
    constexpr bool operator<(const LocalColumnId& other) const { return _id < other._id; }

private:
    int32_t _id = -1;
};

// Position of a file-local column in the Block produced by one FileScanRequest.
//
// This is assigned by TableColumnMapper/TableReader after predicate/non-predicate columns are
// deduplicated. It is not a file schema id and it is not stable across requests. Use value() only
// at the boundary where an existing Block or expression API still expects a size_t/int position.
class LocalIndex {
public:
    constexpr LocalIndex() = default;
    explicit constexpr LocalIndex(size_t index) : _index(index) {}

    constexpr size_t value() const { return _index; }
    constexpr bool operator==(const LocalIndex& other) const { return _index == other._index; }
    constexpr bool operator<(const LocalIndex& other) const { return _index < other._index; }

private:
    size_t _index = 0;
};

// Position of a table/global output column in the final Block returned by TableReader.
//
// This type is reserved for boundaries that need to refer to caller-visible column order. It must
// not be used to index a file-local Block, because schema evolution and lazy materialization can
// make file-local order different from table output order.
class GlobalIndex {
public:
    constexpr GlobalIndex() = default;
    explicit constexpr GlobalIndex(size_t index) : _index(index) {}

    constexpr size_t value() const { return _index; }
    constexpr bool operator==(const GlobalIndex& other) const { return _index == other._index; }
    constexpr bool operator<(const GlobalIndex& other) const { return _index < other._index; }

private:
    size_t _index = 0;
};

// Index of a split-local constant/default value used to materialize columns that are not read from
// the physical file, such as partition columns, added columns with default values, and virtual
// table-format columns.
//
// It is separate from LocalIndex because constants do not occupy a position in the file reader
// output block unless an expression explicitly materializes them.
class ConstantIndex {
public:
    constexpr ConstantIndex() = default;
    explicit constexpr ConstantIndex(size_t index) : _index(index) {}

    constexpr size_t value() const { return _index; }
    constexpr bool operator==(const ConstantIndex& other) const { return _index == other._index; }
    constexpr bool operator<(const ConstantIndex& other) const { return _index < other._index; }

private:
    size_t _index = 0;
};

inline std::ostream& operator<<(std::ostream& os, const LocalColumnId& id) {
    return os << id.value();
}

inline std::ostream& operator<<(std::ostream& os, const LocalIndex& index) {
    return os << index.value();
}

inline std::ostream& operator<<(std::ostream& os, const GlobalIndex& index) {
    return os << index.value();
}

inline std::ostream& operator<<(std::ostream& os, const ConstantIndex& index) {
    return os << index.value();
}

enum ColumnType {
    DATA_COLUMN = 0, // normal data column
    ROW_NUMBER = 1,  // row number in a file
};

// 文件本地 schema 字段。
// 这是 FileReader 暴露给 table 层的 file-local schema 视图，不携带 table/global
// schema 语义。Iceberg field id、name mapping、default/generated/partition 列都不在
// FileReader 内部解释。
struct SchemaField {
    // Column ID for top-level fields. For nested fields, column_id is the index of children.
    int32_t id = -1;
    std::string name;
    DataTypePtr type;
    std::vector<SchemaField> children;
    ColumnType column_type = ColumnType::DATA_COLUMN;
};

// Recursive file-local projection path.
//
// For a root entry in FileScanRequest::{predicate_columns, non_predicate_columns}, index is the
// top-level file column id and column_id() is valid. For children, index is the file-local child id
// under the parent node, not a table child id and not a child output ordinal.
//
// project_all_children=true means the whole subtree under this node is needed. When false, children
// lists the selected child paths. File readers can use this to avoid constructing readers for
// unprojected nested children.
struct LocalColumnIndex {
    int32_t index = -1;
    bool project_all_children = true;
    std::vector<LocalColumnIndex> children {};

    LocalColumnId column_id() const { return LocalColumnId(index); }
};

// Merge two projection trees that point to the same file-local node.
//
// A full projection dominates a partial projection. Two partial projections are merged by child id
// and recursively union their child paths. The caller must only merge projections for the same
// root/child node.
inline Status merge_local_column_index(LocalColumnIndex* target, const LocalColumnIndex& source) {
    DORIS_CHECK(target != nullptr);
    DORIS_CHECK(target->index == source.index);
    if (target->project_all_children) {
        return Status::OK();
    }
    if (source.project_all_children) {
        target->project_all_children = true;
        target->children.clear();
        return Status::OK();
    }
    for (const auto& source_child : source.children) {
        auto target_child_it = std::find_if(
                target->children.begin(), target->children.end(),
                [&](const LocalColumnIndex& child) { return child.index == source_child.index; });
        if (target_child_it == target->children.end()) {
            target->children.push_back(source_child);
            continue;
        }
        RETURN_IF_ERROR(merge_local_column_index(&*target_child_it, source_child));
    }
    return Status::OK();
}

// File-local single-column predicates for file-layer pruning, such as min/max, page index,
// dictionary and bloom filter.
//
// Predicates must all belong to file_column_id. file_child_id_path points to the nested primitive
// leaf under that root; empty means the top-level column itself is the primitive leaf. These
// predicates are pruning hints only and are not row-level conjuncts.
struct FileColumnPredicateFilter {
    LocalColumnId file_column_id = LocalColumnId::invalid();
    // File-local child field-id path under file_column_id. Empty means top-level scalar.
    // The ids are Parquet/Doris file schema child ids, not table ids and not child ordinals.
    std::vector<int32_t> file_child_id_path;
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
