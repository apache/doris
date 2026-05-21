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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr_fwd.h"
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
    ColumnType column_type = ColumnType::DATA_COLUMN;
};

// 已经 localize 到文件 schema 的过滤条件。
// TableColumnMapper 负责把 table-level filter 转成这个结构；FileReader 只消费
// file-local column id、表达式和结构化谓词。
struct FileLocalFilter {
    ColumnId file_column_id = -1;

    // 表达式过滤。适合 cast、复杂表达式或 reader_expression_map 生成的临时列过滤。
    // 它通常不能直接驱动 row group stats、page index、dictionary、bloom filter。
    VExprContextSPtr conjunct;
    // DeletePredicate
    VExprContextSPtr delete_conjunct;

    // 结构化列谓词。适合文件层 pruning，例如 min/max、page index、dictionary、
    // bloom filter 等只理解单列谓词的优化。
    // TODO: conjunct 支持表达所有 filter 语义之后删除。
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
    std::vector<FileLocalFilter> local_filters;
    // fallback path if filters cannot be localized to file-local predicates. The expression can reference projected_file_columns and partition columns.
    std::vector<std::pair<ColumnId, VExprContextSPtr>> reader_expression_map;
    // partition key -> value
    std::map<std::string, Field> partition_values;

    // projected_columns' id is file-local column id, and they are all from file schema.
    // For example,
    // file schema: [0: id (int), 1: name (string), 2: age (int)]
    // predicate: age > 30
    // table-level projection: [name, id]
    // predicate_columns: [2]
    // non_predicate_columns: [1, 0]
    // projected_columns are columns in blocks returned to table reader: [1, 0] means only name and id are projected,
    std::vector<ColumnId> projected_columns;
};

// 文件物理读取层通用接口。
// 该接口只描述 file-local schema、file-local scan request 和 file-local block。
// TableReader/IcebergTableReader 可以通过它组合不同文件格式 reader。
class FileReader {
public:
    virtual ~FileReader() = default;

    // 打开一个物理文件并加载文件级元数据。
    // 该方法只建立 file-local reader 状态，不接收 table schema，也不做 projection/filter
    // 规划；这些输入由 init(FileScanRequest) 提供。
    virtual Status open(io::FileReaderSPtr file, io::IOContext* io_ctx = nullptr) {
        // 真实实现会保存文件句柄、IO 上下文并读取文件元数据。
        _file = std::move(file);
        _io_ctx = io_ctx;
        _eof = false;
        return Status::OK();
    }

    // 返回文件自己的 schema 视图。
    // 返回结果必须是 file-local schema：列 id、类型和 children 都按文件格式展开，
    // 不在这里解释 Iceberg field id、缺失列、默认值或 generated column。
    virtual Status get_schema(std::vector<SchemaField>* file_schema) const {
        // 真实实现会展开文件格式自己的 file-local schema。
        file_schema->clear();
        return Status::OK();
    }

    // 初始化一次 file-local scan。
    // request 由 TableColumnMapper 生成，只包含文件列投影、本地过滤条件和 reader
    // expression。FileReader 可以基于它初始化 row group/page/stripe 等文件格式计划。
    virtual Status init(const FileScanRequest& request) {
        // 真实实现会根据 projected columns、local filters 和 reader expressions
        // 初始化文件格式自己的物理读取计划。
        // _request.projected_file_columns = request.projected_file_columns;
        _request.local_filters = request.local_filters;
        _request.reader_expression_map = request.reader_expression_map;
        return Status::OK();
    }

    // 读取下一批 file-local block。
    // file_block 的列顺序和类型必须遵守 FileScanRequest，而不是 table/global schema。
    // eof 表示当前文件 reader 是否读完；多文件切换由 TableReader 负责。
    virtual Status get_block(Block* file_block, bool* eof) {
        // stub 默认立即 EOF。
        if (eof != nullptr) {
            *eof = true;
        }
        _eof = true;
        return Status::OK();
    }

    // 关闭当前物理文件 reader 并释放文件层状态。
    // 该方法不处理 table-level delete/finalize 状态，后者由 TableReader 子类管理。
    virtual Status close() {
        _file.reset();
        _io_ctx = nullptr;
        _request = FileScanRequest {};
        _eof = true;
        return Status::OK();
    }

protected:
    io::FileReaderSPtr _file;
    io::IOContext* _io_ctx = nullptr;
    FileScanRequest _request;
    bool _eof = true;
};

} // namespace doris::reader
