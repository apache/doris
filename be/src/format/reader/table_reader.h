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

#include <bvar/status.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr_fwd.h"
#include "format/reader/file_reader.h"

namespace doris {
class Block;
class ColumnPredicate;
} // namespace doris

namespace doris::reader {

// table/global schema 中的列视图。
// Iceberg 场景下，id 默认对应 Iceberg field id。该结构不描述文件中的物理列。
struct TableColumn {
    ColumnId id = -1;
    std::string name;
    DataTypePtr type;
    std::vector<TableColumn> children;
};

// table-level filter。
// TableColumnMapper 负责把它转换成 FileLocalFilter 或 reader_expression_map。
struct TableFilter {
    ColumnId table_column_id = -1;

    // 表达式过滤，适合表达 cast、复杂表达式、复杂列提取等语义。
    VExprContextSPtr conjunct;

    // 结构化列谓词，适合下推到文件层做 row group stats、page index、dictionary、
    // bloom filter 等优化。
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
};

// 单个 table column 到 file column 的映射结果。
// 这是 table 层和 file 层的核心边界对象。
struct ColumnMapping {
    ColumnId table_column_id = -1;
    std::optional<ColumnId> file_column_id;
    DataTypePtr file_type;
    DataTypePtr table_type;

    // 最终输出表达式。用于把 file-local value 转成 table/global value，例如 cast、
    // default、partition、generated column 或复杂列 remap。
    VExprContextSPtr finalize_expr;

    // 读时过滤 fallback 表达式。只在 table filter 不能安全转换成 file-local predicate
    // 时使用，服务 reader_expression_map，不等价于 finalize_expr。
    VExprContextSPtr reader_filter_expr;

    std::vector<ColumnMapping> child_mappings;
    bool is_trivial = false;
    bool is_constant = false;
};

enum class TableColumnMappingMode {
    BY_FIELD_ID,
    BY_NAME,
};

enum class TableFilterConversion {
    COPY_DIRECTLY,
    CAST_FILTER,
    EVALUATE_EXPRESSION,
    FINALIZE_ONLY,
};

struct TableColumnMapperOptions {
    TableColumnMappingMode mode = TableColumnMappingMode::BY_FIELD_ID;
    bool allow_missing_columns = true;
    bool enable_reader_expression_fallback = true;
};

// table-level scan 请求。
// 它仍然使用 table/global schema 语义，不能直接传给 FileReader。
struct TableScanRequest {
    std::vector<TableColumn> projected_table_columns;
    std::vector<TableFilter> table_filters;
};

// 通用 table schema 到 file schema 映射层。
// Iceberg 会使用 BY_FIELD_ID；普通 by-name 场景可以复用该组件，但不应把它命名成
// Iceberg-only 组件。
class TableColumnMapper {
public:
    explicit TableColumnMapper(TableColumnMapperOptions options = {})
            : _options(std::move(options)) {}
    virtual ~TableColumnMapper() = default;

    // 建立 table schema 到 file schema 的列映射。
    // 输出的 ColumnMapping 描述 table column 如何从 file column、常量列或表达式得到；
    // 后续 projection、filter localization 和 table block finalize 都应复用这份映射。
    virtual Status create_mapping(const std::vector<TableColumn>& table_schema,
                                  const std::vector<SchemaField>& file_schema,
                                  std::vector<ColumnMapping>* mappings) {
        // 真实实现会做 field id/name matching、类型转换、复杂列 child mapping、缺失列
        // default/partition/generated 表达式构造。
        mappings->clear();
        for (const auto& table_column : table_schema) {
            ColumnMapping mapping;
            mapping.table_column_id = table_column.id;
            mapping.table_type = table_column.type;
            if (const auto* file_field = find_file_field(table_column, file_schema)) {
                mapping.file_column_id = file_field->id;
                mapping.file_type = file_field->type;
                mapping.is_trivial = is_same_type(mapping.table_type, mapping.file_type);
            } else {
                mapping.is_constant = true;
            }
            mappings->push_back(std::move(mapping));
        }
        _mappings = *mappings;
        return Status::OK();
    }

    // 把 table-level scan 请求转换成 file-local scan 请求。
    // table_request 使用 table/global schema；file_request 只包含 FileReader 能理解的
    // projected_file_columns、local_filters 和 reader_expression_map。
    virtual Status create_scan_request(const TableScanRequest& table_request,
                                       const std::vector<ColumnMapping>& mappings,
                                       FileScanRequest* file_request) {
        // 真实实现会把 table projection/filter 转换成 file-local projection/filter。
        file_request->projected_file_columns.clear();
        file_request->local_filters.clear();
        file_request->reader_expression_map.clear();
        _mappings = mappings;
        for (const auto& table_column : table_request.projected_table_columns) {
            const auto* mapping = find_mapping(table_column.id);
            if (mapping != nullptr && mapping->file_column_id.has_value()) {
                file_request->projected_file_columns.push_back(*mapping->file_column_id);
            }
        }
        RETURN_IF_ERROR(localize_filters(table_request.table_filters, file_request));
        return Status::OK();
    }

    // 将 table-level filter 定位到文件 schema。
    // trivial mapping 可以直接复制结构化谓词；类型变化时可以尝试安全 cast；无法安全
    // 下推的表达式应通过 reader_expression_map 或 table-level finalize/filter fallback 处理。
    virtual Status localize_filters(const std::vector<TableFilter>& table_filters,
                                    FileScanRequest* file_request) const {
        // 真实实现会处理 trivial mapping、safe cast、reader expression fallback 和
        // finalize-only filter。stub 只复制能够直接定位到 file column 的谓词。
        for (const auto& filter : table_filters) {
            const auto* mapping = find_mapping(filter.table_column_id);
            if (mapping == nullptr || !mapping->file_column_id.has_value()) {
                continue;
            }
            FileLocalFilter local_filter;
            local_filter.file_column_id = *mapping->file_column_id;
            local_filter.conjunct = filter.conjunct;
            local_filter.predicates = filter.predicates;
            file_request->local_filters.push_back(std::move(local_filter));
        }
        return Status::OK();
    }

    const std::vector<ColumnMapping>& mappings() const { return _mappings; }

private:
    const SchemaField* find_file_field(const TableColumn& table_column,
                                       const std::vector<SchemaField>& file_schema) const {
        for (const auto& field : file_schema) {
            if (_options.mode == TableColumnMappingMode::BY_FIELD_ID &&
                field.id == table_column.id) {
                return &field;
            }
            if (_options.mode == TableColumnMappingMode::BY_NAME &&
                field.name == table_column.name) {
                return &field;
            }
        }
        return nullptr;
    }

    const ColumnMapping* find_mapping(ColumnId table_column_id) const {
        for (const auto& mapping : _mappings) {
            if (mapping.table_column_id == table_column_id) {
                return &mapping;
            }
        }
        return nullptr;
    }

    bool is_same_type(const DataTypePtr& table_type, const DataTypePtr& file_type) const {
        return table_type == file_type;
    }

private:
    TableColumnMapperOptions _options;
    std::vector<ColumnMapping> _mappings;
};

struct BaseDataFile {
    virtual ~BaseDataFile() = default;

    std::string path;
    std::string format;
    int64_t record_count = 0;
    int64_t file_size = 0;
};

struct ScanTask {
    virtual ~ScanTask() = default;

    std::unique_ptr<BaseDataFile> data_file;
};

struct TableReadOptions {
    size_t batch_size = 4096;
    // TODO: deleted? SCHEMA should be derived from table metadata and inited by TableReader it self? it shouldn't be part of read options.
    std::vector<TableColumn> schema;
    TableScanRequest scan_request;
    // Each task denotes a descriptor of a single file to read, along with file-level metadata such as stats and delete files.
    std::vector<std::unique_ptr<ScanTask>> scan_tasks;
};

// table-level reader 基类。
// 该层负责多文件编排和动态分区裁剪等通用 table-level 逻辑，对外输出 table block。
// 子类只需要实现“如何打开下一个具体 reader”和“如何读取当前 reader”的表格式语义。
class TableReader {
public:
    virtual ~TableReader() = default;

    // 初始化 table reader 的通用运行参数。
    // 子类可以在自己的 init(options) 中调用该方法；这里不接收具体表格式 schema/task。
    virtual Status init(TableReadOptions options) {
        _schema = std::move(_options.schema);
        _table_scan_request = std::move(_options.scan_request);
        _scan_tasks = std::move(_options.scan_tasks);
        _next_task_idx = 0;
        return Status::OK();
    }

    // table-level 动态过滤入口。
    // 该方法用于根据 split、partition value 或文件级统计判断是否可以跳过后续 reader。
    // can_filter_all=true 表示当前 table reader 范围内的数据都可以被裁剪。
    virtual Status filter(const VExprContextSPtr& expr, bool* can_filter_all) {
        // 真实实现会基于 split/partition/file stats 判断动态分区裁剪结果。
        (void)expr;
        if (can_filter_all != nullptr) {
            *can_filter_all = false;
        }
        return Status::OK();
    }

    // 对外读取 table block 的统一入口。
    // 基类负责 current reader 的打开、EOF 后切换和关闭；子类只实现 protected hook。
    // table_block 的列必须已经是 table/global schema 语义。
    Status get_block(Block* block, bool* eos) {
        if (eos != nullptr) {
            *eos = false;
        }
        while (block->empty() && !*eos) {
            if (!_data_reader) {
                RETURN_IF_ERROR(create_next_reader(eos));
                if (!_data_reader) {
                    DCHECK(*eos);
                    return Status::OK();
                }
            }

            bool current_eof = false;
            RETURN_IF_ERROR(_data_reader->get_block(block, &current_eof));
            RETURN_IF_ERROR(finalize_chunk(block));
            RETURN_IF_ERROR(materialize_virtual_columns(block));
            if (current_eof) {
                RETURN_IF_ERROR(close_current_reader());
            }
        }
        return Status::OK();
    }

    // 关闭 table reader 及当前正在读取的底层 reader。
    // 子类如果持有额外表格式资源，应 override 后先调用 TableReader::close()。
    virtual Status close() {
        if (_data_reader) {
            RETURN_IF_ERROR(close_current_reader());
        }
        return Status::OK();
    }

protected:
    // 切换到下一个 reader 的通用流程。
    // 该方法先关闭当前 reader，再打开下一个具体 reader；子类不应重复实现这个循环。
    Status create_next_reader(bool* eos) {
        // 多文件切换的公共流程留在基类：关闭当前 reader，然后打开下一个 reader。
        DCHECK(_data_reader == nullptr);
        // TODO: 创建_data_reader
        // _data_reader = std::make_unique<FileReader>(...);
        if (!_data_reader) {
            if (eos != nullptr) {
                *eos = true;
            }
            return Status::OK();
        }
        RETURN_IF_ERROR(open_reader());
        return Status::OK();
    }

    // 打开当前具体 reader。
    // 子类在这里基于当前 split/task 初始化底层 FileReader。
    virtual Status open_reader() {
        std::vector<SchemaField> file_schema;
        RETURN_IF_ERROR(_data_reader->get_schema(&file_schema));
        TableColumnMapperOptions mapper_options;
        mapper_options.mode = TableColumnMappingMode::BY_FIELD_ID;
        _column_mapper = TableColumnMapper(mapper_options);
        RETURN_IF_ERROR(_column_mapper.create_mapping(_schema, file_schema, &_mappings));

        FileScanRequest file_request;
        RETURN_IF_ERROR(
                _column_mapper.create_scan_request(_table_scan_request, _mappings, &file_request));
        RETURN_IF_ERROR(_data_reader->init(file_request));
        return Status::OK();
    }

    // 关闭当前具体 reader。
    // 该 hook 会被 create_next_reader 和 close 调用；实现应保持幂等。
    virtual Status close_current_reader() {
        RETURN_IF_ERROR(_data_reader->close());
        _data_reader.reset();
        return Status::OK();
    }

    // 将 file-local block 转换为 table/global schema block。
    // 这里执行 ColumnMapping 中的 finalize_expr、缺失列填充、partition/generated 列
    // 物化以及复杂列 remap。
    virtual Status finalize_chunk(Block* block) { return Status::OK(); }

    // 物化虚拟列。
    // 例如 _row_id、_last_updated_sequence_number 等，它们不来自文件物理列。
    virtual Status materialize_virtual_columns(Block* table_block) {
        // 真实实现会物化 _row_id、_last_updated_sequence_number 等 Iceberg 虚拟列。
        return Status::OK();
    }

    TableReadOptions _options;
    std::unique_ptr<FileReader> _data_reader;
    std::vector<std::unique_ptr<ScanTask>> _scan_tasks;
    TableScanRequest _table_scan_request;
    std::vector<TableColumn> _schema;
    std::vector<ColumnMapping> _mappings;
    TableColumnMapper _column_mapper;
    size_t _next_task_idx = 0;
};

} // namespace doris::reader
