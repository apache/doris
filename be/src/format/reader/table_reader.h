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
#include "exprs/vexpr_context.h"
#include "exprs/vexpr_fwd.h"
#include "format/reader/column_mapper.h"
#include "format/reader/expr/delete_predicate.h"
#include "format/reader/expr/literal.h"
#include "format/reader/file_reader.h"

namespace doris {
class Block;
class ColumnPredicate;
struct DeleteFileDesc;
} // namespace doris

namespace doris::reader {

using DeleteRows = std::vector<int64_t>;

// table/global schema 中的列视图。
// Iceberg 场景下，id 默认对应 Iceberg field id。该结构不描述文件中的物理列。
struct TableColumn {
    ColumnId id = -1;
    std::string name;
    DataTypePtr type;
    std::vector<TableColumn> children;
    VExprContextSPtr default_expr;
    bool is_partition_key = false;
};

// table-level filter。
// TableColumnMapper 负责把它转换成 FileLocalFilter 或 reader_expression_map。
struct TableFilter {
    // 表达式过滤，适合表达 cast、复杂表达式、复杂列提取等语义。
    VExprContextSPtr conjunct;

    // 结构化列谓词，适合下推到文件层做 row group stats、page index、dictionary、
    // bloom filter 等优化。
    // TODO: conjunct 支持表达所有 filter 语义之后删除。
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;

    bool can_be_localized() const { return true; }
};

enum class TableFilterConversion {
    COPY_DIRECTLY,
    CAST_FILTER,
    EVALUATE_EXPRESSION,
    FINALIZE_ONLY,
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

struct ReadProfile {
    RuntimeProfile::Counter* num_delete_files;
    RuntimeProfile::Counter* num_delete_rows;
    RuntimeProfile::Counter* parse_delete_file_time;
};

struct TableReadOptions {
    const std::vector<TableColumn> projected_columns;
    // All conjuncts from scan operator
    const VExprContext conjuncts;
    const FileFormat format;
    TFileScanRangeParams* scan_params;
    io::IOContext* io_ctx;
    RuntimeState* runtime_state;
    RuntimeProfile* scanner_profile;
    // Each task denotes a descriptor of a single file to read, along with file-level metadata such as stats and delete files.
    std::vector<std::unique_ptr<ScanTask>> scan_tasks;

    std::unique_ptr<ReadProfile> profile;
};

struct SplitReadOptions {
    std::map<std::string, Field> partition_values;
    ShardedKVCache* cache;
    TFileRangeDesc current_range;
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
        _scan_params = options.scan_params;
        _format = options.format;
        _io_ctx = options.io_ctx;
        _runtime_state = options.runtime_state;
        _scanner_profile = options.scanner_profile;
        _scan_tasks = std::move(_options.scan_tasks);
        _next_task_idx = 0;
        _profile = std::move(options.profile);
        TableColumnMapperOptions mapper_options;
        mapper_options.mode = TableColumnMappingMode::BY_FIELD_ID;
        _data_reader.column_mapper = TableColumnMapper(mapper_options);
        // TODO:
        // _table_filters = build_table_filters_from_conjuncts(options.conjuncts);
        return Status::OK();
    }

    // 读取当前 split/partition 之前初始化。
    virtual Status prepare_split(const SplitReadOptions& options);

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
        while (block->empty() && !*eos) {
            if (!_data_reader.reader) {
                RETURN_IF_ERROR(create_next_reader(eos));
                if (!_data_reader.reader) {
                    DCHECK(*eos);
                    return Status::OK();
                }
            }

            bool current_eof = false;
            Block current_block;
            for (const auto& field : _data_reader.block_schema) {
                // TODO: reuse column's memory
                current_block.insert({field.type->create_column(), field.type, field.name});
            }
            size_t current_rows = 0;
            RETURN_IF_ERROR(
                    _data_reader.reader->get_block(&current_block, &current_rows, &current_eof));
            if (current_rows == 0 && !current_eof) {
                continue;
            }

            size_t idx = 0;
            for (const auto& mapping : _data_reader.column_mapper.mappings()) {
                int res_id;
                RETURN_IF_ERROR(mapping.projection->execute(&current_block, &res_id));
                block->replace_by_position(idx, current_block.get_columns()[res_id]);
                idx++;
            }
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
        if (_data_reader.reader) {
            RETURN_IF_ERROR(close_current_reader());
        }
        return Status::OK();
    }

protected:
    virtual bool _parse_delete_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc& desc) {
        return false;
    }
    // 切换到下一个 reader 的通用流程。
    // 该方法先关闭当前 reader，再打开下一个具体 reader；子类不应重复实现这个循环。
    Status create_next_reader(bool* eos) {
        // 多文件切换的公共流程留在基类：关闭当前 reader，然后打开下一个 reader。
        DCHECK(_data_reader.reader == nullptr);
        // TODO: 创建_data_reader
        // _data_reader = std::make_unique<FileReader>(...);
        if (!_data_reader.reader) {
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
        RETURN_IF_ERROR(_data_reader.reader->get_schema(&file_schema));
        _data_reader.block_schema = file_schema;
        RETURN_IF_ERROR(_data_reader.column_mapper.create_mapping(_options.projected_columns,
                                                                  _partition_values, file_schema));

        auto file_request = std::make_unique<FileScanRequest>();
        RETURN_IF_ERROR(_data_reader.column_mapper.create_scan_request(
                _table_filters, _options.projected_columns, file_request.get()));
        RETURN_IF_ERROR(_data_reader.reader->open(file_request));
        return Status::OK();
    }

    // 关闭当前具体 reader。
    // 该 hook 会被 create_next_reader 和 close 调用；实现应保持幂等。
    virtual Status close_current_reader() {
        RETURN_IF_ERROR(_data_reader.reader->close());
        _data_reader.reader.reset();
        _data_reader.column_mapper.clear();
        _data_reader.block_schema.clear();
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

    struct DataReader {
        std::unique_ptr<FileReader> reader;
        TableColumnMapper column_mapper;
        std::vector<SchemaField> block_schema;
    };
    DataReader _data_reader;
    TableReadOptions _options;
    std::vector<std::unique_ptr<ScanTask>> _scan_tasks;
    // partition key -> value
    std::map<std::string, Field> _partition_values;
    size_t _next_task_idx = 0;
    std::map<int32_t, TableFilter> _table_filters;
    std::unique_ptr<ReadProfile> _profile;
    // Parsed from DELETION_VECTOR in Iceberg and Paimon
    DeleteRows* _delete_rows;
    TFileScanRangeParams* _scan_params;
    io::IOContext* _io_ctx;
    RuntimeState* _runtime_state;
    RuntimeProfile* _scanner_profile;
    FileFormat _format;

private:
    Status _parse_delete_predicates(const SplitReadOptions& options);
};

} // namespace doris::reader
