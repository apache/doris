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
#include <gen_cpp/PlanNodes_types.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/vexpr_context.h"
#include "exprs/vexpr_fwd.h"
#include "format/format_common.h"
#include "format/reader/column_mapper.h"
#include "format/reader/expr/delete_predicate.h"
#include "format/reader/expr/literal.h"
#include "format/reader/file_reader.h"
#include "runtime/runtime_profile.h"

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
    RuntimeProfile::Counter* num_delete_files = nullptr;
    RuntimeProfile::Counter* num_delete_rows = nullptr;
    RuntimeProfile::Counter* parse_delete_file_time = nullptr;
};

struct TableReadOptions {
    std::vector<TableColumn> projected_columns;
    // All conjuncts from scan operator
    std::vector<VExprContextSPtr> conjuncts;
    FileFormat format = FileFormat::PARQUET;
    TFileScanRangeParams* scan_params = nullptr;
    io::IOContext* io_ctx = nullptr;
    RuntimeState* runtime_state = nullptr;
    RuntimeProfile* scanner_profile = nullptr;
    // Each task denotes a descriptor of a single file to read, along with file-level metadata such as stats and delete files.
    std::vector<std::unique_ptr<ScanTask>> scan_tasks;

    std::unique_ptr<ReadProfile> profile;
};

struct SplitReadOptions {
    std::map<std::string, Field> partition_values;
    ShardedKVCache* cache = nullptr;
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
        _options = std::move(options);
        _scan_params = _options.scan_params;
        _format = _options.format;
        _io_ctx = _options.io_ctx;
        _runtime_state = _options.runtime_state;
        _scanner_profile = _options.scanner_profile;
        _scan_tasks = std::move(_options.scan_tasks);
        _next_task_idx = 0;
        _current_task = nullptr;
        _profile = std::move(_options.profile);
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
            size_t current_rows = 0;
            Block current_block;
            for (const auto& field : _data_reader.block_schema) {
                // TODO: reuse column's memory
                current_block.insert({field.type->create_column(), field.type, field.name});
            }
            RETURN_IF_ERROR(_data_reader.reader->next(&current_block, &current_rows, &current_eof));

            if (block->columns() == 0) {
                for (const auto& table_column : _options.projected_columns) {
                    block->insert({table_column.type->create_column(), table_column.type,
                                   table_column.name});
                }
            }
            const size_t output_column_count = block->columns();
            size_t idx = 0;
            std::set<ColumnId> projected_file_column_ids;
            for (const auto& mapping : _data_reader.column_mapper.mappings()) {
                if (idx >= output_column_count) {
                    return Status::InternalError(
                            "Column mapping count exceeds output block columns, idx={}, columns={}",
                            idx, output_column_count);
                }
                if (mapping.file_column_id.has_value()) {
                    projected_file_column_ids.insert(*mapping.file_column_id);
                }
                ColumnPtr column;
                RETURN_IF_ERROR(materialize_mapped_column(&current_block, mapping,
                                                          current_rows, &column));
                block->replace_by_position(idx, std::move(column));
                idx++;
            }
            for (size_t i = 0; i < _data_reader.block_schema.size(); ++i) {
                const auto& field = _data_reader.block_schema[i];
                if (!projected_file_column_ids.contains(field.id)) {
                    const auto& helper_column = current_block.get_by_position(i);
                    block->insert({helper_column.column, helper_column.type, helper_column.name});
                }
            }
            RETURN_IF_ERROR(materialize_virtual_columns(block));
            RETURN_IF_ERROR(finalize_chunk(block));
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
        if (_next_task_idx >= _scan_tasks.size()) {
            if (eos != nullptr) {
                *eos = true;
            }
            return Status::OK();
        }
        _current_task = _scan_tasks[_next_task_idx++].get();
        RETURN_IF_ERROR(create_reader_for_task(*_current_task, &_data_reader.reader));
        if (!_data_reader.reader) {
            return Status::InternalError("create_reader_for_task returned null reader");
        }
        if (eos != nullptr) {
            *eos = false;
        }
        RETURN_IF_ERROR(open_reader());
        return Status::OK();
    }

    virtual Status create_reader_for_task(const ScanTask& task,
                                          std::unique_ptr<FileReader>* reader) {
        (void)task;
        (void)reader;
        return Status::NotSupported("TableReader does not know how to create a file reader");
    }

    // 打开当前具体 reader。
    // 子类在这里基于当前 split/task 初始化底层 FileReader。
    virtual Status open_reader() {
        std::vector<SchemaField> file_schema;
        RETURN_IF_ERROR(_data_reader.reader->get_schema(&file_schema));
        RETURN_IF_ERROR(_data_reader.column_mapper.create_mapping(
                _options.projected_columns, _partition_values, file_schema));

        FileScanRequest file_request;
        RETURN_IF_ERROR(_data_reader.column_mapper.create_scan_request(
                _table_filters, _options.projected_columns, &file_request));
        RETURN_IF_ERROR(build_file_block_schema(file_schema, file_request));
        RETURN_IF_ERROR(_data_reader.reader->init(file_request));
        return Status::OK();
    }

    // FileReader returns columns in FileScanRequest::projected_columns order.
    // Keep the input block schema at that same file-local boundary.
    Status build_file_block_schema(const std::vector<SchemaField>& file_schema,
                                   const FileScanRequest& file_request) {
        _data_reader.block_schema.clear();
        for (ColumnId projected_column_id : file_request.projected_columns) {
            const SchemaField* file_field = nullptr;
            for (const auto& field : file_schema) {
                if (field.id == projected_column_id) {
                    file_field = &field;
                    break;
                }
            }
            if (file_field == nullptr) {
                return Status::InternalError(
                        "File scan request references missing file column id {}",
                        projected_column_id);
            }
            _data_reader.block_schema.push_back(*file_field);
        }
        return Status::OK();
    }

    // 关闭当前具体 reader。
    // 该 hook 会被 create_next_reader 和 close 调用；实现应保持幂等。
    virtual Status close_current_reader() {
        if (_data_reader.reader) {
            RETURN_IF_ERROR(_data_reader.reader->close());
            _data_reader.reader.reset();
        }
        _data_reader.column_mapper.clear();
        _data_reader.block_schema.clear();
        _current_task = nullptr;
        return Status::OK();
    }

    // 处理 table/global schema block 的表格式语义，例如 residual filter、delete files、
    // helper column 清理等。
    virtual Status finalize_chunk(Block* block) { return Status::OK(); }

    // 物化虚拟列。
    // 例如 _row_id、_last_updated_sequence_number 等，它们不来自文件物理列。
    virtual Status materialize_virtual_columns(Block* table_block) {
        // 真实实现会物化 _row_id、_last_updated_sequence_number 等 Iceberg 虚拟列。
        return Status::OK();
    }

    Status materialize_mapped_column(Block* source_block, const ColumnMapping& mapping, size_t rows,
                                     ColumnPtr* column) {
        if (column == nullptr || source_block == nullptr) {
            return Status::InvalidArgument("invalid mapped column arguments");
        }
        if (mapping.projection != nullptr) {
            int res_id = -1;
            RETURN_IF_ERROR(mapping.projection->execute(source_block, &res_id));
            ColumnPtr source_column = source_block->get_columns()[res_id];
            if (!mapping.child_mappings.empty()) {
                RETURN_IF_ERROR(materialize_complex_column(source_block, mapping, source_column,
                                                           rows, column));
            } else {
                *column = std::move(source_column);
            }
            return Status::OK();
        }
        if (mapping.default_expr != nullptr) {
            int res_id = -1;
            RETURN_IF_ERROR(mapping.default_expr->execute(source_block, &res_id));
            *column = source_block->get_columns()[res_id]->convert_to_full_column_if_const();
            return Status::OK();
        }
        auto missing_column = mapping.table_type->create_column();
        missing_column->insert_many_defaults(rows);
        *column = std::move(missing_column);
        return Status::OK();
    }

    Status materialize_complex_column(Block* source_block, const ColumnMapping& mapping,
                                      ColumnPtr source_column, size_t rows, ColumnPtr* column) {
        if (mapping.table_type == nullptr) {
            return Status::InvalidArgument("complex column mapping type is null");
        }
        switch (remove_nullable(mapping.table_type)->get_primitive_type()) {
        case PrimitiveType::TYPE_STRUCT:
            return materialize_struct_column(source_block, mapping, std::move(source_column), rows,
                                             column);
        case PrimitiveType::TYPE_ARRAY:
            return materialize_array_column(source_block, mapping, std::move(source_column), rows,
                                            column);
        case PrimitiveType::TYPE_MAP:
            return materialize_map_column(source_block, mapping, std::move(source_column), rows,
                                          column);
        default:
            return Status::InternalError(
                    "unexpected complex mapping type {} for table column id {}",
                    mapping.table_type->get_name(), mapping.table_column_id);
        }
    }

    Status materialize_nested_child_column(Block* source_block, const ColumnMapping& child_mapping,
                                           ColumnPtr source_column, size_t rows,
                                           ColumnPtr* column) {
        if (column == nullptr || !source_column) {
            return Status::InvalidArgument("invalid nested child mapping arguments");
        }
        if (!child_mapping.child_mappings.empty()) {
            return materialize_complex_column(source_block, child_mapping, std::move(source_column),
                                              rows, column);
        }
        *column = source_column->convert_to_full_column_if_const();
        return Status::OK();
    }

    Status materialize_struct_child_column(Block* source_block, const ColumnMapping& child_mapping,
                                           const ColumnStruct& source_struct, size_t rows,
                                           ColumnPtr* column) {
        if (child_mapping.file_child_index.has_value()) {
            if (*child_mapping.file_child_index >= source_struct.tuple_size()) {
                return Status::InternalError(
                        "Struct child index {} exceeds source struct child count {}",
                        *child_mapping.file_child_index, source_struct.tuple_size());
            }
            ColumnPtr source_child = source_struct.get_column_ptr(*child_mapping.file_child_index);
            return materialize_nested_child_column(source_block, child_mapping,
                                                   std::move(source_child), rows, column);
        }
        return materialize_mapped_column(source_block, child_mapping, rows, column);
    }

    Status materialize_struct_column(Block* source_block, const ColumnMapping& mapping,
                                     ColumnPtr source_column, size_t rows, ColumnPtr* column) {
        if (column == nullptr || !source_column) {
            return Status::InvalidArgument("invalid struct column mapping arguments");
        }

        source_column = source_column->convert_to_full_column_if_const();
        const auto* nullable_source = check_and_get_column<ColumnNullable>(source_column.get());
        const ColumnStruct* source_struct = nullptr;
        ColumnPtr source_null_map;
        if (nullable_source != nullptr) {
            source_struct = check_and_get_column<ColumnStruct>(
                    nullable_source->get_nested_column_ptr().get());
            source_null_map = nullable_source->get_null_map_column_ptr()->clone_resized(rows);
        } else {
            source_struct = check_and_get_column<ColumnStruct>(source_column.get());
        }
        if (source_struct == nullptr) {
            return Status::InternalError(
                    "Expected source column for table column id {} to be "
                    "Struct, actual column={}",
                    mapping.table_column_id, source_column->get_name());
        }

        Columns child_columns;
        child_columns.reserve(mapping.child_mappings.size());
        for (const auto& child_mapping : mapping.child_mappings) {
            ColumnPtr child_column;
            RETURN_IF_ERROR(materialize_struct_child_column(source_block, child_mapping,
                                                            *source_struct, rows, &child_column));
            child_columns.push_back(child_column->convert_to_full_column_if_const());
        }

        ColumnPtr struct_column = ColumnStruct::create(child_columns);
        if (mapping.table_type != nullptr && mapping.table_type->is_nullable()) {
            if (!source_null_map) {
                auto null_map = ColumnUInt8::create();
                null_map->insert_many_defaults(rows);
                source_null_map = std::move(null_map);
            }
            *column = ColumnNullable::create(struct_column, source_null_map);
        } else {
            if (source_null_map) {
                return Status::NotSupported(
                        "nullable file struct cannot be mapped to non-nullable table struct, "
                        "table column id={}",
                        mapping.table_column_id);
            }
            *column = std::move(struct_column);
        }
        return Status::OK();
    }

    Status materialize_array_column(Block* source_block, const ColumnMapping& mapping,
                                    ColumnPtr source_column, size_t rows, ColumnPtr* column) {
        if (column == nullptr || !source_column) {
            return Status::InvalidArgument("invalid array column mapping arguments");
        }
        if (mapping.child_mappings.size() != 1) {
            return Status::InternalError(
                    "array column mapping must have one element mapping, got {}",
                    mapping.child_mappings.size());
        }

        source_column = source_column->convert_to_full_column_if_const();
        const auto* nullable_source = check_and_get_column<ColumnNullable>(source_column.get());
        const ColumnArray* source_array = nullptr;
        ColumnPtr source_null_map;
        if (nullable_source != nullptr) {
            source_array = check_and_get_column<ColumnArray>(
                    nullable_source->get_nested_column_ptr().get());
            source_null_map = nullable_source->get_null_map_column_ptr()->clone_resized(rows);
        } else {
            source_array = check_and_get_column<ColumnArray>(source_column.get());
        }
        if (source_array == nullptr) {
            return Status::InternalError(
                    "Expected source column for table column id {} to be Array, actual column={}",
                    mapping.table_column_id, source_column->get_name());
        }

        ColumnPtr nested_column;
        RETURN_IF_ERROR(materialize_nested_child_column(
                source_block, mapping.child_mappings[0], source_array->get_data_ptr(),
                source_array->get_data().size(), &nested_column));
        ColumnPtr array_column = ColumnArray::create(
                nested_column, source_array->get_offsets_ptr()->clone_resized(rows));
        if (mapping.table_type != nullptr && mapping.table_type->is_nullable()) {
            if (!source_null_map) {
                auto null_map = ColumnUInt8::create();
                null_map->insert_many_defaults(rows);
                source_null_map = std::move(null_map);
            }
            *column = ColumnNullable::create(array_column, source_null_map);
        } else {
            if (source_null_map) {
                return Status::NotSupported(
                        "nullable file array cannot be mapped to non-nullable table array, "
                        "table column id={}",
                        mapping.table_column_id);
            }
            *column = std::move(array_column);
        }
        return Status::OK();
    }

    Status materialize_map_column(Block* source_block, const ColumnMapping& mapping,
                                  ColumnPtr source_column, size_t rows, ColumnPtr* column) {
        if (column == nullptr || !source_column) {
            return Status::InvalidArgument("invalid map column mapping arguments");
        }
        if (mapping.child_mappings.size() != 2) {
            return Status::InternalError("map column mapping must have key/value mappings, got {}",
                                         mapping.child_mappings.size());
        }

        source_column = source_column->convert_to_full_column_if_const();
        const auto* nullable_source = check_and_get_column<ColumnNullable>(source_column.get());
        const ColumnMap* source_map = nullptr;
        ColumnPtr source_null_map;
        if (nullable_source != nullptr) {
            source_map =
                    check_and_get_column<ColumnMap>(nullable_source->get_nested_column_ptr().get());
            source_null_map = nullable_source->get_null_map_column_ptr()->clone_resized(rows);
        } else {
            source_map = check_and_get_column<ColumnMap>(source_column.get());
        }
        if (source_map == nullptr) {
            return Status::InternalError(
                    "Expected source column for table column id {} to be Map, actual column={}",
                    mapping.table_column_id, source_column->get_name());
        }

        ColumnPtr key_column;
        RETURN_IF_ERROR(materialize_nested_child_column(
                source_block, mapping.child_mappings[0], source_map->get_keys_ptr(),
                source_map->get_keys().size(), &key_column));
        ColumnPtr value_column;
        RETURN_IF_ERROR(materialize_nested_child_column(
                source_block, mapping.child_mappings[1], source_map->get_values_ptr(),
                source_map->get_values().size(), &value_column));
        ColumnPtr map_column = ColumnMap::create(
                key_column, value_column, source_map->get_offsets_ptr()->clone_resized(rows));
        if (mapping.table_type != nullptr && mapping.table_type->is_nullable()) {
            if (!source_null_map) {
                auto null_map = ColumnUInt8::create();
                null_map->insert_many_defaults(rows);
                source_null_map = std::move(null_map);
            }
            *column = ColumnNullable::create(map_column, source_null_map);
        } else {
            if (source_null_map) {
                return Status::NotSupported(
                        "nullable file map cannot be mapped to non-nullable table map, "
                        "table column id={}",
                        mapping.table_column_id);
            }
            *column = std::move(map_column);
        }
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
    const ScanTask* _current_task = nullptr;
    // partition key -> value
    std::map<std::string, Field> _partition_values;
    size_t _next_task_idx = 0;
    std::map<int32_t, TableFilter> _table_filters;
    std::unique_ptr<ReadProfile> _profile;
    // Parsed from DELETION_VECTOR in Iceberg and Paimon
    DeleteRows* _delete_rows = nullptr;
    TFileScanRangeParams* _scan_params = nullptr;
    io::IOContext* _io_ctx = nullptr;
    RuntimeState* _runtime_state = nullptr;
    RuntimeProfile* _scanner_profile = nullptr;
    FileFormat _format = FileFormat::PARQUET;

private:
    Status _parse_delete_predicates(const SplitReadOptions& options);
};

} // namespace doris::reader
