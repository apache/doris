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

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/vexpr_context.h"
#include "exprs/vexpr_fwd.h"
#include "format/new_parquet/column_reader.h"
#include "format/reader/column_mapper.h"
#include "format/reader/expr/delete_predicate.h"
#include "format/reader/expr/slot_ref.h"
#include "format/reader/file_reader.h"
#include "runtime/descriptors.h"

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
// TableColumnMapper 负责把它转换成 FileExpressionFilter 或 reader_expression_map。
struct TableFilter {
    // 表达式过滤，适合表达 cast、复杂表达式、复杂列提取等语义。
    VExprContextSPtr conjunct;

    // Table slot ids referenced by conjunct. A single expression filter may depend on multiple
    // columns, while ColumnPredicate pruning still belongs to one concrete column.
    std::vector<int32_t> slot_ids;

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

    std::unique_ptr<io::FileDescription> data_file;
};

struct ReadProfile {
    RuntimeProfile::Counter* num_delete_files;
    RuntimeProfile::Counter* num_delete_rows;
    RuntimeProfile::Counter* parse_delete_file_time;
};

struct TableReadOptions {
    const std::vector<TableColumn> projected_columns;
    const TableColumnPredicates column_predicates;
    // All conjuncts from scan operator
    const VExprContext conjuncts;
    const FileFormat format;
    TFileScanRangeParams* scan_params;
    std::shared_ptr<io::IOContext> io_ctx;
    RuntimeState* runtime_state;
    RuntimeProfile* scanner_profile;
    const bool allow_missing_columns = true;

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
    virtual Status init(TableReadOptions options);

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
        DORIS_CHECK(block->columns() == _projected_columns.size());
        block->clear_column_data(_projected_columns.size());

        while (true) {
            if (*eos) {
                return Status::OK();
            }
            if (!_data_reader.reader) {
                RETURN_IF_ERROR(create_next_reader(eos));
                if (!_data_reader.reader) {
                    DCHECK(*eos);
                    return Status::OK();
                }
            }

            bool current_eof = false;
            _data_reader.block_template.clear_column_data();
            size_t current_rows = 0;
            RETURN_IF_ERROR(_data_reader.reader->get_block(&_data_reader.block_template,
                                                           &current_rows, &current_eof));
            if (current_rows == 0) {
                if (current_eof) {
                    RETURN_IF_ERROR(close_current_reader());
                }
                continue;
            }
            DCHECK_EQ(_data_reader.block_template.columns(), _data_reader.scan_schema.size());

            DORIS_CHECK(block->columns() == _data_reader.column_mapper.mappings().size());
            size_t idx = 0;
            for (const auto& mapping : _data_reader.column_mapper.mappings()) {
                ColumnPtr column;
                RETURN_IF_ERROR(_materialize_mapping_column(mapping, &_data_reader.block_template,
                                                            current_rows, &column));
                block->replace_by_position(idx, std::move(column));
                idx++;
            }
            RETURN_IF_ERROR(finalize_chunk(block));
            RETURN_IF_ERROR(materialize_virtual_columns(block));
            if (current_eof) {
                RETURN_IF_ERROR(close_current_reader());
            }
            return Status::OK();
        }
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
    // Parse deletion vector information from table format specific file description.
    virtual Status _parse_deletion_vector_file(const TTableFormatFileDesc& t_desc,
                                               DeleteFileDesc* desc, bool* has_delete_file) {
        DORIS_CHECK(desc != nullptr);
        DORIS_CHECK(has_delete_file != nullptr);
        *has_delete_file = false;
        return Status::OK();
    }

    // Collect row-position deletes that are not represented as a deletion vector descriptor.
    // TableReader uses the collected _delete_rows to plan a common DeletePredicate.
    virtual Status _collect_position_delete_rows(const TTableFormatFileDesc& t_desc) {
        (void)t_desc;
        return Status::OK();
    }
    // 切换到下一个 reader 的通用流程。
    // 该方法先关闭当前 reader，再打开下一个具体 reader；子类不应重复实现这个循环。
    Status create_next_reader(bool* eos);

    // 打开当前具体 reader。
    // 子类在这里基于当前 split/task 初始化底层 FileReader。
    virtual Status open_reader() {
        std::vector<SchemaField> file_schema;
        RETURN_IF_ERROR(_data_reader.reader->get_schema(&file_schema));
        _data_reader.block_schema = file_schema;
        RETURN_IF_ERROR(_data_reader.column_mapper.create_mapping(_projected_columns,
                                                                  _partition_values, file_schema));
        DORIS_CHECK(_data_reader.column_mapper.mappings().size() == _projected_columns.size());
        RETURN_IF_ERROR(_build_table_filters_from_conjuncts());

        auto file_request = std::make_unique<FileScanRequest>();
        RETURN_IF_ERROR(_data_reader.column_mapper.create_scan_request(
                _table_filters, _table_column_predicates, _projected_columns, file_request.get()));
        RETURN_IF_ERROR(customize_file_scan_request(file_request.get()));
        RETURN_IF_ERROR(_open_local_filter_exprs(*file_request));
        _data_reader.scan_schema.clear();
        _data_reader.block_template.clear();
        _data_reader.scan_schema.resize(file_request->column_positions.size());
        for (const auto& [file_column_id, block_position] : file_request->column_positions) {
            DORIS_CHECK(block_position < _data_reader.scan_schema.size());
            const auto* field = _find_schema_field(_data_reader.block_schema, file_column_id);
            DORIS_CHECK(field != nullptr);
            auto projection_it = file_request->complex_projections.find(file_column_id);
            if (projection_it == file_request->complex_projections.end()) {
                _data_reader.scan_schema[block_position] = *field;
            } else {
                RETURN_IF_ERROR(_project_schema_field(*field, projection_it->second,
                                                      &_data_reader.scan_schema[block_position]));
            }
        }
        _data_reader.block_template.reserve(_data_reader.scan_schema.size());
        for (const auto& field : _data_reader.scan_schema) {
            _data_reader.block_template.insert(
                    {field.type->create_column(), field.type, field.name});
        }
        RETURN_IF_ERROR(_data_reader.reader->open(file_request));
        RETURN_IF_ERROR(_open_mapping_exprs());
        return Status::OK();
    }

    Status _build_table_filters_from_conjuncts();
    Status _open_local_filter_exprs(const FileScanRequest& file_request);

    virtual Status customize_file_scan_request(FileScanRequest* file_request) {
        return _append_position_delete_predicate(file_request);
    }

    static size_t _next_block_position(const FileScanRequest& request) {
        size_t next_position = 0;
        for (const auto& [_, block_position] : request.column_positions) {
            next_position = std::max(next_position, block_position + 1);
        }
        return next_position;
    }

    void _append_file_scan_column(FileScanRequest* request, ColumnId column_id,
                                  std::vector<ColumnId>* scan_columns) {
        DORIS_CHECK(request != nullptr);
        DORIS_CHECK(scan_columns != nullptr);
        if (scan_columns == &request->non_predicate_columns &&
            std::find(request->predicate_columns.begin(), request->predicate_columns.end(),
                      column_id) != request->predicate_columns.end()) {
            return;
        }
        const bool newly_added = request->column_positions.count(column_id) == 0;
        if (newly_added) {
            request->column_positions.emplace(column_id, _next_block_position(*request));
            scan_columns->push_back(column_id);
        } else if (std::find(scan_columns->begin(), scan_columns->end(), column_id) ==
                   scan_columns->end()) {
            scan_columns->push_back(column_id);
        }
        if (scan_columns == &request->predicate_columns) {
            request->non_predicate_columns.erase(
                    std::remove(request->non_predicate_columns.begin(),
                                request->non_predicate_columns.end(), column_id),
                    request->non_predicate_columns.end());
        }
        if (column_id == doris::parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID &&
            _find_schema_field(_data_reader.block_schema, column_id) == nullptr) {
            _data_reader.block_schema.push_back(
                    doris::parquet::ParquetColumnReaderFactory::row_position_schema_field());
        }
    }

    Status _append_position_delete_predicate(FileScanRequest* request) {
        DORIS_CHECK(request != nullptr);
        if (_delete_rows == nullptr || _delete_rows->empty()) {
            return Status::OK();
        }
        const auto row_position_column_id =
                doris::parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID;
        _append_file_scan_column(request, row_position_column_id, &request->predicate_columns);

        auto delete_predicate = std::make_shared<DeletePredicate>(*_delete_rows);
        const auto block_position = request->column_positions.at(row_position_column_id);
        delete_predicate->add_child(TableSlotRef::create_shared(
                cast_set<int>(block_position), cast_set<int>(block_position), -1,
                std::make_shared<DataTypeInt64>(),
                doris::parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_NAME));

        FileExpressionFilter delete_filter;
        delete_filter.delete_conjunct = VExprContext::create_shared(std::move(delete_predicate));
        delete_filter.file_column_ids.push_back(row_position_column_id);
        request->expression_filters.push_back(std::move(delete_filter));
        return Status::OK();
    }

    // 关闭当前具体 reader。
    // 该 hook 会被 create_next_reader 和 close 调用；实现应保持幂等。
    virtual Status close_current_reader() {
        RETURN_IF_ERROR(_data_reader.reader->close());
        _data_reader.reader.reset();
        _data_reader.column_mapper.clear();
        _table_filters.clear();
        _table_column_predicates.clear();
        _data_reader.block_schema.clear();
        _data_reader.scan_schema.clear();
        _data_reader.block_template.clear();
        _current_task.reset();
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

    Status _materialize_mapping_column(const ColumnMapping& mapping, Block* current_block,
                                       size_t current_rows, ColumnPtr* column) {
        if (mapping.projection != nullptr) {
            int res_id;
            RETURN_IF_ERROR(mapping.projection->execute(current_block, &res_id));
            *column = current_block->get_columns()[res_id];
            return Status::OK();
        }
        if (mapping.default_expr != nullptr) {
            if (current_block->rows() == current_rows) {
                int res_id;
                RETURN_IF_ERROR(mapping.default_expr->execute(current_block, &res_id));
                *column = current_block->get_columns()[res_id];
            } else {
                DORIS_CHECK(mapping.is_constant);
                Block eval_block;
                eval_block.insert(
                        {mapping.table_type->create_column_const_with_default_value(current_rows),
                         mapping.table_type, "__table_reader_const_rows"});
                int res_id;
                RETURN_IF_ERROR(mapping.default_expr->execute(&eval_block, &res_id));
                *column = eval_block.get_columns()[res_id];
            }
            return Status::OK();
        }
        *column = mapping.table_type->create_column_const_with_default_value(current_rows);
        return Status::OK();
    }

    Status _open_mapping_exprs() {
        RowDescriptor row_desc;
        for (const auto& mapping : _data_reader.column_mapper.mappings()) {
            if (mapping.projection != nullptr) {
                RETURN_IF_ERROR(mapping.projection->prepare(_runtime_state, row_desc));
                RETURN_IF_ERROR(mapping.projection->open(_runtime_state));
            }
            if (mapping.default_expr != nullptr) {
                RETURN_IF_ERROR(mapping.default_expr->prepare(_runtime_state, row_desc));
                RETURN_IF_ERROR(mapping.default_expr->open(_runtime_state));
            }
        }
        return Status::OK();
    }

    struct DataReader {
        std::unique_ptr<FileReader> reader;
        TableColumnMapper column_mapper;
        std::vector<SchemaField> block_schema;
        std::vector<SchemaField> scan_schema;
        Block block_template;
    };
    DataReader _data_reader;
    std::vector<TableColumn> _projected_columns;
    std::unique_ptr<ScanTask> _current_task;
    std::shared_ptr<io::FileSystemProperties> _system_properties;
    // partition key -> value
    std::map<std::string, Field> _partition_values;
    std::vector<TableFilter> _table_filters;
    TableColumnPredicates _table_column_predicates;
    VExprContext _conjuncts {nullptr};
    std::unique_ptr<ReadProfile> _profile;
    // Parsed from row-position based delete files, including position delete and deletion vector.
    DeleteRows* _delete_rows = nullptr;
    TFileScanRangeParams* _scan_params;
    std::shared_ptr<io::IOContext> _io_ctx;
    RuntimeState* _runtime_state;
    RuntimeProfile* _scanner_profile;
    FileFormat _format;

private:
    static const SchemaField* _find_schema_field(const std::vector<SchemaField>& schema,
                                                 ColumnId column_id) {
        for (const auto& field : schema) {
            if (field.id == column_id) {
                return &field;
            }
        }
        return nullptr;
    }

    static Status _project_schema_field(const SchemaField& field, const FieldProjection& projection,
                                        SchemaField* projected_field) {
        if (projected_field == nullptr) {
            return Status::InvalidArgument("projected_field is null");
        }
        *projected_field = field;
        if (projection.project_all_children || projection.children.empty()) {
            return Status::OK();
        }
        projected_field->children.clear();
        for (const auto& child_projection : projection.children) {
            if (child_projection.file_path.empty()) {
                return Status::InvalidArgument("Empty projection path for field {}", field.name);
            }
            const int32_t child_idx = child_projection.file_path.back();
            if (child_idx < 0 || child_idx >= static_cast<int32_t>(field.children.size())) {
                return Status::InvalidArgument("Invalid projection child index {} for field {}",
                                               child_idx, field.name);
            }
            if (child_projection.file_path != field.children[child_idx].file_path) {
                return Status::InvalidArgument("Invalid projection path for field {}",
                                               field.children[child_idx].name);
            }
            SchemaField projected_child;
            RETURN_IF_ERROR(_project_schema_field(field.children[child_idx], child_projection,
                                                  &projected_child));
            projected_field->children.push_back(std::move(projected_child));
        }
        if (projected_field->children.empty()) {
            return Status::NotSupported("Projection for field {} contains no children", field.name);
        }
        RETURN_IF_ERROR(_rebuild_projected_type(field.type, projected_field));
        return Status::OK();
    }

    static Status _rebuild_projected_type(const DataTypePtr& original_type,
                                          SchemaField* projected_field) {
        if (original_type == nullptr) {
            return Status::InvalidArgument("Cannot rebuild projected type for field {}",
                                           projected_field->name);
        }
        DataTypes child_types;
        Strings child_names;
        child_types.reserve(projected_field->children.size());
        child_names.reserve(projected_field->children.size());
        for (const auto& child : projected_field->children) {
            child_types.push_back(child.type);
            child_names.push_back(child.name);
        }

        const auto primitive_type = remove_nullable(original_type)->get_primitive_type();
        DataTypePtr projected_type;
        switch (primitive_type) {
        case TYPE_STRUCT:
            projected_type = std::make_shared<DataTypeStruct>(child_types, child_names);
            break;
        case TYPE_ARRAY:
            DORIS_CHECK(child_types.size() == 1);
            projected_type = std::make_shared<DataTypeArray>(child_types[0]);
            break;
        case TYPE_MAP:
            DORIS_CHECK(child_types.size() == 1);
            DORIS_CHECK(remove_nullable(child_types[0])->get_primitive_type() == TYPE_STRUCT);
            {
                const auto* entry_type =
                        assert_cast<const DataTypeStruct*>(remove_nullable(child_types[0]).get());
                DORIS_CHECK(entry_type->get_elements().size() == 2);
                projected_type = std::make_shared<DataTypeMap>(entry_type->get_element(0),
                                                               entry_type->get_element(1));
            }
            break;
        default:
            return Status::InvalidArgument("Cannot project children from non-complex field {}",
                                           projected_field->name);
        }
        projected_field->type =
                original_type->is_nullable() ? make_nullable(projected_type) : projected_type;
        return Status::OK();
    }

    // Parse row-position deletes from table format specific parameters, and fill in _delete_rows.
    Status _parse_delete_predicates(const SplitReadOptions& options);
};

} // namespace doris::reader
