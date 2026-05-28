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
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "format/new_parquet/column_reader.h"
#include "format/reader/file_reader.h"
#include "format/reader/expr/delete_predicate.h"
#include "format/reader/expr/slot_ref.h"
#include "format/reader/table_reader.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class Block;
} // namespace doris

namespace doris::iceberg {

// Iceberg table-level reader。
// 该层继承 TableReader，复用多文件编排和动态分区裁剪等通用能力；同时组合
// FileReader 完成 data file 物理读取，不继承具体文件格式 reader。
class IcebergTableReader : public reader::TableReader {
public:
    ~IcebergTableReader() override = default;

    Status prepare_split(const reader::SplitReadOptions& options) override {
        _row_lineage_columns = {};
        _iceberg_params = nullptr;
        _delete_predicates_initialized = false;
        _position_delete_rows.clear();
        _equality_delete_files.clear();
        if (options.current_range.__isset.table_format_params &&
            options.current_range.table_format_params.__isset.iceberg_params) {
            const auto& iceberg_params = options.current_range.table_format_params.iceberg_params;
            _iceberg_params = &iceberg_params;
            if (iceberg_params.__isset.first_row_id) {
                _row_lineage_columns.first_row_id = iceberg_params.first_row_id;
            }
            if (iceberg_params.__isset.last_updated_sequence_number) {
                _row_lineage_columns.last_updated_sequence_number =
                        iceberg_params.last_updated_sequence_number;
            }
        }
        return TableReader::prepare_split(options);
    }

protected:
    // 将 file-local block 转换为 table/global schema block。
    // 这里执行 ColumnMapping 中的 finalize_expr、缺失列填充、partition/generated 列
    // 物化以及复杂列 remap。
    Status finalize_chunk(Block* block) override {
        // 真实实现会根据 ColumnMapping 执行 finalize_expr/default/partition/generated
        // expressions，把 file-local block 写成 table block。
        RETURN_IF_ERROR(apply_equality_deletes(block));
        return Status::OK();
    }

    // 物化 Iceberg 虚拟列。
    // 例如 _row_id、_last_updated_sequence_number 等，它们不来自 Parquet 文件物理列。
    Status materialize_virtual_columns(Block* table_block) override {
        for (size_t column_idx = 0; column_idx < _data_reader.column_mapper.mappings().size();
             ++column_idx) {
            const auto& mapping = _data_reader.column_mapper.mappings()[column_idx];
            switch (mapping.virtual_column_type) {
            case reader::TableVirtualColumnType::ROW_ID:
                RETURN_IF_ERROR(_materialize_row_lineage_row_id(table_block, column_idx));
                break;
            case reader::TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER:
                RETURN_IF_ERROR(_materialize_row_lineage_last_updated_sequence_number(table_block,
                                                                                      column_idx));
                break;
            case reader::TableVirtualColumnType::INVALID:
                break;
            }
        }
        return Status::OK();
    }

    // 将 Iceberg position delete / deletion vector 转换成底层 reader 可消费的删除信息。
    // 这一步发生在读取 data file 前，因此会修改 FileScanRequest。
    Status apply_position_deletes(reader::FileScanRequest* request) {
        if (_position_delete_rows.empty()) {
            return Status::OK();
        }
        const auto row_position_column_id =
                doris::parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID;
        _append_file_scan_column(request, row_position_column_id, &request->predicate_columns);

        auto delete_predicate = std::make_shared<DeletePredicate>(_position_delete_rows);
        const auto block_position = request->column_positions.at(row_position_column_id);
        delete_predicate->add_child(TableSlotRef::create_shared(
                cast_set<int>(block_position), cast_set<int>(block_position), -1,
                std::make_shared<DataTypeInt64>(),
                doris::parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_NAME));

        reader::FileExpressionFilter delete_filter;
        delete_filter.delete_conjunct = VExprContext::create_shared(std::move(delete_predicate));
        delete_filter.file_column_ids.push_back(row_position_column_id);
        request->expression_filters.push_back(std::move(delete_filter));
        return Status::OK();
    }

    Status customize_file_scan_request(reader::FileScanRequest* file_request) override {
        DORIS_CHECK(file_request != nullptr);
        RETURN_IF_ERROR(_init_delete_predicates());
        RETURN_IF_ERROR(apply_position_deletes(file_request));
        if (_row_lineage_columns.first_row_id >= 0 && _need_row_lineage_row_id()) {
            RETURN_IF_ERROR(_append_row_position_output_column(file_request));
        }
        return Status::OK();
    }

    Status _parse_delete_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc* desc,
                              bool* has_delete_file) override {
        DORIS_CHECK(desc != nullptr);
        DORIS_CHECK(has_delete_file != nullptr);
        *has_delete_file = false;
        if (!t_desc.__isset.iceberg_params) {
            return Status::OK();
        }
        const auto& iceberg_params = t_desc.iceberg_params;
        if (!iceberg_params.__isset.format_version ||
            iceberg_params.format_version < MIN_SUPPORT_DELETE_FILES_VERSION ||
            !iceberg_params.__isset.delete_files || iceberg_params.delete_files.empty()) {
            return Status::OK();
        }

        const TIcebergDeleteFileDesc* deletion_vector = nullptr;
        for (const auto& delete_file : iceberg_params.delete_files) {
            if (!delete_file.__isset.content || delete_file.content != DELETION_VECTOR) {
                continue;
            }
            if (deletion_vector != nullptr) {
                return Status::DataQualityError("This iceberg data file has multiple DVs.");
            }
            deletion_vector = &delete_file;
        }
        if (deletion_vector == nullptr) {
            return Status::OK();
        }
        if (!deletion_vector->__isset.content_offset ||
            !deletion_vector->__isset.content_size_in_bytes) {
            return Status::InternalError("Deletion vector is missing content offset or length");
        }

        desc->key = _iceberg_delete_vector_cache_key(*deletion_vector);
        desc->path = deletion_vector->path;
        desc->start_offset = deletion_vector->content_offset;
        desc->size = deletion_vector->content_size_in_bytes;
        desc->file_size = -1;
        desc->format = DeleteFileDesc::Format::ICEBERG;
        *has_delete_file = true;
        return Status::OK();
    }

    // 在 table block 上应用 equality delete。
    // equality delete 依赖 table-level 列语义，因此不能下沉到 ParquetReader。
    Status apply_equality_deletes(Block* block) {
        if (!_equality_delete_files.empty()) {
            return Status::NotSupported("Iceberg equality delete is not supported by TableReader");
        }
        return Status::OK();
    }

private:
    static constexpr int MIN_SUPPORT_DELETE_FILES_VERSION = 2;
    static constexpr int POSITION_DELETE = 1;
    static constexpr int EQUALITY_DELETE = 2;
    static constexpr int DELETION_VECTOR = 3;

    struct RowLineageColumns {
        int64_t first_row_id = -1;
        int64_t last_updated_sequence_number = -1;
    };

    class PositionDeleteCollector final : public IcebergPositionDeleteVisitor {
    public:
        PositionDeleteCollector(std::string data_file_path,
                                std::map<std::string, reader::DeleteRows>* rows)
                : _data_file_path(std::move(data_file_path)), _rows(rows) {}

        Status visit(const std::string& file_path, int64_t pos) override {
            if (file_path == _data_file_path) {
                (*_rows)[file_path].push_back(pos);
            }
            return Status::OK();
        }

    private:
        std::string _data_file_path;
        std::map<std::string, reader::DeleteRows>* _rows = nullptr;
    };

    static size_t _next_block_position(const reader::FileScanRequest& request) {
        size_t next_position = 0;
        for (const auto& [_, block_position] : request.column_positions) {
            next_position = std::max(next_position, block_position + 1);
        }
        return next_position;
    }

    static std::string _iceberg_delete_vector_cache_key(const TIcebergDeleteFileDesc& delete_file) {
        const std::string key_prefix = "iceberg_dv:";
        std::string key;
        key.resize(key_prefix.size() + delete_file.path.size() + sizeof(delete_file.content_offset) +
                   sizeof(delete_file.content_size_in_bytes));
        char* data = key.data();
        memcpy(data, key_prefix.data(), key_prefix.size());
        data += key_prefix.size();
        memcpy(data, delete_file.path.data(), delete_file.path.size());
        data += delete_file.path.size();
        memcpy(data, &delete_file.content_offset, sizeof(delete_file.content_offset));
        data += sizeof(delete_file.content_offset);
        memcpy(data, &delete_file.content_size_in_bytes, sizeof(delete_file.content_size_in_bytes));
        return key;
    }

    void _append_file_scan_column(reader::FileScanRequest* request, reader::ColumnId column_id,
                                  std::vector<reader::ColumnId>* scan_columns) {
        DORIS_CHECK(request != nullptr);
        DORIS_CHECK(scan_columns != nullptr);
        const bool newly_added = request->column_positions.count(column_id) == 0;
        if (newly_added) {
            request->column_positions.emplace(column_id, _next_block_position(*request));
            scan_columns->push_back(column_id);
        }
        if (column_id == doris::parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID &&
            _find_row_position_schema_field() == nullptr) {
            _data_reader.block_schema.push_back(
                    doris::parquet::ParquetColumnReaderFactory::row_position_schema_field());
        }
    }

    Status _append_row_position_output_column(reader::FileScanRequest* request) {
        const auto row_position_column_id =
                doris::parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID;
        _append_file_scan_column(request, row_position_column_id, &request->non_predicate_columns);
        _row_position_block_position = request->column_positions.at(row_position_column_id);
        return Status::OK();
    }

    const reader::SchemaField* _find_row_position_schema_field() const {
        const auto row_position_column_id =
                doris::parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID;
        for (const auto& field : _data_reader.block_schema) {
            if (field.id == row_position_column_id) {
                return &field;
            }
        }
        return nullptr;
    }

    Status _init_delete_predicates() {
        if (_delete_predicates_initialized || _iceberg_params == nullptr ||
            !_iceberg_params->__isset.format_version ||
            _iceberg_params->format_version < MIN_SUPPORT_DELETE_FILES_VERSION ||
            !_iceberg_params->__isset.delete_files || _iceberg_params->delete_files.empty()) {
            _delete_predicates_initialized = true;
            return Status::OK();
        }

        std::vector<TIcebergDeleteFileDesc> position_delete_files;
        std::vector<TIcebergDeleteFileDesc> deletion_vector_files;
        for (const auto& delete_file : _iceberg_params->delete_files) {
            if (!delete_file.__isset.content) {
                continue;
            }
            if (delete_file.content == POSITION_DELETE) {
                position_delete_files.push_back(delete_file);
            } else if (delete_file.content == EQUALITY_DELETE) {
                _equality_delete_files.push_back(delete_file);
            } else if (delete_file.content == DELETION_VECTOR) {
                deletion_vector_files.push_back(delete_file);
            }
        }

        if (!deletion_vector_files.empty()) {
            DORIS_CHECK(deletion_vector_files.size() == 1);
            if (_delete_rows != nullptr) {
                _position_delete_rows = *_delete_rows;
            }
        } else if (!position_delete_files.empty()) {
            RETURN_IF_ERROR(_read_position_delete_files(position_delete_files));
        }

        _delete_predicates_initialized = true;
        return Status::OK();
    }

    std::string _data_file_path() const {
        if (_iceberg_params != nullptr && _iceberg_params->__isset.original_file_path) {
            return _iceberg_params->original_file_path;
        }
        DORIS_CHECK(_current_task != nullptr);
        DORIS_CHECK(_current_task->data_file != nullptr);
        return _current_task->data_file->path;
    }

    IcebergDeleteFileReaderOptions _delete_file_reader_options(IcebergDeleteFileIOContext* io_ctx,
                                                               TFileScanRangeParams* scan_params) {
        IcebergDeleteFileReaderOptions options;
        options.state = _runtime_state;
        options.profile = _scanner_profile;
        options.scan_params = scan_params;
        options.io_ctx = &io_ctx->io_ctx;
        options.fs_name = _current_task != nullptr && _current_task->data_file != nullptr
                                  ? &_current_task->data_file->fs_name
                                  : nullptr;
        return options;
    }

    Status _read_position_delete_files(const std::vector<TIcebergDeleteFileDesc>& delete_files) {
        TFileScanRangeParams delete_scan_params = _scan_params == nullptr
                                                          ? TFileScanRangeParams()
                                                          : *_scan_params;
        IcebergDeleteFileIOContext delete_io_ctx(_runtime_state);
        auto options = _delete_file_reader_options(&delete_io_ctx, &delete_scan_params);
        std::map<std::string, reader::DeleteRows> rows_by_file;
        const auto data_file_path = _data_file_path();
        PositionDeleteCollector visitor(data_file_path, &rows_by_file);
        for (const auto& delete_file : delete_files) {
            RETURN_IF_ERROR(read_iceberg_position_delete_file(delete_file, options, &visitor));
        }
        auto rows_it = rows_by_file.find(data_file_path);
        if (rows_it == rows_by_file.end()) {
            return Status::OK();
        }
        _position_delete_rows = std::move(rows_it->second);
        std::sort(_position_delete_rows.begin(), _position_delete_rows.end());
        _position_delete_rows.erase(
                std::unique(_position_delete_rows.begin(), _position_delete_rows.end()),
                _position_delete_rows.end());
        return Status::OK();
    }

    Status _materialize_row_lineage_row_id(Block* table_block, size_t column_idx) {
        if (_row_lineage_columns.first_row_id < 0) {
            return Status::OK();
        }
        DORIS_CHECK(_row_position_block_position < _data_reader.block_template.columns());
        const auto& row_position_column = assert_cast<const ColumnInt64&>(
                *_data_reader.block_template.get_by_position(_row_position_block_position).column);
        DORIS_CHECK(row_position_column.size() == table_block->rows());
        auto column = table_block->get_by_position(column_idx)
                              .column->convert_to_full_column_if_const()
                              ->assume_mutable();
        auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
        auto& null_map = nullable_column->get_null_map_data();
        auto& data =
                assert_cast<ColumnInt64&>(*nullable_column->get_nested_column_ptr()).get_data();
        null_map.resize(row_position_column.size());
        std::fill(null_map.begin(), null_map.end(), 0);
        data.resize(row_position_column.size());
        for (size_t row = 0; row < row_position_column.size(); ++row) {
            data[row] = _row_lineage_columns.first_row_id + row_position_column.get_element(row);
        }
        table_block->replace_by_position(column_idx, std::move(column));
        return Status::OK();
    }

    Status _materialize_row_lineage_last_updated_sequence_number(Block* table_block,
                                                                 size_t column_idx) {
        if (_row_lineage_columns.last_updated_sequence_number < 0) {
            return Status::OK();
        }
        const auto rows = table_block->rows();
        auto data_column = table_block->get_by_position(column_idx).type->create_column();
        data_column->insert(Field::create_field<TYPE_BIGINT>(
                _row_lineage_columns.last_updated_sequence_number));
        auto column = ColumnConst::create(std::move(data_column), rows);
        table_block->replace_by_position(column_idx, std::move(column));
        return Status::OK();
    }

    RowLineageColumns _row_lineage_columns;
    size_t _row_position_block_position = 0;
    const TIcebergFileDesc* _iceberg_params = nullptr;
    bool _delete_predicates_initialized = false;
    reader::DeleteRows _position_delete_rows;
    std::vector<TIcebergDeleteFileDesc> _equality_delete_files;

    bool _need_row_lineage_row_id() const {
        for (const auto& mapping : _data_reader.column_mapper.mappings()) {
            if (mapping.virtual_column_type == reader::TableVirtualColumnType::ROW_ID) {
                return true;
            }
        }
        return false;
    }
};

} // namespace doris::iceberg
