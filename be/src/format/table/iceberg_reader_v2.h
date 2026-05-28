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
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "format/new_parquet/column_reader.h"
#include "format/reader/file_reader.h"
#include "format/reader/table_reader.h"
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
        if (options.current_range.__isset.table_format_params &&
            options.current_range.table_format_params.__isset.iceberg_params) {
            const auto& iceberg_params = options.current_range.table_format_params.iceberg_params;
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
        // 真实实现会把 position delete / deletion vector 转换成 file-local delete 信息。
        (void)request;
        return Status::OK();
    }

    Status customize_file_scan_request(reader::FileScanRequest* file_request) override {
        if (_row_lineage_columns.first_row_id < 0 || !_need_row_lineage_row_id()) {
            return Status::OK();
        }
        DORIS_CHECK(file_request != nullptr);
        const auto row_position_column_id =
                doris::parquet::ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID;
        if (file_request->column_positions.count(row_position_column_id) > 0) {
            return Status::OK();
        }
        _row_position_block_position = file_request->column_positions.size();
        file_request->non_predicate_columns.push_back(row_position_column_id);
        file_request->column_positions.emplace(row_position_column_id, _row_position_block_position);
        _data_reader.block_schema.push_back(
                doris::parquet::ParquetColumnReaderFactory::row_position_schema_field());
        return Status::OK();
    }

    // 在 table block 上应用 equality delete。
    // equality delete 依赖 table-level 列语义，因此不能下沉到 ParquetReader。
    Status apply_equality_deletes(Block* block) {
        // 真实实现会在 table block 上应用 equality delete。
        return Status::OK();
    }

private:
    struct RowLineageColumns {
        int64_t first_row_id = -1;
        int64_t last_updated_sequence_number = -1;
    };

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
        null_map.resize_fill(row_position_column.size(), 0);
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
