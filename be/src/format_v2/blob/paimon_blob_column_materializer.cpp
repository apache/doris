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

#include "format_v2/blob/paimon_blob_column_materializer.h"

#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_number.h"

namespace doris::format::paimon {

PaimonBlobColumnMaterializer::PaimonBlobColumnMaterializer(
        GlobalIndex table_output_position, ColumnDefinition column_definition,
        std::unique_ptr<PaimonBlobColumnReader> column_reader)
        : _table_output_position(table_output_position),
          _column_definition(std::move(column_definition)),
          _column_reader(std::move(column_reader)) {}

void PaimonBlobColumnMaterializer::reset() {
    if (_column_reader != nullptr) {
        _column_reader->reset();
    }
    _file_input_position.reset();
    _input_column_type.reset();
    _file_input_prepared = false;
}

Status PaimonBlobColumnMaterializer::prepare_split(const TPaimonFileDesc& split_params) {
    _file_input_position.reset();
    _input_column_type.reset();
    _file_input_prepared = false;
    if (_column_reader == nullptr) {
        return Status::InternalError("Paimon BLOB column reader has not been selected");
    }
    return _column_reader->prepare_split(split_params);
}

Status PaimonBlobColumnMaterializer::prepare_file_input(const ColumnMapping& column_mapping,
                                                        const FileScanRequest& file_scan_request,
                                                        PaimonBlobInputLayout input_layout) {
    _file_input_prepared = false;
    _file_input_position.reset();
    _input_column_type.reset();
    if (column_mapping.global_index != _table_output_position ||
        !column_mapping.requires_blob_materialization ||
        !column_mapping.table_type->equals(*_column_definition.type)) {
        return Status::InternalError(
                "Paimon BLOB output is missing its table materialization mapping");
    }
    std::optional<LocalColumnId> input_column_id;
    if (input_layout == PaimonBlobInputLayout::ROW_POSITION) {
        input_column_id = LocalColumnId(ROW_POSITION_COLUMN_ID);
        _input_column_type = std::make_shared<DataTypeInt64>();
    } else if (column_mapping.file_local_id) {
        input_column_id = LocalColumnId(*column_mapping.file_local_id);
        _input_column_type = column_mapping.file_type;
    }
    if (input_column_id) {
        const auto input_position_it = file_scan_request.local_positions.find(*input_column_id);
        if (input_position_it == file_scan_request.local_positions.end() ||
            _input_column_type == nullptr) {
            return Status::InternalError("Paimon BLOB input is missing its physical column");
        }
        _file_input_position = input_position_it->second;
    }
    _file_input_prepared = true;
    return Status::OK();
}

Status PaimonBlobColumnMaterializer::materialize(const Block& file_block, Block* table_block,
                                                 const PaimonBlobReadContext& read_context) {
    if (!_file_input_prepared || _column_reader == nullptr) {
        return Status::InternalError("Paimon BLOB file input has not been prepared");
    }
    ColumnPtr result_column;
    if (_file_input_position) {
        const auto& input_column = file_block.get_by_position(_file_input_position->value());
        if (input_column.column->size() != table_block->rows()) {
            return Status::InternalError("Paimon BLOB input rows are not aligned");
        }
        RETURN_IF_ERROR(_column_reader->read(input_column.column, _input_column_type, read_context,
                                             &result_column));
    } else {
        if (!_column_definition.type->is_nullable()) {
            return Status::Corruption("Missing required Paimon BLOB column");
        }
        result_column =
                _column_definition.type->create_column_const_with_default_value(table_block->rows())
                        ->convert_to_full_column_if_const();
    }
    table_block->replace_by_position(_table_output_position.value(), std::move(result_column));
    return Status::OK();
}

} // namespace doris::format::paimon
