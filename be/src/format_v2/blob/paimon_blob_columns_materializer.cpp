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

#include "format_v2/blob/paimon_blob_columns_materializer.h"

#include <algorithm>
#include <array>
#include <limits>

#include "core/block/block.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "format_v2/blob/paimon_blob_column_reader.h"
#include "format_v2/blob/paimon_blob_row_position_reader.h"
#include "runtime/file_scan_profile.h"

namespace doris::format::paimon {

PaimonBlobColumnsMaterializer::PaimonBlobColumnsMaterializer() = default;
PaimonBlobColumnsMaterializer::~PaimonBlobColumnsMaterializer() = default;

Status PaimonBlobColumnsMaterializer::init(const std::vector<ColumnDefinition>& projected_columns,
                                           const std::map<std::string, std::string>& options,
                                           RuntimeProfile* profile) {
    reset();
    _blob_columns.clear();
    _input_layout.reset();
    _descriptor_field_ids.clear();
    _initialized = false;
    _output_mode = PaimonBlobOutputMode::INLINE;
    _materialize_timer = nullptr;
    if (profile != nullptr) {
        _materialize_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                profile, "PaimonBlobMaterializeTime",
                file_scan_profile::parent_or_root(profile, file_scan_profile::TABLE_READER), 1);
    }
    const auto descriptor_option_it = options.find("blob-as-descriptor");
    if (descriptor_option_it != options.end()) {
        if (descriptor_option_it->second != "true" && descriptor_option_it->second != "false") {
            return Status::InvalidArgument("Invalid blob-as-descriptor value {}",
                                           descriptor_option_it->second);
        }
        if (descriptor_option_it->second == "true") {
            _output_mode = PaimonBlobOutputMode::DESCRIPTOR;
        }
    }
    for (size_t table_output_index = 0; table_output_index < projected_columns.size();
         ++table_output_index) {
        const auto& column_definition = projected_columns[table_output_index];
        if (contains_file_type(column_definition.type)) {
            _blob_columns.emplace_back(GlobalIndex(table_output_index), column_definition);
        }
    }
    _initialized = true;
    return Status::OK();
}

void PaimonBlobColumnsMaterializer::reset() {
    for (auto& blob_column : _blob_columns) {
        blob_column.reset();
    }
    _payload_reader.reset();
    _split_active = false;
    _file_inputs_prepared = false;
}

void PaimonBlobColumnsMaterializer::configure_mapper_options(
        TableColumnMapperOptions* options) const {
    options->table_reader_materialized_blob_columns.clear();
    if (has_blob_columns()) {
        const auto materialization_input = _input_layout == PaimonBlobInputLayout::ROW_POSITION
                                                   ? BlobMaterializationInput::NONE
                                                   : BlobMaterializationInput::PHYSICAL_COLUMN;
        for (const auto& blob_column : _blob_columns) {
            options->table_reader_materialized_blob_columns.emplace(
                    blob_column.table_output_position(), materialization_input);
        }
    }
}

std::span<const LocalColumnId> PaimonBlobColumnsMaterializer::additional_row_position_columns()
        const {
    static constexpr std::array row_position_columns {LocalColumnId(ROW_POSITION_COLUMN_ID)};
    if (has_blob_columns() && _input_layout == PaimonBlobInputLayout::ROW_POSITION) {
        return row_position_columns;
    }
    return {};
}

Status PaimonBlobColumnsMaterializer::prepare_split(const TPaimonFileDesc& split_params) {
    reset();
    if (!_initialized) {
        return Status::InternalError("Paimon BLOB materializer has not been initialized");
    }
    if (!split_params.__isset.blob_files && !split_params.__isset.blob_descriptor_field_ids) {
        return Status::OK();
    }
    if (split_params.__isset.blob_files && split_params.__isset.blob_descriptor_field_ids) {
        return Status::InvalidArgument("A native Paimon split cannot mix BLOB input layouts");
    }
    const auto input_layout = split_params.__isset.blob_files
                                      ? PaimonBlobInputLayout::ROW_POSITION
                                      : PaimonBlobInputLayout::DESCRIPTOR_COLUMN;
    std::set<int32_t> descriptor_field_ids;
    if (split_params.__isset.blob_descriptor_field_ids) {
        for (const auto field_id : split_params.blob_descriptor_field_ids) {
            if (field_id < 0 || !descriptor_field_ids.insert(field_id).second) {
                return Status::InvalidArgument("Invalid Paimon BLOB descriptor field id {}",
                                               field_id);
            }
        }
    }
    if (_input_layout &&
        (*_input_layout != input_layout || _descriptor_field_ids != descriptor_field_ids)) {
        return Status::InvalidArgument("Paimon BLOB input layout changed within one query");
    }
    if (input_layout == PaimonBlobInputLayout::ROW_POSITION) {
        if (!split_params.__isset.anchor_first_row_id || !split_params.__isset.anchor_row_count ||
            split_params.anchor_first_row_id < 0 || split_params.anchor_row_count <= 0 ||
            split_params.anchor_first_row_id >
                    std::numeric_limits<int64_t>::max() - split_params.anchor_row_count) {
            return Status::InvalidArgument("Missing or invalid native Paimon BLOB split metadata");
        }
        std::map<int32_t, std::vector<const TPaimonBlobFileDesc*>> blob_files_by_field_id;
        for (const auto& blob_file_descriptor : split_params.blob_files) {
            if (!blob_file_descriptor.__isset.field_id || blob_file_descriptor.field_id < 0) {
                return Status::InvalidArgument("Invalid managed BLOB field id");
            }
            blob_files_by_field_id[blob_file_descriptor.field_id].push_back(&blob_file_descriptor);
        }
        // Validate unprojected groups too, without opening any unprojected file.
        for (auto& [field_id, blob_file_descriptors] : blob_files_by_field_id) {
            RETURN_IF_ERROR(PaimonBlobRowPositionReader::validate_blob_files(
                    &blob_file_descriptors, split_params.anchor_first_row_id,
                    split_params.anchor_row_count));
        }
    }
    if (!_input_layout) {
        std::vector<PaimonBlobColumnMaterializer> selected_blob_columns;
        for (const auto& projected_blob_column : _blob_columns) {
            const auto& column_definition = projected_blob_column.column_definition();
            const bool requires_blob_reader =
                    input_layout == PaimonBlobInputLayout::ROW_POSITION
                            ? remove_nullable(column_definition.type)->get_primitive_type() ==
                                      TYPE_FILE
                            : column_definition.has_identifier_field_id() &&
                                      descriptor_field_ids.contains(
                                              column_definition.get_identifier_field_id());
            if (!requires_blob_reader) {
                continue;
            }
            if (!column_definition.has_identifier_field_id()) {
                return Status::InvalidArgument("Native Paimon BLOB requires a field id");
            }
            auto column_reader = PaimonBlobColumnReader::create(input_layout, column_definition,
                                                                _output_mode, _payload_reader);
            selected_blob_columns.emplace_back(projected_blob_column.table_output_position(),
                                               column_definition, std::move(column_reader));
        }
        _blob_columns = std::move(selected_blob_columns);
        _input_layout = input_layout;
        _descriptor_field_ids = std::move(descriptor_field_ids);
    }
    for (auto& blob_column : _blob_columns) {
        auto status = blob_column.prepare_split(split_params);
        if (!status.ok()) {
            reset();
            return status;
        }
    }
    _split_active = true;
    return Status::OK();
}

Status PaimonBlobColumnsMaterializer::prepare_file_inputs(
        const std::vector<ColumnMapping>& column_mappings,
        const FileScanRequest& file_scan_request) {
    if (!has_blob_columns()) {
        return Status::OK();
    }
    _file_inputs_prepared = false;
    for (auto& blob_column : _blob_columns) {
        const auto mapping_it = std::ranges::find_if(column_mappings, [&](const auto& mapping) {
            return mapping.global_index == blob_column.table_output_position();
        });
        if (mapping_it == column_mappings.end()) {
            return Status::InternalError(
                    "Paimon BLOB output is missing its table materialization mapping");
        }
        RETURN_IF_ERROR(
                blob_column.prepare_file_input(*mapping_it, file_scan_request, *_input_layout));
    }
    _file_inputs_prepared = true;
    return Status::OK();
}

Status PaimonBlobColumnsMaterializer::materialize(const Block& file_block, Block* table_block,
                                                  const ReadContext& read_context) {
    if (!has_blob_columns() || table_block->rows() == 0) {
        return Status::OK();
    }
    SCOPED_TIMER(_materialize_timer);
    if (!_file_inputs_prepared) {
        return Status::InternalError("Paimon BLOB file inputs have not been prepared");
    }
    for (auto& blob_column : _blob_columns) {
        RETURN_IF_ERROR(blob_column.materialize(file_block, table_block, read_context));
    }
    return Status::OK();
}

} // namespace doris::format::paimon
