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

#include "format_v2/blob/paimon_blob_materializer.h"

#include <limits>
#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_nullable.h"
#include "format_v2/blob/paimon_blob_column_reader.h"
#include "runtime/file_scan_profile.h"

namespace doris::format::paimon {

Status PaimonBlobMaterializer::init(const std::map<std::string, std::string>& options,
                                    RuntimeProfile* profile) {
    _materialize_timer = nullptr;
    if (profile) {
        _materialize_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                profile, "PaimonBlobMaterializeTime",
                file_scan_profile::parent_or_root(profile, file_scan_profile::TABLE_READER), 1);
    }
    _blob_as_descriptor = false;
    const auto it = options.find("blob-as-descriptor");
    if (it != options.end()) {
        if (it->second != "true" && it->second != "false") {
            return Status::InvalidArgument("Invalid blob-as-descriptor value {}", it->second);
        }
        _blob_as_descriptor = it->second == "true";
    }
    return Status::OK();
}

void PaimonBlobMaterializer::reset() {
    _blob_column_readers.clear();
    _blob_column_names.clear();
    _anchor_first_row_id = -1;
    _anchor_row_count = 0;
}

Status PaimonBlobMaterializer::prepare_split(
        const TPaimonFileDesc& params, const std::vector<ColumnDefinition>& projected_columns) {
    reset();
    if (!params.__isset.blob_files) {
        return Status::OK();
    }
    if (!params.__isset.anchor_first_row_id || !params.__isset.anchor_row_count ||
        params.anchor_first_row_id < 0 || params.anchor_row_count <= 0 ||
        params.anchor_first_row_id >
                std::numeric_limits<int64_t>::max() - params.anchor_row_count) {
        return Status::InvalidArgument("Missing or invalid native Paimon BLOB split metadata");
    }
    _anchor_first_row_id = params.anchor_first_row_id;
    _anchor_row_count = params.anchor_row_count;
    std::map<int32_t, std::vector<const TPaimonBlobFileDesc*>> files_by_field;
    for (const auto& file : params.blob_files) {
        if (!file.__isset.field_id || file.field_id < 0) {
            return Status::InvalidArgument("Invalid managed BLOB field id");
        }
        files_by_field[file.field_id].push_back(&file);
    }
    for (auto& entry : files_by_field) {
        RETURN_IF_ERROR(PaimonBlobColumnReader::validate_blob_files(
                &entry.second, _anchor_first_row_id, _anchor_row_count));
    }
    // Resolve table output positions and field ids once per split. Only projected FILE columns
    // retain readers; unprojected BLOB files never open readers or keep decoded indexes alive.
    const std::vector<const TPaimonBlobFileDesc*> empty_blob_files;
    for (size_t index = 0; index < projected_columns.size(); ++index) {
        const auto& column = projected_columns[index];
        if (remove_nullable(column.type)->get_primitive_type() != TYPE_FILE) {
            continue;
        }
        if (!column.has_identifier_field_id()) {
            return Status::InvalidArgument("Native Paimon BLOB requires a field id");
        }
        const auto it = files_by_field.find(column.get_identifier_field_id());
        auto reader = std::make_unique<PaimonBlobColumnReader>(
                column, it == files_by_field.end() ? empty_blob_files : it->second,
                _blob_as_descriptor);
        _blob_column_names.insert(column.name);
        _blob_column_readers.emplace(GlobalIndex(index), std::move(reader));
    }
    return Status::OK();
}

Status PaimonBlobMaterializer::materialize(const ColumnInt64& anchor_row_positions,
                                           Block* table_block, const ReadContext& context) {
    if (!has_blob_columns() || table_block->rows() == 0) {
        return Status::OK();
    }
    // Includes lazy file opening, index decoding, payload reads and FILE construction.
    SCOPED_TIMER(_materialize_timer);
    if (anchor_row_positions.size() != table_block->rows()) {
        return Status::InternalError("BLOB row positions and Parquet output are not aligned");
    }
    for (const auto anchor_row_position : anchor_row_positions.get_data()) {
        if (anchor_row_position < 0 || anchor_row_position >= _anchor_row_count) {
            return Status::Corruption("Parquet row position is outside its Paimon row range");
        }
    }
    for (auto& [output_index, reader] : _blob_column_readers) {
        MutableColumnPtr blob_column_result;
        RETURN_IF_ERROR(reader->read_blob(anchor_row_positions, _anchor_first_row_id, context,
                                          &blob_column_result));
        table_block->replace_by_position(output_index.value(), std::move(blob_column_result));
    }
    return Status::OK();
}

} // namespace doris::format::paimon
