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

#include "format_v2/table/deltalake_reader.h"

#include <algorithm>

#include "format/table/deletion_vector_reader.h"
#include "format_v2/column_mapper.h"
#include "format_v2/parquet/parquet_reader.h"
#include "format_v2/table/schema_history_util.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace doris::format::deltalake {

namespace {

bool has_data_field_id(const ColumnDefinition& column) {
    return column.column_type == ColumnType::DATA_COLUMN &&
           (column.has_identifier_field_id() ||
            std::ranges::any_of(column.children, has_data_field_id));
}

void clear_parquet_field_ids(std::vector<ColumnDefinition>& columns) {
    for (auto& column : columns) {
        if (column.column_type == ColumnType::DATA_COLUMN) {
            column.identifier = Field {};
            clear_parquet_field_ids(column.children);
        }
    }
}

} // namespace

Status DeltaReader::prepare_split(const SplitReadOptions& options) {
    {
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.prepare_split_timer);
        _split_schema_id = -1;
        const auto& params = options.current_range.table_format_params;
        if (params.__isset.deltalake_params && params.deltalake_params.__isset.schema_id) {
            _split_schema_id = params.deltalake_params.schema_id;
            DORIS_CHECK(can_map_by_history_schema(_scan_params, _split_schema_id));
        }
    }
    RETURN_IF_ERROR(TableReader::prepare_split(options));
    SCOPED_TIMER(_profile.total_timer);
    SCOPED_TIMER(_profile.prepare_split_timer);
    if (current_split_pruned()) {
        return Status::OK();
    }
    // Delta commits replace data files instead of mutating them. A file may acquire a new DV,
    // but its Parquet bytes (and therefore its page-cache identity) remain unchanged.
    mark_current_data_file_immutable();
    return Status::OK();
}

TableColumnMappingMode DeltaReader::mapping_mode() const {
    // Both mapped modes use table IDs internally. NAME obtains file-side IDs by physical name,
    // while ID uses the ones in the Parquet footer.
    if (_scan_params != nullptr && _scan_params->__isset.deltalake_column_mapping_mode) {
        return _scan_params->deltalake_column_mapping_mode == TDeltaLakeColumnMappingMode::NONE
                       ? TableColumnMappingMode::BY_NAME
                       : TableColumnMappingMode::BY_FIELD_ID;
    }
    return can_map_by_history_schema(_scan_params, _split_schema_id)
                   ? TableColumnMappingMode::BY_FIELD_ID
                   : TableColumnMappingMode::BY_NAME;
}

Status DeltaReader::create_file_reader(std::unique_ptr<FileReader>* reader) {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(_format == FileFormat::PARQUET);
    DORIS_CHECK(_runtime_state != nullptr);
    const bool mapping_timestamp_tz = _scan_params != nullptr &&
                                      _scan_params->__isset.enable_mapping_timestamp_tz &&
                                      _scan_params->enable_mapping_timestamp_tz;
    const bool mapping_varbinary = _scan_params != nullptr &&
                                   _scan_params->__isset.enable_mapping_varbinary &&
                                   _scan_params->enable_mapping_varbinary;
    // Spark's legacy INT96 timestamps are instants. Use the query timezone, independently
    // of Hive's INT96 compatibility setting; timestamp_ntz keeps its Parquet wall-clock flag.
    *reader = std::make_unique<parquet::ParquetReader>(
            _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
            _global_rowid_context, mapping_timestamp_tz, mapping_varbinary,
            _runtime_state->timezone(), false, _parquet_profile_cache);
    return Status::OK();
}

Status DeltaReader::annotate_file_schema(std::vector<ColumnDefinition>* file_schema) {
    DORIS_CHECK(file_schema != nullptr);
    if (_scan_params != nullptr && _scan_params->__isset.deltalake_column_mapping_mode) {
        switch (_scan_params->deltalake_column_mapping_mode) {
        case TDeltaLakeColumnMappingMode::NONE:
            return Status::OK();
        case TDeltaLakeColumnMappingMode::ID:
            // A file containing ID 3 has no value for ID 7, even if both have the same name.
            // Spark rejects an entirely untagged footer when the scan requests a field by ID.
            if (!std::ranges::any_of(*file_schema, has_data_field_id) &&
                std::ranges::any_of(_projected_columns, [](const auto& column) {
                    return !column.is_partition_key && !column.is_synthesized &&
                           has_data_field_id(column);
                })) {
                return Status::DataQualityError("Delta id mapping requires Parquet field IDs in {}",
                                                _current_file_range_desc.path);
            }
            return Status::OK();
        case TDeltaLakeColumnMappingMode::NAME:
            // The physical-name dictionary below supplies the IDs used by the mapper. An old
            // field with a different physical name must not match by its coincidental footer ID.
            clear_parquet_field_ids(*file_schema);
            break;
        }
    }
    // Plans from older FEs carried only the physical-name dictionary.
    if (mapping_mode() == TableColumnMappingMode::BY_FIELD_ID) {
        return annotate_file_schema_from_history(_scan_params, _split_schema_id, file_schema);
    }
    return Status::OK();
}

Status DeltaReader::_parse_deletion_vector_file(const TTableFormatFileDesc& t_desc,
                                                DeleteFileDesc* desc, bool* has_delete_file) {
    DORIS_CHECK(desc != nullptr);
    DORIS_CHECK(has_delete_file != nullptr);
    *has_delete_file = false;
    const auto& params = t_desc.deltalake_params;
    if (!params.__isset.deletion_vector) {
        return Status::OK();
    }
    const auto& dv = params.deletion_vector;
    DORIS_CHECK(dv.__isset.size_in_bytes);
    DORIS_CHECK(dv.__isset.cardinality);
    if (dv.cardinality < 0) {
        return Status::DataQualityError("Delta deletion vector cardinality is negative: {}",
                                        dv.cardinality);
    }
    size_t bytes_read = 0;
    RETURN_IF_ERROR(validate_delta_deletion_vector_read_range(dv.__isset.offset ? dv.offset : 0,
                                                              dv.size_in_bytes, bytes_read));
    desc->format = DeleteFileDesc::Format::DELTA;
    desc->expected_cardinality = static_cast<uint64_t>(dv.cardinality);
    if (dv.__isset.inline_data) {
        DORIS_CHECK(!dv.__isset.path);
        if (dv.inline_data.size() != static_cast<size_t>(dv.size_in_bytes)) {
            return Status::DataQualityError(
                    "Inline Delta deletion vector size mismatch, expected: {}, actual: {}",
                    dv.size_in_bytes, dv.inline_data.size());
        }
        desc->inline_data = dv.inline_data;
        desc->size = dv.size_in_bytes;
        // The exact bytes avoid hash collisions and distinguish different inline DVs without
        // performing I/O. Binary strings, including embedded NULs, are valid KV-cache keys.
        desc->key = "deltalake_dv_inline_" + dv.inline_data;
    } else {
        DORIS_CHECK(dv.__isset.path);
        desc->path = dv.path;
        desc->start_offset = dv.__isset.offset ? dv.offset : 0;
        desc->size = static_cast<int64_t>(bytes_read);
        desc->key =
                fmt::format("deltalake_dv_{}#{}#{}", desc->path, desc->start_offset, desc->size);
    }
    *has_delete_file = true;
    return Status::OK();
}

} // namespace doris::format::deltalake
