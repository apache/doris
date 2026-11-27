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
#include "format_v2/table/delta_sys_table_reader.h"
#include "format_v2/table/parquet_variant_schema.h"
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
    if (current_split_pruned() ||
        options.current_range.table_format_params.deltalake_params.__isset.sys_table_batch) {
        return Status::OK();
    }
    const auto& delta = options.current_range.table_format_params.deltalake_params;
    if (delta.__isset.cdf) {
        const auto& cdf = delta.cdf;
        DORIS_CHECK(cdf.__isset.commit_version && cdf.__isset.commit_timestamp_millis);
        _partition_values.emplace("_commit_version",
                                  Field::create_field<TYPE_BIGINT>(cdf.commit_version));
        if (cdf.__isset.constant_change_type) {
            _partition_values.emplace("_change_type",
                                      Field::create_field<TYPE_STRING>(cdf.constant_change_type));
        }
        for (const auto& column : _projected_columns) {
            if (iequal(column.name, "_commit_timestamp")) {
                Field timestamp;
                RETURN_IF_ERROR(delta_timestamp_field(
                        cdf.commit_timestamp_millis, _runtime_state->timezone_obj(),
                        remove_nullable(column.type)->get_primitive_type() == TYPE_TIMESTAMPTZ,
                        &timestamp));
                _partition_values.emplace(column.name, std::move(timestamp));
            }
        }
    }
    if (delta.__isset.cdf &&
        delta.cdf.row_filter_mode == TDeltaLakeCdfRowFilterMode::KEEP_DELETION_VECTOR_DIFF) {
        DORIS_CHECK(_deletion_vector != nullptr);
        if (delta.cdf.__isset.base_deletion_vector) {
            DeleteFileDesc desc {.fs_name = options.current_range.fs_name};
            RETURN_IF_ERROR(describe_deletion_vector(delta.cdf.base_deletion_vector, &desc));
            std::shared_ptr<DeletionVector> base;
            RETURN_IF_ERROR(_load_deletion_vector(options, desc, &base));
            _deletion_vector = std::make_shared<DeletionVector>(*_deletion_vector - *base);
        }
        _deletion_vector_keep_only = true;
        if (_deletion_vector->isEmpty()) {
            _current_split_pruned = true;
            return Status::OK();
        }
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

bool DeltaReader::_should_enable_condition_cache(const FileScanRequest& file_request) const {
    // System table splits have virtual paths and no persistent file row offsets.
    const auto& delta = _current_file_range_desc.table_format_params.deltalake_params;
    return !delta.__isset.sys_table_batch && !delta.__isset.cdf &&
           TableReader::_should_enable_condition_cache(file_request);
}

Status DeltaReader::create_file_reader(std::unique_ptr<FileReader>* reader) {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(_format == FileFormat::PARQUET);
    DORIS_CHECK(_runtime_state != nullptr);
    const auto& delta = _current_file_range_desc.table_format_params.deltalake_params;
    if (delta.__isset.sys_table_batch) {
        *reader = std::make_unique<DeltaSysTableReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
                delta.sys_table_batch, _projected_columns);
        return Status::OK();
    }
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
    _variant_schema_overrides.clear();
    if (_current_file_range_desc.table_format_params.deltalake_params.__isset.sys_table_batch) {
        return Status::OK();
    }
    const auto mode = mapping_mode();
    if (mode == TableColumnMappingMode::BY_FIELD_ID) {
        if (_scan_params->__isset.deltalake_column_mapping_mode &&
            _scan_params->deltalake_column_mapping_mode == TDeltaLakeColumnMappingMode::ID) {
            // A file containing ID 3 has no value for ID 7, even if both have the same name.
            // Spark rejects an entirely untagged footer when the scan requests a field by ID.
            if (!std::ranges::any_of(*file_schema, has_data_field_id) &&
                std::ranges::any_of(_projected_columns, [&](const auto& column) {
                    if (_current_file_range_desc.table_format_params.deltalake_params.__isset.cdf &&
                        (iequal(column.name, "_change_type") ||
                         iequal(column.name, "_commit_version") ||
                         iequal(column.name, "_commit_timestamp"))) {
                        return false;
                    }
                    return !column.is_partition_key && !column.is_synthesized &&
                           has_data_field_id(column);
                })) {
                return Status::DataQualityError("Delta id mapping requires Parquet field IDs in {}",
                                                _current_file_range_desc.path);
            }
        } else {
            if (_scan_params->__isset.deltalake_column_mapping_mode) {
                // NAME matches the physical dictionary. An old file's coincidental footer ID
                // must not match a dropped and re-added field.
                clear_parquet_field_ids(*file_schema);
            }
            RETURN_IF_ERROR(
                    annotate_file_schema_from_history(_scan_params, _split_schema_id, file_schema));
            // The request carries these markers into Parquet. Keep the mapper's file block in
            // sync, e.g. for an INT96 instant beside a Variant inside a mapped struct.
            for (auto& column : *file_schema) {
                if (has_timestamp_semantics(column)) {
                    RETURN_IF_ERROR(apply_file_timestamp_semantics(&column));
                }
            }
        }
    }
    return annotate_parquet_variants(_projected_columns, file_schema, mode,
                                     &_variant_schema_overrides);
}

void DeltaReader::configure_mapper_options(TableColumnMapperOptions* options) const {
    options->delta_change_feed =
            _current_file_range_desc.table_format_params.deltalake_params.__isset.cdf;
}

Status DeltaReader::customize_file_scan_request(FileScanRequest* file_request) {
    RETURN_IF_ERROR(TableReader::customize_file_scan_request(file_request));
    file_request->variant_schema_overrides = _variant_schema_overrides;
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
    RETURN_IF_ERROR(describe_deletion_vector(params.deletion_vector, desc));
    *has_delete_file = true;
    return Status::OK();
}

Status DeltaReader::describe_deletion_vector(const TDeltaLakeDeletionVectorDesc& dv,
                                             DeleteFileDesc* desc) {
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
    return Status::OK();
}

} // namespace doris::format::deltalake
