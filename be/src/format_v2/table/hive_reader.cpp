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

#include "format_v2/table/hive_reader.h"

#include <utility>

#include "common/consts.h"
#include "format_v2/column_mapper.h"
#include "format_v2/file_reader.h"
#include "runtime/runtime_state.h"

namespace doris::format::hive {
namespace {

TFileFormatType::type format_type_from_context(const format::ProjectedColumnBuildContext& context) {
    DORIS_CHECK(context.scan_params != nullptr);
    if (context.range != nullptr && context.range->__isset.format_type) {
        return context.range->format_type;
    }
    return context.scan_params->format_type;
}

bool use_column_position_mapping(const format::ProjectedColumnBuildContext& context) {
    if (context.runtime_state == nullptr || context.scan_params == nullptr) {
        return false;
    }
    switch (format_type_from_context(context)) {
    case TFileFormatType::FORMAT_PARQUET:
        return !context.runtime_state->query_options().hive_parquet_use_column_names;
    default:
        return false;
    }
}

bool is_file_column_position_slot(const TFileScanSlotInfo& slot_info,
                                  const std::string& column_name) {
    if (column_name.starts_with(BeConsts::GLOBAL_ROWID_COL) ||
        column_name == BeConsts::ICEBERG_ROWID_COL) {
        return false;
    }
    if (slot_info.__isset.is_file_slot) {
        return slot_info.is_file_slot;
    }
    return !slot_info.__isset.category || slot_info.category != TColumnCategory::PARTITION_KEY;
}

} // namespace

Status HiveReader::init(format::TableReadOptions&& options) {
    const format::FileFormat file_format = options.format;
    RETURN_IF_ERROR(format::TableReader::init(std::move(options)));

    // Hive-specific behavior: choose the column matching mode based on file format and the
    // matching session variable.
    //   - hive_orc_use_column_names / hive_parquet_use_column_names == true
    //     => BY_NAME (modern Hive default, match by column name)
    //   - those options == false
    //     => BY_INDEX (mainly for Hive1 ORC `_col0` / `_col1`, match by top-level position;
    //                  Parquet exposes the same switch for consistency)
    // The base init path does not accept file-format-specific mapper configuration, so the mapper
    // must be replaced here after the base initialization completes.
    DORIS_CHECK(_runtime_state != nullptr);
    const auto& query_options = _runtime_state->query_options();
    bool use_column_names = true;
    if (file_format == format::FileFormat::ORC) {
        use_column_names = query_options.hive_orc_use_column_names;
    } else if (file_format == format::FileFormat::PARQUET) {
        use_column_names = query_options.hive_parquet_use_column_names;
    } else {
        return Status::NotSupported("HiveReader does not support file reader format {}",
                                    file_format);
    }

    _mode = use_column_names ? format::TableColumnMappingMode::BY_NAME
                             : format::TableColumnMappingMode::BY_INDEX;
    return Status::OK();
}

Status HiveReader::annotate_projected_column(const TFileScanSlotInfo& slot_info,
                                             format::ProjectedColumnBuildContext* context,
                                             format::ColumnDefinition* column) const {
    RETURN_IF_ERROR(format::TableReader::annotate_projected_column(slot_info, context, column));
    DORIS_CHECK(context != nullptr);
    DORIS_CHECK(column != nullptr);
    if (!use_column_position_mapping(*context) ||
        !is_file_column_position_slot(slot_info, column->name)) {
        return Status::OK();
    }
    const auto* scan_params = context->scan_params;
    DORIS_CHECK(scan_params != nullptr);
    if (!scan_params->__isset.column_idxs ||
        context->next_file_column_idx >= scan_params->column_idxs.size()) {
        return Status::InvalidArgument(
                "Hive positional column mapping is missing file index for column '{}', "
                "required file slot ordinal={}, column_idxs_size={}",
                column->name, context->next_file_column_idx,
                scan_params->__isset.column_idxs ? scan_params->column_idxs.size() : 0);
    }
    const auto file_index = scan_params->column_idxs[context->next_file_column_idx];
    if (file_index < 0) {
        return Status::InvalidArgument(
                "Hive positional column mapping has negative file index {} for column '{}'",
                file_index, column->name);
    }
    column->identifier = Field::create_field<TYPE_INT>(file_index);
    ++context->next_file_column_idx;
    return Status::OK();
}

Status HiveReader::validate_projected_columns(
        const format::ProjectedColumnBuildContext& context) const {
    if (!use_column_position_mapping(context)) {
        return Status::OK();
    }
    DORIS_CHECK(context.scan_params != nullptr);
    if (context.scan_params->__isset.column_idxs &&
        context.next_file_column_idx != context.scan_params->column_idxs.size()) {
        return Status::InvalidArgument(
                "Hive positional column mapping has unused file indexes: consumed={}, "
                "column_idxs_size={}",
                context.next_file_column_idx, context.scan_params->column_idxs.size());
    }
    return Status::OK();
}

} // namespace doris::format::hive
