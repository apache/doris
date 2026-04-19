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

#include "format/table/hudi_reader.h"

#include <vector>

#include "common/status.h"

namespace doris {

// ============================================================================
// HudiParquetReader: on_before_init_reader
// ============================================================================
Status HudiParquetReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    RETURN_IF_ERROR(
            _extract_partition_values(*ctx->range, ctx->tuple_descriptor, _fill_partition_values));
    // Get parquet file metadata schema (file already opened by init_reader)
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    // Build table_info_node using field_id matching (shared with Paimon/Iceberg)
    RETURN_IF_ERROR(gen_table_info_node_by_field_id(
            get_scan_params(), get_scan_range().table_format_params.hudi_params.schema_id,
            get_tuple_descriptor(), *field_desc));
    ctx->table_info_node = table_info_node_ptr;

    // Extract column names from descriptors
    for (const auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            ctx->column_names.push_back(desc.name);
        }
    }
    return Status::OK();
}

// ============================================================================
// HudiOrcReader: on_before_init_reader
// ============================================================================
Status HudiOrcReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    RETURN_IF_ERROR(
            _extract_partition_values(*ctx->range, ctx->tuple_descriptor, _fill_partition_values));
    // Get ORC file type (file already opened by init_reader)
    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(get_file_type(&orc_type_ptr));

    // Build table_info_node using field_id matching
    RETURN_IF_ERROR(gen_table_info_node_by_field_id(
            get_scan_params(), get_scan_range().table_format_params.hudi_params.schema_id,
            get_tuple_descriptor(), orc_type_ptr));
    ctx->table_info_node = table_info_node_ptr;

    // Extract column names from descriptors
    for (const auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            ctx->column_names.push_back(desc.name);
        }
    }
    return Status::OK();
}

} // namespace doris
