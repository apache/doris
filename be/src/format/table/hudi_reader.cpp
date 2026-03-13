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
#include "common/compile_check_begin.h"

// ============================================================================
// HudiParquetReader: on_before_init_reader
// ============================================================================
Status HudiParquetReader::on_before_init_reader(
        std::vector<ColumnDescriptor>& column_descs, std::vector<std::string>& column_names,
        std::shared_ptr<TableSchemaChangeHelper::Node>& table_info_node,
        std::set<uint64_t>& /*column_ids*/, std::set<uint64_t>& filter_column_ids,
        const TFileScanRangeParams& params, const TFileRangeDesc& range,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        RuntimeState* state, std::unordered_map<std::string, uint32_t>* col_name_to_block_idx) {
    _column_descs = &column_descs;
    _fill_col_name_to_block_idx = col_name_to_block_idx;
    // Get parquet file metadata schema (file already opened by init_reader)
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    // Build table_info_node using field_id matching (shared with Paimon/Iceberg)
    RETURN_IF_ERROR(gen_table_info_node_by_field_id(
            get_scan_params(), get_scan_range().table_format_params.hudi_params.schema_id,
            get_tuple_descriptor(), *field_desc));
    table_info_node = table_info_node_ptr;

    // Extract column names from descriptors
    for (const auto& desc : column_descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            column_names.push_back(desc.name);
        }
    }
    return Status::OK();
}

// ============================================================================
// HudiOrcReader: on_before_init_reader
// ============================================================================
Status HudiOrcReader::on_before_init_reader(
        std::vector<ColumnDescriptor>& column_descs, std::vector<std::string>& column_names,
        std::shared_ptr<TableSchemaChangeHelper::Node>& table_info_node,
        std::set<uint64_t>& /*column_ids*/, std::set<uint64_t>& filter_column_ids,
        const TFileScanRangeParams& params, const TFileRangeDesc& range,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        RuntimeState* state, std::unordered_map<std::string, uint32_t>* col_name_to_block_idx) {
    _column_descs = &column_descs;
    _fill_col_name_to_block_idx = col_name_to_block_idx;
    // Get ORC file type (file already opened by init_reader)
    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(get_file_type(&orc_type_ptr));

    // Build table_info_node using field_id matching
    RETURN_IF_ERROR(gen_table_info_node_by_field_id(
            get_scan_params(), get_scan_range().table_format_params.hudi_params.schema_id,
            get_tuple_descriptor(), orc_type_ptr));
    table_info_node = table_info_node_ptr;

    // Extract column names from descriptors
    for (const auto& desc : column_descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            column_names.push_back(desc.name);
        }
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
