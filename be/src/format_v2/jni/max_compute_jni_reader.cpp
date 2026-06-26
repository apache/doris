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

#include "format_v2/jni/max_compute_jni_reader.h"

#include "core/block/block.h"
#include "exprs/vexpr_context.h"

namespace doris::format::max_compute {

MaxComputeJniReader::MaxComputeJniReader(const doris::MaxComputeTableDescriptor* table_desc)
        : _table_desc(table_desc) {}

Status MaxComputeJniReader::validate_scan_range(const TFileRangeDesc& range) const {
    if (!range.__isset.table_format_params) {
        return Status::InternalError("missing table_format_params for max compute jni reader");
    }
    if (!range.table_format_params.__isset.max_compute_params) {
        return Status::InternalError("missing max_compute_params for max compute jni reader");
    }
    const auto& max_compute_params = range.table_format_params.max_compute_params;
    if (!max_compute_params.__isset.session_id || max_compute_params.session_id.empty()) {
        return Status::InternalError(
                "missing session_id for max compute jni reader, possibly caused by FE/BE "
                "protocol mismatch");
    }
    if (!max_compute_params.__isset.table_batch_read_session ||
        max_compute_params.table_batch_read_session.empty()) {
        return Status::InternalError(
                "missing table_batch_read_session for max compute jni reader, possibly caused "
                "by FE/BE protocol mismatch");
    }
    if (!range.__isset.start_offset) {
        return Status::InternalError(
                "missing start_offset for max compute jni reader, possibly caused by FE/BE "
                "protocol mismatch");
    }
    if (!range.__isset.size) {
        return Status::InternalError(
                "missing size for max compute jni reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    if (_scan_params == nullptr) {
        return Status::InternalError(
                "missing scan params for max compute jni reader, possibly caused by FE/BE "
                "protocol mismatch");
    }
    return Status::OK();
}

std::string MaxComputeJniReader::connector_class() const {
    return "org/apache/doris/maxcompute/MaxComputeJniScanner";
}

Status MaxComputeJniReader::build_scanner_params(std::map<std::string, std::string>* params) const {
    DORIS_CHECK(params != nullptr);
    DORIS_CHECK(_table_desc != nullptr);
    params->clear();

    *params = _table_desc->properties();
    (*params)["endpoint"] = _table_desc->endpoint();
    (*params)["quota"] = _table_desc->quota();
    (*params)["project"] = _table_desc->project();
    (*params)["table"] = _table_desc->table();

    const auto& max_compute_params = _current_range.table_format_params.max_compute_params;
    (*params)["session_id"] = max_compute_params.session_id;
    (*params)["scan_serializer"] = max_compute_params.table_batch_read_session;
    (*params)["start_offset"] = std::to_string(_current_range.start_offset);
    (*params)["split_size"] = std::to_string(_current_range.size);
    (*params)["connect_timeout"] = std::to_string(max_compute_params.connect_timeout);
    (*params)["read_timeout"] = std::to_string(max_compute_params.read_timeout);
    (*params)["retry_count"] = std::to_string(max_compute_params.retry_times);
    return Status::OK();
}

Status MaxComputeJniReader::build_jni_columns(
        std::vector<format::JniTableReader::JniColumn>* columns) const {
    DORIS_CHECK(columns != nullptr);
    columns->clear();
    columns->reserve(_projected_columns.size());
    for (size_t i = 0; i < _projected_columns.size(); ++i) {
        const auto& table_column = _projected_columns[i];
        if (table_column.is_partition_key &&
            find_partition_value(table_column, _partition_values) != nullptr) {
            continue;
        }
        columns->push_back({
                .java_name = table_column.name,
                .output_index = i,
                .output_type = table_column.type,
                .transfer_type = table_column.type,
                .replace_type = "not_replace",
        });
    }
    return Status::OK();
}

Status MaxComputeJniReader::finalize_jni_block(Block* jni_block, Block* output_block,
                                               size_t* rows) {
    DORIS_CHECK(jni_block != nullptr);
    DORIS_CHECK(output_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    const auto original_rows = *rows;

    const auto& columns = jni_columns();
    DORIS_CHECK(columns.size() == jni_block->columns());
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& column = columns[i];
        DORIS_CHECK(column.output_index < output_block->columns());
        output_block->get_by_position(column.output_index).type = column.output_type;
        output_block->replace_by_position(column.output_index,
                                          jni_block->get_by_position(i).column);
    }

    for (size_t i = 0; i < _projected_columns.size(); ++i) {
        const auto& table_column = _projected_columns[i];
        const auto* partition_value = find_partition_value(table_column, _partition_values);
        if (!table_column.is_partition_key || partition_value == nullptr) {
            continue;
        }
        output_block->get_by_position(i).type = table_column.type;
        output_block->replace_by_position(
                i, table_column.type->create_column_const(original_rows, *partition_value));
    }
    DORIS_CHECK(output_block->rows() == original_rows);
    if (!_conjuncts.empty()) {
        RETURN_IF_ERROR(
                VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
    }
    *rows = output_block->rows();
    return Status::OK();
}

} // namespace doris::format::max_compute
