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

#include "format_v2/jni/fluss_jni_reader.h"

#include "core/block/block.h"
#include "exprs/vexpr_context.h"
#include "format_v2/column_mapper.h"

namespace doris::format::fluss {

Status FlussJniReader::validate_scan_range(const TFileRangeDesc& range) const {
    if (!range.__isset.table_format_params) {
        return Status::InternalError("missing table_format_params for fluss jni reader");
    }
    if (!range.table_format_params.__isset.fluss_params ||
        range.table_format_params.fluss_params.empty()) {
        return Status::InternalError(
                "missing fluss_params for fluss jni reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    if (_scan_params == nullptr || !_scan_params->__isset.fluss_properties ||
        _scan_params->fluss_properties.empty()) {
        return Status::InternalError(
                "missing fluss_properties for fluss jni reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    return Status::OK();
}

std::string FlussJniReader::connector_class() const {
    return "org/apache/doris/fluss/FlussJniScanner";
}

Status FlussJniReader::build_scanner_params(std::map<std::string, std::string>* params) const {
    DORIS_CHECK(params != nullptr);
    DORIS_CHECK(_scan_params != nullptr);
    // Scan level first, then the range: the range is the more specific of the two, and it is the one
    // that says which bucket and offsets to read. No key is transcribed - both maps are written by FE
    // and read by the Java scanner, so naming them here would only add a place to drift.
    *params = _scan_params->fluss_properties;
    for (const auto& kv : _current_range.table_format_params.fluss_params) {
        (*params)[kv.first] = kv.second;
    }
    return Status::OK();
}

Status FlussJniReader::build_jni_columns(
        std::vector<format::JniTableReader::JniColumn>* columns) const {
    DORIS_CHECK(columns != nullptr);
    columns->clear();
    columns->reserve(_projected_columns.size());
    for (size_t i = 0; i < _projected_columns.size(); ++i) {
        const auto& table_column = _projected_columns[i];
        // A fluss row physically carries its partition column values, but FE declares the partition
        // keys as path_partition_keys and ships them per range, so they are constants here. Asking
        // the scanner for them would read per row what the split already states once - and would
        // read them differently from the legacy JNI path, whose file slots exclude them outright.
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

Status FlussJniReader::finalize_jni_block(Block* jni_block, Block* output_block, size_t* rows) {
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

    // The columns build_jni_columns() left out: materialized from the range instead of read.
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

} // namespace doris::format::fluss
