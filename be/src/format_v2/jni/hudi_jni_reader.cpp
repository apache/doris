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

#include "format_v2/jni/hudi_jni_reader.h"

#include <string_view>

#include "core/block/block.h"
#include "exprs/vexpr_context.h"
#include "util/string_util.h"
#include "util/uid_util.h"

namespace doris::format::hudi {
namespace {

constexpr std::string_view HOODIE_CONF_PREFIX = "hoodie.";
constexpr std::string_view HADOOP_CONF_PREFIX = "hadoop_conf.";

} // namespace

Status HudiJniReader::validate_scan_range(const TFileRangeDesc& range) const {
    if (!range.__isset.table_format_params) {
        return Status::InternalError("missing table_format_params for hudi jni reader");
    }
    if (!range.table_format_params.__isset.hudi_params) {
        return Status::InternalError("missing hudi_params for hudi jni reader");
    }
    const auto& hudi_params = range.table_format_params.hudi_params;
    if (!hudi_params.__isset.base_path || hudi_params.base_path.empty()) {
        return Status::InternalError(
                "missing base_path for hudi jni reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    if (!hudi_params.__isset.data_file_path || hudi_params.data_file_path.empty()) {
        return Status::InternalError(
                "missing data_file_path for hudi jni reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    if (!hudi_params.__isset.data_file_length) {
        return Status::InternalError(
                "missing data_file_length for hudi jni reader, possibly caused by FE/BE "
                "protocol mismatch");
    }
    if (!hudi_params.__isset.column_names) {
        return Status::InternalError(
                "missing column_names for hudi jni reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    if (!hudi_params.__isset.column_types) {
        return Status::InternalError(
                "missing column_types for hudi jni reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    DORIS_CHECK(hudi_params.column_names.size() == hudi_params.column_types.size());
    if (_scan_params == nullptr) {
        return Status::InternalError(
                "missing scan params for hudi jni reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    return Status::OK();
}

std::string HudiJniReader::connector_class() const {
    return "org/apache/doris/hudi/HadoopHudiJniScanner";
}

Status HudiJniReader::build_scanner_params(std::map<std::string, std::string>* params) const {
    DORIS_CHECK(params != nullptr);
    DORIS_CHECK(_scan_params != nullptr);
    params->clear();

    const auto& hudi_params = _current_range.table_format_params.hudi_params;
    (*params)["base_path"] = hudi_params.base_path;
    (*params)["data_file_path"] = hudi_params.data_file_path;
    (*params)["data_file_length"] = std::to_string(hudi_params.data_file_length);
    (*params)["delta_file_paths"] = join(hudi_params.delta_logs, ",");
    (*params)["hudi_column_names"] = join(hudi_params.column_names, ",");
    (*params)["hudi_column_types"] = join(hudi_params.column_types, "#");
    (*params)["instant_time"] = hudi_params.instant_time;
    (*params)["serde"] = hudi_params.serde;
    (*params)["input_format"] = hudi_params.input_format;
    if (_runtime_state != nullptr) {
        (*params)["query_id"] = print_id(_runtime_state->query_id());
    }

    for (const auto& kv : _scan_params->properties) {
        if (kv.first.starts_with(HOODIE_CONF_PREFIX)) {
            (*params)[kv.first] = kv.second;
        } else {
            (*params)[std::string(HADOOP_CONF_PREFIX) + kv.first] = kv.second;
        }
    }
    return Status::OK();
}

Status HudiJniReader::build_jni_columns(
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

Status HudiJniReader::finalize_jni_block(Block* jni_block, Block* output_block, size_t* rows) {
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

} // namespace doris::format::hudi
