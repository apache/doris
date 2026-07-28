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

#include "format_v2/jni/iceberg_sys_table_reader.h"

#include <string_view>

#include "format/jni/jni_data_bridge.h"
#include "util/string_util.h"

namespace doris::format::iceberg {
namespace {

constexpr std::string_view HADOOP_OPTION_PREFIX = "hadoop.";

} // namespace

Status IcebergSysTableJniReader::validate_scan_range(const TFileRangeDesc& range) const {
    if (!range.__isset.table_format_params) {
        return Status::InternalError(
                "missing table_format_params for iceberg sys table jni reader");
    }
    if (!range.table_format_params.__isset.iceberg_params) {
        return Status::InternalError("missing iceberg_params for iceberg sys table jni reader");
    }
    if (!range.table_format_params.iceberg_params.__isset.serialized_split ||
        range.table_format_params.iceberg_params.serialized_split.empty()) {
        return Status::InternalError(
                "missing serialized_split for iceberg sys table jni reader, "
                "possibly caused by FE/BE protocol mismatch");
    }
    return Status::OK();
}

std::string IcebergSysTableJniReader::connector_class() const {
    return "org/apache/doris/iceberg/IcebergSysTableJniScanner";
}

Status IcebergSysTableJniReader::build_scanner_params(
        std::map<std::string, std::string>* params) const {
    DORIS_CHECK(params != nullptr);
    params->clear();
    params->emplace("serialized_split",
                    _current_range.table_format_params.iceberg_params.serialized_split);

    std::vector<std::string> required_types;
    required_types.reserve(_projected_columns.size());
    for (const auto& column : _projected_columns) {
        required_types.emplace_back(JniDataBridge::get_jni_type_with_different_string(column.type));
    }
    (*params)["required_types"] = join(required_types, "#");

    if (_scan_params != nullptr && _scan_params->__isset.properties &&
        !_scan_params->properties.empty()) {
        for (const auto& kv : _scan_params->properties) {
            (*params)[std::string(HADOOP_OPTION_PREFIX) + kv.first] = kv.second;
        }
    }
    return Status::OK();
}

} // namespace doris::format::iceberg
