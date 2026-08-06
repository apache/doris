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

#include "iceberg_sys_table_jni_reader.h"

#include "common/logging.h"
#include "format/jni/jni_data_bridge.h"
#include "runtime/runtime_state.h"
#include "util/string_util.h"

namespace doris {

static const std::string HADOOP_OPTION_PREFIX = "hadoop.";

Status IcebergSysTableJniReader::validate_scan_range(const TFileRangeDesc& range) {
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

IcebergSysTableJniReader::IcebergSysTableJniReader(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        RuntimeProfile* profile, const TFileRangeDesc& range,
        const TFileScanRangeParams* range_params)
        : JniReader(
                  file_slot_descs, state, profile,
                  "org/apache/doris/iceberg/IcebergSysTableJniScanner",
                  [&]() {
                      std::vector<std::string> required_fields;
                      std::vector<std::string> required_types;
                      for (const auto& desc : file_slot_descs) {
                          required_fields.emplace_back(desc->col_name());
                          required_types.emplace_back(
                                  JniDataBridge::get_jni_type_with_different_string(desc->type()));
                      }
                      std::map<std::string, std::string> params;
                      params["serialized_split"] =
                              range.table_format_params.iceberg_params.serialized_split;
                      params["required_fields"] = join(required_fields, ",");
                      params["required_types"] = join(required_types, "#");
                      params["time_zone"] = state->timezone();
                      if (range_params != nullptr && range_params->__isset.properties &&
                          !range_params->properties.empty()) {
                          for (const auto& kv : range_params->properties) {
                              params[HADOOP_OPTION_PREFIX + kv.first] = kv.second;
                          }
                      }
                      return params;
                  }(),
                  [&]() {
                      std::vector<std::string> names;
                      for (const auto& desc : file_slot_descs) {
                          names.emplace_back(desc->col_name());
                      }
                      return names;
                  }(),
                  range.__isset.self_split_weight ? range.self_split_weight : -1),
          _init_status(validate_scan_range(range)) {}

Status IcebergSysTableJniReader::init_reader() {
    RETURN_IF_ERROR(_init_status);
    return open(_state, _profile);
}

} // namespace doris
