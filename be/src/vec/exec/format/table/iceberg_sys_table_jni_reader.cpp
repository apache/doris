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

#include "runtime/runtime_state.h"
#include "util/string_util.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

static const std::string HADOOP_OPTION_PREFIX = "hadoop.";

IcebergSysTableJniReader::IcebergSysTableJniReader(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        RuntimeProfile* profile, const TIcebergMetadataParams& range_params)
        : JniReader(file_slot_descs, state, profile), _range_params(range_params) {}

Status IcebergSysTableJniReader::init_reader(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    std::vector<std::string> required_fields;
    std::vector<std::string> required_types;
    for (const auto& desc : _file_slot_descs) {
        required_fields.emplace_back(desc->col_name());
        required_types.emplace_back(JniConnector::get_jni_type_with_different_string(desc->type()));
    }
    std::map<std::string, std::string> params;
    params["serialized_task"] = _range_params.serialized_task;
    params["required_fields"] = join(required_fields, ",");
    params["required_types"] = join(required_types, "#");
    params["time_zone"] = _state->timezone_obj().name();
    for (const auto& kv : _range_params.hadoop_props) {
        params[HADOOP_OPTION_PREFIX + kv.first] = kv.second;
    }
    _jni_connector =
            std::make_unique<JniConnector>("org/apache/doris/iceberg/IcebergSysTableJniScanner",
                                           std::move(params), required_fields);
    if (_jni_connector == nullptr) {
        return Status::InternalError("JniConnector failed to initialize");
    }
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
