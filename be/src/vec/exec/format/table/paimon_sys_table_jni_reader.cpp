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

#include "paimon_sys_table_jni_reader.h"

#include "runtime/runtime_state.h"
#include "util/string_util.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

const std::string PaimonSysTableJniReader::HADOOP_OPTION_PREFIX = "hadoop.";
const std::string PaimonSysTableJniReader::PAIMON_OPTION_PREFIX = "paimon.";

PaimonSysTableJniReader::PaimonSysTableJniReader(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        RuntimeProfile* profile, const TPaimonMetadataParams& range_params)
        : JniReader(file_slot_descs, state, profile), _range_params(range_params) {
    std::vector<std::string> required_fields;
    std::vector<std::string> required_types;
    for (const auto& desc : _file_slot_descs) {
        required_fields.emplace_back(desc->col_name());
        required_types.emplace_back(JniConnector::get_jni_type_with_different_string(desc->type()));
    }

    std::map<std::string, std::string> params;
    params["db_name"] = _range_params.db_name;
    params["tbl_name"] = _range_params.tbl_name;
    params["query_type"] = _range_params.query_type;
    params["ctl_id"] = std::to_string(_range_params.ctl_id);
    params["db_id"] = std::to_string(_range_params.db_id);
    params["tbl_id"] = std::to_string(_range_params.tbl_id);
    params["serialized_split"] = _range_params.serialized_split;
    params["required_fields"] = join(required_fields, ",");
    params["required_types"] = join(required_types, "#");

    for (const auto& kv : _range_params.paimon_props) {
        params[PAIMON_OPTION_PREFIX + kv.first] = kv.second;
    }

    for (const auto& kv : _range_params.hadoop_props) {
        params[HADOOP_OPTION_PREFIX + kv.first] = kv.second;
    }

    _jni_connector = std::make_unique<JniConnector>(
            "org/apache/doris/paimon/PaimonSysTableJniScanner", std::move(params), required_fields);
}

Status PaimonSysTableJniReader::init_reader(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
