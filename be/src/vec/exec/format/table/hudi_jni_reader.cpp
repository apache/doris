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

#include "hudi_jni_reader.h"

#include <map>
#include <ostream>

#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "vec/core/types.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

const std::string HudiJniReader::HOODIE_CONF_PREFIX = "hoodie.";
const std::string HudiJniReader::HADOOP_CONF_PREFIX = "hadoop_conf.";

HudiJniReader::HudiJniReader(const TFileScanRangeParams& scan_params,
                             const THudiFileDesc& hudi_params,
                             const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state, RuntimeProfile* profile)
        : JniReader(file_slot_descs, state, profile),
          _scan_params(scan_params),
          _hudi_params(hudi_params) {
    std::vector<std::string> required_fields;
    for (auto& desc : _file_slot_descs) {
        required_fields.emplace_back(desc->col_name());
    }

    std::map<String, String> params = {
            {"query_id", print_id(_state->query_id())},
            {"base_path", _hudi_params.base_path},
            {"data_file_path", _hudi_params.data_file_path},
            {"data_file_length", std::to_string(_hudi_params.data_file_length)},
            {"delta_file_paths", join(_hudi_params.delta_logs, ",")},
            {"hudi_column_names", join(_hudi_params.column_names, ",")},
            {"hudi_column_types", join(_hudi_params.column_types, "#")},
            {"required_fields", join(required_fields, ",")},
            {"instant_time", _hudi_params.instant_time},
            {"serde", _hudi_params.serde},
            {"input_format", _hudi_params.input_format}};

    // Use compatible hadoop client to read data
    for (auto& kv : _scan_params.properties) {
        if (kv.first.starts_with(HOODIE_CONF_PREFIX)) {
            params[kv.first] = kv.second;
        } else {
            params[HADOOP_CONF_PREFIX + kv.first] = kv.second;
        }
    }

    _jni_connector = std::make_unique<JniConnector>("org/apache/doris/hudi/HudiJniScanner", params,
                                                    required_fields);
}

Status HudiJniReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    return _jni_connector->get_next_block(block, read_rows, eof);
}

Status HudiJniReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    for (auto& desc : _file_slot_descs) {
        name_to_type->emplace(desc->col_name(), desc->type());
    }
    return Status::OK();
}

Status HudiJniReader::init_reader(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}
} // namespace doris::vectorized
