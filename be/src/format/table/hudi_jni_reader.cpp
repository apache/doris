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

#include "core/types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class Block;
} // namespace doris

namespace doris {
const std::string HudiJniReader::HOODIE_CONF_PREFIX = "hoodie.";
const std::string HudiJniReader::HADOOP_CONF_PREFIX = "hadoop_conf.";

HudiJniReader::HudiJniReader(const TFileScanRangeParams& scan_params,
                             const THudiFileDesc& hudi_params,
                             const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state, RuntimeProfile* profile)
        : JniReader(
                  file_slot_descs, state, profile, "org/apache/doris/hudi/HadoopHudiJniScanner",
                  [&]() {
                      std::vector<std::string> required_fields;
                      for (const auto& desc : file_slot_descs) {
                          required_fields.emplace_back(desc->col_name());
                      }
                      std::map<String, String> params = {
                              {"query_id", print_id(state->query_id())},
                              {"base_path", hudi_params.base_path},
                              {"data_file_path", hudi_params.data_file_path},
                              {"data_file_length", std::to_string(hudi_params.data_file_length)},
                              {"delta_file_paths", join(hudi_params.delta_logs, ",")},
                              {"hudi_column_names", join(hudi_params.column_names, ",")},
                              {"hudi_column_types", join(hudi_params.column_types, "#")},
                              {"required_fields", join(required_fields, ",")},
                              {"instant_time", hudi_params.instant_time},
                              {"serde", hudi_params.serde},
                              {"input_format", hudi_params.input_format},
                              {"time_zone", state->timezone_obj().name()}};
                      for (const auto& kv : scan_params.properties) {
                          if (kv.first.starts_with(HOODIE_CONF_PREFIX)) {
                              params[kv.first] = kv.second;
                          } else {
                              params[HADOOP_CONF_PREFIX + kv.first] = kv.second;
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
                  }()) {}

Status HudiJniReader::init_reader() {
    return open(_state, _profile);
}
} // namespace doris
