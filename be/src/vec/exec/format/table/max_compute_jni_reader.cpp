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

#include "max_compute_jni_reader.h"

#include <glog/logging.h>

#include <map>
#include <ostream>
#include <sstream>

#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "vec/core/types.h"
#include "vec/exec/jni_data_bridge.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
MaxComputeJniReader::MaxComputeJniReader(const MaxComputeTableDescriptor* mc_desc,
                                         const TMaxComputeFileDesc& max_compute_params,
                                         const std::vector<SlotDescriptor*>& file_slot_descs,
                                         const TFileRangeDesc& range, RuntimeState* state,
                                         RuntimeProfile* profile)
        : JniReader(
                  file_slot_descs, state, profile,
                  "org/apache/doris/maxcompute/MaxComputeJniScanner",
                  [&]() {
                      std::ostringstream required_fields;
                      std::ostringstream columns_types;
                      int index = 0;
                      for (const auto& desc : file_slot_descs) {
                          std::string field = desc->col_name();
                          std::string type =
                                  JniDataBridge::get_jni_type_with_different_string(desc->type());
                          if (index == 0) {
                              required_fields << field;
                              columns_types << type;
                          } else {
                              required_fields << "," << field;
                              columns_types << "#" << type;
                          }
                          index++;
                      }
                      auto properties = mc_desc->properties();
                      properties["endpoint"] = mc_desc->endpoint();
                      properties["quota"] = mc_desc->quota();
                      properties["project"] = mc_desc->project();
                      properties["table"] = mc_desc->table();
                      properties["session_id"] = max_compute_params.session_id;
                      properties["scan_serializer"] = max_compute_params.table_batch_read_session;
                      properties["start_offset"] = std::to_string(range.start_offset);
                      properties["split_size"] = std::to_string(range.size);
                      properties["required_fields"] = required_fields.str();
                      properties["columns_types"] = columns_types.str();
                      properties["connect_timeout"] =
                              std::to_string(max_compute_params.connect_timeout);
                      properties["read_timeout"] = std::to_string(max_compute_params.read_timeout);
                      properties["retry_count"] = std::to_string(max_compute_params.retry_times);
                      return properties;
                  }(),
                  [&]() {
                      std::vector<std::string> names;
                      for (const auto& desc : file_slot_descs) {
                          names.emplace_back(desc->col_name());
                      }
                      return names;
                  }()) {}

Status MaxComputeJniReader::init_reader() {
    return open(_state, _profile);
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
