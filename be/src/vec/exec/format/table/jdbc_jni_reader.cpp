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

#include "jdbc_jni_reader.h"

#include <sstream>

#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/jdbc_utils.h"
#include "vec/core/types.h"
#include "vec/exec/jni_data_bridge.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

JdbcJniReader::JdbcJniReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state, RuntimeProfile* profile,
                             const std::map<std::string, std::string>& jdbc_params)
        : JniReader(
                  file_slot_descs, state, profile, "org/apache/doris/jdbc/JdbcJniScanner",
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
                      // Merge JDBC-specific params with schema params
                      std::map<std::string, std::string> params = jdbc_params;
                      params["required_fields"] = required_fields.str();
                      params["columns_types"] = columns_types.str();
                      // Resolve jdbc_driver_url to absolute file:// URL
                      if (params.count("jdbc_driver_url")) {
                          std::string resolved;
                          if (JdbcUtils::resolve_driver_url(params["jdbc_driver_url"], &resolved)
                                      .ok()) {
                              params["jdbc_driver_url"] = resolved;
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
                  }()),
          _jdbc_params(jdbc_params) {}

Status JdbcJniReader::init_reader() {
    return open(_state, _profile);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
