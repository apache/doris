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

#include "trino_connector_jni_reader.h"

#include <map>

#include "common/config.h"
#include "core/types.h"
#include "format/jni/jni_data_bridge.h"
#include "runtime/descriptors.h"
#include "util/jni-util.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

class Block;
} // namespace doris

namespace doris {
const std::string TrinoConnectorJniReader::TRINO_CONNECTOR_OPTION_PREFIX = "trino.";
const std::string TrinoConnectorJniReader::TRINO_CONNECTOR_PLUGIN_DIR =
        "trino_connector_plugin_dir";

TrinoConnectorJniReader::TrinoConnectorJniReader(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        RuntimeProfile* profile, const TFileRangeDesc& range)
        : JniReader(
                  file_slot_descs, state, profile, Jni::plugin::TRINO_CONNECTOR_SCANNER,
                  [&]() {
                      std::vector<std::string> column_names;
                      std::vector<std::string> column_types;
                      for (const auto& desc : file_slot_descs) {
                          column_names.emplace_back(desc->col_name());
                          column_types.emplace_back(
                                  JniDataBridge::get_jni_type_with_different_string(desc->type()));
                      }
                      std::map<String, String> params = {
                              {"catalog_name",
                               range.table_format_params.trino_connector_params.catalog_name},
                              {"db_name", range.table_format_params.trino_connector_params.db_name},
                              {"table_name",
                               range.table_format_params.trino_connector_params.table_name},
                              {"trino_connector_split",
                               range.table_format_params.trino_connector_params
                                       .trino_connector_split},
                              {"trino_connector_table_handle",
                               range.table_format_params.trino_connector_params
                                       .trino_connector_table_handle},
                              {"trino_connector_column_handles",
                               range.table_format_params.trino_connector_params
                                       .trino_connector_column_handles},
                              {"trino_connector_column_metadata",
                               range.table_format_params.trino_connector_params
                                       .trino_connector_column_metadata},
                              {"trino_connector_predicate",
                               range.table_format_params.trino_connector_params
                                       .trino_connector_predicate},
                              {"trino_connector_trascation_handle",
                               range.table_format_params.trino_connector_params
                                       .trino_connector_trascation_handle},
                              {"required_fields", join(column_names, ",")},
                              {"columns_types", join(column_types, "#")}};
                      for (const auto& kv : range.table_format_params.trino_connector_params
                                                    .trino_connector_options) {
                          params[TRINO_CONNECTOR_OPTION_PREFIX + kv.first] = kv.second;
                      }
                      // Where the plugin should look for the Trino connector plugins it
                      // loads inside itself. BE used to push this by calling a static setter
                      // on a class it named directly, which stopped being possible once a
                      // plugin's classes became private to its own classloader. A scan
                      // parameter is the channel that survives isolation.
                      params[TRINO_CONNECTOR_PLUGIN_DIR] =
                              doris::config::trino_connector_plugin_dir;
                      return params;
                  }(),
                  [&]() {
                      std::vector<std::string> names;
                      for (const auto& desc : file_slot_descs) {
                          names.emplace_back(desc->col_name());
                      }
                      return names;
                  }()) {}

Status TrinoConnectorJniReader::init_reader() {
    return open(_state, _profile);
}

} // namespace doris
