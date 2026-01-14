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

#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/jni-util.h"
#include "vec/core/types.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
const std::string TrinoConnectorJniReader::TRINO_CONNECTOR_OPTION_PREFIX = "trino.";

TrinoConnectorJniReader::TrinoConnectorJniReader(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        RuntimeProfile* profile, const TFileRangeDesc& range)
        : JniReader(file_slot_descs, state, profile) {
    std::vector<std::string> column_names;
    std::vector<std::string> column_types;
    for (const auto& desc : _file_slot_descs) {
        std::string field = desc->col_name();
        column_names.emplace_back(field);
        column_types.emplace_back(JniConnector::get_jni_type_with_different_string(desc->type()));
    }
    std::map<String, String> params = {
            {"catalog_name", range.table_format_params.trino_connector_params.catalog_name},
            {"db_name", range.table_format_params.trino_connector_params.db_name},
            {"table_name", range.table_format_params.trino_connector_params.table_name},
            {"trino_connector_split",
             range.table_format_params.trino_connector_params.trino_connector_split},
            {"trino_connector_table_handle",
             range.table_format_params.trino_connector_params.trino_connector_table_handle},
            {"trino_connector_column_handles",
             range.table_format_params.trino_connector_params.trino_connector_column_handles},
            {"trino_connector_column_metadata",
             range.table_format_params.trino_connector_params.trino_connector_column_metadata},
            {"trino_connector_predicate",
             range.table_format_params.trino_connector_params.trino_connector_predicate},
            {"trino_connector_trascation_handle",
             range.table_format_params.trino_connector_params.trino_connector_trascation_handle},
            {"required_fields", join(column_names, ",")},
            {"columns_types", join(column_types, "#")}};

    // Used to create trino connector options
    for (const auto& kv :
         range.table_format_params.trino_connector_params.trino_connector_options) {
        params[TRINO_CONNECTOR_OPTION_PREFIX + kv.first] = kv.second;
    }
    _jni_connector = std::make_unique<JniConnector>(
            "org/apache/doris/trinoconnector/TrinoConnectorJniScanner", params, column_names);
}

Status TrinoConnectorJniReader::init_reader() {
    RETURN_IF_ERROR(_jni_connector->init());
    RETURN_IF_ERROR(_set_spi_plugins_dir());
    return _jni_connector->open(_state, _profile);
}

Status TrinoConnectorJniReader::_set_spi_plugins_dir() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    // get PluginLoader class
    Jni::LocalClass plugin_loader_cls;
    std::string plugin_loader_str = "org/apache/doris/trinoconnector/TrinoConnectorPluginLoader";
    RETURN_IF_ERROR(
            Jni::Util::get_jni_scanner_class(env, plugin_loader_str.c_str(), &plugin_loader_cls));

    Jni::MethodId set_plugins_dir_method;
    RETURN_IF_ERROR(plugin_loader_cls.get_static_method(
            env, "setPluginsDir", "(Ljava/lang/String;)V", &set_plugins_dir_method));

    Jni::LocalString trino_connector_plugin_path;
    RETURN_IF_ERROR(Jni::LocalString::new_string(
            env, doris::config::trino_connector_plugin_dir.c_str(), &trino_connector_plugin_path));

    return plugin_loader_cls.call_static_void_method(env, set_plugins_dir_method)
            .with_arg(trino_connector_plugin_path)
            .call();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
